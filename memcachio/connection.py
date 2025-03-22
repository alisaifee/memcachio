from __future__ import annotations

import asyncio
import dataclasses
import socket
import time
import weakref
from abc import ABC, abstractmethod
from asyncio import (
    BaseProtocol,
    BaseTransport,
    Event,
    Future,
    Lock,
    Transport,
    get_running_loop,
)
from collections import deque
from io import BytesIO
from typing import NotRequired, TypedDict, TypeVar, cast

from memcachio.commands import Command
from memcachio.constants import LINE_END
from memcachio.errors import MemcachedError, NotEnoughData
from memcachio.types import TCPLocator
from memcachio.utils import decodedstr

R = TypeVar("R")


class ConnectionParams(TypedDict):
    connect_timeout: float | None
    read_timeout: float | None
    socket_keepalive: NotRequired[bool | None]
    socket_keepalive_options: NotRequired[dict[int, int | bytes] | None]


@dataclasses.dataclass
class Request:
    connection: weakref.ProxyType[BaseConnection]
    command: Command  # type: ignore[type-arg]
    decode: bool = False
    encoding: str | None = None
    raise_exceptions: bool = True
    future: Future = dataclasses.field(  # type: ignore[type-arg]
        default_factory=lambda: get_running_loop().create_future(),
    )
    created_at: float = dataclasses.field(default_factory=lambda: time.time())

    def __post_init__(self) -> None:
        self.future.add_done_callback(self.cleanup)

    def cleanup(self, future: Future) -> None:  # type: ignore[type-arg]
        self.connection.metrics.last_request_processed = time.time()
        if future.done() and not future.cancelled():
            if not self.future.exception():
                self.connection.metrics.requests_processed += 1
            else:
                self.connection.metrics.requests_failed += 1

    def enforce_deadline(self, timeout: float) -> None:
        if not self.future.done():
            self.future.set_exception(
                TimeoutError(
                    f"command {decodedstr(self.command.name)} timed out after {timeout} seconds",
                ),
            )


@dataclasses.dataclass
class ConnectionMetrics:
    created_at: float = dataclasses.field(default_factory=lambda: time.time())
    requests_processed: int = 0
    requests_failed: int = 0
    last_written: float = 0
    last_read: float = 0
    last_request_processed: float = 0


class BaseConnection(BaseProtocol, ABC):
    """Wraps an asyncio connection using a custom protocol.
    Provides methods for sending commands and reading lines.
    """

    def __init__(
        self,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        max_inflight_requests: int | None = None,
    ) -> None:
        self._transport: Transport | None = None
        self._buffer = BytesIO()
        self._request_queue: deque[Request] = deque()
        self._processed_queue: deque[Request] = deque()
        self._write_ready: Event = Event()
        self._transport_lock: Lock = Lock()
        self._request_lock: Lock = Lock()
        self._connect_timeout: float | None = connect_timeout
        self._read_timeout: float | None = read_timeout
        self._socket_keepalive: bool | None = socket_keepalive
        self._socket_keepalive_options: dict[int, int | bytes] = socket_keepalive_options or {}
        self._max_inflight_requests = max_inflight_requests or 1024
        self._last_error: Exception | None = None
        self.metrics: ConnectionMetrics = ConnectionMetrics()
        self._write_ready.clear()

    @abstractmethod
    async def connect(self) -> None: ...

    def happy(self) -> bool:
        if not self._transport:
            return False
        return (
            len(self._request_queue) < self._max_inflight_requests
            # and max(0.0, self.metrics.last_written - self.metrics.last_request_processed) < 1
            # and self._buffer.tell() == len(self._buffer.getbuffer())
        )

    def write(self, data: bytes) -> None:
        assert self._transport
        self._transport.write(data)

    async def send(self, data: bytes) -> None:
        await self._write_ready.wait()
        self.write(data)
        self.metrics.last_written = time.time()

    def disconnect(self) -> None:
        if self._transport:
            try:
                self._transport.close()
            except RuntimeError:  # noqa
                pass

        while True:
            try:
                request = self._request_queue.popleft()
                if not request.future.done():
                    request.future.set_exception(
                        self._last_error or ConnectionError("Connection lost")
                    )
            except IndexError:
                break
        self._transport = None

    async def create_request(self, command: Command[R]) -> Future[R]:
        request = Request(
            weakref.proxy(self),
            command,
        )
        await self.send(command.build())
        if not command.noreply:
            self._request_queue.append(request)
            if self._read_timeout is not None:
                request.enforce_deadline(self._read_timeout)
            future = request.future
        else:
            future = asyncio.Future()
            future.set_result(None)
        return future

    def connection_made(self, transport: BaseTransport) -> None:
        self._transport = cast(Transport, transport)
        self._write_ready.set()

    def data_received(self, data: bytes) -> None:
        previous_position = self._buffer.tell()
        self.metrics.last_read = time.time()
        self._buffer.write(data)
        self._buffer.seek(previous_position)
        if not data.endswith(LINE_END):
            return
        while self._request_queue:
            request = self._request_queue.popleft()
            try:
                response = request.command.parse(self._buffer)
                if not (request.future.cancelled() or request.future.done()):
                    request.future.set_result(response)
                self._processed_queue.append(request)
            except NotEnoughData as e:
                if e.data_read:
                    self._buffer.seek(self._buffer.tell() - e.data_read)
                self._request_queue.appendleft(request)
                break
            except MemcachedError as e:
                if not (request.future.cancelled() or request.future.done()):
                    if request.raise_exceptions:
                        request.future.set_exception(e)
                    else:
                        request.future.set_result(e)

    def pause_writing(self) -> None:
        """:meta private:"""
        self._write_ready.clear()

    def resume_writing(self) -> None:
        """:meta private:"""
        self._write_ready.set()

    def connection_lost(self, exc: Exception | None) -> None:
        if exc:
            self._last_error = exc
        self.disconnect()


class TCPConnection(BaseConnection):
    def __init__(
        self,
        host_port: TCPLocator,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
    ) -> None:
        self._host, self._port = host_port
        super().__init__(socket_keepalive, socket_keepalive_options, connect_timeout, read_timeout)

    async def connect(self) -> None:
        async with self._transport_lock:
            if self._transport:
                return
            try:
                async with asyncio.timeout(self._connect_timeout):
                    transport, _ = await get_running_loop().create_connection(
                        lambda: self,
                        host=self._host,
                        port=self._port,
                    )
            except TimeoutError:
                msg = f"Unable to establish a connection within {self._connect_timeout} seconds"
                raise ConnectionError(
                    msg,
                )
            if (sock := transport.get_extra_info("socket")) is not None:
                try:
                    # TCP_KEEPALIVE
                    if self._socket_keepalive:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

                        for k, v in self._socket_keepalive_options.items():
                            sock.setsockopt(socket.SOL_TCP, k, v)
                except (OSError, TypeError):
                    # `socket_keepalive_options` might contain invalid options
                    # causing an error
                    transport.close()
                    raise


class UnixSocketConnection(BaseConnection):
    def __init__(
        self,
        path: str,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
    ) -> None:
        self._path = path
        super().__init__(socket_keepalive, socket_keepalive_options, connect_timeout, read_timeout)

    async def connect(self) -> None:
        async with self._transport_lock:
            if self._transport:
                return
            try:
                async with asyncio.timeout(self._connect_timeout):
                    transport, _ = await get_running_loop().create_unix_connection(
                        lambda: self, path=self._path
                    )
            except TimeoutError:
                msg = f"Unable to establish a connection within {self._connect_timeout} seconds"
                raise ConnectionError(
                    msg,
                )
            if (sock := transport.get_extra_info("socket")) is not None:
                try:
                    # TCP_KEEPALIVE
                    if self._socket_keepalive:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

                        for k, v in self._socket_keepalive_options.items():
                            sock.setsockopt(socket.SOL_TCP, k, v)
                except (OSError, TypeError):
                    # `socket_keepalive_options` might contain invalid options
                    # causing an error
                    transport.close()
                    raise
