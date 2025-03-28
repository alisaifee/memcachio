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
from pathlib import Path
from ssl import SSLContext
from typing import NotRequired, TypedDict, TypeVar, Unpack, cast

from memcachio.commands import Command
from memcachio.errors import MemcachedError, NotEnoughData
from memcachio.types import TCPLocator, UnixSocketLocator
from memcachio.utils import decodedstr

R = TypeVar("R")


class ConnectionParams(TypedDict):
    connect_timeout: float | None
    read_timeout: float | None
    socket_keepalive: NotRequired[bool | None]
    socket_keepalive_options: NotRequired[dict[int, int | bytes] | None]
    max_inflight_requests_per_connection: NotRequired[int | None]
    ssl_context: NotRequired[SSLContext | None]


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
        self.connection.metrics.requests_pending += 1
        self.future.add_done_callback(self.cleanup)

    def cleanup(self, future: Future) -> None:  # type: ignore[type-arg]
        metrics = self.connection.metrics
        metrics.last_request_processed = time.time()
        metrics.requests_pending -= 1
        if future.done() and not future.cancelled():
            if not self.future.exception():
                metrics.requests_processed += 1
                metrics.average_response_time = (
                    (time.time() - self.created_at)
                    + metrics.average_response_time * (metrics.requests_processed - 1)
                ) / metrics.requests_processed
            else:
                metrics.requests_failed += 1

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
    last_written: float = 0.0
    last_read: float = 0.0
    last_request_processed: float = 0.0
    average_response_time: float = 0.0
    requests_pending: int = 0


class BaseConnection(BaseProtocol, ABC):
    """Wraps an asyncio connection using a custom protocol.
    Provides methods for sending commands and reading lines.
    """

    def __init__(
        self,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        max_inflight_requests_per_connection: int | None = None,
        ssl_context: SSLContext | None = None,
    ) -> None:
        self._connect_timeout: float | None = connect_timeout
        self._read_timeout: float | None = read_timeout
        self._socket_keepalive: bool | None = socket_keepalive
        self._socket_keepalive_options: dict[int, int | bytes] = socket_keepalive_options or {}
        self._max_inflight_requests_per_connection = max_inflight_requests_per_connection or 10
        self._ssl_context: SSLContext | None = ssl_context
        self._last_error: Exception | None = None
        self._transport: Transport | None = None
        self._buffer = BytesIO()
        self._request_queue: deque[Request] = deque()
        self._write_ready: Event = Event()
        self._transport_lock: Lock = Lock()
        self._request_lock: Lock = Lock()
        self.metrics: ConnectionMetrics = ConnectionMetrics()

    @abstractmethod
    async def connect(self) -> None: ...

    @property
    def connected(self) -> bool:
        return self._transport is not None and self._write_ready.is_set()

    def reusable(self) -> bool:
        if not self.connected:
            return False
        return (
            len(self._request_queue) < self._max_inflight_requests_per_connection
            and self.metrics.average_response_time < 0.01
        )

    async def send(self, data: bytes) -> None:
        assert self._transport
        await self._write_ready.wait()
        self._transport.write(data)
        self.metrics.last_written = time.time()

    def disconnect(self) -> None:
        self._write_ready.clear()
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
        self.metrics = ConnectionMetrics()
        self._transport = None

    async def create_request(self, command: Command[R]) -> Future[R]:
        request = Request(
            weakref.proxy(self),
            command,
        )
        async with self._request_lock:
            if not self.connected:
                await self.connect()
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
        if (sock := self._transport.get_extra_info("socket")) is not None:
            try:
                if self._socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self._socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
            except (OSError, TypeError):
                transport.close()
                raise

        self._write_ready.set()

    def data_received(self, data: bytes) -> None:
        self.metrics.last_read = time.time()
        self._buffer = BytesIO(self._buffer.read() + data)
        while self._request_queue:
            request = self._request_queue.popleft()
            try:
                response = request.command.parse(self._buffer)
                if not (request.future.cancelled() or request.future.done()):
                    request.future.set_result(response)
            except NotEnoughData as e:
                self._buffer.seek(self._buffer.tell() - e.data_read)
                self._request_queue.appendleft(request)
                break
            except MemcachedError as e:
                if not (request.future.cancelled() or request.future.done()):
                    request.future.set_exception(e)

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

    def eof_received(self) -> None:
        self.disconnect()


class TCPConnection(BaseConnection):
    def __init__(
        self,
        host_port: TCPLocator | tuple[str, int],
        **kwargs: Unpack[ConnectionParams],
    ) -> None:
        self._host, self._port = host_port
        super().__init__(**kwargs)

    async def connect(self) -> None:
        async with self._transport_lock:
            if self._transport:
                return
            try:
                async with asyncio.timeout(self._connect_timeout):
                    transport, _ = await get_running_loop().create_connection(
                        lambda: self, host=self._host, port=self._port, ssl=self._ssl_context
                    )
            except TimeoutError:
                msg = f"Unable to establish a connection within {self._connect_timeout} seconds"
                raise ConnectionError(
                    msg,
                )
        await self._write_ready.wait()


class UnixSocketConnection(BaseConnection):
    def __init__(
        self,
        path: UnixSocketLocator,
        **kwargs: Unpack[ConnectionParams],
    ) -> None:
        self._path = str(Path(path).expanduser().absolute())
        super().__init__(**kwargs)

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
        await self._write_ready.wait()
