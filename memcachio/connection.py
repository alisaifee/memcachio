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
from collections.abc import Callable
from contextlib import suppress
from io import BytesIO
from pathlib import Path
from ssl import SSLContext
from typing import Any, Generic, NotRequired, TypedDict, TypeVar, Unpack, cast

from .commands import Command, SetCommand
from .defaults import CONNECT_TIMEOUT, MAX_INFLIGHT_REQUESTS_PER_CONNECTION, READ_TIMEOUT
from .errors import MemcachedError, MemcachioConnectionError, NotEnoughData
from .types import SingleMemcachedInstanceLocator, TCPLocator, UnixSocketLocator
from .utils import bytestr, decodedstr

R = TypeVar("R")


class ConnectionParams(TypedDict):
    #: Maximum time to wait when establishing a connection
    connect_timeout: float | None
    #: Maximum time to wait for a response
    read_timeout: float | None
    #: Whether to set the :data:`socket.TCP_NODELAY` flag on the socket
    socket_nodelay: NotRequired[bool | None]
    #: Whether to set the :data:`socket.SO_KEEPALIVE` flag on the socket
    socket_keepalive: NotRequired[bool | None]
    socket_keepalive_options: NotRequired[dict[int, int | bytes] | None]
    max_inflight_requests_per_connection: NotRequired[int]
    ssl_context: NotRequired[SSLContext | None]
    on_connect_callbacks: NotRequired[list[Callable[[BaseConnection], None]]]
    on_disconnect_callbacks: NotRequired[list[Callable[[BaseConnection], None]]]
    username: NotRequired[str | None]
    password: NotRequired[str | None]


@dataclasses.dataclass
class Request(Generic[R]):
    connection: weakref.ProxyType[BaseConnection]
    command: Command[R]
    decode: bool = False
    encoding: str | None = None
    raise_exceptions: bool = True
    created_at: float = dataclasses.field(default_factory=lambda: time.time())
    timeout_handler: asyncio.Handle | None = None

    def __post_init__(self) -> None:
        self.connection.metrics.requests_pending += 1
        self.command.response.add_done_callback(self.cleanup)

    def cleanup(self, future: Future) -> None:  # type: ignore[type-arg]
        metrics = self.connection.metrics
        metrics.last_request_processed = time.time()
        metrics.requests_pending -= 1
        if future.done() and not future.cancelled():
            if not self.command.response.exception():
                metrics.requests_processed += 1
                metrics.average_response_time = (
                    (time.time() - self.created_at)
                    + metrics.average_response_time * (metrics.requests_processed - 1)
                ) / metrics.requests_processed
                if self.timeout_handler:
                    self.timeout_handler.cancel()
            else:
                metrics.requests_failed += 1


@dataclasses.dataclass
class ConnectionMetrics:
    created_at: float | None = None
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

    locator: SingleMemcachedInstanceLocator
    metrics: ConnectionMetrics

    def __init__(
        self,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        socket_nodelay: bool | None = False,
        connect_timeout: float | None = CONNECT_TIMEOUT,
        read_timeout: float | None = READ_TIMEOUT,
        max_inflight_requests_per_connection: int = MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
        ssl_context: SSLContext | None = None,
        on_connect_callbacks: list[Callable[[BaseConnection], None]] | None = None,
        on_disconnect_callbacks: list[Callable[[BaseConnection], None]] | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        self._connect_timeout: float | None = connect_timeout
        self._read_timeout: float | None = read_timeout
        self._socket_nodelay: bool | None = socket_nodelay
        self._socket_keepalive: bool | None = socket_keepalive
        self._socket_keepalive_options: dict[int, int | bytes] = socket_keepalive_options or {}
        self._max_inflight_requests_per_connection = max_inflight_requests_per_connection
        self._ssl_context: SSLContext | None = ssl_context
        self._last_error: Exception | None = None
        self._transport: Transport | None = None
        self._buffer = BytesIO()
        self._request_queue: deque[Request[Any]] = deque()
        self._write_ready: Event = Event()
        self._transport_lock: Lock = Lock()
        self._request_lock: Lock = Lock()
        self._connect_callbacks = [weakref.proxy(cb) for cb in on_connect_callbacks or []]
        self._disconnect_callbacks = [weakref.proxy(cb) for cb in on_disconnect_callbacks or []]
        self._auth = (bytestr(username), bytestr(password)) if username and password else None
        self.metrics: ConnectionMetrics = ConnectionMetrics()

    @abstractmethod
    async def connect(self) -> None: ...

    @property
    def connected(self) -> bool:
        return self._transport is not None and self._write_ready.is_set()

    def reusable(self) -> bool:
        return (
            self.connected
            and len(self._request_queue) < self._max_inflight_requests_per_connection
            and self.metrics.average_response_time < 0.01
        )

    def send(self, data: bytes) -> None:
        assert self._transport
        self._transport.write(data)
        self.metrics.last_written = time.time()

    def disconnect(self) -> None:
        self.__on_disconnect()

    def create_request(self, command: Command[R]) -> None:
        self.send(bytes(command.build_request()))
        command.request_sent.set_result(True)
        if not command.noreply:
            request = Request(
                weakref.proxy(self),
                command,
            )
            self._request_queue.append(request)
            if self._read_timeout is not None:
                request.timeout_handler = asyncio.get_running_loop().call_later(
                    self._read_timeout,
                    lambda command: command.response.set_exception(
                        TimeoutError(
                            f"command {decodedstr(command.name)} timed out after {self._read_timeout} seconds"
                        )
                    )
                    if not command.response.done()
                    else None,
                    command,
                )

    def connection_made(self, transport: BaseTransport) -> None:
        """
        :meta private:
        """
        self.metrics.created_at = time.time()
        self._transport = cast(Transport, transport)
        if (sock := self._transport.get_extra_info("socket")) is not None:
            try:
                if self._socket_nodelay:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                if self._socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self._socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
            except (OSError, TypeError):
                transport.close()
                raise
        with suppress(RuntimeError):
            [cb(self) for cb in self._connect_callbacks]
        self._write_ready.set()

    def data_received(self, data: bytes) -> None:
        """:meta private:"""
        self.metrics.last_read = time.time()
        self._buffer = BytesIO(self._buffer.read() + data)
        while self._request_queue:
            request = self._request_queue.popleft()
            try:
                response = request.command.parse(self._buffer)
                if not (request.command.response.cancelled() or request.command.response.done()):
                    request.command.response.set_result(response)
            except NotEnoughData as e:
                self._buffer.seek(self._buffer.tell() - e.data_read)
                self._request_queue.appendleft(request)
                break
            except MemcachedError as e:
                if not (request.command.response.cancelled() or request.command.response.done()):
                    request.command.response.set_exception(e)
            except Exception as e:
                self._request_queue.appendleft(request)
                self._last_error = e
                break

    def pause_writing(self) -> None:
        """:meta private:"""
        self._write_ready.clear()

    def resume_writing(self) -> None:
        """:meta private:"""
        self._write_ready.set()

    def connection_lost(self, exc: Exception | None) -> None:
        """
        :meta private:
        """
        if exc:
            self._last_error = exc
        self.__on_disconnect(True, "Connection lost")

    def eof_received(self) -> None:
        """:meta private:"""
        self.__on_disconnect(True, "EOF received")

    async def _authenticate(self) -> None:
        if self._auth:
            auth_command = SetCommand("auth", b"%b %b" % self._auth)
            self.create_request(auth_command)
            await auth_command.response

    def __on_disconnect(self, from_server: bool = False, reason: str | None = None) -> None:
        self._write_ready.clear()
        if not self._transport:
            return
        else:
            try:
                self._transport.close()
            except RuntimeError:
                pass

        while True:
            try:
                request = self._request_queue.popleft()
                if not request.command.response.done():
                    exc = MemcachioConnectionError(reason or "", self.locator)
                    if self._last_error:
                        exc.__cause__ = self._last_error
                    request.command.response.set_exception(exc)
            except IndexError:
                break
        self._buffer = BytesIO()
        self._transport = None
        if from_server:
            with suppress(RuntimeError):
                [cb(self) for cb in self._disconnect_callbacks]


class TCPConnection(BaseConnection):
    def __init__(
        self,
        host_port: TCPLocator | tuple[str, int],
        **kwargs: Unpack[ConnectionParams],
    ) -> None:
        self._host, self._port = host_port
        self.locator = host_port
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
            except (OSError, TimeoutError) as e:
                msg = f"Unable to establish a connection within {self._connect_timeout} seconds"
                raise MemcachioConnectionError(msg, self.locator) from e
        await self._write_ready.wait()
        await self._authenticate()


class UnixSocketConnection(BaseConnection):
    def __init__(
        self,
        path: UnixSocketLocator,
        **kwargs: Unpack[ConnectionParams],
    ) -> None:
        self.locator = self._path = str(Path(path).expanduser().absolute())
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
            except (OSError, TimeoutError) as e:
                msg = f"Unable to establish a connection within {self._connect_timeout} seconds"
                raise MemcachioConnectionError(msg, self.locator) from e
        await self._write_ready.wait()
        await self._authenticate()
