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
from typing import Any, Generic, TypedDict, TypeVar, cast

from .authentication import Authenticator, SimpleAuthenticator
from .commands import Command
from .compat import NotRequired, Unpack, asyncio_timeout
from .defaults import (
    CONNECT_TIMEOUT,
    MAX_AVERAGE_RESPONSE_TIME_FOR_CONNECTION_REUSE,
    MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
    READ_TIMEOUT,
)
from .errors import MemcachedError, MemcachioConnectionError, NotEnoughData
from .types import SingleMemcachedInstanceEndpoint, TCPEndpoint, UnixSocketEndpoint
from .utils import decodedstr

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
    #: Additional options to set if ``socket_keepalive`` is ``True``
    socket_keepalive_options: NotRequired[dict[int, int | bytes] | None]
    #: Maximum number of concurrent requests to allow on the connection
    max_inflight_requests_per_connection: NotRequired[int]
    #:  Threshold for allowing the connection to be reused when there are requests pending.
    max_average_response_time_for_connection_reuse: NotRequired[float]
    #: SSL context to use if connecting to a memcached instance listening on a secure port
    ssl_context: NotRequired[SSLContext | None]
    #: Authentication strategy to use after establishing a connection
    authenticator: NotRequired[Authenticator | None]
    #: Username for SASL authentication
    username: NotRequired[str | None]
    #: Password for SASL authentication
    password: NotRequired[str | None]
    #: List of callables that will be called with the connection as the first argument
    #  when the connection is successfully established.
    on_connect_callbacks: NotRequired[list[Callable[[BaseConnection], None]]]
    #: List of callables that will be called with the connection as the first argument
    #  when the connection is disconnected from the server
    on_disconnect_callbacks: NotRequired[list[Callable[[BaseConnection], None]]]


@dataclasses.dataclass
class Request(Generic[R]):
    connection: weakref.ReferenceType[BaseConnection]
    command: Command[R]
    decode: bool = False
    encoding: str | None = None
    raise_exceptions: bool = True
    created_at: float = dataclasses.field(default_factory=lambda: time.time())
    timeout_handler: asyncio.Handle | None = None

    def __post_init__(self) -> None:
        if connection := self.connection():
            connection.metrics.requests_pending += 1
        self.command.response.add_done_callback(self.__update_metrics)
        self.command.response.add_done_callback(self.__cancel_timer)

    def __cancel_timer(self, future: Future[R]) -> None:
        if (
            self.timeout_handler
            and future.done()
            and not (future.cancelled() or future.exception())
        ):
            self.timeout_handler.cancel()

    def __update_metrics(self, future: Future[R]) -> None:
        if connection := self.connection():
            metrics = connection.metrics
            metrics.last_request_processed = time.time()
            metrics.requests_pending -= 1
            if future.done() and not future.cancelled():
                if not future.exception():
                    metrics.requests_processed += 1
                    metrics.average_response_time = (
                        (time.time() - self.created_at)
                        + metrics.average_response_time * (metrics.requests_processed - 1)
                    ) / metrics.requests_processed
                else:
                    metrics.requests_failed += 1


@dataclasses.dataclass
class ConnectionMetrics:
    """
    Tracks metrics for a connection.
    """

    #: Timestamp when the connection was established.
    created_at: float | None = None
    #: Total number of successfully processed requests.
    requests_processed: int = 0
    #: Total number of requests that failed.
    requests_failed: int = 0
    #: Timestamp when data was last written.
    last_written: float = 0.0
    #: Timestamp when data was last read.
    last_read: float = 0.0
    #: Timestamp when the last request completed processing.
    last_request_processed: float = 0.0
    #: Average time taken to process requests.
    average_response_time: float = 0.0
    #: Number of requests currently pending.
    requests_pending: int = 0


class BaseConnection(BaseProtocol, ABC):
    """Wraps an asyncio connection using a custom protocol.
    Provides methods for sending commands and reading lines.
    """

    endpoint: SingleMemcachedInstanceEndpoint
    #: Metrics related to this connection
    metrics: ConnectionMetrics

    def __init__(
        self,
        *,
        connect_timeout: float | None = CONNECT_TIMEOUT,
        read_timeout: float | None = READ_TIMEOUT,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        socket_nodelay: bool | None = False,
        max_inflight_requests_per_connection: int = MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
        max_average_response_time_for_connection_reuse: float = MAX_AVERAGE_RESPONSE_TIME_FOR_CONNECTION_REUSE,
        ssl_context: SSLContext | None = None,
        authenticator: Authenticator | None = None,
        username: str | None = None,
        password: str | None = None,
        on_connect_callbacks: list[Callable[[BaseConnection], None]] | None = None,
        on_disconnect_callbacks: list[Callable[[BaseConnection], None]] | None = None,
    ) -> None:
        """
        :param connect_timeout: Timeout for establishing a connection.
        :param read_timeout: Timeout for reading data from the connection.
        :param socket_keepalive: Whether to enable SO_KEEPALIVE on the socket.
        :param socket_keepalive_options: Additional keepalive options.
        :param socket_nodelay: Whether to enable TCP_NODELAY on the socket.
        :param max_inflight_requests_per_connection: Maximum concurrent requests allowed.
        :param max_average_response_time_for_connection_reuse: Threshold for allowing the connection to be
         reused when there are requests pending.
        :param ssl_context: SSL context for secure connections.
        :param authenticator: Authentication strategy to use after establishing a connection
        :param username: Username for SASL authentication.
        :param password: Password for SASL authentication.
        :param on_connect_callbacks: Callbacks to invoke upon successful connection.
        :param on_disconnect_callbacks: Callbacks to invoke upon disconnection.
        """
        self._connect_timeout: float | None = connect_timeout
        self._read_timeout: float | None = read_timeout
        self._socket_nodelay: bool | None = socket_nodelay
        self._socket_keepalive: bool | None = socket_keepalive
        self._socket_keepalive_options: dict[int, int | bytes] = socket_keepalive_options or {}
        self._max_inflight_requests_per_connection = max_inflight_requests_per_connection
        self._max_average_response_time_for_connection_reuse = (
            max_average_response_time_for_connection_reuse
        )
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
        self.metrics: ConnectionMetrics = ConnectionMetrics()
        self._authenticator = authenticator or (
            SimpleAuthenticator(username, password) if (username and password) else None
        )

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish a connection to the target memcached server
        """
        ...

    @property
    def connected(self) -> bool:
        """
        Whether the connection is currently connected
        """
        return self._transport is not None and self._write_ready.is_set()

    @property
    def reusable(self) -> bool:
        """
        Whether this connection is healthy enough to handle
        any more concurrent requests
        """
        return (
            len(self._request_queue) < self._max_inflight_requests_per_connection
            and self.metrics.average_response_time
            < self._max_average_response_time_for_connection_reuse
        )

    def send(self, data: bytes) -> None:
        assert self._transport
        self._transport.write(data)
        self.metrics.last_written = time.time()

    def close(self) -> None:
        """
        Disconnect from the memcached server and clear internal
        state.
        """
        self.__on_disconnect()

    def create_request(self, command: Command[R]) -> None:
        """
        Send a request to the memcached server and queue the response
        handling if this is not a ``noreply`` request.
        """
        self.send(bytes(command.build_request()))
        command.request_sent.set_result(True)
        if not command.noreply:
            request = Request(
                weakref.ref(self),
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
        if self.metrics.created_at:
            self.metrics = ConnectionMetrics()

        self.metrics.created_at = time.time()
        self._transport = cast(Transport, transport)
        if (sock := self._transport.get_extra_info("socket")) is not None:
            try:
                if self._socket_nodelay is not None and sock.family in (
                    socket.AF_INET,
                    socket.AF_INET6,
                ):
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, self._socket_nodelay)

                if self._socket_keepalive is not None:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, self._socket_keepalive)
                    for k, v in self._socket_keepalive_options.items():
                        sock.setsockopt(socket.IPPROTO_TCP, k, v)
            except (OSError, TypeError) as err:
                transport.close()
                self._last_error = err
                return
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
                response = request.command.parse(self._buffer, self.endpoint)
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
        if self._authenticator:
            await self._authenticator.authenticate(self)

    def __on_disconnect(self, from_server: bool = False, reason: str | None = None) -> None:
        self._write_ready.clear()
        if self._transport:
            try:
                self._transport.close()
            except RuntimeError:
                pass
            self._transport = None

        while True:
            try:
                request = self._request_queue.popleft()
                if not request.command.response.done():
                    exc = MemcachioConnectionError(reason or "", self.endpoint)
                    if self._last_error:
                        exc.__cause__ = self._last_error
                    request.command.response.set_exception(exc)
            except IndexError:
                break

        self._buffer = BytesIO()

        if from_server:
            with suppress(RuntimeError):
                [cb(self) for cb in self._disconnect_callbacks]


class TCPConnection(BaseConnection):
    def __init__(
        self,
        host_port: TCPEndpoint | tuple[str, int],
        **kwargs: Unpack[ConnectionParams],
    ) -> None:
        self._host, self._port = host_port
        self.endpoint = host_port
        super().__init__(**kwargs)

    async def connect(self) -> None:
        """
        Establish a connection to the target memcached server listening on
        a tcp port
        """
        if not self._transport:
            async with self._transport_lock:
                if not self._transport:
                    try:
                        async with asyncio_timeout(self._connect_timeout):
                            transport, _ = await get_running_loop().create_connection(
                                lambda: self,
                                host=self._host,
                                port=self._port,
                                ssl=self._ssl_context,
                            )
                            await self._write_ready.wait()
                            await self._authenticate()
                    except (OSError, asyncio.TimeoutError) as e:
                        msg = f"Unable to establish a connection within {self._connect_timeout} seconds"
                        raise MemcachioConnectionError(msg, self.endpoint) from (
                            self._last_error or e
                        )


class UnixSocketConnection(BaseConnection):
    def __init__(
        self,
        path: UnixSocketEndpoint,
        **kwargs: Unpack[ConnectionParams],
    ) -> None:
        self.endpoint = self._path = str(Path(path).expanduser().absolute())
        super().__init__(**kwargs)

    async def connect(self) -> None:
        """
        Establish a connection to the target memcached server listening on
        a unix domain socket
        """
        if not self._transport:
            async with self._transport_lock:
                if not self._transport:
                    try:
                        async with asyncio_timeout(self._connect_timeout):
                            transport, _ = await get_running_loop().create_unix_connection(
                                lambda: self, path=self._path
                            )
                    except (OSError, asyncio.TimeoutError) as e:
                        msg = f"Unable to establish a connection within {self._connect_timeout} seconds"
                        raise MemcachioConnectionError(msg, self.endpoint) from e
                    await self._write_ready.wait()
                    await self._authenticate()
