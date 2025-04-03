from __future__ import annotations

import asyncio
import dataclasses
import enum
import logging
import time
import weakref
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from contextlib import suppress
from typing import Any, TypeVar, Unpack, cast

from .commands import Command
from .connection import (
    BaseConnection,
    ConnectionParams,
    TCPConnection,
    UnixSocketConnection,
)
from .defaults import (
    BLOCKING_TIMEOUT,
    IDLE_CONNECTION_TIMEOUT,
    MAX_CONNECTIONS,
    MAXIMUM_RECOVERY_ATTEMPTS,
    MIN_CONNECTIONS,
    MONITOR_UNHEALTHY_ENDPOINTS,
    REMOVE_UNHEALTHY_ENDPOINTS,
)
from .errors import ConnectionNotAvailable, MemcachioConnectionError
from .routing import KeyRouter
from .types import (
    MemcachedEndpoint,
    SingleMemcachedInstanceEndpoint,
    UnixSocketEndpoint,
    normalize_endpoint,
    normalize_single_server_endpoint,
)

R = TypeVar("R")

logger = logging.getLogger(__name__)


class EndpointStatus(enum.IntEnum):
    """
    Enumeration of endpoint statuses.
    Used by :meth:`~ClusterPool.mark_endpoint`
    """

    #: Mark the endpoint as up and usable
    UP = enum.auto()
    #: Mark the endpoint as down and not in use
    DOWN = enum.auto()


@dataclasses.dataclass
class EndpointHealthcheckConfig:
    #: Whether to remove unhealthy endpoints on connection errors
    remove_unhealthy_endpoints: bool = REMOVE_UNHEALTHY_ENDPOINTS
    #: Whether to monitor unhealthy endpoints after they have been
    #: removed and attempt to restore them if they recover
    monitor_unhealthy_endpoints: bool = MONITOR_UNHEALTHY_ENDPOINTS
    #: Maximum attempts to make to recover unhealthy endpoints
    maximum_recovery_attempts: int = MAXIMUM_RECOVERY_ATTEMPTS


class Pool(ABC):
    """
    The abstract base class for a connection pool used by
    :class:`~memcachio.Client`
    """

    def __init__(
        self,
        endpoint: MemcachedEndpoint,
        min_connections: int = MIN_CONNECTIONS,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        **connection_args: Unpack[ConnectionParams],
    ):
        """
        :param endpoint: The memcached server address(es)
        :param min_connections: The minimum number of connections to keep in the pool.
        :param max_connections: The maximum number of simultaneous connections to memcached.
        :param blocking_timeout: The timeout (in seconds) to wait for a connection to become available.
        :param idle_connection_timeout: The maximum time to allow a connection to remain idle in the pool
         before being disconnected
        :param connection_args: Arguments to pass to the constructor of :class:`~memcachio.BaseConnection`.
         refer to :class:`~memcachio.connection.ConnectionParams`
        """
        self.endpoint = normalize_endpoint(endpoint)
        self._max_connections = max_connections
        self._min_connections = min_connections
        self._blocking_timeout = blocking_timeout
        self._idle_connection_timeout = idle_connection_timeout
        self._connection_parameters: ConnectionParams = connection_args

    async def execute_command(self, command: Command[R]) -> None:
        """
        Dispatches a command to memcached. To receive the response the future
        pointed to by ``command.response`` should be awaited as it will be updated
        when the response (or exception) is available on the transport.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """
        Closes the connection pool and disconnects all active connections
        """
        ...

    @abstractmethod
    async def initialize(self) -> None:
        """
        Initialize the connection pool. The method can throw
        a connection error if the target server(s) can't be connected
        to.
        """
        ...

    def __del__(self) -> None:
        with suppress(RuntimeError):
            self.close()


class SingleServerPool(Pool):
    """
    Connection pool to manage connections to a single memcached
    server.
    """

    def __init__(
        self,
        endpoint: SingleMemcachedInstanceEndpoint,
        min_connections: int = MIN_CONNECTIONS,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        **connection_args: Unpack[ConnectionParams],
    ) -> None:
        super().__init__(
            endpoint,
            min_connections=min_connections,
            max_connections=max_connections,
            blocking_timeout=blocking_timeout,
            idle_connection_timeout=idle_connection_timeout,
            **connection_args,
        )
        self._server_endpoint = endpoint
        self._connections: asyncio.Queue[BaseConnection | None] = asyncio.LifoQueue(
            self._max_connections
        )
        self._pool_lock: asyncio.Lock = asyncio.Lock()
        self._connection_class: type[TCPConnection | UnixSocketConnection]
        self._initialized = False
        self._connection_parameters.setdefault("on_connect_callbacks", []).append(
            self.__on_connection_created
        )
        self._connection_parameters.setdefault("on_disconnect_callbacks", []).append(
            self.__on_connection_disconnected
        )
        self._active_connections: weakref.WeakSet[BaseConnection] = weakref.WeakSet()
        while True:
            try:
                self._connections.put_nowait(None)
            except asyncio.QueueFull:
                break

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._pool_lock:
            if self._initialized:
                return
            for _ in range(self._min_connections):
                connection = self._connections.get_nowait()
                try:
                    if not connection:
                        self._connections.put_nowait(await self._create_connection())
                except ConnectionError:
                    self._connections.put_nowait(None)
                    raise
            self._initialized = True

    async def execute_command(self, command: Command[R]) -> None:
        connection, release = None, None
        try:
            connection, release = await self.acquire(command)
            await connection.connect()
            connection.create_request(command)
            await command.request_sent
            if release:
                if command.noreply:
                    self._connections.put_nowait(connection)
                else:
                    command.response.add_done_callback(
                        lambda _: self._connections.put_nowait(connection)
                    )
        except MemcachioConnectionError:
            if release:
                self._connections.put_nowait(None)
            raise

    async def acquire(self, command: Command[Any]) -> tuple[BaseConnection, bool]:
        await self.initialize()
        released = False
        try:
            async with asyncio.timeout(self._blocking_timeout):
                connection = await self._connections.get()
                try:
                    if connection and connection.reusable:
                        self._connections.put_nowait(connection)
                        released = True
                    else:
                        if not connection:
                            connection = await self._create_connection()
                except ConnectionError:
                    self._connections.put_nowait(None)
                    raise
            return connection, not released
        except TimeoutError:
            raise ConnectionNotAvailable(self._server_endpoint, self._blocking_timeout)

    def close(self) -> None:
        while True:
            try:
                if connection := self._connections.get_nowait():
                    connection.close()
            except asyncio.QueueEmpty:
                break
        while True:
            try:
                self._connections.put_nowait(None)
            except asyncio.QueueFull:
                break
        self._initialized = False

    async def _create_connection(self) -> BaseConnection:
        connection: BaseConnection
        if isinstance(self._server_endpoint, UnixSocketEndpoint):
            connection = UnixSocketConnection(self._server_endpoint, **self._connection_parameters)
        else:
            connection = TCPConnection(self._server_endpoint, **self._connection_parameters)
        if not connection.connected:
            await connection.connect()
        return connection

    def __check_connection_idle(self, connection: BaseConnection) -> None:
        if (
            time.time() - connection.metrics.last_read > self._idle_connection_timeout
            and connection.metrics.requests_pending == 0
            and len(self._active_connections) > self._min_connections
        ):
            connection.close()
            self._active_connections.discard(connection)
        elif connection.connected:
            asyncio.get_running_loop().call_later(
                self._idle_connection_timeout, self.__check_connection_idle, connection
            )

    def __on_connection_created(self, connection: BaseConnection) -> None:
        self._active_connections.add(connection)
        if self._idle_connection_timeout:
            asyncio.get_running_loop().call_later(
                self._idle_connection_timeout, self.__check_connection_idle, connection
            )

    def __on_connection_disconnected(self, connection: BaseConnection) -> None:
        self._active_connections.discard(connection)


class ClusterPool(Pool):
    """
    Connection pool to manage connections to multiple memcached
    servers.

    For multi-key commands, rendezvous hashing is used to distribute the command
    to the appropriate endpoints.
    """

    def __init__(
        self,
        endpoint: Sequence[SingleMemcachedInstanceEndpoint],
        min_connections: int = MIN_CONNECTIONS,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        hashing_function: Callable[[str], int] | None = None,
        endpoint_healthcheck_config: EndpointHealthcheckConfig | None = None,
        **connection_args: Unpack[ConnectionParams],
    ) -> None:
        """
        :param endpoint: The memcached server address(es)
        :param min_connections: The minimum number of connections per endpoint to keep in the pool.
        :param max_connections: The maximum number of simultaneous connections per  memcached endpoint.
        :param blocking_timeout: The timeout (in seconds) to wait for a connection to become available.
        :param idle_connection_timeout: The maximum time to allow a connection to remain idle in the pool
         before being disconnected
        :param hashing_function: A function to use for routing keys to
         endpoints for multi-key commands. If none is provided the default
         :func:`hashlib.md5` implementation from the standard library is used.
        :param endpoint_healthcheck_config: Endpoint healthcheck configuration to control whether
         endpoints are automatically removed/recovered based on healthchecks.
        :param connection_args: Arguments to pass to the constructor of :class:`~memcachio.BaseConnection`.
         refer to :class:`~memcachio.connection.ConnectionParams`
        """
        self._cluster_pools: dict[SingleMemcachedInstanceEndpoint, SingleServerPool] = {}
        self._pool_lock = asyncio.Lock()
        self._initialized = False
        super().__init__(
            endpoint,
            min_connections=min_connections,
            max_connections=max_connections,
            blocking_timeout=blocking_timeout,
            idle_connection_timeout=idle_connection_timeout,
            **connection_args,
        )
        self._all_endpoints: set[SingleMemcachedInstanceEndpoint] = {
            normalize_single_server_endpoint(endpoint)
            for endpoint in cast(Iterable[SingleMemcachedInstanceEndpoint], self.endpoint)
        }
        self._unhealthy_endpoints: set[SingleMemcachedInstanceEndpoint] = set()
        self._router = KeyRouter(self._all_endpoints, hasher=hashing_function)
        self._health_check_locks: dict[SingleMemcachedInstanceEndpoint, asyncio.Lock] = defaultdict(
            asyncio.Lock
        )
        self.__endpoint_healthcheck_config: EndpointHealthcheckConfig = (
            endpoint_healthcheck_config or EndpointHealthcheckConfig()
        )

    @property
    def endpoints(self) -> set[SingleMemcachedInstanceEndpoint]:
        return self._all_endpoints - self._unhealthy_endpoints

    def add_endpoint(self, endpoint: SingleMemcachedInstanceEndpoint) -> None:
        """
        Add a new endpoint to this pool
        """
        normalized_endpoint = normalize_single_server_endpoint(endpoint)
        self._all_endpoints.add(normalized_endpoint)
        self._router.add_node(normalized_endpoint)
        if normalized_endpoint not in self._cluster_pools:
            self._cluster_pools[normalized_endpoint] = SingleServerPool(
                normalized_endpoint,
                min_connections=self._min_connections,
                max_connections=self._max_connections,
                blocking_timeout=self._blocking_timeout,
                idle_connection_timeout=self._idle_connection_timeout,
                **self._connection_parameters,
            )

    def remove_endpoint(self, endpoint: SingleMemcachedInstanceEndpoint) -> None:
        """
        Remove a endpoint from this pool. This will immediately also close
        all connections to that endpoint.
        """
        normalized_endpoint = normalize_single_server_endpoint(endpoint)
        self._all_endpoints.discard(normalized_endpoint)
        self._router.remove_node(normalized_endpoint)
        if pool := self._cluster_pools.pop(normalized_endpoint, None):
            pool.close()

    def mark_endpoint(
        self, endpoint: SingleMemcachedInstanceEndpoint, status: EndpointStatus
    ) -> None:
        """
        Change the status of a endpoint in this pool.
        Marking a endpoint as :enum:`EndpointStatus.DOWN` will immediately stop routing
        requests to it, while marking it as :enum:`EndpointStatus.UP` will immediately
        start routing requests to it.
        """
        normalized_endpoint = normalize_single_server_endpoint(endpoint)
        match status:
            case EndpointStatus.UP:
                self._unhealthy_endpoints.discard(normalized_endpoint)
                self._router.add_node(normalized_endpoint)
            case EndpointStatus.DOWN:
                self._unhealthy_endpoints.add(normalized_endpoint)
                self._router.remove_node(normalized_endpoint)

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._pool_lock:
            if self._initialized:
                return
            for endpoint in self.endpoints:
                self.add_endpoint(endpoint)
            await asyncio.gather(
                *[self._cluster_pools[endpoint].initialize() for endpoint in self.endpoints]
            )
            self._initialized = True

    async def execute_command(self, command: Command[R]) -> None:
        """
        Dispatches a command to the appropriate memcached endpoint(s).
        To receive the response the future pointed to by ``command.response`` should be awaited
        as it will be updated when the response(s) (or exception) are available on the transport
        and merged (if it is a command that spans multiple endpoints).
        """
        try:
            await self.initialize()
            if command.keys and len(command.keys) == 1:
                await self._cluster_pools[self._router.get_node(command.keys[0])].execute_command(
                    command
                )
            else:
                mapping = defaultdict(list)
                if command.keys:
                    for key in command.keys:
                        mapping[self._router.get_node(key)].append(key)
                    endpoint_commands = {
                        endpoint: command.clone(keys) for endpoint, keys in mapping.items()
                    }
                else:
                    endpoint_commands = {
                        endpoint: command.clone(command.keys) for endpoint in self.endpoints
                    }
                await asyncio.gather(
                    *[
                        self._cluster_pools[endpoint].execute_command(endpoint_command)
                        for endpoint, endpoint_command in endpoint_commands.items()
                    ]
                )
                if not command.noreply:
                    command.response.set_result(
                        command.merge(
                            await asyncio.gather(
                                *[command.response for command in endpoint_commands.values()]
                            )
                        )
                    )
        except MemcachioConnectionError as e:
            if self.__endpoint_healthcheck_config.remove_unhealthy_endpoints:
                asyncio.get_running_loop().call_soon(
                    asyncio.create_task, self.__check_endpoint_health(e.endpoint)
                )
            raise

    async def __check_endpoint_health(
        self, endpoint: SingleMemcachedInstanceEndpoint, attempt: int = 0
    ) -> None:
        if (
            self._health_check_locks[endpoint].locked()
            or endpoint not in self._cluster_pools
            or attempt > self.__endpoint_healthcheck_config.maximum_recovery_attempts
        ):
            return

        async with self._health_check_locks[endpoint]:
            try:
                if pool := self._cluster_pools.get(endpoint, None):
                    await pool.initialize()
                    if any(pool._active_connections):
                        if self.__endpoint_healthcheck_config.monitor_unhealthy_endpoints:
                            logger.info(
                                f"Memcached server at {endpoint} has recovered after {2**attempt} seconds"
                            )
                            self.mark_endpoint(endpoint, EndpointStatus.UP)
                        return
                    else:
                        pool.close()
            except MemcachioConnectionError:
                self.mark_endpoint(endpoint, EndpointStatus.DOWN)
                if (
                    not self.__endpoint_healthcheck_config.monitor_unhealthy_endpoints
                    or attempt == self.__endpoint_healthcheck_config.maximum_recovery_attempts
                ):
                    logger.error(f"Memcached server at {endpoint} unreachable and marked down")
                    return
            except Exception:
                logger.exception("Unknown error while checking endpoint health")
                return

        if (
            endpoint in self._unhealthy_endpoints
            and self.__endpoint_healthcheck_config.monitor_unhealthy_endpoints
            and attempt < self.__endpoint_healthcheck_config.maximum_recovery_attempts
        ):
            delay = 2**attempt
            logger.debug(
                f"Memcached server at {endpoint} still down, attempting recovery attempt in {delay} seconds"
            )
            asyncio.get_running_loop().call_later(
                delay, asyncio.create_task, self.__check_endpoint_health(endpoint, attempt + 1)
            )

    def close(self) -> None:
        for pool in self._cluster_pools.values():
            pool.close()
        self._unhealthy_endpoints.clear()
        self._cluster_pools.clear()
        self._initialized = False
