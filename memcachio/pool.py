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
from contextlib import closing, suppress
from typing import Any, Literal, TypeVar, cast

from .commands import AWSAutoDiscoveryConfig, Command
from .compat import Unpack, asyncio_timeout
from .connection import (
    BaseConnection,
    ConnectionParams,
    TCPConnection,
    UnixSocketConnection,
)
from .defaults import (
    BLOCKING_TIMEOUT,
    CONNECT_TIMEOUT,
    IDLE_CONNECTION_TIMEOUT,
    MAX_CONNECTIONS,
    MAXIMUM_ERROR_COUNT_FOR_ENDPOINT_REMOVAL,
    MAXIMUM_RECOVERY_ATTEMPTS,
    MIN_CONNECTIONS,
    MONITOR_UNHEALTHY_ENDPOINTS,
    READ_TIMEOUT,
    REMOVE_UNHEALTHY_ENDPOINTS,
    RETRY_BACKOFF_POLICY,
)
from .errors import ConnectionNotAvailable, MemcachioConnectionError
from .routing import KeyRouter
from .types import (
    AWSAutoDiscoveryEndpoint,
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
    Used by :meth:`~ClusterPool.update_endpoint_status`
    """

    #: Mark the endpoint as up and usable
    UP = enum.auto()
    #: Mark the endpoint as down and not in use
    DOWN = enum.auto()


@dataclasses.dataclass
class EndpointHealthcheckConfig:
    #: Whether to remove unhealthy endpoints on connection errors
    remove_unhealthy_endpoints: bool = REMOVE_UNHEALTHY_ENDPOINTS
    #: Maximum numbers of errors to tolerate before marking an endpoint
    #: as unhealthy
    maximum_error_count_for_removal: int = MAXIMUM_ERROR_COUNT_FOR_ENDPOINT_REMOVAL
    #: Whether to monitor unhealthy endpoints after they have been
    #: removed and attempt to restore them if they recover
    monitor_unhealthy_endpoints: bool = MONITOR_UNHEALTHY_ENDPOINTS
    #: Maximum attempts to make to recover unhealthy endpoints
    maximum_recovery_attempts: int = MAXIMUM_RECOVERY_ATTEMPTS
    #: Retry backoff policy
    retry_backoff_policy: Literal["linear", "exponential"] = RETRY_BACKOFF_POLICY


@dataclasses.dataclass
class PoolMetrics:
    """
    Tracks metrics for a connection pool.
    """

    #: Timestamp when the pool was initialized.
    created_at: float | None = None
    #: Total number of successfully processed requests.
    requests_processed: int = 0
    #: Total number of requests that failed.
    requests_failed: int = 0
    #: Timestamp when the last connection was established
    last_connection_created: float = 0.0
    #: Timestamp when the last request completed processing.
    last_request_processed: float = 0.0
    #: Average time taken to process requests.
    average_response_time: float = 0.0
    #: Number of requests currently pending.
    requests_pending: int = 0
    #: Current connections
    current_connections: int = 0
    #: Maximum connections created
    maximum_connections: int = 0
    #: Total number of connection errors
    connection_errors: int = 0
    #: Current endpoint status
    status: EndpointStatus | None = None
    #: Number of times the pool was marked down
    down_count: int = 0
    #: Number of times the node was switched from down to up
    recovery_count: int = 0

    def on_connection_created(self, connection: BaseConnection) -> None:
        self.last_connection_created = time.time()
        self.current_connections += 1
        self.maximum_connections = max(self.maximum_connections, self.current_connections)
        if not self.status:
            self.status = EndpointStatus.UP

    def on_connection_error(self, connection: BaseConnection | None, exc: Exception) -> None:
        self.connection_errors += 1
        if not self.status:
            self.status = EndpointStatus.UP

    def on_connection_terminated(self, connection: BaseConnection) -> None:
        self.current_connections -= 1

    def on_command_dispatched(self, command: Command[Any]) -> None:
        self.requests_pending += 1
        pass

    def on_status_update(self, status: EndpointStatus) -> None:
        if self.status != status:
            match status:
                case EndpointStatus.DOWN:
                    self.down_count += 1
                case EndpointStatus.UP:
                    self.recovery_count += 1
        self.status = status

    def on_command_completed(self, command: Command[Any]) -> None:
        self.requests_pending -= 1
        self.last_request_processed = time.time()
        if not command.noreply:
            if command.response.done() and not command.response.cancelled():
                if not command.response.exception():
                    self.average_response_time = (
                        (self.requests_processed * self.average_response_time)
                        + command.response_time
                    ) / (self.requests_processed + 1)
                    self.requests_processed += 1
                else:
                    self.requests_failed += 1
        elif command.request_sent.done():
            self.requests_processed += 1

    @classmethod
    def merge(cls, metrics: Iterable[PoolMetrics]) -> PoolMetrics:
        if not metrics:
            return PoolMetrics()
        return PoolMetrics(
            created_at=min(metrics, key=lambda m: m.created_at or 0).created_at,
            requests_processed=sum([m.requests_processed for m in metrics]),
            requests_failed=sum([m.requests_failed for m in metrics]),
            requests_pending=sum([m.requests_pending for m in metrics]),
            last_connection_created=max(
                metrics, key=lambda m: m.last_connection_created
            ).last_connection_created,
            last_request_processed=max(
                metrics, key=lambda m: m.last_request_processed
            ).last_request_processed,
            average_response_time=sum(
                [m.requests_processed * m.average_response_time for m in metrics]
            )
            / (sum([m.requests_processed for m in metrics]) or 1),
            current_connections=sum([m.current_connections for m in metrics]),
            maximum_connections=sum([m.maximum_connections for m in metrics]),
            connection_errors=sum([m.connection_errors for m in metrics]),
            recovery_count=sum([m.recovery_count for m in metrics]),
            down_count=sum([m.down_count for m in metrics]),
            status=EndpointStatus.UP
            if all([m.status == EndpointStatus.UP for m in metrics])
            else EndpointStatus.DOWN,
        )


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

    @property
    @abstractmethod
    def metrics(self) -> PoolMetrics:
        """
        Pool health metrics
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

    @abstractmethod
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
        self.__server_endpoint = endpoint
        self.__connections: asyncio.Queue[BaseConnection | None] = asyncio.LifoQueue(
            self._max_connections
        )
        self.__pool_lock: asyncio.Lock = asyncio.Lock()
        self._connection_class: type[TCPConnection | UnixSocketConnection]
        self.__initialized = False
        self._connection_parameters.setdefault("on_connect_callbacks", []).append(
            self.__on_connection_created
        )
        self._connection_parameters.setdefault("on_disconnect_callbacks", []).append(
            self.__on_connection_disconnected
        )
        self._active_connections: weakref.WeakSet[BaseConnection] = weakref.WeakSet()
        self.__metrics = PoolMetrics()

        while True:
            try:
                self.__connections.put_nowait(None)
            except asyncio.QueueFull:
                break

    @property
    def metrics(self) -> PoolMetrics:
        return self.__metrics

    async def initialize(self) -> None:
        if self.__initialized:
            return
        async with self.__pool_lock:
            if self.__initialized:
                return
            if not self.metrics.created_at:
                self.metrics.created_at = time.time()
            for _ in range(self._min_connections):
                connection = self.__connections.get_nowait()
                try:
                    if not connection:
                        self.__connections.put_nowait(await self.__create_connection())
                except ConnectionError as e:
                    self.metrics.on_connection_error(connection, e)
                    self.__connections.put_nowait(None)
                    raise
            self.__initialized = True

    async def execute_command(self, command: Command[R]) -> None:
        connection, release = None, None
        try:
            connection, release = await self.__get_connection_from_pool()
            await connection.connect()
            connection.create_request(command)
            self.metrics.on_command_dispatched(command)
            await command.request_sent
            (command.request_sent if command.noreply else command.response).add_done_callback(
                lambda _: self.metrics.on_command_completed(command)
            )
            if release:
                if command.noreply:
                    self.__connections.put_nowait(connection)
                else:
                    command.response.add_done_callback(
                        lambda _: self.__connections.put_nowait(connection)
                    )
        except MemcachioConnectionError as e:
            self.metrics.on_connection_error(connection, e)
            if release:
                self.__connections.put_nowait(None)
            raise

    def close(self) -> None:
        while True:
            try:
                if connection := self.__connections.get_nowait():
                    connection.close()
            except asyncio.QueueEmpty:
                break
        while True:
            try:
                self.__connections.put_nowait(None)
            except asyncio.QueueFull:
                break
        self.__initialized = False

    async def __get_connection_from_pool(self) -> tuple[BaseConnection, bool]:
        await self.initialize()
        released = False
        try:
            async with asyncio_timeout(self._blocking_timeout):
                connection = await self.__connections.get()
                try:
                    if connection and connection.reusable:
                        self.__connections.put_nowait(connection)
                        released = True
                    else:
                        if not connection:
                            connection = await self.__create_connection()
                except ConnectionError as e:
                    self.metrics.on_connection_error(connection, e)
                    self.__connections.put_nowait(None)
                    raise
            return connection, not released
        except asyncio.TimeoutError:
            raise ConnectionNotAvailable(self.__server_endpoint, self._blocking_timeout)

    async def __create_connection(self) -> BaseConnection:
        connection: BaseConnection
        if isinstance(self.__server_endpoint, UnixSocketEndpoint):
            connection = UnixSocketConnection(self.__server_endpoint, **self._connection_parameters)
        else:
            connection = TCPConnection(self.__server_endpoint, **self._connection_parameters)
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
        self.metrics.on_connection_created(connection)
        if self._idle_connection_timeout:
            asyncio.get_running_loop().call_later(
                self._idle_connection_timeout, self.__check_connection_idle, connection
            )

    def __on_connection_disconnected(self, connection: BaseConnection) -> None:
        self.metrics.on_connection_terminated(connection)
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
        endpoint: Sequence[SingleMemcachedInstanceEndpoint] | AWSAutoDiscoveryEndpoint,
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
        :param endpoint_healthcheck_config: Configuration to control whether
         endpoints are automatically removed/recovered based on health checks.
        :param connection_args: Arguments to pass to the constructor of :class:`~memcachio.BaseConnection`.
         refer to :class:`~memcachio.connection.ConnectionParams`
        """
        self._cluster_pools: dict[SingleMemcachedInstanceEndpoint, SingleServerPool] = {}
        self.__pool_lock = asyncio.Lock()
        self.__initialized = False
        super().__init__(
            endpoint,
            min_connections=min_connections,
            max_connections=max_connections,
            blocking_timeout=blocking_timeout,
            idle_connection_timeout=idle_connection_timeout,
            **connection_args,
        )
        self.__autodiscovery_current_version: int = 0
        self.__autodiscovery_endpoint: AWSAutoDiscoveryEndpoint | None = None
        self.__all_endpoints: set[SingleMemcachedInstanceEndpoint] = set()
        if isinstance(self.endpoint, AWSAutoDiscoveryEndpoint):
            self.__autodiscovery_endpoint = self.endpoint
        else:
            self.__all_endpoints = {
                normalize_single_server_endpoint(endpoint)
                for endpoint in cast(Iterable[SingleMemcachedInstanceEndpoint], self.endpoint)
            }
        self.__unhealthy_endpoints: set[SingleMemcachedInstanceEndpoint] = set()
        self._router = KeyRouter(self.__all_endpoints, hasher=hashing_function)
        self.__healthcheck_tasks: dict[SingleMemcachedInstanceEndpoint, asyncio.Task[None]] = {}
        self.__endpoint_healthcheck_config: EndpointHealthcheckConfig = (
            endpoint_healthcheck_config or EndpointHealthcheckConfig()
        )
        self.__autodiscovery_task: asyncio.Task[None] | None = None

    @property
    def metrics(self) -> PoolMetrics:
        """
        Aggregate metrics obtained from the sub-pools for each
        endpoint that this cluster pool is configured against.
        """
        return PoolMetrics.merge(
            [
                self._cluster_pools[endpoint].metrics
                for endpoint in self.__all_endpoints
                if endpoint in self._cluster_pools
            ]
        )

    @property
    def endpoints(self) -> set[SingleMemcachedInstanceEndpoint]:
        return self.__all_endpoints - self.__unhealthy_endpoints

    async def __autodiscovery_query(self) -> None:
        if self.__autodiscovery_endpoint:
            with closing(
                TCPConnection(
                    (self.__autodiscovery_endpoint.host, self.__autodiscovery_endpoint.port),
                    connect_timeout=self._connection_parameters.get(
                        "connect_timeout", CONNECT_TIMEOUT
                    ),
                    read_timeout=self._connection_parameters.get("read_timeout", READ_TIMEOUT),
                )
            ) as connection:
                await connection.connect()
                command = AWSAutoDiscoveryConfig()
                connection.create_request(command)
                autodiscovery_version, endpoints = await command.response
                if autodiscovery_version > self.__autodiscovery_current_version:
                    new_endpoints = endpoints - self.__all_endpoints
                    discarded_endpoints = self.__all_endpoints - endpoints
                    for endpoint in new_endpoints:
                        self.add_endpoint(endpoint)
                    for endpoint in discarded_endpoints:
                        self.remove_endpoint(endpoint)
                    self.__autodiscovery_current_version = autodiscovery_version

    async def __refresh_autodiscovered_endpoints(self) -> None:
        if not self.__autodiscovery_endpoint:
            return
        while True:
            try:
                await self.__autodiscovery_query()
                await asyncio.sleep(self.__autodiscovery_endpoint.refresh_interval)
            except asyncio.CancelledError:
                break

    async def initialize(self) -> None:
        if self.__initialized:
            return
        async with self.__pool_lock:
            if self.__initialized:
                return
            if self.__autodiscovery_endpoint:
                await self.__autodiscovery_query()
                self.__autodiscovery_task = asyncio.create_task(
                    self.__refresh_autodiscovered_endpoints()
                )
            else:
                for endpoint in self.endpoints:
                    self.add_endpoint(endpoint)
            await asyncio.gather(
                *[self._cluster_pools[endpoint].initialize() for endpoint in self.endpoints]
            )
            self.__initialized = True

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
                if (
                    not (current_task := self.__healthcheck_tasks.get(e.endpoint, None))
                    or current_task.done()
                ):
                    self.__healthcheck_tasks[e.endpoint] = asyncio.create_task(
                        self.__check_endpoint_health(e.endpoint)
                    )
            raise

    async def __check_endpoint_health(self, endpoint: SingleMemcachedInstanceEndpoint) -> None:
        attempt = 0
        while True:
            try:
                try:
                    if pool := self._cluster_pools.get(endpoint, None):
                        if (
                            pool.metrics.connection_errors
                            < self.__endpoint_healthcheck_config.maximum_error_count_for_removal
                        ):
                            return
                        await pool.initialize()
                        if pool.metrics.current_connections > 0:
                            if self.__endpoint_healthcheck_config.monitor_unhealthy_endpoints:
                                logger.info(
                                    f"Memcached server at {endpoint} has recovered after {2**attempt} seconds"
                                )
                                self.update_endpoint_status(endpoint, EndpointStatus.UP)
                            break
                        else:
                            pool.close()
                except MemcachioConnectionError:
                    self.update_endpoint_status(endpoint, EndpointStatus.DOWN)
                    if (
                        not self.__endpoint_healthcheck_config.monitor_unhealthy_endpoints
                        or attempt == self.__endpoint_healthcheck_config.maximum_recovery_attempts
                    ):
                        logger.error(f"Memcached server at {endpoint} unreachable and marked down")
                        break
                except Exception:
                    logger.exception("Unknown error while checking endpoint health")
                    break

                if (
                    endpoint in self.__unhealthy_endpoints
                    and self.__endpoint_healthcheck_config.monitor_unhealthy_endpoints
                    and attempt < self.__endpoint_healthcheck_config.maximum_recovery_attempts
                ):
                    match self.__endpoint_healthcheck_config.retry_backoff_policy:
                        case "linear":
                            delay = attempt
                        case "exponential":
                            delay = 2**attempt
                    logger.debug(
                        f"Memcached server at {endpoint} still down, attempting recovery attempt in {delay} seconds"
                    )
                    attempt += 1
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break

    def close(self) -> None:
        for pool in self._cluster_pools.values():
            pool.close()
        for task in self.__healthcheck_tasks.values():
            task.cancel()
        if self.__autodiscovery_task:
            self.__autodiscovery_task.cancel()
            self.__autodiscovery_task = None
        self.__healthcheck_tasks.clear()
        self.__unhealthy_endpoints.clear()
        self.__initialized = False

    def add_endpoint(self, endpoint: SingleMemcachedInstanceEndpoint) -> None:
        """
        Add a new endpoint to this pool
        """
        normalized_endpoint = normalize_single_server_endpoint(endpoint)
        self.__all_endpoints.add(normalized_endpoint)
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
        Remove an endpoint from this pool. This will immediately also close
        all connections to that endpoint.
        """
        normalized_endpoint = normalize_single_server_endpoint(endpoint)
        self.__all_endpoints.discard(normalized_endpoint)
        self._router.remove_node(normalized_endpoint)
        if pool := self._cluster_pools.pop(normalized_endpoint, None):
            pool.close()

    def update_endpoint_status(
        self, endpoint: SingleMemcachedInstanceEndpoint, status: EndpointStatus
    ) -> None:
        """
        Change the status of an endpoint in this pool.
        Marking an endpoint as :enum:`EndpointStatus.DOWN` will immediately stop routing
        requests to it, while marking it as :enum:`EndpointStatus.UP` will immediately
        start routing requests to it.
        """
        normalized_endpoint = normalize_single_server_endpoint(endpoint)
        match status:
            case EndpointStatus.UP:
                self.__unhealthy_endpoints.discard(normalized_endpoint)
                self._router.add_node(normalized_endpoint)
            case EndpointStatus.DOWN:
                self.__unhealthy_endpoints.add(normalized_endpoint)
                self._router.remove_node(normalized_endpoint)
        self._cluster_pools[normalized_endpoint].metrics.on_status_update(status)
