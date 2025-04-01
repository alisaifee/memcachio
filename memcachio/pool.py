from __future__ import annotations

import asyncio
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
    MIN_CONNECTIONS,
)
from .errors import ConnectionNotAvailable
from .routing import KeyRouter
from .types import (
    MemcachedLocator,
    SingleMemcachedInstanceLocator,
    UnixSocketLocator,
    is_single_server,
    normalize_locator,
)

R = TypeVar("R")


class Pool(ABC):
    """
    The abstract base class for a connection pool used by
    :class:`~memcachio.Client`
    """

    def __init__(
        self,
        locator: MemcachedLocator,
        min_connections: int = MIN_CONNECTIONS,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        **connection_args: Unpack[ConnectionParams],
    ):
        """
        :param locator: The memcached server address(es)
        :param min_connections: The minimum number of connections to keep in the pool.
        :param max_connections: The maximum number of simultaneous connections to memcached.
        :param blocking_timeout: The timeout (in seconds) to wait for a connection to become available.
        :param idle_connection_timeout: The maximum time to allow a connection to remain idle in the pool
         before being disconnected
        :param connection_args: Arguments to pass to the constructor of :class:`~memcachio.BaseConnection`.
         refer to :class:`~memcachio.connection.ConnectionParams`
        """
        self.locator = normalize_locator(locator)
        self._max_connections = max_connections
        self._min_connections = min_connections
        self._blocking_timeout = blocking_timeout
        self._idle_connection_timeout = idle_connection_timeout
        self._connection_parameters: ConnectionParams = connection_args

    @abstractmethod
    def close(self) -> None:
        """
        Closes the connection pool and disconnects all active connections
        """
        ...

    async def execute_command(self, command: Command[R]) -> None:
        """
        Dispatches a command to memcached. To receive the response the future
        pointed to by ``command.response`` should be awaited as it will be updated
        when the response (or exception) is available on the transport.
        """
        ...

    @classmethod
    def from_locator(
        cls,
        locator: MemcachedLocator,
        min_connections: int = MIN_CONNECTIONS,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        hashing_function: Callable[[str], int] | None = None,
        **connection_args: Unpack[ConnectionParams],
    ) -> Pool:
        """
        Returns either a :class:`~memcachio.SingleServerPool` or :class:`~memcachio.ClusterPool`
        depending on whether ``locator`` is a single instance or a collection of servers

        :meta private:
        """
        kls: type[Pool]
        extra_args = {}
        if is_single_server(locator):
            kls = SingleServerPool
        else:
            kls = ClusterPool
            extra_args["hashing_function"] = hashing_function
        return kls(
            locator,
            min_connections=min_connections,
            max_connections=max_connections,
            blocking_timeout=blocking_timeout,
            idle_connection_timeout=idle_connection_timeout,
            **extra_args,
            **connection_args,
        )

    def __del__(self) -> None:
        with suppress(RuntimeError):
            self.close()


class SingleServerPool(Pool):
    """
    Connection pool to manage connections to a single memcached
    server instance.
    """

    def __init__(
        self,
        locator: SingleMemcachedInstanceLocator,
        min_connections: int = MIN_CONNECTIONS,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        **connection_args: Unpack[ConnectionParams],
    ) -> None:
        super().__init__(
            locator,
            min_connections=min_connections,
            max_connections=max_connections,
            blocking_timeout=blocking_timeout,
            idle_connection_timeout=idle_connection_timeout,
            **connection_args,
        )
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

    async def execute_command(self, command: Command[R]) -> None:
        connection, release = None, None
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

    async def _create_connection(self) -> BaseConnection:
        connection: BaseConnection | None = None
        if is_single_server(self.locator):
            if isinstance(self.locator, UnixSocketLocator):
                connection = UnixSocketConnection(self.locator, **self._connection_parameters)
            else:
                connection = TCPConnection(self.locator, **self._connection_parameters)
        assert connection
        if not connection.connected:
            await connection.connect()
        return connection

    def __check_connection_idle(self, connection: BaseConnection) -> None:
        if (
            time.time() - connection.metrics.last_read > self._idle_connection_timeout
            and connection.metrics.requests_pending == 0
            and len(self._active_connections) > self._min_connections
        ):
            connection.disconnect()
            self._active_connections.remove(connection)
        elif connection.connected:
            asyncio.get_running_loop().call_later(
                self._idle_connection_timeout, self.__check_connection_idle, connection
            )

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._pool_lock:
            if self._initialized:
                return
            for _ in range(self._min_connections):
                connection = self._connections.get_nowait()
                if not connection:
                    self._connections.put_nowait(await self._create_connection())

            self._initialized = True

    async def acquire(self, command: Command[Any]) -> tuple[BaseConnection, bool]:
        await self.initialize()
        released = False
        try:
            async with asyncio.timeout(self._blocking_timeout):
                connection = await self._connections.get()
                if connection and connection.reusable:
                    self._connections.put_nowait(connection)
                    released = True
                else:
                    if not connection:
                        connection = await self._create_connection()
                        self._connections.put_nowait(connection)
                        released = True
            return connection, not released
        except TimeoutError:
            raise ConnectionNotAvailable(self, self._blocking_timeout)

    def close(self) -> None:
        while True:
            try:
                if connection := self._connections.get_nowait():
                    connection.disconnect()
            except asyncio.QueueEmpty:
                break

    def __on_connection_disconnected(self, connection: BaseConnection) -> None:
        self._active_connections.remove(connection)

    def __on_connection_created(self, connection: BaseConnection) -> None:
        self._active_connections.add(connection)
        if self._idle_connection_timeout:
            asyncio.get_running_loop().call_later(
                self._idle_connection_timeout, self.__check_connection_idle, connection
            )


class ClusterPool(Pool):
    """
    Connection pool to manage connections to a multiple memcached
    server instances.

    For multi-key commands, rendezvous hashing is used to distribute the command
    to the appropriate nodes.
    """

    def __init__(
        self,
        locator: Sequence[SingleMemcachedInstanceLocator],
        min_connections: int = MIN_CONNECTIONS,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        hashing_function: Callable[[str], int] | None = None,
        **connection_args: Unpack[ConnectionParams],
    ) -> None:
        """
        :param locator: The memcached server address(es)
        :param min_connections: The minimum number of connections per node to keep in the pool.
        :param max_connections: The maximum number of simultaneous connections per  memcached node.
        :param blocking_timeout: The timeout (in seconds) to wait for a connection to become available.
        :param idle_connection_timeout: The maximum time to allow a connection to remain idle in the pool
         before being disconnected
        :param hashing_function: A function to use for routing keys to
         nodes for multi-key commands. If none is provided the default
         :func:`hashlib.md5` implementation from the standard library is used.
        :param connection_args: Arguments to pass to the constructor of :class:`~memcachio.BaseConnection`.
         refer to :class:`~memcachio.connection.ConnectionParams`
        """
        self._cluster_pools: dict[SingleMemcachedInstanceLocator, SingleServerPool] = {}
        self._pool_lock = asyncio.Lock()
        self._initialized = False
        super().__init__(
            locator,
            min_connections=min_connections,
            max_connections=max_connections,
            blocking_timeout=blocking_timeout,
            idle_connection_timeout=idle_connection_timeout,
            **connection_args,
        )
        self._router = KeyRouter(self.nodes, hasher=hashing_function)

    @property
    def nodes(self) -> set[SingleMemcachedInstanceLocator]:
        return set(cast(Iterable[SingleMemcachedInstanceLocator], self.locator))

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._pool_lock:
            if self._initialized:
                return
            for node in self.nodes:
                self._cluster_pools[node] = SingleServerPool(
                    node,
                    min_connections=self._min_connections,
                    max_connections=self._max_connections,
                    blocking_timeout=self._blocking_timeout,
                    idle_connection_timeout=self._idle_connection_timeout,
                    **self._connection_parameters,
                )

            await asyncio.gather(*[pool.initialize() for pool in self._cluster_pools.values()])
            self._initialized = True

    async def execute_command(self, command: Command[R]) -> None:
        """
        Dispatches a command to the appropriate memcached node(s).
        To receive the response the future pointed to by ``command.response`` should be awaited
        as it will be updated when the response(s) (or exception) are available on the transport
        and merged (if it is a command that spans multiple nodes).
        """
        mapping = defaultdict(list)
        await self.initialize()
        if command.keys:
            for key in command.keys:
                mapping[self._router.get_node(key)].append(key)
            node_commands = {node: command.clone(keys) for node, keys in mapping.items()}
        else:
            node_commands = {
                node: command.clone(command.keys) for node in self._cluster_pools.keys()
            }
        await asyncio.gather(
            *[
                self._cluster_pools[node].execute_command(node_command)
                for node, node_command in node_commands.items()
            ]
        )
        if not command.noreply:
            command.response.set_result(
                command.merge(
                    await asyncio.gather(*[command.response for command in node_commands.values()])
                )
            )

    def close(self) -> None:
        for pool in self._cluster_pools.values():
            pool.close()
        self._cluster_pools.clear()
