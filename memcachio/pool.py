from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from asyncio import Future
from collections import defaultdict
from collections.abc import Iterable, Sequence
from contextlib import suppress
from typing import Any, TypeVar, Unpack, cast, overload

from memcachio.commands import Command
from memcachio.connection import (
    BaseConnection,
    ConnectionParams,
    TCPConnection,
    UnixSocketConnection,
)
from memcachio.defaults import (
    BLOCKING_TIMEOUT,
    IDLE_CONNECTION_TIMEOUT,
    MAX_CONNECTIONS,
    PURGE_UNHEALTHY_NODES,
)
from memcachio.routing import KeyRouter
from memcachio.types import (
    MemcachedLocator,
    SingleMemcachedInstanceLocator,
    UnixSocketLocator,
    is_single_server,
    normalize_locator,
)

R = TypeVar("R")


class Pool(ABC):
    def __init__(
        self,
        locator: MemcachedLocator,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        **connection_args: Unpack[ConnectionParams],
    ):
        self._locator = normalize_locator(locator)
        self._max_connections = max_connections
        self._blocking_timeout = blocking_timeout
        self._idle_connection_timeout = idle_connection_timeout
        self._connection_parameters: ConnectionParams = connection_args

    @abstractmethod
    def close(self) -> None: ...

    @overload
    async def execute_command(self, command: Command[R]) -> R: ...
    @overload
    async def execute_command(self, command: Command[R], noreply: bool = ...) -> R | None: ...
    @abstractmethod
    async def execute_command(self, command: Command[R], noreply: bool = False) -> R | None: ...

    @classmethod
    def from_locator(
        cls,
        locator: MemcachedLocator,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        purge_unhealthy_nodes: bool = PURGE_UNHEALTHY_NODES,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        **connection_args: Unpack[ConnectionParams],
    ) -> Pool:
        kls: type[Pool]
        extra_args = {}
        if is_single_server(locator):
            kls = SingleServerPool
        else:
            kls = ClusterPool
            extra_args = {"purge_unhealthy_nodes": purge_unhealthy_nodes}
        return kls(
            locator,
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
    def __init__(
        self,
        locator: SingleMemcachedInstanceLocator,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        **connection_args: Unpack[ConnectionParams],
    ) -> None:
        super().__init__(
            locator,
            max_connections,
            blocking_timeout,
            idle_connection_timeout,
            **connection_args,
        )
        self._connections: asyncio.Queue[BaseConnection | None] = asyncio.LifoQueue(
            self._max_connections
        )
        self._pool_lock: asyncio.Lock = asyncio.Lock()
        self._connection_class: type[TCPConnection | UnixSocketConnection]
        self._connect_failed: int = 0
        self._initialized = False
        self._monitor_task: asyncio.Task[None] | None = None
        self._locator = self._locator
        while True:
            try:
                self._connections.put_nowait(None)
            except asyncio.QueueFull:
                break

    @overload
    async def execute_command(self, command: Command[R]) -> R: ...
    @overload
    async def execute_command(self, command: Command[R], noreply: bool = ...) -> R | None: ...
    async def execute_command(self, command: Command[R], noreply: bool = False) -> R | None:
        connection, release = None, None
        try:
            connection, release = await self.acquire(command)
            response = None
            if noreply:
                await (await connection.create_request(command, noreply=True))
            else:
                request: Future[R] = await connection.create_request(command)
                response = await request
        except (ConnectionError, OSError):
            self._connect_failed += 1
            raise
        finally:
            if release and connection:
                self._connections.put_nowait(connection)
        return response

    async def _create_connection(self) -> BaseConnection:
        connection: BaseConnection | None = None
        if is_single_server(self._locator):
            if isinstance(self._locator, UnixSocketLocator):
                connection = UnixSocketConnection(self._locator, **self._connection_parameters)
            else:
                connection = TCPConnection(self._locator, **self._connection_parameters)
        assert connection
        try:
            if not connection.connected:
                await connection.connect()
        except:
            self._connect_failed += 1
            raise
        return connection

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._pool_lock:
            if self._initialized:
                return
            try:
                connection = self._connections.get_nowait()
            except asyncio.QueueEmpty:
                connection = None
            if not connection:
                self._connections.put_nowait(await self._create_connection())
            if not self._monitor_task:
                self._monitor_task = asyncio.create_task(self.__monitor())

            self._initialized = True

    async def acquire(self, command: Command[Any]) -> tuple[BaseConnection, bool]:
        await self.initialize()
        released = False
        try:
            async with asyncio.timeout(self._blocking_timeout):
                connection = await self._connections.get()
                if connection and connection.reusable():
                    self._connections.put_nowait(connection)
                    released = True
                else:
                    if not connection:
                        connection = await self._create_connection()
                        self._connections.put_nowait(connection)
                        released = True
            return connection, not released
        except TimeoutError:
            raise

    def close(self) -> None:
        while True:
            try:
                if connection := self._connections.get_nowait():
                    connection.disconnect()
            except asyncio.QueueEmpty:
                break

        if self._monitor_task and not self._monitor_task.done():
            with suppress(RuntimeError):
                self._monitor_task.cancel()

    async def __monitor(self) -> None:
        while True:
            try:
                for i in range(self._connections.qsize() - 1):
                    if connection := self._connections._queue[i]:  # type: ignore[attr-defined]
                        if (
                            connection.connected
                            and not connection.metrics.requests_pending
                            and time.time() - connection.metrics.last_read
                            > self._idle_connection_timeout
                        ):
                            connection.disconnect()
                await asyncio.sleep(self._idle_connection_timeout)
            except asyncio.CancelledError:
                break

    @property
    def deadpool(self) -> bool:
        return self._connect_failed > 0


class ClusterPool(Pool):
    def __init__(
        self,
        locator: Sequence[SingleMemcachedInstanceLocator],
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        purge_unhealthy_nodes: bool = PURGE_UNHEALTHY_NODES,
        idle_connection_timeout: float = IDLE_CONNECTION_TIMEOUT,
        **connection_args: Unpack[ConnectionParams],
    ) -> None:
        self._cluster_pools: dict[SingleMemcachedInstanceLocator, SingleServerPool] = {}
        self._pool_lock = asyncio.Lock()
        self._initialized = False
        self._purge_unhealthy_nodes = purge_unhealthy_nodes
        super().__init__(
            locator, max_connections, blocking_timeout, idle_connection_timeout, **connection_args
        )
        self._unhealthy_nodes: set[SingleMemcachedInstanceLocator] = set()
        self._router = KeyRouter(self.nodes)
        self._cluster_monitor_task: asyncio.Task[None] | None = None

    @property
    def nodes(self) -> set[SingleMemcachedInstanceLocator]:
        return (
            set(cast(Iterable[SingleMemcachedInstanceLocator], self._locator))
            - self._unhealthy_nodes
        )

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._pool_lock:
            if self._initialized:
                return
            for node in self.nodes:
                self._cluster_pools[node] = SingleServerPool(
                    node, self._max_connections, **self._connection_parameters
                )

            await asyncio.gather(*[pool.initialize() for pool in self._cluster_pools.values()])

            if not self._cluster_monitor_task:
                self._cluster_monitor_task = asyncio.create_task(self.__cluster_monitor())
            self._initialized = True

    @overload
    async def execute_command(self, command: Command[R]) -> R: ...
    @overload
    async def execute_command(self, command: Command[R], noreply: bool = ...) -> R | None: ...
    async def execute_command(self, command: Command[R], noreply: bool = False) -> R | None:
        mapping = defaultdict(list)
        await self.initialize()
        if command.keys:
            for key in command.keys:
                mapping[self._router.get_node(key)].append(key)
            results = await asyncio.gather(
                *[
                    self._cluster_pools[node].execute_command(command.subset(keys), noreply=noreply)
                    for node, keys in mapping.items()
                ]
            )

        else:
            results = await asyncio.gather(
                *[
                    pool.execute_command(command, noreply=noreply)
                    for pool in self._cluster_pools.values()
                ]
            )
        if noreply:
            return None
        return command.merge(results)

    async def __cluster_monitor(self) -> None:
        while True:
            try:
                if self._purge_unhealthy_nodes:
                    for node, pool in self._cluster_pools.items():
                        if pool.deadpool:
                            self._router.remove_node(node)
                            self._unhealthy_nodes.add(node)
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break

    def close(self) -> None:
        for pool in self._cluster_pools.values():
            pool.close()
        self._cluster_pools.clear()
        if self._cluster_monitor_task and not self._cluster_monitor_task.done():
            with suppress(RuntimeError):
                self._cluster_monitor_task.cancel()
