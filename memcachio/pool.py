from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import suppress
from ssl import SSLContext
from typing import Any, Iterable, Type, TypeVar, cast

from memcachio.commands import Command
from memcachio.connection import (
    BaseConnection,
    ConnectionParams,
    TCPConnection,
    UnixSocketConnection,
)
from memcachio.routing import KeyRouter
from memcachio.types import (
    ServerLocator,
    SingleServerLocator,
    UnixSocketLocator,
    is_single_server,
    normalize_locator,
)

R = TypeVar("R")


class Pool(ABC):
    def __init__(
        self,
        locator: ServerLocator,
        max_connections: int = 2,
        blocking_timeout: float = 30.0,
        idle_connection_timeout: float = 5.0,
        # connection parameters
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        max_inflight_requests_per_connection: int | None = None,
        ssl_context: SSLContext | None = None,
    ):
        self._locator = normalize_locator(locator)
        self._max_connections = max_connections
        self._blocking_timeout = blocking_timeout
        self._idle_connection_timeout = idle_connection_timeout
        self._connection_parameters: ConnectionParams = {
            "connect_timeout": connect_timeout,
            "read_timeout": read_timeout,
            "socket_keepalive": socket_keepalive,
            "socket_keepalive_options": socket_keepalive_options,
            "max_inflight_requests_per_connection": max_inflight_requests_per_connection,
            "ssl_context": ssl_context,
        }

    @abstractmethod
    def close(self) -> None: ...

    @abstractmethod
    async def execute_command(self, command: Command[R]) -> R: ...

    @classmethod
    def from_locator(
        cls,
        locator: ServerLocator,
        max_connections: int = 2,
        blocking_timeout: float = 30,
        purge_unhealthy_nodes: bool = False,
        idle_connection_timeout: float = 5.0,
        # connection parameters
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        max_inflight_requests_per_connection: int | None = None,
        ssl_context: SSLContext | None = None,
    ) -> Pool:
        kls: Type[Pool]
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
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            max_inflight_requests_per_connection=max_inflight_requests_per_connection,
            ssl_context=ssl_context,
            **extra_args,
        )

    def __del__(self) -> None:
        with suppress(RuntimeError):
            self.close()


class SingleServerPool(Pool):
    def __init__(
        self,
        locator: SingleServerLocator,
        max_connections: int = 2,
        blocking_timeout: float = 30.0,
        idle_connection_timeout: float = 5.0,
        # connection parameters
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        max_inflight_requests_per_connection: int | None = None,
        ssl_context: SSLContext | None = None,
    ) -> None:
        super().__init__(
            locator,
            max_connections,
            blocking_timeout,
            idle_connection_timeout,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            max_inflight_requests_per_connection=max_inflight_requests_per_connection,
            ssl_context=ssl_context,
        )
        self._connections: asyncio.Queue[BaseConnection | None] = asyncio.LifoQueue(
            self._max_connections
        )
        self._pool_lock: asyncio.Lock = asyncio.Lock()
        self._connection_class: Type[TCPConnection | UnixSocketConnection]
        self._connect_failed: int = 0
        self._initialized = False
        self._monitor_task: asyncio.Task[None] | None = None
        self._locator = self._locator
        while True:
            try:
                self._connections.put_nowait(None)
            except asyncio.QueueFull:
                break

    async def execute_command(self, command: Command[R]) -> R:
        connection, release = None, None
        try:
            connection, release = await self.acquire(command)
            request = await connection.create_request(command)
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
        except asyncio.TimeoutError:
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
                            print("disconnecting idle")
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
        locator: list[SingleServerLocator],
        max_connections: int = 2,
        blocking_timeout: float = 30.0,
        purge_unhealthy_nodes: bool = False,
        idle_connection_timeout: float = 5.0,
        # connection parameters
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        max_inflight_requests_per_connection: int | None = None,
        ssl_context: SSLContext | None = None,
    ) -> None:
        self._cluster_pools: dict[SingleServerLocator, SingleServerPool] = {}
        self._pool_lock = asyncio.Lock()
        self._initialized = False
        self._purge_unhealthy_nodes = purge_unhealthy_nodes
        super().__init__(
            locator,
            max_connections,
            blocking_timeout,
            idle_connection_timeout,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            max_inflight_requests_per_connection=max_inflight_requests_per_connection,
            ssl_context=ssl_context,
        )
        self._unhealthy_nodes: set[SingleServerLocator] = set()
        self._router = KeyRouter(self.nodes)
        self._cluster_monitor_task: asyncio.Task[None] | None = None

    @property
    def nodes(self) -> set[SingleServerLocator]:
        return set(cast(Iterable[SingleServerLocator], self._locator)) - self._unhealthy_nodes

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

    async def execute_command(self, command: Command[R]) -> R:
        mapping = defaultdict(lambda: [])
        await self.initialize()
        if command.keys:
            for key in command.keys:
                mapping[self._router.get_node(key)].append(key)
            results = await asyncio.gather(
                *[
                    self._cluster_pools[node].execute_command(command.subset(keys))
                    for node, keys in mapping.items()
                ]
            )

        else:
            results = await asyncio.gather(
                *[pool.execute_command(command) for pool in self._cluster_pools.values()]
            )
        if command.noreply:
            return None  # type: ignore[return-value]
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
