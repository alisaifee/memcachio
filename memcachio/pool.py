from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Type, TypeVar

from memcachio.commands import Command, MultiKeyCommand, NoKeyCommand, SingleKeyCommand
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
)

R = TypeVar("R")


class Pool(ABC):
    def __init__(
        self,
        locator: ServerLocator,
        max_connections: int = 2,
        blocking_timeout: int = 30,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
    ):
        self._locator = locator
        self._max_connections = max_connections
        self._blocking_timeout = blocking_timeout
        self._connection_parameters: ConnectionParams = {
            "connect_timeout": connect_timeout,
            "read_timeout": read_timeout,
            "socket_keepalive": socket_keepalive,
            "socket_keepalive_options": socket_keepalive_options,
        }

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def execute_command(self, command: Command[R]) -> R: ...

    @classmethod
    def from_locator(
        cls,
        locator: ServerLocator,
        max_connections: int,
        connect_timeout: float | None,
        read_timeout: float | None,
    ) -> Pool:
        kls: Type[Pool]
        if is_single_server(locator):
            kls = SingleServerPool
        else:
            kls = ClusterPool

        return kls(
            locator,
            max_connections=max_connections,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )


class SingleServerPool(Pool):
    def __init__(
        self,
        locator: SingleServerLocator,
        max_connections: int = 2,
        blocking_timeout: int = 30,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
    ) -> None:
        super().__init__(
            locator,
            max_connections,
            blocking_timeout,
            connect_timeout,
            read_timeout,
            socket_keepalive,
            socket_keepalive_options,
        )
        self._total_connections: int = 0
        self._connections: asyncio.Queue[BaseConnection | None] = asyncio.LifoQueue(
            self._max_connections
        )
        self._pool_lock: asyncio.Lock = asyncio.Lock()
        self._connection_class: Type[TCPConnection | UnixSocketConnection]
        self._connect_failed: int = 0
        self._initialized = False
        while True:
            try:
                self._connections.put_nowait(None)
            except asyncio.QueueFull:
                break

    async def execute_command(self, command: Command[R]) -> R:
        connection = await self.acquire(command)
        request = await connection.create_request(command)
        norelease = False
        if connection.happy():
            norelease = True
            self._connections.put_nowait(connection)
        response = await request
        if not norelease:
            self._connections.put_nowait(connection)
        return response

    async def _create_connection(self) -> BaseConnection:
        connection: BaseConnection | None = None
        self._total_connections += 1
        if is_single_server(self._locator):
            if isinstance(self._locator, UnixSocketLocator):
                connection = UnixSocketConnection(self._locator, **self._connection_parameters)
            else:
                connection = TCPConnection(self._locator, **self._connection_parameters)
        assert connection
        try:
            await connection.connect()
        except:
            self._connect_failed += 1
            raise
        return connection

    async def initialize(self) -> None:
        self._initialized = True

    async def acquire(self, command: Command[Any]) -> BaseConnection:
        try:
            async with asyncio.timeout(self._blocking_timeout):
                connection = await self._connections.get()
                if not connection:
                    print("creating")
                    connection = await self._create_connection()
            return connection
        except asyncio.TimeoutError:
            raise

    async def close(self) -> None:
        while True:
            try:
                if connection := self._connections.get_nowait():
                    connection.disconnect()
            except asyncio.QueueEmpty:
                break

    @property
    def deadpool(self) -> bool:
        return self._connect_failed > 0


class ClusterPool(Pool):
    _locator: list[SingleServerLocator]

    def __init__(
        self,
        locator: list[SingleServerLocator],
        max_connections: int = 2,
        blocking_timeout: int = 30,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        socket_keepalive: bool | None = True,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
    ) -> None:
        self._cluster_pools: dict[SingleServerLocator, SingleServerPool] = {}
        self._pool_lock = asyncio.Lock()
        self._initialized = False
        super().__init__(
            locator,
            max_connections,
            blocking_timeout,
            connect_timeout,
            read_timeout,
            socket_keepalive,
            socket_keepalive_options,
        )
        self._router = KeyRouter(self._locator)
        self._cluster_monitor_task: asyncio.Task[None] | None = None

    async def __cluster_monitor(self) -> None:
        while True:
            try:
                for node, pool in self._cluster_pools.items():
                    if pool.deadpool:
                        self._router.remove_node(node)
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break

    async def execute_command(self, command: Command[R]) -> R:
        mapping = defaultdict(lambda: [])
        await self.initialize()
        if isinstance(command, MultiKeyCommand):
            for key in command.keys:
                mapping[self._router.get_node(key)].append(key)
            results = await asyncio.gather(
                *[
                    self._cluster_pools[node].execute_command(command.subset(keys))
                    for node, keys in mapping.items()
                ]
            )

        elif isinstance(command, NoKeyCommand):
            results = await asyncio.gather(
                *[pool.execute_command(command) for pool in self._cluster_pools.values()]
            )
        else:
            assert isinstance(command, SingleKeyCommand)
            node = self._router.get_node(command.key)
            results = [await self._cluster_pools[node].execute_command(command)]
        if command.noreply:
            return None  # type: ignore[return-value]
        return command.merge(results)

    async def initialize(self) -> None:
        async with self._pool_lock:
            if self._initialized:
                return
            for node in self._locator:
                self._cluster_pools[node] = SingleServerPool(
                    node, self._max_connections, **self._connection_parameters
                )

            for pool in self._cluster_pools.values():
                await pool.initialize()

            if not self._cluster_monitor_task:
                self._cluster_monitor_task = asyncio.create_task(self.__cluster_monitor())
            self._initialized = True

    async def close(self) -> None:
        for pool in self._cluster_pools.values():
            await pool.close()
        self._cluster_pools.clear()
        if self._cluster_monitor_task:
            self._cluster_monitor_task.cancel()
