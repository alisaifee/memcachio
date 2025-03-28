from __future__ import annotations

from ssl import SSLContext
from typing import AnyStr, Generic, Literal, TypeVar, Unpack, overload

from memcachio.commands import (
    AddCommand,
    AppendCommand,
    CheckAndSetCommand,
    Command,
    DecrCommand,
    DeleteCommand,
    FlushAllCommand,
    GatCommand,
    GatsCommand,
    GetCommand,
    GetsCommand,
    IncrCommand,
    PrependCommand,
    ReplaceCommand,
    SetCommand,
    StatsCommand,
    TouchCommand,
    VersionCommand,
)
from memcachio.connection import ConnectionParams
from memcachio.pool import Pool
from memcachio.types import KeyT, MemcachedItem, ServerLocator, ValueT

R = TypeVar("R")


class Client(Generic[AnyStr]):
    connection_pool: Pool

    @overload
    def __init__(
        self: Client[str],
        server_locator: ServerLocator | None = ...,
        decode_responses: Literal[True] = True,
        encoding: str = ...,
        max_connections: int = ...,
        blocking_timeout: float = ...,
        purge_unhealthy_nodes: bool = ...,
        connection_pool: Pool | None = ...,
        **connection_kwargs: Unpack[ConnectionParams],
    ) -> None: ...

    @overload
    def __init__(
        self: Client[bytes],
        server_locator: ServerLocator | None = ...,
        decode_responses: Literal[False] = False,
        encoding: str = ...,
        max_connections: int = ...,
        blocking_timeout: float = ...,
        purge_unhealthy_nodes: bool = ...,
        connection_pool: Pool | None = ...,
        **connection_kwargs: Unpack[ConnectionParams],
    ) -> None: ...
    def __init__(
        self,
        server_locator: ServerLocator | None = None,
        decode_responses: bool = False,
        encoding: str = "utf-8",
        max_connections: int = 10,
        blocking_timeout: float = 30.0,
        purge_unhealthy_nodes: bool = False,
        connection_pool: Pool | None = None,
        connect_timeout: float | None = 1.0,
        read_timeout: float | None = None,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        max_inflight_requests_per_connection: int | None = None,
        ssl_context: SSLContext | None = None,
    ) -> None:
        if server_locator:
            self.connection_pool = Pool.from_locator(
                server_locator,
                max_connections=max_connections,
                blocking_timeout=blocking_timeout,
                purge_unhealthy_nodes=purge_unhealthy_nodes,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                socket_keepalive=socket_keepalive,
                socket_keepalive_options=socket_keepalive_options,
                max_inflight_requests_per_connection=max_inflight_requests_per_connection,
                ssl_context=ssl_context,
            )
        elif connection_pool:
            self.connection_pool = connection_pool
        else:
            raise ValueError("One of `server_locator` or `connection_pool` must be provided")
        self.decode_responses = decode_responses
        self.encoding = encoding

    async def execute_command(self, command: Command[R]) -> R:
        return await self.connection_pool.execute_command(command)

    async def get(self, *keys: KeyT) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        return await self.execute_command(
            GetCommand(*keys, decode=self.decode_responses, encoding=self.encoding)
        )

    async def gets(self, *keys: KeyT) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        return await self.execute_command(
            GetsCommand(*keys, decode=self.decode_responses, encoding=self.encoding)
        )

    async def gat(self, *keys: KeyT, expiry: int) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        return await self.execute_command(
            GatCommand(*keys, expiry=expiry, decode=self.decode_responses, encoding=self.encoding)
        )

    async def gats(self, *keys: KeyT, expiry: int) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        return await self.execute_command(
            GatsCommand(*keys, expiry=expiry, decode=self.decode_responses, encoding=self.encoding)
        )

    async def set(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool:
        return await self.execute_command(
            SetCommand(
                key, value, flags=flags, expiry=expiry, noreply=noreply, encoding=self.encoding
            )
        )

    async def cas(
        self,
        key: KeyT,
        value: ValueT,
        cas: int,
        /,
        flags: int = 0,
        expiry: int = 0,
        noreply: bool = False,
    ) -> bool:
        return await self.execute_command(
            CheckAndSetCommand(
                key,
                value,
                flags=flags,
                expiry=expiry,
                noreply=noreply,
                cas=cas,
                encoding=self.encoding,
            ),
        )

    async def add(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool:
        return await self.execute_command(
            AddCommand(
                key, value, flags=flags, expiry=expiry, noreply=noreply, encoding=self.encoding
            )
        )

    async def append(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool:
        return await self.execute_command(
            AppendCommand(
                key, value, flags=flags, expiry=expiry, noreply=noreply, encoding=self.encoding
            )
        )

    async def prepend(self, key: KeyT, value: ValueT, /, noreply: bool = False) -> bool:
        return await self.execute_command(
            PrependCommand(key, value, noreply=noreply, encoding=self.encoding)
        )

    async def replace(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool:
        return await self.execute_command(
            ReplaceCommand(
                key, value, flags=flags, expiry=expiry, noreply=noreply, encoding=self.encoding
            )
        )

    async def incr(self, key: KeyT, value: int, /, noreply: bool = False) -> int | None:
        return await self.execute_command(IncrCommand(key, value, noreply))

    async def decr(self, key: KeyT, value: int, /, noreply: bool = False) -> int | None:
        return await self.execute_command(DecrCommand(key, value, noreply))

    async def delete(self, key: KeyT, /, noreply: bool = False) -> bool:
        return await self.execute_command(DeleteCommand(key, noreply=noreply))

    async def touch(self, key: KeyT, expiry: int, /, noreply: bool = False) -> bool:
        return await self.execute_command(TouchCommand(key, expiry=expiry, noreply=noreply))

    async def flushall(self, expiry: int = 0, /, noreply: bool = False) -> bool:
        return await self.execute_command(FlushAllCommand(expiry, noreply))

    async def stats(self, arg: str | None = None) -> dict[AnyStr, AnyStr]:
        return await self.execute_command(
            StatsCommand(arg, decode_responses=self.decode_responses, encoding=self.encoding)
        )

    async def version(self) -> str:
        return await self.execute_command(VersionCommand(noreply=False))

    def __del__(self) -> None:
        if self.connection_pool:
            self.connection_pool.close()
