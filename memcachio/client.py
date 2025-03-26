from __future__ import annotations

from typing import TYPE_CHECKING, AnyStr, Generic, Literal, TypeVar, overload

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
from memcachio.pool import Pool

if TYPE_CHECKING:
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
        connect_timeout: float | None = ...,
        read_timeout: float | None = ...,
        connection_pool: Pool | None = ...,
    ) -> None: ...

    @overload
    def __init__(
        self: Client[bytes],
        server_locator: ServerLocator | None = ...,
        decode_responses: Literal[False] = False,
        encoding: str = ...,
        max_connections: int = ...,
        connect_timeout: float | None = ...,
        read_timeout: float | None = ...,
        connection_pool: Pool | None = ...,
    ) -> None: ...
    def __init__(
        self,
        server_locator: ServerLocator | None = None,
        decode_responses: bool = False,
        encoding: str = "utf-8",
        max_connections: int = 10,
        connect_timeout: float | None = 1,
        read_timeout: float | None = None,
        connection_pool: Pool | None = None,
    ) -> None:
        if server_locator:
            self.connection_pool = Pool.from_locator(
                server_locator,
                max_connections=max_connections,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
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
            GetCommand(keys, decode=self.decode_responses, encoding=self.encoding)
        )

    async def gets(self, *keys: KeyT) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        return await self.execute_command(
            GetsCommand(keys, decode=self.decode_responses, encoding=self.encoding)
        )

    async def gat(self, *keys: KeyT, expiry: int) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        return await self.execute_command(
            GatCommand(keys, expiry, decode=self.decode_responses, encoding=self.encoding)
        )

    async def gats(self, *keys: KeyT, expiry: int) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        return await self.execute_command(
            GatsCommand(keys, expiry, decode=self.decode_responses, encoding=self.encoding)
        )

    async def set(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool:
        return await self.execute_command(
            SetCommand(key, value, flags, expiry, noreply, encoding=self.encoding)
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
            CheckAndSetCommand(key, value, flags, expiry, noreply, cas=cas, encoding=self.encoding),
        )

    async def add(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool:
        return await self.execute_command(
            AddCommand(key, value, flags, expiry, noreply, encoding=self.encoding)
        )

    async def append(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool:
        return await self.execute_command(
            AppendCommand(key, value, flags, expiry, noreply, encoding=self.encoding)
        )

    async def prepend(self, key: KeyT, value: ValueT, /, noreply: bool = False) -> bool:
        return await self.execute_command(
            PrependCommand(key, value, noreply=noreply, encoding=self.encoding)
        )

    async def replace(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool:
        return await self.execute_command(
            ReplaceCommand(key, value, flags, expiry, noreply, encoding=self.encoding)
        )

    async def incr(self, key: KeyT, value: int, /, noreply: bool = False) -> int | None:
        return await self.execute_command(IncrCommand(key, value, noreply))

    async def decr(self, key: KeyT, value: int, /, noreply: bool = False) -> int | None:
        return await self.execute_command(DecrCommand(key, value, noreply))

    async def delete(self, key: KeyT, /, noreply: bool = False) -> bool:
        return await self.execute_command(DeleteCommand(key, noreply))

    async def touch(self, key: KeyT, expiry: int, /, noreply: bool = False) -> bool:
        return await self.execute_command(TouchCommand(key, expiry, noreply))

    async def flushall(self, expiry: int = 0, /, noreply: bool = False) -> bool:
        return await self.execute_command(FlushAllCommand(expiry, noreply))

    async def stats(self, arg: str | None = None) -> dict[AnyStr, AnyStr]:
        return await self.execute_command(
            StatsCommand(arg, decode_responses=self.decode_responses, encoding=self.encoding)
        )

    async def version(self) -> str:
        return await self.execute_command(VersionCommand(False))
