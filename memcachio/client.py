from __future__ import annotations

from ssl import SSLContext
from typing import (
    AnyStr,
    Generic,
    Literal,
    ParamSpec,
    TypeVar,
    overload,
)

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
from memcachio.defaults import (
    BLOCKING_TIMEOUT,
    CONNECT_TIMEOUT,
    ENCODING,
    MAX_CONNECTIONS,
    MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
    MIN_CONNECTIONS,
    READ_TIMEOUT,
)
from memcachio.pool import Pool
from memcachio.types import KeyT, MemcachedItem, MemcachedLocator, ValueT

R = TypeVar("R")
P = ParamSpec("P")


class Client(Generic[AnyStr]):
    connection_pool: Pool

    @overload
    def __init__(
        self: Client[str],
        memcached_location: MemcachedLocator | None = ...,
        decode_responses: Literal[True] = True,
        encoding: str = ...,
        min_connections: int = ...,
        max_connections: int = ...,
        blocking_timeout: float = ...,
        connection_pool: Pool | None = ...,
        connect_timeout: float | None = ...,
        read_timeout: float | None = ...,
        socket_nodelay: bool | None = ...,
        socket_keepalive: bool | None = ...,
        socket_keepalive_options: dict[int, int | bytes] | None = ...,
        max_inflight_requests_per_connection: int = ...,
        ssl_context: SSLContext | None = ...,
        username: str | None = ...,
        password: str | None = ...,
    ) -> None: ...

    @overload
    def __init__(
        self: Client[bytes],
        memcached_location: MemcachedLocator | None = ...,
        decode_responses: Literal[False] = False,
        encoding: str = ...,
        min_connections: int = ...,
        max_connections: int = ...,
        blocking_timeout: float = ...,
        connection_pool: Pool | None = ...,
        connect_timeout: float | None = ...,
        read_timeout: float | None = ...,
        socket_nodelay: bool | None = ...,
        socket_keepalive: bool | None = ...,
        socket_keepalive_options: dict[int, int | bytes] | None = ...,
        max_inflight_requests_per_connection: int = ...,
        ssl_context: SSLContext | None = ...,
        username: str | None = ...,
        password: str | None = ...,
    ) -> None: ...

    def __init__(
        self,
        memcached_location: MemcachedLocator | None = None,
        decode_responses: Literal[True, False] = False,
        encoding: str = ENCODING,
        min_connections: int = MIN_CONNECTIONS,
        max_connections: int = MAX_CONNECTIONS,
        blocking_timeout: float = BLOCKING_TIMEOUT,
        connection_pool: Pool | None = None,
        connect_timeout: float | None = CONNECT_TIMEOUT,
        read_timeout: float | None = READ_TIMEOUT,
        socket_nodelay: bool | None = None,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        max_inflight_requests_per_connection: int = MAX_INFLIGHT_REQUESTS_PER_CONNECTION,
        ssl_context: SSLContext | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        """
        Initialize the Memcached client.

        Either a memcached location or an existing connection pool must be provided.

        :param memcached_location: The memcached server address(es) or locator configuration.
        :param decode_responses: If True, decode responses using the specified encoding; otherwise, responses are returned as bytes.
        :param encoding: The character encoding used when decoding responses.
        :param max_connections: The maximum number of simultaneous connections to memcached.
        :param min_connections: The minimum number of connections to keep in the pool.
        :param blocking_timeout: The timeout (in seconds) to wait for a connection to become available.
        :param connection_pool: An optional pre-initialized connection pool. If provided, memcached_location must be None.
        :param connect_timeout: Timeout (in seconds) for establishing a connection.
        :param read_timeout: Timeout (in seconds) for reading from a connection.
        :param socket_nodelay: If True, disable Nagle's algorithm on the socket.
        :param socket_keepalive: If True, enable TCP keepalive on the socket.
        :param socket_keepalive_options: Additional options for configuring socket keepalive.
        :param max_inflight_requests_per_connection: Maximum number of requests allowed to be in-flight per connection.
        :param ssl_context: An SSL context to use for encrypted connections.
        :param username: Username for SASL authentication (if required).
        :param password: Password for SASL authentication (if required).
        :raises ValueError: If both or neither memcached_location and connection_pool are provided.
        """
        if memcached_location and not connection_pool:
            self.connection_pool = Pool.from_locator(
                memcached_location,
                min_connections=min_connections,
                max_connections=max_connections,
                blocking_timeout=blocking_timeout,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                socket_nodelay=socket_nodelay,
                socket_keepalive=socket_keepalive,
                socket_keepalive_options=socket_keepalive_options,
                max_inflight_requests_per_connection=max_inflight_requests_per_connection,
                ssl_context=ssl_context,
                username=username,
                password=password,
            )
        elif connection_pool and not memcached_location:
            self.connection_pool = connection_pool
        elif connection_pool and memcached_location:
            raise ValueError(
                "One of `memcached_location` or `connection_pool` must be provided not both"
            )
        else:
            raise ValueError("One of `memcached_location` or `connection_pool` must be provided")
        self.decode_responses = decode_responses
        self.encoding = encoding

    async def execute_command(self, command: Command[R]) -> None:
        """
        Execute a given memcached command using the connection pool.

        :param command: A memcached command instance to be executed.

        :meta private:
        """
        await self.connection_pool.execute_command(command)

    async def get(self, *keys: KeyT) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        """
        Retrieve one or more items from memcached.

        :param keys: One or more keys identifying the items to be retrieved.
        :return: A dictionary mapping each found key to its corresponding memcached item.
        """
        command = GetCommand[AnyStr](*keys, decode=self.decode_responses, encoding=self.encoding)
        await self.execute_command(command)
        return await command.response

    async def gets(self, *keys: KeyT) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        """
        Retrieve items along with their CAS (Check And Set) identifiers from memcached.

        :param keys: One or more keys identifying the items to be retrieved.
        :return: A dictionary mapping each found key to its corresponding memcached item, including CAS value.
        """
        command = GetsCommand[AnyStr](*keys, decode=self.decode_responses, encoding=self.encoding)
        await self.execute_command(command)
        return await command.response

    async def gat(self, *keys: KeyT, expiry: int) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        """
        Retrieve items from memcached and update their expiration time.

        :param keys: One or more keys identifying the items to be retrieved.
        :param expiry: New expiration time (in seconds) to be applied to the items.
        :return: A dictionary mapping each found key to its corresponding memcached item.
        """
        command = GatCommand[AnyStr](
            *keys, expiry=expiry, decode=self.decode_responses, encoding=self.encoding
        )
        await self.execute_command(command)
        return await command.response

    async def gats(self, *keys: KeyT, expiry: int) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        """
        Retrieve items with CAS identifiers and update their expiration time.

        :param keys: One or more keys identifying the items to be retrieved.
        :param expiry: New expiration time (in seconds) to be applied to the items.
        :return: A dictionary mapping each found key to its corresponding memcached item, including CAS value.
        """
        command = GatsCommand[AnyStr](
            *keys, expiry=expiry, decode=self.decode_responses, encoding=self.encoding
        )
        await self.execute_command(command)
        return await command.response

    @overload
    async def set(
        self, key: KeyT, value: ValueT, /, flags: int = ..., expiry: int = ...
    ) -> bool: ...

    @overload
    async def set(
        self, key: KeyT, value: ValueT, /, flags: int = ..., expiry: int = ..., noreply: bool = ...
    ) -> bool | None: ...

    async def set(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool | None:
        """
        Store a key-value pair in memcached.

        :param key: The key under which the value should be stored.
        :param value: The value to be stored.
        :param flags: Arbitrary flags stored alongside the value (default: 0).
        :param expiry: Expiration time in seconds (default: 0, meaning no expiration).
        :param noreply: If True, the command will not wait for a reply from the server.
        :return: True if the item was stored successfully, False otherwise; returns None if noreply is True.
        """
        command = SetCommand(
            key, value, flags=flags, expiry=expiry, noreply=noreply, encoding=self.encoding
        )
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def cas(
        self,
        key: KeyT,
        value: ValueT,
        cas: int,
        /,
        flags: int = ...,
        expiry: int = ...,
    ) -> bool: ...

    @overload
    async def cas(
        self,
        key: KeyT,
        value: ValueT,
        cas: int,
        /,
        flags: int = ...,
        expiry: int = ...,
        noreply: bool = ...,
    ) -> bool | None: ...

    async def cas(
        self,
        key: KeyT,
        value: ValueT,
        cas: int,
        /,
        flags: int = 0,
        expiry: int = 0,
        noreply: bool = False,
    ) -> bool | None:
        """
        Perform a CAS (Check-And-Set) operation on an item in memcached.

        :param key: The key of the item to update.
        :param value: The new value to store.
        :param cas: The CAS identifier that must match the current CAS value of the item.
        :param flags: Arbitrary flags stored alongside the value (default: 0).
        :param expiry: Expiration time in seconds (default: 0, meaning no expiration).
        :param noreply: If True, the command will not wait for a reply from the server.
        :return: True if the item was updated successfully, False otherwise; returns None if noreply is True.
        """
        command = CheckAndSetCommand(
            key,
            value,
            flags=flags,
            expiry=expiry,
            noreply=noreply,
            cas=cas,
            encoding=self.encoding,
        )
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def add(
        self, key: KeyT, value: ValueT, /, flags: int = ..., expiry: int = ...
    ) -> bool: ...

    @overload
    async def add(
        self, key: KeyT, value: ValueT, /, flags: int = ..., expiry: int = ..., noreply: bool = ...
    ) -> bool | None: ...

    async def add(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool | None:
        """
        Add a key-value pair to memcached only if the key does not already exist.

        :param key: The key under which the value should be added.
        :param value: The value to be stored.
        :param flags: Arbitrary flags stored alongside the value (default: 0).
        :param expiry: Expiration time in seconds (default: 0, meaning no expiration).
        :param noreply: If True, do not wait for a reply from the server.
        :return: True if the item was added, False otherwise; returns None if noreply is True.
        """
        command = AddCommand(
            key, value, flags=flags, expiry=expiry, noreply=noreply, encoding=self.encoding
        )
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def append(
        self,
        key: KeyT,
        value: ValueT,
        /,
    ) -> bool: ...

    @overload
    async def append(self, key: KeyT, value: ValueT, /, noreply: bool = ...) -> bool | None: ...

    async def append(self, key: KeyT, value: ValueT, /, noreply: bool = False) -> bool | None:
        """
        Append data to an existing item stored in memcached.

        :param key: The key of the item to append data to.
        :param value: The data to append.
        :param noreply: If True, do not wait for a reply from the server.
        :return: True if the append operation succeeded, False otherwise; returns None if noreply is True.
        """
        command = AppendCommand(key, value, noreply=noreply, encoding=self.encoding)
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def prepend(self, key: KeyT, value: ValueT, /) -> bool: ...

    @overload
    async def prepend(self, key: KeyT, value: ValueT, /, noreply: bool = ...) -> bool | None: ...

    async def prepend(self, key: KeyT, value: ValueT, /, noreply: bool = False) -> bool | None:
        """
        Prepend data to an existing item stored in memcached.

        :param key: The key of the item to which data should be prepended.
        :param value: The data to prepend.
        :param noreply: If True, do not wait for a reply from the server.
        :return: True if the prepend operation succeeded, False otherwise; returns None if noreply is True.
        """
        command = PrependCommand(key, value, noreply=noreply, encoding=self.encoding)
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def replace(
        self,
        key: KeyT,
        value: ValueT,
        /,
        flags: int = ...,
        expiry: int = ...,
    ) -> bool: ...

    @overload
    async def replace(
        self,
        key: KeyT,
        value: ValueT,
        /,
        flags: int = ...,
        expiry: int = ...,
        noreply: bool = False,
    ) -> bool | None: ...

    async def replace(
        self, key: KeyT, value: ValueT, /, flags: int = 0, expiry: int = 0, noreply: bool = False
    ) -> bool | None:
        """
        Replace the value for an existing key in memcached.

        :param key: The key whose value is to be replaced.
        :param value: The new value to store.
        :param flags: Arbitrary flags stored alongside the value (default: 0).
        :param expiry: Expiration time in seconds (default: 0, meaning no expiration).
        :param noreply: If True, do not wait for a reply from the server.
        :return: True if the replace operation succeeded, False otherwise; returns None if noreply is True.
        """
        command = ReplaceCommand(
            key, value, flags=flags, expiry=expiry, noreply=noreply, encoding=self.encoding
        )
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def incr(self, key: KeyT, value: int, /) -> int | None: ...

    @overload
    async def incr(self, key: KeyT, value: int, /, noreply: bool = ...) -> int | None: ...

    async def incr(self, key: KeyT, value: int, /, noreply: bool = False) -> int | None:
        """
        Increment the numeric value of a key in memcached.

        :param key: The key whose value should be incremented.
        :param value: The amount by which to increment the current value.
        :param noreply: If True, do not wait for a reply from the server.
        :return: The new value after incrementing, or None if noreply is True.
        """
        command = IncrCommand(key, value, noreply)
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def decr(self, key: KeyT, value: int, /) -> int | None: ...

    @overload
    async def decr(self, key: KeyT, value: int, /, noreply: bool = ...) -> int | None: ...

    async def decr(self, key: KeyT, value: int, /, noreply: bool = False) -> int | None:
        """
        Decrement the numeric value of a key in memcached.

        :param key: The key whose value should be decremented.
        :param value: The amount by which to decrement the current value.
        :param noreply: If True, do not wait for a reply from the server.
        :return: The new value after decrementing, or None if noreply is True.
        """
        command = DecrCommand(key, value, noreply)
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def delete(self, key: KeyT, /) -> bool: ...

    @overload
    async def delete(self, key: KeyT, /, noreply: bool = ...) -> bool | None: ...

    async def delete(self, key: KeyT, /, noreply: bool = False) -> bool | None:
        """
        Delete an item from memcached.

        :param key: The key of the item to be deleted.
        :param noreply: If True, do not wait for a reply from the server.
        :return: True if the deletion was successful, False otherwise; returns None if noreply is True.
        """
        command = DeleteCommand(key, noreply=noreply)
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    @overload
    async def touch(self, key: KeyT, expiry: int, /) -> bool: ...

    @overload
    async def touch(self, key: KeyT, expiry: int, /, noreply: bool = ...) -> bool | None: ...

    async def touch(self, key: KeyT, expiry: int, /, noreply: bool = False) -> bool | None:
        """
        Update the expiration time for an existing key without modifying its value.

        :param key: The key to update.
        :param expiry: The new expiration time in seconds.
        :param noreply: If True, do not wait for a reply from the server.
        :return: True if the expiration time was updated successfully, False otherwise; returns None if noreply is True.
        """
        command = TouchCommand(key, expiry=expiry, noreply=noreply)
        await self.execute_command(command)
        if noreply:
            return None
        return await command.response

    async def flushall(self, expiry: int = 0, /) -> bool:
        """
        Invalidate all existing items in memcached.

        :param expiry: Delay (in seconds) before flushing all items (default: 0).
        :return: True if the flush operation succeeded, False otherwise.
        """
        command = FlushAllCommand(expiry=expiry)
        await self.execute_command(command)
        return await command.response

    async def stats(self, arg: str | None = None) -> dict[AnyStr, AnyStr]:
        """
        Retrieve server statistics from memcached.

        :param arg: An optional argument to specify a subset of statistics.
        :return: A dictionary of statistic keys and their corresponding values.
        """
        command = StatsCommand[AnyStr](
            arg, decode_responses=self.decode_responses, encoding=self.encoding
        )
        await self.execute_command(command)
        return await command.response

    async def version(self) -> str:
        """
        Retrieve the memcached server version.

        :return: A string representing the version of the memcached server.
        """
        command = VersionCommand(noreply=False)
        await self.execute_command(command)
        return await command.response

    def __del__(self) -> None:
        """
        Clean up the client by closing the connection pool.
        """
        if pool := getattr(self, "connection_pool", None):
            pool.close()
