from __future__ import annotations

import abc
import asyncio
import copy
import dataclasses
import time
import weakref
from asyncio import Future
from collections import ChainMap
from collections.abc import Sequence
from io import BytesIO
from typing import AnyStr, ClassVar, Generic, TypeVar, cast

from .compat import Self
from .constants import LINE_END, Commands, Responses
from .errors import (
    AutoDiscoveryError,
    ClientError,
    MemcachedError,
    NotEnoughData,
    ServerError,
)
from .types import KeyT, MemcachedItem, SingleMemcachedInstanceEndpoint, TCPEndpoint, ValueT
from .utils import bytestr, decodedstr

R = TypeVar("R")


@dataclasses.dataclass
class Request(Generic[R]):
    command: weakref.ProxyType[Command[R]]
    header: bytes
    body: list[bytes] = dataclasses.field(default_factory=lambda: [])

    def __bytes__(self) -> bytes:
        request_body: bytes = self.command.name
        if self.header:
            request_body += b" %b" % self.header
        if self.command.noreply:
            request_body += b" noreply"
        request_body += LINE_END
        request_body += LINE_END.join(self.body)
        return request_body


class Command(abc.ABC, Generic[R]):
    """
    The abstract generic command class used throughout the
    client -> connection pool -> connection lifecycle to
    manage dispatching a request, parsing the response (if not
    a ``noreply`` request) and resolving the future to receive
    the eventual parsed response.

    This class is not meant for direct use, however it's structure
    might be interesting if implementing a custom connection pool or
    using the connection classes directly.

    As an example the following snippet demonstrates creating a
    ``set`` command that takes no flags or expiry and is dispatched
    directly to a connection without using :class:`~memcachio.Client`
    or a :class:`~memcachio.pool.Pool`::

        import asyncio
        import weakref
        from io import BytesIO

        import memcachio
        from memcachio.commands import Command, Request
        from memcachio.constants import Commands


        class MyCustomSetCommand(Command[bool]):
            name = Commands.SET

            def __init__(self, key: str, value: bytes, noreply: bool = False):
                self.value = value
                super().__init__(key, noreply=noreply)

            def build_request(self) -> memcachio.commands.Request[bool]:
                return Request(
                    weakref.proxy(self),
                    b"%b 0 0 %d" % (self.keys[0].encode(), len(self.value)),
                    [self.value + b"\\r\\n"],
                )

            def parse(self, data: BytesIO, endpoint: SingleMemcachedInstanceEndpoint) -> bool:
                response = data.readline()
                return response.rstrip() == b"STORED"


        async def example():
            command = MyCustomSetCommand("fubar", b"1234")
            connection = memcachio.TCPConnection(("localhost", 11211))
            await connection.connect()
            connection.create_request(command)
            assert await command.response


        asyncio.run(example())

    """

    __slots__ = (
        "__weakref__",
        "_keys",
        "noreply",
        "request_sent",
        "response",
        "response_time",
        "created_at",
    )

    #: The name of the command
    name: ClassVar[Commands]
    #: A future that should be set when the request has been written
    #:  to the socket
    request_sent: Future[bool]
    #: A future that should be set when the request has received a response
    #:  and parsed
    response: Future[R]
    #:
    response_time: float

    def __init__(self, *keys: KeyT, noreply: bool = False):
        """
        :param keys:  The keys this command operates on
        :param noreply: Whether the server should send a response
         to the command.
        """
        self.noreply = noreply
        self.created_at = time.time()
        self.response_time = 0
        self._keys: list[str] = [decodedstr(key) for key in keys or []]
        self.request_sent = asyncio.get_running_loop().create_future()
        self.response = asyncio.get_running_loop().create_future()
        (self.request_sent if self.noreply else self.response).add_done_callback(
            lambda _: self._update_response_time()
        )

    def merge(self, responses: list[R]) -> R:
        return responses[0]

    @property
    def keys(self) -> list[str]:
        return self._keys

    def clone(self, keys: Sequence[KeyT]) -> Self:
        subset = copy.copy(self)
        subset._keys = list(decodedstr(key) for key in keys)
        subset.request_sent = asyncio.get_running_loop().create_future()
        subset.response = asyncio.get_running_loop().create_future()
        (subset.request_sent if subset.noreply else subset.response).add_done_callback(
            lambda _: subset._update_response_time()
        )
        return subset

    @abc.abstractmethod
    def build_request(self) -> Request[R]:
        """
        Build the header and (optional) payload for the request to
        send to the memcached server
        """
        ...

    @abc.abstractmethod
    def parse(self, data: BytesIO, endpoint: SingleMemcachedInstanceEndpoint) -> R:
        """
        Parse the response from the buffer assuming the
        current position of the buffer contains the response
        for this command.
        """
        ...

    def _update_response_time(self) -> None:
        self.response_time = time.time() - self.created_at

    def _check_header(self, header: bytes) -> None:
        if not header.endswith(LINE_END):
            raise NotEnoughData(len(header))
        response = header.rstrip()
        if response.startswith(Responses.CLIENT_ERROR):
            raise ClientError(decodedstr(response.split(Responses.CLIENT_ERROR)[1]).strip())
        elif response.startswith(Responses.SERVER_ERROR):
            raise ServerError(decodedstr(response.split(Responses.SERVER_ERROR)[1]).strip())
        elif response.startswith(Responses.ERROR):
            raise MemcachedError(decodedstr(response).strip())
        return None


class BasicResponseCommand(Command[bool]):
    success: ClassVar[Responses]

    def parse(self, data: BytesIO, endpoint: SingleMemcachedInstanceEndpoint) -> bool:
        response = data.readline()
        self._check_header(response)
        if not response.rstrip() == self.success.value:
            return False
        return True


class GetCommand(Command[dict[AnyStr, MemcachedItem[AnyStr]]]):
    __slots__ = ("decode_responses", "encoding", "items")
    name = Commands.GET

    def __init__(self, *keys: KeyT, decode: bool = False, encoding: str = "utf-8") -> None:
        self.items: list[MemcachedItem[AnyStr]] = []
        self.decode_responses = decode
        self.encoding = encoding
        super().__init__(*keys, noreply=False)

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), f"{' '.join(self.keys)}".encode())

    def parse(
        self, data: BytesIO, endpoint: SingleMemcachedInstanceEndpoint
    ) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        while True:
            header = data.readline()
            self._check_header(header)
            if header.lstrip() == (Responses.END + LINE_END):
                break
            parts = header.split()
            if len(parts) < 4 or parts[0] != Responses.VALUE:
                msg = f"Unexpected response header: {decodedstr(header)}"
                raise ValueError(msg)
            key = parts[1]
            flags = int(parts[2])
            size = int(parts[3])
            cas = int(parts[4]) if len(parts) > 4 else None
            value = data.read(size)
            if len(value) != size:
                raise NotEnoughData(len(value) + len(header))
            item = MemcachedItem[AnyStr](
                cast(AnyStr, decodedstr(key, self.encoding) if self.decode_responses else key),
                flags,
                size,
                cas,
                cast(AnyStr, decodedstr(value, self.encoding) if self.decode_responses else value),
            )
            data.read(2)
            self.items.append(item)
        return {i.key: i for i in self.items}

    def merge(
        self, results: list[dict[AnyStr, MemcachedItem[AnyStr]]]
    ) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        merged = {}
        for res in results:
            for key, item in res.items():
                merged[key] = item
        return merged


class GetsCommand(GetCommand[AnyStr]):
    name = Commands.GETS


class GatCommand(GetCommand[AnyStr]):
    __slots__ = ("expiry",)
    name = Commands.GAT

    def __init__(
        self,
        *keys: KeyT,
        expiry: int = 0,
        decode: bool = False,
        encoding: str = "utf-8",
    ) -> None:
        self.expiry = expiry
        super().__init__(*keys, decode=decode, encoding=encoding)

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), f"{self.expiry} {' '.join(self.keys)}".encode())


class GatsCommand(GatCommand[AnyStr]):
    name = Commands.GATS


class GenericStoreCommand(BasicResponseCommand):
    __slots__ = ("cas", "encoding", "expiry", "flags", "value")

    def __init__(
        self,
        key: KeyT,
        value: ValueT,
        *,
        flags: int | None = None,
        expiry: int = 0,
        noreply: bool = False,
        cas: int | None = None,
        encoding: str = "utf-8",
    ) -> None:
        self.encoding = encoding
        self.flags = flags
        self.expiry = expiry
        self.value = bytestr(value, self.encoding)
        self.cas = cas
        super().__init__(key, noreply=noreply)

    def build_request(self) -> Request[R]:
        header = f"{decodedstr(self.keys[0])} {self.flags or 0} {self.expiry}"
        header += f" {len(self.value)}"
        if self.cas is not None:
            header += f" {self.cas}"
        return Request(weakref.proxy(self), header.encode(), [self.value + LINE_END])


class SetCommand(GenericStoreCommand):
    name = Commands.SET
    success = Responses.STORED


class CheckAndSetCommand(GenericStoreCommand):
    name = Commands.CAS
    success = Responses.STORED


class AddCommand(GenericStoreCommand):
    name = Commands.ADD
    success = Responses.STORED


class AppendCommand(GenericStoreCommand):
    name = Commands.APPEND
    success = Responses.STORED


class PrependCommand(GenericStoreCommand):
    name = Commands.PREPEND
    success = Responses.STORED


class ReplaceCommand(GenericStoreCommand):
    name = Commands.REPLACE
    success = Responses.STORED


class ArithmenticCommand(Command[int | None]):
    __slots__ = ("amount",)

    def __init__(self, key: KeyT, amount: int, noreply: bool) -> None:
        self.amount = amount
        super().__init__(key, noreply=noreply)

    def build_request(self) -> Request[R]:
        request = f"{decodedstr(self.keys[0])} {self.amount}"
        return Request(weakref.proxy(self), request.encode())

    def parse(self, data: BytesIO, endpoint: SingleMemcachedInstanceEndpoint) -> int | None:
        response = data.readline()
        self._check_header(response)
        response = response.rstrip()
        if response == Responses.NOT_FOUND:
            return None
        return int(response)


class IncrCommand(ArithmenticCommand):
    name = Commands.INCR


class DecrCommand(ArithmenticCommand):
    name = Commands.DECR


class DeleteCommand(BasicResponseCommand):
    name = Commands.DELETE
    success = Responses.DELETED

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), bytestr(self.keys[0]))


class TouchCommand(BasicResponseCommand):
    __slots__ = ("expiry",)
    name = Commands.TOUCH
    success = Responses.TOUCHED

    def __init__(self, key: KeyT, *, expiry: int, noreply: bool = False) -> None:
        self.expiry = expiry
        super().__init__(key, noreply=noreply)

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), f"{self.keys[0]} {self.expiry}".encode())


class FlushAllCommand(BasicResponseCommand):
    __slots__ = ("expiry",)
    name = Commands.FLUSH_ALL
    success = Responses.OK

    def __init__(self, expiry: int) -> None:
        self.expiry = expiry
        super().__init__()

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), bytestr(self.expiry))

    def merge(self, results: list[bool]) -> bool:
        return all(results)


class VersionCommand(Command[dict[SingleMemcachedInstanceEndpoint, str]]):
    name = Commands.VERSION

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), b"")

    def parse(
        self, data: BytesIO, endpoint: SingleMemcachedInstanceEndpoint
    ) -> dict[SingleMemcachedInstanceEndpoint, str]:
        response = data.readline()
        self._check_header(response)
        return {endpoint: response.partition(Responses.VERSION)[-1].strip().decode("utf-8")}

    def merge(
        self, responses: list[dict[SingleMemcachedInstanceEndpoint, str]]
    ) -> dict[SingleMemcachedInstanceEndpoint, str]:
        return dict(ChainMap(*responses))


class StatsCommand(Command[dict[SingleMemcachedInstanceEndpoint, dict[AnyStr, AnyStr]]]):
    name = Commands.STATS

    def __init__(
        self, arg: str | None = None, *, decode_responses: bool = False, encoding: str = "utf-8"
    ):
        self.arg = arg
        self.decode_responses = decode_responses
        self.encoding = encoding
        super().__init__(noreply=False)

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), b"" if not self.arg else bytestr(self.arg))

    def parse(
        self, data: BytesIO, endpoint: SingleMemcachedInstanceEndpoint
    ) -> dict[SingleMemcachedInstanceEndpoint, dict[AnyStr, AnyStr]]:
        stats = {}
        while True:
            section = data.readline()
            self._check_header(section)
            if section.startswith(Responses.END):
                break
            elif section.startswith(Responses.STAT):
                part = section.lstrip(Responses.STAT).strip()
                item, value = (decodedstr(part) if self.decode_responses else part).split()
                stats[cast(AnyStr, item)] = cast(AnyStr, value)
        return {endpoint: stats}

    def merge(
        self, responses: list[dict[SingleMemcachedInstanceEndpoint, dict[AnyStr, AnyStr]]]
    ) -> dict[SingleMemcachedInstanceEndpoint, dict[AnyStr, AnyStr]]:
        return dict(ChainMap(*responses))


class AWSAutoDiscoveryConfig(Command[tuple[int, set[SingleMemcachedInstanceEndpoint]]]):
    name = Commands.CONFIG

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), b"get cluster")

    def parse(
        self, data: BytesIO, endpoint: SingleMemcachedInstanceEndpoint
    ) -> tuple[int, set[SingleMemcachedInstanceEndpoint]]:
        header = data.readline()
        self._check_header(header)
        version = int(data.readline())
        parsed_endpoints = [host.split(b"|") for host in data.readline().strip().split(b" ")]
        data.readline()
        if (data.readline().strip()) != Responses.END:
            raise AutoDiscoveryError("Malformed response")
        return version, {TCPEndpoint(host[1].decode(), int(host[2])) for host in parsed_endpoints}
