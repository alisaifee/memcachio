from __future__ import annotations

import abc
import asyncio
import copy
import dataclasses
import weakref
from asyncio import Future
from collections.abc import Sequence
from io import BytesIO
from typing import AnyStr, ClassVar, Generic, Self, TypeVar, cast

from .constants import LINE_END, Commands, Responses
from .errors import ClientError, MemcachedError, NotEnoughData, ServerError
from .types import KeyT, MemcachedItem, ValueT
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
    __slots__ = ("__weakref__", "_keys", "noreply", "request_sent", "response")
    name: ClassVar[Commands]
    readonly: ClassVar[bool] = False
    request_sent: Future[bool]
    response: Future[R]

    def __init__(self, *keys: KeyT, noreply: bool = False):
        self.noreply = noreply
        self._keys: list[str] = [decodedstr(key) for key in keys or []]
        self.request_sent = asyncio.get_running_loop().create_future()
        self.response = asyncio.get_running_loop().create_future()

    def merge(self, responses: list[R]) -> R:
        return responses[0]

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

    @property
    def keys(self) -> list[str]:
        return self._keys

    def clone(self, keys: Sequence[KeyT]) -> Self:
        subset = copy.copy(self)
        subset._keys = list(decodedstr(key) for key in keys)
        subset.request_sent = asyncio.get_running_loop().create_future()
        subset.response = asyncio.get_running_loop().create_future()
        return subset

    @abc.abstractmethod
    def build_request(self) -> Request[R]: ...

    @abc.abstractmethod
    def parse(self, data: BytesIO) -> R: ...


class BasicResponseCommand(Command[bool]):
    success: ClassVar[Responses]

    def parse(self, data: BytesIO) -> bool:
        response = data.readline()
        self._check_header(response)
        if not response.rstrip() == self.success.value:
            return False
        return True


class GetCommand(Command[dict[AnyStr, MemcachedItem[AnyStr]]]):
    __slots__ = ("decode_responses", "encoding", "items")
    name = Commands.GET
    readonly = True

    def __init__(self, *keys: KeyT, decode: bool = False, encoding: str = "utf-8") -> None:
        self.items: list[MemcachedItem[AnyStr]] = []
        self.decode_responses = decode
        self.encoding = encoding
        super().__init__(*keys, noreply=False)

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), f"{' '.join(self.keys)}".encode())

    def parse(self, data: BytesIO) -> dict[AnyStr, MemcachedItem[AnyStr]]:
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
    readonly = False

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
    readonly = False


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

    def parse(self, data: BytesIO) -> int | None:
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


class VersionCommand(Command[str]):
    name = Commands.VERSION

    def build_request(self) -> Request[R]:
        return Request(weakref.proxy(self), b"")

    def parse(self, data: BytesIO) -> str:
        response = data.readline()
        self._check_header(response)
        return response.partition(Responses.VERSION)[-1].strip().decode("utf-8")


class StatsCommand(Command[dict[AnyStr, AnyStr]]):
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

    def parse(self, data: BytesIO) -> dict[AnyStr, AnyStr]:
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
        return stats
