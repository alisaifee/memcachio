from __future__ import annotations

import abc
import copy
from io import BytesIO
from typing import AnyStr, ClassVar, Generic, Self, Sequence, TypeVar, cast

from memcachio.constants import LINE_END, Commands, Responses
from memcachio.errors import ClientError, MemcachedError, NotEnoughData
from memcachio.types import KeyT, MemcachedItem, ValueT
from memcachio.utils import bytestr, decodedstr

R = TypeVar("R")


class Command(abc.ABC, Generic[R]):
    __slots__ = ("noreply", "_keys")
    name: ClassVar[Commands]
    readonly: ClassVar[bool] = False

    def __init__(self, *keys: KeyT, noreply: bool = False):
        self.noreply = noreply
        self._keys: list[str] = [decodedstr(key) for key in keys or []]

    def build(self) -> bytes:
        request = self.name.value
        if request_params := self.build_request_parameters():
            request += b" " + request_params
        request += LINE_END
        return request

    def merge(self, responses: list[R]) -> R:
        return responses[0]

    def _check_header(self, header: bytes) -> None:
        if not header.endswith(LINE_END):
            raise NotEnoughData(len(header))
        response = header.rstrip()
        if response.startswith(Responses.CLIENT_ERROR):
            raise ClientError(decodedstr(response.split(Responses.CLIENT_ERROR)[1]).strip())
        if response.startswith(Responses.ERROR):
            raise MemcachedError(decodedstr(response).strip())
        return None

    @property
    def keys(self) -> list[str]:
        return self._keys

    def subset(self, keys: Sequence[KeyT]) -> Self:
        subset = copy.copy(self)
        subset._keys = list(decodedstr(key) for key in keys)
        return subset

    @abc.abstractmethod
    def build_request_parameters(self) -> bytes: ...

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
    __slots__ = ("items", "decode_responses", "encoding")
    name = Commands.GET
    readonly = True

    def __init__(self, *keys: KeyT, decode: bool = False, encoding: str = "utf-8") -> None:
        self.items: list[MemcachedItem[AnyStr]] = []
        self.decode_responses = decode
        self.encoding = encoding
        super().__init__(*keys, noreply=False)

    def build_request_parameters(self) -> bytes:
        return f"{' '.join(self.keys)}".encode()

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

    def build_request_parameters(self) -> bytes:
        return f"{self.expiry} {' '.join(self.keys)}".encode()


class GatsCommand(GatCommand[AnyStr]):
    name = Commands.GATS
    readonly = False


class GenericStoreCommand(BasicResponseCommand):
    __slots__ = ("encoding", "flags", "expiry", "value", "cas")

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

    def build_request_parameters(self) -> bytes:
        header = f"{decodedstr(self.keys[0])} {self.flags or 0} {self.expiry}"
        header += f" {len(self.value)}"
        if self.cas is not None:
            header += f" {self.cas}"
        if self.noreply:
            header += " noreply"
        header += "\r\n"
        packet = header.encode(self.encoding)
        packet += self.value
        return packet


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

    def build_request_parameters(self) -> bytes:
        request = f"{decodedstr(self.keys[0])} {self.amount}"
        if self.noreply:
            request += " noreply"
        return request.encode("utf-8")

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

    def build_request_parameters(self) -> bytes:
        request = bytestr(self.keys[0])
        if self.noreply:
            request += b" noreply"
        return request


class TouchCommand(BasicResponseCommand):
    __slots__ = ("expiry",)
    name = Commands.TOUCH
    success = Responses.TOUCHED

    def __init__(self, key: KeyT, *, expiry: int, noreply: bool) -> None:
        self.expiry = expiry
        super().__init__(key, noreply=noreply)

    def build_request_parameters(self) -> bytes:
        request = f"{self.keys[0]} {self.expiry}".encode("utf-8")
        if self.noreply:
            request += b" noreply"
        return request


class FlushAllCommand(BasicResponseCommand):
    __slots__ = ("expiry",)
    name = Commands.FLUSH_ALL
    success = Responses.OK

    def __init__(self, expiry: int, noreply: bool) -> None:
        self.expiry = expiry
        super().__init__(noreply=noreply)

    def build_request_parameters(self) -> bytes:
        return bytestr(self.expiry)

    def merge(self, results: list[bool]) -> bool:
        return all(results)


class VersionCommand(Command[str]):
    name = Commands.VERSION

    def build_request_parameters(self) -> bytes:
        return b""

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

    def build_request_parameters(self) -> bytes:
        return b"" if not self.arg else bytestr(self.arg)

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
