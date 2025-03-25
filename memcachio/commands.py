from __future__ import annotations

import abc
import copy
from typing import TYPE_CHECKING, AnyStr, ClassVar, Generic, Self, Sequence, TypeVar, cast

from memcachio.constants import LINE_END, Commands, Responses
from memcachio.errors import ClientError, MemcachedError, NotEnoughData
from memcachio.types import KeyT, MemcachedItem, ValueT
from memcachio.utils import bytestr, decodedstr

if TYPE_CHECKING:
    from io import BytesIO

R = TypeVar("R")


class Command(abc.ABC, Generic[R]):
    name: ClassVar[Commands]
    __slots__ = ("noreply",)

    def __init__(self, noreply: bool):
        self.noreply = noreply

    def build(self) -> bytes:
        request = self.name.value
        if request_params := self.build_request_parameters():
            request += b" " + request_params
        request += LINE_END
        return request

    @abc.abstractmethod
    def build_request_parameters(self) -> bytes: ...

    @abc.abstractmethod
    def parse(self, data: BytesIO) -> R: ...

    def merge(self, responses: list[R]) -> R:
        return responses[0]

    def check_header(self, header: bytes) -> None:
        if not header.endswith(LINE_END):
            raise NotEnoughData(len(header))
        response = header.rstrip()
        if response.startswith(Responses.CLIENT_ERROR):
            raise ClientError(decodedstr(response.split(Responses.CLIENT_ERROR)[1]).strip())
        if response.startswith(Responses.ERROR):
            raise MemcachedError(decodedstr(response).strip())
        return None


class MultiKeyCommand(Command[R]):
    __slots__ = ("_keys",)

    def __init__(self, keys: tuple[KeyT, ...], noreply: bool):
        self._keys = keys
        super().__init__(noreply)

    @property
    def keys(self) -> list[str]:
        return [decodedstr(k) for k in self._keys]

    def subset(self, keys: Sequence[KeyT]) -> Self:
        subset = copy.copy(self)
        subset._keys = tuple(keys)
        return subset


class SingleKeyCommand(Command[R]):
    __slots__ = ("_key",)

    def __init__(self, key: KeyT, noreply: bool):
        self._key = key
        super().__init__(noreply)

    @property
    def key(self) -> str:
        return decodedstr(self._key)

    def build_request_parameters(self) -> bytes:
        assert self.key
        return bytestr(self.key) + LINE_END


class NoKeyCommand(Command[R]):
    pass


class BasicResponseCommand(Command[bool]):
    success: ClassVar[Responses]

    def parse(self, data: BytesIO) -> bool:
        response = data.readline()
        self.check_header(response)
        return response.rstrip() == self.success.value


class GetCommand(MultiKeyCommand[dict[AnyStr, MemcachedItem[AnyStr]]]):
    name = Commands.GET
    __slots__ = ("items", "decode_responses", "encoding")

    def __init__(
        self, keys: tuple[KeyT, ...], *, decode: bool = False, encoding: str = "utf-8"
    ) -> None:
        self.items: list[MemcachedItem[AnyStr]] = []
        self.decode_responses = decode
        self.encoding = encoding
        super().__init__(keys, noreply=False)

    def build_request_parameters(self) -> bytes:
        return f"{' '.join(self.keys)}".encode()

    def parse(self, data: BytesIO) -> dict[AnyStr, MemcachedItem[AnyStr]]:
        while True:
            header = data.readline()
            self.check_header(header)
            if header == (Responses.END + LINE_END):
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
            item = MemcachedItem[AnyStr](
                cast(AnyStr, decodedstr(key, self.encoding) if self.decode_responses else key),
                flags,
                size,
                cas,
                cast(AnyStr, decodedstr(value, self.encoding) if self.decode_responses else value),
            )
            if len(value) != size:
                raise NotEnoughData(len(value) + len(header))
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
    name = Commands.GAT

    def __init__(
        self,
        keys: tuple[KeyT, ...],
        exptime: int,
        *,
        decode: bool = False,
        encoding: str = "utf-8",
    ) -> None:
        self.exptime = exptime
        super().__init__(keys, decode=decode, encoding=encoding)

    def build_request_parameters(self) -> bytes:
        return f"{self.exptime} {' '.join(self.keys)}".encode()


class GatsCommand(GatCommand[AnyStr]):
    name = Commands.GATS


class GenericStoreCommand(BasicResponseCommand, SingleKeyCommand[bool]):
    def __init__(
        self,
        key: KeyT,
        value: ValueT,
        flags: int,
        exptime: int,
        noreply: bool,
        /,
        cas: int | None = None,
        encoding: str = "utf-8",
    ) -> None:
        self.encoding = encoding
        self.flags = flags
        self.exptime = exptime
        self.value = bytestr(value, self.encoding)
        self.cas = cas
        super().__init__(key, noreply)

    def build_request_parameters(self) -> bytes:
        header = f"{decodedstr(self.key)} {self.flags} {self.exptime}"
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


class ArithmenticCommand(SingleKeyCommand[int | None]):
    def __init__(self, key: KeyT, amount: int, noreply: bool) -> None:
        self._amount = amount
        super().__init__(key, noreply)

    def build_request_parameters(self) -> bytes:
        request = f"{decodedstr(self._key)} {self._amount}"
        if self.noreply:
            request += " noreply"
        return request.encode("utf-8")

    def parse(self, data: BytesIO) -> int | None:
        response = data.readline()
        self.check_header(response)
        response = response.rstrip()
        if response == Responses.NOT_FOUND:
            return None
        return int(response)


class IncrCommand(ArithmenticCommand):
    name = Commands.INCR


class DecrCommand(ArithmenticCommand):
    name = Commands.DECR


class DeleteCommand(BasicResponseCommand, SingleKeyCommand[bool]):
    name = Commands.DELETE
    success = Responses.DELETED


class TouchCommand(BasicResponseCommand, SingleKeyCommand[bool]):
    name = Commands.TOUCH
    success = Responses.TOUCHED


class FlushAllCommand(BasicResponseCommand, NoKeyCommand[bool]):
    name = Commands.FLUSH_ALL
    success = Responses.OK

    def __init__(self, exptime: int, noreply: bool) -> None:
        self.exptime = exptime
        super().__init__(noreply)

    def build_request_parameters(self) -> bytes:
        return bytestr(self.exptime)

    def merge(self, results: list[bool]) -> bool:
        return all(results)


class VersionCommand(NoKeyCommand[str]):
    name = Commands.VERSION

    def build_request_parameters(self) -> bytes:
        return b""

    def parse(self, data: BytesIO) -> str:
        response = data.readline()
        self.check_header(response)
        return response.partition(Responses.VERSION)[-1].strip().decode("utf-8")


class StatsCommand(NoKeyCommand[dict[AnyStr, AnyStr]]):
    name = Commands.STATS

    def __init__(
        self, arg: str | None = None, *, decode_responses: bool = False, encoding: str = "utf-8"
    ):
        self.arg = arg
        self.decode_responses = decode_responses
        self.encoding = encoding
        super().__init__(False)

    def build_request_parameters(self) -> bytes:
        return b"" if not self.arg else bytestr(self.arg)

    def parse(self, data: BytesIO) -> dict[AnyStr, AnyStr]:
        stats = {}
        while True:
            section = data.readline()
            self.check_header(section)
            if section.startswith(Responses.END):
                break
            elif section.startswith(Responses.STAT):
                part = section.lstrip(Responses.STAT).strip()
                item, value = (decodedstr(part) if self.decode_responses else part).split()
                stats[cast(AnyStr, item)] = cast(AnyStr, value)
        return stats
