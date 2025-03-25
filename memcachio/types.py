from __future__ import annotations

from dataclasses import dataclass
from typing import AnyStr, Generic, NamedTuple

KeyT = str | bytes
ValueT = str | bytes | int

UnixSocketLocator = str


class TCPLocator(NamedTuple):
    host: str
    port: int


SingleServerLocator = UnixSocketLocator | TCPLocator
ServerLocator = SingleServerLocator | list[SingleServerLocator]


@dataclass
class MemcachedItem(Generic[AnyStr]):
    key: AnyStr
    flags: int
    size: int
    cas: int | None
    value: AnyStr
