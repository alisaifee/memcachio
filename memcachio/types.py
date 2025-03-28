from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import AnyStr, Generic, NamedTuple, Sequence, TypeGuard, cast

KeyT = str | bytes
ValueT = str | bytes | int

UnixSocketLocator = str | Path


class TCPLocator(NamedTuple):
    host: str
    port: int


SingleServerLocator = UnixSocketLocator | TCPLocator | tuple[str, int]
ServerLocator = SingleServerLocator | Sequence[SingleServerLocator]


def is_single_server(locator: ServerLocator) -> TypeGuard[SingleServerLocator]:
    if isinstance(locator, (UnixSocketLocator, TCPLocator)):
        return True
    if (
        isinstance(locator, Sequence)
        and len(locator) == 2
        and isinstance(locator[0], str)
        and isinstance(locator[1], int)
    ):
        return True
    return False


def normalize_single_server_locator(locator: SingleServerLocator) -> SingleServerLocator:
    if not isinstance(locator, UnixSocketLocator):
        return TCPLocator(*locator)
    return locator


def normalize_locator(locator: ServerLocator) -> ServerLocator:
    if is_single_server(locator):
        return normalize_single_server_locator(locator)
    else:
        return [
            normalize_single_server_locator(single)
            for single in cast(Sequence[SingleServerLocator], locator)
        ]


@dataclass
class MemcachedItem(Generic[AnyStr]):
    key: AnyStr
    flags: int
    size: int
    cas: int | None
    value: AnyStr
