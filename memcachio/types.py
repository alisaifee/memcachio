from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import AnyStr, Generic, NamedTuple, TypeGuard, cast

#: Acceptable types for a memcached key
KeyT = str | bytes
#: Acceptable types for values to store
ValueT = str | bytes | int


class TCPLocator(NamedTuple):
    """
    Location of a memcached server listening on a tcp port
    """

    #: IPV4/6 host address
    host: str
    #: IPV4/6 port
    port: int


#: Path to a memcached server listening on a UDS socket
UnixSocketLocator = str | Path

#: The total description of a single memcached instance
SingleMemcachedInstanceLocator = UnixSocketLocator | TCPLocator | tuple[str, int]
#: The total description of either a single memcached instance or a memcached cluster
MemcachedLocator = SingleMemcachedInstanceLocator | Sequence[SingleMemcachedInstanceLocator]


@dataclass
class MemcachedItem(Generic[AnyStr]):
    """
    Data class returned by retrieval commands such as
    :meth:`~memcachio.Client.get`, :meth:`~memcachio.Client.gets`,
    :meth:`~memcachio.Client.gat` and :meth:`~memcachio.Client.gats`
    """

    #: The key of the item
    key: AnyStr
    #: Any flags set on the item
    flags: int
    #: The size (in bytes) of the data stored in the item
    size: int
    #: The CAS value for the item if retrieved
    cas: int | None
    #: The data value of the item
    value: AnyStr


def is_single_server(locator: MemcachedLocator) -> TypeGuard[SingleMemcachedInstanceLocator]:
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


def normalize_single_server_locator(
    locator: SingleMemcachedInstanceLocator,
) -> SingleMemcachedInstanceLocator:
    if not isinstance(locator, UnixSocketLocator):
        return TCPLocator(*locator)
    return locator


def normalize_locator(locator: MemcachedLocator) -> MemcachedLocator:
    if is_single_server(locator):
        return normalize_single_server_locator(locator)
    else:
        return [
            normalize_single_server_locator(single)
            for single in cast(Sequence[SingleMemcachedInstanceLocator], locator)
        ]
