from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import AnyStr, Generic, NamedTuple, TypeGuard, cast

#: Acceptable types for a memcached key
KeyT = str | bytes
#: Acceptable types for values to store
ValueT = str | bytes | int


class TCPEndpoint(NamedTuple):
    """
    Location of a memcached server listening on a tcp port
    """

    #: IPV4/6 host address
    host: str
    #: IPV4/6 port
    port: int


#: Path to a memcached server listening on a UDS socket
UnixSocketEndpoint = str | Path


class AWSAutoDiscoveryEndpoint(NamedTuple):
    """
    Location of a memcached auto-discovery endpoint
    for use with AWS Elasticache
    """

    #: IPV4/6 host address
    host: str
    #: IPV4/6 port
    port: int
    #: How often to trigger a refresh to check for updates
    refresh_interval: float


#: The total description of a single memcached instance
SingleMemcachedInstanceEndpoint = UnixSocketEndpoint | TCPEndpoint | tuple[str, int]
#: The total description of either a single memcached instance or a memcached cluster
MemcachedEndpoint = (
    SingleMemcachedInstanceEndpoint
    | Sequence[SingleMemcachedInstanceEndpoint]
    | AWSAutoDiscoveryEndpoint
)


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


def is_single_server(endpoint: MemcachedEndpoint) -> TypeGuard[SingleMemcachedInstanceEndpoint]:
    if isinstance(endpoint, AWSAutoDiscoveryEndpoint):
        return False
    if isinstance(endpoint, (UnixSocketEndpoint, TCPEndpoint)):
        return True
    if (
        isinstance(endpoint, Sequence)
        and len(endpoint) == 2
        and isinstance(endpoint[0], str)
        and isinstance(endpoint[1], int)
    ):
        return True
    return False


def normalize_single_server_endpoint(
    endpoint: SingleMemcachedInstanceEndpoint,
) -> SingleMemcachedInstanceEndpoint:
    if not isinstance(endpoint, UnixSocketEndpoint):
        return TCPEndpoint(*endpoint)
    return endpoint


def normalize_endpoint(endpoint: MemcachedEndpoint) -> MemcachedEndpoint:
    if is_single_server(endpoint):
        return normalize_single_server_endpoint(endpoint)
    elif isinstance(endpoint, AWSAutoDiscoveryEndpoint):
        return endpoint
    else:
        return [
            normalize_single_server_endpoint(single)
            for single in cast(Sequence[SingleMemcachedInstanceEndpoint], endpoint)
        ]
