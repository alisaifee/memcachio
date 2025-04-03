"""memcachio

async memcached client
"""

from __future__ import annotations

from . import _version, defaults, errors
from .client import Client
from .connection import BaseConnection, ConnectionParams, TCPConnection, UnixSocketConnection
from .pool import ClusterPool, EndpointHealthcheckConfig, EndpointStatus, Pool, SingleServerPool
from .types import MemcachedEndpoint, MemcachedItem, TCPEndpoint

__all__ = [
    "BaseConnection",
    "Client",
    "ClusterPool",
    "ConnectionParams",
    "MemcachedItem",
    "MemcachedEndpoint",
    "EndpointStatus",
    "EndpointHealthcheckConfig",
    "Pool",
    "SingleServerPool",
    "TCPConnection",
    "TCPEndpoint",
    "UnixSocketConnection",
    "defaults",
    "errors",
]
__version__ = _version.get_versions()["version"]
