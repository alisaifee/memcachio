"""memcachio

async memcached client
"""

from __future__ import annotations

from . import _version, defaults, errors
from .client import Client
from .connection import BaseConnection, TCPConnection, UnixSocketConnection
from .pool import ClusterPool, Pool, SingleServerPool
from .types import MemcachedItem, MemcachedLocator, TCPLocator

__all__ = [
    "BaseConnection",
    "Client",
    "ClusterPool",
    "MemcachedItem",
    "MemcachedLocator",
    "Pool",
    "SingleServerPool",
    "TCPConnection",
    "TCPLocator",
    "UnixSocketConnection",
    "defaults",
    "errors",
]
__version__ = _version.get_versions()["version"]
