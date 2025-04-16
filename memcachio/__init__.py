"""memcachio

async memcached client
"""

from __future__ import annotations

from . import _version, defaults, errors
from .authentication import Authenticator, SimpleAuthenticator
from .client import Client
from .connection import (
    BaseConnection,
    ConnectionMetrics,
    ConnectionParams,
    TCPConnection,
    UnixSocketConnection,
)
from .pool import (
    ClusterPool,
    EndpointHealthcheckConfig,
    EndpointStatus,
    Pool,
    PoolMetrics,
    SingleServerPool,
)
from .types import (
    AWSAutoDiscoveryEndpoint,
    MemcachedEndpoint,
    MemcachedItem,
    TCPEndpoint,
    UnixSocketEndpoint,
)

__all__ = [
    "Authenticator",
    "AWSAutoDiscoveryEndpoint",
    "BaseConnection",
    "Client",
    "ClusterPool",
    "ConnectionParams",
    "ConnectionMetrics",
    "MemcachedItem",
    "MemcachedEndpoint",
    "EndpointStatus",
    "EndpointHealthcheckConfig",
    "Pool",
    "PoolMetrics",
    "SimpleAuthenticator",
    "SingleServerPool",
    "TCPConnection",
    "TCPEndpoint",
    "UnixSocketConnection",
    "UnixSocketEndpoint",
    "defaults",
    "errors",
]
__version__ = _version.get_versions()["version"]
