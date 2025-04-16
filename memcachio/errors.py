from __future__ import annotations

from typing import TYPE_CHECKING

from .types import SingleMemcachedInstanceEndpoint

if TYPE_CHECKING:
    pass


class MemcachedError(Exception):
    """
    Base exception for any errors raised by the memcached
    servers
    """

    pass


class ClientError(MemcachedError):
    """
    Raised when memcached responds with ``CLIENT_ERROR``
    """

    pass


class ServerError(MemcachedError):
    """
    Raised when memcached responds with ``SERVER_ERROR``
    """

    pass


class NotEnoughData(Exception):
    """
    :meta private:
    """

    def __init__(self, data_read: int):
        self.data_read = data_read
        super().__init__()


class MemcachioConnectionError(ConnectionError):
    """
    Base exception for any connection errors encountered.
    """

    #: The memcached server where the connection error originated from
    endpoint: SingleMemcachedInstanceEndpoint

    def __init__(self, message: str, endpoint: SingleMemcachedInstanceEndpoint):
        self.endpoint = endpoint
        super().__init__(f"{message or 'Connection error'} (memcached server: {endpoint})")


class ConnectionNotAvailable(MemcachioConnectionError):
    """
    Raised when a connection couldn't be acquired from the pool within
    the configured timeout
    """

    def __init__(self, endpoint: SingleMemcachedInstanceEndpoint, timeout: float):
        message = f"Unable to get a connection from the pool in {timeout} seconds"
        super().__init__(message, endpoint=endpoint)


class NoAvailableNodes(ValueError):
    """
    Raised when no nodes are available in the cluster
    """

    pass


class AutoDiscoveryError(MemcachedError):
    """
    Raised when the response for autodiscovery is invalid
    """

    pass
