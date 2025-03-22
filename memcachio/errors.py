from __future__ import annotations

from typing import TYPE_CHECKING

from .types import MemcachedLocator

if TYPE_CHECKING:
    from .pool import Pool


class MemcachedError(Exception):
    pass


class ClientError(MemcachedError):
    pass


class ServerError(MemcachedError):
    pass


class NotEnoughData(Exception):
    def __init__(self, data_read: int):
        self.data_read = data_read
        super().__init__()


class MemcachioConnectionError(ConnectionError):
    def __init__(self, message: str, instance: MemcachedLocator):
        self.instance = instance
        super().__init__(f"{message or 'Connection error'} (memcached instance: {instance})")


class ConnectionNotAvailable(MemcachioConnectionError):
    def __init__(self, pool: Pool, timeout: float):
        message = f"Unable to get a connection from the pool to {pool.locator} in {timeout} seconds"
        super().__init__(message, instance=pool.locator)
