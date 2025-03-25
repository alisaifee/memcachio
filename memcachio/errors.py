from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class MemcachedError(Exception):
    pass


class ClientError(MemcachedError):
    pass


class NotEnoughData(Exception):
    def __init__(self, data_read: int):
        self.data_read = data_read
        super().__init__()
