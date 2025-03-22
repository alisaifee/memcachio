from __future__ import annotations

from typing import TYPE_CHECKING

from memcachio.utils import decodedstr

if TYPE_CHECKING:
    from collections.abc import Sequence

    from memcachio.types import KeyT


class MemcachedError(Exception):
    pass


class ClientError(MemcachedError):
    pass


class NotStored(MemcachedError):
    def __init__(self, keys: Sequence[KeyT]) -> None:
        super().__init__(f"{' '.join([decodedstr(k) for k in keys])} not stored")


class NotEnoughData(Exception):
    def __init__(self, data_read: int):
        self.data_read = data_read
        super().__init__()
