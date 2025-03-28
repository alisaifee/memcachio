from __future__ import annotations


class MemcachedError(Exception):
    pass


class ClientError(MemcachedError):
    pass


class NotEnoughData(Exception):
    def __init__(self, data_read: int):
        self.data_read = data_read
        super().__init__()
