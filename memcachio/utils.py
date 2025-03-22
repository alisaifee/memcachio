from __future__ import annotations

from .types import ValueT


def decodedstr(value: ValueT, encoding: str = "utf-8") -> str:
    if isinstance(value, bytes):
        return value.decode(encoding)
    if isinstance(value, str):
        return value
    return str(value)


def bytestr(value: ValueT, encoding: str = "utf-8") -> bytes:
    if isinstance(value, bytes):
        return value
    else:
        return str(value).encode(encoding)
