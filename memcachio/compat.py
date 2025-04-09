from __future__ import annotations

import sys

if sys.version_info[:2] < (3, 11):
    from async_timeout import timeout as asyncio_timeout
    from typing_extensions import NotRequired, Self, Unpack
else:
    from asyncio import timeout as asyncio_timeout
    from typing import NotRequired, Self, Unpack

__all__ = ["asyncio_timeout", "NotRequired", "Self", "Unpack"]
