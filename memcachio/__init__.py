from __future__ import annotations

from .client import Client

__all__ = ["Client"]

from . import _version

__version__ = _version.get_versions()["version"]
