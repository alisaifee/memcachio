from __future__ import annotations

from . import _version
from .client import Client
from .types import ServerLocator, TCPLocator

__all__ = ["Client", "TCPLocator", "ServerLocator"]
__version__ = _version.get_versions()["version"]
