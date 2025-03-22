from __future__ import annotations

import hashlib
from collections.abc import Callable

from .types import SingleMemcachedInstanceLocator


def md5_hasher(key: str) -> int:
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


class KeyRouter:
    def __init__(
        self,
        nodes: set[SingleMemcachedInstanceLocator] | None = None,
        hasher: Callable[[str], int] | None = None,
    ) -> None:
        self._hasher = hasher or md5_hasher
        self.nodes: set[SingleMemcachedInstanceLocator] = nodes or set()

    def add_node(self, node: SingleMemcachedInstanceLocator) -> None:
        self.nodes.add(node)

    def remove_node(self, node: SingleMemcachedInstanceLocator) -> None:
        self.nodes.discard(node)

    def get_node(self, key: str) -> SingleMemcachedInstanceLocator:
        return max(self.nodes, key=lambda node: self._hasher(f"{node}:{key}"))
