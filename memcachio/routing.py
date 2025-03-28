from __future__ import annotations

import bisect
import hashlib
import importlib.util
from typing import Callable

from memcachio.types import SingleServerLocator

MMH3_AVAILABLE = importlib.util.find_spec("mmh3") is not None
if MMH3_AVAILABLE:
    pass


def md5_hasher(key: str) -> int:
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) & 0xFFFFFFFF


class KeyRouter:
    def __init__(
        self,
        nodes: set[SingleServerLocator] | None = None,
        hasher: Callable[[str], int] | None = None,
    ) -> None:
        self.ring: dict[int, SingleServerLocator] = {}
        self._sorted_keys: list[int] = []
        self._hasher = hasher or md5_hasher
        self._spread = 3
        for node in nodes or set():
            self.add_node(node)

    def add_node(self, node: SingleServerLocator) -> None:
        hash_variants = [self._hasher(f"{node}:{i}") for i in range(self._spread)]
        for hash_value in hash_variants:
            self.ring[hash_value] = node
            bisect.insort(self._sorted_keys, hash_value)

    def remove_node(self, node: SingleServerLocator) -> None:
        hash_variants = [self._hasher(f"{node}:{i}") for i in range(3)]
        for hash_value in hash_variants:
            if hash_value in self.ring:
                self._sorted_keys.remove(hash_value)
                del self.ring[hash_value]

    def get_node(self, key: str) -> SingleServerLocator:
        hash_value = self._hasher(key)
        idx = bisect.bisect(self._sorted_keys, hash_value)
        if idx == len(self._sorted_keys):
            idx = 0
        return self.ring[self._sorted_keys[idx]]
