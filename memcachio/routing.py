from __future__ import annotations

import hashlib
from collections.abc import Callable

from .errors import NoAvailableNodes
from .types import SingleMemcachedInstanceEndpoint


def md5_hasher(key: str) -> int:
    return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)


class KeyRouter:
    def __init__(
        self,
        nodes: set[SingleMemcachedInstanceEndpoint] | None = None,
        hasher: Callable[[str], int] | None = None,
    ) -> None:
        """
        Rendezvous Hashing implementation to route keys to memcached
        nodes.

        :param nodes: set of memcached nodes that are candidates
        :param hasher: function to use to hash a key to a node. If not
         provided, a default implementation using :func:`hashlib.md5` from
         the standard library will be used.
        """
        self._hasher = hasher or md5_hasher
        self.nodes: set[SingleMemcachedInstanceEndpoint] = nodes or set()

    def add_node(self, node: SingleMemcachedInstanceEndpoint) -> None:
        """
        Add a node to the set of candidate nodes
        """
        self.nodes.add(node)

    def remove_node(self, node: SingleMemcachedInstanceEndpoint) -> None:
        """
        Remove a node from the set of candidate nodes
        """
        self.nodes.discard(node)

    def get_node(self, key: str) -> SingleMemcachedInstanceEndpoint:
        """
        Get the node associated with ``key``
        """
        if not self.nodes:
            raise NoAvailableNodes()
        return max(self.nodes, key=lambda node: self._hasher(f"{node}:{key}"))
