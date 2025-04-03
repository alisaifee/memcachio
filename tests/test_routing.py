from __future__ import annotations

import hashlib
import uuid
from collections import Counter

import mmh3
import pytest

from memcachio.errors import NoAvailableNodes
from memcachio.routing import KeyRouter


@pytest.mark.parametrize(
    "hasher",
    [
        None,
        mmh3.hash,
        lambda key: int.from_bytes(hashlib.blake2s(key.encode()).digest(), "big"),
    ],
    ids=["default(md5)", "mmh3", "blake2s"],
)
class TestRouting:
    def test_empty_router(self, hasher):
        router = KeyRouter(set(), hasher)
        with pytest.raises(NoAvailableNodes):
            router.get_node("fubar")

    def test_single_node(self, hasher):
        router = KeyRouter(hasher=hasher)
        router.add_node("/var/tmp/socket")
        assert {"/var/tmp/socket"} == {router.get_node(f"key{i}") for i in range(4096)}

    @pytest.mark.parametrize(
        "nodes",
        [
            ("/var/tmp/1", "/var/tmp/2"),
            (("localhost", 11211), ("localhost", 11212)),
            (("localhost", 11211), ("localhost", 11212), "/var/tmp/socket"),
        ],
    )
    def test_multiple_nodes(self, nodes, hasher):
        router = KeyRouter(set(nodes), hasher=hasher)
        counter = Counter(router.get_node(f"key{i}") for i in range(4096))
        for node in nodes:
            assert 1.0 / len(nodes) == pytest.approx(counter[node] / 4096, 1e-1)

    @pytest.mark.parametrize(
        "nodes",
        [
            ("/var/tmp/1", "/var/tmp/2", "/var/tmp/3"),
            (("localhost", 11211), ("localhost", 11212), ("localhost", 11213)),
        ],
    )
    def test_multiple_nodes_with_removal(self, nodes, hasher):
        router = KeyRouter(set(nodes), hasher=hasher)
        mapping = {}
        for i in range(4096):
            mapping.setdefault(router.get_node(f"key{i}"), set()).add(i)
        router.remove_node(nodes[-1])
        new_mapping = {}
        for i in range(4096):
            new_mapping.setdefault(router.get_node(f"key{i}"), set()).add(i)
        assert mapping[nodes[0]].issubset(new_mapping[nodes[0]])
        assert mapping[nodes[1]].issubset(new_mapping[nodes[1]])

        moved_to_node_0 = mapping[nodes[2]].intersection(new_mapping[nodes[0]])
        moved_to_node_1 = mapping[nodes[2]].intersection(new_mapping[nodes[1]])
        assert pytest.approx(len(moved_to_node_0) / 4096, 1e-1) == pytest.approx(
            len(moved_to_node_1) / 4096, 1e-1
        )

    @pytest.mark.benchmark(group="router")
    def test_hash_performance(self, benchmark, hasher):
        nodes = {("localhost", i) for i in range(10)}
        keys = [uuid.uuid4().hex for _ in range(pow(2, 9))]
        router = KeyRouter(nodes, hasher=hasher)
        benchmark(lambda: (router.get_node(key) for key in keys))
