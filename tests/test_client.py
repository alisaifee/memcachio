from __future__ import annotations

import memcachio
from memcachio.pool import ClusterPool, SingleServerPool
from memcachio.types import TCPLocator


class TestClient:
    async def test_construction_with_single_tcp_locator(self, memcached_1):
        client = memcachio.Client(TCPLocator(*memcached_1))
        assert isinstance(client.connection_pool, SingleServerPool)

    async def test_construction_with_multiple_tcp_locators(self, memcached_1, memcached_2):
        client = memcachio.Client([TCPLocator(*memcached_1), TCPLocator(*memcached_2)])
        assert isinstance(client.connection_pool, ClusterPool)
