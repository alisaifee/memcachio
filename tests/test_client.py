from __future__ import annotations

import asyncio

import memcachio
from memcachio.commands import Command
from memcachio.pool import ClusterPool, Pool, R, SingleServerPool
from memcachio.types import TCPLocator


class TestClient:
    async def test_construction_with_single_tcp_locator(self, memcached_1):
        client = memcachio.Client(TCPLocator(*memcached_1))
        assert isinstance(client.connection_pool, SingleServerPool)

    async def test_construction_with_multiple_tcp_locators(self, memcached_1, memcached_2):
        client = memcachio.Client([TCPLocator(*memcached_1), TCPLocator(*memcached_2)])
        assert isinstance(client.connection_pool, ClusterPool)

    async def test_client_from_custom_pool(self, memcached_1):
        class MyPool(Pool):
            async def close(self) -> None:
                pass

            async def execute_command(self, command: Command[R]) -> R:
                future = asyncio.Future()
                future.set_result(True)
                return future

        client = memcachio.Client(connection_pool=MyPool(memcached_1))
        assert await client.set("fubar", 1)
