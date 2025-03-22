from __future__ import annotations

import asyncio
import uuid

import pytest
from pytest_lazy_fixtures import lf

import memcachio
from memcachio.connection import TCPConnection
from memcachio.pool import SingleServerPool


@pytest.mark.parametrize(
    "locator",
    [pytest.param(lf(target)) for target in ["memcached_1", "memcached_cluster"]],
)
class TestConcurrency:
    async def test_single_concurrent_command(self, locator):
        client = memcachio.Client(locator)
        await client.set("key", 0)
        assert list(range(1, 1025)) == (
            sorted(await asyncio.gather(*[client.incr("key", 1) for _ in range(1024)]))
        )

    @pytest.mark.parametrize("max_connections", (1, 2, 4, 32))
    async def test_max_connection(self, locator, max_connections, mocker):
        client = memcachio.Client(locator, max_connections=max_connections)
        total_max_connections = (
            max_connections
            if isinstance(client.connection_pool, SingleServerPool)
            else max_connections * len(client.connection_pool.nodes)
        )
        connect = mocker.spy(TCPConnection, "connect")
        await asyncio.gather(*[client.set(f"key{i}", i) for i in range(4096)])
        assert connect.call_count <= total_max_connections

    @pytest.mark.parametrize("concurrent_requests", (pow(2, 10), pow(2, 11), pow(2, 12)))
    @pytest.mark.parametrize(
        "key_size",
        (1 * 1024, 16 * 1024, 32 * 1024),
    )
    async def test_multiple_commands(self, locator, concurrent_requests, key_size):
        client = memcachio.Client(locator, max_connections=32)
        await client.flushall()

        async def tester(client, request_id):
            value = (uuid.uuid4().hex.encode("utf-8") + bytes(key_size))[:key_size]
            assert await client.set(f"{request_id}", value)
            return (await client.get(f"{request_id}")).get(f"{request_id}".encode()).value == value

        assert {True} == set(
            await asyncio.gather(*[tester(client, i) for i in range(concurrent_requests)])
        )
