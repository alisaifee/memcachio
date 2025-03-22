from __future__ import annotations

import asyncio
from unittest.mock import ANY, call

import pytest
from pytest_lazy_fixtures import lf

from memcachio import BaseConnection, ClusterPool, Pool, SingleServerPool
from memcachio.commands import FlushAllCommand, GetCommand, SetCommand, TouchCommand, VersionCommand
from memcachio.errors import ConnectionNotAvailable


@pytest.mark.parametrize(
    "locator",
    [pytest.param(lf(target)) for target in ["memcached_1", "memcached_uds"]],
)
class TestSingleInstancePool:
    async def test_pool_from_locator(self, locator):
        pool = Pool.from_locator(locator)
        assert isinstance(pool, SingleServerPool)
        assert locator == pool.locator

    async def test_pool_expansion(self, locator, mocker):
        pool = Pool.from_locator(
            locator,
            max_connections=4,
            blocking_timeout=0.01,
            max_inflight_requests_per_connection=0,
        )
        await pool.initialize()
        assert not all(pool._connections._queue)
        commands = [VersionCommand() for _ in range(4)]
        await asyncio.gather(*[pool.execute_command(command) for command in commands])
        assert all(pool._connections._queue)

    async def test_blocking_timeout(self, locator, mocker):
        pool = Pool.from_locator(
            locator,
            max_connections=1,
            blocking_timeout=0.01,
            max_inflight_requests_per_connection=0,
        )
        await pool.execute_command(SetCommand("key", bytes(4096), noreply=True))

        with pytest.raises(
            ConnectionNotAvailable, match="Unable to get a connection.*in 0.01 seconds"
        ):
            await asyncio.gather(*[pool.execute_command(GetCommand("key")) for i in range(4)])

    async def test_idle_connection_timeout(self, locator):
        pool = Pool.from_locator(
            locator,
            max_connections=10,
            min_connections=4,
            max_inflight_requests_per_connection=0,
            idle_connection_timeout=0.1,
        )
        set_command = SetCommand("key", bytes(1024))
        await pool.execute_command(set_command)
        await set_command.response

        gets = [GetCommand("key") for _ in range(1024)]
        await asyncio.gather(*[pool.execute_command(get_command) for get_command in gets])
        await asyncio.gather(*[get_command.response for get_command in gets])
        assert len(pool._active_connections) == 10
        await asyncio.sleep(0.5)
        assert len(pool._active_connections) == 4


@pytest.mark.parametrize(
    "cluster_locator",
    [
        pytest.param([lf(t) for t in target])
        for target in [
            ["memcached_1"],
            ["memcached_1", "memcached_2"],
            ["memcached_3", "memcached_uds"],
        ]
    ],
)
class TestClusterPool:
    async def test_cluster_pool_from_locator(self, cluster_locator):
        pool = Pool.from_locator(cluster_locator)
        assert isinstance(pool, ClusterPool)
        assert cluster_locator == pool.locator

    async def test_cluster_pool_single_key_command(self, cluster_locator, mocker):
        pool = Pool.from_locator(cluster_locator)
        command = TouchCommand("key", expiry=1)
        send = mocker.spy(BaseConnection, "send")
        await pool.execute_command(command)

        send.assert_called_once_with(ANY, b"touch key 1\r\n")

    async def test_cluster_pool_multi_key_command(self, cluster_locator):
        pool = Pool.from_locator(cluster_locator)
        await asyncio.gather(
            *[pool.execute_command(SetCommand(f"key{i}", i, noreply=True)) for i in range(1024)]
        )

    async def test_cluster_pool_keyless_command(self, cluster_locator, mocker):
        pool = Pool.from_locator(cluster_locator)
        command = FlushAllCommand(0)
        send = mocker.spy(BaseConnection, "send")
        await pool.execute_command(command)
        send.assert_has_calls(
            [
                call(ANY, b"flush_all 0\r\n"),
            ]
            * len(cluster_locator)
        )
        assert await command.response
