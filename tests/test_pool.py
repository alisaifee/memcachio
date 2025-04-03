from __future__ import annotations

import asyncio
import re
from contextlib import closing
from unittest.mock import ANY, call

import pytest
from pytest_lazy_fixtures import lf

from memcachio import BaseConnection, ClusterPool, SingleServerPool
from memcachio.commands import FlushAllCommand, GetCommand, SetCommand, TouchCommand, VersionCommand
from memcachio.errors import ConnectionNotAvailable, MemcachioConnectionError, NoAvailableNodes
from memcachio.pool import NodeHealthcheckConfig, NodeStatus
from tests.conftest import pypy_flaky_marker

pytestmark = pypy_flaky_marker()


@pytest.mark.parametrize(
    "locator",
    [pytest.param(lf(target)) for target in ["memcached_1", "memcached_uds"]],
)
class TestSingleInstancePool:
    async def test_pool_expansion(self, locator):
        pool = SingleServerPool(
            locator,
            max_connections=4,
            max_inflight_requests_per_connection=0,
        )
        with closing(pool):
            commands = [VersionCommand() for _ in range(16)]
            await asyncio.gather(*[pool.execute_command(command) for command in commands])
            assert len(pool._active_connections) == 4

    async def test_blocking_timeout(self, locator):
        pool = SingleServerPool(
            locator,
            max_connections=1,
            blocking_timeout=0.001,
            max_inflight_requests_per_connection=0,
        )
        with closing(pool):
            await pool.execute_command(SetCommand("key", bytes(4096), noreply=True))

            with pytest.raises(
                ConnectionNotAvailable, match="Unable to get a connection.*in 0.001 seconds"
            ):
                await asyncio.gather(*[pool.execute_command(GetCommand("key")) for i in range(16)])

    async def test_idle_connection_timeout(self, locator):
        pool = SingleServerPool(
            locator,
            max_connections=10,
            min_connections=4,
            max_inflight_requests_per_connection=0,
            idle_connection_timeout=0.5,
        )
        with closing(pool):
            set_command = SetCommand("key", bytes(1024))
            await pool.execute_command(set_command)
            await set_command.response

            gets = [GetCommand("key") for _ in range(1024)]
            await asyncio.gather(*[pool.execute_command(get_command) for get_command in gets])
            await asyncio.gather(*[get_command.response for get_command in gets])

            assert len(pool._active_connections) == 10
            await asyncio.sleep(1.5)
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
    async def test_cluster_pool_single_key_command(self, cluster_locator, mocker):
        pool = ClusterPool(cluster_locator)
        with closing(pool):
            command = TouchCommand("key", expiry=1)
            send = mocker.spy(BaseConnection, "send")
            await pool.execute_command(command)

            send.assert_called_once_with(ANY, b"touch key 1\r\n")

    async def test_cluster_pool_multi_key_command(self, cluster_locator):
        pool = ClusterPool(cluster_locator)
        with closing(pool):
            await asyncio.gather(
                *[pool.execute_command(SetCommand(f"key{i}", i, noreply=True)) for i in range(1024)]
            )

    async def test_cluster_pool_keyless_command(self, cluster_locator, mocker):
        pool = ClusterPool(cluster_locator)
        with closing(pool):
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

    async def test_node_removal(self, cluster_locator, mocker):
        pool = ClusterPool(cluster_locator)
        nodes = pool.nodes
        first_node = list(nodes).pop()
        with closing(pool):
            pool.remove_node("/var/tmp/not-in-pool")
            assert pool.nodes == nodes
            assert first_node in pool.nodes
            pool.remove_node(first_node)
            assert first_node not in pool.nodes

    async def test_all_nodes_removed(self, cluster_locator, mocker):
        pool = ClusterPool(cluster_locator)
        nodes = pool.nodes
        with closing(pool):
            await pool.initialize()
            [pool.remove_node(node) for node in nodes]
            get = GetCommand("key")
            with pytest.raises(NoAvailableNodes):
                await pool.execute_command(get)

    async def test_node_addition(self, cluster_locator, mocker):
        pool = ClusterPool(cluster_locator)
        nodes = pool.nodes
        new_locator = "/var/tmp/new-instance-not-real.sock"
        with closing(pool):
            pool.add_node(new_locator)
            assert pool.nodes == nodes | {new_locator}
            [pool.remove_node(k) for k in nodes if k != new_locator]
            get = GetCommand("key")
            with pytest.raises(
                MemcachioConnectionError, match=f"memcached instance: {new_locator}"
            ):
                await pool.execute_command(get)

    async def test_mark_node_unhealthy(self, cluster_locator, mocker):
        pool = ClusterPool(cluster_locator)
        bad_node = "/var/tmp/new-instance-not-real.sock"
        pool.add_node(bad_node)
        with closing(pool):
            get = GetCommand("key")
            with pytest.raises(MemcachioConnectionError, match=f"memcached instance: {bad_node}"):
                await pool.execute_command(get)
            pool.mark_node(bad_node, NodeStatus.DOWN)
            get = GetCommand("key")
            await pool.execute_command(get)
            assert {} == await get.response

    async def test_auto_removal_unhealthy_node(self, cluster_locator, mocker):
        pool = ClusterPool(
            cluster_locator,
            node_healthcheck_config=NodeHealthcheckConfig(remove_unhealthy_nodes=True),
        )
        with closing(pool):
            await pool.initialize()
            target_node = pool._router.get_node("key")

            async def raise_error(*args):
                if instance_pool := pool._cluster_pools.get(target_node, None):
                    for connection in instance_pool._active_connections:
                        connection.close()
                    instance_pool.close()
                raise MemcachioConnectionError(instance=target_node, message="something bad")

            with closing(pool):
                get = GetCommand("key")
                mocker.patch.object(
                    pool._cluster_pools[target_node],
                    "execute_command",
                    side_effect=raise_error,
                    autospec=True,
                )
                mocker.patch.object(
                    pool._cluster_pools[target_node],
                    "initialize",
                    side_effect=raise_error,
                    autospec=True,
                )
                with pytest.raises(
                    MemcachioConnectionError,
                    match=re.escape(f"something bad (memcached instance: {target_node})"),
                ):
                    await pool.execute_command(get)
                await asyncio.sleep(0.01)
                assert target_node not in pool.nodes

    async def test_auto_recovery_unhealthy_node(self, cluster_locator, mocker):
        pool = ClusterPool(
            cluster_locator,
            node_healthcheck_config=NodeHealthcheckConfig(
                remove_unhealthy_nodes=True, monitor_unhealthy_nodes=True
            ),
        )
        with closing(pool):
            await pool.initialize()
            target_node = pool._router.get_node("key")

            async def raise_error(*args, **kwargs):
                if instance_pool := pool._cluster_pools.get(target_node, None):
                    for connection in instance_pool._active_connections:
                        connection.close()
                raise MemcachioConnectionError(instance=target_node, message="something bad")

            with closing(pool):
                get = GetCommand("key")

                mocker.patch.object(
                    pool._cluster_pools[target_node],
                    "execute_command",
                    side_effect=raise_error,
                    autospec=True,
                )
                mocker.patch.object(
                    pool._cluster_pools[target_node],
                    "initialize",
                    side_effect=raise_error,
                    autospec=True,
                )

                with pytest.raises(
                    MemcachioConnectionError,
                    match=re.escape(f"something bad (memcached instance: {target_node})"),
                ):
                    await pool.execute_command(get)

                await asyncio.sleep(0.01)

                assert target_node not in pool.nodes

                mocker.stopall()

                await asyncio.sleep(5)

                assert target_node in pool.nodes

                get = GetCommand("key")
                await pool.execute_command(get)
                assert {} == await get.response
