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
from memcachio.pool import EndpointHealthcheckConfig, EndpointStatus
from tests.conftest import pypy_flaky_marker

pytestmark = pypy_flaky_marker()


@pytest.mark.parametrize(
    "endpoint",
    [pytest.param(lf(target)) for target in ["memcached_1", "memcached_uds"]],
)
class TestSingleInstancePool:
    async def test_pool_expansion(self, endpoint):
        pool = SingleServerPool(
            endpoint,
            max_connections=4,
            max_inflight_requests_per_connection=0,
        )
        with closing(pool):
            commands = [VersionCommand() for _ in range(16)]
            await asyncio.gather(*[pool.execute_command(command) for command in commands])
            assert len(pool._active_connections) == 4

    async def test_blocking_timeout(self, endpoint):
        pool = SingleServerPool(
            endpoint,
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

    async def test_idle_connection_timeout(self, endpoint):
        pool = SingleServerPool(
            endpoint,
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
    "cluster_endpoint",
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
    async def test_cluster_pool_single_key_command(self, cluster_endpoint, mocker):
        pool = ClusterPool(cluster_endpoint)
        with closing(pool):
            command = TouchCommand("key", expiry=1)
            send = mocker.spy(BaseConnection, "send")
            await pool.execute_command(command)

            send.assert_called_once_with(ANY, b"touch key 1\r\n")

    async def test_cluster_pool_multi_key_command(self, cluster_endpoint):
        pool = ClusterPool(cluster_endpoint)
        with closing(pool):
            await asyncio.gather(
                *[pool.execute_command(SetCommand(f"key{i}", i, noreply=True)) for i in range(1024)]
            )
            get_command = GetCommand(*[f"key{i}" for i in range(1024)])
            await pool.execute_command(get_command)
            assert set(range(1024)) == set(
                [int(k.value) for k in (await get_command.response).values()]
            )

    async def test_cluster_pool_keyless_command(self, cluster_endpoint, mocker):
        pool = ClusterPool(cluster_endpoint)
        with closing(pool):
            command = FlushAllCommand(0)
            send = mocker.spy(BaseConnection, "send")
            await pool.execute_command(command)
            send.assert_has_calls(
                [
                    call(ANY, b"flush_all 0\r\n"),
                ]
                * len(cluster_endpoint)
            )
            assert await command.response

    async def test_endpoint_removal(self, cluster_endpoint, mocker):
        pool = ClusterPool(cluster_endpoint)
        endpoints = pool.endpoints
        first_endpoint = list(endpoints).pop()
        with closing(pool):
            pool.remove_endpoint("/var/tmp/not-in-pool")
            assert pool.endpoints == endpoints
            assert first_endpoint in pool.endpoints
            pool.remove_endpoint(first_endpoint)
            assert first_endpoint not in pool.endpoints

    async def test_all_endpoints_removed(self, cluster_endpoint, mocker):
        pool = ClusterPool(cluster_endpoint)
        endpoints = pool.endpoints
        with closing(pool):
            await pool.initialize()
            [pool.remove_endpoint(endpoint) for endpoint in endpoints]
            get = GetCommand("key")
            with pytest.raises(NoAvailableNodes):
                await pool.execute_command(get)

    async def test_endpoint_addition(self, cluster_endpoint, mocker):
        pool = ClusterPool(cluster_endpoint)
        endpoints = pool.endpoints
        new_endpoint = "/var/tmp/new-instance-not-real.sock"
        with closing(pool):
            pool.add_endpoint(new_endpoint)
            assert pool.endpoints == endpoints | {new_endpoint}
            [pool.remove_endpoint(k) for k in endpoints if k != new_endpoint]
            get = GetCommand("key")
            with pytest.raises(MemcachioConnectionError, match=f"memcached server: {new_endpoint}"):
                await pool.execute_command(get)

    async def test_mark_endpoint_unhealthy(self, cluster_endpoint, mocker):
        pool = ClusterPool(cluster_endpoint)
        bad_endpoint = "/var/tmp/new-instance-not-real.sock"
        pool.add_endpoint(bad_endpoint)
        with closing(pool):
            get = GetCommand("key")
            with pytest.raises(MemcachioConnectionError, match=f"memcached server: {bad_endpoint}"):
                await pool.execute_command(get)
            pool.update_endpoint_status(bad_endpoint, EndpointStatus.DOWN)
            get = GetCommand("key")
            await pool.execute_command(get)
            assert {} == await get.response

    async def test_auto_removal_unhealthy_endpoint(self, cluster_endpoint, mocker):
        pool = ClusterPool(
            cluster_endpoint,
            endpoint_healthcheck_config=EndpointHealthcheckConfig(
                remove_unhealthy_endpoints=True, maximum_error_count_for_removal=1
            ),
        )
        with closing(pool):
            await pool.initialize()
            target_endpoint = pool._router.get_node("key")

            async def raise_error(*args):
                exc = MemcachioConnectionError(endpoint=target_endpoint, message="something bad")
                if instance_pool := pool._cluster_pools.get(target_endpoint, None):
                    for connection in instance_pool._active_connections:
                        connection.close()
                    instance_pool.close()
                    instance_pool.metrics.on_connection_error(None, exc)
                raise exc

            with closing(pool):
                get = GetCommand("key")
                mocker.patch.object(
                    pool._cluster_pools[target_endpoint],
                    "execute_command",
                    side_effect=raise_error,
                    autospec=True,
                )
                mocker.patch.object(
                    pool._cluster_pools[target_endpoint],
                    "initialize",
                    side_effect=raise_error,
                    autospec=True,
                )
                with pytest.raises(
                    MemcachioConnectionError,
                    match=re.escape(f"something bad (memcached server: {target_endpoint})"),
                ):
                    await pool.execute_command(get)
                await asyncio.sleep(0.01)
                assert target_endpoint not in pool.endpoints

    async def test_auto_recovery_unhealthy_endpoint(self, cluster_endpoint, mocker):
        pool = ClusterPool(
            cluster_endpoint,
            endpoint_healthcheck_config=EndpointHealthcheckConfig(
                remove_unhealthy_endpoints=True,
                monitor_unhealthy_endpoints=True,
                maximum_error_count_for_removal=1,
            ),
        )
        with closing(pool):
            await pool.initialize()
            target_endpoint = pool._router.get_node("key")

            async def raise_error(*args, **kwargs):
                exc = MemcachioConnectionError(endpoint=target_endpoint, message="something bad")
                instance_pool = pool._cluster_pools.get(target_endpoint, None)
                for connection in instance_pool._active_connections:
                    connection.close()
                instance_pool.metrics.on_connection_error(None, exc)
                raise exc

            with closing(pool):
                get = GetCommand("key")

                mocker.patch.object(
                    pool._cluster_pools[target_endpoint],
                    "execute_command",
                    side_effect=raise_error,
                    autospec=True,
                )
                mocker.patch.object(
                    pool._cluster_pools[target_endpoint],
                    "initialize",
                    side_effect=raise_error,
                    autospec=True,
                )

                with pytest.raises(
                    MemcachioConnectionError,
                    match=re.escape(f"something bad (memcached server: {target_endpoint})"),
                ):
                    await pool.execute_command(get)

                await asyncio.sleep(0.01)
                assert target_endpoint not in pool.endpoints

                mocker.stopall()

                await asyncio.sleep(5)
                assert target_endpoint in pool.endpoints

                get = GetCommand("key")
                await pool.execute_command(get)
                assert {} == await get.response

    async def test_auto_recovery_fail(self, cluster_endpoint, mocker):
        pool = ClusterPool(
            cluster_endpoint,
            endpoint_healthcheck_config=EndpointHealthcheckConfig(
                remove_unhealthy_endpoints=True,
                monitor_unhealthy_endpoints=True,
                maximum_recovery_attempts=2,
                maximum_error_count_for_removal=1,
            ),
        )
        with closing(pool):
            await pool.initialize()
            target_endpoint = pool._router.get_node("key")

            async def raise_error(*args, **kwargs):
                exc = MemcachioConnectionError(endpoint=target_endpoint, message="something bad")
                instance_pool = pool._cluster_pools.get(target_endpoint, None)
                for connection in instance_pool._active_connections:
                    connection.close()
                instance_pool.metrics.on_connection_error(None, exc)
                raise exc

            with closing(pool):
                get = GetCommand("key")

                mocker.patch.object(
                    pool._cluster_pools[target_endpoint],
                    "execute_command",
                    side_effect=raise_error,
                    autospec=True,
                )
                mocker.patch.object(
                    pool._cluster_pools[target_endpoint],
                    "initialize",
                    side_effect=raise_error,
                    autospec=True,
                )

                with pytest.raises(
                    MemcachioConnectionError,
                    match=re.escape(f"something bad (memcached server: {target_endpoint})"),
                ):
                    await pool.execute_command(get)

                await asyncio.sleep(0.01)
                assert target_endpoint not in pool.endpoints
                await asyncio.sleep(2)
                assert target_endpoint not in pool.endpoints
