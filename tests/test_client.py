from __future__ import annotations

import asyncio
import ssl
from contextlib import closing

import pytest

import memcachio
from memcachio import Authenticator, AWSAutoDiscoveryEndpoint, BaseConnection
from memcachio.commands import Command, SetCommand
from memcachio.errors import ClientError, MemcachioConnectionError
from memcachio.pool import ClusterPool, Pool, PoolMetrics, R, SingleServerPool
from memcachio.types import TCPEndpoint


class TestClient:
    async def test_invalid_construction(self, mocker):
        with pytest.raises(ValueError, match="One of `memcached_location` or `connection_pool`"):
            memcachio.Client(None)
        with pytest.raises(
            ValueError, match="One of `memcached_location` or `connection_pool`.*not both"
        ):
            memcachio.Client("fubar", connection_pool=mocker.Mock())

    async def test_construction_with_single_tcp_endpoint(self, memcached_1):
        client = memcachio.Client(TCPEndpoint(*memcached_1))
        assert isinstance(client.connection_pool, SingleServerPool)

    async def test_construction_with_multiple_tcp_endpoints(self, memcached_1, memcached_2):
        client = memcachio.Client([TCPEndpoint(*memcached_1), TCPEndpoint(*memcached_2)])
        assert isinstance(client.connection_pool, ClusterPool)

    async def test_ssl_context(self, memcached_ssl):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        client = memcachio.Client(TCPEndpoint(*memcached_ssl), ssl_context=ssl_context)
        with pytest.raises(MemcachioConnectionError):
            await client.version()
        ssl_context.verify_mode = ssl.VerifyMode.CERT_NONE
        assert await client.version()

    async def test_client_from_custom_pool(self, memcached_1):
        class MyPool(Pool):
            async def initialize(self) -> None:
                pass

            def close(self) -> None:
                pass

            @property
            def metrics(self) -> PoolMetrics:
                return PoolMetrics()

            async def execute_command(self, command: Command[R]) -> None:
                command.response.set_result(True)

        client = memcachio.Client(connection_pool=MyPool(memcached_1))
        assert await client.set("fubar", 1)

    async def test_sasl_authentication(self, memcached_sasl):
        client = memcachio.Client(memcached_sasl)
        with closing(client.connection_pool):
            with pytest.raises(ClientError, match="unauthenticated"):
                await client.get("test")
            client = memcachio.Client(memcached_sasl, username="user", password="wrong")
            with pytest.raises(ClientError, match="authentication failure"):
                await client.get("test")
            client = memcachio.Client(memcached_sasl, username="user", password="password")
            await client.get("test")

    async def test_sasl_authentication_with_custom_authenticator(self, memcached_sasl):
        class MyAuthenticator(Authenticator):
            def __init__(self, username: str, password: str, lie: bool = False):
                self.username = username
                self.password = password
                self.lie = lie

            async def authenticate(self, connection: BaseConnection) -> bool:
                value = f"{self.username} {self.password}" if not self.lie else "lies lies"
                command = SetCommand("auth", value)
                connection.create_request(command)
                return await command.response

        client = memcachio.Client(memcached_sasl, authenticator=MyAuthenticator("user", "password"))
        with closing(client.connection_pool):
            await client.get("test")
        lie_client = memcachio.Client(
            memcached_sasl, authenticator=MyAuthenticator("user", "password", True)
        )
        with closing(lie_client.connection_pool):
            with pytest.raises(ClientError, match="authentication failure"):
                await lie_client.get("test")

    async def test_autodiscovery_client(self, elasticache_endpoint, memcached_1):
        client = memcachio.Client(AWSAutoDiscoveryEndpoint(*elasticache_endpoint.location, 0.1))
        assert len(await client.version()) == 2
        elasticache_endpoint.remove_server("memcached-1")
        await asyncio.sleep(0.5)
        assert len(await client.version()) == 1
        elasticache_endpoint.add_server("memcached-1", memcached_1)
        await asyncio.sleep(0.5)
        assert len(await client.version()) == 2
