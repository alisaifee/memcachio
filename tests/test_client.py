from __future__ import annotations

import ssl

import pytest

import memcachio
from memcachio.commands import Command
from memcachio.errors import ClientError, MemcachioConnectionError
from memcachio.pool import ClusterPool, Pool, R, SingleServerPool
from memcachio.types import TCPLocator


class TestClient:
    async def test_invalid_construction(self, mocker):
        with pytest.raises(ValueError, match="One of `memcached_location` or `connection_pool`"):
            memcachio.Client(None)
        with pytest.raises(
            ValueError, match="One of `memcached_location` or `connection_pool`.*not both"
        ):
            memcachio.Client("fubar", connection_pool=mocker.Mock())

    async def test_construction_with_single_tcp_locator(self, memcached_1):
        client = memcachio.Client(TCPLocator(*memcached_1))
        assert isinstance(client.connection_pool, SingleServerPool)

    async def test_construction_with_multiple_tcp_locators(self, memcached_1, memcached_2):
        client = memcachio.Client([TCPLocator(*memcached_1), TCPLocator(*memcached_2)])
        assert isinstance(client.connection_pool, ClusterPool)

    async def test_ssl_context(self, memcached_ssl):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        client = memcachio.Client(TCPLocator(*memcached_ssl), ssl_context=ssl_context)
        with pytest.raises(MemcachioConnectionError):
            await client.version()
        ssl_context.verify_mode = ssl.VerifyMode.CERT_NONE
        assert await client.version()

    async def test_client_from_custom_pool(self, memcached_1):
        class MyPool(Pool):
            def close(self) -> None:
                pass

            async def execute_command(self, command: Command[R]) -> None:
                command.response.set_result(True)

        client = memcachio.Client(connection_pool=MyPool(memcached_1))
        assert await client.set("fubar", 1)

    async def test_sasl_authentication(selfself, memcached_sasl):
        client = memcachio.Client(memcached_sasl)
        with pytest.raises(ClientError, match="unauthenticated"):
            await client.get("test")
        client = memcachio.Client(memcached_sasl, username="user", password="wrong")
        with pytest.raises(ClientError, match="authentication failure"):
            await client.get("test")
        client = memcachio.Client(memcached_sasl, username="user", password="password")
        await client.get("test")
