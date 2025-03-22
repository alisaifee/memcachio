from __future__ import annotations

import asyncio
from unittest.mock import ANY

import pytest

import memcachio
from memcachio.errors import ClientError
from tests.conftest import targets


@targets("memcached_tcp_client", "memcached_tcp_cluster_client", "memcached_uds_client")
class TestCommands:
    async def test_get(self, client: memcachio.Client):
        assert {} == await client.get("not-exist")
        assert await client.set("exists", 1)
        assert {b"exists": ANY} == await client.get("exists")

    async def test_get_many(self, client: memcachio.Client):
        values = {f"key-{i}": i for i in range(100)}
        assert {} == await client.get(*values.keys())
        for key, value in values.items():
            assert await client.set(key, value)
        assert {key.encode("utf-8"): ANY for key in values.keys()} == await client.get(
            *values.keys(), "no-key"
        )

    async def test_set(self, client: memcachio.Client):
        assert await client.set("key", 1)
        assert (await client.get("key")).get(b"key").data == b"1"

        assert await client.set("key", 2, 1)
        assert (await client.get("key")).get(b"key").flags == 1

        assert await client.set("key", 3, exptime=1)
        await asyncio.sleep(1)
        assert not await client.get("key")

        assert None is await client.set("key", 4, noreply=True)
        assert (await client.get("key")).get(b"key").data == b"4"

    async def test_incr(self, client: memcachio.Client):
        assert await client.incr("key", 1) is None
        assert await client.set("key", 1)
        assert 2 == await client.incr("key", 1)

        with pytest.raises(ClientError, match="invalid numeric delta"):
            await client.incr("key", pow(2, 64))

        await client.set("other-key", "one")
        with pytest.raises(ClientError, match="non-numeric value"):
            await client.incr("other-key", 1)

    async def test_decr(self, client: memcachio.Client):
        assert await client.decr("key", 1) is None
        assert await client.set("key", 1)
        assert 0 == await client.decr("key", 1)

        with pytest.raises(ClientError, match="invalid numeric delta"):
            await client.decr("key", pow(2, 64))

        await client.set("other-key", "one")
        with pytest.raises(ClientError, match="non-numeric value"):
            await client.decr("other-key", 1)

    async def test_stats(self, client: memcachio.Client):
        assert b"bytes_read" in await client.stats()
        assert b"active_slabs" in await client.stats("slabs")
