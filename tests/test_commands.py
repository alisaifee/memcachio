from __future__ import annotations

import asyncio
import re
from unittest.mock import ANY

import pytest

import memcachio
from memcachio.errors import ClientError
from tests.conftest import targets


@targets(
    "memcached_tcp_client",
    "memcached_ssl_client",
    "memcached_tcp_cluster_client",
    "memcached_uds_client",
)
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

    async def test_gets(self, client: memcachio.Client):
        assert {} == await client.get("not-exist")
        assert await client.set("exists", 1)
        assert (await client.gets("exists")).get(b"exists").cas is not None

    async def test_gets_many(self, client: memcachio.Client):
        values = {f"key-{i}": i for i in range(100)}
        assert {} == await client.gets(*values.keys())
        for key, value in values.items():
            assert await client.set(key, value)
        cass = [value.cas for _, value in (await client.gets(*values.keys())).items()]
        assert len(cass) == 100
        assert not any(cas is None for cas in cass)

    async def test_gat(self, client: memcachio.Client):
        assert {} == await client.gat("not-exist", expiry=1)
        assert await client.set("exists", 1)
        assert {b"exists": ANY} == await client.gat("exists", expiry=1)
        await asyncio.sleep(1)
        assert {} == await client.get("exists")

    async def test_gat_many(self, client: memcachio.Client):
        values = {f"key-{i}": i for i in range(100)}
        assert {} == await client.gat(*values.keys(), expiry=1)
        for key, value in values.items():
            assert await client.set(key, value)
        assert len((await client.gat(*values.keys(), expiry=1)).items()) == 100
        await asyncio.sleep(1)
        assert len((await client.gat(*values.keys(), expiry=1)).items()) == 0

    async def test_gats(self, client: memcachio.Client):
        assert {} == await client.gats("not-exist", expiry=1)
        assert await client.set("exists", 1)
        assert (await client.gats("exists", expiry=1)).get(b"exists").cas is not None
        assert {} == await client.gats("exist", expiry=1)

    async def test_gats_many(self, client: memcachio.Client):
        values = {f"key-{i}": i for i in range(100)}
        assert {} == await client.gats(*values.keys(), expiry=1)
        for key, value in values.items():
            assert await client.set(key, value)
        assert len((await client.gats(*values.keys(), expiry=1)).items()) == 100
        await asyncio.sleep(1)
        assert len((await client.gats(*values.keys(), expiry=1)).items()) == 0

    async def test_set(self, client: memcachio.Client):
        assert await client.set("key", 1)
        assert (await client.get("key")).get(b"key").value == b"1"

        assert await client.set("key", 2, 1)
        assert (await client.get("key")).get(b"key").flags == 1

        assert await client.set("key", 3, expiry=1)
        await asyncio.sleep(1)
        assert not await client.get("key")

        assert None is await client.set("key", 4, noreply=True)
        assert (await client.get("key")).get(b"key").value == b"4"

    async def test_cas(self, client: memcachio.Client):
        assert await client.set("key", 1)
        cas = (await client.gets("key")).get(b"key").cas
        assert cas is not None
        assert not await client.cas("key", 2, cas + 1)
        assert await client.cas("key", 2, cas)
        item = (await client.gets("key")).get(b"key")
        assert item.cas != cas
        assert item.value == b"2"

    async def test_add(self, client: memcachio.Client):
        assert await client.set("key", 1)
        assert not await client.add("key", 2)
        assert await client.add("newkey", 2)
        item = (await client.get("newkey")).get(b"newkey")
        assert item.value == b"2"

    async def test_append(self, client: memcachio.Client):
        assert not await client.append("key", "o")
        assert await client.set("key", "fo")
        assert await client.append("key", "o")
        item = (await client.get("key")).get(b"key")
        assert item.value == b"foo"

    async def test_prepend(self, client: memcachio.Client):
        assert not await client.prepend("key", "f")
        assert await client.set("key", "oo")
        assert await client.prepend("key", "f")
        item = (await client.get("key")).get(b"key")
        assert item.value == b"foo"

    async def test_replace(self, client: memcachio.Client):
        assert not await client.replace("key", 1)
        assert await client.set("key", 1)
        item = (await client.get("key")).get(b"key")
        assert item.value == b"1"
        assert await client.replace("key", 2, expiry=2)
        item = (await client.get("key")).get(b"key")
        assert item.value == b"2"
        await asyncio.sleep(2)
        assert {} == await client.get("key")

    async def test_delete(self, client: memcachio.Client):
        assert await client.set("key", 1)
        assert not await client.delete("no-key")
        assert await client.delete("key")

    async def test_touch(self, client: memcachio.Client):
        assert await client.set("key", 1)
        assert await client.touch("key", 1)
        await asyncio.sleep(1)
        assert not await client.delete("key")

    async def test_incr(self, client: memcachio.Client):
        assert await client.incr("key", 1) is None
        assert await client.set("key", 1)
        assert 2 == await client.incr("key", 1)
        assert await client.incr("key", 1, noreply=True) is None
        assert 4 == await client.incr("key", 1)

        with pytest.raises(ClientError, match="invalid numeric delta"):
            await client.incr("key", pow(2, 64))

        await client.set("other-key", "one")
        with pytest.raises(ClientError, match="non-numeric value"):
            await client.incr("other-key", 1)

    async def test_decr(self, client: memcachio.Client):
        assert await client.decr("key", 1) is None
        assert await client.set("key", 3)
        assert 2 == await client.decr("key", 1)
        assert await client.decr("key", 1, noreply=True) is None
        assert 0 == await client.decr("key", 1)

        with pytest.raises(ClientError, match="invalid numeric delta"):
            await client.decr("key", pow(2, 64))

        await client.set("other-key", "one")
        with pytest.raises(ClientError, match="non-numeric value"):
            await client.decr("other-key", 1)

    async def test_stats(self, client: memcachio.Client):
        stats = await client.stats()
        for instance, stats in stats.items():
            assert b"bytes_read" in stats
        slab_stats = await client.stats("slabs")
        for instance, stats in slab_stats.items():
            assert b"active_slabs" in stats

    async def test_version(self, client: memcachio.Client):
        versions = await client.version()
        for instance, version in versions.items():
            assert re.match(r"\d+.\d+.\d+", version)

    async def test_all_noreply(self, client: memcachio.Client):
        assert None is await client.set("fubar", 1, noreply=True)
        assert None is await client.cas("fubar", 1, 0, noreply=True)
        assert None is await client.add("fubar", 1, noreply=True)
        assert None is await client.incr("fubar", 1, noreply=True)
        assert None is await client.decr("fubar", 2, noreply=True)
        assert b"0" == (await client.get("fubar")).get(b"fubar").value
        assert None is await client.replace("fubar", 6, noreply=True)
        assert None is await client.append("fubar", 6, noreply=True)
        assert None is await client.prepend("fubar", 6, noreply=True)
        value = await client.gat("fubar", "fubar2", expiry=2)
        assert None is await client.touch("fubar", 1, noreply=True)
        await asyncio.sleep(1)
        assert None is await client.delete("fubar2", noreply=True)
        assert value.get(b"fubar").value == b"666"
        assert {} == await client.get("fubar", "fubar2")
