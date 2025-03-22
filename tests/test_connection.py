from __future__ import annotations

import asyncio
from io import BytesIO

import pytest

import memcachio
from memcachio import UnixSocketConnection
from memcachio.commands import Command, FlushAllCommand, GetCommand, R, Request, SetCommand
from memcachio.errors import ClientError, MemcachioConnectionError, ServerError
from tests.conftest import flush_server


class TestConnectionErrors:
    @pytest.mark.parametrize("address", [("192.0.2.0", 11211), ("100::1", 11211)])
    async def test_tcp_host_no_response(self, address):
        connection = memcachio.TCPConnection(address, connect_timeout=0.1)
        with pytest.raises(MemcachioConnectionError):
            await connection.connect()

    async def test_tcp_host_wrong_type_of_server(self):
        connection = memcachio.TCPConnection(("8.8.8.8", 53))
        await connection.connect()
        command = GetCommand("test")
        connection.create_request(command)
        assert connection.connected
        with pytest.raises(MemcachioConnectionError):
            await command.response
        assert not connection.connected
        assert connection.metrics.requests_failed == 1

    async def test_uds_invalid_socket(self):
        connection = UnixSocketConnection("/var/tmp/invalid.sock")
        with pytest.raises(MemcachioConnectionError, match="Unable to establish a connection"):
            await connection.connect()

    async def test_connect_timeout(self, memcached_1):
        await flush_server(memcached_1)
        connection = memcachio.TCPConnection(memcached_1, connect_timeout=0.00001)
        with pytest.raises(
            MemcachioConnectionError, match="Unable to establish a connection within"
        ):
            await connection.connect()

    async def test_read_timeout(self, memcached_1):
        await flush_server(memcached_1)
        connection = memcachio.TCPConnection(
            memcached_1, read_timeout=0.0001, max_inflight_requests_per_connection=1024
        )
        await connection.connect()
        set_commands = [SetCommand(f"key{i}", bytes(32 * 1024)) for i in range(10)]
        [connection.create_request(command) for command in set_commands]
        get_commands = [GetCommand(f"key{i}") for i in range(10)]
        [connection.create_request(command) for command in get_commands]
        with pytest.raises(TimeoutError, match="command .* timed out after 0.0001 seconds"):
            await asyncio.gather(*[command.response for command in set_commands + get_commands])

    async def test_server_error(self, memcached_1):
        await flush_server(memcached_1)
        connection = memcachio.TCPConnection(memcached_1)
        await connection.connect()
        flush_all = FlushAllCommand(0)
        connection.create_request(flush_all)
        await flush_all.response
        data = bytes(2 * 1024 * 1024)
        set_command = SetCommand("key", data)
        connection.create_request(set_command)
        with pytest.raises(ServerError, match="object too large for cache"):
            await set_command.response

    async def test_client_error(self, memcached_1):
        await flush_server(memcached_1)
        connection = memcachio.TCPConnection(memcached_1)
        await connection.connect()

        class BadCommand(Command[bool]):
            name = b"set"

            def build_request(self) -> Request[R]:
                return Request(self, b"key 0 0 2", [b"123\r\n"])

            def parse(self, data: BytesIO) -> R:
                header = data.readline()
                self._check_header(header)
                return False

        bad_command = BadCommand()
        connection.create_request(bad_command)
        with pytest.raises(ClientError, match="bad data chunk"):
            await bad_command.response
