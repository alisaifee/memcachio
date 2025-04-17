from __future__ import annotations

import asyncio
import os
import platform
import socket
import ssl
import sys
import time

import pytest
from pytest_lazy_fixtures import lf

import memcachio
from memcachio.types import TCPEndpoint


def pypy_flaky_marker():
    return pytest.mark.xfail(sys.implementation.name == "pypy", reason="Flaky on pypy")


def ping_socket(host, port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))

        if os.environ.get("CI") == "True":
            time.sleep(5)
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def host_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()

    return ip


@pytest.fixture(scope="session")
def host_ip_env(host_ip):
    os.environ["HOST_IP"] = str(host_ip)


class FakeElasticacheServer:
    def __init__(self, servers: dict[str, tuple[str, int]]):
        self.servers = servers
        self.server = None
        self.version = 1

    @property
    def location(self) -> tuple[str, int]:
        return ("localhost", 55551)

    async def start(self) -> None:
        self.server = await asyncio.start_server(self.handle_client, "localhost", 55551)
        await self.server.serve_forever()

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode().lower()
        if "config get cluster" in message:
            servers = []
            for server, location in self.servers.items():
                servers.append(f"{server}|{location[0]}|{location[1]}")

            response = f"CONFIG cluster 0 {len(' '.join(servers))}\r\n"
            response += f"{self.version}\n"
            response += f"{' '.join(servers)}\n"
            response += "\r\n"
            response += "END\r\n"
        else:
            response = f"Command '{message}' not recognized\n"

        writer.write(response.encode())
        await writer.drain()
        writer.close()

    def add_server(self, name: str, endpoint: tuple[str, int]):
        self.servers[name] = endpoint
        self.version += 1

    def remove_server(self, name):
        self.servers.pop(name)
        self.version += 1

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


@pytest.fixture
async def elasticache_endpoint(memcached_1, memcached_2):
    # Configure the server with the list of memcached server addresses
    server = FakeElasticacheServer(
        servers={"memcached-1": memcached_1, "memcached-2": memcached_2},
    )

    asyncio.ensure_future(server.start())
    await asyncio.sleep(0.1)
    yield server
    await server.stop()


@pytest.fixture(scope="session")
def docker_services(host_ip_env, docker_services):
    return docker_services


@pytest.fixture(scope="session")
def memcached_1(docker_services):
    docker_services.start("memcached-1")
    docker_services.wait_for_service("memcached-1", 11211, ping_socket)
    yield ("localhost", 44441)


@pytest.fixture(scope="session")
def memcached_2(docker_services):
    docker_services.start("memcached-2")
    docker_services.wait_for_service("memcached-2", 11211, ping_socket)
    yield ("localhost", 44442)


@pytest.fixture(scope="session")
def memcached_3(docker_services):
    docker_services.start("memcached-3")
    docker_services.wait_for_service("memcached-3", 11211, ping_socket)
    yield ("localhost", 44443)


@pytest.fixture(scope="session")
def memcached_sasl(docker_services):
    docker_services.start("memcached-sasl")
    docker_services.wait_for_service("memcached-sasl", 11211, ping_socket)
    yield ("localhost", 44445)


@pytest.fixture(scope="session")
def memcached_cluster(memcached_1, memcached_2, memcached_3):
    yield (memcached_1, memcached_2, memcached_3)


@pytest.fixture(scope="session")
def memcached_ssl(docker_services):
    docker_services.start("memcached-ssl")
    docker_services.wait_for_service("memcached-ssl", 11211, ping_socket)
    yield ("localhost", 44444)


@pytest.fixture(scope="session")
def memcached_uds(docker_services):
    if platform.system().lower() == "darwin":
        pytest.skip("Fixture not supported on OSX")
    docker_services.start("memcached-uds")
    yield "/tmp/memcachio.sock"


@pytest.fixture
async def memcached_tcp_client(memcached_1, request):
    client = memcachio.Client(memcached_1)
    await client.flushall()
    yield client
    client.connection_pool.close()


@pytest.fixture
async def memcached_ssl_client(memcached_ssl, request):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.VerifyMode.CERT_NONE
    client = memcachio.Client(memcached_ssl, ssl_context=ssl_context)
    await client.flushall()
    yield client
    client.connection_pool.close()


@pytest.fixture
async def memcached_uds_client(memcached_uds, request):
    client = memcachio.Client(memcached_uds)
    await client.flushall()
    yield client
    client.connection_pool.close()


@pytest.fixture
async def memcached_tcp_cluster_client(memcached_1, memcached_2, memcached_3, request):
    client = memcachio.Client(
        [TCPEndpoint(*memcached_1), TCPEndpoint(*memcached_2), TCPEndpoint(*memcached_3)]
    )
    await client.flushall()
    yield client
    client.connection_pool.close()


@pytest.fixture(scope="session")
def docker_services_project_name():
    return "memcachio"


@pytest.fixture(scope="session")
def docker_compose_files(pytestconfig):
    """Get the docker-compose.yml absolute path.
    Override this fixture in your tests if you need a custom location.
    """

    return ["docker-compose.yml"]


def targets(*targets):
    return pytest.mark.parametrize(
        "client",
        [pytest.param(lf(target)) for target in targets],
    )


async def flush_server(endpoint):
    client = memcachio.Client(endpoint)
    await client.flushall()


@pytest.fixture(autouse=True)
async def wait_for_async_tasks():
    yield
    pending = {t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()}
    [t.cancel() for t in pending]
    await asyncio.gather(*pending, return_exceptions=True)
