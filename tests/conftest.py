from __future__ import annotations

import os
import platform
import socket
import ssl

import pytest
from pytest_lazy_fixtures import lf

import memcachio
from memcachio.types import TCPLocator


async def check_test_constraints(request, client):
    for marker in request.node.iter_markers():
        if marker.name == "os" and not marker.args[0].lower() == platform.system().lower():
            return pytest.skip(f"Skipped for {platform.system()}")


def ping_socket(host, port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))

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


@pytest.fixture(scope="session")
def docker_services(host_ip_env, docker_services):
    return docker_services


@pytest.fixture(scope="session")
def memcached_1(docker_services):
    docker_services.start("memcached-1")
    docker_services.wait_for_service("memcached-1", 11211, ping_socket)
    yield ("localhost", 33211)


@pytest.fixture(scope="session")
def memcached_2(docker_services):
    docker_services.start("memcached-2")
    docker_services.wait_for_service("memcached-2", 11211, ping_socket)
    yield ("localhost", 33212)


@pytest.fixture(scope="session")
def memcached_3(docker_services):
    docker_services.start("memcached-3")
    docker_services.wait_for_service("memcached-3", 11211, ping_socket)
    yield ("localhost", 33213)


@pytest.fixture(scope="session")
def memcached_cluster(memcached_1, memcached_2, memcached_3):
    yield (memcached_1, memcached_2, memcached_3)


@pytest.fixture(scope="session")
def memcached_ssl(docker_services):
    docker_services.start("memcached-ssl")
    docker_services.wait_for_service("memcached-ssl", 11211, ping_socket)
    yield ("localhost", 33214)


@pytest.fixture(scope="session")
def memcached_uds(docker_services):
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
    if platform.system().lower() == "darwin":
        pytest.skip("Fixture not supported on OSX")
    client = memcachio.Client(memcached_uds)
    await client.flushall()
    yield client
    client.connection_pool.close()


@pytest.fixture
async def memcached_tcp_cluster_client(memcached_1, memcached_2, memcached_3, request):
    client = memcachio.Client(
        [TCPLocator(*memcached_1), TCPLocator(*memcached_2), TCPLocator(*memcached_3)]
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
