from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from .commands import SetCommand

if TYPE_CHECKING:
    from .connection import BaseConnection


class Authenticator(ABC):
    """
    Abstract class for authentication strategy used by
    :class:`memcachio.BaseConnection`.
    """

    @abstractmethod
    async def authenticate(self, connection: BaseConnection) -> bool:
        """
        The method that will be called immediately after a connection
        is established to the server. This should perform any authentication
        handshake required by the server. See :class:`memcachio.SimpleAuthenticator`
        for an example.

        :param connection: The connection instance that was established
        """
        ...


class SimpleAuthenticator(Authenticator):
    """
    A username/password authentication strategy for the ASCII
    protocol as defined in the `memcached protocol documentation
    <https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L186>`_.
    """

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    async def authenticate(self, connection: BaseConnection) -> bool:
        """
        Authenticate the connection using a fake ``set`` command with the
        username and password as the payload.
        """
        auth_command = SetCommand("auth", f"{self.username} {self.password}")
        connection.create_request(auth_command)
        return await auth_command.response


class MemCachierAuthenticator(SimpleAuthenticator):
    """
    A username/password authentication strategy for the ASCII
    protocol as defined by the `MemCachier documentation
    <https://www.memcachier.com/documentation/supported-protocols-ascii-binary>_

    To use with a memcachier instance::

        from memcachio import Client
        from memcachio.authentication import MemCachierAuthenticator

        mc_auth = MemCachierAuthenticator("your-memcachier-username", "your-memcachier-password")
        client = Client(("mcXXX.dev.ec2.memcachier.com", 11211), authenticator=mc_auth)


    """

    async def authenticate(self, connection: BaseConnection) -> bool:
        """
        Authenticate the connection using a ``set`` command with the
        username as the key and password as the payload.
        """
        auth_command = SetCommand(self.username, self.password)
        connection.create_request(auth_command)
        return await auth_command.response
