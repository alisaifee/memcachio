:tocdepth: 3

API Documentation
=================

Client
------
.. autoclass:: memcachio.Client
   :class-doc-from: both

--------------
Default values
--------------
.. automodule:: memcachio.defaults
   :no-inherited-members:


Connection Pool
---------------
.. autoclass:: memcachio.Pool
   :class-doc-from: both

.. autoclass:: memcachio.SingleServerPool
   :class-doc-from: both

.. autoclass:: memcachio.ClusterPool
   :class-doc-from: both

.. autoclass:: memcachio.EndpointHealthcheckConfig
   :no-inherited-members:

.. autoenum:: memcachio.EndpointStatus
   :no-inherited-members:

.. autoclass:: memcachio.PoolMetrics
   :no-inherited-members:

Connections
-----------
.. autoclass:: memcachio.BaseConnection
   :class-doc-from: both
   :no-inherited-members:
.. autoclass:: memcachio.TCPConnection
   :class-doc-from: both
.. autoclass:: memcachio.UnixSocketConnection
   :class-doc-from: both
.. autoclass:: memcachio.ConnectionMetrics
   :no-inherited-members:

Exception types
---------------
.. automodule:: memcachio.errors
 :no-inherited-members:

Types
-----
.. automodule:: memcachio.types
   :no-inherited-members:
.. autoclass:: memcachio.ConnectionParams
   :no-inherited-members:
   :class-doc-from: both

Implementation Details
----------------------

-------------
Command Types
-------------
.. autoclass:: memcachio.commands.Command
   :no-inherited-members:
   :class-doc-from: both

.. autoclass:: memcachio.commands.Request
   :no-inherited-members:
   :class-doc-from: both

--------------
Authentication
--------------

.. autoclass:: memcachio.Authenticator
   :no-inherited-members:
   :class-doc-from: both

.. autoclass:: memcachio.SimpleAuthenticator
   :no-inherited-members:
   :class-doc-from: both

-------
Routing
-------

.. autoclass:: memcachio.routing.KeyRouter
   :class-doc-from: both
