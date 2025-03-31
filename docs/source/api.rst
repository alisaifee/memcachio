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
.. autoclass:: memcachio.pool.Pool
   :class-doc-from: both

.. autoclass:: memcachio.pool.SingleServerPool
   :class-doc-from: both

.. autoclass:: memcachio.pool.ClusterPool
   :class-doc-from: both

Connections
-----------
.. autoclass:: memcachio.BaseConnection
   :class-doc-from: both
   :no-inherited-members:
.. autoclass:: memcachio.TCPConnection
   :class-doc-from: both
   :no-inherited-members:
.. autoclass:: memcachio.UnixSocketConnection
   :class-doc-from: both
   :no-inherited-members:
.. autoclass:: memcachio.connection.ConnectionMetrics
   :no-inherited-members:

Exception types
---------------
.. automodule:: memcachio.errors
 :no-inherited-members:

Types
-------------------
.. automodule:: memcachio.types
   :no-inherited-members:
.. autoclass:: memcachio.connection.ConnectionParams
   :no-inherited-members:
