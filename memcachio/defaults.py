from __future__ import annotations

ENCODING = "utf-8"

#: Minimum number of connections to retain in the pool
MIN_CONNECTIONS = 1
#: Maximum connections to grow the pool to
MAX_CONNECTIONS = 2
#: Maximum time to leave a connection idle before disconnecting
IDLE_CONNECTION_TIMEOUT = 10.0
#: Maximum time to wait to retrieve a connection from the pool
BLOCKING_TIMEOUT = 5.0

#: Maxiumum time to wait to establish a connection
CONNECT_TIMEOUT = 1.0
#: Maxiumum time to wait to read a response for a request
READ_TIMEOUT = None
#: Maxiumum number of concurrent requests to pipeline on each connection
MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100
#:  Threshold for allowing the connection to be reused when there are requests pending.
MAX_AVERAGE_RESPONSE_TIME_FOR_CONNECTION_REUSE = 0.05

#: Whether to remove unhealthy nodes on connection errors.
#: This is the default value for :attr:`~memcachio.NodeHealthcheckConfig.remove_unhealthy_nodes`
REMOVE_UNHEALTHY_NODES = False
#: Whether to monitor unhealthy nodes after they have been
#: removed and attempt to restore them if they recover
#: This is the default value for :attr:`~memcachio.NodeHealthcheckConfig.monitor_unhealthy_nodes`
MONITOR_UNHEALTHY_NODES = False
#: Maximum attempts to make to recover unhealthy nodes
#: This is the default value for :attr:`~memcachio.NodeHealthcheckConfig.maximum_recovery_attempts`
MAXIMUM_RECOVERY_ATTEMPTS = 4
