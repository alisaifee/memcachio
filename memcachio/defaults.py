from __future__ import annotations

from typing import Literal

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

#: Whether to remove unhealthy endpoints on connection errors.
#: This is the default value for :attr:`~memcachio.endpointHealthcheckConfig.remove_unhealthy_endpoints`
REMOVE_UNHEALTHY_ENDPOINTS = False
#: Maximum numbers of errors to tolerate before marking an endpoint
#: as unhealthy
MAXIMUM_ERROR_COUNT_FOR_ENDPOINT_REMOVAL = 2
#: Whether to monitor unhealthy endpoints after they have been
#: removed and attempt to restore them if they recover
#: This is the default value for :attr:`~memcachio.endpointHealthcheckConfig.monitor_unhealthy_endpoints`
MONITOR_UNHEALTHY_ENDPOINTS = False
#: Maximum attempts to make to recover unhealthy endpoints
#: This is the default value for :attr:`~memcachio.endpointHealthcheckConfig.maximum_recovery_attempts`
MAXIMUM_RECOVERY_ATTEMPTS = 4
#:
RETRY_BACKOFF_POLICY: Literal["linear"] = "linear"
