# memcachio

[![docs](https://readthedocs.org/projects/memcachio/badge/?version=stable)](https://memcachio.readthedocs.org)
[![codecov](https://codecov.io/gh/alisaifee/memcachio/branch/master/graph/badge.svg)](https://codecov.io/gh/alisaifee/memcachio)
[![Latest Version in PyPI](https://img.shields.io/pypi/v/memcachio.svg)](https://pypi.python.org/pypi/memcachio/)
[![ci](https://github.com/alisaifee/memcachio/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/alisaifee/memcachio/actions?query=branch%3Amaster+workflow%3ACI)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/memcachio.svg)](https://pypi.python.org/pypi/memcachio/)

______________________________________________________________________

A pure python async client for Memcached with support for
single (TCP or UDS) or multiple hosts
______________________________________________________________________

## Installation

To install memcachio:

```bash
$ pip install memcachio
```

## Quick start

### Single Node or Cluster client

```python
import asyncio

from memcachio import Client


async def example() -> None:

    #: basic client
    raw_client = Client(("localhost", 11211))
    #: client that decodes the byte responses
    decoding_client = Client(("localhost", 11211), decode_responses=True)
    # or with a cluster
    # cluster_client = Client([("localhost", 11211), ("localhost", 11212)], decode_responses=True)

    await raw_client.flushall()
    await raw_client.set("foo", b"1")
    await raw_client.set("bar", b"2")

    assert 2 == await raw_client.incr("foo", 1)

    # use the raw client to get a value.
    # Note the mapping returned has byte keys
    assert (await raw_client.get("foo")).get(b"foo").value == b"2"

    # get the values with the decoding client and touch their expiry to be 1 second.
    # Note the mapping and the values are strings.
    gat_and_touch_many = await decoding_client.gat("foo", "bar", expiry=1)
    assert ["2", "2"] == [item.value for item in gat_and_touch_many.values()]

    await asyncio.sleep(1)

    assert {} == await decoding_client.get("foo", "bar")


asyncio.run(example())
```

## Compatibility

memcachio is tested against memcached versions `1.6.x`

### Supported python versions

- 3.11
- 3.12
- 3.13
- PyPy 3.11


## References

- [Documentation (Stable)](http://memcachio.readthedocs.org/en/stable)
- [Documentation (Latest)](http://memcachio.readthedocs.org/en/latest)
- [Changelog](http://memcachio.readthedocs.org/en/stable/release_notes.html)
