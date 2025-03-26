# memcachio

[![docs](https://readthedocs.org/projects/memcachio/badge/?version=stable)](https://memcachio.readthedocs.org)
[![codecov](https://codecov.io/gh/alisaifee/memcachio/branch/master/graph/badge.svg)](https://codecov.io/gh/alisaifee/memcachio)
[![Latest Version in PyPI](https://img.shields.io/pypi/v/memcachio.svg)](https://pypi.python.org/pypi/memcachio/)
[![ci](https://github.com/alisaifee/memcachio/actions/workflows/main.yml/badge.svg?branch=master)](https://github.com/alisaifee/memcachio/actions?query=branch%3Amaster+workflow%3ACI)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/memcachio.svg)](https://pypi.python.org/pypi/memcachio/)

______________________________________________________________________

memcachio is an async memcached client

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

async def example():
    client = Memcachio(("localhost", 11211), decode_responses=True)
    # or with a cluster
    # client = Memcachio([("localhost", 11211), ("localhost", 11212)], decode_responses=True)
    await client.flushall()
    await client.set('foo', 1)
    await client.set('bar', 2)
    assert 2 == await client.increment("foo", 1)
    assert (await client.get('foo')).get("foo").value == "2"
    many = await client.gat("foo", "bar", expiry=1)
    assert ["2", "2"] == [item.value for item in many.items()]
    await asyncio.sleep(1)
    assert {} == await client.gat("foo", "bar", expiry=1)

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
