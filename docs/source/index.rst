=========
memcachio
=========
.. container:: badges

   .. image:: https://img.shields.io/github/actions/workflow/status/alisaifee/memcachio/main.yml?logo=github&style=for-the-badge&labelColor=#282828
      :alt: CI status
      :target: https://github.com/alisaifee/memcachio/actions?query=branch%3Amaster+workflow%3ACI
      :class: header-badge

   .. image::  https://img.shields.io/pypi/v/memcachio.svg?style=for-the-badge
      :target: https://pypi.python.org/pypi/memcachio/
      :alt: Latest Version in PyPI
      :class: header-badge

   .. image:: https://img.shields.io/pypi/pyversions/memcachio.svg?style=for-the-badge
      :target: https://pypi.python.org/pypi/memcachio/
      :alt: Supported Python versions
      :class: header-badge

   .. image:: https://img.shields.io/codecov/c/github/alisaifee/memcachio?logo=codecov&style=for-the-badge&labelColor=#282828
      :target: https://codecov.io/gh/alisaifee/memcachio
      :alt: Code coverage
      :class: header-badge

A pure python async Memcached client with **0** dependencies
with support for:

- All memcached commands
- Memcached servers serving on TCP or Unix Domain Sockets
- Memcached clusters
- SSL transport
- SASL Authentication
- Connection reuse for multiple concurrent requests
- Dynamically adjusted connection pooling
- Auto discovery with AWS ElastiCache

Installation
============

.. code-block:: bash

    $ pip install memcachio

Getting started
===============

Single Node or Cluster client
-----------------------------

.. code-block:: python


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






See :class:`~memcachio.Client` for detailed descriptions of available
options when constructing a client.

Compatibility
=============

**memcachio** is tested against memcached versions ``1.6.x``

Supported python versions
-------------------------

- 3.10
- 3.11
- 3.12
- 3.13
- PyPy 3.10
- PyPy 3.11


.. toctree::
    :maxdepth: 2
    :hidden:

    api
    release_notes
    glossary
