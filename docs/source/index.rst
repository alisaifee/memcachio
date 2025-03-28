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


Compatibility
=============

**memcachio** is tested against memcached versions ``1.6.x``

Supported python versions
-------------------------

- 3.11
- 3.12
- 3.13
- PyPy 3.11


.. toctree::
    :maxdepth: 2
    :hidden:

    history
    glossary
