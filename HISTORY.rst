Changelog
==========

v0.5.1
------
Release Date: 2025-09-29

* Bug Fix

  * Fix version attribute in build

v0.5
----
Release Date: 2025-09-22

* Compatibility

  * Update dev dependencies


v0.4.3
------
Release Date: 2025-09-22

v0.4.2
------
Release Date: 2025-09-18

* Chore

  * Migrate project metadata and build setup to pyproject.toml
v0.4.1
------
Release Date: 2025-04-17

* Bug fix

  * Fix parsing of autodiscovery config get command
  * Ensure autodiscovery task gets cancelled on pool
    close.

v0.4.0
------
Release Date: 2025-04-16

* Features

  * Add support for auto discovery with AWS Elasticache
  * Add support for custom authentication strategies (enables support for MemCachier)

v0.3
----
Release Date: 2025-04-09

* Features

  * Allow Clients to configure hashing strategy
  * Expose connection reuse thresholds
  * Add cluster health check monitoring and adaptive removal/recovery
    of instances from the cluster

* Performance

  * Optimize single key commands in cluster mode by skipping hashing

* Compatibility

  * Add support for python 3.10

* Bug Fix

  * Fix consistence of no key commands to return results by endpoint
    when necessary (``stats`` and ``version``)

v0.2.0
------
Release Date: 2025-03-31

* Documentation

  * Add documentation for public APIs

v0.1.2
------
Release Date: 2025-03-31

* Compatibility

  * Fix python version classifiers

v0.1.1
------
Release Date: 2025-03-31

* Chores

  * Add release scripts


v0.1.0
------
Release Date: 2025-03-31

Initial Release

* Features

  * Support for single or cluster hosts (TCP/UDS)
  * SASL Authentication
  * SSL connections











