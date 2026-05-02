"""Test package root for the Product Service.

Marks ``services/product-service/tests/`` as a Python package so that
sibling subpackages (``tests.unit``, ``tests.integration``) are
importable via absolute paths from pytest and IDE tooling.

This module is intentionally empty beyond this docstring:

* No imports — preserves the strict service-init-free contract that
  ``conftest.py`` follows. Importing ``tests`` must NEVER trigger
  service bootstrap, Docker calls, network connections, or Pydantic
  validation against environment-supplied secrets.
* No ``__all__`` — pytest discovers fixtures via ``conftest.py`` and
  tests via filename convention (``test_*.py``); no re-exports needed.
* No executable code at module scope.

Subpackages:

* :mod:`tests.unit` — fast, hermetic unit tests using ``mongomock`` /
  ``mongomock-motor`` for MongoDB-backed code paths and ``respx`` for
  HTTPX request mocking (e.g., JWKS endpoint verification).
* :mod:`tests.integration` — Testcontainer-backed integration tests
  exercising the full pipeline against real MongoDB and Apache Kafka
  instances, plus Schema Registry for AAP R-14 schema validation.

Top-level test fixtures live in :mod:`tests.conftest`; those fixtures
are automatically inherited by both subpackages without explicit imports.
"""
