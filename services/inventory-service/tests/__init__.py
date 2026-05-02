"""Inventory Service test suite root package.

This package marks ``services/inventory-service/tests/`` as a Python regular
package so pytest correctly resolves fixtures across nested conftest modules
and so individual test modules import as ``tests.unit.<...>`` /
``tests.integration.<...>`` rather than relying on namespace-package magic.

Subpackage layout:
  - ``tests.unit``         — Pure-Python unit tests for domain logic, repository
                             adapters, warehouse adapter contract conformance,
                             event payload schema validation, and the
                             reservation expiry scheduler (mocked time).
  - ``tests.integration``  — Full saga-step tests against Testcontainers
                             Postgres + Kafka: order.created → reserve →
                             inventory.reserved (or inventory.reservation_failed),
                             order.cancelled → release → inventory.released,
                             order.fulfilled → finalize → inventory.released,
                             plus AAP R-17 DLQ routing verification.

Shared fixtures live in the sibling ``conftest.py`` (root of this package);
subfolder-scoped fixtures (Testcontainers Postgres/Kafka, FastAPI app under
test) live in ``tests/integration/conftest.py``.

This module intentionally contains no executable code, no imports, and no
``__all__`` declaration — it exists solely to mark the directory as a
regular package.
"""
