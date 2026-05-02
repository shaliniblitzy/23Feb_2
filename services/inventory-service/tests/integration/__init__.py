"""Integration test package for the Inventory Service.

This package contains full-fidelity integration tests that exercise the
Inventory Service against REAL Postgres + Kafka brokers (via Testcontainers)
and REAL Schema Registry validation. Tests verify the saga-step contract
between the Order Service (producer of ``order.*`` events) and the Inventory
Service (consumer of ``order.*`` and producer of ``inventory.*`` outcome
events).

Package Layout
--------------
- ``conftest.py`` — Canonical 15-phase fixture file. SOLE owner of
  testcontainers and heavyweight ``src.*`` imports. Provides container
  fixtures (Postgres 16, Kafka, Schema Registry), settings, app instance,
  Kafka clients, JWKS keypair, log capture, data-seeding helpers, and
  the ``consumer_runner`` / ``expiry_scheduler`` test-only facades.
- ``test_order_created_saga.py`` — AAP saga step #1: ``order.created`` →
  ``inventory.reserved`` / ``inventory.reservation_failed``. Covers happy
  path, multi-item reservations, insufficient stock, idempotency, manual
  offset commit, correlation-id propagation, schema validation, default
  warehouse resolution, and structured logging.
- ``test_order_cancelled_saga.py`` — AAP saga step #2: ``order.cancelled``
  → ``inventory.released`` (with ``cancellation_reason``). Covers all 7
  cancellation reasons, no-op cases, idempotency, multi-item, and logging.
- ``test_order_fulfilled_saga.py`` — AAP saga step #3: ``order.fulfilled``
  → ``inventory.released(final=true)``. Covers happy path, no-op cases,
  idempotency, multi-item, and logging. Note: ``available_qty`` is
  UNCHANGED on fulfillment (the inventory was actually shipped).
- ``test_low_stock_emission.py`` — Verifies the low-stock threshold path:
  ``inventory.low-stock`` event emission, multi-item per-line crossing,
  feature-flag suppression, idempotency, and failure isolation.
- ``test_dlq_routing.py`` — AAP R-17 KEYSTONE: DLQ routing on poison
  messages. Covers decode-failure routing, processing-failure routing,
  service-DLQ envelope shape, offset commit discipline, and structured
  logging on DLQ events.
- ``test_reservation_expiry.py`` — AAP R-20: Reservation expiry scheduler
  (UNIQUE to inventory-service among saga participants). Verifies
  active-reservation expiry, terminal-status untouchability, batch
  expiry, freezegun time discipline, feature-flag gating, and atomicity.

Test Stack
----------
- ``pytest`` + ``pytest-asyncio`` (asyncio backend)
- ``testcontainers[kafka,postgres]`` for real broker fixtures
- ``confluent_kafka`` for raw Kafka producer/consumer access
- ``psycopg`` (async) for direct DB queries
- ``structlog`` for log capture and assertion
- ``freezegun`` for the reservation-expiry test only
- ``cryptography`` + ``PyJWT`` for JWKS keypair and JWT issuance fixtures

Test Database
-------------
- Postgres 16 container (``inventory_db_test``) — migrated via Alembic
  using the SAME ``migrations/`` directory as production.

Topic Catalog
-------------
- Consumed: ``order.created``, ``order.cancelled``, ``order.fulfilled``
- Produced: ``inventory.reserved``, ``inventory.reservation_failed``,
  ``inventory.released``, ``inventory.low-stock`` (note hyphen)
- DLQ: ``inventory.dlq`` (service-wide); per-source DLQ:
  ``order.created.dlq``, ``order.cancelled.dlq``, ``order.fulfilled.dlq``
- Retry: ``order.created.retry``, ``order.cancelled.retry``,
  ``order.fulfilled.retry``

Execution Conventions
---------------------
- Run serially (NOT with ``pytest-xdist -n auto``) — each test session
  spawns its own containers; parallel execution causes port collisions.
- Auto-skip if Docker daemon is unavailable.
- CI: ``pytest services/inventory-service/tests/integration/ --maxfail=3
  --timeout=120``
- Coverage target: 80%+ on saga-step paths.

AAP Cross-References
--------------------
- AAP Section 0.5.2.6 — ``services/*/tests/integration/**/*`` is in scope
- AAP R-7 — PostgreSQL semantics with ``SELECT … FOR UPDATE``
- AAP R-9 — Migrations applied on container startup
- AAP R-13 — Correlation-ID propagation through fixtures
- AAP R-14 — Schema Registry validation (real container)
- AAP R-17 — DLQ routing (KEYSTONE; ``test_dlq_routing.py``)
- AAP R-18 — Saga participant role
- AAP R-19 — Health probes
- AAP R-20 — Reservation expiry scheduler (UNIQUE to inventory-service)
- AAP R-25 — No real secrets; test container credentials only
- AAP R-26 — Structured JSON log capture on real saga executions

Out-of-Scope (handled elsewhere)
--------------------------------
- Cross-service end-to-end checkout: repository-wide ``tests/e2e/``
- Cross-service contract tests: repository-wide ``tests/contract/``
- Order Service saga-coordinator state machine: ``services/order-service/tests/``
- Notification Service email/SMS dispatch: ``services/notification-service/tests/``
- Payment Service provider-failover: ``services/payment-service/tests/``
"""

from __future__ import annotations

# This package intentionally exports nothing. All fixtures live in
# ``conftest.py`` and are auto-discovered by pytest. Test modules import
# from pytest, stdlib, and lightweight Pydantic schemas only.
__all__: list[str] = []
