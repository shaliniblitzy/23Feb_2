"""Hermetic unit-test suite for the Inventory Service.

This package contains pure-Python unit tests covering isolated domain logic
with no external dependencies. **Every test in this package MUST be hermetic** —
no real Postgres, no real Kafka brokers, no real Schema Registry, no live
network HTTP. All I/O collaborators are mocked:

* Postgres adapters are tested with mocked ``psycopg`` connections.
* Kafka producers/consumers are tested with mocked ``confluent_kafka`` clients.
* Outbound HTTP (e.g., the JWKS endpoint) is mocked via :mod:`respx`.

Subfolder layout
----------------
* ``domain/`` — Business-logic tests for ``StockItem`` / ``Reservation`` /
  ``Warehouse`` aggregates and the reservation orchestration service.
  Reservation state transitions, idempotency-by-``order_id``, low-stock
  threshold detection, optimistic-lock retry behavior.
* ``repository/`` — PostgreSQL adapter tests with mocked ``psycopg``
  connections. SQL statement construction, parameterization, transaction
  lifecycle (commit on success, rollback on error).
* ``warehouse/`` — ``WarehouseAdapter`` Protocol conformance tests for the
  ``DatabaseWarehouseAdapter`` and the registry resolver.
* ``events/`` — Event payload schema validation, producer logic, consumer
  handler logic. Uses :mod:`jsonschema` to validate produced event payloads
  against registered Avro/JSON schemas.
* ``scheduler/`` — Reservation expiration scheduler unit tests with
  :func:`freezegun.freeze_time` for deterministic time control.

Integration tests that DO spin up real Testcontainers live under
``services/inventory-service/tests/integration/`` — that is the ONLY place
for real-broker / real-DB fixtures.

Shared fixtures from ``services/inventory-service/tests/conftest.py`` are
inherited automatically (``faker_instance``, ``correlation_id``,
``settings_factory``, ``captured_logs``, ``assert_required_log_fields``, etc.).

AAP references
--------------
* R-6 — Database-per-service (no cross-service DB access in unit tests).
* R-7 — Polyglot persistence (PostgreSQL semantics).
* R-14 — Schema Registry validation (event payload schema tests).
* R-15 — Retry behavior (optimistic-lock retry, HTTP retry policy).
* R-17 — DLQ routing (consumer error-handling tests).
* R-19 — Health probe behavior.
* R-20 — Reservation expiry scheduler (UNIQUE to inventory-service).
* R-25 — No real secrets (all credentials mocked via :class:`SecretStr`).
* R-26 — Structured JSON log assertions.
"""
