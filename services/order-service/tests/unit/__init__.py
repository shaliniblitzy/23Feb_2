"""Order Service — unit-test package.

Houses HERMETIC unit tests for the Order Service: NO Docker, NO real
Postgres, NO real Kafka, NO real HTTP. All external dependencies are
mocked via ``unittest.mock``, ``respx`` (HTTP), and pure in-memory fakes.

The test tree mirrors the ``src/`` package structure:

    services/order-service/tests/unit/
    ├── __init__.py             — this file (package marker only)
    ├── conftest.py             — per-tier fixtures (mocks, respx, JWKS,
                                   freezer, settings shorthand)
    ├── domain/                 — OrderAggregate, OrderStatus, IdempotencyKey
    ├── saga/                   — Saga state machine, coordinator, scheduler,
                                   compensators (AAP R-18 reference impl)
    ├── events/                 — Kafka producer/consumer, event schemas,
                                   topic helpers, retry/DLQ routing
    ├── repository/             — OrderRepository, SagaRepository,
                                   IdempotencyRepository SQL construction
    ├── controllers/            — HTTP route handlers (orders, health)
    └── middleware/             — JWT validation (R-21, R-22),
                                   correlation-id propagation (R-13)

Coverage targets per AAP:
    src/saga/         ≥90% (R-18 critical surface)
    src/domain/       ≥90%
    src/events/       ≥80%
    src/repository/   ≥80%
    src/controllers/  ≥80%
    src/middleware/   ≥80%

Aggregate target: ≥85% (CI fails below 80%).

This module intentionally exposes no symbols, performs no imports of
submodules, and triggers no side effects. It exists solely so that
pytest discovers the directory as a Python package.
"""

from __future__ import annotations
