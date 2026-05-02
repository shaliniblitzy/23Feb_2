"""Order Service integration test suite.

This package contains all Testcontainer-backed integration tests for the
Order Service — the AAP R-18 saga coordinator. These tests exercise the
full saga lifecycle (CREATED → INVENTORY_RESERVED → PAYMENT_TAKEN →
FULFILLED, plus all compensation paths) against real Postgres and real
Kafka containers.

Test modules:
    - test_alembic_migrations: Schema validation across all 4 tables
      (orders, order_items, order_status_history, saga_state) including
      partial indexes, CHECK constraints, and FK CASCADE rules.
    - test_app_startup: FastAPI lifespan, dual background tasks
      (order-kafka-consumer + order-saga-scheduler), 4-dependency
      readiness checks, fail-fast on missing critical settings.
    - test_correlation_id_propagation: AAP R-13 correlation_id flowing
      from HTTP request → Kafka events → Postgres rows → structured
      logs across the full saga lifecycle.
    - test_dlq_routing: AAP R-17 retry/DLQ topic routing for both
      decode-failure (direct to DLQ) and processing-failure
      (exhaust retries → DLQ → DLQExhaustionCompensator → FAILED).
    - test_full_checkout_saga: AAP R-18 happy-path saga from POST
      /orders → 202 → inventory.reserved → payment.succeeded →
      order.fulfilled with all 4 state transitions verified.
    - test_health_probe_dependencies: AAP R-19 liveness/readiness
      probes under real container failures (Postgres pause, Kafka
      pause).
    - test_idempotency: AAP R-8 two-tier idempotency (HTTP-layer
      StoredResponse cache + DB-layer idempotency_keys table + UNIQUE
      constraint defense-in-depth).
    - test_inventory_failure_compensation: AAP R-18 compensation
      branch — inventory.reservation_failed → CANCELLED (no inventory
      release needed, single-step compensation).
    - test_payment_failure_compensation: AAP R-18 compensation
      branch — payment.failed after inventory reserved →
      COMPENSATING_INVENTORY → CANCELLED (two-step compensation).
    - test_saga_recovery_after_restart: AAP R-18 saga durability —
      saga_state survives simulated process crashes; consumer
      recovery via Kafka offset replay.
    - test_saga_timeout_compensation: AAP R-18 saga liveness —
      SagaScheduler picks up timed-out sagas via FOR UPDATE SKIP
      LOCKED and triggers compensation.
    - test_structured_logs: AAP R-26 structured JSON logs with
      required fields plus order-service-unique saga extras
      (saga_id, order_id, current_step, awaiting_event).

Configuration: see conftest.py for the 15-phase canonical pattern
that establishes session-scoped Kafka and Postgres containers,
applies Alembic migrations, builds REAL SagaCoordinator and
SagaScheduler, and provides all fixtures the test modules depend on.
"""
