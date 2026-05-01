"""Integration tests for the Notification Service.

This package contains tests that run against live Testcontainers
(Apache Kafka broker and a Postgres 16 database). External provider
APIs (SendGrid, AWS SES, Twilio, AWS SNS, JWKS endpoint) remain
mocked at this tier — only the INTERNAL dependencies (Kafka and
Postgres) are real. The fixtures that wire these containers live in
``tests/integration/conftest.py``; see that module for session- and
function-scoped fixture definitions.

Test modules in this package:

* ``test_migrations`` — Alembic upgrade head + downgrade -1 round-trip
  against a fresh Postgres Testcontainer (AAP R-9).
* ``test_startup_failfast`` — The FastAPI app refuses to start if
  POSTGRES_URL, KAFKA_BOOTSTRAP, JWKS_URL or provider credentials
  are missing or unreachable (AAP R-19).
* ``test_admin_api`` — JWT-authenticated admin endpoints for template
  CRUD, user-channel-prefs CRUD, and notification-log query.
* ``test_consume_user_registered`` — Full pipeline for the
  ``user.registered`` welcome flow (Email + SMS fan-out).
* ``test_consume_order_events`` — Full pipeline for ``order.created``,
  ``order.cancelled``, ``order.fulfilled``.
* ``test_consume_payment_events`` — Full pipeline for
  ``payment.succeeded``, ``payment.failed`` (CRITICAL override),
  ``payment.refunded``.
* ``test_dlq_routing`` — AAP R-17 retry + DLQ routing semantics:
  after ``max_attempts`` exhaustion, messages land in
  ``notifications.<channel>.dlq`` with full failure context.
* ``test_provider_failover`` — AAP R-20 circuit-breaker degraded mode:
  opening the breaker on one provider does NOT affect the other
  provider; the healthy provider keeps serving during the open state.

The parent ``tests/conftest.py`` is intentionally service-init-free;
this subpackage's ``conftest.py`` is the sole location in the test
suite where ``testcontainers`` is imported.
"""
