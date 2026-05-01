"""Integration tests for the Recommendation Engine service.

This package contains tests that run against live Testcontainers
(Kafka, Postgres with pgvector, Redis). The fixtures that wire these
containers live in ``tests/integration/conftest.py``; see that module
for session- and function-scoped fixture definitions.

Test modules in this package:

* ``test_event_pipeline`` — end-to-end Kafka event -> pgvector pipeline
* ``test_recommendation_api`` — full HTTP API surface with JWT auth
* ``test_fallback_chain`` — four-tier graceful degradation
* ``test_kafka_retry_dlq`` — retry topic + DLQ routing semantics
* ``test_migrations`` — V001-V006 migration schema verification
* ``test_observability`` — AAP R-26 structured logs + Prometheus metrics

The parent ``tests/conftest.py`` is intentionally service-init-free;
this subpackage's ``conftest.py`` is the sole location in the test
suite where ``testcontainers`` is imported.
"""
