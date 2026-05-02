"""Product Service integration test subpackage.

Houses **Testcontainer-backed integration tests** for the Product Service that
exercise the full pipeline against **real MongoDB and Apache Kafka instances**
plus **Schema Registry**. Verifies that the service's behavior is correct
end-to-end: HTTP routes, MongoDB query/write paths, Kafka event emission,
schema validation, retry/DLQ routing, idempotency, optimistic-version
concurrency, and health probes.

**Boundary** — this subpackage is scoped to integration concerns only:

- Hermetic, fast unit tests live in ``services/product-service/tests/unit/``.
- Cross-service end-to-end checkout flows live at the repository root in
  ``tests/e2e/``.

**Module roster** (per folder spec):

- ``conftest.py`` — Testcontainers MongoDB + Kafka + Schema Registry fixtures.
- ``test_product_crud.py`` — End-to-end CRUD round-trips against real MongoDB.
- ``test_event_emission.py`` — Verify ``product.created`` / ``product.updated``
  reach Kafka with valid Avro/JSON Schema (AAP R-14).
- ``test_indexes.py`` — Verify UNIQUE indexes (``sku``, ``slug``), multikey on
  ``category_ids``, text search on ``name`` + ``description``.
- ``test_version_conflict.py`` — Optimistic concurrency: concurrent updates → 409.
- ``test_idempotency.py`` — ``Idempotency-Key`` header replay semantics.
- ``test_dlq_routing.py`` — AAP R-17: schema-validation failures land in
  ``product.created.dlq``.
- ``test_health_probes.py`` — AAP R-19: ``/health/live`` + ``/health/ready``
  respond correctly.

**Pure-producer note** — The Product Service is a pure event PRODUCER (per AAP
Section 0.4.2 — ``kafka.topics.consume = []``). Integration tests verify
outbound emission to Kafka but do NOT spin up consumer harnesses. The
``kafka_consumer`` fixture in ``conftest.py`` is a TEST-SIDE consumer used to
assert that the service produced the expected events; the service-under-test
itself is never configured to consume.

**Conftest inheritance** — Tests in this subpackage inherit fixtures from three
layers: (1) the root ``services/product-service/tests/conftest.py`` providing
14 canonical fixtures (``anyio_backend``, ``faker``, ``correlation_id``,
``structlog`` capture, ``settings_factory``, etc.); (2) the local
``services/product-service/tests/integration/conftest.py`` providing all
Testcontainer fixtures, real-repository fixtures, JWT factory,
pause-context-managers, and autouse cleanup; (3) pytest auto-discovery of
fixtures defined within individual test files. The integration conftest is the
SOLE owner of ``testcontainers`` imports across the entire Product Service test
tree.

**AAP cross-references** verified by this subpackage's tests:

- AAP Section 0.5.2.6 Group 6 — ``services/*/tests/integration/**/*`` mandate.
- AAP Section 0.6.1 — explicit in-scope.
- AAP R-7 — Polyglot persistence (MongoDB).
- AAP R-13 — Correlation ID propagation.
- AAP R-14 — Schema Registry validation.
- AAP R-17 — DLQ routing.
- AAP R-19 — Liveness/readiness probes.
- AAP R-21, R-22, R-23 — JWT authentication.
- AAP R-26 — Structured logs.
- AAP R-30 — Event naming ``product.created``, ``product.updated``.
- AAP R-32 — Producers don't know consumers.
- AAP R-33 — Self-contained event payloads.
"""
