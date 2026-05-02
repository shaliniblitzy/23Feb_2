"""Unit tests for the FastAPI controllers in the Product Service.

This package hosts hermetic, fast-running HTTP-layer unit tests that
exercise the route handlers in :mod:`src.controllers.products` and
:mod:`src.controllers.categories`. Tests in this package are in the
``unit`` test tier — they MUST NOT touch real I/O subsystems (no real
MongoDB, no real Kafka brokers, no real network sockets, no real time
freezing for transport-layer behaviors).

Hermeticity contract
--------------------

Every test in this package MUST satisfy the following invariants:

* MongoDB: backed by ``mongomock-motor`` via the ``mongo_client`` /
  ``mongo_collections`` fixtures defined in ``tests/unit/conftest.py``.
* Kafka producer: a ``MagicMock`` (the ``mock_kafka_producer`` fixture)
  that records ``produce`` / ``flush`` / ``poll`` calls for assertion.
* JWKS client: a ``MagicMock`` (the ``mock_jwks_client`` fixture) whose
  ``validate_token`` returns claims with ``scope='products:admin'`` by
  default; tests override the return value to exercise scope-rejection
  and missing-token paths.
* Event publisher: a ``MagicMock`` wrapping the real ``EventPublisher``
  spec, with ``AsyncMock`` instances for ``publish_product_created`` and
  ``publish_product_updated``. Tests assert on
  ``app_with_mocks.container.event_publisher.publish_*.assert_awaited_*``
  rather than on the raw ``mock_kafka_producer``.
* Time: real ``datetime.now`` is used; ``freezegun`` is reserved for the
  rare test that needs deterministic timestamp comparison.
* HTTP transport: ``httpx.AsyncClient`` driven by ``ASGITransport``
  against the in-process FastAPI ``app`` from the composite
  ``app_with_mocks`` fixture.

Test files
----------

* ``test_product_routes.py`` — covers the seven endpoints in
  :mod:`src.controllers.products`: ``GET /products/``,
  ``GET /products/by-slug/{slug}``, ``GET /products/{product_id}``,
  ``GET /products/{product_id}/media``, ``POST /products/``,
  ``PUT /products/{product_id}``, ``DELETE /products/{product_id}``.
  Verifies pagination, filtering, sort validation, idempotency-key
  replay, version-conflict mapping (HTTP 409), JWT scope enforcement
  (``products:admin``), Kafka event emission (``product.created``,
  ``product.updated``), Decimal-as-string serialization, soft-delete
  semantics, and circuit-breaker fallback for event publishing.
* ``test_category_routes.py`` — covers the five endpoints in
  :mod:`src.controllers.categories`: ``GET /categories/``,
  ``GET /categories/{category_id}``,
  ``GET /categories/{category_id}/products``, ``POST /categories/``,
  ``PUT /categories/{category_id}``. Verifies parent-path resolution,
  slug auto-generation, mutator-based partial updates, version-conflict
  mapping, JWT scope enforcement, and the absence of Kafka emissions
  (per AAP Section 0.4.2 — Category mutations are NOT in the
  ``product.*`` topic family).

AAP rules verified by this package
----------------------------------

* **R-13** — Correlation ID propagation through HTTP responses
  (``X-Correlation-ID`` echo).
* **R-21, R-22** — JWT validation only (Auth Service is sole issuer);
  JWKS client returns claims, controller enforces ``products:admin``
  scope on writes.
* **R-25** — Error responses do NOT leak internal details
  (``error_type`` is the exception class name, never ``str(exc)``).
* **R-26** — Structured JSON logs with required fields
  (``timestamp``, ``level``, ``service``, ``correlation_id``, ``event``,
  plus event-specific fields like ``product_id``, ``sku``, ``slug``,
  ``new_version``, ``new_status``).
* **R-30** — Event topic naming (``product.created``,
  ``product.updated``); partition key = ``product_id`` (asserted on the
  publisher mock).
* **R-32** — Producer agnostic of consumers (publisher mock receives the
  domain object; tests do not reference any consumer).
* **R-33** — Self-contained event payloads (Decimal-as-string
  convention).

Cross-references
----------------

* Companion package: :mod:`tests.integration.controllers` —
  Testcontainer-backed end-to-end exercise of the same routes against
  real MongoDB and a real Kafka broker.
* Sibling unit packages: :mod:`tests.unit.domain` (aggregate
  validation), :mod:`tests.unit.repository` (mongomock CRUD),
  :mod:`tests.unit.events` (publisher / consumer wiring),
  :mod:`tests.unit.resilience` (retry / breaker policy).
"""
