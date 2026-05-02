"""Unit tests for the Product Service Kafka event-production layer.

This sub-package contains hermetic unit tests for the Product Service's
**event publishing pipeline** in :mod:`src.events`:

* :mod:`src.events.payloads` — :class:`ProductEventEnvelope`,
  :class:`ProductPayload`, :class:`VariantPayload` Pydantic models, plus
  the constants :data:`EVENT_TYPE_PRODUCT_CREATED`,
  :data:`EVENT_TYPE_PRODUCT_UPDATED`, :data:`PRODUCER_NAME`, and
  :data:`DEFAULT_EVENT_VERSION`.
* :mod:`src.events.schemas` — :class:`SchemaSerializer` wrapper around
  Confluent Schema Registry's :class:`JSONSerializer`.
* :mod:`src.events.producer` — :class:`EventProducer` retry / DLQ /
  serialization-order orchestration.
* :mod:`src.events.publisher` — :class:`EventPublisher` high-level
  ``publish_product_created`` / ``publish_product_updated`` API with
  circuit-breaker + correlation-ID propagation.

Test modules in this sub-package
--------------------------------

* ``test_payload_builder`` — Pure in-memory tests of
  :meth:`ProductEventEnvelope.from_product` covering envelope-field
  population, type coercion (Decimal -> str, UUID -> str, datetime ->
  RFC 3339 UTC), correlation-ID normalization, frozen semantics, variant
  serialization, version field discipline, and self-containment per
  AAP R-33.
* ``test_event_naming`` — Tests of event-type constants, AAP R-30 naming
  regex compliance, EventPublisher routing (topic + partition-key +
  required headers), EventProducer schema-validation order (AAP R-14),
  bounded retries (AAP R-15), and DLQ topic routing (AAP R-17).

AAP rule coverage
-----------------

* **AAP R-13** — Correlation ID propagation. The ``X-Correlation-ID``
  Kafka header (PascalCase) is stamped on every event; omitted when
  correlation_id is None or empty/whitespace.
* **AAP R-14** — Schema Registry validation. ``SchemaSerializer.serialize``
  is invoked BEFORE ``kafka_producer.produce`` for every event.
* **AAP R-15** — Bounded retries. ``EventProducer`` retries up to
  ``max_send_attempts`` times before giving up.
* **AAP R-17** — DLQ routing. After exhausting retries, ``EventProducer``
  routes the message to ``<topic>.dlq`` (e.g., ``product.created.dlq``).
* **AAP R-30** — Event naming convention. ``"product.created"`` and
  ``"product.updated"`` (lowercase, dot-separated, singular domain).
  Partition key equals ``product.id`` to preserve per-product ordering.
* **AAP R-31** — Version field on every event. The envelope carries
  both ``event_version`` (semver) and ``product.version`` (aggregate
  optimistic-concurrency counter).
* **AAP R-32** — Pure producer. The Product Service has ZERO consumer
  responsibilities (consumes no Kafka events per AAP Section 0.4.2);
  envelope shape is consumer-agnostic.
* **AAP R-33** — Self-contained envelopes. Every event embeds the full
  Product aggregate (no callback links, no HATEOAS-style ``_links``).

Hermeticity contract
--------------------

These tests are pure in-memory unit tests:

* No real Kafka broker, no real Schema Registry, no real network calls.
* No real MongoDB, no real PostgreSQL, no real HTTPX.
* Use the ``mock_kafka_producer`` fixture from
  ``services/product-service/tests/unit/conftest.py`` (a ``MagicMock``
  with ``produce``/``flush``/``poll`` methods).
* Use the ``closed_breaker`` fixture from the same conftest for a real
  ``pybreaker.CircuitBreaker`` in the CLOSED state with high enough
  ``fail_max`` to allow test calls to pass through.
* Use real :class:`~src.domain.product.Product` aggregates via the
  :meth:`Product.new` factory.

This sub-package's ``__init__.py`` deliberately contains NO imports, NO
``__all__``, NO executable code -- importing the package must NEVER
trigger any production-code import. This ensures that pytest collection
remains hermetic.
"""
