"""Events sub-package for the Product Service.

This package contains the Kafka producer infrastructure for the Product
Service. It emits ``product.created`` and ``product.updated`` events to
Kafka after every successful catalog mutation, partitioned by ``product_id``
so downstream consumers (Recommendation Engine, Inventory Service,
Notification Service, search-index pipelines) observe a strict per-product
ordering of state transitions (AAP R-30).

Per AAP Section 0.4.2 and the folder specification, the Product Service is
a **pure event producer** — it does NOT consume any Kafka events. This
package therefore contains NO ``consumer.py``, NO event handlers, and NO
consumer offset management code, distinguishing it from the corresponding
``events/`` packages in sibling services (``order-service``,
``notification-service``, ``inventory-service``) which all run long-lived
consumer poll loops as part of their saga / dispatch / reservation logic.
The simpler runtime profile reduces shutdown risk: there are no consumer
offsets to flush and no scheduler poll loops to drain.

Module layout
-------------
* :mod:`src.events.payloads` — Pydantic v2 envelope models for
  ``product.created`` / ``product.updated`` events. Each event carries a
  full self-contained :class:`ProductPayload` per AAP R-33 so consumers
  (Recommendation Engine, Inventory Service, Notification Service) never
  need to call back to this service to interpret an event. The unified
  envelope fields (``event_type``, ``event_version``, ``correlation_id``)
  satisfy AAP R-13, R-30, R-31.
* :mod:`src.events.schemas` — :class:`SchemaSerializer` that wraps the
  Confluent ``SchemaRegistryClient`` and validates / serializes payloads
  against the latest registered schema before produce (AAP R-14). Topic
  schemas mirror the canonical JSON Schema definitions registered under
  ``infrastructure/kafka/schemas/`` so producers and consumers share a
  single authoritative contract.
* :mod:`src.events.producer` — :class:`EventProducer` low-level wrapper
  around ``confluent_kafka.Producer`` with idempotent semantics
  (``enable.idempotence=True`` configured at the broker level), exponential
  backoff with jitter (AAP R-15), and DLQ routing on persistent failure
  (AAP R-17). Partition key is always ``product_id`` so consumers see
  strict ordering per product (AAP R-30).
* :mod:`src.events.publisher` — :class:`EventPublisher` high-level facade
  that wraps :class:`EventProducer` with a ``pybreaker.CircuitBreaker``
  (AAP R-16) and exposes domain-level methods
  (``publish_product_created``, ``publish_product_updated``) called from
  controllers / command handlers. The container (``src/container.py``)
  is the sole construction site for :class:`EventPublisher`; callers
  always receive an already-wired instance through dependency injection
  rather than importing event classes directly.

Conventions — explicit submodule imports only
----------------------------------------------
This file is a **minimal package marker** — no submodule imports, no logic,
no side effects. Importing :mod:`src.events` is essentially free and
triggers no I/O, no Kafka client construction, no Schema Registry HTTP
calls, no network access (AAP R-19 fail-fast at startup). Heavy
initialisation belongs in ``src.main.lifespan`` and
``src.container.build_container``.

Callers must import specific symbols from their owning submodules:

    >>> from src.events.publisher import EventPublisher
    >>> from src.events.producer import EventProducer
    >>> from src.events.schemas import SchemaSerializer
    >>> from src.events.payloads import ProductEventEnvelope, ProductPayload

This explicit per-submodule import discipline enforces:

* **Acyclic sibling submodules** — re-exports here would couple
  ``payloads`` to ``schemas`` to ``producer`` to ``publisher`` at import
  time, manufacturing cycles where none exist in the source dependency
  graph.
* **A lean import graph** — ``confluent_kafka``, ``pydantic``, ``httpx``,
  and ``pybreaker`` only load when the specific submodule that needs them
  is imported by a caller. Test suites that exercise pure domain code
  pay zero import cost for this package.

The pattern mirrors ``services/inventory-service/src/events/__init__.py``
and ``services/order-service/src/events/__init__.py`` which use the same
minimal style. The alternative re-exporter pattern (e.g.
``services/notification-service/src/events/__init__.py``) is appropriate
only where an external module constraint (such as a single import name
required by a consumer runner) justifies the additional coupling — that
constraint does not apply here because the Product Service has no
consumer runner and exposes no event-related symbols outside the
container-built :class:`EventPublisher`.

AAP cross-references: 0.4.2 (Product Service is a pure producer),
0.5.2.2 bullet 4 (Product Service module organisation),
R-13 (correlation-id propagation), R-14 (Schema Registry validation),
R-15 (HTTP / produce retry policy), R-16 (circuit breakers),
R-17 (retry / DLQ topology), R-19 (fail-fast at startup with
side-effect-free imports), R-30 (event naming and partitioning),
R-31 (event versioning), R-33 (events are self-contained).
"""

from __future__ import annotations

__all__: list[str] = []
