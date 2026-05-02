"""Inventory Service Kafka events sub-package.

This package is the Inventory Service's I/O bridge between the domain layer
and the Apache Kafka messaging backbone (AAP Sections 0.4.2, 0.5.2.2 bullet 5).
It owns the producer that emits ``inventory.reserved`` / ``inventory.released``
/ ``inventory.low-stock`` outcome events, the consumer that drains the inbound
``order.created`` / ``order.cancelled`` / ``order.fulfilled`` topics into the
saga participant logic, the per-event schema definitions, and the dead-letter
topology that absorbs poison messages.

Submodules (descriptive only — this file does NOT import any of them)
---------------------------------------------------------------------
schemas
    Pydantic v2 models for every consumed and emitted event payload, including
    the unified envelope fields (``event_type``, ``event_version``,
    ``correlation_id``) that satisfy AAP R-13, R-30, R-31. Schemas mirror the
    Avro/JSON definitions registered with Schema Registry under
    ``infrastructure/kafka/schemas/`` (AAP R-14).
producer
    ``EventProducer`` — the Schema-Registry-validated Kafka producer used by
    the saga handlers and the reservation expiry scheduler to emit outcome
    events (AAP R-14).
consumer
    ``KafkaConsumerRunner`` — the long-running poll loop with retry-topic and
    DLQ topology (AAP R-17), wired to the per-topic handler classes below.
dlq
    ``DlqWriter`` — emits to the per-topic ``<topic>.retry`` and ``<topic>.dlq``
    topics plus the service-wide ``inventory.dlq`` for unrecoverable failures
    (AAP R-17).
handlers
    Per-consumed-topic handler classes (``OrderCreatedHandler``,
    ``OrderCancelledHandler``, ``OrderFulfilledHandler``) that translate
    inbound order events into reservation state transitions and outcome
    events (AAP Section 0.5.2.2 bullet 5).

Note: topic names themselves are sourced from
``src.config.settings.TopicsSettings``; there is intentionally no ``topics``
submodule in this package — keeping topic names with the rest of the
configuration avoids drift and centralises environment-driven overrides.

Convention — explicit submodule imports only
--------------------------------------------
This file is a *minimal package marker*: it exposes no symbols and does not
import its own submodules. Callers must reach into the desired submodule
explicitly, e.g. ``from src.events.producer import EventProducer``. This
deliberate convention enforces:

* AAP R-19 fail-fast at startup — importing ``src.events`` triggers no I/O,
  no DB or Kafka client construction, no network access. Heavy initialisation
  belongs in ``src.main.lifespan`` and ``src.container.build_container``.
* Acyclic sibling submodules — re-exports here would couple ``schemas`` to
  ``producer`` to ``consumer`` to ``handlers`` at import time, manufacturing
  cycles where none exist in the source dependency graph.
* A lean import graph — ``confluent_kafka``, ``pydantic``, and ``httpx`` only
  load when the specific submodule that needs them is imported by a caller.

The alternative re-exporter pattern (used by ``src/config/__init__.py``) is
appropriate only for small, stable surfaces such as the ``Settings`` object;
the events package is a larger, more dynamic surface that benefits from
explicit per-submodule imports.

AAP cross-references: 0.4.2 (new service-to-service integrations),
0.5.2.2 bullet 5 (Inventory Service module organisation),
R-19 (fail-fast at startup with side-effect-free imports).
"""

from __future__ import annotations

__all__: list[str] = []
"""Intentionally empty — submodules are imported explicitly by callers.

This is the standard pattern across the inventory-service repository for
sub-packages that act as namespaces for cohesive but independently-imported
modules (cf. ``src/config/__init__.py`` for the alternative re-exporter
pattern, used only where the surface area is small and stable).
"""
