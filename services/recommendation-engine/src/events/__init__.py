"""Kafka event integration for the Recommendation Engine.

This package groups the consumer-side event integration: schema
declarations for inbound topics, dispatch tables, and (in later
checkpoints) the actual Kafka consumer wiring. The Recommendation
Engine is a CONSUMER-only service in CP1 — it reacts to events
produced by other services (Product, Order, User) and uses them to
update its feature pipeline.

Subpackages and modules
-----------------------
* ``src.events.schemas``
    Pydantic v2 event payload classes that mirror the JSON-Schemas
    registered in the canonical Schema Registry (under
    ``infrastructure/kafka/schemas/``, deferred to CP3). Each event
    class carries the unified envelope ``{event_id, event_type,
    event_version, occurred_at}`` plus the domain-specific payload.

Inbound topics consumed (AAP Section 0.4.2)
-------------------------------------------
* ``product.created``       -> :class:`ProductCreatedEvent`
* ``product.updated``       -> :class:`ProductUpdatedEvent`
* ``order.created``         -> :class:`OrderCreatedEvent`
* ``order.fulfilled``       -> :class:`OrderFulfilledEvent`
* ``user.registered``       -> :class:`UserRegisteredEvent`
* ``user.updated``          -> :class:`UserUpdatedEvent`

Public API
----------
This package marker re-exports nothing. Callers MUST import the
specific event class they need from the concrete schema module:

    from src.events.schemas.event_models import (
        ProductCreatedEvent,
        ProductUpdatedEvent,
        OrderCreatedEvent,
        OrderFulfilledEvent,
        UserRegisteredEvent,
        UserUpdatedEvent,
    )

Keeping ``__all__`` empty preserves the explicit-import discipline
documented at the package root (see ``src/__init__.py``).

Compliance notes
----------------
- AAP R-14 — Every consumed event is validated against the
  Schema-Registry-registered JSON-Schema; the Pydantic classes here
  mirror those schemas so the in-memory contract matches the wire
  contract exactly.
- AAP R-30 — Topic names follow ``<domain>.<verb>``; the
  ``event_type`` field on each event class matches its topic name.
- AAP R-31 — Every event payload carries a schema-version field
  (``event_version: int``) so consumers can branch on schema evolution.
- AAP R-32 — Producers do not enumerate consumers; this consumer
  registers no callbacks anywhere in the producing service.
- AAP R-33 — Events are self-contained: payloads carry every field
  the consumer needs without the consumer calling back to the producer.
"""

from __future__ import annotations

# ``__all__`` is intentionally empty. This package marker exists solely so that
# ``services/recommendation-engine/src/events/`` is recognised as a regular
# Python package. Re-exporting event classes here would create a side-effecty
# import chain (Pydantic class construction) on package import, conflicting
# with the lazy-import pattern preferred for the FastAPI startup hot path.
__all__: list[str] = []
