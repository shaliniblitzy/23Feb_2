"""Inbound event-schema declarations for the Recommendation Engine.

This package contains the Pydantic v2 mirrors of the JSON-Schemas
registered in the canonical Schema Registry under
``infrastructure/kafka/schemas/`` (deferred to CP3). Each event class
declares the unified envelope ``{event_id, event_type, event_version,
occurred_at}`` plus its domain-specific payload.

Modules
-------
* ``src.events.schemas.event_models``
    Concrete event classes for every topic the Recommendation Engine
    consumes (``product.*``, ``order.*``, ``user.*``).

Public API
----------
This package marker re-exports nothing. Callers MUST import the
specific event class they need:

    from src.events.schemas.event_models import (
        ProductCreatedEvent,
        ProductUpdatedEvent,
        OrderCreatedEvent,
        OrderFulfilledEvent,
        UserRegisteredEvent,
        UserUpdatedEvent,
    )

Keeping ``__all__`` empty preserves the explicit-import discipline
documented at the package root (see ``src/__init__.py``) and avoids
the import-time Pydantic class construction that re-exports would
trigger.

Compliance notes
----------------
- AAP R-14 — Every event is validated against the Schema-Registry-
  registered schema (canonical JSON-Schema); the Pydantic classes
  here mirror those schemas exactly.
- AAP R-30 — Each event's ``event_type`` field matches its topic
  name in lowercase-dotted form (e.g. ``"product.created"``).
- AAP R-31 — Every event carries a schema-version field
  (``event_version: int >= 1``).
"""

from __future__ import annotations

# ``__all__`` is intentionally empty. This package marker exists solely so that
# ``services/recommendation-engine/src/events/schemas/`` is recognised as a
# regular Python package and so that the explicit-import discipline above is
# preserved across the codebase.
__all__: list[str] = []
