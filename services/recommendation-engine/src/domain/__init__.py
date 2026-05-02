"""Domain layer for the Recommendation Engine.

This package holds the pure-Python domain models, value objects, and
domain-error hierarchy. Modules here MUST be free of I/O and framework
coupling — they are the canonical types referenced by every other
layer (controllers, repositories, event handlers, inference path).

Modules
-------
* ``src.domain.models``
    Pydantic v2 frozen models for ``Features``, ``FeatureDelta``,
    ``Recommendation``, and supporting value objects. Models declare
    strict type constraints and are designed to round-trip through the
    persistence layer without information loss.

* ``src.domain.errors``
    The ``DomainError`` hierarchy (5 family roots: invalid input,
    not found, conflict, dependency, internal). Every higher-level
    exception thrown by the service inherits from one of these family
    roots so that controllers and middleware can map exceptions to
    HTTP status codes uniformly.

Design rules
------------
1. **No I/O** — domain modules MUST NOT import from ``src.repository``,
   ``src.events``, or any module that performs network or filesystem
   access. This guarantees deterministic, fast unit tests.
2. **Framework neutral** — domain modules MUST NOT import from
   FastAPI, Kafka, Redis, asyncpg, or any other framework. Pydantic
   is the only allowed third-party dependency.
3. **Frozen by default** — all Pydantic models in this package use
   ``ConfigDict(frozen=True)`` so they are immutable, hashable, and
   safe to share across asyncio coroutines.

Public API
----------
This package marker re-exports nothing. Callers MUST import the
specific symbol they need:

    from src.domain.models import Features, FeatureDelta, Recommendation
    from src.domain.errors import DomainError, NotFoundError, InvalidInputError

Keeping ``__all__`` empty preserves the explicit-import discipline
documented at the package root (see ``src/__init__.py``).

Compliance notes
----------------
- AAP R-31 — Domain models that mirror wire-format events (e.g.
  ``FeatureDelta``) carry a schema version field so that consumer-side
  replay is auditable.
- AAP R-26 — Domain modules emit no log lines; logging happens at the
  layer that invoked the domain operation. This keeps logs free of
  duplicated entries when the same domain object flows through
  multiple layers.
"""

from __future__ import annotations

# ``__all__`` is intentionally empty. See package docstring for the
# explicit-import discipline this package enforces.
__all__: list[str] = []
