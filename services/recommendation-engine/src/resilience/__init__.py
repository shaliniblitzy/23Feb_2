"""Resilience primitives for the Recommendation Engine.

This package groups the cross-cutting reliability primitives mandated by
the AAP for every outbound dependency call:

* ``src.resilience.retry``
    Tenacity-backed exponential-backoff-with-jitter retry decorator
    (AAP R-15). Bounded attempt count, configurable initial interval,
    multiplier, and maximum delay; honours timeouts from
    :mod:`src.resilience.timeout`.

* ``src.resilience.circuit_breaker``
    pybreaker-backed three-state circuit-breaker (AAP R-16) with
    closed / open / half-open transitions. State surfaced through the
    readiness probe defined in :mod:`src.controllers.health`.

* ``src.resilience.timeout``
    Asyncio-compatible timeout helpers used by the HTTP clients,
    Kafka producers, and inference path so that no outbound dependency
    can block the event loop indefinitely.

Composition rule
----------------
Per the project-wide convention (also documented in the Notification
Service's circuit-breaker module), **retries sit INSIDE circuit
breakers**: a single decorated call site looks like
``circuit_breaker(retry(timeout(call)))``. This ordering ensures that
the breaker's failure-rate counters reflect EVERY underlying attempt
(including retried ones) rather than treating an exhausted retry chain
as a single failure. The breaker therefore opens correctly under
sustained downstream brownouts.

Public API
----------
This package marker intentionally re-exports nothing. Callers MUST
import the concrete submodule they need:

    from src.resilience.retry import async_retry, retry_policy
    from src.resilience.circuit_breaker import build_circuit_breaker
    from src.resilience.timeout import async_timeout

Keeping ``__all__`` empty enforces explicit imports and avoids the
import-time side effects that re-exporting would otherwise create
inside FastAPI's startup hot path (the lifespan callback in
:mod:`src.main`).

Compliance notes
----------------
- AAP R-15 — Retry with exponential backoff + jitter, bounded attempts.
- AAP R-16 — Circuit breaker with explicit closed/open/half-open
  state transitions.
- AAP R-19 — Liveness/readiness probes; the breaker's ``state`` is
  surfaced to ``/health/ready`` so a stuck-open breaker fails the
  readiness check rather than being silently treated as healthy.
- AAP R-20 — Fallback declared for every dependency; the resilience
  primitives here are the building blocks the fallback chain composes
  with degraded-mode (popularity-based) recommendations.
"""

from __future__ import annotations

# ``__all__`` is intentionally empty. This package marker exists solely so that
# ``services/recommendation-engine/src/resilience/`` is recognised as a regular
# Python package and so that the explicit-import discipline documented above
# is preserved across the codebase. Re-exporting submodule symbols here would
# create unnecessary import-time coupling and would risk circular imports if
# the resilience modules were ever imported during package initialisation.
__all__: list[str] = []
