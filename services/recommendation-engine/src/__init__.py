"""Recommendation Engine — Python source package.

Entry point: ``src.main:app`` (loaded by uvicorn).

This module is intentionally empty beyond the package declaration. All runtime
initialization happens in ``src.main.lifespan``, and all dependency wiring lives
in ``src.container.build_container``. Nothing here should import submodules at
package-init time — doing so risks circular imports and import-time side
effects that break testability.

Package layout (high-level)
---------------------------
- ``src.main``         — FastAPI/ASGI application factory and lifespan hooks.
- ``src.container``    — Dependency-injection container (service composition).
- ``src.config``       — Strongly typed settings loader (pydantic-settings).
- ``src.controllers``  — HTTP controllers (request/response only — no logic).
- ``src.domain``       — Core domain models, value objects, and pure logic.
- ``src.embeddings``   — Vector representation utilities.
- ``src.features``     — Feature pipeline / feature fetchers.
- ``src.inference``    — ML inference runtime adapters.
- ``src.fallback``     — Degraded-mode (popularity-based) fallback path
                         (AAP R-20 — fallback declared for every dependency).
- ``src.events``       — Kafka producers/consumers and topic bindings
                         (AAP R-30 — domain-named event contracts).
- ``src.repository``   — Persistence adapters (vector store, Redis cache).
- ``src.middleware``   — Cross-cutting concerns (correlation-ID, auth, logging).
- ``src.resilience``   — Retry, circuit-breaker, timeout policies
                         (AAP R-15, R-16, R-17).

Versioning
----------
``__version__`` MUST stay aligned with ``service.version`` in
``services/recommendation-engine/config/default.yaml``. Bump both atomically
on every backward-incompatible change so log lines, ``/health`` responses,
and the configured service identity always agree.

Compliance notes
----------------
- AAP R-26: Logging configuration is initialized in ``src.main.lifespan``,
  not here, so importing this package never triggers I/O.
- AAP R-19: Liveness / readiness probes are wired in ``src.main`` so that
  ``import src`` remains side-effect-free for unit tests.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Package metadata
# ---------------------------------------------------------------------------
# ``__version__`` is the canonical Python identifier for this package's
# semantic version. Keep it in lockstep with ``service.version`` declared in
# ``services/recommendation-engine/config/default.yaml``. This value is also
# surfaced through the service's ``/health`` endpoint (constructed in
# ``src.main``) so operators can correlate deployed builds with config.
__version__: str = "1.0.0"

# ``__all__`` declares the public API exported by ``from src import *``.
# By design this package re-exports nothing: callers MUST import the specific
# submodule they need (e.g. ``from src.main import app``) rather than relying
# on package-level shortcuts. Keeping this list empty enforces that discipline
# and prevents accidental coupling at the package boundary.
__all__: list[str] = []
