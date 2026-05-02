"""User Service health-check package.

Provides the liveness and readiness probe implementations invoked by:

* ``src/controllers/health_router.py`` -- HTTP entry points for
  ``GET /health/live`` and ``GET /health/ready`` (AAP R-19).
* ``src/container.py`` -- fail-fast startup checks that re-use the same
  readiness logic so a service that becomes ready at runtime could also
  have started at boot (AAP R-19).

Public API
----------
This package marker intentionally exposes NO symbols. Consumers import
specific submodules directly, for example::

    from src.health.probes import (
        LivenessProbe,
        ReadinessProbe,
        ProbeResult,
    )

Sub-modules
-----------
``probes``
    Liveness and readiness probe classes with structured-result and
    Prometheus-instrumented dependency checks (Postgres, Kafka producer,
    Kafka consumer, Schema Registry).

Authoritative AAP references
----------------------------
* AAP R-19 -- Liveness/readiness probes; fail-fast on missing critical
  dependencies at startup.
* AAP Section 0.4.5 -- Cross-cutting middleware contract that this package
  satisfies via separate probe semantics for ``/health/live`` and
  ``/health/ready``.

Side-effect-freedom
-------------------
Importing ``src.health`` performs NO I/O, configures NO logging, and
registers NO metrics. All behavior is encapsulated in the submodules and
is invoked only by an explicit consumer (the HTTP router or the DI
container).
"""

from __future__ import annotations

__all__: list[str] = []
