"""Observability sub-package for the Order Service.

This sub-package contains the three pillars of observability for the
Order Service:

    logger.py    — structured-JSON logger configuration via dictConfig
                   (AAP R-26) plus structlog contextvars bridge so saga
                   handlers can `bind_contextvars(saga_id=..., ...)` and
                   the binding flows into JSON output.
    metrics.py   — Prometheus metric definitions (orders_*, saga_*, kafka_*)
                   exposed via `/metrics` (AAP R-27, R-28). Uniquely
                   includes the saga_* metric family that operationalizes
                   AAP R-18 (saga pattern with explicit compensation).
    tracing.py   — OpenTelemetry distributed tracing setup. Auto-instruments
                   FastAPI, HTTPX, and SQLAlchemy. Saga coordinator emits
                   manual spans (saga.transition, saga.compensation,
                   saga.step.<name>) consuming the global TracerProvider
                   configured here.

Submodules are imported by callers explicitly (no eager wiring at package
init) so unit tests can import individual modules without spinning up
the full observability stack.

Conventions:
    * Logger names follow `order_service.<module>.<submodule>`
      (e.g., `order_service.observability.logger`).
    * NO `print()` statements anywhere (AAP R-26).
"""
from __future__ import annotations

# Empty `__all__` so `from src.observability import *` does NOT pull in
# submodule symbols implicitly. Callers must import what they need
# explicitly: `from src.observability.logger import configure_logging`,
# `from src.observability.tracing import configure_tracing, get_tracer`,
# `from src.observability.metrics import orders_placed_total, ...`.
__all__: list[str] = []
