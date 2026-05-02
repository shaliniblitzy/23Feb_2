"""HTTP controller package for the Order Service.

This package contains FastAPI router modules that expose the Order
Service's HTTP surface (mounted by ``services/order-service/src/main.py``):

* :mod:`src.controllers.orders_controller` — exports ``router: APIRouter``
  with the six order-related endpoints (place, fetch, list, cancel,
  status-history, saga-state); mounted under ``prefix="/orders"``.
* :mod:`src.controllers.health_controller` — exports ``router: APIRouter``
  with the liveness and readiness endpoints (``/health/live`` and
  ``/health/ready``); mounted UNPREFIXED.

This package marker intentionally has **no submodule imports and no
re-exports**:

* Importers reach controllers by their fully-qualified module path
  (``from src.controllers.orders_controller import router``); this is
  the convention enforced by the explicit ``include_router`` calls in
  ``main.py``.
* Importing this package must have **no side effects** — no logger
  configuration, no Prometheus metric registration, no FastAPI
  application construction. All such side effects live in the
  individual submodules and in ``main.py``'s lifespan.
* Removing re-exports keeps test startup fast and prevents accidental
  circular imports between the controllers package and the observability
  layer it consumes.

AAP rules satisfied:
    R-26 — no ``print()``; no module-import-time logging; no side effects.
"""

from __future__ import annotations

__all__: list[str] = []
