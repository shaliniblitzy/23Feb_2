"""Inventory Service application package.

This package contains the application source code for the Inventory Service —
the stock reservation engine that owns ``inventory_db`` (PostgreSQL), produces
``inventory.reserved`` / ``inventory.released`` / ``inventory.low-stock`` events,
and consumes ``order.created`` / ``order.cancelled`` / ``order.fulfilled``
events from the Order Service saga (AAP Sections 0.1.3, 0.4.2, 0.5.2.2 bullet 5).

Sub-package layout
------------------
config/
    Pydantic-settings loaders that materialize a single ``Settings`` object
    from ``config/default.yaml`` overlaid with environment variables.
container.py
    Dependency-injection container; wires every long-lived resource (Postgres
    pool, Kafka producer/consumer, JWKS client, repositories, warehouse
    adapter registry, reservation expiry scheduler) exactly once at startup.
controllers/
    FastAPI routers. Health (``/health/live``, ``/health/ready``), stock
    queries (``/inventory/{productId}``), reservation lookups, and admin
    warehouse management.
domain/
    Aggregate roots and domain logic (``StockItem``, ``Reservation``,
    ``Warehouse``, ``StockMovement``) with no I/O dependencies.
events/
    Kafka producer + consumer wiring, per-event handler implementations
    (``order.created``, ``order.cancelled``, ``order.fulfilled``), Pydantic
    payload models, and DLQ routing.
exceptions.py
    Domain exceptions raised across all layers; carry stable error codes
    and the ``is_retryable`` flag that drives Kafka retry/DLQ routing.
main.py
    FastAPI ``app`` factory, lifespan, and module-level ``app`` instance
    consumed by the container ``CMD`` in the Dockerfile.
middleware/
    FastAPI middleware (correlation-id propagation, JWT validation against
    JWKS, structured access logs).
observability/
    Logging configuration (structlog + python-json-logger), Prometheus
    metric registries, OpenTelemetry tracer setup.
repository/
    SQL adapters (psycopg + SQLAlchemy 2.0) for ``stock_items``,
    ``reservations``, ``warehouses``, ``stock_movements``.
scheduler/
    Background reservation expiration scheduler (AAP R-20). Polls
    ``reservations`` for ``expires_at < now() AND status = 'ACTIVE'``,
    releases stock atomically, emits ``inventory.released(expired=true)``.
warehouse/
    ``WarehouseAdapter`` interface plus implementations
    (``DatabaseWarehouseAdapter`` today; ``ExternalWMSWarehouseAdapter``
    in the future). Selection driven by the ``warehouses.adapter_type``
    column at runtime.

Conventions
-----------
* This package marker MUST stay minimal — no submodule imports, no side
  effects at import time. The FastAPI ``app`` is constructed in
  ``src.main`` and only assigned at module-level there.
* Every submodule SHOULD start with ``from __future__ import annotations``.
* External dependencies are declared in ``services/inventory-service/requirements.txt``;
  configuration in ``services/inventory-service/config/default.yaml``.
"""

from __future__ import annotations

from typing import Final

__version__: Final[str] = "1.0.0"

__all__: list[str] = ["__version__"]
