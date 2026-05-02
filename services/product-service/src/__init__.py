"""Product Service application package.

This package contains the application source code for the Product Service —
the catalog and category hierarchy authority that owns ``product_db``
(MongoDB) and produces ``product.created`` / ``product.updated`` Kafka
events on every catalog change (AAP Sections 0.1.1 Component #4, 0.4.2,
0.4.4, 0.5.2.2 bullet 4).

Database choice: MongoDB (AAP R-7) — flexible schema for highly variable
product attributes (apparel needs size/color, electronics needs voltage,
groceries needs weight/expiry). The document-oriented model embeds variants
inside products and uses dedicated collections for media metadata to avoid
expensive JOINs that a relational store would impose.

Event production: This service is a **pure event producer** (AAP Section
0.4.2). It does NOT consume any Kafka events and has NO scheduled
background work — distinguishing it from sibling Python services:
``order-service`` (consumes inventory/payment events; runs saga scheduler),
``notification-service`` (consumes order/payment/user events; runs retry
scheduler), ``inventory-service`` (consumes order events; runs reservation
expiry scheduler), and ``recommendation-engine`` (consumes product/order
events). The simpler runtime profile reduces shutdown risk: there are no
consumer offsets to flush and no scheduler poll loops to drain.

Sub-package layout
------------------
config/
    Pydantic-settings loaders that materialize a single ``Settings``
    object from ``services/product-service/config/default.yaml`` overlaid
    with environment variables (``.env`` locally, Kubernetes Secrets in
    production per AAP R-25).
container.py
    Dependency-injection container. Wires every long-lived resource
    (MongoDB client, Kafka producer + Schema Registry, JWKS client,
    repositories, idempotency store, category tree cache, circuit
    breakers) exactly once at startup; disposed in reverse order at
    shutdown.
controllers/
    FastAPI routers — HTTP layer:
        * ``health`` (``/health/live``, ``/health/ready``)
        * ``products`` (list / search / CRUD)
        * ``categories`` (tree + admin)
        * ``media`` (``/products/{id}/media``)
domain/
    Pure domain layer (no I/O dependencies):
        * ``Product`` aggregate + embedded ``Variant`` value object
        * ``Category`` aggregate (tree node with materialized path)
        * ``ProductMedia`` aggregate
        * Domain commands (``CreateProduct``, ``UpdateProduct``,
          ``DeprecateProduct``)
        * Domain exceptions (``ProductNotFound``, ``VersionConflict``,
          ``DuplicateSku``)
events/
    Kafka producer wiring + schema mapping. Produces ``product.created``
    and ``product.updated`` events (partition key = ``product_id`` per
    AAP R-30); routes persistent failures to ``product.created.dlq``
    and ``product.updated.dlq`` (AAP R-17). NO consumer module — this
    service is a pure producer.
main.py
    FastAPI ``app`` factory, lifespan, and module-level ``app`` instance
    consumed by the container ``CMD`` in the Dockerfile.
middleware/
    FastAPI middleware (correlation-id propagation per AAP R-13, JWT
    validation against JWKS per AAP R-21/R-22, structured access logs
    per AAP R-26, error handler that maps domain exceptions to HTTP
    status codes).
observability/
    Logging configuration (structlog + python-json-logger), Prometheus
    metric registrations, OpenTelemetry tracer setup with the
    ``opentelemetry-instrumentation-pymongo`` instrumentation specific
    to this MongoDB-backed service.
repository/
    MongoDB adapter layer using ``motor`` (async). Implements
    repositories for ``products``, ``categories``, and ``product_media``
    collections. Optimistic concurrency on ``products.version`` is
    enforced via ``findOneAndUpdate`` with the ``version`` filter and
    ``$inc: { version: 1 }``; conflicts raise ``VersionConflict``.

Conventions
-----------
* This package marker MUST stay minimal — no submodule imports, no side
  effects at import time. The FastAPI ``app`` is constructed in
  ``src.main`` and only assigned at module-level there.
* Every submodule SHOULD start with ``from __future__ import annotations``.
* External dependencies are declared in
  ``services/product-service/requirements.txt``; configuration in
  ``services/product-service/config/default.yaml``.
* The ``__version__`` constant below is bumped manually as the service
  evolves and must match the ``service.version`` field in
  ``config/default.yaml``.
"""

from __future__ import annotations

from typing import Final

__version__: Final[str] = "1.0.0"

__all__: list[str] = ["__version__"]
