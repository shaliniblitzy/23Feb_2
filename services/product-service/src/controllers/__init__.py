"""HTTP controllers (FastAPI APIRouter modules) for the Product Service.

This package contains the HTTP layer of the Product Service per
AAP Section 0.5.2.2 bullet 4 and the parent ``src/`` folder spec.
Each submodule defines a FastAPI ``APIRouter`` named ``router`` that is
mounted by ``src.main:create_app()``:

* ``controllers.health`` — Liveness (``/health/live``) and readiness
  (``/health/ready``) probes per AAP R-19. Mounted UNPREFIXED.
* ``controllers.products`` — Product catalog read and admin write
  endpoints. Mounted at ``/products``. Emits ``product.created`` and
  ``product.updated`` Kafka events on admin writes (AAP R-30, R-32, R-33).
* ``controllers.categories`` — Category hierarchy read and admin write
  endpoints. Mounted at ``/categories``.
* ``controllers.media`` — Product media reference management. Declares
  its own router-level prefix ``/products/{product_id}/media`` and is
  mounted by ``main.py`` without an additional prefix.

Controllers are intentionally thin: they translate HTTP requests into
domain commands, invoke repositories and the event publisher via the DI
container available at ``request.app.state.container``, and let
ErrorHandlerMiddleware translate domain exceptions to HTTP responses
per AAP R-25.

This package does NOT re-export the submodules' routers; consumers
import them directly, e.g. ``from src.controllers.products import
router as products_router``.

Cross-references:
    * AAP Section 0.1.1 Component #4 (Product Service responsibilities)
    * AAP Section 0.4.2 (Integration matrix; gateway routing of
      ``/products`` and ``/categories``)
    * AAP Section 0.4.5 (Cross-cutting middleware envelope)
    * AAP Section 0.5.2.2 bullet 4 (Implementation directive)
    * AAP Section 0.6.1 (``services/product-service/**/*`` in scope)
    * AAP R-19 (Health probes)
    * AAP R-21, R-22 (JWT validation; admin scope ``products:admin``)
    * AAP R-25 (No internal details in error payloads)
    * AAP R-26 (Structured JSON logs)
    * AAP R-30 (Event partition key = product_id)
"""

from __future__ import annotations

__all__: list[str] = []
