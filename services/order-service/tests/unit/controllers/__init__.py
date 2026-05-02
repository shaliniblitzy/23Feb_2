"""HTTP route handler unit tests — orders and health controllers.

Covers the FastAPI route handlers in:

    * ``src.controllers.orders_controller``
        - POST /orders/ (place_order)
        - GET /orders/{order_id} (get_order)
        - GET /orders/ (list_orders)
        - POST /orders/{order_id}/cancel (cancel_order)
        - GET /orders/{order_id}/status-history (get_status_history)
        - GET /orders/{order_id}/saga-state (get_saga_state)
    * ``src.controllers.health_controller``
        - GET /health/live (liveness)
        - GET /health/ready (readiness with 4 dependency probes)

Tests are HERMETIC: NO real Postgres, NO real Kafka, NO real
Schema Registry, NO real JWT issuer. The ``SagaCoordinator``,
repositories, and infrastructure clients are mocked via the
fixtures in the parent ``conftest.py``.

Two test surfaces are used:

    * **Direct route invocation** with mocked dependencies for
      true unit isolation (preferred for business-logic
      verification).
    * **FastAPI TestClient** when explicitly testing route
      binding, status-code mapping, and exception-handler
      wiring (preferred for HTTP-contract verification).

AAP rules verified by the test files in this package:

    * R-13 - correlation-ID propagation through controllers to
      structured logs.
    * R-18 - controllers delegate to ``SagaCoordinator`` without
      embedding business logic.
    * R-19 - liveness/readiness probes; bounded timeouts; never
      raise.
    * R-21 - JWT validation invariants; controllers receive the
      principal from request state.
    * R-26 - structured JSON logs with required fields on every
      controller action.
"""

from __future__ import annotations
