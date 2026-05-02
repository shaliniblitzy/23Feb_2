"""Saga step modules for the Order Service.

Each module in this package encapsulates per-step orchestration logic
that the central ``SagaCoordinator`` dispatches to. The modules are
**pure orchestration sugar**: they delegate to ``SagaCoordinator``'s
atomic helpers (``_apply_forward_transition``, ``_apply_fulfillment``,
``trigger_compensation``) and provide only:

    - ``structlog`` context binding (``saga_id``, ``order_id``,
      ``current_step``, ``correlation_id``, ``step=<step_label>``).
    - OpenTelemetry span emission (``saga.step.<name>``).
    - Per-step latency observation
      (``saga_step_latency_ms{step=<step_label>}``).

Submodules
----------

- ``create_order_step``  — Step 1: CREATE_ORDER. Bootstrap entry
  invoked by the HTTP ``POST /orders`` handler.
- ``await_inventory_step`` — Step 2: AWAIT_INVENTORY. Consumes
  ``inventory.reserved`` (forward) or
  ``inventory.reservation_failed`` (compensation).
- ``await_payment_step``  — Step 3: AWAIT_PAYMENT. Consumes
  ``payment.succeeded`` (forward) or ``payment.failed``
  (compensation).
- ``confirm_order_step``  — Step 4: CONFIRM_ORDER. Terminal happy
  path; emits ``order.fulfilled`` and TERMINATES the saga.

Architectural rules (AAP R-18)
------------------------------

- Step modules MUST NOT open Postgres transactions; they delegate to
  ``SagaCoordinator``'s helpers.
- Step modules MUST NOT directly emit Kafka events; events flow
  through the coordinator's helpers.
- Step modules MUST observe ``saga_step_latency_ms`` per the labels
  declared in their respective module-level constants
  (``create_order``, ``inventory_reserve``, ``payment_capture``,
  ``order_fulfill``).

Import policy
-------------

This package marker intentionally does NOT re-export submodule
symbols, avoiding a circular-import risk with ``saga/coordinator.py``
which imports the step modules lazily. Consumers must import
submodules explicitly:

    from src.saga.steps import create_order_step
    await create_order_step.execute(...)
"""

from __future__ import annotations

__all__: list[str] = []
