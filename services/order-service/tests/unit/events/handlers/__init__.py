"""Tests for the four event handlers consumed by the Order Service.

Each handler delegates to ``saga_coordinator.handle_event`` after
performing transient validation. These tests verify the handler does
NOT contain saga business logic — it merely adapts the deserialized
event to the coordinator API.

Test modules in this package:

* ``test_inventory_reserved_handler.py`` — Success path: saga advances
  to INVENTORY_RESERVED state.
* ``test_inventory_reservation_failed_handler.py`` — Failure path: saga
  cancels WITHOUT inventory release (nothing was reserved).
* ``test_payment_succeeded_handler.py`` — Success path: saga advances to
  PAYMENT_TAKEN -> FULFILLED. Verifies dual-provider transparency
  (Stripe + Razorpay) per AAP R-10.
* ``test_payment_failed_handler.py`` — Failure path: saga cancels WITH
  inventory release (compensation required, per AAP R-18). Verifies the
  ``is_retryable`` flag distinguishes transient vs terminal failures.

Conftest fixture inheritance:

* From ``services/order-service/tests/conftest.py`` (root):
  ``correlation_id``, ``captured_logs``, ``assert_required_log_fields``,
  ``anyio_backend``.
* From ``services/order-service/tests/unit/conftest.py`` (per-tier):
  ``saga_coordinator_mock`` (AsyncMock spec=SagaCoordinator) — the
  primary fixture for every test in this package.

Logger capture note: handlers in this package use stdlib
``logging.getLogger()`` with the ``extra={...}`` parameter. Use
pytest's built-in ``caplog`` fixture for log assertions; the conftest's
``captured_logs`` (structlog-based) does NOT capture stdlib log records.
"""

from __future__ import annotations
