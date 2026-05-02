"""Domain exceptions for the Inventory Service.

This module defines the **single inheritance hierarchy** of domain-level
exceptions raised by every layer of the Inventory Service — repositories,
domain logic, warehouse adapters, controllers, event consumers/producers,
schedulers, and middleware. It is intentionally a **dependency-free leaf
module**: it does not import from ``src.*`` and only depends on the standard
library so that *every* other module in the service can ``from src.exceptions
import ...`` without risk of circular imports.

Inheritance hierarchy
---------------------
``InventoryServiceError`` is the **root** of the hierarchy. Every concrete
exception subclasses it directly — the hierarchy is intentionally **flat** to
keep classification obvious; categorization is provided by the
``code`` class attribute rather than by intermediate base classes.

::

    InventoryServiceError                 (base; never raised directly)
    ├── InsufficientStockError            (reservation domain)
    ├── ReservationNotFoundError          (reservation domain)
    ├── ReservationAlreadyExistsError     (reservation domain)
    ├── ReservationStateError             (reservation domain)
    ├── ReservationExpiredError           (reservation domain)
    ├── StockItemNotFoundError            (stock master-data)
    ├── OptimisticLockError               (stock concurrency)         [retryable]
    ├── WarehouseNotFoundError            (warehouse master-data)
    ├── WarehouseUnavailableError         (warehouse status)
    ├── AdapterNotRegisteredError         (configuration)
    ├── WarehouseAdapterError             (adapter call)              [retryable]
    ├── EventValidationError              (Schema Registry)
    ├── EventPublishError                 (Kafka producer)            [retryable]
    ├── UnauthorizedError                 (HTTP 401)
    ├── ForbiddenError                    (HTTP 403)
    └── ValidationError                   (HTTP 400)

Exception contract
------------------
Every exception in this module exposes three pieces of structured metadata
that downstream layers — logging, error-handler middleware, Kafka
retry/DLQ routing, and tenacity retry policies — consume to make
consistent decisions:

* ``code`` (class-level ``str``) — a stable, machine-readable identifier
  in dotted ``inventory.<snake_case>`` form (e.g. ``inventory.insufficient_stock``).
  Codes are part of the service's external contract: they appear in the
  ``error`` field of error events emitted to Kafka and in JSON error
  payloads returned by the HTTP API. Once published, codes MUST NOT change
  without a versioning strategy.

* ``is_retryable`` (class-level ``bool``) — declares whether the *operation
  that raised the exception* may be safely retried. This boolean is the
  keystone field of the module — it directly drives:

  * **AAP R-15**: tenacity ``retry_if_exception_type`` policies in
    repositories and HTTP clients.
  * **AAP R-17**: Kafka consumer routing in ``events/consumer.py``. A
    raised exception with ``is_retryable=True`` causes the consumer to
    publish the message to ``<topic>.retry`` (bounded retries with
    exponential backoff). When ``is_retryable=False`` (or after retries
    exhaust), the consumer publishes to ``<topic>.dlq`` (poison message).

* ``details`` (instance-level ``dict[str, Any]``) — free-form mapping of
  context that callers attach to the exception for log enrichment. Always
  initialized as a fresh dict (never a shared mutable default).

Canonical retryable subset
--------------------------
Only **three** exception classes carry ``is_retryable = True``; every other
exception is permanent:

* :class:`OptimisticLockError` — DB row contention on optimistic-locking
  ``stock_items`` updates.
* :class:`WarehouseAdapterError` — transient WMS boundary failure
  (network timeout, 5xx response).
* :class:`EventPublishError` — transient Kafka producer failure.

Canonical non-retryable subset
------------------------------
:class:`InsufficientStockError`, :class:`ReservationNotFoundError`,
:class:`ReservationAlreadyExistsError`, :class:`ReservationStateError`,
:class:`ReservationExpiredError`, :class:`StockItemNotFoundError`,
:class:`WarehouseNotFoundError`, :class:`WarehouseUnavailableError`,
:class:`AdapterNotRegisteredError`, :class:`EventValidationError`,
:class:`UnauthorizedError`, :class:`ForbiddenError`, :class:`ValidationError`.

HTTP status mapping (handled in ``middleware/error_handler.py``)
----------------------------------------------------------------
The error-handler middleware translates each ``code`` to an HTTP status:

============================================== ====================
``code``                                        HTTP status
============================================== ====================
``inventory.unauthorized``                      401
``inventory.forbidden``                         403
``inventory.validation_failed``                 400
``inventory.insufficient_stock``                409
``inventory.reservation_not_found``             404
``inventory.warehouse_not_found``               404
``inventory.stock_item_not_found``              404
``inventory.reservation_already_exists``        409
``inventory.reservation_state_invalid``         409
``inventory.reservation_expired``               410
``inventory.warehouse_unavailable``             503
*everything else*                               500
============================================== ====================

AAP cross-references
--------------------
* **AAP Section 0.5.2.2 bullet 5** — these exceptions are the failure
  modes of the stock reservation engine and warehouse adapter
  abstraction.
* **AAP R-15** — retry policies depend on distinguishing transient
  (retryable) vs permanent (non-retryable) failures.
* **AAP R-17** — Kafka consumers route to retry or DLQ topics based
  on ``is_retryable``.
* **AAP R-21 / R-22** — ``UnauthorizedError`` and ``ForbiddenError``
  are the *only* exception classes the auth middleware should raise.
* **AAP R-26** — ``code`` and ``details`` are emitted as structured
  log fields by the logging middleware.
"""

from __future__ import annotations

from typing import Any


class InventoryServiceError(Exception):
    """Base class for all domain exceptions raised by the Inventory Service.

    Carries structured metadata that downstream layers (logging, error-handler
    middleware, Kafka retry/DLQ routing) consume to make consistent decisions.

    Attributes:
        code:         Stable, machine-readable identifier used in logs and
                      emitted error events. Subclasses override this.
        is_retryable: ``True`` if a caller may safely retry the operation that
                      raised this exception. Drives Kafka retry/DLQ routing
                      and tenacity retry decisions.
        message:      Human-readable description of the failure. Always equal
                      to the ``message`` argument passed to ``__init__``.
        details:      Free-form mapping of context that callers want to
                      attach to the exception for log enrichment.

    AAP References:
        * R-15: tenacity retry decisions test ``is_retryable``.
        * R-17: Kafka consumer DLQ routing tests ``is_retryable`` to choose
          between ``<topic>.retry`` (retryable) and ``<topic>.dlq`` (poison)
          destinations.
        * R-26: ``code`` and ``details`` are emitted as structured log fields.
    """

    code: str = "inventory.error"
    is_retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the exception with a human-readable message and optional context.

        Args:
            message: Human-readable description of the failure. Stored on
                ``self.message`` and forwarded to ``Exception.__init__`` so
                that ``str(exc)`` and traceback rendering both surface the
                same text.
            details: Optional mapping of structured context fields (e.g.
                ``{"order_id": ..., "product_id": ...}``). Copied into a
                fresh dict on each instance to avoid shared-mutable-default
                aliasing; callers are free to mutate the original mapping
                after construction without affecting this instance's
                ``details``.
        """
        super().__init__(message)
        self.message: str = message
        # Defensive copy: if ``details`` is supplied, copy into a new dict
        # so the caller cannot mutate this exception's context post-hoc and
        # we never alias a caller-owned dict. When ``details`` is ``None``
        # (or any falsy value), allocate a fresh empty dict — never a
        # shared mutable default.
        self.details: dict[str, Any] = dict(details) if details else {}

    def __str__(self) -> str:  # pragma: no cover - delegated to ``self.message``
        """Return the human-readable message.

        Identical to the value passed to ``__init__`` so that ``str(exc)``
        renders consistently regardless of subclass.
        """
        return self.message

    def to_dict(self) -> dict[str, Any]:
        """Render the exception as a structured dict for logging and error events.

        This is the canonical serialization used by the error-handler
        middleware (HTTP error responses) and the DLQ writer (the ``error``
        field of the ``inventory.dlq`` event payload). The returned dict
        always contains exactly these four keys:

        * ``code`` — the class-level ``code`` attribute.
        * ``message`` — the instance-level ``message`` attribute.
        * ``is_retryable`` — the class-level ``is_retryable`` attribute.
        * ``details`` — a fresh copy of the instance-level ``details`` dict
          so mutating the returned mapping does not affect the exception.

        Returns:
            A new ``dict[str, Any]`` describing the exception.
        """
        return {
            "code": self.code,
            "message": self.message,
            "is_retryable": self.is_retryable,
            "details": dict(self.details),
        }


# ---------------------------------------------------------------------------
# Reservation-domain exceptions
# ---------------------------------------------------------------------------


class InsufficientStockError(InventoryServiceError):
    """Stock reservation failed because available quantity is below the requested amount.

    NON-RETRYABLE: the caller must either reduce the requested quantity or
    accept the rejection. Routed to ``inventory.reservation_failed`` event
    by the ``order.created`` handler; never retried.
    """

    code = "inventory.insufficient_stock"
    is_retryable = False


class ReservationNotFoundError(InventoryServiceError):
    """No reservation matches the supplied ``order_id`` (or ``reservation_id``).

    NON-RETRYABLE: this is a logic error or an out-of-order event. The
    consumer routes the originating message to the per-source-topic DLQ
    so an operator can inspect.
    """

    code = "inventory.reservation_not_found"
    is_retryable = False


class ReservationAlreadyExistsError(InventoryServiceError):
    """A reservation already exists for the supplied ``order_id``.

    NON-RETRYABLE: by design (idempotency key). The Kafka ``order.created``
    handler treats ``INSERT ... ON CONFLICT (order_id) DO NOTHING``
    returning zero rows as a successful idempotent NOOP and DOES NOT
    propagate this exception in that path; this exception is reserved
    for unexpected callers (e.g. an admin endpoint trying to create a
    duplicate manual reservation).
    """

    code = "inventory.reservation_already_exists"
    is_retryable = False


class ReservationStateError(InventoryServiceError):
    """Reservation is not in the expected state for the requested transition.

    NON-RETRYABLE. Examples:

    * Trying to release a reservation that is already ``EXPIRED``.
    * Trying to fulfill a reservation that is already ``RELEASED``.
    * Trying to expire a reservation that is ``FULFILLED``.
    """

    code = "inventory.reservation_state_invalid"
    is_retryable = False


class ReservationExpiredError(InventoryServiceError):
    """Operation rejected because the reservation has already expired.

    NON-RETRYABLE: the caller (e.g. an admin endpoint or a delayed
    ``order.fulfilled`` event) must accept that the reservation can no
    longer be honored.
    """

    code = "inventory.reservation_expired"
    is_retryable = False


# ---------------------------------------------------------------------------
# Stock / optimistic-lock exceptions
# ---------------------------------------------------------------------------


class StockItemNotFoundError(InventoryServiceError):
    """No ``stock_items`` row matches the supplied ``(product_id, warehouse_id)``.

    NON-RETRYABLE: indicates a missing master-data row. Caller may need to
    create the stock item via the admin endpoint.
    """

    code = "inventory.stock_item_not_found"
    is_retryable = False


class OptimisticLockError(InventoryServiceError):
    """Concurrent write conflict on a row that uses optimistic locking.

    RETRYABLE: the caller (``StockRepository.update_stock_with_optimistic_lock``)
    will retry up to ``settings.reservation.optimistic_lock.max_retries``
    times with exponential backoff before re-raising. Outside the retry
    loop, the exception bubbles up and is routed to the per-source-topic
    retry topic by the Kafka consumer (AAP R-17).

    AAP References:
        * R-15: this is the canonical retryable exception; tenacity
          ``retry_if_exception_type(OptimisticLockError)`` is the
          policy driver in ``StockRepository``.
    """

    code = "inventory.optimistic_lock_conflict"
    is_retryable = True


class WarehouseNotFoundError(InventoryServiceError):
    """No ``warehouses`` row matches the supplied ``warehouse_id`` or ``name``.

    NON-RETRYABLE: master-data error.
    """

    code = "inventory.warehouse_not_found"
    is_retryable = False


class WarehouseUnavailableError(InventoryServiceError):
    """Warehouse exists but is in a non-operational status.

    NON-RETRYABLE for the current request. Status values that trigger:

    * ``MAINTENANCE``
    * ``DECOMMISSIONED``
    """

    code = "inventory.warehouse_unavailable"
    is_retryable = False


# ---------------------------------------------------------------------------
# Adapter / configuration exceptions
# ---------------------------------------------------------------------------


class AdapterNotRegisteredError(InventoryServiceError):
    """The requested ``adapter_type`` has no implementation registered.

    NON-RETRYABLE: indicates a configuration / deployment error. The
    container failed to register an adapter that a ``warehouses.adapter_type``
    column references.
    """

    code = "inventory.adapter_not_registered"
    is_retryable = False


class WarehouseAdapterError(InventoryServiceError):
    """Outbound call to a warehouse adapter failed (e.g. external WMS).

    RETRYABLE: a transient failure (network timeout, 5xx) at the WMS
    boundary. The Kafka consumer routes the inbound message to the retry
    topic; the HTTP client additionally retries internally via tenacity
    (AAP R-15).

    Subclasses MAY set ``is_retryable = False`` to indicate a permanent
    failure (e.g. 4xx response, schema mismatch).
    """

    code = "inventory.warehouse_adapter_failed"
    is_retryable = True


# ---------------------------------------------------------------------------
# Event / schema exceptions
# ---------------------------------------------------------------------------


class EventValidationError(InventoryServiceError):
    """Inbound or outbound Kafka event failed Schema Registry validation.

    NON-RETRYABLE: a malformed event will never validate even after
    retries. Routed straight to the per-source-topic DLQ on the consume
    side (AAP R-14, R-17).
    """

    code = "inventory.event_validation_failed"
    is_retryable = False


class EventPublishError(InventoryServiceError):
    """Failed to publish an ``inventory.*`` event to Kafka after retries.

    RETRYABLE within bounded scope: the ``event_producer`` has its own
    bounded retry loop. When this exception escapes, the consumer that
    owns the inbound message routes it to the retry / DLQ topic and
    refuses to commit the offset.
    """

    code = "inventory.event_publish_failed"
    is_retryable = True


# ---------------------------------------------------------------------------
# Authorization / validation exceptions
# ---------------------------------------------------------------------------


class UnauthorizedError(InventoryServiceError):
    """JWT validation failed or required scope is missing.

    NON-RETRYABLE: the API caller must obtain a valid token. Translated
    to HTTP 401 by the error-handler middleware.

    AAP References:
        * R-21, R-22: the only exception class permitted to be raised by
          ``JWKSClient.validate(token)`` and the auth middleware.
    """

    code = "inventory.unauthorized"
    is_retryable = False


class ForbiddenError(InventoryServiceError):
    """Token is valid but does not carry the required scope.

    NON-RETRYABLE. Translated to HTTP 403 by the error-handler middleware.
    """

    code = "inventory.forbidden"
    is_retryable = False


class ValidationError(InventoryServiceError):
    """Request payload failed domain-level validation.

    NON-RETRYABLE. Translated to HTTP 400 by the error-handler middleware.
    """

    code = "inventory.validation_failed"
    is_retryable = False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


__all__: list[str] = [
    "InventoryServiceError",
    # Reservation domain
    "InsufficientStockError",
    "ReservationNotFoundError",
    "ReservationAlreadyExistsError",
    "ReservationStateError",
    "ReservationExpiredError",
    # Stock + optimistic lock
    "StockItemNotFoundError",
    "OptimisticLockError",
    # Warehouse
    "WarehouseNotFoundError",
    "WarehouseUnavailableError",
    # Adapter
    "AdapterNotRegisteredError",
    "WarehouseAdapterError",
    # Events / schemas
    "EventValidationError",
    "EventPublishError",
    # Authorization / validation
    "UnauthorizedError",
    "ForbiddenError",
    "ValidationError",
]
