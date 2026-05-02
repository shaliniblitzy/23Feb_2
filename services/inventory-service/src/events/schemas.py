"""Pydantic v2 event payload models for the Inventory Service.

Canonical in-process representations of every Kafka event the Inventory
Service consumes from its upstream collaborators and emits to its
downstream consumers. Each model in this module mirrors a JSON Schema
registered with the Confluent Schema Registry under
``infrastructure/kafka/schemas/`` (AAP R-14 -- Schema Registry conformance).
Field names, types, and optionality MUST match the registered schema
exactly so that:

* Producer-side serialisation validates against the same shape the
  Schema Registry has registered (the registry rejects produce
  attempts whose payload does not conform).
* Consumer-side deserialisation yields strongly-typed Python objects
  for the saga handlers and the reservation-expiry scheduler.

Sibling reference patterns
--------------------------
* ``services/order-service/src/events/schemas.py`` -- canonical Pydantic
  v2 event-model pattern (private ``_BaseEvent``, frozen models, integer
  minor units, UUIDs, RFC 3339 datetimes).
* ``services/notification-service/src/events/schemas.py`` -- alternative
  variant for notification events.

Event taxonomy
--------------

Consumed (mirrors of order-service emitted events; AAP Section 0.4.2)
    * ``order.created``    -> :class:`OrderCreatedEvent`
    * ``order.cancelled``  -> :class:`OrderCancelledEvent`
    * ``order.fulfilled``  -> :class:`OrderFulfilledEvent`

Emitted (Inventory Service produces these on stock state changes;
AAP Section 0.5.2.2 bullet 5)
    * ``inventory.reserved``               -> :class:`InventoryReservedEvent`
    * ``inventory.reservation_failed``     -> :class:`InventoryReservationFailedEvent`
    * ``inventory.released``               -> :class:`InventoryReleasedEvent`
    * ``inventory.low-stock``              -> :class:`InventoryLowStockEvent`

NOTE: ``inventory.low-stock`` uses a HYPHEN (not a dot) by convention --
this is intentional and verified against ``infrastructure/kafka/topics.yaml``.
All other inventory events use DOT notation (``inventory.reserved``,
``inventory.released``, ``inventory.reservation_failed``).

Conventions enforced by every model in this module
--------------------------------------------------
* Frozen / immutable instances (``frozen=True``). Events are emitted by
  handlers and the expiry scheduler and must not be mutated after
  construction; mutation raises at runtime, preventing accidental
  side-effects in handlers that share the event across coroutines.
* Integer minor units for every monetary amount (cents, paisa). Floats
  are NEVER used for currency -- minor-unit integers are language-
  agnostic and serialise cleanly to JSON-Schema ``"type": "integer"``.
* All identifiers are :class:`uuid.UUID`. Pydantic v2 serialises ``UUID``
  to a string in ``model_dump(mode="json")``, matching the JSON-Schema
  ``"format": "uuid"`` constraint.
* All timestamps are timezone-aware :class:`datetime.datetime` (RFC 3339
  / ISO 8601). Pydantic v2 serialises them to RFC 3339 strings in JSON
  mode. Field names use the ``_at`` suffix consistently
  (``occurred_at``, ``fulfilled_at``, ``expires_at``).
* Every concrete event inherits the private :class:`_BaseEvent` and
  thereby carries ``event_id``, ``event_version``, ``correlation_id``,
  and ``occurred_at`` -- the unified envelope the producer stamps onto
  every wire payload.

Side-effect freedom
-------------------
This module performs ZERO logging, ZERO I/O, and ZERO mutable global
state. It imports ONLY from the Python standard library and Pydantic.
It MUST NOT import from ``src.events.producer``, ``src.events.consumer``,
``src.events.handlers``, or any other ``src.*`` sibling -- those modules
depend on this one; importing back would create a circular dependency.
This is the leaf module of the events sub-package.

Authority
---------
* AAP Section 0.4.2 -- Inventory consumes ``order.created``,
  ``order.cancelled``, ``order.fulfilled``. Inventory emits
  ``inventory.reserved``, ``inventory.reservation_failed``,
  ``inventory.released``, ``inventory.low-stock``.
* AAP Section 0.5.2.2 bullet 5 -- Inventory Service is the stock
  reservation engine; emits ``inventory.reserved`` /
  ``inventory.released`` / ``inventory.low-stock`` after stock state
  changes.
* AAP R-14 (KEYSTONE) -- All Kafka events validated against Schema
  Registry; this module mirrors the registered JSON-Schemas exactly.
* AAP R-30 -- Topic names mirror event names (``inventory.reserved``
  carries :class:`InventoryReservedEvent`, etc.).
* AAP R-31 (KEYSTONE) -- Every event payload includes an
  ``event_version: int`` field (``ge=1``) for backward-compatible
  evolution. Producers also stamp the value into a Kafka header.
* AAP R-33 -- Events are self-contained; consumers do not need to call
  back to the producer to act on the payload.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# =============================================================================
# Shared base class -- private; not exported.
# =============================================================================


class _BaseEvent(BaseModel):
    """Common envelope fields for every event handled by the Inventory Service.

    Carries identity (``event_id``), versioning (``event_version`` per
    AAP R-31), distributed tracing (``correlation_id`` per AAP R-13),
    and ordering (``occurred_at``, RFC 3339 timezone-aware).

    Frozen for safety: events are emitted by handlers and the expiry
    scheduler and must not be mutated after construction.
    ``extra="ignore"`` allows forward-compatible evolution of the
    on-the-wire schema -- consumers silently drop fields they do not
    recognise (AAP R-31), so a producer at schema version N+1 does not
    break consumers built against schema version N.

    The class is intentionally PRIVATE (leading underscore): it has no
    semantic meaning outside this module and must not appear in
    :data:`__all__`. Consumer code should depend on the concrete event
    classes, not on this base.

    Subclasses MUST NOT redeclare any of the four envelope fields --
    they are inherited unchanged from this base (DRY; matches AAP R-31
    expectation that ``event_version`` is uniformly present on every
    event).
    """

    # ------------------------------------------------------------------
    # Pydantic v2 model configuration.
    #
    # ``frozen=True``                -- immutability + hashability.
    # ``str_strip_whitespace=True``  -- trim whitespace from string
    #                                   fields before subsequent
    #                                   validation runs.
    # ``extra="ignore"``             -- silently drop unknown fields so
    #                                   a producer at schema version
    #                                   N+1 does not break consumers
    #                                   built against schema version N
    #                                   (AAP R-14 / R-31 forward-compat).
    # ``populate_by_name=True``      -- alias-tolerant construction so
    #                                   future field aliases work for
    #                                   both alias and Python name.
    # ------------------------------------------------------------------
    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="ignore",
        populate_by_name=True,
    )

    event_id: UUID = Field(
        ...,
        description=(
            "Globally unique event identifier (UUID v4) used for "
            "consumer-side idempotency and distributed tracing. "
            "Pydantic v2 serialises UUIDs to strings in JSON mode."
        ),
    )
    event_version: int = Field(
        ...,
        ge=1,
        description=(
            "Schema version; bumped on backward-incompatible evolution "
            "(AAP R-31). Must be >= 1; 0 is reserved as the int default "
            "sentinel and rejected at validation time. Producers also "
            "surface this value as the ``schema-version`` Kafka header "
            "for fast-skip in consumers."
        ),
    )
    correlation_id: str = Field(
        ...,
        min_length=1,
        description=(
            "End-to-end correlation ID (AAP R-13). Stamped at the API "
            "Gateway on the inbound HTTP request and propagated through "
            "every downstream service via the ``X-Correlation-ID`` "
            "header and Kafka message headers, enabling distributed "
            "tracing across the entire saga."
        ),
    )
    occurred_at: datetime = Field(
        ...,
        description=(
            "RFC 3339 timezone-aware timestamp of the domain occurrence "
            "(NOT the time the event was published). Pydantic v2 "
            "serialises this to an ISO 8601 string in JSON mode."
        ),
    )


# =============================================================================
# Shared enums -- exported.
# =============================================================================


class CancellationReason(str, Enum):
    """Cancellation reason codes mirrored from the order-service domain.

    Inventory uses the reason for stock-movement metadata and DLQ
    envelope diagnostics; the cancel handler does not branch on the
    value (the action is uniformly "release the reservation tied to
    the order_id"), but the value is propagated onto the
    :class:`InventoryReleasedEvent` so downstream consumers
    (Notification Service, Recommendation Engine) can surface the
    reason in user-facing messages and re-ranking signals.

    The values are duplicated from the order-service ``CancellationReason``
    enum (which is a free-form domain value owned by the order-service
    saga) and MUST stay in lockstep with that definition. Any drift
    would cause Schema Registry incompatibility on the consumer side.

    Inheriting from ``str`` makes instances JSON-serialise to their
    string value, matching the registered Schema Registry JSON-Schema
    enum constraint and enabling typed branching by the saga
    coordinator without free-form string parsing.
    """

    USER_REQUESTED = "USER_REQUESTED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    INVENTORY_UNAVAILABLE = "INVENTORY_UNAVAILABLE"
    SAGA_COMPENSATION = "SAGA_COMPENSATION"
    EXPIRED = "EXPIRED"
    FRAUD_SUSPECTED = "FRAUD_SUSPECTED"
    OTHER = "OTHER"


class ReservationFailureReason(str, Enum):
    """Reasons for a reservation failure event.

    Captures the failure taxonomy at the schema level so the saga
    coordinator (Order Service) can branch on a typed value rather
    than a free-form string. The reservation handler determines the
    reason from the exception type raised inside the reservation
    transaction:

    * :class:`InsufficientStockError`     -> :attr:`INSUFFICIENT_STOCK`
    * :class:`WarehouseUnavailableError`  -> :attr:`WAREHOUSE_UNAVAILABLE`
    * :class:`StockItemNotFoundError`     -> :attr:`STOCK_ITEM_NOT_FOUND`
    * :class:`ValidationError`            -> :attr:`VALIDATION_FAILED`
    * any other (unrecognised)            -> :attr:`SYSTEM_ERROR`

    Inheriting from ``str`` makes instances JSON-serialise to their
    string value, matching the registered Schema Registry JSON-Schema
    enum constraint.
    """

    INSUFFICIENT_STOCK = "INSUFFICIENT_STOCK"
    WAREHOUSE_UNAVAILABLE = "WAREHOUSE_UNAVAILABLE"
    STOCK_ITEM_NOT_FOUND = "STOCK_ITEM_NOT_FOUND"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    SYSTEM_ERROR = "SYSTEM_ERROR"


# =============================================================================
# Shared value objects -- exported for handler / test reuse.
# =============================================================================


class OrderItem(BaseModel):
    """A single line item on an inbound order event.

    Mirrors the ``OrderItem`` shape emitted by the order-service in its
    ``order.created`` payload. Inventory uses ``product_id`` and
    ``quantity`` to locate stock and compute reservations; the price
    fields (``unit_price_minor_units``, ``currency``) are passed
    through to outbound events for traceability only -- no math is
    performed on them by this service.

    Frozen for the same reasons as :class:`_BaseEvent`: hashable,
    safely shareable across coroutines, and immune to accidental
    in-flight mutation by handlers.

    Attributes:
        product_id: Opaque cross-service product identifier (Product
            Service catalog id). NOT a foreign key per AAP R-6 -- the
            Inventory Service has no read-through to the Product
            Service's database.
        sku: Optional human-readable stock-keeping unit code. Bounded
            length so log lines stay sized predictably.
        quantity: Number of units requested for this line; ``>= 1``.
            Zero-quantity lines would be a producer bug and are
            rejected at validation time.
        unit_price_minor_units: Per-unit price in the parent order's
            currency minor units (cents/paisa); ``>= 0``. Zero is
            permitted to support promotional bundles ("buy one, get
            one free" sets the bonus line's unit price to 0).
        currency: ISO 4217 three-letter currency code (validated
            length-only at this layer; the order-service is the source
            of truth for currency code validity).
    """

    model_config = ConfigDict(frozen=True, extra="ignore", populate_by_name=True)

    product_id: UUID = Field(
        ...,
        description=(
            "Opaque cross-service product identifier (Product Service "
            "catalog id). NOT a foreign key per AAP R-6."
        ),
    )
    sku: Optional[str] = Field(
        default=None,
        max_length=128,
        description=(
            "Optional human-readable stock-keeping unit code "
            "propagated from the order-service for log/observability "
            "convenience. Bounded length so structured log lines "
            "remain predictably sized."
        ),
    )
    quantity: int = Field(
        ...,
        ge=1,
        description=(
            "Number of units requested for this line; must be >= 1. "
            "Zero-quantity lines indicate a producer bug and are "
            "rejected at validation time."
        ),
    )
    unit_price_minor_units: int = Field(
        ...,
        ge=0,
        description=(
            "Per-unit price in the parent order's currency minor "
            "units (cents/paisa). Integer to avoid currency-precision "
            "errors. Zero is permitted to support promotional bundles."
        ),
    )
    currency: str = Field(
        ...,
        min_length=3,
        max_length=3,
        description=(
            "ISO 4217 three-letter currency code (e.g. 'USD', 'INR'). "
            "Length validated here; the order-service is the source "
            "of truth for currency-code validity."
        ),
    )


class ReservedItem(BaseModel):
    """A line item on an :class:`InventoryReservedEvent`.

    Identifies the per-product reservation outcome including the
    warehouse that fulfilled the request. The post-reservation stock
    state (``available_qty_after``, ``reserved_qty_after``) is
    included so the Recommendation Engine and other downstream
    consumers can surface low-stock state without re-querying the
    Inventory Service (AAP R-33 -- self-contained events).

    Frozen for hashability and immutability.

    Attributes:
        product_id: The product whose stock was reserved.
        warehouse_id: The warehouse that fulfilled the reservation
            (multi-warehouse fulfillment is supported via the
            ``WarehouseAdapter`` abstraction).
        quantity: Number of units reserved on this line.
        available_qty_after: Remaining available stock for the
            ``(product_id, warehouse_id)`` tuple AFTER the reservation
            applied. Lets downstream consumers compute "is this product
            now low?" without a callback.
        reserved_qty_after: Total reserved stock for the
            ``(product_id, warehouse_id)`` tuple AFTER the reservation
            applied. Useful for capacity-planning consumers.
    """

    model_config = ConfigDict(frozen=True, extra="ignore", populate_by_name=True)

    product_id: UUID = Field(
        ...,
        description="The product whose stock was reserved.",
    )
    warehouse_id: UUID = Field(
        ...,
        description=(
            "The warehouse that fulfilled the reservation. "
            "Resolved by the WarehouseAdapter registry per the "
            "``warehouses.adapter_type`` column."
        ),
    )
    quantity: int = Field(
        ...,
        ge=1,
        description="Number of units reserved on this line; must be >= 1.",
    )
    available_qty_after: int = Field(
        ...,
        ge=0,
        description=(
            "Remaining available stock for the (product_id, "
            "warehouse_id) tuple AFTER the reservation applied. "
            "Enables downstream consumers (Recommendation Engine) to "
            "compute low-stock state without a callback (AAP R-33)."
        ),
    )
    reserved_qty_after: int = Field(
        ...,
        ge=0,
        description=(
            "Total reserved stock for the (product_id, warehouse_id) "
            "tuple AFTER the reservation applied. Useful for "
            "capacity-planning consumers."
        ),
    )


class ReleasedItem(BaseModel):
    """A line item on an :class:`InventoryReleasedEvent`.

    Same identifiers as :class:`ReservedItem`, plus the post-release
    stock state so downstream consumers can update caches and
    aggregates without callbacks (AAP R-33).

    Frozen for hashability and immutability.

    Attributes:
        product_id: The product whose stock was released.
        warehouse_id: The warehouse that held the reservation.
        quantity: Number of units released on this line.
        available_qty_after: Available stock for the
            ``(product_id, warehouse_id)`` tuple AFTER the release
            applied. For ``final=True`` releases (order fulfilled,
            stock has shipped), this value will NOT have been
            incremented by the released quantity -- the stock has
            departed. For ``final=False`` releases (cancel/expiry),
            available_qty_after will reflect the returned stock.
        reserved_qty_after: Total reserved stock for the
            ``(product_id, warehouse_id)`` tuple AFTER the release
            applied (always decremented).
    """

    model_config = ConfigDict(frozen=True, extra="ignore", populate_by_name=True)

    product_id: UUID = Field(
        ...,
        description="The product whose stock was released.",
    )
    warehouse_id: UUID = Field(
        ...,
        description="The warehouse that held the reservation.",
    )
    quantity: int = Field(
        ...,
        ge=1,
        description="Number of units released on this line; must be >= 1.",
    )
    available_qty_after: int = Field(
        ...,
        ge=0,
        description=(
            "Available stock for the (product_id, warehouse_id) tuple "
            "AFTER the release applied. For ``final=True`` releases "
            "(order fulfilled), the stock has shipped and this value "
            "is NOT incremented; for ``final=False`` releases "
            "(cancel/expiry), this value reflects the returned stock."
        ),
    )
    reserved_qty_after: int = Field(
        ...,
        ge=0,
        description=(
            "Total reserved stock for the (product_id, warehouse_id) "
            "tuple AFTER the release applied. Always decremented by "
            "the released quantity regardless of the ``final`` flag."
        ),
    )



# =============================================================================
# Consumed events -- mirrors of order-service emitted events.
# =============================================================================


class OrderCreatedEvent(_BaseEvent):
    """Mirrors order-service ``OrderCreatedEvent`` (topic: ``order.created``).

    The inventory ``OrderCreatedHandler`` reserves stock for each item
    in the order. Idempotency is enforced via ``order_id``: the
    ``reservations`` table has a UNIQUE constraint on ``order_id`` and
    the handler uses ``INSERT ... ON CONFLICT (order_id) DO NOTHING``
    so duplicate ``order.created`` events (a consequence of Kafka's
    at-least-once delivery semantics on consumer rebalances) become
    NOOPs after the first successful reservation. The prior
    reservation outcome is then re-emitted to keep the saga driving
    forward.

    Self-contained per AAP R-33: every field the inventory handler
    needs to perform the reservation is present on the event. The
    handler never calls back to the order-service to "resolve" the
    event.

    Attributes:
        order_id: Saga-driving order identifier; used as the
            idempotency key.
        user_id: The user who placed the order; carried forward onto
            outbound events for downstream observability.
        items: One or more line items to reserve; ``min_length=1``.
        total_amount_minor_units: Total order amount in minor units;
            passed through for traceability only -- inventory does
            not perform any math on it.
        currency: ISO 4217 currency code; same pass-through semantics
            as the per-line-item currency.
        saga_id: Optional saga correlator. The order-service may not
            always set it (e.g. for non-saga test events) and the
            inventory handlers MUST NOT require it.
    """

    order_id: UUID = Field(
        ...,
        description=(
            "Saga-driving order identifier; used as the idempotency "
            "key on the ``reservations`` table."
        ),
    )
    user_id: UUID = Field(
        ...,
        description=(
            "The user who placed the order; carried forward onto "
            "outbound events for downstream observability."
        ),
    )
    items: list[OrderItem] = Field(
        ...,
        min_length=1,
        description=(
            "One or more line items to reserve. ``min_length=1`` "
            "because an empty order is meaningless and would indicate "
            "a producer bug."
        ),
    )
    total_amount_minor_units: int = Field(
        ...,
        ge=0,
        description=(
            "Total order amount in minor units. Pass-through only -- "
            "inventory does not perform any math on it."
        ),
    )
    currency: str = Field(
        ...,
        min_length=3,
        max_length=3,
        description="ISO 4217 three-letter currency code (pass-through).",
    )
    saga_id: Optional[UUID] = Field(
        default=None,
        description=(
            "Optional saga correlator stamped by the Order Service "
            "saga coordinator. May be absent on non-saga test events; "
            "handlers MUST NOT require it."
        ),
    )


class OrderCancelledEvent(_BaseEvent):
    """Mirrors order-service ``OrderCancelledEvent`` (topic: ``order.cancelled``).

    The inventory ``OrderCancelledHandler`` looks up the active
    reservation by ``order_id`` (if any) and releases it: returns
    reserved stock to availability and emits
    :class:`InventoryReleasedEvent` with ``final=False`` and
    ``expired=False``. If no active reservation exists for the
    ``order_id`` (e.g. cancel arrived before reserve, or reserve
    already failed), the handler is a NOOP.

    Self-contained per AAP R-33: the cancellation reason is carried in
    the payload so the inventory handler does not have to call back to
    the order-service for context.

    Attributes:
        order_id: The order whose reservation should be released.
        user_id: Optional user identifier; absent for cases where the
            cancellation is system-driven (e.g. saga compensation).
        cancellation_reason: Typed enumeration of the reason; see
            :class:`CancellationReason`.
        saga_id: Optional saga correlator.
    """

    order_id: UUID = Field(
        ...,
        description="The order whose reservation should be released.",
    )
    user_id: Optional[UUID] = Field(
        default=None,
        description=(
            "Optional user identifier; absent for cases where the "
            "cancellation is system-driven (e.g. saga compensation)."
        ),
    )
    cancellation_reason: CancellationReason = Field(
        ...,
        description=(
            "Typed enumeration of the cancellation reason. Inventory "
            "propagates this onto the outbound :class:"
            "`InventoryReleasedEvent` for downstream consumers."
        ),
    )
    saga_id: Optional[UUID] = Field(
        default=None,
        description="Optional saga correlator.",
    )


class OrderFulfilledEvent(_BaseEvent):
    """Mirrors order-service ``OrderFulfilledEvent`` (topic: ``order.fulfilled``).

    The inventory ``OrderFulfilledHandler`` finalises the reservation:
    ``reserved_qty`` is decremented but ``available_qty`` is NOT
    restored (the stock has shipped). Emits
    :class:`InventoryReleasedEvent` with ``final=True`` and
    ``expired=False``. Like the cancel handler, this handler is a
    NOOP if no active reservation exists for the ``order_id``.

    Self-contained per AAP R-33.

    Attributes:
        order_id: The order whose reservation should be finalised.
        user_id: Optional user identifier; absent for system-driven
            fulfillments.
        fulfilled_at: RFC 3339 timezone-aware timestamp of the
            fulfillment occurrence (NOT the time the event was
            published; ``occurred_at`` from :class:`_BaseEvent`
            covers that).
        saga_id: Optional saga correlator.
    """

    order_id: UUID = Field(
        ...,
        description="The order whose reservation should be finalised.",
    )
    user_id: Optional[UUID] = Field(
        default=None,
        description=(
            "Optional user identifier; absent for system-driven "
            "fulfillments."
        ),
    )
    fulfilled_at: datetime = Field(
        ...,
        description=(
            "RFC 3339 timezone-aware timestamp of the fulfillment "
            "occurrence (NOT the time the event was published)."
        ),
    )
    saga_id: Optional[UUID] = Field(
        default=None,
        description="Optional saga correlator.",
    )



# =============================================================================
# Emitted events -- produced by the Inventory Service.
# =============================================================================


class InventoryReservedEvent(_BaseEvent):
    """Topic ``inventory.reserved`` -- emitted on successful stock reservation.

    Carried by the saga: the Order Service consumes this event to
    advance the order state to ``AWAITING_PAYMENT``; the Notification
    Service consumes it for "Your order is confirmed" emails. The
    Recommendation Engine consumes it as a strong purchase-intent
    signal for personalisation.

    Self-contained per AAP R-33: the per-line ``available_qty_after``
    and ``reserved_qty_after`` allow the Recommendation Engine to
    update its low-stock awareness without re-querying inventory.

    Attributes:
        order_id: The order this reservation belongs to.
        reservation_id: The new reservation's primary key (UUID v4
            generated at reserve time). Distinct from ``order_id`` so
            that the reservation lifecycle can be observed
            independently of the order lifecycle.
        expires_at: Deadline at which the reservation will be
            auto-released by the expiry scheduler if not finalised by
            an ``order.cancelled`` or ``order.fulfilled`` (AAP R-20 --
            fallback path against stuck sagas).
        items: Per-line reservation outcomes; ``min_length=1``.
        saga_id: Optional saga correlator (mirrored from the
            ``order.created`` event that triggered the reservation).
    """

    order_id: UUID = Field(
        ...,
        description="The order this reservation belongs to.",
    )
    reservation_id: UUID = Field(
        ...,
        description=(
            "The new reservation's primary key (UUID v4 generated at "
            "reserve time). Distinct from ``order_id`` so the "
            "reservation lifecycle can be observed independently."
        ),
    )
    expires_at: datetime = Field(
        ...,
        description=(
            "When the reservation will auto-expire if not finalised "
            "(AAP R-20 -- fallback path against stuck sagas). The "
            "expiry scheduler scans for reservations whose "
            "``expires_at`` has passed and emits "
            ":class:`InventoryReleasedEvent` with ``expired=True``."
        ),
    )
    items: list[ReservedItem] = Field(
        ...,
        min_length=1,
        description=(
            "Per-line reservation outcomes including post-reservation "
            "stock state; ``min_length=1`` because a successful "
            "reservation must touch at least one line."
        ),
    )
    saga_id: Optional[UUID] = Field(
        default=None,
        description=(
            "Optional saga correlator (mirrored from the "
            "``order.created`` event that triggered the reservation)."
        ),
    )


class InventoryReservationFailedEvent(_BaseEvent):
    """Topic ``inventory.reservation_failed`` -- emitted on reservation failure.

    Drives saga compensation in the Order Service: the order saga
    coordinator transitions the order to ``CANCELLED`` with reason
    :attr:`CancellationReason.INVENTORY_UNAVAILABLE` (or another reason
    derived from the failure). Insufficient stock is a normal business
    outcome and is NOT a DLQ event -- treating it as an exception
    would conflate infrastructure failures with business logic, both
    of which require very different operational responses.

    Self-contained per AAP R-33: the typed ``reason`` enumeration
    lets the saga coordinator branch deterministically without
    free-form string parsing.

    Attributes:
        order_id: The order whose reservation failed.
        reason: Typed failure reason; see :class:`ReservationFailureReason`.
        failed_product_ids: Optional list of product IDs that could
            not be reserved (empty list permitted; some failures are
            global and not tied to specific products, e.g.
            ``WAREHOUSE_UNAVAILABLE``).
        message: Optional human-readable failure detail; bounded
            length so DLQ rows stay sized predictably.
        saga_id: Optional saga correlator (mirrored from the
            triggering ``order.created`` event).
    """

    order_id: UUID = Field(
        ...,
        description="The order whose reservation failed.",
    )
    reason: ReservationFailureReason = Field(
        ...,
        description=(
            "Typed failure reason. Lets the Order Service saga "
            "coordinator branch deterministically without free-form "
            "string parsing."
        ),
    )
    failed_product_ids: list[UUID] = Field(
        default_factory=list,
        description=(
            "Product IDs that could not be reserved. May be an empty "
            "list -- some failures are global (e.g. "
            "``WAREHOUSE_UNAVAILABLE``) and not tied to specific "
            "products."
        ),
    )
    message: Optional[str] = Field(
        default=None,
        max_length=1000,
        description=(
            "Optional human-readable failure detail (truncated to "
            "1000 chars so DLQ rows stay sized predictably). Avoid "
            "including secrets or PII."
        ),
    )
    saga_id: Optional[UUID] = Field(
        default=None,
        description=(
            "Optional saga correlator (mirrored from the triggering "
            "``order.created`` event)."
        ),
    )


class InventoryReleasedEvent(_BaseEvent):
    """Topic ``inventory.released`` -- emitted on reservation release.

    A single event topic carries three release pathways, distinguished
    by the ``final`` and ``expired`` flags:

    * ``final=False, expired=False`` -- reservation was cancelled by
      an ``order.cancelled`` event. Available stock IS restored.
    * ``final=False, expired=True``  -- reservation was released by
      the expiry scheduler (AAP R-20 fallback path). Available stock
      IS restored. The ``cancellation_reason`` is typically
      :attr:`CancellationReason.EXPIRED`.
    * ``final=True,  expired=False`` -- reservation was finalised by
      an ``order.fulfilled`` event. Stock has shipped, so
      ``reserved_qty`` is decremented but ``available_qty`` is NOT
      restored.

    The combination ``final=True, expired=True`` is meaningless and
    will not be emitted. Consumers branch on ``expired`` for
    fallback-path detection (e.g. the Notification Service may send
    a different message when stock was released by the expiry
    scheduler vs by an explicit user cancellation).

    Self-contained per AAP R-33: the per-line items carry post-release
    stock state.

    Attributes:
        order_id: The order whose reservation was released.
        reservation_id: The reservation's primary key.
        items: Per-line release outcomes; ``min_length=1``.
        final: True if the release is from order fulfilled (stock has
            shipped); False if cancelled or expired.
        expired: True if the release is from the expiry scheduler
            (AAP R-20 fallback path); False if released by an
            ``order.cancelled`` or ``order.fulfilled`` event.
        cancellation_reason: Optional reason; populated when the
            release is driven by an ``order.cancelled`` event or by
            the expiry scheduler. None for ``final=True`` releases.
        saga_id: Optional saga correlator (mirrored from the
            triggering event).
    """

    order_id: UUID = Field(
        ...,
        description="The order whose reservation was released.",
    )
    reservation_id: UUID = Field(
        ...,
        description="The reservation's primary key (carried for traceability).",
    )
    items: list[ReleasedItem] = Field(
        ...,
        min_length=1,
        description=(
            "Per-line release outcomes including post-release stock "
            "state; ``min_length=1`` because a successful release "
            "must touch at least one line."
        ),
    )
    final: bool = Field(
        ...,
        description=(
            "True if the release is from order fulfilled (stock has "
            "shipped); False if cancelled or expired. The ``final`` "
            "flag tells consumers whether available stock has been "
            "restored or shipped out of the warehouse."
        ),
    )
    expired: bool = Field(
        ...,
        description=(
            "True if the release is from the expiry scheduler "
            "(AAP R-20 fallback path); False if released by an "
            "``order.cancelled`` or ``order.fulfilled`` event. "
            "Consumers may branch on this flag to surface different "
            "user-facing messages for system-driven vs user-driven "
            "cancellations."
        ),
    )
    cancellation_reason: Optional[CancellationReason] = Field(
        default=None,
        description=(
            "Optional reason; populated when the release is driven by "
            "an ``order.cancelled`` event or by the expiry scheduler. "
            "None for ``final=True`` releases (fulfillment has no "
            "cancellation reason)."
        ),
    )
    saga_id: Optional[UUID] = Field(
        default=None,
        description=(
            "Optional saga correlator (mirrored from the triggering "
            "event)."
        ),
    )


class InventoryLowStockEvent(_BaseEvent):
    """Topic ``inventory.low-stock`` (HYPHENATED, not dotted).

    Emitted post-reserve when ``available_qty <= low_stock_threshold``
    for a ``(product_id, warehouse_id)`` tuple. Consumed by:

    * The Recommendation Engine -- to deboost low-stock products in
      its rankings (a low-stock product surfaced to a user only to
      become unavailable at checkout is a poor UX signal).
    * The Notification Service -- to send operational alerts to the
      merchant operations team so that procurement can replenish.

    The topic name uses a HYPHEN per AAP convention; this is
    intentional and verified against ``infrastructure/kafka/topics.yaml``.
    All other inventory events use DOT notation
    (``inventory.reserved``, ``inventory.released``,
    ``inventory.reservation_failed``).

    Self-contained per AAP R-33: ``available_qty`` and
    ``low_stock_threshold`` are both carried so the consumer can
    compute severity (e.g. "critical" if available_qty is 0,
    "warning" if available_qty is >0 but <= threshold).

    Attributes:
        product_id: The product that crossed below threshold.
        warehouse_id: The warehouse hosting the low stock.
        available_qty: Current available stock; ``>= 0``.
        low_stock_threshold: The threshold that was crossed; ``>= 0``.
        sku: Optional human-readable SKU code, propagated for log
            convenience.
    """

    product_id: UUID = Field(
        ...,
        description="The product that crossed below threshold.",
    )
    warehouse_id: UUID = Field(
        ...,
        description="The warehouse hosting the low stock.",
    )
    available_qty: int = Field(
        ...,
        ge=0,
        description=(
            "Current available stock for the (product_id, "
            "warehouse_id) tuple at the moment of emission."
        ),
    )
    low_stock_threshold: int = Field(
        ...,
        ge=0,
        description=(
            "The threshold that was crossed (admin-configurable per "
            "SKU and per warehouse). Carried so consumers can compute "
            "severity without re-querying inventory."
        ),
    )
    sku: Optional[str] = Field(
        default=None,
        max_length=128,
        description=(
            "Optional human-readable SKU code propagated for log "
            "convenience. Bounded length so structured log lines "
            "remain predictably sized."
        ),
    )


# =============================================================================
# Public re-export -- grouped by category, alphabetised within each group.
# =============================================================================

__all__ = [
    # Base types and enums
    "CancellationReason",
    "OrderItem",
    "ReservationFailureReason",
    "ReservedItem",
    "ReleasedItem",
    # Consumed events
    "OrderCancelledEvent",
    "OrderCreatedEvent",
    "OrderFulfilledEvent",
    # Emitted events
    "InventoryLowStockEvent",
    "InventoryReleasedEvent",
    "InventoryReservationFailedEvent",
    "InventoryReservedEvent",
]

