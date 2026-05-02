"""Pydantic v2 event schemas for the Order Service.

In-process representations of the canonical JSON-Schemas registered in
the Confluent Schema Registry (under ``infrastructure/kafka/schemas/``).
Every event payload carries a ``version: int`` field per AAP R-31 so
backward-compatible schema evolution is auditable end-to-end.

Models exported:

  Produced (3, AAP Section 0.4.2):
    * :class:`OrderCreatedEvent`          -> topic ``order.created``
    * :class:`OrderCancelledEvent`        -> topic ``order.cancelled``
    * :class:`OrderFulfilledEvent`        -> topic ``order.fulfilled``

  Consumed (4, drive the saga state machine per AAP R-18):
    * :class:`InventoryReservedEvent`             -> topic ``inventory.reserved``
    * :class:`InventoryReservationFailedEvent`    -> topic ``inventory.reservation_failed``
    * :class:`PaymentSucceededEvent`              -> topic ``payment.succeeded``
    * :class:`PaymentFailedEvent`                 -> topic ``payment.failed``

  Shared value objects:
    * :class:`OrderItem`           -- line-item payload reused by the four events
                                       that need to carry the order's line items.
    * :class:`CancellationReason`  -- canonical string-enum of the reasons an
                                       order can be cancelled (consumed by
                                       :class:`OrderCancelledEvent`).

All event classes inherit from a private :class:`_BaseEvent` that enforces:

- ``frozen=True``                -- instances are hashable and immutable; safe
  to share across asyncio coroutines and to use as dict keys; mutation raises
  at runtime (preventing accidental side-effects in handlers).
- ``str_strip_whitespace=True``  -- incoming string fields are trimmed at the
  validation boundary so whitespace-only inputs are caught by ``min_length``
  constraints rather than slipping through.
- ``extra="ignore"``              -- forward-compatible by design: a consumer
  built against version N silently ignores fields added in version N+1.
  Combined with ``version: int`` this realises AAP R-14 (Schema Registry
  validates produce; consumers tolerate evolution).
- ``populate_by_name=True``       -- field aliases (if any) work for both alias
  and Python attribute name; future-proofs schema evolution.
- ``version: int`` field with ``ge=1`` -- declared once on the base class so
  authors of new events cannot forget it (AAP R-31). The ``Field(..., ge=1)``
  annotation makes 0 (the int default) invalid, preventing accidentally-
  uninitialised events.

Money in minor units
--------------------
All monetary amounts are integers in the currency's minor units (cents,
paisa, etc.). ``Decimal`` is intentionally NOT used at the event layer:
minor-unit integers are language-agnostic and serialise cleanly to
JSON-Schema ``"type": "integer"``. Currency is an ISO 4217 code validated
as exactly 3 uppercase ASCII letters.

UUIDs and Datetimes
-------------------
All identifiers (``order_id``, ``saga_id``, ``user_id``, ``payment_id``,
``reservation_id``, ``product_id``) are :class:`uuid.UUID` to avoid
collision between services and to keep schemas language-agnostic.
Pydantic v2 serialises ``UUID`` to a string in ``model_dump(mode="json")``,
matching the JSON-Schema ``"format": "uuid"`` constraint.

All timestamps are :class:`datetime.datetime` (timezone-aware, UTC).
Pydantic v2 serialises them to RFC 3339 strings in JSON mode. Field names
use the suffix ``_at`` consistently (``occurred_at``, ``reserved_at``,
``captured_at``, ``failed_at``, ``fulfilled_at``).

Side-effect freedom
-------------------
This module performs ZERO logging, ZERO I/O, and ZERO mutable global state.
It imports ONLY from the Python standard library and Pydantic. It MUST NOT
import from :mod:`src.events.producer`, :mod:`src.events.consumer`, or any
handler -- those modules depend on this one; importing back would create a
circular dependency.

Authority
---------
- AAP Section 0.4.2  -- Order Service produces ``order.*`` topics; consumes
                        ``inventory.*`` and ``payment.*`` topics.
- AAP Section 0.4.4  -- ``order_db`` schema (orders, order_items,
                        order_status_history, saga_state); the order/item
                        shapes here mirror those tables.
- AAP R-14           -- Every event is validated against the Schema Registry;
                        this file mirrors the registered JSON-Schemas exactly.
- AAP R-18           -- Saga pattern with explicit compensation steps; the
                        consumed events drive saga state transitions.
- AAP R-30           -- Topic names mirror event names: the ``order.created``
                        topic carries ``OrderCreatedEvent`` payloads.
- AAP R-31           -- Every event payload includes a ``version: int`` field
                        for backward-compatible evolution.
- AAP R-32           -- Producers do not enumerate consumers; consumers parse
                        self-contained payloads.
- AAP R-33           -- Events are self-contained; consumers do not need to
                        call back to the producer to act on the payload.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# =============================================================================
# Shared base class -- private; not exported.
# =============================================================================


class _BaseEvent(BaseModel):
    """Common base for every event in this module.

    All concrete event classes inherit from this base and therefore obtain:

    * Frozen / immutable instances (safe to share across asyncio coroutines;
      hashable and usable as dict keys).
    * String whitespace stripping at the validation boundary (defensive).
    * Forward-compatibility (``extra="ignore"``): unknown fields from a newer
      producer are silently dropped instead of raising at the consumer.
    * The mandatory ``version: int`` field (AAP R-31). ``Field(..., ge=1)``
      makes ``0`` invalid, preventing accidentally-uninitialised events.
    * ``populate_by_name=True`` so future field aliases work transparently.

    The class is intentionally PRIVATE (leading underscore): it has no
    semantic meaning outside this module and must not appear in
    :data:`__all__`. Consumer code should depend on the concrete event
    classes, not on this base.
    """

    # ------------------------------------------------------------------
    # Pydantic v2 model configuration.
    #
    # ``frozen=True``                -- immutability + hashability.
    # ``str_strip_whitespace=True``  -- trim whitespace from string fields
    #                                   before subsequent validation runs.
    # ``extra="ignore"``             -- silently drop unknown fields so a
    #                                   producer at schema version N+1 does
    #                                   not break consumers built against
    #                                   schema version N (AAP R-14).
    # ``populate_by_name=True``      -- alias-tolerant construction.
    # ------------------------------------------------------------------
    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="ignore",
        populate_by_name=True,
    )

    version: int = Field(
        ...,
        ge=1,
        description=(
            "Event payload schema version (AAP R-31). Must be >= 1; "
            "0 is reserved as the int default sentinel and rejected at "
            "validation time. Producers bump this when they evolve the "
            "schema; the producer module surfaces it as the "
            "``schema-version`` Kafka header for fast-skip in consumers."
        ),
    )


# =============================================================================
# Shared value objects -- exported for handler / test reuse.
# =============================================================================


class OrderItem(BaseModel):
    """Order line-item payload carried inside multiple event types.

    Used by:

    * :class:`OrderCreatedEvent`           -- the items the user placed.
    * :class:`OrderFulfilledEvent`         -- the items being fulfilled.
    * :class:`InventoryReservedEvent`      -- the items successfully reserved.
    * :class:`InventoryReservationFailedEvent`.``failed_items`` -- the items
      that could not be reserved (may be empty for global-failure events).

    Re-using a single value-object across all four events ensures the wire
    format is uniform: a downstream consumer that knows how to parse one
    :class:`OrderItem` instance can reuse the same parser everywhere. Schema
    Registry consequently only needs one ``OrderItem`` JSON-Schema subject.

    The class is FROZEN so two equal instances are interchangeable and so
    instances cannot be mutated by handlers in-flight. ``extra="ignore"``
    keeps the type forward-compatible (a producer that adds a ``sku`` field
    in a later version does not break older consumers).

    Attributes:
        product_id: Opaque cross-service reference to the Product Service
            catalog. NOT a foreign key (AAP R-6 -- the Order Service has no
            read-through to product-service's database). The Order Service
            does not validate that the product exists at the Product Service
            from this layer.
        quantity: Number of units ordered for this line; ``>= 1``.
        unit_price_minor_units: Per-unit price in the parent order's currency
            minor units (cents/paisa); ``>= 0``. Zero is permitted to support
            promotional bundles ("buy one, get one free" sets the bonus
            line's unit price to 0).
        line_total_minor_units: ``quantity * unit_price_minor_units``;
            ``>= 0``. Carried explicitly in the event so consumers do not
            need to recompute (AAP R-33 -- self-contained events). Producers
            are responsible for computing this consistently.
    """

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="ignore",
        populate_by_name=True,
    )

    product_id: UUID = Field(
        ...,
        description=(
            "Opaque cross-service product identifier (Product Service "
            "catalog id). NOT a foreign key per AAP R-6."
        ),
    )
    quantity: int = Field(
        ...,
        ge=1,
        description="Number of units ordered for this line; must be >= 1.",
    )
    unit_price_minor_units: int = Field(
        ...,
        ge=0,
        description=(
            "Per-unit price in the order currency's minor units "
            "(cents/paisa). Zero allowed for promotional bundle "
            "free-item lines."
        ),
    )
    line_total_minor_units: int = Field(
        ...,
        ge=0,
        description=(
            "quantity * unit_price; carried explicitly in the event so "
            "consumers do not need to recompute (AAP R-33 self-contained "
            "events)."
        ),
    )


class CancellationReason(str, Enum):
    """Canonical reasons an order may be cancelled.

    Inheriting from ``str`` makes Pydantic v2 serialise enum members as their
    string ``value`` in JSON mode (e.g., ``"USER_REQUESTED"``), matching the
    Schema Registry's ``"enum": [...]`` constraint exactly.

    New reasons require an explicit enum value addition AND a schema-version
    bump (AAP R-31): adding a value is a forward-compatible change for
    consumers built against an earlier version (they will simply not match
    the new value), but it must be a deliberate, audited action.

    Members:
        USER_REQUESTED:   The customer cancelled the order via the public
                          ``DELETE /orders/{id}`` endpoint.
        INVENTORY_FAILED: The Inventory Service emitted
                          ``inventory.reservation_failed``; saga compensates.
        PAYMENT_FAILED:   The Payment Service emitted ``payment.failed`` and
                          either is_retryable=False or the fallback provider
                          was exhausted (AAP R-10 dual-provider routing).
        SAGA_TIMEOUT:     The saga deadline elapsed without a terminal
                          response from a downstream step.
        DLQ_EXHAUSTED:    A consumed step-reply event reached its DLQ after
                          retry exhaustion (AAP R-17); the saga gives up.
        ADMIN_OVERRIDE:   An operator force-cancelled via an internal tool.
    """

    USER_REQUESTED = "USER_REQUESTED"
    INVENTORY_FAILED = "INVENTORY_FAILED"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    SAGA_TIMEOUT = "SAGA_TIMEOUT"
    DLQ_EXHAUSTED = "DLQ_EXHAUSTED"
    ADMIN_OVERRIDE = "ADMIN_OVERRIDE"


# =============================================================================
# Type aliases used in multiple event classes.
# =============================================================================

#: ISO 4217 currency code: exactly 3 uppercase ASCII letters
#: (``"USD"``, ``"EUR"``, ``"INR"``, ...). Defining the constraint via a
#: ``typing.Annotated`` alias rather than copy-pasting :class:`pydantic.Field`
#: arguments keeps the constraint definition DRY and identical across every
#: event that carries a currency code.
CurrencyCode = Annotated[
    str,
    Field(
        min_length=3,
        max_length=3,
        pattern=r"^[A-Z]{3}$",
        description="ISO 4217 currency code: 3 uppercase ASCII letters.",
    ),
]


# =============================================================================
# Produced events (Order Service -> Kafka).
# =============================================================================


class OrderCreatedEvent(_BaseEvent):
    """Order placed and saga started -- AAP R-18 step 1.

    Topic: ``order.created`` (AAP R-30).
    Producer: Order Service (saga coordinator).
    Consumers (informational): Inventory Service (reserve stock), Payment
        Service (capture payment), Notification Service (placement
        confirmation), Recommendation Engine (interaction signal).

    Emitted on ``POST /orders`` after the ``orders`` row plus the
    ``saga_state`` row are durably persisted in ``order_db`` (AAP Section
    0.4.4). Per AAP R-32 the producer does not enumerate or call its
    consumers; downstream services subscribe and act independently.

    Attributes:
        version: Schema version (inherited; AAP R-31).
        order_id: Order aggregate id (matches ``orders.id``).
        saga_id: Saga instance id (matches ``saga_state.saga_id``).
        user_id: Owning user (foreign-conceptual; no FK per AAP R-6).
        currency: ISO 4217 currency code.
        total_amount_minor_units: Total amount in currency minor units;
            ``>= 1`` defensive lower bound matching
            ``ORDER_MIN_TOTAL_AMOUNT_MINOR_UNITS`` in
            ``services/order-service/config/default.yaml``.
        items: Order line items; ``min_length=1``, ``max_length=200``
            (matches ``ORDER_MAX_ITEMS`` in ``default.yaml``).
        occurred_at: UTC RFC 3339 timestamp at which the saga was created.
        idempotency_key: Optional client-supplied
            ``Idempotency-Key`` header (max 128 chars, matches
            ``idempotency.max_length`` in ``default.yaml``).
    """

    order_id: UUID = Field(
        ...,
        description="Order aggregate id (matches orders.id).",
    )
    saga_id: UUID = Field(
        ...,
        description="Saga instance id (matches saga_state.saga_id).",
    )
    user_id: UUID = Field(
        ...,
        description="Owning user; opaque cross-service reference (no FK).",
    )
    currency: CurrencyCode = Field(
        ...,
        description="ISO 4217 currency code.",
    )
    total_amount_minor_units: int = Field(
        ...,
        ge=1,
        description=(
            "Total order amount in the currency's minor units; defensive "
            "lower bound matches ORDER_MIN_TOTAL_AMOUNT_MINOR_UNITS in "
            "services/order-service/config/default.yaml."
        ),
    )
    items: list[OrderItem] = Field(
        ...,
        min_length=1,
        max_length=200,
        description=(
            "Order line items; max_length matches ORDER_MAX_ITEMS=200 in "
            "services/order-service/config/default.yaml."
        ),
    )
    occurred_at: datetime = Field(
        ...,
        description="UTC RFC 3339 timestamp at which the saga was created.",
    )
    idempotency_key: str | None = Field(
        default=None,
        max_length=128,
        description=(
            "Optional client-provided Idempotency-Key header value. "
            "max_length=128 matches idempotency.max_length in "
            "services/order-service/config/default.yaml."
        ),
    )


class OrderCancelledEvent(_BaseEvent):
    """Order cancelled -- saga compensation in progress or terminal CANCELLED.

    Topic: ``order.cancelled`` (AAP R-30).
    Producer: Order Service (saga coordinator).
    Consumers (informational): Inventory Service (releases held stock if any),
        Payment Service (issues refund if payment captured), Notification
        Service (cancellation notice).

    Emitted on user cancellation, payment failure, saga timeout, or DLQ
    exhaustion. Per AAP R-18 the consumers self-determine compensation steps
    based on the ``inventory_reserved`` and ``payment_captured`` booleans
    which the saga coordinator populates from ``saga_state``. This makes the
    event SELF-CONTAINED (AAP R-33): consumers do not need to call back to
    the Order Service to decide whether they must compensate.

    Attributes:
        version: Schema version (inherited; AAP R-31).
        order_id: Order aggregate id.
        saga_id: Saga instance id.
        user_id: Owning user id.
        currency: ISO 4217 currency code.
        total_amount_minor_units: Total amount that was originally placed
            (in minor units); ``>= 0`` -- a zero-total order is a degenerate
            but legitimate cancel-of-a-zero-total order, so the lower bound
            is permissive here.
        reason: Canonical cancellation reason.
        reason_detail: Optional human-readable detail (max 512 chars).
        inventory_reserved: True iff stock had been reserved before
            cancellation; consumers use this to decide whether to release
            the reservation.
        payment_captured: True iff payment had been captured before
            cancellation; consumers use this to decide whether to issue a
            refund.
        occurred_at: UTC RFC 3339 timestamp of cancellation.
    """

    order_id: UUID = Field(
        ...,
        description="Order aggregate id (matches orders.id).",
    )
    saga_id: UUID = Field(
        ...,
        description="Saga instance id (matches saga_state.saga_id).",
    )
    user_id: UUID = Field(
        ...,
        description="Owning user id.",
    )
    currency: CurrencyCode = Field(
        ...,
        description="ISO 4217 currency code.",
    )
    total_amount_minor_units: int = Field(
        ...,
        ge=0,
        description=(
            "Total order amount in the currency's minor units; "
            ">=0 here (cancellation of zero-total orders is permitted)."
        ),
    )
    reason: CancellationReason = Field(
        ...,
        description="Canonical cancellation reason (string enum).",
    )
    reason_detail: str | None = Field(
        default=None,
        max_length=512,
        description=(
            "Optional human-readable detail (e.g., a provider error "
            "message or operator note). Capped at 512 characters."
        ),
    )
    inventory_reserved: bool = Field(
        ...,
        description=(
            "True iff stock had been reserved before cancellation; "
            "consumers use this to decide whether to release the "
            "reservation (AAP R-33 self-contained event)."
        ),
    )
    payment_captured: bool = Field(
        ...,
        description=(
            "True iff payment had been captured before cancellation; "
            "consumers use this to decide whether to issue a refund "
            "(AAP R-33 self-contained event)."
        ),
    )
    occurred_at: datetime = Field(
        ...,
        description="UTC RFC 3339 timestamp at which the order was cancelled.",
    )


class OrderFulfilledEvent(_BaseEvent):
    """Order saga completed successfully -- terminal happy-path state.

    Topic: ``order.fulfilled`` (AAP R-30).
    Producer: Order Service (saga coordinator).
    Consumers (informational): Notification Service (shipping/fulfillment
        notice), Recommendation Engine (strong purchase signal).

    Emitted at the saga's ``CONFIRM_ORDER`` step completion (terminal
    happy path). The ``payment_id`` and ``items`` are carried so consumers
    can act without callbacks (AAP R-33).

    Attributes:
        version: Schema version (inherited; AAP R-31).
        order_id: Order aggregate id.
        saga_id: Saga instance id.
        user_id: Owning user id.
        currency: ISO 4217 currency code.
        total_amount_minor_units: Total amount fulfilled (in minor units);
            ``>= 1`` -- a fulfilled order MUST have a positive total.
        payment_id: Identifier of the captured payment (Payment Service).
        fulfilled_at: UTC RFC 3339 timestamp at which the saga reached
            ``FULFILLED``.
        items: Fulfilled line items; ``min_length=1``, ``max_length=200``.
    """

    order_id: UUID = Field(
        ...,
        description="Order aggregate id (matches orders.id).",
    )
    saga_id: UUID = Field(
        ...,
        description="Saga instance id (matches saga_state.saga_id).",
    )
    user_id: UUID = Field(
        ...,
        description="Owning user id.",
    )
    currency: CurrencyCode = Field(
        ...,
        description="ISO 4217 currency code.",
    )
    total_amount_minor_units: int = Field(
        ...,
        ge=1,
        description=(
            "Total fulfilled amount in the currency's minor units; "
            ">=1 -- a fulfilled order must have a positive total."
        ),
    )
    payment_id: UUID = Field(
        ...,
        description=(
            "Identifier of the captured payment in the Payment Service "
            "(opaque cross-service reference)."
        ),
    )
    fulfilled_at: datetime = Field(
        ...,
        description=(
            "UTC RFC 3339 timestamp at which the saga reached the "
            "FULFILLED terminal state."
        ),
    )
    items: list[OrderItem] = Field(
        ...,
        min_length=1,
        max_length=200,
        description=(
            "Fulfilled line items; max_length matches ORDER_MAX_ITEMS=200 "
            "in services/order-service/config/default.yaml."
        ),
    )


# =============================================================================
# Consumed events (Kafka -> Order Service saga handlers).
# =============================================================================


class InventoryReservedEvent(_BaseEvent):
    """Inventory reservation succeeded for an order -- advance saga state.

    Topic: ``inventory.reserved`` (AAP R-30).
    Producer: Inventory Service.
    Consumer for the Order Service: advances saga to ``INVENTORY_RESERVED``;
        the saga then triggers the payment capture step.

    Attributes:
        version: Schema version (inherited; AAP R-31).
        order_id: Owning order id (matches the OrderCreatedEvent.order_id
            that triggered the reservation).
        saga_id: Owning saga id.
        reservation_id: Inventory Service's reservation aggregate id;
            carried so the Order Service can reference it during
            compensation (issue a release request keyed on this id).
        reserved_at: UTC RFC 3339 timestamp of the reservation.
        items: Reserved line items; mirrors the order items so consumers
            can audit and reconcile. ``min_length=1`` -- a reservation
            event for zero items is meaningless.
    """

    order_id: UUID = Field(
        ...,
        description="Owning order id; mirrors OrderCreatedEvent.order_id.",
    )
    saga_id: UUID = Field(
        ...,
        description="Owning saga id; mirrors OrderCreatedEvent.saga_id.",
    )
    reservation_id: UUID = Field(
        ...,
        description=(
            "Inventory Service's reservation aggregate id; carried so the "
            "Order Service can reference it during saga compensation."
        ),
    )
    reserved_at: datetime = Field(
        ...,
        description="UTC RFC 3339 timestamp at which the reservation was made.",
    )
    items: list[OrderItem] = Field(
        ...,
        min_length=1,
        description=(
            "Reserved line items; mirrors the order items so consumers "
            "can audit and reconcile."
        ),
    )


class InventoryReservationFailedEvent(_BaseEvent):
    """Inventory reservation failed -- trigger saga compensation to CANCELLED.

    Topic: ``inventory.reservation_failed`` (AAP R-30).
    Producer: Inventory Service.
    Consumer for the Order Service: triggers compensation. Because no
        payment has been taken yet at this saga step, the compensation is a
        single transition: emit ``OrderCancelledEvent`` with
        ``inventory_reserved=False`` and ``payment_captured=False``,
        ``reason=CancellationReason.INVENTORY_FAILED``.

    Attributes:
        version: Schema version (inherited; AAP R-31).
        order_id: Owning order id.
        saga_id: Owning saga id.
        reason_code: Machine-readable reason code (e.g.,
            ``INSUFFICIENT_STOCK``, ``WAREHOUSE_UNAVAILABLE``); 1..64 chars.
        reason_detail: Optional human-readable detail (max 512 chars).
        failed_items: Items that could not be reserved; may be empty for
            global failures (e.g., the warehouse system is offline). Default
            is an empty list so producers don't have to send ``[]`` explicitly.
        occurred_at: UTC RFC 3339 timestamp of the failure.
    """

    order_id: UUID = Field(
        ...,
        description="Owning order id; mirrors OrderCreatedEvent.order_id.",
    )
    saga_id: UUID = Field(
        ...,
        description="Owning saga id; mirrors OrderCreatedEvent.saga_id.",
    )
    reason_code: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description=(
            "Machine-readable reason code such as INSUFFICIENT_STOCK or "
            "WAREHOUSE_UNAVAILABLE; 1..64 characters."
        ),
    )
    reason_detail: str | None = Field(
        default=None,
        max_length=512,
        description="Optional human-readable detail (max 512 characters).",
    )
    failed_items: list[OrderItem] = Field(
        default_factory=list,
        description=(
            "Items that could not be reserved; may be empty for global "
            "failures (e.g., the warehouse system is offline)."
        ),
    )
    occurred_at: datetime = Field(
        ...,
        description="UTC RFC 3339 timestamp at which the failure was observed.",
    )


class PaymentSucceededEvent(_BaseEvent):
    """Payment captured successfully -- advance saga to PAYMENT_TAKEN -> FULFILLED.

    Topic: ``payment.succeeded`` (AAP R-30).
    Producer: Payment Service.
    Consumer for the Order Service: advances the saga from
        ``INVENTORY_RESERVED`` to ``PAYMENT_TAKEN``, then emits
        :class:`OrderFulfilledEvent` to terminate the saga on the happy path.

    Attributes:
        version: Schema version (inherited; AAP R-31).
        order_id: Owning order id.
        saga_id: Owning saga id.
        payment_id: Payment Service's payment aggregate id (opaque
            cross-service reference).
        provider: Provider name (e.g., ``"stripe"``, ``"razorpay"``); 1..32
            chars. Per AAP R-10 both providers must be integrated; this
            field tells the saga which provider was used so reconciliation
            and refunds (if later needed) target the correct adapter.
        provider_reference: Provider-side payment-intent / order id used
            for reconciliation (e.g., Stripe's ``pi_*`` id); 1..128 chars.
        currency: ISO 4217 currency code.
        amount_minor_units: Captured amount in minor units; ``>= 1``.
        captured_at: UTC RFC 3339 timestamp of the successful capture.
    """

    order_id: UUID = Field(
        ...,
        description="Owning order id; mirrors OrderCreatedEvent.order_id.",
    )
    saga_id: UUID = Field(
        ...,
        description="Owning saga id; mirrors OrderCreatedEvent.saga_id.",
    )
    payment_id: UUID = Field(
        ...,
        description="Payment Service's payment aggregate id.",
    )
    provider: str = Field(
        ...,
        min_length=1,
        max_length=32,
        description=(
            "Provider name (e.g., stripe, razorpay); 1..32 characters. "
            "Per AAP R-10 both providers are integrated concurrently; this "
            "field disambiguates which adapter handled the charge."
        ),
    )
    provider_reference: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description=(
            "Provider-side payment-intent / order id for reconciliation "
            "(e.g., Stripe pi_*, Razorpay pay_*); 1..128 characters."
        ),
    )
    currency: CurrencyCode = Field(
        ...,
        description="ISO 4217 currency code.",
    )
    amount_minor_units: int = Field(
        ...,
        ge=1,
        description=(
            "Captured amount in the currency's minor units; >=1 -- a "
            "succeeded payment must capture a positive amount."
        ),
    )
    captured_at: datetime = Field(
        ...,
        description="UTC RFC 3339 timestamp at which the payment was captured.",
    )


class PaymentFailedEvent(_BaseEvent):
    """Payment failed -- trigger saga compensation: release inventory -> CANCELLED.

    Topic: ``payment.failed`` (AAP R-30).
    Producer: Payment Service.
    Consumer for the Order Service: triggers compensation. The saga reads
        ``is_retryable`` to decide whether to attempt a fallback provider
        (Stripe <-> Razorpay per AAP R-10) before transitioning to
        ``CANCELLED``. The producer's ``is_retryable`` is a hint; the saga
        has the final say.

    Attributes:
        version: Schema version (inherited; AAP R-31).
        order_id: Owning order id.
        saga_id: Owning saga id.
        payment_id: Payment Service's payment aggregate id; may be ``None``
            for early failures where no payment row was created (e.g.,
            initial provider rejection at intent-creation time).
        provider: Provider name (e.g., ``"stripe"``, ``"razorpay"``);
            1..32 chars.
        provider_reference: Provider-side reference; may be ``None`` for
            early failures where the provider never returned a reference.
        reason_code: Machine-readable reason such as ``CARD_DECLINED``,
            ``INSUFFICIENT_FUNDS``, ``PROVIDER_TIMEOUT``; 1..64 chars.
        reason_detail: Optional human-readable detail (max 512 chars).
        is_retryable: If True, the saga MAY attempt a fallback provider
            before cancelling. Defaults to False (fail-closed).
        failed_at: UTC RFC 3339 timestamp of the failure.
    """

    order_id: UUID = Field(
        ...,
        description="Owning order id; mirrors OrderCreatedEvent.order_id.",
    )
    saga_id: UUID = Field(
        ...,
        description="Owning saga id; mirrors OrderCreatedEvent.saga_id.",
    )
    payment_id: UUID | None = Field(
        default=None,
        description=(
            "Payment Service's payment aggregate id; may be null on early "
            "failures where no payment row was created."
        ),
    )
    provider: str = Field(
        ...,
        min_length=1,
        max_length=32,
        description="Provider name (e.g., stripe, razorpay); 1..32 characters.",
    )
    provider_reference: str | None = Field(
        default=None,
        max_length=128,
        description=(
            "Provider-side reference; null for early failures where the "
            "provider never returned a reference."
        ),
    )
    reason_code: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description=(
            "Machine-readable reason such as CARD_DECLINED, "
            "INSUFFICIENT_FUNDS, PROVIDER_TIMEOUT; 1..64 characters."
        ),
    )
    reason_detail: str | None = Field(
        default=None,
        max_length=512,
        description="Optional human-readable detail (max 512 characters).",
    )
    is_retryable: bool = Field(
        default=False,
        description=(
            "If True, the saga MAY attempt a fallback provider before "
            "cancelling (Stripe <-> Razorpay per AAP R-10). Defaults to "
            "False -- fail-closed."
        ),
    )
    failed_at: datetime = Field(
        ...,
        description="UTC RFC 3339 timestamp at which the failure was observed.",
    )


# =============================================================================
# Public API surface -- explicit allow-list keeps re-exports deliberate.
# Exactly 9 names: 2 shared value objects + 3 produced events + 4 consumed events.
# The private :class:`_BaseEvent` is INTENTIONALLY not exported -- it has no
# semantic meaning outside this module and consumers should depend on the
# concrete event classes only.
# =============================================================================
__all__ = [
    # Shared value objects
    "OrderItem",
    "CancellationReason",
    # Produced events (Order Service -> Kafka)
    "OrderCreatedEvent",
    "OrderCancelledEvent",
    "OrderFulfilledEvent",
    # Consumed events (Kafka -> Order Service saga handlers)
    "InventoryReservedEvent",
    "InventoryReservationFailedEvent",
    "PaymentSucceededEvent",
    "PaymentFailedEvent",
]
