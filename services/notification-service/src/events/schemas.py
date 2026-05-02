"""Pydantic v2 event schemas for Kafka messages consumed by the Notification Service.

Each class in this module corresponds 1:1 with a Kafka topic name declared in
:class:`src.domain.events.EventType` and the central
``infrastructure/kafka/topics.yaml`` manifest:

    Class                       | Topic
    ----------------------------+-----------------------
    UserRegisteredEvent         | user.registered
    OrderCreatedEvent           | order.created
    OrderCancelledEvent         | order.cancelled
    OrderFulfilledEvent         | order.fulfilled
    PaymentSucceededEvent       | payment.succeeded
    PaymentFailedEvent          | payment.failed
    PaymentRefundedEvent        | payment.refunded

Plus the supporting :class:`OrderLineItem` referenced by the three Order events.

All event classes inherit from a private :class:`_EventBase` that enforces:

- ``extra="ignore"`` -- unknown fields from upstream producers are silently
  dropped, preserving forward-compatibility on the consumer side
  (AAP R-31, R-33). When the Order Service later adds a ``gift_card_amount``
  field to ``order.created``, this consumer keeps working without code
  changes; without ``extra="ignore"`` the Notification Service would route
  every such message to the DLQ -- a regression.
- ``frozen=True`` -- instances are immutable after construction. Handlers
  receive a validated event and convert it to a :class:`NotificationIntent`;
  they cannot accidentally mutate event payloads in flight (which would
  corrupt subsequent retries' validation state if Pydantic ever logged the
  input).
- ``populate_by_name=True`` -- field aliases (if any) work for both alias
  and Python attribute name; required for any future ``AliasChoices``
  usage and harmless when no aliases exist.
- ``str_strip_whitespace=True`` -- incoming string fields are trimmed at
  the validation boundary so whitespace-only inputs are caught by the
  existing ``min_length`` constraints rather than slipping through.
- A ``@field_validator("occurred_at")`` that REJECTS naive datetimes (no
  ``tzinfo``); see :meth:`_EventBase._must_be_tz_aware`. Producers MUST
  emit RFC 3339 / ISO 8601 timestamps with explicit offsets.

Versioning Discipline (AAP R-31)
--------------------------------
``event_version`` is a positive integer (``int >= 1``). This matches
the canonical wire-format envelope adopted across every service in
the platform (Payment Service, Order Service, Recommendation Engine
and this service): producers bump the integer monotonically when the
schema evolves in a backward-incompatible way; consumers branch on
``event_version`` to handle the new shape. The Schema Registry on
the producer side additionally enforces structural-compatibility
(``backward`` / ``forward`` / ``full``) on top of this integer
discriminator.

Side-effect freedom
-------------------
This module performs ZERO logging, ZERO I/O, and ZERO mutable global
state. It imports ONLY from the Python standard library and Pydantic;
it MUST NOT import from any ``src.*`` package because every other module
in :mod:`src.events` depends on these classes (importing back would
introduce a circular dependency).

Authority
---------
- AAP Section 0.1.1 Component #8     -- Notification Service.
- AAP Section 0.4.2                  -- Integration matrix (consumed events).
- AAP Section 0.5.2.2 bullet 8       -- Implementation directive.
- AAP R-14, R-30, R-31, R-33         -- Schema validation, naming, versioning,
                                        self-contained events.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Final, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    StringConstraints,
    field_validator,
)


# ---------------------------------------------------------------------------
# Module-level pattern constants (private; not exported)
# ---------------------------------------------------------------------------
# Currency code: ISO 4217 (exactly three uppercase ASCII letters, e.g. "USD",
# "EUR", "INR"). The producer-side Schema Registry contracts use the same
# regex; mirroring it on the consumer side catches drift early.
_CURRENCY_PATTERN: Final[str] = r"^[A-Z]{3}$"

# E.164 phone number format: leading '+' followed by 1-15 digits, with the
# first digit in 1-9 (no leading zero per the E.164 specification).
# Reference: ITU-T Recommendation E.164. Example: "+14155552671",
# "+919876543210".
_E164_PATTERN: Final[str] = r"^\+[1-9]\d{1,14}$"

# Locale: BCP-47 language tag, lenient pattern. Accepts a 2-3 letter language
# code (e.g. "en", "fr", "zh") optionally followed by 1+ '-'-separated subtags
# of 2-8 alphanumeric characters (e.g. "en-US", "zh-Hant-TW", "sr-Latn-RS").
# The full BCP-47 grammar is more elaborate, but this pattern accepts every
# real-world locale we will see in production while rejecting obvious garbage.
_LOCALE_PATTERN: Final[str] = r"^[a-zA-Z]{2,3}(-[a-zA-Z0-9]{2,8})*$"


# Allowed payment-provider literals for `PaymentEvent.provider` fields. Per
# AAP R-10, the Notification Service is provider-agnostic but only TWO
# providers are supported by the Payment Service: Stripe (global) and
# Razorpay (India). Adding a third provider (e.g. PayPal) means widening
# this Literal -- a type-system change that surfaces every consumer site
# that needs updating, which is the point.
PaymentProvider = Literal["stripe", "razorpay"]


# ---------------------------------------------------------------------------
# Reusable Annotated types for event payload fields
# ---------------------------------------------------------------------------
# Using ``Annotated`` with :class:`pydantic.StringConstraints` is the v2 idiom
# for attaching pattern / length / strip-whitespace constraints to scalar
# string types without subclassing. Each alias below is reused across multiple
# event classes to keep the constraints centralized -- changing the bound in
# one place updates every consumer.

# Generic non-empty trimmed string capped at 512 characters. Used for free-
# form fields like ``user_name``, ``failure_reason``, ``failure_code``,
# ``cancelled_by``, ``carrier``, and provider-side IDs that have no formal
# upper bound but must not be empty after trimming.
NonEmptyStr = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=512),
]

# ISO 4217 currency code (exactly three uppercase letters). The combination
# of ``min_length=3``, ``max_length=3``, and ``pattern=_CURRENCY_PATTERN``
# is intentional: ``min_length``/``max_length`` give Pydantic a fast-path
# rejection for obviously-wrong inputs before the regex runs.
CurrencyCode = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=3,
        max_length=3,
        pattern=_CURRENCY_PATTERN,
    ),
]

# E.164 phone number with leading '+'. The regex bound is 1-15 digits per
# the ITU specification; max_length is intentionally not set because
# the regex already enforces a hard upper bound (``\+`` + 15 digits = 16
# characters). Setting ``max_length`` would be redundant; setting
# ``min_length=2`` is implied by the pattern's ``\+[1-9]\d{1,14}``.
PhoneE164 = Annotated[
    str,
    StringConstraints(strip_whitespace=True, pattern=_E164_PATTERN),
]

# BCP-47 locale tag. The maximum length of 35 characters is the longest
# realistic BCP-47 tag (language + script + region + variant chain); 2 is
# the minimum for the shortest possible tag (e.g. "en", "fr").
LocaleStr = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=2,
        max_length=35,
        pattern=_LOCALE_PATTERN,
    ),
]

# ``event_version`` is a positive integer (>= 1) shared by every event in
# the platform's canonical wire envelope. The integer is bumped
# monotonically by the producer when the schema evolves in a backward-
# incompatible way; consumers branch on the value to handle the new shape.
# This consumer just requires the field to be a positive int so that it
# can flow into structured logs and DLQ metadata without ever becoming
# the cause of a downstream parsing failure. The Schema Registry on the
# producer side additionally enforces structural-compatibility
# (``backward`` / ``forward`` / ``full``) on top of this discriminator.
EventVersionInt = Annotated[
    int,
    Field(ge=1, description="Positive integer event-schema version (AAP R-31)."),
]


# ---------------------------------------------------------------------------
# Private base class for all consumed event schemas
# ---------------------------------------------------------------------------
class _EventBase(BaseModel):
    """Private base class shared by every consumed event schema.

    This class enforces the four Pydantic v2 ``ConfigDict`` settings required
    by the folder specification's "Key Design Contracts" -- ``extra="ignore"``,
    ``frozen=True``, ``populate_by_name=True``, ``str_strip_whitespace=True``
    -- and declares the four common envelope fields every event carries:
    ``event_id``, ``user_id``, ``event_version``, and ``occurred_at``. The
    leading underscore in the class name signals that this class is an
    internal implementation detail; downstream code MUST instantiate one of
    the concrete event classes (:class:`UserRegisteredEvent`,
    :class:`OrderCreatedEvent`, etc.) rather than this base.

    Subclasses MUST declare:
        * ``event_type``: a ``Literal[<dotted-event-name>]`` constant whose
          value matches the corresponding Kafka topic name; the constant
          enables type narrowing in mypy and provides ergonomic test
          construction (the field defaults to its literal value).
        * Any additional domain-specific fields required by the event.

    Subclasses MUST NOT redeclare:
        * ``event_id``, ``user_id``, ``event_version``, ``occurred_at`` --
          these are inherited unchanged from this base.

    Note:
        ``_EventBase`` is intentionally NOT included in :data:`__all__`; tests
        and downstream code should not subclass it directly.
    """

    model_config = ConfigDict(
        extra="ignore",
        frozen=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    event_id: UUID
    user_id: UUID
    event_version: EventVersionInt
    occurred_at: datetime

    @field_validator("occurred_at")
    @classmethod
    def _must_be_tz_aware(cls, v: datetime) -> datetime:
        """Reject naive ``datetime`` values; events MUST carry tz-aware times.

        Producers are required to emit RFC 3339 / ISO 8601 timestamps with
        an explicit UTC offset (``Z`` or ``+00:00``). A naive ``datetime``
        cannot be unambiguously interpreted across regions and indicates a
        producer-side bug (typically using :func:`datetime.datetime.now`
        instead of :func:`datetime.datetime.now` with ``tz=datetime.UTC``).
        Silently accepting naive datetimes here would propagate the bug
        downstream into delivery logs, retry timers, and Kibana time
        filters -- so we fail loud at the validation boundary instead.

        The check covers both the common case (``tzinfo is None``) AND the
        pathological case where a custom ``tzinfo`` subclass returns
        ``None`` from :meth:`datetime.datetime.utcoffset` (e.g. a
        partially-implemented stub). Both conditions render the timestamp
        un-serializable as RFC 3339, so both are rejected here.

        Args:
            v: The proposed value for the ``occurred_at`` field.

        Returns:
            ``v`` unchanged when it is genuinely timezone-aware.

        Raises:
            ValueError: If ``v.tzinfo is None`` or if
                ``v.utcoffset() is None``. The error message includes the
                phrase ``"timezone-aware"`` so monitoring queries on DLQ
                logs can match it for naive-datetime drift detection.
        """
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError(
                "occurred_at must be timezone-aware (RFC 3339 with offset, "
                "e.g. '2025-04-01T12:34:56Z' or '...+00:00').",
            )
        return v


# ---------------------------------------------------------------------------
# OrderLineItem -- supporting embedded model
# ---------------------------------------------------------------------------
class OrderLineItem(BaseModel):
    """A single line item within an order event payload.

    Embedded inside :class:`OrderCreatedEvent`, :class:`OrderCancelledEvent`,
    and :class:`OrderFulfilledEvent`. Used by the templating layer to render
    itemized order summaries in email and SMS notifications.

    Unlike the seven concrete event classes, :class:`OrderLineItem` is NOT
    an event in its own right; it does not have an ``event_id``, a
    ``user_id``, or an ``occurred_at``. It therefore inherits directly
    from :class:`pydantic.BaseModel` (NOT :class:`_EventBase`) and
    re-declares the same ``ConfigDict`` settings to keep the immutability
    and forward-compatibility guarantees consistent with the enclosing
    event classes.

    Attributes:
        product_id: UUID of the catalog product line item.
        name: Display name of the product as captured at order time
            (a snapshot value -- the catalog name may change later).
        quantity: Number of units ordered. Must be ``>= 1``; zero or
            negative quantities indicate a producer bug and are rejected.
        unit_price: Price per unit at order time, captured as a
            :class:`decimal.Decimal` to avoid IEEE-754 rounding errors
            in receipts. Pydantic accepts numeric strings (``"12.34"``)
            and ints (``12``) and coerces them to :class:`Decimal`.
            Must be ``>= 0`` (a zero-priced free promotional item is
            legitimate; a negative price is not).
        currency: ISO 4217 currency code (three uppercase letters).
    """

    model_config = ConfigDict(
        extra="ignore",
        frozen=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )

    product_id: UUID
    name: NonEmptyStr
    quantity: Annotated[int, Field(ge=1)]
    unit_price: Annotated[Decimal, Field(ge=Decimal("0"))]
    currency: CurrencyCode


# ---------------------------------------------------------------------------
# UserRegisteredEvent (Auth Service producer)
# ---------------------------------------------------------------------------
class UserRegisteredEvent(_EventBase):
    """Event emitted by the Auth Service when a new user registers.

    Triggers the ``welcome`` notification on email (always) and SMS (if the
    user provided a phone number AND opted in to SMS marketing). The handler
    in ``src/events/handlers/welcome.py`` builds a :class:`NotificationIntent`
    from this payload and routes it through the channel router.

    Topic: ``user.registered``
    Producers: Auth Service (``services/auth-service``)
    Consumers: Notification Service, Recommendation Engine

    Attributes:
        event_id: Stable UUID identifying this event instance (inherited).
        user_id: UUID of the newly-registered user (inherited).
        event_version: Producer-supplied schema version (inherited).
        occurred_at: RFC 3339 timestamp; must be timezone-aware (inherited).
        event_type: Always ``"user.registered"``; provided as a
            :class:`Literal` for both type narrowing and ergonomic test
            construction.
        email: Recipient email address; required because every welcome
            notification includes an email send. Validated by
            :class:`pydantic.EmailStr` (RFC 5321/5322).
        phone: Optional E.164 phone number. ``None`` is accepted because
            many users register without supplying a phone (web signup
            flows). When ``None``, the SMS channel is skipped for this
            user even if SMS opt-in is set; phone numbers can be added
            later through profile-update flows that are out of scope here.
        user_name: Display name used in the email greeting (e.g.
            ``"Welcome, Alice!"``). Must be non-empty after trimming.
        locale: BCP-47 locale tag for template rendering (e.g.
            ``"en-US"``, ``"fr-CA"``, ``"zh-Hant-TW"``). Defaults to
            ``"en-US"`` when the producer does not supply one.
    """

    event_type: Literal["user.registered"] = "user.registered"
    email: EmailStr
    phone: PhoneE164 | None = None
    user_name: NonEmptyStr
    locale: LocaleStr = "en-US"


# ---------------------------------------------------------------------------
# OrderCreatedEvent (Order Service producer)
# ---------------------------------------------------------------------------
class OrderCreatedEvent(_EventBase):
    """Event emitted by the Order Service when an order transitions to CREATED.

    The ``order.created`` event is part of the Order Service's saga-coordinator
    flow (AAP Section 0.1.1, Component #6). Once the saga has reserved
    inventory and queued payment, this event is published to Kafka so that
    downstream consumers (Notification Service, Recommendation Engine) can
    react.

    Triggers the ``order_created`` notification (transactional email
    confirmation; SMS for users who opted in).

    Topic: ``order.created``
    Producers: Order Service (``services/order-service``)
    Consumers: Notification Service, Recommendation Engine, Inventory
        Service, Payment Service

    Attributes:
        event_id, user_id, event_version, occurred_at: Inherited envelope.
        event_type: Always ``"order.created"``.
        order_id: UUID of the created order.
        order_total: Order subtotal-plus-tax-plus-shipping at the time of
            creation. Must be ``>= 0`` (a zero-total promotional order is
            legitimate, e.g. a free-trial activation).
        currency: ISO 4217 currency code matching the order's pricing
            currency.
        items: List of :class:`OrderLineItem` objects. May be empty for
            digital-goods orders that do not enumerate line items at the
            event boundary (the Order Service's downstream consumers can
            look up details via the Order Service API if needed; the
            Notification Service does not require a non-empty list).
        shipping_address: Optional pre-formatted shipping address string,
            included when present so the welcome email can render the
            address inline without an extra round-trip to the User
            Service. Capped at 2048 characters to bound payload size.
        locale: BCP-47 locale tag; defaults to ``"en-US"``.
    """

    event_type: Literal["order.created"] = "order.created"
    order_id: UUID
    order_total: Annotated[Decimal, Field(ge=Decimal("0"))]
    currency: CurrencyCode
    items: list[OrderLineItem] = Field(default_factory=list)
    shipping_address: str | None = Field(default=None, max_length=2048)
    locale: LocaleStr = "en-US"


# ---------------------------------------------------------------------------
# OrderCancelledEvent (Order Service producer)
# ---------------------------------------------------------------------------
class OrderCancelledEvent(_EventBase):
    """Event emitted by the Order Service when an order is cancelled.

    Cancellation can originate from the user (UI cancel button), the system
    (saga compensation after payment failure), or the merchant (manual
    intervention). The :attr:`cancelled_by` field captures the originator
    to drive notification copy and operator dashboards.

    Triggers the ``order_cancelled`` notification (email always; SMS
    when the user opted in). The notification includes the
    :attr:`reason` for transparency.

    Topic: ``order.cancelled``
    Producers: Order Service (``services/order-service``)
    Consumers: Notification Service, Recommendation Engine, Inventory
        Service, Payment Service

    Attributes:
        event_id, user_id, event_version, occurred_at: Inherited envelope.
        event_type: Always ``"order.cancelled"``.
        order_id: UUID of the cancelled order.
        order_total: Order total at the time of cancellation. Captured
            here for receipt parity with the original
            :class:`OrderCreatedEvent`.
        currency: ISO 4217 currency code.
        reason: Human-readable cancellation reason supplied by the
            originator (e.g. ``"customer requested"``,
            ``"inventory unavailable"``, ``"payment authorization
            failed"``). Required because cancellations without a reason
            are user-hostile and indicate a producer bug.
        cancelled_by: Optional originator label. Producer-side convention
            limits this to ``"user"``, ``"system"``, or ``"merchant"``,
            but no enum is enforced here so the field can carry richer
            provenance strings as the Order Service evolves (e.g.
            ``"system:saga-compensation"``).
        items: List of :class:`OrderLineItem` objects; may be empty.
        locale: BCP-47 locale tag; defaults to ``"en-US"``.
    """

    event_type: Literal["order.cancelled"] = "order.cancelled"
    order_id: UUID
    order_total: Annotated[Decimal, Field(ge=Decimal("0"))]
    currency: CurrencyCode
    reason: NonEmptyStr
    cancelled_by: NonEmptyStr | None = None
    items: list[OrderLineItem] = Field(default_factory=list)
    locale: LocaleStr = "en-US"


# ---------------------------------------------------------------------------
# OrderFulfilledEvent (Order Service producer)
# ---------------------------------------------------------------------------
class OrderFulfilledEvent(_EventBase):
    """Event emitted by the Order Service when an order is fulfilled.

    Fulfillment marks the terminal happy-path state of the order saga: the
    items have shipped (or, for digital goods, been provisioned) and the
    user is being notified of dispatch with carrier and tracking metadata.

    Triggers the ``order_fulfilled`` notification with embedded tracking
    info. The Notification Service renders carrier-specific tracking links
    when both :attr:`carrier` and :attr:`tracking_number` are present.

    Topic: ``order.fulfilled``
    Producers: Order Service (``services/order-service``)
    Consumers: Notification Service, Recommendation Engine

    Attributes:
        event_id, user_id, event_version, occurred_at: Inherited envelope.
        event_type: Always ``"order.fulfilled"``.
        order_id: UUID of the fulfilled order.
        tracking_number: Optional carrier-issued tracking identifier.
            ``None`` is accepted because some fulfillment flows (digital
            goods, in-store pickup) have no carrier tracking. Bounded
            to 128 characters to defeat absurdly long inputs.
        carrier: Optional carrier display name (e.g. ``"FedEx"``,
            ``"DHL"``, ``"USPS"``, ``"BlueDart"``). ``None`` when the
            fulfillment has no carrier (digital, in-store).
        estimated_delivery: Optional estimated delivery timestamp; if
            present MUST be timezone-aware (validated by
            :meth:`_delivery_must_be_tz_aware`).
        items: List of :class:`OrderLineItem`; may be empty.
        locale: BCP-47 locale tag; defaults to ``"en-US"``.
    """

    event_type: Literal["order.fulfilled"] = "order.fulfilled"
    order_id: UUID
    tracking_number: str | None = Field(default=None, max_length=128)
    carrier: NonEmptyStr | None = None
    estimated_delivery: datetime | None = None
    items: list[OrderLineItem] = Field(default_factory=list)
    locale: LocaleStr = "en-US"

    @field_validator("estimated_delivery")
    @classmethod
    def _delivery_must_be_tz_aware(cls, v: datetime | None) -> datetime | None:
        """Reject naive datetimes for ``estimated_delivery`` when supplied.

        ``None`` is accepted (the field is optional and many fulfillment
        flows do not have a known ETA). When a value is supplied, the same
        timezone-awareness rule that applies to ``occurred_at`` applies
        here so the templated email displays an unambiguous local time.

        Args:
            v: Proposed value; ``None`` or a :class:`datetime`.

        Returns:
            ``None`` if ``v`` is ``None``, otherwise ``v`` unchanged.

        Raises:
            ValueError: If ``v`` is a naive :class:`datetime`.
        """
        if v is None:
            return v
        if v.tzinfo is None or v.utcoffset() is None:
            raise ValueError(
                "estimated_delivery must be timezone-aware when provided.",
            )
        return v


# ---------------------------------------------------------------------------
# PaymentSucceededEvent (Payment Service producer)
# ---------------------------------------------------------------------------
class PaymentSucceededEvent(_EventBase):
    """Event emitted by the Payment Service when a payment is confirmed.

    The Payment Service emits this event after the upstream provider
    (Stripe or Razorpay) reports a successful charge through their
    webhook callback. The :attr:`provider_payment_id` is the
    provider-issued identifier (Stripe ``ch_...``; Razorpay
    ``pay_...``) used for cross-system reconciliation and is included
    in the email receipt for user reference.

    Triggers the ``payment_succeeded`` notification (transactional email
    receipt; SMS only if the user opted in to payment-success SMS).

    Topic: ``payment.succeeded``
    Producers: Payment Service (``services/payment-service``)
    Consumers: Notification Service, Order Service (saga continuation),
        Recommendation Engine

    Attributes:
        event_id, user_id, event_version, occurred_at: Inherited envelope.
        event_type: Always ``"payment.succeeded"``.
        payment_id: UUID of the Payment Service's internal payment
            record (NOT the provider's ID -- that is
            :attr:`provider_payment_id`).
        order_id: UUID of the order this payment settled.
        amount: Amount settled. Must be ``> 0`` (a successful zero-charge
            payment is meaningless; providers never report ``$0`` as
            ``"succeeded"``).
        currency: ISO 4217 currency code matching the provider charge.
        provider: Either ``"stripe"`` or ``"razorpay"``. Constrained by
            :data:`PaymentProvider`.
        provider_payment_id: Provider-issued payment identifier
            (e.g. Stripe ``"ch_3PHj..."``). Required because every
            successful charge has one and the receipt template
            references it.
        locale: BCP-47 locale tag; defaults to ``"en-US"``.
    """

    event_type: Literal["payment.succeeded"] = "payment.succeeded"
    payment_id: UUID
    order_id: UUID
    amount: Annotated[Decimal, Field(gt=Decimal("0"))]
    currency: CurrencyCode
    provider: PaymentProvider
    provider_payment_id: NonEmptyStr
    locale: LocaleStr = "en-US"


# ---------------------------------------------------------------------------
# PaymentFailedEvent (Payment Service producer; CRITICAL event)
# ---------------------------------------------------------------------------
class PaymentFailedEvent(_EventBase):
    """Event emitted by the Payment Service when a payment attempt fails.

    Listed in :data:`src.domain.events.CRITICAL_EVENTS`: the resulting
    notification is delivered on BOTH email AND SMS regardless of the
    user's opt-out preferences (per the folder spec for
    ``services/notification-service/src/channels`` and AAP R-11). A
    failed payment risks account lockout, missed orders, or fraud
    exposure; suppressing notification on user opt-out would be
    user-hostile.

    Topic: ``payment.failed``
    Producers: Payment Service (``services/payment-service``)
    Consumers: Notification Service, Order Service (saga compensation),
        Recommendation Engine (negative-signal feature)

    Attributes:
        event_id, user_id, event_version, occurred_at: Inherited envelope.
        event_type: Always ``"payment.failed"``.
        payment_id: UUID of the Payment Service's internal payment
            attempt record.
        order_id: UUID of the order this payment was meant to settle.
        amount: Amount of the FAILED attempt. Must be ``> 0`` because
            the Payment Service never initiates a ``$0`` charge.
        currency: ISO 4217 currency code.
        provider: Either ``"stripe"`` or ``"razorpay"``.
        provider_payment_id: Provider-issued identifier IF a provider-
            side record was created (e.g. Stripe returned an error AFTER
            creating the charge intent). ``None`` when the attempt
            failed before the provider issued an ID (e.g. local
            validation error, network timeout to provider, auth-decline
            at the provider's gateway). Reflecting this with ``Optional``
            avoids producer-side hacks like passing the empty string.
        failure_reason: Human-readable failure reason (e.g.
            ``"Your card was declined."``). Required.
        failure_code: Provider-specific machine code (e.g. Stripe's
            ``"card_declined"``). Required because the notification
            template branches on the code to render kind copy
            (``"Try a different card"`` vs ``"Contact your bank"``).
        locale: BCP-47 locale tag; defaults to ``"en-US"``.
    """

    event_type: Literal["payment.failed"] = "payment.failed"
    payment_id: UUID
    order_id: UUID
    amount: Annotated[Decimal, Field(gt=Decimal("0"))]
    currency: CurrencyCode
    provider: PaymentProvider
    provider_payment_id: NonEmptyStr | None = None
    failure_reason: NonEmptyStr
    failure_code: NonEmptyStr
    locale: LocaleStr = "en-US"


# ---------------------------------------------------------------------------
# PaymentRefundedEvent (Payment Service producer)
# ---------------------------------------------------------------------------
class PaymentRefundedEvent(_EventBase):
    """Event emitted by the Payment Service when a refund completes.

    Refunds may be partial (the user returned one item out of three) or
    full (the order was cancelled post-payment). Both cases emit the same
    event shape; the consumer infers partial-vs-full from
    :attr:`refund_amount` relative to the original payment amount (which
    the consumer can join from its own delivery log if needed).

    Triggers the ``payment_refunded`` notification (email confirmation
    with refund amount; SMS when the user opted in).

    Topic: ``payment.refunded``
    Producers: Payment Service (``services/payment-service``)
    Consumers: Notification Service, Order Service

    Attributes:
        event_id, user_id, event_version, occurred_at: Inherited envelope.
        event_type: Always ``"payment.refunded"``.
        payment_id: UUID of the Payment Service's internal record for the
            ORIGINAL payment that this refund reverses (NOT the refund's
            own ID -- that is :attr:`refund_id`).
        order_id: UUID of the order whose payment is being refunded.
        refund_id: UUID of the Payment Service's internal record for THIS
            refund. Distinct from :attr:`payment_id`.
        refund_amount: Amount being refunded. Must be ``> 0`` -- a
            ``$0`` refund is meaningless and indicates a producer bug.
        currency: ISO 4217 currency code matching the original payment.
        provider: Either ``"stripe"`` or ``"razorpay"``.
        provider_refund_id: Provider-issued refund identifier
            (e.g. Stripe ``"re_3PHj..."``). Required because every
            successful refund has one.
        locale: BCP-47 locale tag; defaults to ``"en-US"``.
    """

    event_type: Literal["payment.refunded"] = "payment.refunded"
    payment_id: UUID
    order_id: UUID
    refund_id: UUID
    refund_amount: Annotated[Decimal, Field(gt=Decimal("0"))]
    currency: CurrencyCode
    provider: PaymentProvider
    provider_refund_id: NonEmptyStr
    locale: LocaleStr = "en-US"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
# `_EventBase`, the regex constants (`_CURRENCY_PATTERN`, `_E164_PATTERN`,
# `_LOCALE_PATTERN`), the reusable Annotated aliases (`NonEmptyStr`,
# `CurrencyCode`, `PhoneE164`, `LocaleStr`, `EventVersionInt`), and the
# `PaymentProvider` Literal alias are all intentionally NOT exported from
# this module. They are private implementation details:
#
#   * `_EventBase` -- subclassing should be confined to this module.
#   * Regex constants -- pattern bounds are an implementation detail; if
#     they need to change in the future, callers should not rely on them.
#   * Reusable Annotated aliases (including `EventVersionInt`) -- they
#     exist solely to deduplicate the constraint declarations across event
#     classes; downstream code that needs a constrained scalar should
#     declare its own constraints.
#   * `PaymentProvider` -- a future `src.domain.types` module may declare
#     a public `PaymentProvider` alias; pre-emptively keeping this private
#     avoids a name conflict.
__all__ = [
    "OrderCancelledEvent",
    "OrderCreatedEvent",
    "OrderFulfilledEvent",
    "OrderLineItem",
    "PaymentFailedEvent",
    "PaymentRefundedEvent",
    "PaymentSucceededEvent",
    "UserRegisteredEvent",
]
