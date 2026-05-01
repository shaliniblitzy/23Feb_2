"""Domain enumerations for the Payment Service.

This module is the import-graph leaf for the Payment Service: it imports
ONLY from the Python standard library (specifically :mod:`enum`) and is
imported by every other module in ``services/payment-service/src/``
(directly via ``from src.domain.enums import ...`` or transitively via
the :mod:`src.domain` package's re-exports). Nothing in ``src/*`` may be
imported here.

Every class in this module extends :class:`enum.StrEnum` (Python 3.11+)
so members are simultaneously valid :class:`str` instances. This means:

* ``json.dumps({"status": PaymentStatus.SUCCEEDED})`` produces
  ``'{"status": "SUCCEEDED"}'`` natively --- no custom encoder needed.
* DB columns of type ``TEXT`` / ``VARCHAR`` accept enum members directly
  via psycopg / SQLAlchemy without explicit ``.value`` access.
* Pydantic v2 strict mode coerces matching strings to enum members on
  input AND serializes back to the string value on output.

Currency is intentionally NOT modeled as an enum --- see
:class:`src.domain.models.Money` which validates ISO 4217 codes via a
lightweight string normalizer. ISO 4217 has 180+ codes (a huge enum)
and providers also accept test currencies (e.g., Stripe's ``XTS``).
Modeling Currency as a closed enum is high-friction and brittle; the
``Money`` validator keeps the open-ended set tractable while the
closed-set enums in this module remain small and predictable.

Design invariants
-----------------
* **No** :func:`enum.auto` --- every member assigns its string value
  EXPLICITLY. The strings here are externally visible (DB columns,
  Kafka events, webhook URL paths, audit logs); :func:`enum.auto`
  would derive values from member names and silently break the
  contract on rename. Explicit values are stable across refactors and
  match the convention established by the sibling notification-service.
* **No methods, no behavior** --- pure-data enums. Predicates such as
  "is this status terminal?" or "is this outcome retryable?" live on
  the consumers (saga, repository, scheduler), NOT on the enum. This
  preserves the "thin domain leaf" invariant.
* **No I/O at import time** --- only enum construction and the
  ``__all__`` list are evaluated when this module is loaded. No
  logging, no HTTP framework, no database driver, no Pydantic.
* **Two intentional spellings of CANCEL{LED,ED}** --- the folder spec
  uses the US spelling ``CANCELED`` (one ``L``) on
  :class:`AttemptStatus` and the UK spelling ``CANCELLED`` (two
  ``L``\\s) on :class:`PaymentStatus`. They model different concepts
  (per-attempt cancellation versus per-payment cancellation) and they
  match downstream PostgreSQL column constraints exactly. DO NOT
  "fix" the spelling --- the divergence is documented and intentional.
* **Casing rationale** --- :class:`ProviderName` values are LOWERCASE
  because they appear in URL paths (``/webhooks/stripe``,
  ``/webhooks/razorpay``) and DB columns where lowercase is
  conventional. Every other enum's values are UPPER_SNAKE_CASE
  because they appear in audit logs, Kafka event payloads, and DB
  state-machine columns where uppercase improves grep-ability.

Authoritative references
------------------------
* AAP Section 0.1.1 Component #7 --- Payment Service (Stripe + Razorpay)
  overview.
* AAP Section 0.4.4 --- ``payment_db`` schema (``payments``,
  ``payment_attempts``, ``refunds``, ``provider_webhooks``,
  ``idempotency_keys`` tables); column values come from this module.
* AAP Section 0.5.2.2 bullet 7 --- Payment Service implementation plan
  with provider adapter layer and webhook handlers.
* AAP R-10 --- dual-provider concurrent integration; :class:`ProviderName`
  is THE switch.
* AAP R-30 --- events named ``<domain>.<verb>``; the Payment Service
  produces ``payment.succeeded``, ``payment.failed``,
  ``payment.refunded`` (encoded as :class:`PaymentEventType`).
"""

from __future__ import annotations

from enum import StrEnum


class ProviderName(StrEnum):
    """Payment-provider identifier.

    String values match the ``payments.provider`` and
    ``provider_webhooks.provider`` columns and the ``X-Provider`` query
    parameter accepted by the webhook controller. They also appear as
    URL path segments (``/webhooks/stripe``, ``/webhooks/razorpay``)
    where lowercase is conventional.

    Per AAP R-10 the service supports both providers concurrently;
    selection at runtime is driven by ``ProviderRouter`` using
    currency, region, and merchant preference. This enum is THE switch
    that decides which adapter handles a charge or refund call.

    This enum is a CLOSED SET. Adding a third provider (e.g., PayPal)
    is an architecture-level change that requires an AAP amendment,
    a Schema Registry version bump, and coordinated DB/migrations
    work --- it is NOT a routine code change.

    Members:
        STRIPE: Stripe payment provider (global). Implemented by the
            :class:`StripeProvider` adapter. Value: ``"stripe"``.
        RAZORPAY: Razorpay payment provider (India-focused). Implemented
            by the :class:`RazorpayProvider` adapter. Value:
            ``"razorpay"``.

    Example:
        >>> ProviderName.STRIPE == "stripe"
        True
        >>> ProviderName("razorpay") is ProviderName.RAZORPAY
        True
        >>> isinstance(ProviderName.STRIPE, str)
        True
    """

    STRIPE = "stripe"
    RAZORPAY = "razorpay"


class Region(StrEnum):
    """Geographic region used by :class:`ProviderRouter` to choose
    between Stripe (global) and Razorpay (India-focused).

    Values are short uppercase codes --- easy to log, easy to store,
    and aligned with common region taxonomies (ISO 3166-1 alpha-2 for
    countries; ``APAC`` / ``EU`` / ``OTHER`` for groupings).

    Routing semantics (see ``providers/routing.py``):
      * ``IN`` --- prefer Razorpay (India-focused provider).
      * ``US`` / ``EU`` / ``UK`` / ``APAC`` / ``OTHER`` --- prefer
        Stripe (global provider).

    The ``OTHER`` member is the catch-all bucket; the router treats
    any unknown region as ``OTHER`` and falls back to Stripe by
    default. Member-name comparisons are direct (no normalization
    layer) --- the router compares enum members by identity.

    Members:
        IN: India. Routes to Razorpay first.
            Value: ``"IN"``.
        US: United States. Routes to Stripe first.
            Value: ``"US"``.
        EU: European Union (broad grouping). Routes to Stripe first.
            Value: ``"EU"``.
        UK: United Kingdom (post-Brexit explicit grouping). Routes to
            Stripe first. Value: ``"UK"``.
        APAC: Asia-Pacific excluding India. Routes to Stripe first.
            Value: ``"APAC"``.
        OTHER: Catch-all for unmatched regions. Routes to Stripe first.
            Value: ``"OTHER"``.

    Example:
        >>> Region.IN == "IN"
        True
        >>> Region("OTHER") is Region.OTHER
        True
        >>> isinstance(Region.US, str)
        True
    """

    IN = "IN"
    US = "US"
    EU = "EU"
    UK = "UK"
    APAC = "APAC"
    OTHER = "OTHER"


class PaymentStatus(StrEnum):
    """Lifecycle status of a ``Payment`` (mirrors ``payments.status``
    column).

    State transitions (no skipping forward; only valid transitions
    are enforced by ``OrderSaga`` / ``PaymentsRepository``)::

        PENDING --> PROCESSING --+--> SUCCEEDED --+--> PARTIALLY_REFUNDED --> REFUNDED
                                 |                +--> REFUNDED
                                 +--> FAILED
                  +--> CANCELLED

    Notes:
      * ``PENDING`` is the initial state on row insert (before the
        first provider call).
      * ``PROCESSING`` is a brief in-flight state while the provider
        adapter is awaiting a response.
      * ``SUCCEEDED`` and ``FAILED`` are terminal except for the
        refund path: from ``SUCCEEDED`` the row may transition to
        ``PARTIALLY_REFUNDED`` and then eventually to ``REFUNDED``
        once the cumulative refund amount equals the original charge.
      * ``CANCELLED`` covers explicit user cancellations and saga
        compensations triggered before any provider call succeeded
        (e.g., the OrderSaga compensates a payment because inventory
        reservation failed).

    Spelling note: ``CANCELLED`` here uses the UK spelling (TWO
    ``L``\\s). :class:`AttemptStatus` uses the US spelling
    (``CANCELED``, ONE ``L``) for the per-attempt analogue. The
    divergence is intentional and matches the folder spec verbatim;
    DO NOT alias them.

    Values are UPPER_SNAKE_CASE --- they appear in DB columns and
    Kafka event payloads and must remain stable across releases.

    Members:
        PENDING: Initial state. Row inserted; no provider call yet.
            Value: ``"PENDING"``.
        PROCESSING: Provider call is in flight; awaiting response.
            Value: ``"PROCESSING"``.
        SUCCEEDED: Provider returned a 2xx response with a charge
            identifier. May transition to ``PARTIALLY_REFUNDED`` or
            ``REFUNDED`` later via the refund path. Value:
            ``"SUCCEEDED"``.
        FAILED: Provider returned a hard failure (e.g., card declined,
            account suspended). Terminal. Value: ``"FAILED"``.
        REFUNDED: Cumulative refund amount equals the original charge;
            payment is fully refunded. Terminal. Value: ``"REFUNDED"``.
        PARTIALLY_REFUNDED: Cumulative refund amount is less than the
            original charge but greater than zero. May transition to
            ``REFUNDED`` if a subsequent refund completes the
            cumulative amount. Value: ``"PARTIALLY_REFUNDED"``.
        CANCELLED: Saga or user cancelled the payment before any
            provider call succeeded. Terminal. Value: ``"CANCELLED"``.

    Example:
        >>> PaymentStatus.SUCCEEDED == "SUCCEEDED"
        True
        >>> PaymentStatus("PARTIALLY_REFUNDED") is PaymentStatus.PARTIALLY_REFUNDED
        True
        >>> isinstance(PaymentStatus.PENDING, str)
        True
    """

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"
    PARTIALLY_REFUNDED = "PARTIALLY_REFUNDED"
    CANCELLED = "CANCELLED"


class RefundStatus(StrEnum):
    """Lifecycle status of a ``Refund`` (mirrors ``refunds.status``
    column).

    State transitions::

        PENDING --> PROCESSING --+--> SUCCEEDED
                                 +--> FAILED

    Mirrors a deliberately simpler subset of :class:`PaymentStatus`
    because refunds, once submitted to a provider, follow a
    deterministic provider-side lifecycle. There is no user-cancellation
    step (compared to payments where the saga may cancel before any
    provider call) and no further refund/cancellation lifecycle beyond
    success or failure. If a future requirement adds a cancellation
    state, it must be coordinated with the DDL migration AND with the
    refund processing logic.

    Members:
        PENDING: Initial state. Refund row inserted; no provider call
            yet. Value: ``"PENDING"``.
        PROCESSING: Provider refund call is in flight; awaiting
            response. Value: ``"PROCESSING"``.
        SUCCEEDED: Provider confirmed the refund. Terminal. Value:
            ``"SUCCEEDED"``.
        FAILED: Provider rejected the refund (e.g., charge already
            disputed, refund window expired). Terminal. Value:
            ``"FAILED"``.

    Example:
        >>> RefundStatus.SUCCEEDED == "SUCCEEDED"
        True
        >>> RefundStatus("PENDING") is RefundStatus.PENDING
        True
        >>> isinstance(RefundStatus.FAILED, str)
        True
    """

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class AttemptStatus(StrEnum):
    """Outcome of a single outbound provider call (one row in
    ``payment_attempts``).

    Distinct from :class:`PaymentStatus` because a single payment may
    have multiple attempts:

      * Retry-on-transient with exponential backoff per AAP R-15
        (e.g., a Stripe HTTP 503 followed by a successful retry yields
        two ``payment_attempts`` rows: one ``TIMEOUT`` or ``FAILED``
        followed by one ``SUCCEEDED``).
      * Fallback from Stripe to Razorpay (or vice versa) per AAP R-20
        when the primary provider's circuit breaker is open.

    The per-payment row in ``payments`` aggregates across these per-
    attempt rows; the per-attempt rows themselves are immutable audit
    records.

    Spelling note: ``CANCELED`` here uses the US spelling (ONE ``L``).
    :class:`PaymentStatus` uses the UK spelling (``CANCELLED``, TWO
    ``L``\\s) for the per-payment analogue. The divergence is
    intentional and matches the folder spec verbatim --- the two
    columns represent different concepts (per-attempt cancellation
    versus per-payment cancellation) and downstream PostgreSQL CHECK
    constraints are written against these spellings exactly. DO NOT
    "fix" or alias them; "helpful" reformatters that collapse the two
    spellings will silently break the DB schema.

    :class:`AttemptStatus` covers BOTH charge and refund attempts ---
    the ``payment_attempts`` table is shared between the two flows and
    discriminated by a separate ``attempt_kind`` column.

    Members:
        IN_FLIGHT: The provider request has been sent; no response
            yet. Rows in this state are recovered on service restart
            by checking the provider's idempotency key against the
            persisted ``idempotency_keys`` row before issuing a new
            request. Value: ``"IN_FLIGHT"``.
        SUCCEEDED: Provider returned a 2xx response and a charge or
            refund identifier. Terminal. Value: ``"SUCCEEDED"``.
        FAILED: Provider returned a hard failure (e.g.,
            ``card_declined``, ``insufficient_funds``). Treated as
            non-retryable; the saga / repository decides whether to
            cancel the payment or retry with a different provider.
            Terminal. Value: ``"FAILED"``.
        TIMEOUT: Request exceeded the per-attempt timeout. Treated as
            transient and retried per AAP R-15 unless the maximum
            attempt budget is exhausted. Terminal for THIS attempt;
            subsequent attempts are recorded as new rows. Value:
            ``"TIMEOUT"``.
        CANCELED: Caller cancelled the attempt mid-call (rare; e.g.,
            the circuit breaker tripped between the request being
            assembled and being sent, or the saga compensated while
            the request was being prepared). Terminal. Value:
            ``"CANCELED"``.

    Example:
        >>> AttemptStatus.IN_FLIGHT == "IN_FLIGHT"
        True
        >>> AttemptStatus("CANCELED") is AttemptStatus.CANCELED
        True
        >>> isinstance(AttemptStatus.SUCCEEDED, str)
        True
    """

    IN_FLIGHT = "IN_FLIGHT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    CANCELED = "CANCELED"


class PaymentEventType(StrEnum):
    """Discriminator for events the Payment Service produces or for
    canonical ``ProviderEvent`` records produced by the webhook
    translator.

    Per AAP R-30 events are named ``<domain>.<verb>``; this enum
    captures the canonical INTERNAL event types. The Kafka topic names
    (``payment.succeeded``, ``payment.failed``, ``payment.refunded``)
    are intentionally DIFFERENT from these enum values --- the topic
    map lives in :mod:`src.events.producer`. This enum documents the
    canonical EVENT TYPES; topic mapping is a transport concern owned
    by the messaging layer. Mixing them here would couple the domain
    to the messaging layer and violate layered-architecture
    discipline.

    Topic mapping (for reference; canonical map in producer):
      * :attr:`CHARGE_SUCCEEDED` --> Kafka topic ``payment.succeeded``.
      * :attr:`CHARGE_FAILED` --> Kafka topic ``payment.failed``.
      * :attr:`REFUND_SUCCEEDED` --> Kafka topic ``payment.refunded``.
      * :attr:`REFUND_FAILED` --> not emitted as a public Kafka event;
        consumed by the saga via repository state.
      * :attr:`DISPUTE_CREATED` --> reserved; downstream alerting only.
      * :attr:`WEBHOOK_RECEIVED` --> not emitted; audit-only.

    Members:
        CHARGE_SUCCEEDED: A provider charge call succeeded; emitted as
            Kafka topic ``payment.succeeded``. Value:
            ``"CHARGE_SUCCEEDED"``.
        CHARGE_FAILED: A provider charge call failed terminally;
            emitted as Kafka topic ``payment.failed``. Value:
            ``"CHARGE_FAILED"``.
        REFUND_SUCCEEDED: A refund completed; emitted as Kafka topic
            ``payment.refunded``. Value: ``"REFUND_SUCCEEDED"``.
        REFUND_FAILED: A refund failed; logged and persisted but NOT
            emitted as a public Kafka event (consumed by the saga via
            repository state). Value: ``"REFUND_FAILED"``.
        DISPUTE_CREATED: Provider issued a dispute notification (via
            webhook); used for downstream alerting; not yet a public
            Kafka event but reserved for future use. Value:
            ``"DISPUTE_CREATED"``.
        WEBHOOK_RECEIVED: Generic catch-all for webhook events that do
            not match any of the above; preserved for audit logging
            in the ``provider_webhooks.event_type`` column. Value:
            ``"WEBHOOK_RECEIVED"``.

    Example:
        >>> PaymentEventType.CHARGE_SUCCEEDED == "CHARGE_SUCCEEDED"
        True
        >>> PaymentEventType("WEBHOOK_RECEIVED") is PaymentEventType.WEBHOOK_RECEIVED
        True
        >>> isinstance(PaymentEventType.REFUND_FAILED, str)
        True
    """

    CHARGE_SUCCEEDED = "CHARGE_SUCCEEDED"
    CHARGE_FAILED = "CHARGE_FAILED"
    REFUND_SUCCEEDED = "REFUND_SUCCEEDED"
    REFUND_FAILED = "REFUND_FAILED"
    DISPUTE_CREATED = "DISPUTE_CREATED"
    WEBHOOK_RECEIVED = "WEBHOOK_RECEIVED"


__all__ = [
    "AttemptStatus",
    "PaymentEventType",
    "PaymentStatus",
    "ProviderName",
    "Region",
    "RefundStatus",
]
