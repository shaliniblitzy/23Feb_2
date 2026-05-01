"""Canonical event type and topic identifiers for the Notification Service.

This module is the **single source of truth** for every Kafka topic name the
Notification Service consumes. It defines the closed catalog of seven domain
events the service is wired to react to per AAP Section 0.4.2 (the
"All Services -> Notification Service" row of the integration matrix), and
exposes the derived constants used by the Kafka consumer runner and the
channel router.

Consumers of this module
------------------------
* :mod:`src.events.dispatcher` --- matches incoming Kafka topic names against
  :class:`EventType` to route messages to the correct handler.
* :mod:`src.events.consumer` --- subscribes to :data:`ALL_CONSUMED_TOPICS` at
  startup; that tuple is passed verbatim to ``consumer.subscribe(...)``.
* :mod:`src.channels.router` --- reads :data:`CRITICAL_EVENTS` to decide
  whether a notification must override a user's opt-out preference (e.g.
  ``payment.failed`` is delivered regardless of user channel preferences for
  security / fraud-mitigation reasons).
* :mod:`src.events.handlers.*` --- per-event handler modules reference their
  corresponding :class:`EventType` member directly.
* :mod:`src.repository.notification_log_repo` --- persists :class:`EventType`
  values into the ``event_type`` column of the ``notification_log`` table
  (AAP Section 0.4.4).
* :mod:`src.domain.__init__` --- re-exports :class:`EventType`,
  :data:`CRITICAL_EVENTS`, and :data:`ALL_CONSUMED_TOPICS` for ergonomic
  internal imports.

Architectural invariants (AAP-mandated)
---------------------------------------
* **No internal coupling** --- this module imports ONLY from the Python
  standard library. It is at the bottom of the import DAG and must never
  depend on any other ``src/*`` module, on Pydantic, on a Kafka client, or
  on logging. (Folder spec for ``services/notification-service/src/domain/``.)
* **No I/O at import time** --- only enum construction and two pure
  expressions execute when the module is loaded (AAP R-33).
* **1:1 with Kafka topics** --- every member's string value is a Kafka topic
  name declared in ``infrastructure/kafka/topics.yaml``. No event type is
  consumed by the Notification Service that is not enumerated here.
* **No hard-coded event strings elsewhere** --- every other module in the
  Notification Service must reference :class:`EventType` members or a
  derived constant; raw string literals like ``"order.created"`` are
  forbidden in business logic and configuration consumers.

References
----------
* AAP Section 0.4.2 --- integrations table lists the seven consumed events.
* AAP R-30 --- event naming convention ``<domain>.<verb>``.
* AAP R-33 --- events are self-contained; consumers do not call producers
  to interpret events.
* AAP Section 0.4.4 --- the ``notification_log`` table stores ``event_type``
  values that must match this enum's string values.
* Folder spec for ``services/notification-service/src/domain/``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final


class EventType(StrEnum):
    """Canonical event/topic identifier for every Kafka topic the
    Notification Service consumes.

    Values follow the AAP R-30 naming convention ``<domain>.<verb>`` and
    match the topic names declared in ``infrastructure/kafka/topics.yaml``
    1:1. Because :class:`StrEnum` is used, every member is also a plain
    :class:`str` --- the enum members can be passed directly to Kafka
    clients, YAML dumpers, JSON serializers, and Python f-strings without
    explicit ``.value`` access.

    Example:
        >>> EventType.ORDER_CREATED == "order.created"
        True
        >>> EventType("order.created") is EventType.ORDER_CREATED
        True
        >>> f"topic={EventType.ORDER_CREATED}"
        'topic=order.created'

    The seven members below are the FULL set of events the Notification
    Service consumes per AAP Section 0.4.2. Adding a new event requires:

      1. Add the topic to ``infrastructure/kafka/topics.yaml`` (and its
         retry/DLQ pair per AAP R-17).
      2. Register the schema under ``infrastructure/kafka/schemas/`` per
         AAP R-14.
      3. Add a member here with an explicit string literal value.
      4. Register a handler in ``src/events/dispatcher.py``.
      5. (If security-critical) add the member to :data:`CRITICAL_EVENTS`,
         which requires regional regulatory review (GDPR, CAN-SPAM, TCPA).

    Note:
        Explicit string literal values are used (no :func:`enum.auto`)
        because these strings are externally visible Kafka topic names,
        database column values, and Schema Registry keys. ``auto()`` would
        silently change values if members were reordered.
    """

    # Identity domain --- emitted by the Auth Service on user signup.
    USER_REGISTERED = "user.registered"

    # Order domain --- emitted by the Order Service across saga transitions
    # (AAP Section 0.4.2 producer row).
    ORDER_CREATED = "order.created"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_FULFILLED = "order.fulfilled"

    # Payment domain --- emitted by the Payment Service after Stripe /
    # Razorpay provider callbacks (AAP Section 0.4.2 producer row).
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    PAYMENT_REFUNDED = "payment.refunded"


CRITICAL_EVENTS: Final[frozenset[EventType]] = frozenset({
    # Security / payment issues; override user opt-out per folder spec.
    EventType.PAYMENT_FAILED,
})
"""Events that override user opt-out preferences.

:data:`CRITICAL_EVENTS` is the subset of :class:`EventType` members whose
notifications MUST be delivered even when the recipient has opted out of
the channel (email or SMS). The :class:`~src.channels.router.ChannelRouter`
reads this set via ``router.is_critical(event_type)`` to bypass the user's
:class:`~src.domain.user_preferences.UserPreferences` opt-out flag.

The current single member is :attr:`EventType.PAYMENT_FAILED`: when a
user's payment fails, we must notify them regardless of their opt-out
state because leaving them uninformed risks account lockout, missed
orders, or fraud exposure. Other events (e.g.,
:attr:`EventType.ORDER_FULFILLED`) are transactional but not
security-critical --- those respect user preferences.

Adding a new member to this set is a product-policy decision that MUST
be reviewed alongside regional regulations (GDPR, CAN-SPAM, TCPA) ---
unsolicited SMS messaging is strongly regulated in most jurisdictions.

Implementation notes:

* Type is :class:`frozenset` (not :class:`set`) so accidental mutation in
  downstream code raises ``AttributeError`` rather than silently extending
  the set.
* Membership checks are O(1), which matters because the router consults
  this constant on every dispatched event.
* The :class:`~typing.Final` qualifier signals to ``mypy --strict`` that
  reassignment is a static type error.
"""


ALL_CONSUMED_TOPICS: Final[tuple[str, ...]] = tuple(et.value for et in EventType)
"""All Kafka topic names this service subscribes to, in declaration order.

:data:`ALL_CONSUMED_TOPICS` is the immutable list of Kafka topic names this
service subscribes to. The :class:`~src.events.consumer.KafkaConsumerRunner`
passes this tuple directly to ``consumer.subscribe(ALL_CONSUMED_TOPICS)``
at startup.

Because the tuple is **derived** from :class:`EventType`, adding a new
event automatically expands the subscription --- no separate update is
required, eliminating the class of bugs that arise from two declarations
of the same list drifting out of sync.

Implementation notes:

* Element type is plain :class:`str` (not :class:`EventType`) --- although
  :class:`StrEnum` members are themselves strings, declaring the tuple's
  element type as ``str`` makes the contract with Kafka client libraries
  explicit (they expect ``Iterable[str]`` for ``subscribe``).
* Type is :class:`tuple` (not :class:`list`) so consumers cannot mutate
  the canonical subscription list at runtime; it also signals
  "don't modify this" to readers.
* Ordering matches enum declaration order, guaranteed by Python's
  :class:`enum.EnumType`.
* The :class:`~typing.Final` qualifier signals to ``mypy --strict`` that
  reassignment is a static type error.
"""


__all__ = [
    "ALL_CONSUMED_TOPICS",
    "CRITICAL_EVENTS",
    "EventType",
]
