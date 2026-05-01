"""Channel, status, outcome, and criticality enums for the Notification Service.

This module is the **single source of truth** for the canonical string
constants the Notification Service uses across channels, repositories,
schedulers, and event handlers. It defines four closed enumerations:

* :class:`ChannelType` --- the transport channel (Email or SMS).
* :class:`NotificationStatus` --- the lifecycle state stored in the
  ``notification_log.status`` column.
* :class:`DeliveryOutcome` --- the result classification returned by a
  :class:`NotificationChannel` adapter; drives retry / DLQ scheduling.
* :class:`CriticalFlag` --- the template-criticality classifier consumed
  by the channel router to decide opt-out overrides.

Every enum below is a :class:`enum.StrEnum` (Python 3.11+). StrEnum members
ARE strings: comparing a member to its value with ``==`` yields ``True``,
and any context that expects a :class:`str` (Postgres column, Kafka header,
JSON payload, log line, Prometheus label) accepts a member without an
explicit ``.value`` access. This eliminates a whole class of "I forgot
``.value``" bugs that used to surface with the ``(str, Enum)`` mixin
pattern.

Architectural invariants (AAP-mandated)
---------------------------------------
* **Import-graph root** --- this module imports ONLY from the Python
  standard library (``enum`` and ``__future__``). It MUST NOT depend on
  any other ``src/*`` module, on Pydantic, on a Kafka client, on a
  database driver, or on the standard ``logging`` library. It is the
  most foundational package in ``services/notification-service/src/``
  alongside :mod:`src.domain.errors` and :mod:`src.domain.events` ---
  every other ``src/*`` package imports from here.
* **No I/O, no side effects at import time** --- only enum construction
  and the ``__all__`` list are evaluated when this module is loaded.
* **Explicit string values, not** :func:`enum.auto` --- the strings
  defined here are externally visible: they appear in PostgreSQL columns,
  Kafka message headers, JSON payloads, and Kibana log indices. Using
  :func:`enum.auto` would silently change the values if members were
  reordered or inserted, breaking persistence and log queries. Explicit
  literals are stable across refactors.
* **No methods, no ``raise`` statements** --- pure-data enums stay easy
  to reason about. Behavior such as "is this outcome retryable?" lives
  in the scheduler / router that consumes these enums, not on the enum
  itself. This preserves the "thin domain" invariant.
* **Value casing is intentional** --- :class:`NotificationStatus` and
  :class:`DeliveryOutcome` members render as ``UPPER_SNAKE_CASE`` to
  match the PostgreSQL state-machine column convention; while
  :class:`ChannelType` and :class:`CriticalFlag` render as
  ``lower_snake_case`` to match Kafka header conventions and JSON-friendly
  template metadata. The cases are deliberately distinct so that joined
  queries like ``SELECT channel, status FROM notification_log`` produce
  visually disambiguated rows.

Compliance notes
----------------
* AAP Section 0.1.1 Component #8 --- Notification Service must support
  multi-channel delivery for both Email and SMS; :class:`ChannelType`
  enumerates the two supported channels.
* AAP Section 0.4.4 --- the ``notification_db`` schema includes the
  ``notification_log``, ``delivery_attempts``, ``templates``, and
  ``user_channel_prefs`` tables; this module's enums are the canonical
  values for the ``status``, ``channel``, and ``criticality`` columns.
* AAP R-11 --- both Email and SMS channels are integrated concurrently
  behind a unified :class:`NotificationChannel` interface;
  :class:`ChannelType` is the discriminator. :class:`CriticalFlag`
  enables a critical template to override a user's opt-out preference.
* AAP R-15 --- every outbound HTTP call must have a retry policy with
  exponential backoff and bounded retry attempts;
  :class:`DeliveryOutcome.RETRYABLE` and
  :class:`DeliveryOutcome.TERMINAL` are the discriminator values that
  drive scheduler decisions.
* AAP R-17 --- every Kafka consumer must support retry topics and
  dead-letter topics; :class:`NotificationStatus.PENDING_RETRY` and
  :class:`NotificationStatus.DEAD_LETTER` mark rows on the retry-topic
  and DLQ-topic boundaries respectively.
* AAP R-26 --- structured JSON logs must include status fields; StrEnum
  members serialize directly into JSON without any encoder customization.

Cross-references
----------------
This module is imported by every other ``src/*`` package that handles
channel selection, status persistence, delivery outcomes, or template
criticality:

* ``src/channels/channel.py`` --- :class:`NotificationChannel`'s
  ``channel_type`` property is typed as :class:`ChannelType`.
* ``src/channels/email/email_channel.py`` --- declares
  ``channel_type = ChannelType.EMAIL``.
* ``src/channels/sms/sms_channel.py`` --- declares
  ``channel_type = ChannelType.SMS``.
* ``src/channels/router.py`` --- reads :class:`CriticalFlag` from
  template metadata to decide whether a critical template should bypass
  a user's opt-out preference.
* ``src/repository/models.py`` --- the ``status`` column on
  ``NotificationLogRow`` is typed as :class:`NotificationStatus`.
* ``src/repository/notification_log_repo.py`` --- writes every
  :class:`NotificationStatus` member through the row lifecycle.
* ``src/repository/delivery_attempts_repo.py`` --- writes
  :class:`ChannelType` values into the ``channel`` column on each
  attempt row.
* ``src/scheduler/scheduler.py`` --- branches on
  :class:`DeliveryOutcome.RETRYABLE` versus
  :class:`DeliveryOutcome.TERMINAL` to decide retry vs. DLQ routing.
* ``src/scheduler/dlq_writer.py`` --- uses :class:`ChannelType` to
  select the destination DLQ topic name (``email.dlq`` /
  ``sms.dlq``).
* ``src/events/handlers/*.py`` --- every handler returns
  :class:`DeliveryOutcome` from its ``handle()`` method.
* :mod:`src.domain` --- re-exports every class defined here for
  ergonomic short imports inside the service.

References
----------
* AAP Section 0.1.1 Component #8 --- Notification Service overview.
* AAP Section 0.4.4 --- ``notification_db`` schema.
* AAP R-11 --- dual-channel concurrency behind a unified interface.
* AAP R-15 --- exponential-backoff retry policy.
* AAP R-17 --- retry / DLQ topic routing.
* AAP R-26 --- structured JSON logs include status fields.
* Folder spec for ``services/notification-service/src/domain/``.
"""

from __future__ import annotations

from enum import StrEnum


class ChannelType(StrEnum):
    """Transport channel for a notification.

    Canonical values persisted in the ``user_channel_prefs.channel``
    column and in ``delivery_attempts.channel``. Also used in Kafka
    message headers when a single event targets multiple channels and in
    structured log fields for per-channel error-rate dashboards.

    Members:
        EMAIL: Email channel. Implemented by the ``EmailChannel`` adapter
            (which dispatches to SendGrid or AWS SES via per-deployment
            configuration). Value: ``"email"``.
        SMS: SMS channel. Implemented by the ``SmsChannel`` adapter
            (which dispatches to Twilio or AWS SNS via per-deployment
            configuration). Value: ``"sms"``.

    Example:
        >>> ChannelType.EMAIL == "email"
        True
        >>> ChannelType("email") is ChannelType.EMAIL
        True
        >>> isinstance(ChannelType.SMS, str)
        True
        >>> f"channel={ChannelType.EMAIL}"
        'channel=email'
    """

    EMAIL = "email"
    SMS = "sms"


class NotificationStatus(StrEnum):
    """Lifecycle state of a notification row in ``notification_log``.

    The status drives both persistence and dispatch decisions:

    * Happy path: ``PENDING`` -> ``SUCCESS``.
    * Retry path: ``PENDING`` -> ``PENDING_RETRY`` -> ``SUCCESS`` |
      ``DEAD_LETTER``.
    * Cancelled path: ``PENDING`` -> ``CANCELLED`` (e.g., user opted-out
      after the event arrived but before dispatch completed); also
      ``PENDING_RETRY`` -> ``CANCELLED`` if a downstream event
      supersedes the in-flight notification.

    ``SUCCESS``, ``DEAD_LETTER``, and ``CANCELLED`` are all terminal:
    once a row reaches one of these states the scheduler will not
    re-attempt delivery, and the corresponding Kafka offset / DLQ row
    has already been committed.

    Members:
        PENDING: Initial state. The dispatcher has accepted the event
            and queued a delivery attempt. Value: ``"PENDING"``.
        SUCCESS: Provider confirmed acceptance (HTTP 2xx). Terminal.
            Value: ``"SUCCESS"``.
        PENDING_RETRY: Last attempt failed with a retryable error
            (5xx, 429, timeout, connection error); the
            :class:`RetryScheduler` will pick the row up on the next
            poll and back off per AAP R-15. Value: ``"PENDING_RETRY"``.
        DEAD_LETTER: All retry attempts exhausted, or a terminal
            provider error occurred (invalid recipient, account
            suspended, permanent template error, 4xx non-429). The row
            has been routed to ``<channel>.dlq`` per AAP R-17.
            Terminal. Value: ``"DEAD_LETTER"``.
        CANCELLED: Delivery was deliberately aborted (user opt-out
            change applied after enqueue, template deleted, event
            superseded by a later domain event). Distinguished from
            ``DEAD_LETTER`` so Kibana dashboards can separate
            "deliberately not sent" from "tried and failed". Terminal.
            Value: ``"CANCELLED"``.

    Example:
        >>> NotificationStatus.PENDING == "PENDING"
        True
        >>> NotificationStatus("PENDING_RETRY") is NotificationStatus.PENDING_RETRY
        True
        >>> isinstance(NotificationStatus.DEAD_LETTER, str)
        True
    """

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    PENDING_RETRY = "PENDING_RETRY"
    DEAD_LETTER = "DEAD_LETTER"
    CANCELLED = "CANCELLED"


class DeliveryOutcome(StrEnum):
    """Outcome classification returned by a :class:`NotificationChannel`.

    The :class:`RetryScheduler` and the dispatcher consume this enum to
    decide the next action after each delivery attempt:

    * ``SUCCESS`` --- commit the Kafka consumer offset, mark the
      ``notification_log`` row as ``NotificationStatus.SUCCESS``.
    * ``RETRYABLE`` --- enqueue the row for the scheduler's next poll,
      mark the row as ``NotificationStatus.PENDING_RETRY``, and apply
      exponential backoff with jitter per AAP R-15. After the
      configured max-retry budget is exhausted the scheduler escalates
      to ``TERMINAL`` semantics.
    * ``TERMINAL`` --- route the row to ``<channel>.dlq`` immediately,
      mark the row as ``NotificationStatus.DEAD_LETTER``, and do NOT
      retry. The three-way split (rather than a binary success / failure)
      keeps the retry budget from being wasted on errors that have no
      chance of succeeding (e.g., an invalid email address).

    Members:
        SUCCESS: Provider accepted the message (HTTP 2xx). No further
            action required; the dispatcher commits the offset.
            Value: ``"SUCCESS"``.
        RETRYABLE: Transient failure --- 5xx server error, 429 rate
            limit, request timeout, connection reset, or any other
            error the provider documents as recoverable. Scheduler
            MUST retry per AAP R-15. Value: ``"RETRYABLE"``.
        TERMINAL: Non-retryable failure --- invalid recipient address,
            account suspended, permanent template-rendering error,
            4xx client error other than 429. Scheduler MUST route to
            DLQ per AAP R-17 and MUST NOT retry. Value: ``"TERMINAL"``.

    Example:
        >>> DeliveryOutcome.SUCCESS == "SUCCESS"
        True
        >>> DeliveryOutcome("RETRYABLE") is DeliveryOutcome.RETRYABLE
        True
        >>> isinstance(DeliveryOutcome.TERMINAL, str)
        True
    """

    SUCCESS = "SUCCESS"
    RETRYABLE = "RETRYABLE"
    TERMINAL = "TERMINAL"


class CriticalFlag(StrEnum):
    """Template-level criticality classifier.

    Indicates whether a given notification template may override a
    user's opt-out preference for the target channel. Stored as the
    ``criticality`` column on the ``templates`` table (AAP Section
    0.4.4) and embedded in template metadata JSON. Consumed by the
    :class:`ChannelRouter` to decide whether to honor or bypass the
    user's :class:`UserPreferences` opt-out flag.

    The classifier is deliberately a separate enum from
    :data:`src.domain.events.CRITICAL_EVENTS`:

    * :class:`CriticalFlag` is **template-level** --- attached to each
      template row and consulted on every dispatch.
    * :data:`CRITICAL_EVENTS` is **event-level** --- attached to the
      :class:`EventType` taxonomy and consulted once per event type.

    Both feed into the channel router's opt-out-override decision but
    operate at different granularities. Keeping them decoupled means a
    new critical template can be added without modifying
    ``events.py``, and a new critical event can be added without
    touching every existing template.

    Adding new ``CRITICAL`` templates or events is a product-policy
    decision and SHOULD be reviewed against regional regulations
    (GDPR, CAN-SPAM, TCPA) --- unsolicited SMS messaging is heavily
    regulated in most jurisdictions and overriding an opt-out
    inappropriately exposes the business to legal risk.

    Members:
        CRITICAL: Template MAY bypass the user's opt-out for its
            channel. Reserved for security, payment, fraud, and
            account-state messages (e.g., ``payment.failed`` SMS
            still sends even when the user has opted out of SMS
            marketing). Value: ``"critical"``.
        NON_CRITICAL: Template MUST respect the user's opt-out
            strictly. Reserved for marketing, promotional, and other
            optional communications. Value: ``"non_critical"``.

    Example:
        >>> CriticalFlag.CRITICAL == "critical"
        True
        >>> CriticalFlag("non_critical") is CriticalFlag.NON_CRITICAL
        True
        >>> isinstance(CriticalFlag.CRITICAL, str)
        True
    """

    CRITICAL = "critical"
    NON_CRITICAL = "non_critical"


__all__ = [
    "ChannelType",
    "CriticalFlag",
    "DeliveryOutcome",
    "NotificationStatus",
]
