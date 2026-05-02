"""Kafka topic name constants and retry/DLQ helpers for the Order Service.

The constants mirror the defaults in ``../config/default.yaml`` and the
companion env-var template ``../.env.example``. Topic names follow AAP R-30
(``<domain>.<verb>``); retry / DLQ suffixes follow AAP R-17.

Why constants in addition to settings?
    The settings module (``src/config/settings.py``) is the single source of
    truth at runtime — operators may override topic names via env vars per
    environment. The constants in this file are the in-code defaults used by
    handlers, tests, and ad-hoc scripts that need a stable reference. Both
    layers MUST stay in lock-step.

The Order Service plays a DUAL role on the Kafka bus per AAP Section 0.4.2:

    PRODUCER of the ``order.*`` domain topics (saga lifecycle events):
        * ``order.created``   — saga kickoff, consumed by Inventory + Payment +
                                Notification + Recommendation services
        * ``order.cancelled`` — saga compensation outcome
        * ``order.fulfilled`` — saga terminal success state

    CONSUMER of the ``inventory.*`` and ``payment.*`` step-reply topics for
    saga choreography (AAP R-18 — explicit compensation steps):
        * ``inventory.reserved``             — advances saga PENDING -> RESERVED
        * ``inventory.reservation_failed``   — triggers saga compensation
        * ``payment.succeeded``              — advances saga RESERVED -> PAID
        * ``payment.failed``                 — triggers inventory release
                                                compensation

The service-level ``order.dlq`` catches outbound poison messages (events the
producer cannot serialize). Per-consumed-topic DLQs are computed at runtime via
``dlq_topic()`` (e.g., ``inventory.reserved.dlq``).

This module is intentionally side-effect-free: no env-var reads, no logging,
no I/O. Importing it MUST NOT trigger any work — it is a pure declarative
reference. Runtime overrides flow exclusively through ``src/config/settings.py``.

AAP rules satisfied: R-17, R-30.
"""

from __future__ import annotations

from typing import Final

# -----------------------------------------------------------------------------
# Topics PRODUCED by Order Service (saga coordinator emits these on state
# transitions). Mirrors AAP Section 0.4.2 + ../config/default.yaml topics.produced.
# -----------------------------------------------------------------------------
ORDER_CREATED: Final[str] = "order.created"
ORDER_CANCELLED: Final[str] = "order.cancelled"
ORDER_FULFILLED: Final[str] = "order.fulfilled"

# -----------------------------------------------------------------------------
# Topics CONSUMED by Order Service (saga state machine drivers).
# Mirrors AAP Section 0.4.2 + ../config/default.yaml topics.consumed.
# -----------------------------------------------------------------------------
INVENTORY_RESERVED: Final[str] = "inventory.reserved"
INVENTORY_RESERVATION_FAILED: Final[str] = "inventory.reservation_failed"
PAYMENT_SUCCEEDED: Final[str] = "payment.succeeded"
PAYMENT_FAILED: Final[str] = "payment.failed"

# -----------------------------------------------------------------------------
# Service-level Dead-Letter Topic for poison messages (events the consumer
# cannot deserialize at all). Per AAP R-17 each *consumed* topic also has its
# own ``<topic>.dlq`` companion produced by ``dlq_topic()`` below.
# Mirrors AAP Section 0.4.2 + ../config/default.yaml topics.dlq.order.
# -----------------------------------------------------------------------------
ORDER_DLQ: Final[str] = "order.dlq"

# -----------------------------------------------------------------------------
# Suffixes from default.yaml kafka.retry. Operators may override via env vars
# (KAFKA_RETRY_TOPIC_SUFFIX, KAFKA_DLQ_TOPIC_SUFFIX); the consumer/runner reads
# the runtime values from settings, but these constants are the canonical
# defaults for handlers and tests that need a stable reference.
# -----------------------------------------------------------------------------
RETRY_TOPIC_SUFFIX: Final[str] = ".retry"
DLQ_TOPIC_SUFFIX: Final[str] = ".dlq"


# -----------------------------------------------------------------------------
# Convenience tuples — used by tests, the consumer runner, and ad-hoc tooling.
# A test asserting every consumed topic has a registered handler can iterate
# ``CONSUMED_TOPICS`` instead of repeating the literal list.
# -----------------------------------------------------------------------------
PRODUCED_TOPICS: Final[tuple[str, ...]] = (
    ORDER_CREATED,
    ORDER_CANCELLED,
    ORDER_FULFILLED,
)

CONSUMED_TOPICS: Final[tuple[str, ...]] = (
    INVENTORY_RESERVED,
    INVENTORY_RESERVATION_FAILED,
    PAYMENT_SUCCEEDED,
    PAYMENT_FAILED,
)


# -----------------------------------------------------------------------------
# Helpers — compile-time defaults with runtime override hook (AAP R-17).
# -----------------------------------------------------------------------------


def retry_topic(topic: str, *, suffix: str = RETRY_TOPIC_SUFFIX) -> str:
    """Compute the retry topic name for a given input topic (AAP R-17).

    The retry topic carries messages whose first processing attempt failed but
    have not yet exhausted the retry budget. The consumer subscribes to both
    the primary topic and its retry sibling and uses the ``attempt-count``
    header to differentiate retries from first attempts. After the maximum
    in-process attempts are exhausted, the message is routed to the DLQ topic
    via ``dlq_topic()``.

    Args:
        topic: The primary topic name (e.g., ``inventory.reserved``).
        suffix: Override the suffix; defaults to ``.retry`` (the value in
            ``../config/default.yaml`` ``kafka.retry.topic_suffix``). Operators
            may override at runtime via the ``KAFKA_RETRY_TOPIC_SUFFIX`` env var
            and pass the resolved value here.

    Returns:
        ``f"{topic}{suffix}"`` — e.g., ``"inventory.reserved.retry"``.

    Raises:
        ValueError: If ``topic`` is empty. Returning ``".retry"`` for an empty
            input would silently subscribe the consumer to a non-existent
            topic and is a hard-to-diagnose ops bug — fail fast instead.

    Example:
        >>> retry_topic("inventory.reserved")
        'inventory.reserved.retry'
        >>> retry_topic("inventory.reserved", suffix=".RETRY")
        'inventory.reserved.RETRY'
    """
    if not topic:
        raise ValueError("topic must be a non-empty string")
    return f"{topic}{suffix}"


def dlq_topic(topic: str, *, suffix: str = DLQ_TOPIC_SUFFIX) -> str:
    """Compute the DLQ topic name for a given input topic (AAP R-17).

    The DLQ topic carries messages that exhausted retries (poison messages,
    schema-validation failures, or unrecoverable handler errors). DLQ messages
    are inspected in Kibana and replayed by ops tooling; they are NOT consumed
    by the same consumer group. Each consumed topic owns a dedicated DLQ
    sibling so triage can isolate failures by domain (e.g., a schema drift in
    Payment events does not pollute the Inventory DLQ).

    Args:
        topic: The primary topic name (e.g., ``payment.failed``).
        suffix: Override the suffix; defaults to ``.dlq`` (the value in
            ``../config/default.yaml`` ``kafka.retry.dlq_topic_suffix``).
            Operators may override at runtime via ``KAFKA_DLQ_TOPIC_SUFFIX``.

    Returns:
        ``f"{topic}{suffix}"`` — e.g., ``"payment.failed.dlq"``.

    Raises:
        ValueError: If ``topic`` is empty. Returning ``".dlq"`` for an empty
            input would silently route messages to a non-existent topic and
            is a hard-to-diagnose ops bug — fail fast instead.

    Example:
        >>> dlq_topic("payment.failed")
        'payment.failed.dlq'
        >>> dlq_topic("payment.failed", suffix=".DLQ")
        'payment.failed.DLQ'
    """
    if not topic:
        raise ValueError("topic must be a non-empty string")
    return f"{topic}{suffix}"


# -----------------------------------------------------------------------------
# Public API surface — explicit allow-list keeps re-exports deliberate.
# 8 topic constants + 2 suffix constants + 2 convenience tuples + 2 helpers = 14.
# -----------------------------------------------------------------------------
__all__ = [
    # Produced
    "ORDER_CREATED",
    "ORDER_CANCELLED",
    "ORDER_FULFILLED",
    # Consumed
    "INVENTORY_RESERVED",
    "INVENTORY_RESERVATION_FAILED",
    "PAYMENT_SUCCEEDED",
    "PAYMENT_FAILED",
    # DLQ
    "ORDER_DLQ",
    # Suffixes
    "RETRY_TOPIC_SUFFIX",
    "DLQ_TOPIC_SUFFIX",
    # Convenience tuples
    "PRODUCED_TOPICS",
    "CONSUMED_TOPICS",
    # Helpers
    "retry_topic",
    "dlq_topic",
]
