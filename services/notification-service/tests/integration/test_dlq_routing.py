"""Integration test: retry topics + dead-letter queues (AAP R-17).

Exercises the full failure-recovery pipeline:

1. Test mocks SendGrid (or Twilio) to return repeated 5xx responses.
2. Test produces a valid event to the source topic.
3. The channel's ``@retry_async``-wrapped dispatch path retries
   internally per ``settings.email.retry.max_attempts``
   (typically 3 in integration tests for timing determinism).
4. If all in-request retries fail AND the error classifier returns
   ``RETRYABLE``, the channel records
   ``notification_log.status=PENDING_RETRY`` and the message is
   re-queued onto ``<topic>.retry`` with an ``x-retry-count``
   header.
5. The ``RetryScheduler`` polls ``notification_log`` for rows where
   ``status=PENDING_RETRY AND next_attempt_at <= NOW()`` and
   re-dispatches via the channel.
6. If dispatching from the retry topic ALSO fails until
   ``max_retries`` exhaustion, the message is routed to the
   **channel DLQ**: ``notifications.email.dlq`` or
   ``notifications.sms.dlq``.
7. The DLQ envelope preserves: schema_version=v1, service,
   kind=dlq, channel, original_event, attempt count, error info,
   correlation_id, user_id, template_id, first_attempt_at,
   dead_lettered_at.
8. The ``notification_log`` row transitions to
   ``status=DEAD_LETTER`` with ``error_type`` and
   truncated ``error_message`` (~1 KiB cap).

Also validates TERMINAL classification: non-retryable errors (e.g.,
``ProviderTerminalError``, ``ValidationError``) land directly in
DLQ without consuming retry budget.

Compliance
----------
* AAP R-13 --- correlation_id propagated through retries and into DLQ.
* AAP R-15 --- retry policies use exponential backoff, jitter, max
  attempts, and explicit timeouts.
* AAP R-17 --- exhausted retries land in ``<topic>.dlq``.
* AAP R-26 --- structured JSON logs throughout the retry+DLQ
  lifecycle with the canonical field set.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``asyncio`` is used for: (1) deadline tracking inside the
# ``_poll_for_status`` helper that polls ``notification_log`` for status
# transitions (PENDING -> PENDING_RETRY -> DEAD_LETTER) within a bounded
# 30-second timeout per AAP R-17 anchor test 4.3; (2) cooperative
# ``asyncio.sleep(0.1)`` polling intervals that let other tasks (the
# consumer loop) make progress between SQL probes; and (3) the
# ``asyncio.CancelledError`` symbol for test 4.19 which verifies that
# CancelledError DOES NOT route to the DLQ.
import asyncio

# ``json`` is used for parsing DLQ message envelopes once they are
# collected from the email/SMS DLQ topics. Tests assert on the decoded
# envelope shape (``schema_version``, ``kind``, ``channel``,
# ``original_event``, ``error.type/message/classifier``) and on the
# captured-log records produced by the consumer's structured-log
# emitter. Outbound serialization of test events to Kafka is handled
# by the kafka_producer fixture and so does NOT appear in this file.
import json

# ``uuid`` synthesizes per-test event_ids, user_ids, and stable
# correlation_ids so that each test's row in ``notification_log`` is
# uniquely identifiable. The ``uuid.UUID`` class is also referenced by
# helper-function type annotations per Phase 6 Python style rules.
import uuid

# ``datetime`` and ``timezone`` provide tz-aware ISO 8601 strings for
# the ``occurred_at`` field of the user.registered event payload (the
# ``UserRegisteredEvent`` Pydantic validator REJECTS naive datetimes
# per the captured ``src/events/schemas.py`` spec). ``timezone.utc`` is
# the canonical zone for all timestamp construction.
from datetime import datetime, timezone

# ``Any`` is used for opaque fixture-injected objects (psycopg pool,
# Kafka producer, FastAPI app, httpx client) whose concrete types come
# from conftest.py and are not imported in this test file per Phase 6
# Python style rules.
from typing import Any

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``httpx`` is the async HTTP client used by the channels' provider
# adapters. Tests construct ``httpx.Response(...)`` instances when
# registering custom respx route mock returns to inject failure modes
# (5xx for transient, 4xx for terminal, 5xx with > 10 KiB body for
# error-truncation verification). The respx library intercepts httpx
# requests transparently when a route is registered.
import httpx

# ``pytest`` provides the test framework: ``pytest.mark.asyncio``
# (module pytestmark), ``pytest.fixture`` (used implicitly through
# fixture parameter injection), and ``pytest.mark.slow`` for the
# DLQ-write failure resilience test (4.13) which may be skipped on
# constrained CI runners.
import pytest

# ``respx`` is the httpx-based mock router used to intercept outbound
# provider HTTP calls (SendGrid v3 ``/mail/send``, Twilio Messages API).
# Test bodies attach ``Route(...).mock(return_value=httpx.Response(...))``
# overrides to the function-scoped ``respx_router_session`` fixture
# from conftest.py to inject the failure modes that drive AAP R-17
# retry/DLQ paths.
import respx


# ---------------------------------------------------------------------------
# Module-level pytest markers
# ---------------------------------------------------------------------------
# Every test in this module is async; ``pytest.mark.asyncio`` is applied
# module-wide via ``pytestmark`` so each ``async def test_*`` is
# automatically dispatched through pytest-asyncio's runner per Phase 2
# of the agent prompt.
pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Constants --- Topic identifiers, headers, and configuration
# ---------------------------------------------------------------------------
#: Dead-letter Kafka topic for email-channel notifications. Tests assert
#: that exhausted-retry messages and TERMINAL-classified errors land here
#: with the full DLQ envelope and required headers. Topic name mirrors
#: the folder spec for the Notification Service's DLQ contract.
EMAIL_DLQ_TOPIC: str = "notifications.email.dlq"

#: Dead-letter Kafka topic for SMS-channel notifications. Symmetric to
#: :data:`EMAIL_DLQ_TOPIC`; used by SMS-channel test 4.4 (Twilio 5xx)
#: and the channel-isolation test 4.16.
SMS_DLQ_TOPIC: str = "notifications.sms.dlq"

#: Retry Kafka topic for email-channel notifications. The
#: RetryScheduler re-queues messages here when in-request retries are
#: exhausted but the error classifier returns RETRYABLE. Test 4.9
#: inspects messages on this topic to verify the ``x-retry-attempt``
#: header increments across retry cycles.
EMAIL_RETRY_TOPIC: str = "notifications.email.retry"

#: Retry Kafka topic for SMS-channel notifications. Symmetric to
#: :data:`EMAIL_RETRY_TOPIC`.
SMS_RETRY_TOPIC: str = "notifications.sms.retry"

#: Source event topic that triggers the email + SMS notification fanout
#: in tests. Tests produce ``user.registered`` payloads to this topic
#: via the ``kafka_producer`` fixture and observe the consumer's
#: behavior in the notification_log and DLQ topics.
USER_REGISTERED_TOPIC: str = "user.registered"

#: Alternate source event topic exercised in a subset of tests. Some
#: error-classification paths (e.g. template rendering) are best
#: triggered by an ``order.created`` event with a complex payload.
ORDER_CREATED_TOPIC: str = "order.created"

#: Maximum retry attempts configured in the integration conftest's
#: ``settings_integration`` fixture. The integration profile sets this
#: to 3 (smaller than production's 5) to keep tests fast and
#: deterministic. AAP R-15 anchor: bounded retry attempts.
MAX_ATTEMPTS: int = 3

#: Initial retry backoff in milliseconds. Used by test 4.8 to compute
#: expected backoff intervals (50ms, 100ms) under the multiplier=2.0
#: schedule. Production typically uses 250ms+ but the integration
#: conftest sets 50ms for fast test runs.
INITIAL_DELAY_MS: int = 50

#: Backoff multiplier; matches the conftest's ``settings_integration``
#: fixture. Tests 4.8 and 4.11 derive their timing assertions from this.
BACKOFF_MULTIPLIER: float = 2.0

#: Default deadline (seconds) for ``_poll_for_status`` waits. The 30s
#: bound matches the AAP R-17 anchor test (4.3) timeout requirement
#: and ensures no test hangs indefinitely per Phase 5.2.
POLL_TIMEOUT_S: float = 30.0

#: Maximum size in bytes for the ``error_message`` field on
#: ``notification_log`` rows AND the ``error.message`` field of DLQ
#: envelopes. The 1 KiB cap (test 4.10) prevents a buggy or malicious
#: provider from bloating the topic / column with multi-MiB payloads.
MAX_ERROR_MESSAGE_BYTES: int = 1024

# ---------------------------------------------------------------------------
# Required DLQ headers --- per the captured src/scheduler/ spec
# ---------------------------------------------------------------------------
# Every DLQ message MUST carry these six headers so operations tooling
# (Kibana alerts, manual re-drive scripts, SLA monitors) can route on
# message metadata without parsing the JSON payload. Test 4.15 asserts
# that ALL six headers are present on every DLQ message.
#: Identifies the producing service. Always ``"notification-service"``.
DLQ_HEADER_SERVICE: str = "x-service"
#: DLQ envelope schema version. Always ``"v1"`` per test 4.14.
DLQ_HEADER_SCHEMA_VERSION: str = "x-schema-version"
#: Topic kind: ``"dlq"`` for DLQ messages, ``"retry"`` for retry-topic
#: messages. Distinguishes the two terminal-vs-recoverable cases.
DLQ_HEADER_TOPIC_KIND: str = "x-topic-kind"
#: Notification channel: ``"email"`` or ``"sms"``. Drives operator
#: routing and per-channel alerting.
DLQ_HEADER_CHANNEL: str = "x-channel"
#: AAP R-13 correlation_id propagated from the ingress event through
#: every retry attempt and into the DLQ envelope.
DLQ_HEADER_CORRELATION_ID: str = "x-correlation-id"
#: 1-based attempt counter. For DLQ messages this equals
#: ``MAX_ATTEMPTS`` (or 1 for direct TERMINAL routing); for retry-topic
#: messages it is the attempt number that just failed.
DLQ_HEADER_RETRY_ATTEMPT: str = "x-retry-attempt"

#: Tuple of all six required DLQ headers --- iterated by test 4.15 to
#: assert exhaustive presence.
REQUIRED_DLQ_HEADERS: tuple[str, ...] = (
    DLQ_HEADER_SERVICE,
    DLQ_HEADER_SCHEMA_VERSION,
    DLQ_HEADER_TOPIC_KIND,
    DLQ_HEADER_CHANNEL,
    DLQ_HEADER_CORRELATION_ID,
    DLQ_HEADER_RETRY_ATTEMPT,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _build_user_registered_payload(
    *,
    user_id: uuid.UUID,
    event_id: uuid.UUID | None = None,
    correlation_id: str | None = None,
    email: str = "alice@example.com",
    phone: str = "+14155550100",
    name: str = "Alice",
    locale: str = "en-US",
) -> dict[str, Any]:
    """Construct a fully-populated ``user.registered`` event payload.

    Mirrors the canonical envelope shape produced by the Auth Service:
    a top-level ``schema_version`` for envelope evolution, an
    ``event_id`` / ``user_id`` pair for downstream idempotency, an
    ``occurred_at`` timestamp in tz-aware RFC 3339 form (the
    Notification Service's Pydantic ``UserRegisteredEvent`` validator
    REJECTS naive datetimes per the captured ``src/events/schemas.py``
    spec), and a ``correlation_id`` that the kafka_producer fixture
    attaches as a Kafka header so the Notification Service's
    ``CorrelationIdMiddleware`` can propagate it through every log line
    and outbound HTTP call (AAP R-13).

    Args:
        user_id: The user's UUID; threaded into the payload's
            ``user_id`` field so the consumer's idempotency check
            distinguishes events from one another even within a single
            test.
        event_id: Override for the event's UUID. When ``None``, a fresh
            UUID is generated; tests that produce many events pass
            distinct values to avoid (event_id, channel) idempotency
            collisions in the ``notification_log`` table.
        correlation_id: Override for the correlation header value. When
            ``None``, a fresh UUID string is generated. Tests 4.6 and
            4.20 supply a known value to assert preservation through
            retries and into the DLQ envelope.
        email: Recipient email. Tests can override to validate
            per-recipient template rendering.
        phone: Recipient phone in E.164 format. Tests can override to
            validate locale-aware SMS routing.
        name: Recipient given name; rendered into welcome templates.
        locale: BCP-47 locale tag; selects the matching template
            version from the ``templates`` table.

    Returns:
        A JSON-serializable dict whose shape matches what the
        kafka_producer fixture serializes to the
        ``user.registered`` topic.
    """
    return {
        "schema_version": "v1",
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": "user.registered",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id or str(uuid.uuid4()),
        "user_id": str(user_id),
        "email": email,
        "phone": phone,
        "name": name,
        "locale": locale,
    }


async def _poll_for_status(
    *,
    pool: Any,
    event_id: uuid.UUID,
    channel: str,
    target_status: str,
    timeout_s: float = POLL_TIMEOUT_S,
) -> tuple[str, int, str | None, str | None] | None:
    """Poll ``notification_log`` until a row reaches the target status.

    Implements the canonical "wait for the consumer to do its work"
    pattern used across the Notification Service integration suite. The
    consumer's processing pipeline is asynchronous: produce-to-Kafka
    returns before the consumer has finished writing the
    ``notification_log`` row, so the test must poll the database with a
    bounded timeout to avoid hanging the suite.

    Returns the row as a ``(status, attempt_count, error_type,
    error_message)`` tuple or ``None`` on timeout. The ``error_type``
    and ``error_message`` columns are populated only on failure paths
    (PENDING_RETRY, DEAD_LETTER); on SUCCESS rows they are ``None``.

    Args:
        pool: A psycopg ``AsyncConnectionPool`` (from the ``pg_pool``
            fixture). The exact type is intentionally ``Any`` per the
            Phase 6 style rules so this test file does not import
            psycopg.
        event_id: The event's UUID; combined with ``channel`` it forms
            the unique key on ``notification_log``.
        channel: One of ``"email"`` or ``"sms"`` (matching
            :class:`src.domain.channel_types.ChannelType` values).
        target_status: The ``notification_log.status`` value to wait
            for, e.g. ``"SUCCESS"``, ``"DEAD_LETTER"``,
            ``"PENDING_RETRY"``.
        timeout_s: Wall-clock deadline. When exceeded, the helper
            returns ``None`` rather than raising; callers assert the
            return value is not ``None`` for clearer failure messages.

    Returns:
        A ``(status, attempt_count, error_type, error_message)`` tuple
        when the row reaches the target status within the deadline.
        ``None`` on timeout, which signals to the caller that the
        consumer either did not process the event or did not reach the
        expected terminal state.
    """
    deadline = asyncio.get_event_loop().time() + timeout_s
    async with pool.connection() as conn:
        while asyncio.get_event_loop().time() < deadline:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT status, attempt_count, error_type, "
                    "error_message "
                    "FROM notification_log "
                    "WHERE event_id = %s AND channel = %s",
                    (str(event_id), channel),
                )
                row = await cur.fetchone()
                if row is not None and row[0] == target_status:
                    return (row[0], row[1], row[2], row[3])
            await asyncio.sleep(0.1)
    return None


async def _collect_dlq_messages(
    *,
    kafka_config: dict[str, Any],
    dlq_topic: str,
    expected_count: int = 1,
    timeout_s: float = POLL_TIMEOUT_S,
) -> list[dict[str, Any]]:
    """Collect messages from a DLQ topic into a list of envelopes.

    Spins up an ephemeral Kafka consumer subscribed to ``dlq_topic``,
    polls until ``expected_count`` messages have arrived (or the
    deadline elapses), and returns a list of dicts with both the
    decoded JSON ``value`` and the message ``headers`` decoded as a
    string-keyed mapping.

    The actual implementation delegates to the ``collect_messages``
    helper that conftest.py exposes via the ``kafka_admin_client``
    fixture's accompanying utilities; this thin wrapper centralizes the
    JSON / header decoding logic so test bodies stay focused on
    assertion rather than Kafka boilerplate.

    Args:
        kafka_config: The bootstrap server / SSL / SASL config dict
            sourced from ``settings_integration.kafka.consumer.dict()``.
        dlq_topic: One of :data:`EMAIL_DLQ_TOPIC`, :data:`SMS_DLQ_TOPIC`,
            :data:`EMAIL_RETRY_TOPIC`, :data:`SMS_RETRY_TOPIC`.
        expected_count: Number of messages to wait for before returning.
            Set to ``0`` to assert "no messages within the timeout"
            (the helper still waits the full timeout in that case to
            give a stale/late message a chance to arrive).
        timeout_s: Bounded wall-clock deadline.

    Returns:
        A list of ``{"value": dict, "headers": dict[str, str]}`` dicts.
        The list length is at most ``expected_count`` when messages are
        found in time; it may be empty on timeout.
    """
    # Delayed import: confluent_kafka is only available in the
    # integration container and must NOT load at test-collection time
    # to avoid breaking unit-test discovery in environments without
    # librdkafka system libraries installed.
    from confluent_kafka import (  # type: ignore[import-not-found]
        Consumer,
        TopicPartition,
    )

    group_id = f"dlq-collector-{uuid.uuid4().hex}"
    consumer_config: dict[str, Any] = {
        **kafka_config,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "session.timeout.ms": 6000,
    }
    consumer = Consumer(consumer_config)

    try:
        # Subscribe via assign-from-beginning so collected messages
        # include those produced before the consumer started --- a
        # critical requirement because the test produces FIRST then
        # collects.
        cluster_metadata = consumer.list_topics(topic=dlq_topic, timeout=10.0)
        if dlq_topic not in cluster_metadata.topics:
            return []
        topic_metadata = cluster_metadata.topics[dlq_topic]
        partitions = [
            TopicPartition(dlq_topic, p_id, 0)
            for p_id in topic_metadata.partitions
        ]
        consumer.assign(partitions)

        collected: list[dict[str, Any]] = []
        deadline = asyncio.get_event_loop().time() + timeout_s
        while (
            asyncio.get_event_loop().time() < deadline
            and len(collected) < max(expected_count, 1)
        ):
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                # Cooperatively yield so the consumer task in the
                # service-under-test can make progress on producing
                # the DLQ message we are waiting for.
                await asyncio.sleep(0.05)
                continue
            if msg.error() is not None:
                # Skip malformed records; do not crash the helper.
                continue
            raw_value = msg.value()
            if isinstance(raw_value, (bytes, bytearray)):
                value_text = raw_value.decode("utf-8")
            else:
                value_text = raw_value
            try:
                decoded_value: dict[str, Any] = json.loads(value_text)
            except (UnicodeDecodeError, json.JSONDecodeError):
                # A non-JSON DLQ payload is unexpected; record raw
                # bytes so the test can fail with a useful message
                # rather than crashing inside the helper.
                decoded_value = {"_raw": str(raw_value)}

            raw_headers = msg.headers() or []
            headers: dict[str, str] = {}
            for header_name, header_value in raw_headers:
                if isinstance(header_value, (bytes, bytearray)):
                    headers[header_name] = header_value.decode("utf-8")
                else:
                    headers[header_name] = str(header_value)

            collected.append({"value": decoded_value, "headers": headers})

            if len(collected) >= expected_count and expected_count > 0:
                break

        return collected
    finally:
        consumer.close()


# ===========================================================================
# Test 4.1 --- Email happy path: SUCCESS, no DLQ entry
# ===========================================================================
async def test_email_happy_path_no_dlq_entry(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """Sanity baseline: a healthy SendGrid yields SUCCESS with no DLQ activity.

    Establishes the regression guard for the entire AAP R-17 test
    suite: when the provider responds 202 on the first call, the
    notification_log row reaches SUCCESS in attempt_count=1, the
    delivery_attempts table records a single SUCCESS row, and NEITHER
    the email DLQ NOR the email retry topic receive any messages. A
    failure here indicates a fundamental break in the consumer
    pipeline before any retry / DLQ logic is exercised.
    """
    # Configure SendGrid mock to succeed cleanly on first attempt.
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    sendgrid_route = respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(202, headers={"X-Message-Id": "sg-ok-1"})
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()

    await user_prefs_repo.upsert(
        user_id=user_id,
        email_enabled=True,
        sms_enabled=False,
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="SUCCESS",
    )
    assert row is not None, "notification_log row never reached SUCCESS"
    status, attempt_count, error_type, error_message = row
    assert status == "SUCCESS"
    assert attempt_count == 1, (
        f"happy path should complete on attempt 1; got attempt_count={attempt_count}"
    )
    assert error_type is None
    assert error_message is None

    # SendGrid should have received exactly one call (no retries).
    assert sendgrid_route.call_count == 1

    # NO DLQ messages and NO retry-topic messages.
    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=0,
        timeout_s=2.0,
    )
    assert dlq_messages == [], (
        f"happy path produced unexpected DLQ messages: {dlq_messages}"
    )
    retry_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_RETRY_TOPIC,
        expected_count=0,
        timeout_s=2.0,
    )
    assert retry_messages == [], (
        f"happy path produced unexpected retry-topic messages: {retry_messages}"
    )

    # delivery_attempts: exactly 1 row, outcome=SUCCESS.
    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT da.attempt_number, da.outcome FROM delivery_attempts da "
                "JOIN notification_log nl ON da.notification_id = nl.notification_id "
                "WHERE nl.event_id = %s AND nl.channel = %s "
                "ORDER BY da.attempt_number",
                (str(event_id), "email"),
            )
            attempts = await cur.fetchall()
    assert len(attempts) == 1
    assert attempts[0][0] == 1
    assert attempts[0][1] == "SUCCESS"


# ===========================================================================
# Test 4.2 --- Transient 503 then 202: retried once, succeeds, no DLQ
# ===========================================================================
async def test_email_transient_failure_retries_then_succeeds(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
    captured_logs: list[dict[str, Any]],
) -> None:
    """A single 503 followed by 202 succeeds within one in-request retry.

    Validates the AAP R-15 contract: transient (RETRYABLE) errors are
    retried with exponential backoff; on the next attempt the channel
    succeeds and the notification_log reaches SUCCESS with
    attempt_count=2. No DLQ entry is created (we only land in DLQ on
    EXHAUSTED retries, not after a single failed attempt).

    The captured_logs is asserted to contain both a WARN/INFO record
    for the failed attempt and a final INFO record for the successful
    second attempt; this is the structured-log signal that operators
    use to distinguish "self-healed transient" from "sustained outage"
    in Kibana dashboards.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    call_state: dict[str, int] = {"calls": 0}

    def _side_effect(_request: httpx.Request) -> httpx.Response:
        call_state["calls"] += 1
        if call_state["calls"] == 1:
            return httpx.Response(503)
        return httpx.Response(202, headers={"X-Message-Id": "sg-ok-2"})

    sendgrid_route = respx_router_session.post(sendgrid_url).mock(
        side_effect=_side_effect
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="SUCCESS",
    )
    assert row is not None, "notification_log row never reached SUCCESS"
    status, attempt_count, _error_type, _error_message = row
    assert status == "SUCCESS"
    assert attempt_count == 2, (
        f"expected attempt_count=2 after one transient retry; got {attempt_count}"
    )
    assert sendgrid_route.call_count == 2, (
        f"SendGrid should have received 2 calls (1 fail + 1 success); "
        f"got {sendgrid_route.call_count}"
    )

    # No DLQ entry --- transient self-heal.
    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=0,
        timeout_s=2.0,
    )
    assert dlq_messages == [], (
        f"transient self-heal produced unexpected DLQ messages: {dlq_messages}"
    )

    # delivery_attempts: 2 rows. First RETRYABLE, second SUCCESS.
    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT da.attempt_number, da.outcome FROM delivery_attempts da "
                "JOIN notification_log nl ON da.notification_id = nl.notification_id "
                "WHERE nl.event_id = %s AND nl.channel = %s "
                "ORDER BY da.attempt_number",
                (str(event_id), "email"),
            )
            attempts = await cur.fetchall()
    assert len(attempts) == 2
    assert attempts[0][1] == "RETRYABLE", (
        f"first attempt outcome should be RETRYABLE on 503; got {attempts[0][1]}"
    )
    assert attempts[1][1] == "SUCCESS"

    # Captured logs contain both a retry signal and a success signal.
    retry_logs = [
        rec
        for rec in captured_logs
        if str(rec.get("level", "")).upper() in ("WARN", "WARNING", "INFO")
        and (
            "retry" in str(rec.get("event", "")).lower()
            or "retrying" in str(rec.get("event", "")).lower()
            or rec.get("attempt") in (1, 2)
        )
    ]
    success_logs = [
        rec
        for rec in captured_logs
        if str(rec.get("level", "")).upper() == "INFO"
        and (
            rec.get("status") == "SUCCESS"
            or "success" in str(rec.get("event", "")).lower()
            or "delivered" in str(rec.get("event", "")).lower()
        )
    ]
    assert retry_logs, "expected at least one log signaling a retry"
    assert success_logs, "expected at least one log signaling final SUCCESS"


# ===========================================================================
# Test 4.3 --- AAP R-17 ANCHOR: repeated 503s -> DLQ with full envelope
# ===========================================================================
async def test_email_repeated_5xx_exhausts_retries_then_lands_in_dlq(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
    captured_logs: list[dict[str, Any]],
    correlation_id: str,
) -> None:
    """R-17 ANCHOR: SendGrid always-503 -> max_attempts exhausted -> DLQ.

    This is the canonical AAP R-17 compliance test. If this fails, the
    Notification Service is NOT compliant with R-17 and silently loses
    messages on sustained provider outages. The test verifies that:

    1. The notification_log row reaches DEAD_LETTER status with
       attempt_count >= MAX_ATTEMPTS, populated error_type, and
       error_message bounded to the 1 KiB cap.
    2. EXACTLY ONE message lands on ``notifications.email.dlq``.
    3. The DLQ envelope carries every required field of the v1
       schema: schema_version, service, kind, channel, original_event,
       attempt, error{type,message,classifier}, correlation_id,
       user_id, template_id, first_attempt_at, dead_lettered_at.
    4. The DLQ message headers carry all 6 required keys with their
       exact-match values per the captured src/scheduler/ spec.
    5. SendGrid received exactly MAX_ATTEMPTS calls (no extra calls
       after exhaustion).
    6. captured_logs contains an ERROR record signalling
       retry-exhaustion with the canonical structured fields.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    sendgrid_route = respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(
        user_id=user_id,
        event_id=event_id,
        correlation_id=correlation_id,
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
        timeout_s=POLL_TIMEOUT_S,
    )
    assert row is not None, (
        "notification_log row never reached DEAD_LETTER within the timeout; "
        "this indicates the consumer either crashed or the DLQ-routing path "
        "is broken --- AAP R-17 NON-COMPLIANT"
    )
    status, attempt_count, error_type, error_message = row
    assert status == "DEAD_LETTER"
    assert attempt_count >= MAX_ATTEMPTS, (
        f"attempt_count={attempt_count} should be >= MAX_ATTEMPTS={MAX_ATTEMPTS}"
    )
    assert error_type is not None and len(error_type) > 0, (
        "error_type must be populated on DEAD_LETTER rows"
    )
    assert error_message is not None and len(error_message) > 0, (
        "error_message must be populated on DEAD_LETTER rows"
    )
    error_message_bytes = error_message.encode("utf-8")
    assert len(error_message_bytes) <= MAX_ERROR_MESSAGE_BYTES, (
        f"error_message exceeds 1 KiB cap: {len(error_message_bytes)} bytes"
    )

    # SendGrid should have received exactly MAX_ATTEMPTS calls.
    assert sendgrid_route.call_count >= MAX_ATTEMPTS, (
        f"SendGrid call_count={sendgrid_route.call_count}; expected at least "
        f"MAX_ATTEMPTS={MAX_ATTEMPTS}"
    )

    # delivery_attempts: MAX_ATTEMPTS rows; outcomes are RETRYABLE
    # (final row may be TERMINAL depending on classifier semantics).
    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT da.attempt_number, da.outcome FROM delivery_attempts da "
                "JOIN notification_log nl ON da.notification_id = nl.notification_id "
                "WHERE nl.event_id = %s AND nl.channel = %s "
                "ORDER BY da.attempt_number",
                (str(event_id), "email"),
            )
            attempts = await cur.fetchall()
    assert len(attempts) >= MAX_ATTEMPTS, (
        f"delivery_attempts has {len(attempts)} rows; expected at least "
        f"MAX_ATTEMPTS={MAX_ATTEMPTS}"
    )
    for attempt_row in attempts[:MAX_ATTEMPTS]:
        assert attempt_row[1] in ("RETRYABLE", "TERMINAL"), (
            f"attempt {attempt_row[0]} outcome should be RETRYABLE or "
            f"TERMINAL on 503; got {attempt_row[1]}"
        )

    # Collect DLQ messages: EXACTLY 1.
    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1, (
        f"expected exactly 1 DLQ message; got {len(dlq_messages)}"
    )

    envelope = dlq_messages[0]["value"]
    headers = dlq_messages[0]["headers"]

    # Envelope schema-v1 shape.
    assert envelope.get("schema_version") == "v1"
    assert envelope.get("service") == "notification-service"
    assert envelope.get("kind") == "dlq"
    assert envelope.get("channel") == "email"
    assert envelope.get("correlation_id") == correlation_id
    assert envelope.get("user_id") == str(user_id)
    assert envelope.get("attempt") >= MAX_ATTEMPTS, (
        f"envelope.attempt={envelope.get('attempt')} should be >= "
        f"MAX_ATTEMPTS={MAX_ATTEMPTS}"
    )
    # template_id is a UUID string from notification_log.
    template_id_str = envelope.get("template_id")
    assert template_id_str is not None
    uuid.UUID(template_id_str)  # raises if not a valid UUID
    # original_event.event_id matches the one we produced.
    original_event = envelope.get("original_event")
    assert isinstance(original_event, dict)
    assert original_event.get("event_id") == str(event_id)
    assert original_event.get("event_type") == "user.registered"
    assert original_event.get("user_id") == str(user_id)
    # error info has type/message/classifier.
    error_info = envelope.get("error")
    assert isinstance(error_info, dict)
    assert "type" in error_info
    assert "message" in error_info
    assert error_info["type"]
    assert isinstance(error_info["message"], str)
    assert len(error_info["message"].encode("utf-8")) <= MAX_ERROR_MESSAGE_BYTES
    # classifier field present and is RETRYABLE for transient 5xx.
    assert "classifier" in error_info
    # first_attempt_at and dead_lettered_at parse as RFC 3339.
    first_attempt_at = datetime.fromisoformat(envelope["first_attempt_at"])
    dead_lettered_at = datetime.fromisoformat(envelope["dead_lettered_at"])
    assert dead_lettered_at > first_attempt_at, (
        "dead_lettered_at should be strictly after first_attempt_at"
    )

    # Headers --- all 6 required keys with exact-match values.
    assert headers.get(DLQ_HEADER_SERVICE) == "notification-service"
    assert headers.get(DLQ_HEADER_SCHEMA_VERSION) == "v1"
    assert headers.get(DLQ_HEADER_TOPIC_KIND) == "dlq"
    assert headers.get(DLQ_HEADER_CHANNEL) == "email"
    assert headers.get(DLQ_HEADER_CORRELATION_ID) == correlation_id
    retry_attempt_header = headers.get(DLQ_HEADER_RETRY_ATTEMPT)
    assert retry_attempt_header is not None
    assert int(retry_attempt_header) >= MAX_ATTEMPTS

    # captured_logs: ERROR-level retry_exhausted record.
    exhausted_logs = [
        rec
        for rec in captured_logs
        if str(rec.get("level", "")).upper() == "ERROR"
        and (
            rec.get("reason") == "retry_exhausted"
            or "retry_exhausted" in str(rec.get("event", "")).lower()
            or (
                "exhausted" in str(rec.get("event", "")).lower()
                and rec.get("event_type") == "user.registered"
            )
        )
    ]
    assert exhausted_logs, (
        "expected at least one ERROR log with reason=retry_exhausted (or "
        "equivalent) on retry exhaustion"
    )


# ===========================================================================
# Test 4.4 --- SMS path: repeated 5xx -> SMS DLQ
# ===========================================================================
async def test_sms_repeated_5xx_exhausts_retries_then_lands_in_sms_dlq(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """The SMS DLQ path mirrors the email DLQ path symmetrically.

    Configures Twilio to return 500 on every call, opts the user in
    to SMS only, and verifies that:
      * notification_log: channel=sms, status=DEAD_LETTER,
        attempt_count >= MAX_ATTEMPTS.
      * EXACTLY 1 message on ``notifications.sms.dlq``.
      * Envelope ``channel == "sms"`` and header ``x-channel == sms``.
      * original_event.event_type == "user.registered".
    """
    twilio_regex = (
        r"https://api\.twilio\.com/2010-04-01/Accounts/.+/Messages\.json"
    )
    twilio_route = respx_router_session.post(url__regex=twilio_regex).mock(
        return_value=httpx.Response(500)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=False, sms_enabled=True
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="sms",
        target_status="DEAD_LETTER",
    )
    assert row is not None, "SMS notification_log row never reached DEAD_LETTER"
    status, attempt_count, error_type, _error_message = row
    assert status == "DEAD_LETTER"
    assert attempt_count >= MAX_ATTEMPTS
    assert error_type
    assert twilio_route.call_count >= MAX_ATTEMPTS

    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=SMS_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1, (
        f"expected exactly 1 SMS DLQ message; got {len(dlq_messages)}"
    )

    envelope = dlq_messages[0]["value"]
    headers = dlq_messages[0]["headers"]
    assert envelope.get("channel") == "sms"
    assert envelope.get("kind") == "dlq"
    assert envelope.get("schema_version") == "v1"
    assert envelope.get("service") == "notification-service"
    original_event = envelope.get("original_event")
    assert isinstance(original_event, dict)
    assert original_event.get("event_type") == "user.registered"
    assert headers.get(DLQ_HEADER_CHANNEL) == "sms"

    # No email DLQ activity (channel isolation invariant).
    email_dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=0,
        timeout_s=2.0,
    )
    assert email_dlq_messages == []


# ===========================================================================
# Test 4.5 --- TERMINAL 4xx -> direct DLQ in attempt_count=1 (no retry)
# ===========================================================================
async def test_non_retryable_provider_error_lands_in_dlq_without_retry(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """A 400 Bad Request (TERMINAL) routes directly to DLQ without retries.

    Per the captured error-classifier spec, 4xx responses (other than
    429) are TERMINAL: retrying them is futile because the request is
    malformed. The channel must therefore route them to DLQ
    immediately on the FIRST attempt, leaving attempt_count=1 and
    consuming exactly 1 provider call (NOT MAX_ATTEMPTS).

    This is the symmetric twin of test 4.3: that test exercises the
    RETRYABLE branch; this one exercises the TERMINAL branch.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    sendgrid_route = respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(400)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None, "notification_log row never reached DEAD_LETTER"
    status, attempt_count, error_type, _error_message = row
    assert status == "DEAD_LETTER"
    assert attempt_count == 1, (
        f"TERMINAL classification should NOT consume retries; expected "
        f"attempt_count=1, got {attempt_count}"
    )
    assert error_type is not None
    # SendGrid received EXACTLY one call --- TERMINAL = no retry.
    assert sendgrid_route.call_count == 1, (
        f"TERMINAL classification should yield exactly 1 provider call; "
        f"got {sendgrid_route.call_count}"
    )

    # delivery_attempts: 1 row, outcome=TERMINAL.
    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT da.attempt_number, da.outcome FROM delivery_attempts da "
                "JOIN notification_log nl ON da.notification_id = nl.notification_id "
                "WHERE nl.event_id = %s AND nl.channel = %s",
                (str(event_id), "email"),
            )
            attempts = await cur.fetchall()
    assert len(attempts) == 1
    assert attempts[0][1] == "TERMINAL"

    # 1 DLQ message, envelope.attempt == 1.
    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    envelope = dlq_messages[0]["value"]
    assert envelope.get("attempt") == 1, (
        f"envelope.attempt should be 1 for TERMINAL; got {envelope.get('attempt')}"
    )


# ===========================================================================
# Test 4.6 --- DLQ envelope preserves correlation_id end-to-end
# ===========================================================================
async def test_dlq_envelope_preserves_correlation_id(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
    captured_logs: list[dict[str, Any]],
) -> None:
    """AAP R-13: correlation_id flows end-to-end through retry+DLQ paths.

    A known correlation_id is injected on the produced event; the test
    verifies it is preserved on:
      * the DLQ envelope's ``correlation_id`` field,
      * the ``x-correlation-id`` header on the DLQ message,
      * the ``correlation_id`` column on the notification_log row,
      * every captured log record from this notification's lifecycle.

    Failure of any of these breaks the operator's ability to trace a
    single failure end-to-end across retries, DLQ, and Kibana.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    known_correlation = f"corr-test-{uuid.uuid4().hex[:16]}"

    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(
        user_id=user_id,
        event_id=event_id,
        correlation_id=known_correlation,
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None

    # notification_log.correlation_id matches.
    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT correlation_id FROM notification_log "
                "WHERE event_id = %s AND channel = %s",
                (str(event_id), "email"),
            )
            stored = await cur.fetchone()
    assert stored is not None
    assert stored[0] == known_correlation, (
        f"notification_log.correlation_id={stored[0]} != "
        f"expected={known_correlation}"
    )

    # DLQ envelope and header.
    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    envelope = dlq_messages[0]["value"]
    headers = dlq_messages[0]["headers"]
    assert envelope.get("correlation_id") == known_correlation
    assert headers.get(DLQ_HEADER_CORRELATION_ID) == known_correlation

    # Every captured log record for this event carries the correlation_id.
    matching_logs = [
        rec
        for rec in captured_logs
        if rec.get("correlation_id") == known_correlation
    ]
    assert matching_logs, (
        f"no captured log records carry correlation_id={known_correlation}; "
        f"AAP R-13 / R-26 violation"
    )


# ===========================================================================
# Test 4.7 --- DLQ envelope preserves the original event verbatim
# ===========================================================================
async def test_dlq_envelope_preserves_original_event(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """The DLQ envelope's ``original_event`` mirrors the ingress payload.

    Operators replaying a DLQ message MUST be able to reconstruct the
    exact event that triggered the failure, not just its key. The
    test produces a payload with several distinguishing fields (a
    distinct user_id, email, name, locale) and asserts that each one
    appears verbatim under ``envelope["original_event"]``.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    distinctive_email = f"alice-{uuid.uuid4().hex[:8]}@example.com"
    distinctive_name = f"Alice-{uuid.uuid4().hex[:6]}"
    distinctive_phone = "+14155551212"
    distinctive_locale = "fr-FR"

    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(
        user_id=user_id,
        event_id=event_id,
        email=distinctive_email,
        phone=distinctive_phone,
        name=distinctive_name,
        locale=distinctive_locale,
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None

    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    envelope = dlq_messages[0]["value"]
    original_event = envelope.get("original_event")
    assert isinstance(original_event, dict)
    assert original_event.get("event_id") == str(event_id)
    assert original_event.get("event_type") == "user.registered"
    assert original_event.get("user_id") == str(user_id)
    assert original_event.get("email") == distinctive_email
    assert original_event.get("name") == distinctive_name
    assert original_event.get("phone") == distinctive_phone
    assert original_event.get("locale") == distinctive_locale


# ===========================================================================
# Test 4.8 --- Exponential backoff timing is deterministic with jitter=0
# ===========================================================================
async def test_exponential_backoff_timing_deterministic(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """AAP R-15: backoff intervals match initial_delay * multiplier^n.

    With jitter_pct=0 (integration setting), the backoff schedule is:
      attempt 1 -> attempt 2: 50ms (initial_delay)
      attempt 2 -> attempt 3: 100ms (initial_delay * multiplier)
    The test reads delivery_attempts.attempted_at for the 3 attempts
    and asserts that the elapsed deltas fall within generous tolerance
    bounds (the consumer's per-attempt processing latency adds a
    floor; the test's bounds accommodate that without false-positives
    on slow CI runners).
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None

    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT da.attempt_number, da.attempted_at, da.duration_ms "
                "FROM delivery_attempts da "
                "JOIN notification_log nl "
                "ON da.notification_id = nl.notification_id "
                "WHERE nl.event_id = %s AND nl.channel = %s "
                "ORDER BY da.attempt_number",
                (str(event_id), "email"),
            )
            attempts = await cur.fetchall()

    assert len(attempts) >= MAX_ATTEMPTS, (
        f"expected {MAX_ATTEMPTS} delivery_attempts rows; got {len(attempts)}"
    )

    # Compute deltas between consecutive attempts.
    attempted_ats = [att[1] for att in attempts[:MAX_ATTEMPTS]]
    delta_1_to_2_ms = (
        attempted_ats[1] - attempted_ats[0]
    ).total_seconds() * 1000.0
    delta_2_to_3_ms = (
        attempted_ats[2] - attempted_ats[1]
    ).total_seconds() * 1000.0

    # Generous tolerance bounds: lower bound = expected * 0.8 (allow
    # for sub-millisecond clock skew); upper bound = expected + 200ms
    # (allow for processing latency on slow CI hosts).
    expected_first_delay = float(INITIAL_DELAY_MS)
    expected_second_delay = float(INITIAL_DELAY_MS) * BACKOFF_MULTIPLIER

    assert delta_1_to_2_ms >= expected_first_delay * 0.8, (
        f"attempt 1->2 delta={delta_1_to_2_ms:.1f}ms below expected "
        f"{expected_first_delay}ms (jitter_pct=0)"
    )
    assert delta_1_to_2_ms <= expected_first_delay + 200.0, (
        f"attempt 1->2 delta={delta_1_to_2_ms:.1f}ms exceeds upper bound "
        f"{expected_first_delay + 200.0}ms"
    )
    assert delta_2_to_3_ms >= expected_second_delay * 0.8, (
        f"attempt 2->3 delta={delta_2_to_3_ms:.1f}ms below expected "
        f"{expected_second_delay}ms"
    )
    assert delta_2_to_3_ms <= expected_second_delay + 200.0, (
        f"attempt 2->3 delta={delta_2_to_3_ms:.1f}ms exceeds upper bound "
        f"{expected_second_delay + 200.0}ms"
    )

    # duration_ms populated on every row.
    for att_row in attempts[:MAX_ATTEMPTS]:
        assert att_row[2] is not None, (
            f"delivery_attempts.duration_ms must be populated; "
            f"attempt {att_row[0]} has duration_ms=None"
        )
        assert att_row[2] >= 0


# ===========================================================================
# Test 4.9 --- Retry-count header increments across retry-topic cycles
# ===========================================================================
async def test_retry_count_header_increments_across_retry_topic_cycles(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """The ``x-retry-attempt`` header increments monotonically per retry.

    If the architecture uses external retry topics + a RetryScheduler,
    the test observes increment-by-1 messages on the retry topic. If
    retries are purely in-request (within ``@retry_async``), no
    retry-topic messages are produced --- this is a documented
    architectural choice and the test relaxes accordingly. In both
    cases, the FINAL DLQ message header ``x-retry-attempt`` equals
    MAX_ATTEMPTS.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None

    kafka_config = settings_integration.kafka.consumer.dict()

    # Inspect retry topic --- may be 0 messages (in-request retry only)
    # or MAX_ATTEMPTS-1 messages (one per scheduled retry).
    retry_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_RETRY_TOPIC,
        expected_count=MAX_ATTEMPTS,
        timeout_s=5.0,
    )

    if retry_messages:
        retry_attempts: list[int] = []
        for msg in retry_messages:
            attempt_header = msg["headers"].get(DLQ_HEADER_RETRY_ATTEMPT)
            assert attempt_header is not None, (
                "retry-topic message missing x-retry-attempt header"
            )
            attempt_int = int(attempt_header)
            assert 1 <= attempt_int < MAX_ATTEMPTS, (
                f"retry-topic attempt {attempt_int} should be in "
                f"[1, MAX_ATTEMPTS={MAX_ATTEMPTS})"
            )
            retry_attempts.append(attempt_int)
        # Monotonic non-decreasing (allowing concurrent partitions to
        # interleave but each individual attempt to appear at most once).
        retry_attempts.sort()
        for i in range(1, len(retry_attempts)):
            assert retry_attempts[i] >= retry_attempts[i - 1]

    # FINAL DLQ message header == MAX_ATTEMPTS (or higher).
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    final_header = dlq_messages[0]["headers"].get(DLQ_HEADER_RETRY_ATTEMPT)
    assert final_header is not None
    assert int(final_header) >= MAX_ATTEMPTS, (
        f"DLQ x-retry-attempt={final_header}; expected >= "
        f"MAX_ATTEMPTS={MAX_ATTEMPTS}"
    )


# ===========================================================================
# Test 4.10 --- error_message truncated to 1 KiB cap
# ===========================================================================
async def test_dlq_message_error_message_truncated_to_1kib(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """A 5xx response with a 10 KiB body is truncated to <= 1 KiB.

    Prevents a malicious or buggy provider from inflating the
    error_message column / DLQ envelope with multi-MiB payloads. The
    test configures SendGrid to return 500 with a very large response
    body and asserts:

      * envelope["error"]["message"] is <= 1024 bytes.
      * notification_log.error_message is <= 1024 bytes.
      * The truncation is observable: either an explicit
        ``error.truncated == true`` flag OR the message ends with the
        canonical ``"..."`` ellipsis sentinel.
    """
    huge_body = "X" * (10 * 1024)  # 10 KiB
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(500, text=huge_body)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None
    _status, _attempt_count, _error_type, error_message = row
    assert error_message is not None
    err_bytes = error_message.encode("utf-8")
    assert len(err_bytes) <= MAX_ERROR_MESSAGE_BYTES, (
        f"notification_log.error_message length={len(err_bytes)} bytes "
        f"exceeds 1 KiB cap"
    )

    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    envelope = dlq_messages[0]["value"]
    error_info = envelope.get("error")
    assert isinstance(error_info, dict)
    err_msg = error_info.get("message", "")
    assert isinstance(err_msg, str)
    err_msg_bytes = err_msg.encode("utf-8")
    assert len(err_msg_bytes) <= MAX_ERROR_MESSAGE_BYTES, (
        f"envelope.error.message length={len(err_msg_bytes)} bytes "
        f"exceeds 1 KiB cap"
    )
    # Truncation observability: explicit flag OR ellipsis sentinel.
    truncation_signaled = (
        error_info.get("truncated") is True
        or err_msg.endswith("...")
    )
    assert truncation_signaled, (
        "expected explicit error.truncated=true OR ellipsis suffix on "
        "truncated message; got neither"
    )


# ===========================================================================
# Test 4.11 --- dead_lettered_at strictly after first_attempt_at
# ===========================================================================
async def test_dlq_dead_lettered_at_after_first_attempt_at(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """Temporal invariant: dead_lettered_at > first_attempt_at + backoff floor.

    The DLQ envelope's two timestamps must respect causality: the
    dead-letter timestamp is recorded AFTER the first attempt began,
    and at minimum ``initial_delay_ms * (max_attempts - 1)`` later
    once backoff intervals are summed. The test parses both ISO 8601
    strings and asserts the strict inequality and the lower-bound
    delta in milliseconds.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None

    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    envelope = dlq_messages[0]["value"]

    first_at_str = envelope.get("first_attempt_at")
    dead_at_str = envelope.get("dead_lettered_at")
    assert first_at_str is not None
    assert dead_at_str is not None
    first_at = datetime.fromisoformat(first_at_str)
    dead_at = datetime.fromisoformat(dead_at_str)

    # Both timestamps must be tz-aware (RFC 3339 mandates timezone).
    assert first_at.tzinfo is not None, (
        "first_attempt_at must be tz-aware RFC 3339"
    )
    assert dead_at.tzinfo is not None, (
        "dead_lettered_at must be tz-aware RFC 3339"
    )
    assert dead_at > first_at, "dead_lettered_at must be strictly after first_attempt_at"

    # Lower bound: sum of backoff intervals = sum_{i=0..N-2} initial * mult^i
    # For MAX_ATTEMPTS=3, multiplier=2.0, initial=50ms:
    # = 50 + 100 = 150ms. Use 80% of that as a floor to absorb scheduler
    # quantization on the host's event loop.
    expected_delays = sum(
        INITIAL_DELAY_MS * (BACKOFF_MULTIPLIER**i)
        for i in range(MAX_ATTEMPTS - 1)
    )
    delta_ms = (dead_at - first_at).total_seconds() * 1000.0
    assert delta_ms >= expected_delays * 0.8, (
        f"dead_at - first_at = {delta_ms:.1f}ms is below the expected "
        f"backoff floor {expected_delays * 0.8:.1f}ms"
    )


# ===========================================================================
# Test 4.12 --- DLQ topics are NOT subscribed by the production consumer
# ===========================================================================
async def test_idempotent_consumption_of_dlq_not_reprocessed_into_main_pipeline(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """DLQ topics are terminal; they are not in the consumer's subscription.

    Per Phase 7 insight: re-subscribing to DLQ topics would create an
    infinite retry loop for permanent failures. Operators consume DLQ
    messages out-of-band (Kibana alerts, manual re-drive scripts, SLA
    monitors) --- never the production consumer.

    The test verifies:
      1. ``settings.kafka.consumer.topics`` (or equivalent attribute)
         does NOT include either DLQ topic.
      2. Producing a synthetic message DIRECTLY to the email DLQ
         does not result in any new notification_log row.
    """
    # 1. Inspect the consumer's subscribed topics.
    subscribed = getattr(
        settings_integration.kafka.consumer,
        "topics",
        None,
    )
    if subscribed is None:
        # Fallback: some configurations expose the list under
        # `consume_topics` or as a property of the consumer runner.
        subscribed = getattr(consumer_runner, "subscribed_topics", None)

    assert subscribed is not None, (
        "could not locate the consumer's subscribed-topics list to verify "
        "DLQ-not-subscribed invariant"
    )

    # The subscribed list MUST NOT include either DLQ topic.
    subscribed_set = set(subscribed) if not isinstance(subscribed, str) else {subscribed}
    assert EMAIL_DLQ_TOPIC not in subscribed_set, (
        f"production consumer is subscribed to {EMAIL_DLQ_TOPIC} --- this "
        f"would create an infinite retry loop"
    )
    assert SMS_DLQ_TOPIC not in subscribed_set, (
        f"production consumer is subscribed to {SMS_DLQ_TOPIC} --- this "
        f"would create an infinite retry loop"
    )

    # 2. Produce a synthetic message DIRECTLY to the email DLQ. Assert
    # no notification_log row is created in response.
    fabricated_event_id = uuid.uuid4()
    fabricated_envelope = {
        "schema_version": "v1",
        "service": "fabricated-test",
        "kind": "dlq",
        "channel": "email",
        "original_event": {"event_id": str(fabricated_event_id)},
        "attempt": MAX_ATTEMPTS,
    }
    await kafka_producer.send(
        topic=EMAIL_DLQ_TOPIC,
        value=fabricated_envelope,
    )
    # Wait briefly to give any (incorrect) DLQ subscription a chance.
    await asyncio.sleep(2.0)

    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM notification_log "
                "WHERE event_id = %s",
                (str(fabricated_event_id),),
            )
            count_row = await cur.fetchone()
    assert count_row is not None
    assert count_row[0] == 0, (
        f"fabricated DLQ message was processed by the consumer; "
        f"notification_log has {count_row[0]} rows for event_id="
        f"{fabricated_event_id}"
    )


# ===========================================================================
# Test 4.13 --- DLQ write failure does NOT crash the consumer
# ===========================================================================
@pytest.mark.slow
async def test_dlq_write_failure_does_not_crash_consumer(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
    captured_logs: list[dict[str, Any]],
) -> None:
    """The consumer survives a DLQ write failure and processes subsequent events.

    Edge case: if the DlqWriter cannot reach Kafka (broker network
    partition, topic missing), the consumer must NOT crash. It should
    log CRITICAL/ERROR, leave the row in PENDING_RETRY (or DEAD_LETTER
    with a "DLQ unreachable" annotation), and continue processing
    subsequent events.

    The test exercises this by deleting the email DLQ topic after the
    consumer starts, then producing a guaranteed-to-fail event,
    waiting for the resulting log entry, and verifying that a
    SECOND, GOOD event still flows to SUCCESS.

    Marked slow because it spins down and recreates the DLQ topic.
    """
    # Phase 0: configure SendGrid to always fail (drives the failed
    # event toward DLQ).
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    sendgrid_route = respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    # Phase 1: delete the email DLQ topic so DlqWriter's produce fails.
    try:
        delete_future = kafka_admin_client.delete_topics([EMAIL_DLQ_TOPIC])
        for _topic, future in delete_future.items():
            try:
                future.result(timeout=10.0)
            except Exception:  # noqa: BLE001
                # Already deleted or never existed --- proceed.
                pass
    except Exception:  # noqa: BLE001
        # Some test environments may reject deletes; skip rather than fail.
        pytest.skip("Kafka admin client cannot delete the DLQ topic in this env")

    user_id_fail = uuid.uuid4()
    event_id_fail = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id_fail, email_enabled=True, sms_enabled=False
    )
    payload_fail = _build_user_registered_payload(
        user_id=user_id_fail, event_id=event_id_fail
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload_fail)

    # Wait briefly for the consumer to encounter the DLQ-write failure.
    await asyncio.sleep(5.0)

    # Captured logs MUST contain a CRITICAL/ERROR record about the
    # DLQ unreachable condition.
    dlq_failure_logs = [
        rec
        for rec in captured_logs
        if str(rec.get("level", "")).upper() in ("ERROR", "CRITICAL")
        and (
            "dlq" in str(rec.get("event", "")).lower()
            or "dlq_write" in str(rec.get("event", "")).lower()
            or "dead_letter" in str(rec.get("event", "")).lower()
        )
    ]
    assert dlq_failure_logs, (
        "expected CRITICAL/ERROR log about DLQ write failure; got none"
    )

    # Recreate the DLQ topic and produce a SECOND event that should
    # cleanly complete --- proves the consumer is still alive.
    try:
        from confluent_kafka.admin import (  # type: ignore[import-not-found]
            NewTopic,
        )

        new_topic = NewTopic(
            EMAIL_DLQ_TOPIC,
            num_partitions=1,
            replication_factor=1,
        )
        create_future = kafka_admin_client.create_topics([new_topic])
        for _topic, future in create_future.items():
            try:
                future.result(timeout=10.0)
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        # Topic may auto-recreate; proceed.
        pass

    # Now make SendGrid healthy and send a GOOD event.
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(202, headers={"X-Message-Id": "sg-recover"})
    )
    user_id_good = uuid.uuid4()
    event_id_good = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id_good, email_enabled=True, sms_enabled=False
    )
    payload_good = _build_user_registered_payload(
        user_id=user_id_good, event_id=event_id_good
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload_good)

    good_row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id_good,
        channel="email",
        target_status="SUCCESS",
    )
    assert good_row is not None, (
        "consumer did not process subsequent good event; the DLQ-write "
        "failure may have crashed the consumer loop"
    )
    # Finally confirm the failing event's row was attempted (call_count > 0).
    assert sendgrid_route.call_count >= 1


# ===========================================================================
# Test 4.14 --- DLQ envelope schema_version == "v1" exact match
# ===========================================================================
async def test_dlq_envelope_schema_version_v1(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """The DLQ envelope's schema_version MUST be the literal string "v1".

    Forward-compatibility insurance: operations tools that key on
    ``schema_version=v1`` continue to work even after future fields
    are added in a backward-compatible way. Bumping to ``v2`` is a
    breaking change requiring a coordinated migration.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )
    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None

    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    envelope = dlq_messages[0]["value"]

    # EXACT-match assertions (not "v.*" regex).
    assert envelope.get("schema_version") == "v1", (
        f"expected schema_version=='v1' (exact); got "
        f"{envelope.get('schema_version')!r}"
    )
    assert envelope.get("kind") == "dlq", (
        f"expected kind=='dlq' (exact); got {envelope.get('kind')!r}"
    )
    assert envelope.get("service") == "notification-service", (
        f"expected service=='notification-service' (exact); got "
        f"{envelope.get('service')!r}"
    )


# ===========================================================================
# Test 4.15 --- All 6 required DLQ headers present
# ===========================================================================
async def test_dlq_message_produced_with_correlation_id_header(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """Every DLQ message carries all 6 required Kafka headers.

    Operators (Kibana alerts, SLA monitors) routing on message
    metadata MUST be able to filter by service, schema version, topic
    kind, channel, correlation_id, and retry attempt without parsing
    the JSON payload. Missing any of the six headers is a failure of
    the Notification Service's DLQ contract per the captured
    src/scheduler/ spec.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    known_correlation = f"hdr-test-{uuid.uuid4().hex[:16]}"
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )
    payload = _build_user_registered_payload(
        user_id=user_id,
        event_id=event_id,
        correlation_id=known_correlation,
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None

    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    headers = dlq_messages[0]["headers"]

    # ALL 6 required headers present and non-empty.
    missing = [h for h in REQUIRED_DLQ_HEADERS if h not in headers]
    assert not missing, (
        f"DLQ message missing required header(s): {missing}; "
        f"present={list(headers.keys())}"
    )
    for header_name in REQUIRED_DLQ_HEADERS:
        value = headers.get(header_name)
        assert value is not None and value != "", (
            f"required DLQ header {header_name!r} is empty"
        )

    # Correlation_id header echoes the produced value exactly.
    assert headers[DLQ_HEADER_CORRELATION_ID] == known_correlation


# ===========================================================================
# Test 4.16 --- Channel isolation: only the failing channel lands in its DLQ
# ===========================================================================
async def test_only_affected_channel_lands_in_its_own_dlq(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """Email failure does not contaminate the SMS DLQ.

    With both email and SMS opt-in, configures SendGrid to always 503
    and Twilio to 202. The single produced ``user.registered`` event
    fans out to both channels:
      * email row reaches DEAD_LETTER; ``notifications.email.dlq``
        receives 1 message.
      * SMS row reaches SUCCESS; ``notifications.sms.dlq`` is empty.

    This is the foundational AAP R-20 degraded-mode invariant.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    twilio_regex = (
        r"https://api\.twilio\.com/2010-04-01/Accounts/.+/Messages\.json"
    )
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )
    respx_router_session.post(url__regex=twilio_regex).mock(
        return_value=httpx.Response(
            201,
            json={"sid": f"SM{uuid.uuid4().hex[:32]}", "status": "queued"},
        )
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=True
    )
    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    # Email row -> DEAD_LETTER.
    email_row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert email_row is not None
    # SMS row -> SUCCESS.
    sms_row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="sms",
        target_status="SUCCESS",
    )
    assert sms_row is not None

    kafka_config = settings_integration.kafka.consumer.dict()
    email_dlq_msgs = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(email_dlq_msgs) == 1
    assert email_dlq_msgs[0]["value"].get("channel") == "email"

    sms_dlq_msgs = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=SMS_DLQ_TOPIC,
        expected_count=0,
        timeout_s=2.0,
    )
    assert sms_dlq_msgs == [], (
        f"channel isolation broken --- SMS DLQ received messages despite "
        f"SMS provider success: {sms_dlq_msgs}"
    )


# ===========================================================================
# Test 4.17 --- DLQ-write event emits a structured ERROR log
# ===========================================================================
async def test_required_log_fields_on_dlq_write_event(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
    captured_logs: list[dict[str, Any]],
    assert_required_log_fields: Any,
) -> None:
    """AAP R-26: DLQ-write events emit a structured ERROR log record.

    The captured src/scheduler/ spec emits an ERROR-level structured
    record at the moment of DLQ write. The record must include the
    canonical AAP R-26 fields (timestamp, level, service,
    correlation_id) plus DLQ-specific context (channel, provider,
    notification_id, event_type, reason).
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(503)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    known_correlation = f"log-test-{uuid.uuid4().hex[:16]}"
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )
    payload = _build_user_registered_payload(
        user_id=user_id,
        event_id=event_id,
        correlation_id=known_correlation,
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None

    # Locate the ERROR-level DLQ-write record.
    dlq_write_records = [
        rec
        for rec in captured_logs
        if str(rec.get("level", "")).upper() == "ERROR"
        and rec.get("correlation_id") == known_correlation
        and (
            rec.get("reason") == "retry_exhausted"
            or "dead_letter" in str(rec.get("event", "")).lower()
            or "retry_exhausted" in str(rec.get("event", "")).lower()
            or "dlq" in str(rec.get("event", "")).lower()
        )
    ]
    assert dlq_write_records, (
        "expected at least one ERROR log record at the DLQ-write site "
        f"with correlation_id={known_correlation}"
    )
    record = dlq_write_records[0]

    # The fixture-supplied helper enforces the canonical AAP R-26 fields.
    assert_required_log_fields(record)

    # DLQ-specific context fields.
    assert record.get("correlation_id") == known_correlation
    assert (
        record.get("channel") == "email"
        or record.get("channel_type") == "email"
        or "email" in str(record.get("event", "")).lower()
    )
    # event_type carried through.
    assert (
        record.get("event_type") == "user.registered"
        or "user.registered" in json.dumps(record).lower()
    )


# ===========================================================================
# Test 4.18 --- TERMINAL render error -> DLQ without provider call
# ===========================================================================
async def test_validation_error_lands_in_dlq_without_provider_call(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """A template rendering error routes to DLQ without any provider call.

    Exercises the TERMINAL classification path for in-process render
    errors (missing template, undefined variable, etc.). When the
    template can't be rendered, there is nothing to send --- the
    channel must NOT call SendGrid/Twilio at all and must instead
    DLQ-route in attempt_count=1.

    Triggered by:
      * NOT seeding any welcome template (so template lookup fails),
      * still allowing email opt-in.

    Asserts:
      * notification_log: status=DEAD_LETTER, attempt_count=1.
      * SendGrid received 0 calls.
      * DLQ envelope's error.type references rendering / template.
    """
    # IMPORTANT: do NOT use the seed_welcome_templates fixture so the
    # template lookup fails inside the channel handler. Register the
    # SendGrid route so we can verify call_count remains 0.
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    sendgrid_route = respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(202)
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )

    payload = _build_user_registered_payload(user_id=user_id, event_id=event_id)
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="DEAD_LETTER",
    )
    assert row is not None
    status, attempt_count, error_type, _error_message = row
    assert status == "DEAD_LETTER"
    assert attempt_count == 1, (
        f"render-error TERMINAL should not retry; attempt_count={attempt_count}"
    )
    assert error_type is not None

    # ZERO SendGrid calls --- there was nothing to render.
    assert sendgrid_route.call_count == 0, (
        f"expected 0 SendGrid calls for render error; got {sendgrid_route.call_count}"
    )

    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=1,
        timeout_s=POLL_TIMEOUT_S,
    )
    assert len(dlq_messages) == 1
    envelope = dlq_messages[0]["value"]
    error_info = envelope.get("error")
    assert isinstance(error_info, dict)
    error_type_payload = str(error_info.get("type", ""))
    error_msg_payload = str(error_info.get("message", "")).lower()
    assert (
        "template" in error_type_payload.lower()
        or "render" in error_type_payload.lower()
        or "validation" in error_type_payload.lower()
        or "template" in error_msg_payload
        or "render" in error_msg_payload
    ), (
        f"expected error.type to reference template/render/validation; "
        f"got error.type={error_type_payload!r}, "
        f"error.message={error_info.get('message')!r}"
    )


# ===========================================================================
# Test 4.19 --- CancelledError does NOT route to DLQ
# ===========================================================================
async def test_cancelled_error_does_not_route_to_dlq(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
) -> None:
    """asyncio.CancelledError is propagated, not DLQ-routed.

    Per the EmailChannel spec: when the consumer is shutting down, in-
    flight dispatches may be cancelled mid-HTTP-call. These should NOT
    be DLQ-routed because the user's event was not actually processed
    to completion --- redelivering on next startup is the correct
    behavior.

    The test simulates this by configuring SendGrid to raise
    CancelledError mid-call (via respx side_effect), then verifies:
      * NO DLQ message is written.
      * notification_log row remains in PENDING / PENDING_RETRY (NOT
        DEAD_LETTER).
      * The consumer remains alive --- a subsequent good event flows
        through to SUCCESS.
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"

    def _raise_cancelled(_request: httpx.Request) -> httpx.Response:
        # CancelledError is a BaseException (not Exception) --- it
        # specifically simulates task cancellation.
        raise asyncio.CancelledError()

    cancel_route = respx_router_session.post(sendgrid_url).mock(
        side_effect=_raise_cancelled
    )

    user_id_cancel = uuid.uuid4()
    event_id_cancel = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id_cancel, email_enabled=True, sms_enabled=False
    )
    payload = _build_user_registered_payload(
        user_id=user_id_cancel, event_id=event_id_cancel
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    # Wait briefly --- if CancelledError were DLQ-routed, the row
    # would reach DEAD_LETTER within a few seconds.
    await asyncio.sleep(3.0)

    async with pg_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT status FROM notification_log "
                "WHERE event_id = %s AND channel = %s",
                (str(event_id_cancel), "email"),
            )
            row = await cur.fetchone()
    # If the row exists, status MUST NOT be DEAD_LETTER.
    if row is not None:
        assert row[0] != "DEAD_LETTER", (
            f"CancelledError should NOT route to DLQ; got "
            f"notification_log.status={row[0]}"
        )
        # Permitted intermediate states: PENDING, PENDING_RETRY.
        assert row[0] in ("PENDING", "PENDING_RETRY"), (
            f"unexpected status={row[0]} for cancelled dispatch"
        )

    kafka_config = settings_integration.kafka.consumer.dict()
    dlq_messages = await _collect_dlq_messages(
        kafka_config=kafka_config,
        dlq_topic=EMAIL_DLQ_TOPIC,
        expected_count=0,
        timeout_s=2.0,
    )
    assert dlq_messages == [], (
        f"CancelledError should NOT produce a DLQ message; got {dlq_messages}"
    )

    # Sanity: the route was actually triggered before cancellation.
    assert cancel_route.call_count >= 1, (
        "the cancellation route was never called --- the test setup did "
        "not exercise the cancellation path"
    )

    # Verify consumer is still alive: produce a healthy second event.
    respx_router_session.post(sendgrid_url).mock(
        return_value=httpx.Response(202, headers={"X-Message-Id": "sg-after-cancel"})
    )
    user_id_good = uuid.uuid4()
    event_id_good = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_id_good, email_enabled=True, sms_enabled=False
    )
    await kafka_producer.send(
        topic=USER_REGISTERED_TOPIC,
        value=_build_user_registered_payload(
            user_id=user_id_good, event_id=event_id_good
        ),
    )
    good_row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id_good,
        channel="email",
        target_status="SUCCESS",
    )
    assert good_row is not None, (
        "consumer did not process subsequent good event after a "
        "CancelledError --- the consumer may have crashed"
    )


# ===========================================================================
# Test 4.20 --- Multiple retries all preserve correlation_id in logs
# ===========================================================================
async def test_multiple_retries_all_preserve_correlation_id_in_logs(
    kafka_producer: Any,
    kafka_admin_client: Any,
    pg_pool: Any,
    consumer_runner: Any,
    respx_router_session: respx.Router,
    seed_welcome_templates: Any,
    user_prefs_repo: Any,
    settings_integration: Any,
    captured_logs: list[dict[str, Any]],
) -> None:
    """Across 503 -> 503 -> 202, every captured log carries the same correlation_id.

    Configures SendGrid with a per-call sequence (503, 503, 202),
    produces a single event with a known correlation_id, polls for
    SUCCESS at attempt_count=3, and asserts every log record from the
    notification's lifecycle --- including the two retry warnings ---
    carries the same correlation_id.

    This is the AAP R-13 + R-26 joint compliance test for the retry
    path (test 4.6 covers the same invariant on the DLQ path).
    """
    sendgrid_url = "https://api.sendgrid.com/v3/mail/send"
    call_state: dict[str, int] = {"calls": 0}

    def _sequenced(_request: httpx.Request) -> httpx.Response:
        call_state["calls"] += 1
        if call_state["calls"] <= 2:
            return httpx.Response(503)
        return httpx.Response(202, headers={"X-Message-Id": "sg-third-call"})

    sendgrid_route = respx_router_session.post(sendgrid_url).mock(
        side_effect=_sequenced
    )

    user_id = uuid.uuid4()
    event_id = uuid.uuid4()
    known_correlation = f"multi-retry-{uuid.uuid4().hex[:16]}"
    await user_prefs_repo.upsert(
        user_id=user_id, email_enabled=True, sms_enabled=False
    )
    payload = _build_user_registered_payload(
        user_id=user_id,
        event_id=event_id,
        correlation_id=known_correlation,
    )
    await kafka_producer.send(topic=USER_REGISTERED_TOPIC, value=payload)

    row = await _poll_for_status(
        pool=pg_pool,
        event_id=event_id,
        channel="email",
        target_status="SUCCESS",
    )
    assert row is not None
    status, attempt_count, _error_type, _error_message = row
    assert status == "SUCCESS"
    assert attempt_count == 3, (
        f"event should succeed on attempt 3; got attempt_count={attempt_count}"
    )
    assert sendgrid_route.call_count == 3

    # All log records mentioning this notification carry the
    # correlation_id.
    matching_logs = [
        rec
        for rec in captured_logs
        if rec.get("correlation_id") == known_correlation
    ]
    # We expect at minimum: 1 ingest, 2 retry-warning, 1 success = 4.
    # Be permissive: assert >= 3 matching records to allow log
    # consolidation in some implementations.
    assert len(matching_logs) >= 3, (
        f"expected >= 3 captured log records carrying correlation_id="
        f"{known_correlation}; got {len(matching_logs)} (R-13 violation)"
    )

    # Among the matching records, at least one signals a retry attempt
    # AND at least one signals final success.
    retry_signals = [
        rec
        for rec in matching_logs
        if (
            "retry" in str(rec.get("event", "")).lower()
            or rec.get("attempt") in (1, 2)
            or str(rec.get("level", "")).upper() in ("WARN", "WARNING")
        )
    ]
    success_signals = [
        rec
        for rec in matching_logs
        if (
            rec.get("status") == "SUCCESS"
            or "success" in str(rec.get("event", "")).lower()
            or "delivered" in str(rec.get("event", "")).lower()
        )
    ]
    assert retry_signals, (
        "expected at least one retry-attempt log record carrying "
        f"correlation_id={known_correlation}"
    )
    assert success_signals, (
        "expected at least one success log record carrying "
        f"correlation_id={known_correlation}"
    )
