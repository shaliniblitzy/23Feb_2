"""Dead-letter queue + retry topic writer for the Inventory Service.

This module defines :class:`DlqWriter` -- the thin async wrapper over
:class:`confluent_kafka.Producer` that routes failed Kafka messages to one of
three target topologies, depending on the failure mode and origin of the
poison message.

Three target topologies
-----------------------
Per-source-topic retry topic (``<source>.retry``)
    Used by :class:`KafkaConsumerRunner` when a *retryable* failure is raised
    for a message and the next attempt count is still within the configured
    retry budget. The original payload is wrapped in the standard JSON
    envelope, the ``attempt-count`` header is incremented, and the consumer
    will re-poll the message from the retry topic with backoff. Topic name
    is derived as ``f"{source_topic}{retry_topic_suffix}"`` (e.g.
    ``order.created.retry``).

Per-source-topic DLQ (``<source>.dlq``)
    Used by :class:`KafkaConsumerRunner` when a message has either exhausted
    its retry budget on the retry topic, or raised a *non-retryable*
    exception (e.g. deserialization failure / poison message, terminal
    business error). Topic name is derived as
    ``f"{source_topic}{dlq_topic_suffix}"`` (e.g. ``order.created.dlq``).

Service-wide DLQ (``inventory.dlq``)
    Used for service-internal poison events that do **not** have a single
    inbound source topic -- for example, outbound event publish exhaustion
    via :class:`EventPublishError`, reservation expiry scheduler tick
    failures, or warehouse adapter persistent failure. Callers supply a
    free-form ``kind_label`` so operators can disambiguate the failure
    family in the DLQ stream.

Envelope contract (AAP R-26)
----------------------------
Every DLQ / retry message body is a UTF-8 encoded JSON object with this
canonical shape so operators can parse the stream uniformly in Kibana or
``kafka-ui``::

    {
        "schema_version": "v1",
        "service":        "inventory-service",
        "kind":           "retry" | "dlq" | "service-dlq",
        "source_topic":   "<consumed topic>" | "<kind_label>",
        "original_event_b64": "<base64-encoded original message bytes>",
        "attempt":        <int>,
        "error": {
            "code":         "<inventory.* error code>",
            "message":      "<human-readable failure>",
            "is_retryable": true | false
        },
        "correlation_id":   "<X-Correlation-ID propagated from source>",
        "envelope_id":      "<fresh UUID4 per envelope>",
        "dead_lettered_at": "<RFC 3339 UTC timestamp>",
        "metadata":         { ... optional, only for service-DLQ ... }
    }

The original Kafka message bytes are preserved verbatim and base64-encoded
into ``original_event_b64`` so binary or Avro-encoded payloads survive the
round-trip without character-set corruption. Operators can decode the field
offline with any base64 tool to recover the raw payload exactly.

Headers stamped (AAP R-13, R-26)
--------------------------------
The following headers are always stamped on every produced DLQ / retry
message; original headers from the source message are preserved (event-id,
event-type, schema-version, producer-service) so the lineage chain is
intact even after the message has been re-routed:

* ``X-Correlation-ID``      -- propagated from the source message header.
* ``x-service``             -- always ``inventory-service``.
* ``x-schema-version``      -- always ``v1`` (envelope schema version).
* ``x-topic-kind``          -- ``retry`` | ``dlq`` | ``service-dlq``.
* ``attempt-count``         -- decimal string of the attempt index.
* ``error.code``            -- stable machine-readable error code.
* ``error.message``         -- truncated to <= 512 chars for header limits.
* ``error.is_retryable``    -- ``"true"`` or ``"false"``.

Async bridge to confluent_kafka.Producer
----------------------------------------
:meth:`DlqWriter._produce` bridges the synchronous, callback-driven
``confluent_kafka.Producer.produce()`` API onto an :class:`asyncio.Future`
that resolves when the broker delivers an ack. The bridge:

* Creates a fresh ``Future`` per produce call.
* Registers a delivery callback that resolves the Future with either
  ``set_result(None)`` on success or ``set_exception(KafkaException(...))``
  on failure. The callback uses :func:`loop.call_soon_threadsafe` because
  the callback fires on confluent_kafka's poll thread, not the asyncio
  event loop thread.
* Calls :func:`asyncio.to_thread` for the blocking ``poll(0)`` and
  ``flush(timeout)`` calls so the event loop is never starved.
* On :class:`BufferError` (queue full), the writer briefly polls the
  producer to drain in-flight buffers, then retries the produce once.

Failure semantics
-----------------
When delivery fails permanently, ``_produce`` raises :class:`KafkaException`
**directly** -- it does not wrap into :class:`EventPublishError` (which is
the contract of :class:`EventProducer`). The consumer's failure routing
layer needs to distinguish "the DLQ producer itself failed" (do not commit
offset; broker will redeliver the original message) from "the domain event
producer failed" (route via the consumer's own retry/DLQ chain).

AAP cross-references
--------------------
* **AAP Section 0.4.2** -- Inventory service has DLQ topics
  ``order.created.dlq``, ``order.cancelled.dlq``, ``order.fulfilled.dlq``,
  and ``inventory.dlq`` (service-wide).
* **AAP Section 0.5.2.2 bullet 5** -- Inventory Service emits events;
  failures route to DLQ.
* **AAP R-13** -- Correlation IDs propagated through every cross-cutting
  hop; DLQ messages preserve the ``X-Correlation-ID`` header verbatim.
* **AAP R-17** (KEYSTONE) -- Retry topics + DLQ topics required. On retry
  exhaustion, message goes to ``<topic>.dlq``. This module is the
  Inventory Service's realisation of that contract.
* **AAP R-26** -- Structured JSON DLQ envelopes with required fields
  (timestamp, level, service, correlation_id, ...). The envelope contract
  documented above is the canonical realisation for this service.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Final, Mapping

from confluent_kafka import KafkaException, Producer

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_SCHEMA_VERSION: Final[str] = "v1"
"""DLQ envelope schema version. Bump on backward-incompatible envelope changes."""

_SERVICE_NAME: Final[str] = "inventory-service"
"""Stamped into the envelope and `x-service` header for cross-service DLQ debugging."""

_HEADER_CORRELATION_ID: Final[str] = "X-Correlation-ID"
_HEADER_SERVICE: Final[str] = "x-service"
_HEADER_SCHEMA_VERSION: Final[str] = "x-schema-version"
_HEADER_TOPIC_KIND: Final[str] = "x-topic-kind"
_HEADER_ATTEMPT_COUNT: Final[str] = "attempt-count"
_HEADER_ERROR_CODE: Final[str] = "error.code"
_HEADER_ERROR_MESSAGE: Final[str] = "error.message"
_HEADER_ERROR_IS_RETRYABLE: Final[str] = "error.is_retryable"

_KIND_RETRY: Final[str] = "retry"
_KIND_DLQ: Final[str] = "dlq"
_KIND_SERVICE_DLQ: Final[str] = "service-dlq"

_DEFAULT_BUFFER_RETRY_DELAY_S: Final[float] = 0.5
"""When the producer queue is full, poll briefly to drain buffers before retrying."""


# ---------------------------------------------------------------------------
# DlqWriter
# ---------------------------------------------------------------------------


class DlqWriter:
    """Writes failed Kafka messages to retry, DLQ, or service-wide DLQ topics.

    The writer wraps the original Kafka message body in a JSON envelope that
    captures the failure context: source topic, attempt number, error
    code/message, correlation ID, and timestamps. The envelope contract is
    documented in the module docstring.

    Lifecycle
    ---------
    * Constructed once by ``src.container.build_container`` with a process-
      wide :class:`confluent_kafka.Producer` instance configured with
      ``enable.idempotence=True`` and ``acks=all`` (the same instance also
      shared with :class:`EventProducer`).
    * Used by :class:`KafkaConsumerRunner._handle_failure` for per-topic
      ``write_to_retry`` / ``write_to_topic_dlq`` routing of inbound poison
      messages.
    * Used by handlers and the reservation expiry scheduler for
      ``write_to_service_dlq`` when an outbound :class:`EventPublishError`
      itself exhausts retries.
    * Flushed on container shutdown via ``await dlq_writer.flush()``.

    Threading & async model
    -----------------------
    The underlying ``confluent_kafka.Producer.produce()`` is a synchronous,
    callback-driven API that returns immediately and fires the supplied
    delivery callback on its own poll thread once the broker acknowledges
    (or rejects) the message. :meth:`_produce` bridges this onto
    :class:`asyncio.Future`, using :func:`loop.call_soon_threadsafe` to
    resolve the future from the poll thread, and :func:`asyncio.to_thread`
    to offload the blocking ``poll(0)`` / ``flush(timeout)`` calls so the
    event loop is never starved.

    Failure semantics
    -----------------
    On Kafka delivery failure, :meth:`_produce` raises :class:`KafkaException`
    directly -- it does **not** wrap into :class:`EventPublishError` (which
    is the contract of :class:`EventProducer`). The consumer's failure
    routing layer treats the two distinctly: a domain-event publish failure
    is recoverable via the consumer's retry/DLQ chain, but a DLQ-itself
    failure means we must NOT commit the consumer offset (so the broker
    redelivers the original message instead of silently dropping it).
    """

    __slots__ = (
        "_producer",
        "_service_dlq_topic",
        "_retry_topic_suffix",
        "_dlq_topic_suffix",
        "_flush_timeout_s",
        "_log",
    )

    def __init__(
        self,
        *,
        producer: Producer,
        service_dlq_topic: str,
        retry_topic_suffix: str,
        dlq_topic_suffix: str,
        flush_timeout_s: float = 5.0,
    ) -> None:
        """Construct a :class:`DlqWriter` bound to a shared producer instance.

        Args:
            producer: The process-wide :class:`confluent_kafka.Producer`
                instance (the same one shared with :class:`EventProducer`).
                The producer must be configured with
                ``enable.idempotence=True`` and ``acks=all`` so DLQ writes
                are durable and exactly-once even under broker re-elections.
            service_dlq_topic: The service-wide DLQ topic name (typically
                ``"inventory.dlq"``). Sourced from
                ``settings.topics.dlq.inventory_dlq``.
            retry_topic_suffix: Suffix appended to a source topic name to
                derive the retry topic (e.g. ``".retry"`` so
                ``order.created`` -> ``order.created.retry``). Must start
                with ``"."``.
            dlq_topic_suffix: Suffix appended to a source topic name to
                derive the per-topic DLQ topic (e.g. ``".dlq"`` so
                ``order.created`` -> ``order.created.dlq``). Must start
                with ``"."``.
            flush_timeout_s: Default timeout in seconds for :meth:`flush`.
                Must be > 0. The :meth:`flush` caller may override this
                per-call.

        Raises:
            ValueError: If ``service_dlq_topic`` is empty, either suffix
                does not start with ``"."``, or ``flush_timeout_s <= 0``.
        """
        if not service_dlq_topic:
            raise ValueError("DlqWriter: service_dlq_topic must be non-empty")
        if not retry_topic_suffix.startswith("."):
            raise ValueError(
                f"DlqWriter: retry_topic_suffix must start with '.' "
                f"(got: {retry_topic_suffix!r})"
            )
        if not dlq_topic_suffix.startswith("."):
            raise ValueError(
                f"DlqWriter: dlq_topic_suffix must start with '.' "
                f"(got: {dlq_topic_suffix!r})"
            )
        if flush_timeout_s <= 0:
            raise ValueError(
                f"DlqWriter: flush_timeout_s must be > 0 (got: {flush_timeout_s})"
            )
        self._producer = producer
        self._service_dlq_topic = service_dlq_topic
        self._retry_topic_suffix = retry_topic_suffix
        self._dlq_topic_suffix = dlq_topic_suffix
        self._flush_timeout_s = flush_timeout_s
        self._log = logging.getLogger("inventory_service.events.dlq")

    # ------------------------------------------------------------------
    # Public API -- write_to_retry
    # ------------------------------------------------------------------

    async def write_to_retry(
        self,
        *,
        source_topic: str,
        key: bytes | None,
        value: bytes | None,
        headers: Mapping[str, str],
        attempt: int,
        error_code: str,
        error_message: str,
        error_is_retryable: bool,
    ) -> None:
        """Forward a message to ``<source_topic><retry_topic_suffix>``.

        Used by :class:`KafkaConsumerRunner` when a *retryable* failure is
        raised for an inbound message and the next attempt count is still
        within the configured ``max_attempts`` budget. The standard envelope
        is wrapped around the original payload, the ``attempt-count`` header
        is incremented (and stamped into the envelope), and error metadata
        is preserved on both header and envelope sides.

        Args:
            source_topic: The original consumed topic (e.g. ``order.created``).
                The retry topic is derived as
                ``f"{source_topic}{self._retry_topic_suffix}"``.
            key: The original Kafka message key (preserved verbatim so
                consumers can continue partition-pinning by key, e.g. by
                ``order_id``). ``None`` if the source message had no key.
            value: The original Kafka message value bytes. Base64-encoded
                into the ``original_event_b64`` envelope field. ``None`` is
                stored as the empty string.
            headers: The original consumed message headers. Used to read
                the ``X-Correlation-ID`` for envelope propagation, and to
                preserve original lineage headers (event-id, event-type,
                schema-version, producer-service) when present.
            attempt: The next attempt index that the retry topic consumer
                will see (i.e. the *post-increment* value after the current
                attempt failed). Stamped into the envelope ``attempt`` field
                and the ``attempt-count`` header.
            error_code: A stable, machine-readable error code from
                :mod:`src.exceptions` (e.g. ``"inventory.optimistic_lock"``).
                Stamped into both the envelope ``error.code`` and the
                ``error.code`` header.
            error_message: A human-readable description of the failure.
                Truncated to <= 512 chars in the header; full text preserved
                in the envelope.
            error_is_retryable: ``True`` if the underlying exception is
                retryable (it always should be for this method, since the
                consumer only routes retryable failures here).

        Raises:
            KafkaException: If the broker rejects the produce (e.g. topic
                ACL denial, broker unavailable). Surfacing this directly to
                the consumer signals "do not commit offset" so the original
                message will be redelivered.
        """
        retry_topic = f"{source_topic}{self._retry_topic_suffix}"
        envelope = self._build_envelope(
            kind=_KIND_RETRY,
            source_topic=source_topic,
            original_value=value,
            attempt=attempt,
            error_code=error_code,
            error_message=error_message,
            error_is_retryable=error_is_retryable,
            correlation_id=headers.get(_HEADER_CORRELATION_ID, ""),
        )
        out_headers = self._build_headers(
            kind=_KIND_RETRY,
            source_headers=headers,
            attempt=attempt,
            error_code=error_code,
            error_message=error_message,
            error_is_retryable=error_is_retryable,
        )
        await self._produce(
            topic=retry_topic,
            key=key,
            value=envelope,
            headers=out_headers,
            log_extra={
                "source_topic": source_topic,
                "retry_topic": retry_topic,
                "kind": _KIND_RETRY,
                "attempt": attempt,
                "error_code": error_code,
            },
        )


    # ------------------------------------------------------------------
    # Public API -- write_to_topic_dlq
    # ------------------------------------------------------------------

    async def write_to_topic_dlq(
        self,
        *,
        source_topic: str,
        key: bytes | None,
        value: bytes | None,
        headers: Mapping[str, str],
        error_code: str,
        error_message: str,
        error_is_retryable: bool,
    ) -> None:
        """Forward a message to ``<source_topic><dlq_topic_suffix>``.

        Used by :class:`KafkaConsumerRunner` when:

        * A message has exhausted its retry budget on
          ``<source_topic><retry_topic_suffix>``.
        * A *non-retryable* exception (e.g. an
          :class:`InsufficientStockError` that the handler chooses to mark
          terminal, or a :class:`ReservationNotFoundError` for a stale
          compensation event) is raised.
        * Deserialisation fails entirely (poison message that violates the
          registered Schema Registry contract).

        The current ``attempt-count`` header value is read off the inbound
        message and stamped into the envelope unchanged -- this is the
        terminal attempt, and the count is preserved as evidence of how many
        retries were consumed before the message was dead-lettered.

        Args:
            source_topic: The original consumed topic. The DLQ topic is
                derived as ``f"{source_topic}{self._dlq_topic_suffix}"``.
            key: The original Kafka message key (preserved verbatim).
            value: The original Kafka message value bytes. Base64-encoded
                into the envelope.
            headers: The original consumed message headers. Used to read
                the ``X-Correlation-ID``, the current ``attempt-count``,
                and to preserve original lineage headers.
            error_code: Stable machine-readable error code.
            error_message: Human-readable failure description.
            error_is_retryable: Whether the underlying exception is retryable
                (will be ``False`` in most cases when reaching the DLQ;
                ``True`` if reached via retry exhaustion).

        Raises:
            KafkaException: If the broker rejects the produce. Surfacing
                this directly signals "do not commit offset".
        """
        dlq_topic = f"{source_topic}{self._dlq_topic_suffix}"
        attempt = self._read_attempt(headers)
        envelope = self._build_envelope(
            kind=_KIND_DLQ,
            source_topic=source_topic,
            original_value=value,
            attempt=attempt,
            error_code=error_code,
            error_message=error_message,
            error_is_retryable=error_is_retryable,
            correlation_id=headers.get(_HEADER_CORRELATION_ID, ""),
        )
        out_headers = self._build_headers(
            kind=_KIND_DLQ,
            source_headers=headers,
            attempt=attempt,
            error_code=error_code,
            error_message=error_message,
            error_is_retryable=error_is_retryable,
        )
        await self._produce(
            topic=dlq_topic,
            key=key,
            value=envelope,
            headers=out_headers,
            log_extra={
                "source_topic": source_topic,
                "dlq_topic": dlq_topic,
                "kind": _KIND_DLQ,
                "error_code": error_code,
            },
        )


    # ------------------------------------------------------------------
    # Public API -- write_to_service_dlq
    # ------------------------------------------------------------------

    async def write_to_service_dlq(
        self,
        *,
        kind_label: str,
        original_value: bytes | None,
        correlation_id: str,
        error_code: str,
        error_message: str,
        error_is_retryable: bool,
        attempt: int = 0,
        extra_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Forward a message to the service-wide ``inventory.dlq`` topic.

        Used for service-internal poison events that do **not** have a single
        inbound source topic. Typical callers:

        * The reservation expiry scheduler when its tick handler fails
          permanently (``kind_label='expiry_scheduler_failure'``).
        * The reservation-state handlers when an outbound
          :class:`EventPublishError` exhausts its retry budget
          (``kind_label='event_publish_exhausted'``).
        * A future ``ExternalWMSWarehouseAdapter`` on persistent provider
          failure (``kind_label='warehouse_adapter_persistent_failure'``).

        Because there is no inbound source topic, the writer cannot read
        original lineage headers (event-id, producer-service, etc.). Callers
        are expected to supply the correlation ID explicitly so it is not
        lost. The free-form ``kind_label`` is stamped into the envelope's
        ``source_topic`` field as a compact descriptor for operators
        filtering the global DLQ stream.

        The Kafka message key is set to the correlation ID (UTF-8 encoded)
        when present, so DLQ entries originating from the same workflow
        cluster on the same partition for downstream investigation tools.

        Args:
            kind_label: A free-form descriptor of the failure family
                (e.g. ``'expiry_scheduler_failure'``,
                ``'event_publish_exhausted'``). Stamped into the envelope's
                ``source_topic`` field so operators can filter / group in
                the DLQ stream.
            original_value: The original payload bytes (e.g. the serialised
                outbound event the producer failed to publish). May be
                ``None`` for failures that are not message-shaped (e.g. a
                scheduler tick failure).
            correlation_id: Workflow correlation ID (typically propagated
                from the originating order). Stamped into the envelope and
                ``X-Correlation-ID`` header. Empty string if unknown.
            error_code: Stable machine-readable error code.
            error_message: Human-readable failure description.
            error_is_retryable: Whether the underlying exception is
                retryable. By the time a failure reaches the service DLQ
                it has typically already exhausted its retry budget so this
                flag is informational, but it is preserved verbatim.
            attempt: The attempt index at the point of dead-lettering.
                Defaults to ``0`` for failures that are not retried in a
                bounded fashion (e.g. scheduler ticks).
            extra_metadata: Optional free-form mapping of additional
                context fields (e.g. ``{'reservation_id': ..., 'sku': ...}``).
                Coerced to JSON-serialisable forms via :func:`_to_jsonable`
                so callers can pass UUIDs, datetimes, or nested mappings
                without worrying about serialisation. Only included in the
                envelope when truthy.

        Raises:
            KafkaException: If the broker rejects the produce.
        """
        envelope = self._build_envelope(
            kind=_KIND_SERVICE_DLQ,
            source_topic=kind_label,
            original_value=original_value,
            attempt=attempt,
            error_code=error_code,
            error_message=error_message,
            error_is_retryable=error_is_retryable,
            correlation_id=correlation_id,
            extra_metadata=extra_metadata,
        )
        out_headers = self._build_headers_simple(
            kind=_KIND_SERVICE_DLQ,
            correlation_id=correlation_id,
            attempt=attempt,
            error_code=error_code,
            error_message=error_message,
            error_is_retryable=error_is_retryable,
        )
        await self._produce(
            topic=self._service_dlq_topic,
            key=correlation_id.encode("utf-8") if correlation_id else None,
            value=envelope,
            headers=out_headers,
            log_extra={
                "service_dlq_topic": self._service_dlq_topic,
                "kind": _KIND_SERVICE_DLQ,
                "kind_label": kind_label,
                "error_code": error_code,
            },
        )


    # ------------------------------------------------------------------
    # Public API -- flush
    # ------------------------------------------------------------------

    async def flush(self, timeout_s: float | None = None) -> int:
        """Block until all pending DLQ messages are flushed or timeout.

        Should be called on container shutdown so any in-flight DLQ writes
        durably ack before the process exits. Per-message flushing is
        intentionally NOT supported -- ``confluent_kafka.Producer`` batches
        produces internally and per-message flush would defeat batching and
        crater throughput.

        Args:
            timeout_s: Optional override for the constructor's
                ``flush_timeout_s``. If ``None``, uses the configured default.

        Returns:
            The number of messages still queued after the timeout. ``0``
            means a full drain succeeded; any positive value means the
            broker did not ack everything in time and those messages may be
            silently dropped on process exit. Callers should log a warning
            on a non-zero return.
        """
        timeout = timeout_s if timeout_s is not None else self._flush_timeout_s
        return await asyncio.to_thread(self._producer.flush, timeout)

    # ------------------------------------------------------------------
    # Internal helpers -- envelope / header construction
    # ------------------------------------------------------------------

    def _build_envelope(
        self,
        *,
        kind: str,
        source_topic: str,
        original_value: bytes | None,
        attempt: int,
        error_code: str,
        error_message: str,
        error_is_retryable: bool,
        correlation_id: str,
        extra_metadata: Mapping[str, Any] | None = None,
    ) -> bytes:
        """Build the JSON envelope wrapping the original Kafka message.

        The original_value bytes are preserved verbatim (base64-encoded so
        operators can recover the raw payload from a DLQ tool). All other
        fields are top-level for easy filtering in Kibana / kafka-ui.

        Args:
            kind: One of ``_KIND_RETRY``, ``_KIND_DLQ``, ``_KIND_SERVICE_DLQ``.
            source_topic: Source topic (or ``kind_label`` for service DLQ).
            original_value: The original message bytes; encoded as base64.
                ``None`` becomes the empty string.
            attempt: Current attempt count.
            error_code: Stable error code.
            error_message: Human-readable failure description.
            error_is_retryable: Whether the underlying exception is retryable.
            correlation_id: Workflow correlation ID.
            extra_metadata: Optional metadata mapping; coerced via
                :func:`_to_jsonable` so non-trivial Python types
                (UUID, datetime, nested mappings) serialise cleanly.

        Returns:
            UTF-8 encoded JSON bytes with compact separators.
        """
        original_b64 = (
            base64.b64encode(original_value).decode("ascii") if original_value else ""
        )
        envelope_dict: dict[str, Any] = {
            "schema_version": _SCHEMA_VERSION,
            "service": _SERVICE_NAME,
            "kind": kind,
            "source_topic": source_topic,
            "original_event_b64": original_b64,
            "attempt": attempt,
            "error": {
                "code": error_code,
                "message": error_message,
                "is_retryable": error_is_retryable,
            },
            "correlation_id": correlation_id,
            "envelope_id": str(uuid.uuid4()),
            "dead_lettered_at": datetime.now(timezone.utc).isoformat(),
        }
        if extra_metadata:
            # Defensively coerce so JSON encoding never fails on UUID,
            # datetime, or nested non-primitive types supplied by callers.
            envelope_dict["metadata"] = {
                str(k): _to_jsonable(v) for k, v in extra_metadata.items()
            }
        return json.dumps(
            envelope_dict, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")

    def _build_headers(
        self,
        *,
        kind: str,
        source_headers: Mapping[str, str],
        attempt: int,
        error_code: str,
        error_message: str,
        error_is_retryable: bool,
    ) -> list[tuple[str, bytes]]:
        """Build header list for retry/DLQ messages, preserving lineage headers.

        Stamps the standard set of envelope headers (correlation ID, service,
        schema version, topic kind, attempt count, error metadata) and
        preserves the original lineage headers (event-id, event-type,
        schema-version, producer-service) from the source message when
        present. The error message is truncated to <= 512 chars to respect
        Kafka header size guidance and downstream tooling limits.

        Args:
            kind: Topic kind tag (``retry`` / ``dlq``).
            source_headers: Original consumed message headers.
            attempt: Attempt count to stamp.
            error_code: Stable error code.
            error_message: Human-readable failure description.
            error_is_retryable: Whether the failure is retryable.

        Returns:
            List of ``(name, utf-8-bytes)`` tuples in the form
            ``confluent_kafka.Producer.produce`` expects.
        """
        correlation_id = source_headers.get(_HEADER_CORRELATION_ID, "")
        headers: dict[str, str] = {
            _HEADER_CORRELATION_ID: correlation_id,
            _HEADER_SERVICE: _SERVICE_NAME,
            _HEADER_SCHEMA_VERSION: _SCHEMA_VERSION,
            _HEADER_TOPIC_KIND: kind,
            _HEADER_ATTEMPT_COUNT: str(attempt),
            _HEADER_ERROR_CODE: error_code,
            _HEADER_ERROR_MESSAGE: _truncate_for_header(error_message, max_len=512),
            _HEADER_ERROR_IS_RETRYABLE: "true" if error_is_retryable else "false",
        }
        # Preserve original event-id, event-type, schema-version,
        # producer-service if present so the lineage chain is not lost
        # when the message is re-routed through retry/DLQ.
        for preserve_key in (
            "event-id",
            "event-type",
            "schema-version",
            "producer-service",
        ):
            if preserve_key in source_headers and source_headers[preserve_key]:
                headers[preserve_key] = source_headers[preserve_key]
        return [(k, v.encode("utf-8")) for k, v in headers.items()]

    def _build_headers_simple(
        self,
        *,
        kind: str,
        correlation_id: str,
        attempt: int,
        error_code: str,
        error_message: str,
        error_is_retryable: bool,
    ) -> list[tuple[str, bytes]]:
        """Build header list for service-wide DLQ messages (no source headers).

        Service-wide DLQ entries originate from internal failures (scheduler
        ticks, outbound publish exhaustion) where there is no inbound source
        message and therefore no original lineage headers to preserve. The
        correlation ID must be supplied explicitly by the caller.

        Args:
            kind: Topic kind tag (``service-dlq``).
            correlation_id: Workflow correlation ID supplied by the caller.
            attempt: Attempt count.
            error_code: Stable error code.
            error_message: Human-readable failure description.
            error_is_retryable: Whether the failure is retryable.

        Returns:
            List of ``(name, utf-8-bytes)`` tuples.
        """
        headers: dict[str, str] = {
            _HEADER_CORRELATION_ID: correlation_id,
            _HEADER_SERVICE: _SERVICE_NAME,
            _HEADER_SCHEMA_VERSION: _SCHEMA_VERSION,
            _HEADER_TOPIC_KIND: kind,
            _HEADER_ATTEMPT_COUNT: str(attempt),
            _HEADER_ERROR_CODE: error_code,
            _HEADER_ERROR_MESSAGE: _truncate_for_header(error_message, max_len=512),
            _HEADER_ERROR_IS_RETRYABLE: "true" if error_is_retryable else "false",
        }
        return [(k, v.encode("utf-8")) for k, v in headers.items()]

    @staticmethod
    def _read_attempt(headers: Mapping[str, str]) -> int:
        """Read ``attempt-count`` header from a source message, defaulting to 0.

        Kafka headers are stringly-typed, so the count is parsed via
        :func:`int`; missing, empty, or malformed values default to ``0``
        rather than raising -- a missing header from a fresh-from-source
        message must NOT prevent dead-lettering.

        Args:
            headers: Source message headers.

        Returns:
            The parsed attempt count, or ``0`` on any parse failure.
        """
        raw = headers.get(_HEADER_ATTEMPT_COUNT, "")
        if not raw:
            return 0
        try:
            return int(raw)
        except (ValueError, TypeError):
            return 0

    # ------------------------------------------------------------------
    # Internal helper -- async produce bridge
    # ------------------------------------------------------------------

    async def _produce(
        self,
        *,
        topic: str,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]],
        log_extra: dict[str, Any],
    ) -> None:
        """Bridge ``confluent_kafka.Producer.produce()`` to asyncio.

        Mirrors the async pattern used by :class:`EventProducer._produce`,
        but does **not** wrap broker failures into
        :class:`EventPublishError`. Instead it raises
        :class:`KafkaException` directly so the consumer's failure routing
        layer can distinguish "the DLQ producer itself failed" (must not
        commit offset; broker will redeliver the original message) from
        "a domain event publish failed" (route via consumer's retry/DLQ
        chain).

        On :class:`BufferError`, the producer's internal queue is full --
        we briefly poll the producer (offloaded to a thread so the event
        loop is never starved) to drain in-flight buffers, then retry the
        produce exactly once. If the queue is still full after the second
        attempt, the :class:`BufferError` propagates to the caller.

        Args:
            topic: Destination topic name.
            key: Kafka message key (preserved from the source message, or
                a fresh value such as the correlation-id bytes for service
                DLQ writes).
            value: Pre-serialised JSON envelope bytes.
            headers: Pre-built header list as produced by
                :meth:`_build_headers` or :meth:`_build_headers_simple`.
            log_extra: Structured ``extra=`` dict for log lines emitted
                during the produce; merged with ``"topic"`` on the success
                line.

        Raises:
            KafkaException: If the broker rejects the produce after the
                callback fires, or on broker-internal errors.
            BufferError: If the producer's queue remains full after the
                buffer-drain retry. (This is intentional -- callers can
                use this to detect and surface persistent producer
                exhaustion.)
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()

        def delivery_callback(err: Any, msg: Any) -> None:
            # NOTE: this callback fires on confluent_kafka's internal poll
            # thread, NOT the asyncio event-loop thread, so we must use
            # call_soon_threadsafe to interact with the future.
            if err is not None:
                if not future.done():
                    loop.call_soon_threadsafe(
                        future.set_exception,
                        KafkaException(err),
                    )
                return
            if not future.done():
                loop.call_soon_threadsafe(future.set_result, None)

        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
                on_delivery=delivery_callback,
            )
        except BufferError:
            # Producer queue is full -- drain briefly, then retry once.
            self._log.warning("dlq.producer.buffer_full", extra=log_extra)
            await asyncio.to_thread(self._producer.poll, _DEFAULT_BUFFER_RETRY_DELAY_S)
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=headers,
                on_delivery=delivery_callback,
            )

        # Drive callback firing without blocking the event loop. poll(0)
        # services any already-acked messages immediately and returns; the
        # confluent-kafka producer's internal poll thread services the
        # actual broker I/O in the background.
        await asyncio.to_thread(self._producer.poll, 0)
        await future

        self._log.info(
            "dlq.producer.delivered",
            extra={**log_extra, "topic": topic},
        )



# ---------------------------------------------------------------------------
# Module-level helpers (outside the DlqWriter class)
# ---------------------------------------------------------------------------


def _truncate_for_header(value: str, *, max_len: int = 512) -> str:
    """Truncate a string for safe use in a Kafka header.

    Kafka headers have configurable per-cluster size limits, but
    conventionally the total header bytes are capped at a few KB. Error
    messages can be arbitrarily long (e.g. truncated stack traces or full
    SQL statements), so we truncate to a conservative ``max_len`` and
    append ``"...[truncated]"`` so operators can immediately see they are
    looking at a trimmed value rather than the full text.

    Args:
        value: The string to truncate. Empty / falsy values pass through
            as the empty string.
        max_len: The maximum length of the returned string, including the
            truncation marker. Default ``512`` -- conservative enough to
            keep header overhead modest while still leaving room for
            useful failure context.

    Returns:
        ``value`` unchanged if shorter than ``max_len``; otherwise the
        first ``max_len - 14`` characters of ``value`` followed by
        ``"...[truncated]"`` (which is exactly 14 characters long, so the
        returned string is exactly ``max_len`` chars).
    """
    if not value:
        return ""
    if len(value) <= max_len:
        return value
    return value[: max_len - 14] + "...[truncated]"


def _to_jsonable(value: Any) -> Any:
    """Coerce common Python types to JSON-serialisable forms.

    ``json.dumps`` rejects :class:`uuid.UUID`, :class:`datetime.datetime`,
    sets, custom objects, and a few other Python types. The DLQ writer's
    ``extra_metadata`` parameter is intentionally typed as
    ``Mapping[str, Any]`` so callers can pass these directly without
    pre-serialising; this helper coerces them to a JSON-serialisable form
    so :meth:`DlqWriter._build_envelope` can never crash on metadata.

    Coercion rules
    --------------
    * ``None`` and JSON-native scalars (``str``, ``int``, ``float``,
      ``bool``) -- returned unchanged.
    * ``list`` / ``tuple`` -- recursively coerced; tuples become lists.
    * ``dict`` -- recursively coerced; keys are coerced to ``str``.
    * :class:`datetime.datetime` -- formatted via :meth:`datetime.isoformat`.
    * :class:`uuid.UUID` -- formatted via :func:`str`.
    * Anything else -- coerced via :func:`str` as a last-resort fallback.

    Args:
        value: The value to coerce.

    Returns:
        A JSON-serialisable representation of ``value``.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------

__all__ = ["DlqWriter"]

