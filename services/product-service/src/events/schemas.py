"""Producer-side Schema Registry serializer wrapper for the Product Service.

This module exposes a single class :class:`SchemaSerializer` -- a thin,
thread-safe wrapper around Confluent's
:class:`confluent_kafka.schema_registry.json_schema.JSONSerializer` that
realises **AAP R-14** (every Kafka event must be validated against
Schema Registry before produce). The wrapper is intentionally narrow: it
caches one :class:`JSONSerializer` instance per topic and exposes a
single public method :meth:`SchemaSerializer.serialize` that returns
Confluent wire-format bytes ready to be handed to
``confluent_kafka.Producer.produce``.

Position in the events package
------------------------------
::

    services/product-service/src/events/
    +-- __init__.py
    +-- payloads.py     (Pydantic v2 envelope models -- not imported here)
    +-- schemas.py      (THIS FILE -- Schema Registry serializer wrapper)
    +-- producer.py     (calls ``serialize(topic, payload)`` BEFORE produce)
    +-- publisher.py    (high-level facade orchestrating producer +
                         publisher)

What this file is NOT
---------------------
* **NOT a Pydantic model file.** Pydantic event envelope models live in
  :mod:`services.product-service.src.events.payloads`. This module
  contains zero Pydantic.
* **NOT a topic catalog.** Topic name constants live in
  :mod:`services.product-service.src.config.settings` (loaded from
  ``services/product-service/config/default.yaml``) -- never inlined
  here.
* **NOT a deserializer.** The Product Service is a *pure event
  producer* per AAP Section 0.4.2: it only emits ``product.created`` /
  ``product.updated`` -- it never consumes Kafka events. Sibling
  services that consume ``product.*`` events (Recommendation Engine,
  Inventory Service, search index pipeline) own their own
  ``JSONDeserializer`` wiring.

Why a wrapper instead of using ``JSONSerializer`` directly?
-----------------------------------------------------------
The native :class:`JSONSerializer` is **per-topic** -- each topic
needs its own serializer instance because the serializer caches the
schema id obtained from Schema Registry on the first call and reuses
it on subsequent calls. Centralising this behaviour in one wrapper
gives us:

1. **Per-topic caching** so we make a single Schema Registry lookup
   per topic per process (the cache is populated lazily on first use
   of a topic and is immutable for the lifetime of the process).
2. **Centralised configuration** -- the production-grade discipline
   (``auto.register.schemas=False`` and ``use.latest.version=True``)
   is applied uniformly across every topic (AAP R-14).
3. **A clean async-friendly interface** -- the only public method is
   :meth:`SchemaSerializer.serialize` which takes a topic name and a
   plain ``dict`` and returns ``bytes``. Callers do not need to
   construct :class:`SerializationContext` or
   :class:`MessageField` objects.
4. **Future extensibility** -- e.g., adding metric counters for
   serialization latency, or supporting Avro alongside JSON, requires
   no change in the producer.

Authoritative AAP references
----------------------------
* **AAP R-14 (KEYSTONE)** -- All Kafka events must be validated against
  Schema Registry before produce. This file is THE producer-side gate
  that enforces the contract.
* **AAP R-30** -- Topic names follow ``<domain>.<verb>`` and the
  Schema Registry subject for the value is ``<topic>-value``
  (TopicNameStrategy).
* **AAP R-31** -- Events include a ``event_version`` field so that
  schemas can evolve in a backward-compatible fashion. The
  ``use.latest.version=True`` discipline guarantees deployments
  serialise against the newest registered schema.
* **AAP R-26** -- Structured logging discipline. The module emits
  ``events.schemas.serializer_built`` (DEBUG, on cache miss),
  ``events.schemas.serialization_failed`` (ERROR, when payload does
  not match the registered schema), and ``events.schemas.serializer_error``
  (ERROR, for unexpected exceptions including
  :class:`SchemaRegistryError` for transient registry unavailability).

Error categorisation contract
-----------------------------
Two distinct exception flavours surface to the caller, with intentionally
different semantics:

* :class:`confluent_kafka.serialization.SerializationError`
  -- the payload does not conform to the registered JSON Schema (a
  programming bug in the publisher / payload model). The caller MUST
  treat this as fatal / non-retryable; retrying is futile because the
  payload will fail validation again. The producer escalates this to
  the developer / observability dashboards via the DLQ.
* :class:`confluent_kafka.schema_registry.error.SchemaRegistryError`
  (caught by the broad ``except Exception`` clause) -- the Schema
  Registry is unreachable or the subject is not registered. The
  caller MAY retry under the standard retry / circuit-breaker policy
  (AAP R-15 / R-16); the producer's resilience wrapper handles this.

Side-effect freedom
-------------------
This module performs ZERO I/O at import time. It does not connect to
Kafka, does not connect to Schema Registry, does not read from disk,
and does not mutate global state. The only side effects happen inside
:meth:`SchemaSerializer.serialize`, where the underlying
:class:`JSONSerializer` may issue HTTP GETs to the registry on the
first call for a given topic and the dict-cache assignment registers
the resulting :class:`JSONSerializer` for reuse.

Thread-safety notes
-------------------
* The cache (``self._serializer_cache``) is a plain ``dict``. Python's
  GIL guarantees atomic ``dict[key] = value`` insertion, so concurrent
  callers may construct the same per-topic
  :class:`JSONSerializer` twice (one is harmlessly discarded), but the
  cache will never become corrupt. This is acceptable for our use case
  where the cache is typically warmed on the first event in each
  topic.
* Each cached :class:`JSONSerializer` instance is treated as a
  per-topic singleton thereafter; we do not assume its
  ``__call__`` is thread-safe and we do not require it to be -- the
  instances are written once and read many times, and Confluent's
  implementation does internal id caching that is GIL-protected.
* The module does NOT expose any async methods. Schema Registry
  lookups are sync; the upstream ``EventProducer`` wraps the call to
  :meth:`SchemaSerializer.serialize` in :func:`asyncio.to_thread` so
  the FastAPI request loop is not blocked.

Hard constraints
----------------
* ``schema_str=None`` is passed to :class:`JSONSerializer`; the
  serializer fetches the schema from the registry under
  ``use.latest.version=True`` -- supported by ``confluent-kafka``
  >= 2.10 (the project pins ``>=2.3.0,<3.0.0`` and CI installs the
  latest matching version).
* ``auto.register.schemas=False`` -- producers MUST NOT register new
  schemas at runtime. Schemas are registered out-of-band by the
  platform team via ``infrastructure/kafka/schemas/`` (AAP R-14).
* ``use.latest.version=True`` -- producers always serialise against
  the newest registered schema, forcing deployments to update the
  schema BEFORE deploying code that uses new fields.
* TopicNameStrategy -- the Schema Registry subject for the value is
  ``f"{topic}-value"``. Other naming strategies (RecordNameStrategy,
  TopicRecordNameStrategy) are intentionally NOT supported because
  the Product Service uses a single event type per topic.
"""

from __future__ import annotations

import logging
from typing import Any, Final, Literal

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    SerializationError,
)


# =============================================================================
# Module-level constants
# =============================================================================

#: Logger name for this module. Follows the dotted-namespace convention used
#: across the Product Service so log filters in Kibana can target the
#: ``product_service.events.*`` family or ``product_service.events.schemas``
#: specifically (AAP R-26).
_LOGGER_NAME: Final[str] = "product_service.events.schemas"

#: Sole supported subject naming strategy. Confluent Schema Registry exposes
#: three canonical strategies (TopicNameStrategy, RecordNameStrategy, and
#: TopicRecordNameStrategy). The Product Service uses a single event type
#: per topic, so TopicNameStrategy -- which generates the subject name as
#: ``f"{topic}-{message_field}"`` -- is the right granularity. Allowing only
#: this strategy at the type level (via :data:`Literal`) keeps the public
#: API narrow and prevents accidental misconfiguration.
_SUBJECT_STRATEGY_TOPIC_NAME: Final[str] = "topic_name"

#: Suffix appended to the topic name when forming the value subject under
#: TopicNameStrategy. Used only in DEBUG log lines for human-readable
#: diagnostics; the actual subject computation is performed inside
#: :class:`JSONSerializer` by Confluent's default
#: ``topic_subject_name_strategy``.
_VALUE_SUBJECT_SUFFIX: Final[str] = "-value"

#: Configuration applied to every per-topic :class:`JSONSerializer` instance.
#: A fresh shallow copy of this dict is passed to each serializer (Confluent
#: mutates the dict it receives via ``conf_copy.pop(...)``). The two keys
#: enforce the production-grade schema discipline mandated by AAP R-14:
#:
#: * ``auto.register.schemas=False`` -- producers MUST NOT register new
#:   schemas at runtime. Every schema change goes through the platform
#:   team's registration workflow under ``infrastructure/kafka/schemas/``.
#:   This prevents drift between environments (a producer registering a
#:   compatible-only-in-staging schema would corrupt the registry).
#: * ``use.latest.version=True`` -- always serialise against the LATEST
#:   registered schema version. This forces an upgrade order: register
#:   schema FIRST, then deploy code. Backward-incompatible schema changes
#:   fail at registration time, not at runtime.
#:
#: NOTE: ``auto.register.schemas`` and ``use.latest.version`` are mutually
#: exclusive in Confluent's :class:`JSONSerializer` -- enabling both raises
#: ``ValueError("cannot enable both use.latest.version and auto.register.schemas")``
#: at construction time. Our combination (``False`` + ``True``) is the
#: canonical production setting.
_SERIALIZER_CONF: Final[dict[str, Any]] = {
    "auto.register.schemas": False,
    "use.latest.version": True,
}


# =============================================================================
# Public class
# =============================================================================


class SchemaSerializer:
    """Schema-Registry-validated JSON serializer for Kafka payloads.

    This class is a thin wrapper around Confluent's
    :class:`confluent_kafka.schema_registry.json_schema.JSONSerializer`
    that caches one serializer per topic, centralises the
    production-grade configuration (``auto.register.schemas=False`` and
    ``use.latest.version=True``), and exposes a single public method
    :meth:`serialize` returning Confluent wire-format bytes.

    Usage
    -----
    ::

        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr_client = SchemaRegistryClient({"url": "http://localhost:8081"})
        serializer = SchemaSerializer(
            schema_registry=sr_client,
            subject_strategy="topic_name",
        )

        wire_bytes = serializer.serialize(
            topic="product.created",
            payload={
                "event_id": "...",
                "event_version": 1,
                "correlation_id": "...",
                "occurred_at": "2026-05-02T12:00:00Z",
                "product": {...},
            },
        )

    The returned bytes include the standard Confluent wire-format prefix
    (magic byte ``0x00`` + 4-byte schema id) so downstream
    Schema-Registry-aware deserializers can fetch the schema from the
    registry and decode the JSON body.

    Caching behaviour
    -----------------
    * The first call to :meth:`serialize` for a given topic builds a
      :class:`JSONSerializer` configured with ``schema_str=None`` and
      ``use.latest.version=True``, causing Confluent to fetch the latest
      registered schema for the topic's value subject (``<topic>-value``)
      from Schema Registry on the first :meth:`__call__` invocation.
    * Subsequent calls for the same topic reuse the cached serializer,
      which avoids repeated Schema Registry round-trips and reuses the
      schema id Confluent has already cached internally.
    * Cache entries are immutable for the lifetime of the process. To
      pick up a schema change, restart the service.

    Thread-safety
    -------------
    The cache is populated under a lock-free dict assignment. Because
    Python's GIL guarantees atomic dict insertions, concurrent callers
    may construct the same serializer twice (one is harmlessly
    discarded), but the cache will never become corrupt. This is the
    same pattern used by the sibling Notification Service and Order
    Service serializer wrappers.

    Async-friendliness
    ------------------
    Schema Registry lookups are synchronous; this class is therefore
    synchronous as well. The upstream :class:`EventProducer` wraps each
    :meth:`serialize` call in :func:`asyncio.to_thread` so the FastAPI
    request loop is not blocked while the registry HTTP call is in
    flight (rare -- happens only on the first event for each topic).
    """

    # __slots__ keeps the per-instance memory footprint small and signals
    # that there are no other dynamic attributes on the object. The
    # serializer is typically a process-wide singleton constructed once at
    # startup by the dependency-injection container; the slot economy is a
    # micro-optimisation rather than a critical concern, but it also
    # serves as documentation of the canonical instance shape.
    __slots__ = (
        "_log",
        "_schema_registry",
        "_serializer_cache",
        "_subject_strategy",
    )

    def __init__(
        self,
        *,
        schema_registry: SchemaRegistryClient,
        subject_strategy: Literal["topic_name"] = "topic_name",
    ) -> None:
        """Initialise the serializer with a Schema Registry client.

        Parameters
        ----------
        schema_registry
            Pre-configured Confluent
            :class:`SchemaRegistryClient`. The container is responsible
            for materialising this with the URL and (optional) Basic
            auth credentials sourced from
            :class:`SchemaRegistrySettings`. The registry is held by
            reference; we never mutate it.
        subject_strategy
            Subject naming strategy. Only ``"topic_name"`` is supported
            (TopicNameStrategy: subject = ``<topic>-value``); any other
            value raises :class:`ValueError`. The :class:`Literal` type
            hint enforces this at the type-checker level so misuse is
            caught before runtime by ``mypy --strict``.

        Raises
        ------
        ValueError
            If ``subject_strategy`` is anything other than
            ``"topic_name"``.
        """
        # Even though ``Literal["topic_name"]`` enforces this at the type
        # level, we double-check at runtime so callers that bypass the
        # type-checker (e.g., dynamic kwargs) get an immediate, clear
        # failure instead of a confusing downstream Confluent error.
        if subject_strategy != _SUBJECT_STRATEGY_TOPIC_NAME:
            raise ValueError(
                f"only subject_strategy='{_SUBJECT_STRATEGY_TOPIC_NAME}' "
                f"is supported, got {subject_strategy!r}"
            )

        self._schema_registry: SchemaRegistryClient = schema_registry
        self._subject_strategy: str = subject_strategy
        # Per-topic JSONSerializer cache. Empty at construction; populated
        # lazily on first call to :meth:`serialize` for each topic.
        self._serializer_cache: dict[str, JSONSerializer] = {}
        self._log: logging.Logger = logging.getLogger(_LOGGER_NAME)

    # ------------------------------------------------------------------
    # Public API -- the only method callers should depend on.
    # ------------------------------------------------------------------

    def serialize(self, topic: str, payload: dict[str, Any]) -> bytes:
        """Validate ``payload`` against the registered schema and return
        wire-format bytes.

        The returned ``bytes`` are formatted as a Confluent Schema
        Registry wire-format message:

        ::

            +--------+------------------+-----------------+
            | 0x00   | schema_id        | JSON payload    |
            | 1 byte | 4 bytes (BE u32) | UTF-8 encoded   |
            +--------+------------------+-----------------+

        which downstream Schema-Registry-aware deserializers can
        unpack to recover both the schema id and the JSON body.

        Parameters
        ----------
        topic
            Kafka topic name (e.g., ``"product.created"`` or
            ``"product.updated"``). The subject queried in Schema
            Registry is ``f"{topic}-value"`` (TopicNameStrategy).
            MUST be a non-empty string.
        payload
            JSON-serialisable ``dict`` matching the registered schema.
            All values must already be primitive JSON types -- UUIDs as
            strings, ``datetime`` instances as RFC 3339 strings, and
            ``Decimal`` instances as integers or strings -- which is
            precisely the output of Pydantic v2's
            ``model_dump(mode="json")``.

        Returns
        -------
        bytes
            Confluent wire-format bytes. Never empty; never ``None``.

        Raises
        ------
        ValueError
            If ``topic`` is empty.
        confluent_kafka.serialization.SerializationError
            If the payload does not match the registered schema. The
            caller (typically :class:`EventProducer`) treats this as a
            fatal, non-retryable error.
        confluent_kafka.schema_registry.error.SchemaRegistryError
            If Schema Registry is unreachable or the subject for
            ``topic`` is not registered. The caller's retry / circuit
            breaker policy applies.
        Exception
            Any other unexpected exception raised by the underlying
            :class:`JSONSerializer` is logged and re-raised verbatim.
        """
        # Defensive validation -- a Confluent JSONSerializer with an empty
        # topic would generate a meaningless subject (``"-value"``) and
        # confuse Schema Registry. Fail fast with a clear message.
        if not topic:
            raise ValueError("topic must be a non-empty string")

        serializer = self._get_or_build_serializer(topic)

        # SerializationContext carries the topic name and the message
        # field (KEY vs VALUE). We always serialise to the value field;
        # keys are produced by the EventProducer using a separate
        # StringSerializer. The context is intentionally constructed
        # per-call (NOT cached) because Confluent reserves the right to
        # mutate it in future versions.
        ctx = SerializationContext(topic, MessageField.VALUE)

        try:
            wire_bytes = serializer(payload, ctx)
        except SerializationError:
            # Schema validation failure -- the payload's shape does not
            # match the registered JSON Schema. This is a programming
            # bug (envelope model out of sync with registered schema)
            # and MUST NOT be retried. ``logger.exception`` includes the
            # full traceback at ERROR level for AAP R-26 compliance.
            self._log.exception(
                "events.schemas.serialization_failed",
                extra={"topic": topic},
            )
            raise
        except Exception:
            # Any other failure -- most commonly
            # SchemaRegistryError when the registry is unreachable, but
            # also any unexpected exception bubbling up from Confluent's
            # internals. Log and re-raise so the producer's retry /
            # circuit-breaker logic can apply (network errors are
            # retryable, validation errors are not -- the distinction
            # is preserved by the explicit ``except SerializationError``
            # clause above).
            self._log.exception(
                "events.schemas.serializer_error",
                extra={"topic": topic},
            )
            raise

        if wire_bytes is None:
            # Defensive: Confluent's JSONSerializer returns ``None`` only
            # for ``None`` payloads (which represent Kafka null values).
            # Product Service never produces null events; receiving
            # ``None`` here indicates a contract violation upstream and
            # is escalated as a SerializationError so the EventProducer
            # treats it as fatal/non-retryable.
            raise SerializationError(
                f"serializer returned None for non-null payload on "
                f"topic {topic!r}"
            )
        return wire_bytes

    # ------------------------------------------------------------------
    # Internal helpers -- not part of the public API.
    # ------------------------------------------------------------------

    def _get_or_build_serializer(self, topic: str) -> JSONSerializer:
        """Return the cached :class:`JSONSerializer` for ``topic``, building it on miss.

        The serializer is configured uniformly via
        :data:`_SERIALIZER_CONF`:

        * ``auto.register.schemas=False`` -- schemas are registered
          out-of-band by the platform team.
        * ``use.latest.version=True`` -- always serialise against the
          newest registered version.
        * ``subject.name.strategy`` -- TopicNameStrategy (Confluent's
          default; subject = ``f"{topic}-value"``).

        Parameters
        ----------
        topic
            Kafka topic name keying the cache.

        Returns
        -------
        JSONSerializer
            Cached or newly-constructed serializer for the topic.
        """
        cached = self._serializer_cache.get(topic)
        if cached is not None:
            return cached

        # Confluent's JSONSerializer signature
        # (confluent-kafka >= 2.10 supports schema_str=None, requiring
        # only the registry client + use.latest.version=True to fetch
        # the schema lazily on first :meth:`__call__`):
        #
        #     JSONSerializer(schema_str, schema_registry_client,
        #                    to_dict=None, conf=None)
        #
        # We pass ``schema_str=None`` because we want the serializer to
        # FETCH the schema from the registry (use.latest.version=True
        # guarantees the newest version is used). ``to_dict`` is left
        # at its default (``None``) because our payloads are already
        # plain dicts -- Pydantic v2 envelope models are dumped via
        # ``model_dump(mode="json")`` BEFORE this method is invoked.
        #
        # IMPORTANT: a fresh ``dict(_SERIALIZER_CONF)`` is supplied per
        # serializer because Confluent's ``__init__`` mutates the dict
        # via ``conf_copy.pop(...)``. Passing the same dict reference to
        # multiple constructions would empty it after the first call
        # and trigger ``ValueError`` thereafter.
        #
        # Explicit type annotation is needed because confluent-kafka does
        # not ship type stubs; without it ``mypy --strict`` treats the
        # constructor result as ``Any`` and flags the subsequent
        # ``return serializer`` as ``no-any-return``.
        serializer: JSONSerializer = JSONSerializer(
            schema_str=None,
            schema_registry_client=self._schema_registry,
            conf=dict(_SERIALIZER_CONF),
        )

        # Cache the serializer for reuse. Concurrent callers from
        # different threads may both arrive here for the same topic;
        # the dict assignment is atomic under the GIL, so the cache
        # never becomes corrupt -- at worst we discard a duplicate
        # serializer instance. Explicit locking would be premature
        # optimisation for this rare race.
        self._serializer_cache[topic] = serializer

        # AAP R-26 -- structured DEBUG log for cache misses so operators
        # can correlate first-event latency with the Schema Registry
        # round-trip.
        self._log.debug(
            "events.schemas.serializer_built",
            extra={
                "topic": topic,
                "subject": f"{topic}{_VALUE_SUBJECT_SUFFIX}",
            },
        )
        return serializer


# =============================================================================
# Public surface declaration
# =============================================================================
#
# Only :class:`SchemaSerializer` is exported. Module-level constants
# (``_LOGGER_NAME``, ``_SUBJECT_STRATEGY_TOPIC_NAME``,
# ``_VALUE_SUBJECT_SUFFIX``, ``_SERIALIZER_CONF``) are private (leading
# underscore) and intentionally NOT part of the public API; they are
# implementation details that may change without notice.
__all__: list[str] = ["SchemaSerializer"]
