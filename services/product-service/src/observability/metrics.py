"""Prometheus metric declarations for the Product Service.

This module declares ALL metrics emitted by the Product Service as
module-level Prometheus client objects. Importing this module
auto-registers every metric against the default
``prometheus_client.REGISTRY``, so the ``/metrics`` endpoint (mounted
by ``src/main.py`` via ``prometheus_client.make_asgi_app()``) exposes
them from the very first scrape — no metrics will appear missing
because of lazy registration.

The authoritative metric catalogue is defined in the Product Service
``README.md`` *Observability* section and the folder spec for
``src/observability/``. Concretely:

* ``http_requests_total{route,method,status}``                 — Counter
* ``http_request_latency_ms{route,method}``                    — Histogram
* ``mongo_operations_total{operation,collection,status}``      — Counter
* ``mongo_operation_latency_ms{operation,collection}``         — Histogram
* ``kafka_producer_send_total{topic,status}``                  — Counter
* ``kafka_producer_circuit_breaker_state{breaker_name}``       — Gauge
* ``dlq_depth{topic}``                                         — Gauge
* ``catalog_size{collection}``                                 — Gauge
* ``category_tree_cache_hits_total``                           — Counter
* ``category_tree_cache_misses_total``                         — Counter

All metrics in this module are designed to be **PII-free**. Labels
contain only low-cardinality dimensions: HTTP route TEMPLATES (e.g.,
``/api/v1/products/{id}``) and methods, HTTP status codes, MongoDB
collection and operation names, Kafka topic names, circuit breaker
names, and ``success``/``error`` status flags. Per-instance
identifiers (``user_id``, ``product_id``, ``correlation_id`` …) belong
on log lines and OpenTelemetry traces — NEVER on metrics, where they
would explode Prometheus' memory and time-series storage.

Side-effect contract: this module performs ZERO side effects at
import time beyond metric instantiation. It does NOT read environment
variables, acquire loggers, perform I/O, issue network calls, call
``print``, or eagerly bind labels via ``metric.labels(...)`` at
module level. Callers bind labels lazily at the call site. This pure-
leaf-module discipline lets unit tests ``import metrics`` without
needing Settings, container, or any other infrastructure.

Naming conventions:

* ``snake_case`` Prometheus metric names (the strings passed to the
  Counter/Gauge/Histogram constructors).
* ``UPPER_SNAKE_CASE`` Python variable names — module-level constants,
  ``Final``-qualified.
* ``_total`` suffix on every Counter (Prometheus convention).
* ``_ms`` suffix when units are milliseconds — the README mandates ms,
  not seconds; cross-service consistency with sibling Python services
  (inventory-service, order-service) keeps Kibana dashboards portable.
* No ``service`` label — the scrape config supplies
  ``service.name="product-service"`` as an external dimension.

AAP cross-references:

* AAP R-26          — Structured JSON logs (handled by ``logging_config.py``).
* AAP R-27          — Metricbeat / Prometheus scrapes the ``/metrics``
                       endpoint and forwards through Logstash to ELK.
* AAP R-28          — Per-domain Kibana dashboards consume these metrics.
* AAP Section 0.5.2.4 — ELK + Prometheus observability stack.
"""

from __future__ import annotations

from typing import Final

from prometheus_client import REGISTRY, Counter, Gauge, Histogram

# ``REGISTRY`` is rebound to ``_DEFAULT_REGISTRY`` so the import survives
# unused-import linting. Each metric singleton constructed below auto-
# registers against this default REGISTRY (no ``registry=`` kwarg is
# passed); this is exactly the registry that
# ``prometheus_client.make_asgi_app()`` (called with no arguments in
# ``src/main.py``) scrapes when ``/metrics`` is hit.
_DEFAULT_REGISTRY: Final = REGISTRY


# ---------------------------------------------------------------------------
# Histogram bucket constants (TUPLES — immutable so callers cannot mutate
# them and silently invalidate every Histogram that bound the same tuple).
# Aligned with sibling Python services so cross-service Kibana dashboards
# work without re-bucketing.
# ---------------------------------------------------------------------------

#: Histogram buckets for HTTP request latency in milliseconds. Spans
#: 1ms → 10s; denser in the 1-100ms band where most catalog reads land.
#: Upper buckets capture worst-case fall-back paths (e.g., JWKS fetch).
HTTP_LATENCY_BUCKETS_MS: Final[tuple[float, ...]] = (
    1.0, 5.0, 10.0, 25.0, 50.0,
    100.0, 250.0, 500.0, 1000.0,
    2500.0, 5000.0, 10000.0,
)

#: Histogram buckets for MongoDB operation latency in milliseconds.
#: Spans 0.5ms → 5s. Steady-state ops are sub-100ms on warm connections;
#: upper buckets capture pathological tail events (driver retries,
#: replica-set step-downs, cold-start cache misses).
MONGO_LATENCY_BUCKETS_MS: Final[tuple[float, ...]] = (
    0.5, 1.0, 2.5, 5.0, 10.0,
    25.0, 50.0, 100.0, 250.0,
    500.0, 1000.0, 5000.0,
)


# ---------------------------------------------------------------------------
# Circuit breaker state constants
# ---------------------------------------------------------------------------
# Gauge value mapping for ``kafka_producer_circuit_breaker_state``:
#   0 -> CLOSED    (normal operation; requests flow)
#   1 -> HALF_OPEN (probing recovery; few requests permitted)
#   2 -> OPEN      (failing fast; requests rejected immediately)
# Encoded as integers (not labels) because Prometheus gauges are
# numeric and the state is naturally ordinal (0 = healthy, 2 =
# unhealthy). PromQL alerts can use ``... >= 1`` to detect any non-
# CLOSED state across all breakers.
BREAKER_STATE_CLOSED: Final[int] = 0
BREAKER_STATE_HALF_OPEN: Final[int] = 1
BREAKER_STATE_OPEN: Final[int] = 2

#: Mapping from breaker state name (lowercase, normalized) to gauge
#: value. Internal — used only by :func:`breaker_state_value`. Accepts
#: the state-name conventions used by ``pybreaker`` (the breaker
#: library pinned in ``requirements.txt``) plus underscore / hyphen /
#: no-separator spelling variations of "half open".
_BREAKER_STATE_VALUES: Final[dict[str, int]] = {
    "closed": BREAKER_STATE_CLOSED,
    "half_open": BREAKER_STATE_HALF_OPEN,
    "half-open": BREAKER_STATE_HALF_OPEN,
    "halfopen": BREAKER_STATE_HALF_OPEN,
    "open": BREAKER_STATE_OPEN,
}


# ---------------------------------------------------------------------------
# HTTP metrics (incremented by StructuredLoggingMiddleware per request)
# ---------------------------------------------------------------------------

#: Counter incrementing once per HTTP request handled by the service.
#: Labels (LOW-CARDINALITY):
#:
#:   - ``route``  — FastAPI route TEMPLATE (e.g. ``/api/v1/products/{id}``,
#:                  NOT the resolved path ``/api/v1/products/42``).
#:   - ``method`` — HTTP method, uppercase (``GET``, ``POST``, ...).
#:   - ``status`` — HTTP response status as a string (``"200"``, ``"500"``).
#:
#: PromQL — request rate per route::
#:     sum by (route) (rate(http_requests_total[5m]))
#: Error rate::
#:     sum(rate(http_requests_total{status=~"5.."}[5m]))
#:       / sum(rate(http_requests_total[5m]))
HTTP_REQUESTS_TOTAL: Final[Counter] = Counter(
    "http_requests_total",
    "Total HTTP requests handled by the Product Service.",
    labelnames=("route", "method", "status"),
)

#: Histogram tracking HTTP request latency in milliseconds. Labels are
#: ``route`` and ``method`` only — ``status`` is intentionally omitted
#: to keep cardinality bounded (per-status latency is rarely useful and
#: would inflate time-series count by ~10x).
#:
#: PromQL — p95 latency per route::
#:     histogram_quantile(0.95,
#:       sum by (le, route) (rate(http_request_latency_ms_bucket[5m])))
HTTP_REQUEST_LATENCY_MS: Final[Histogram] = Histogram(
    "http_request_latency_ms",
    "HTTP request latency in milliseconds.",
    labelnames=("route", "method"),
    buckets=HTTP_LATENCY_BUCKETS_MS,
)


# ---------------------------------------------------------------------------
# MongoDB metrics (incremented by repository methods)
# ---------------------------------------------------------------------------

#: Counter incrementing once per MongoDB operation issued by a
#: repository. Labels:
#:
#:   - ``operation``  — bounded enum: ``find``, ``find_one``,
#:                      ``insert_one``, ``insert_many``, ``update_one``,
#:                      ``update_many``, ``delete_one``, ``delete_many``,
#:                      ``count_documents``, ``aggregate``,
#:                      ``find_with_pagination``.
#:   - ``collection`` — one of ``products``, ``categories``,
#:                      ``product_media`` (the only three collections
#:                      used by Product Service per AAP Section 0.4.4).
#:   - ``status``     — ``success`` or ``error``.
#:
#: PromQL — error rate per collection::
#:     sum by (collection) (
#:       rate(mongo_operations_total{status="error"}[5m]))
MONGO_OPERATIONS_TOTAL: Final[Counter] = Counter(
    "mongo_operations_total",
    "Total MongoDB operations issued by Product Service repositories.",
    labelnames=("operation", "collection", "status"),
)

#: Histogram tracking MongoDB operation latency in milliseconds. Labels
#: are ``operation`` and ``collection`` (same sets as
#: :data:`MONGO_OPERATIONS_TOTAL`); ``status`` is omitted for the same
#: cardinality reason as HTTP latency.
#:
#: PromQL — p99 latency for find_one on products::
#:     histogram_quantile(0.99,
#:       sum by (le) (rate(mongo_operation_latency_ms_bucket{
#:         operation="find_one", collection="products"}[5m])))
MONGO_OPERATION_LATENCY_MS: Final[Histogram] = Histogram(
    "mongo_operation_latency_ms",
    "MongoDB operation latency in milliseconds.",
    labelnames=("operation", "collection"),
    buckets=MONGO_LATENCY_BUCKETS_MS,
)


# ---------------------------------------------------------------------------
# Kafka producer metrics (incremented by EventProducer / EventPublisher)
# ---------------------------------------------------------------------------

#: Counter incrementing once per Kafka ``producer.send()`` outcome.
#: Labels:
#:
#:   - ``topic``  — target topic name. Product Service produces ONLY
#:                  to ``product.created``, ``product.updated``,
#:                  ``product.created.dlq``, ``product.updated.dlq``
#:                  (per AAP Section 0.4.2 / R-30).
#:   - ``status`` — ``success`` (broker ack received) or ``error``
#:                  (delivery failed after all retries).
#:
#: NOTE: messages routed to a DLQ topic still increment THIS counter
#: with their DESTINATION topic in the ``topic`` label. The
#: :data:`DLQ_DEPTH` gauge separately tracks DLQ population.
#:
#: PromQL — produce error rate (alertable)::
#:     sum(rate(kafka_producer_send_total{status="error"}[5m]))
KAFKA_PRODUCER_SEND_TOTAL: Final[Counter] = Counter(
    "kafka_producer_send_total",
    "Total Kafka producer send outcomes by topic and status.",
    labelnames=("topic", "status"),
)

#: Gauge representing the current state of a circuit breaker
#: protecting an outbound dependency. Values: 0=CLOSED, 1=HALF_OPEN,
#: 2=OPEN (see :data:`BREAKER_STATE_CLOSED` etc.).
#:
#: Label:
#:
#:   - ``breaker_name`` — identifier of the breaker. Product Service
#:                        has TWO: ``auth_service_breaker`` (protecting
#:                        JWKS fetches per AAP R-22) and
#:                        ``kafka_producer_breaker`` (protecting Kafka
#:                        produce). Both are wired in
#:                        ``src/container.py``.
#:
#: NOTE on the metric NAME: the README catalogue specifies the metric
#: name as ``kafka_producer_circuit_breaker_state`` because it was
#: originally introduced for the Kafka producer breaker. The
#: ``breaker_name`` label generalizes it to ANY breaker in the service
#: (matching the labelled form documented in the folder spec); both
#: breakers emit on the same metric series and Kibana panels filter by
#: ``breaker_name``.
#:
#: PromQL — alert: any breaker open for 5m::
#:     max_over_time(kafka_producer_circuit_breaker_state[5m]) == 2
KAFKA_PRODUCER_CIRCUIT_BREAKER_STATE: Final[Gauge] = Gauge(
    "kafka_producer_circuit_breaker_state",
    "Circuit breaker state by name. 0=CLOSED, 1=HALF_OPEN, 2=OPEN.",
    labelnames=("breaker_name",),
)


# ---------------------------------------------------------------------------
# DLQ depth metric (incremented by EventProducer DLQ-routing path)
# ---------------------------------------------------------------------------

#: Gauge tracking how many messages have been routed to each DLQ topic
#: since process startup. Implemented as a Gauge (NOT a Counter) per
#: the README catalogue — operators want a snapshot ("current depth"),
#: not a rate-of-growth. A periodic compaction or DLQ-replay task could
#: decrement this gauge if/when DLQ messages are reprocessed.
#:
#: Label:
#:
#:   - ``topic`` — DLQ topic name (e.g., ``product.created.dlq``,
#:                 ``product.updated.dlq`` per AAP Section 0.4.2).
#:
#: PromQL — alertable on any DLQ growth in the last hour::
#:     max_over_time(dlq_depth[1h]) - min_over_time(dlq_depth[1h]) > 0
DLQ_DEPTH: Final[Gauge] = Gauge(
    "dlq_depth",
    "Number of messages routed to each DLQ topic since startup.",
    labelnames=("topic",),
)


# ---------------------------------------------------------------------------
# Catalog size metric (refreshed periodically by readiness/healthcheck)
# ---------------------------------------------------------------------------

#: Gauge tracking the number of documents per MongoDB collection.
#: Refreshed periodically by the readiness probe handler (or a
#: scheduled background task) calling ``count_documents({})`` on each
#: collection. A Gauge — not a Counter — because operators want a
#: snapshot ("how big is the catalog right now?"), not a rate.
#:
#: Label:
#:
#:   - ``collection`` — one of ``products``, ``categories``,
#:                      ``product_media`` (matching
#:                      :data:`MONGO_OPERATIONS_TOTAL`'s ``collection``
#:                      labels for cross-metric correlation).
CATALOG_SIZE: Final[Gauge] = Gauge(
    "catalog_size",
    "Number of documents per MongoDB collection.",
    labelnames=("collection",),
)


# ---------------------------------------------------------------------------
# Category tree LRU cache effectiveness (folder spec addition)
# ---------------------------------------------------------------------------

#: Counter incrementing on every CategoryTreeCache hit (the LRU
#: returned a cached value). Read alongside
#: :data:`CATEGORY_TREE_CACHE_MISSES_TOTAL` to compute hit ratio in
#: PromQL: ``rate(hits) / (rate(hits) + rate(misses))``.
#:
#: NO LABELS — the metric is service-wide. The cache is a single
#: process-global TTLCache (per AAP R-7 the catalog domain uses an in-
#: process cache rather than introducing Redis). Per-key labels would
#: create unbounded cardinality and are explicitly disallowed.
CATEGORY_TREE_CACHE_HITS_TOTAL: Final[Counter] = Counter(
    "category_tree_cache_hits_total",
    "Total category tree LRU cache hits.",
)

#: Counter incrementing on every CategoryTreeCache miss (the LRU did
#: NOT have the requested entry; the service had to query MongoDB).
#: NO LABELS — see :data:`CATEGORY_TREE_CACHE_HITS_TOTAL`.
CATEGORY_TREE_CACHE_MISSES_TOTAL: Final[Counter] = Counter(
    "category_tree_cache_misses_total",
    "Total category tree LRU cache misses.",
)


# ---------------------------------------------------------------------------
# Helper: breaker state name -> gauge value
# ---------------------------------------------------------------------------


def breaker_state_value(state_name: str) -> int:
    """Map a circuit breaker state name to its gauge integer value.

    Accepts the state-name conventions used by common Python circuit
    breaker libraries (``pybreaker`` uses ``"closed"``/``"open"``/
    ``"half_open"``). Lookup is case-insensitive and tolerant of minor
    spelling variations (``half_open``, ``half-open``, ``halfopen``).
    Unknown names raise :class:`ValueError` so callers see a clear
    error rather than the helper silently emitting ``0`` (a misleading
    "healthy" reading that would mask an unrecognized failure mode).

    Used by ``src/container.py`` and the resilience callbacks wired
    around ``pybreaker.CircuitBreaker`` instances to translate breaker-
    state-changed callbacks into a numeric gauge value suitable for
    ``KAFKA_PRODUCER_CIRCUIT_BREAKER_STATE
    .labels(breaker_name=...).set(...)``.

    Args:
        state_name: Breaker state name. Case-insensitive; surrounding
            whitespace is stripped before lookup. Accepted values
            (after normalization): ``closed``, ``half_open``,
            ``half-open``, ``halfopen``, ``open``.

    Returns:
        The gauge integer value: :data:`BREAKER_STATE_CLOSED` (0),
        :data:`BREAKER_STATE_HALF_OPEN` (1), or
        :data:`BREAKER_STATE_OPEN` (2).

    Raises:
        ValueError: If ``state_name`` is not one of the recognized
            values after normalization.
    """
    normalized = state_name.strip().lower()
    if normalized in _BREAKER_STATE_VALUES:
        return _BREAKER_STATE_VALUES[normalized]
    raise ValueError(
        f"Unknown circuit breaker state: {state_name!r}. "
        "Expected one of: closed, half_open, half-open, halfopen, open."
    )


# ---------------------------------------------------------------------------
# Public surface — explicit ``__all__`` documents the module's exports.
# ---------------------------------------------------------------------------
__all__: list[str] = [
    # Histogram bucket constants
    "HTTP_LATENCY_BUCKETS_MS",
    "MONGO_LATENCY_BUCKETS_MS",
    # Circuit breaker state constants + helper
    "BREAKER_STATE_CLOSED",
    "BREAKER_STATE_HALF_OPEN",
    "BREAKER_STATE_OPEN",
    "breaker_state_value",
    # HTTP metrics
    "HTTP_REQUESTS_TOTAL",
    "HTTP_REQUEST_LATENCY_MS",
    # MongoDB metrics
    "MONGO_OPERATIONS_TOTAL",
    "MONGO_OPERATION_LATENCY_MS",
    # Kafka metrics
    "KAFKA_PRODUCER_SEND_TOTAL",
    "KAFKA_PRODUCER_CIRCUIT_BREAKER_STATE",
    # DLQ metric
    "DLQ_DEPTH",
    # Catalog metric
    "CATALOG_SIZE",
    # Cache metrics
    "CATEGORY_TREE_CACHE_HITS_TOTAL",
    "CATEGORY_TREE_CACHE_MISSES_TOTAL",
]
