"""Prometheus metric definitions for the User Service.

Defines every metric instrument the User Service publishes on its
``/metrics`` endpoint. Metricbeat scrapes the endpoint per AAP R-27
and ships samples to Elasticsearch; Kibana dashboards in
``infrastructure/elk/kibana/dashboards/`` consume these metric names
verbatim per AAP R-28.

Cardinality discipline (CRITICAL)
---------------------------------
Prometheus label cardinality is the dominant cost of metric storage.
Labels chosen here are deliberately bounded: ``method`` (~7 HTTP
verbs), ``route`` (FastAPI route TEMPLATE such as ``/users/{id}`` --
NEVER a literal URL with embedded IDs; ~12 declared routes),
``status`` (~30 HTTP codes), ``change_kind`` (5-value enum: profile,
preferences, address_added, address_updated, address_removed),
``topic`` (~6 Kafka topics), ``event_type`` (~10 outbox event types),
``status`` for ``jwks_fetch_total`` (enum: success, failure,
cb_open), ``exception_type`` (~15 exception classes), ``name`` for
``circuit_breaker_state`` (~3 breaker names).

High-cardinality identifiers (``user_id``, ``email``, ``phone``,
``correlation_id``) MUST NOT be labels -- they live in structured
logs and trace attributes instead.

ZERO dependency on internal ``src.*`` modules. Side effects are
limited to defining instruments at import time -- no I/O, no
environment reads, no logging, no ``print``.

AAP cross-references: R-17 (DLQ / retry), R-26 (structured logs),
R-27 (Metricbeat scraping), R-28 (Kibana dashboards), Section 0.4.5
(cross-cutting interceptors), Section 0.5.2.4 (ELK backbone),
Section 0.5.2.2 bullet 3 (User Service responsibilities).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    make_asgi_app,
)

if TYPE_CHECKING:
    # FastAPI referenced only as a string-quoted annotation on
    # :func:`mount_prometheus_endpoint`. The TYPE_CHECKING guard keeps
    # this module importable from non-FastAPI contexts (CLI tools).
    from fastapi import FastAPI


# Latency buckets in MILLISECONDS for HTTP request latency. Calibrated
# for User Service CRUD (~5 ms p50 cache hits to ~5 s p99 worst-case
# slow Postgres / JWKS bootstrap). 10 s ceiling is comfortable for a
# profile / preference / address service.
HTTP_LATENCY_BUCKETS_MS: Final[tuple[float, ...]] = (
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
)


# Metric instruments are registered against ``prometheus_client.REGISTRY``
# at module import time and imported by middleware, repositories,
# services, command handlers, the JWKS fetcher, the Kafka consumer
# runner, and the outbox dispatcher. ``Final`` marks the binding
# immutable; sample state is mutable and concurrency-safe.

# ---- HTTP metrics (AAP R-26, R-27) ----------------------------------------
# Updated by ``RequestLoggingMiddleware`` per request. Drives request
# volume + p95 latency tiles on the User Service Kibana dashboard
# (AAP R-28). The ``route`` label is the FastAPI route TEMPLATE.
HTTP_REQUESTS_TOTAL: Final[Counter] = Counter(
    "http_requests_total",
    "Total count of HTTP requests received by the User Service.",
    labelnames=("method", "route", "status"),
)

HTTP_REQUEST_LATENCY_MS: Final[Histogram] = Histogram(
    "http_request_latency_ms",
    "End-to-end HTTP request latency in milliseconds.",
    labelnames=("method", "route"),
    buckets=HTTP_LATENCY_BUCKETS_MS,
)


# ---- Domain event metrics (command handlers) ------------------------------
# Incremented by ``RegisterUserCommandHandler`` on successful
# materialization of a ``user.registered`` Kafka event. Update +
# soft-delete handlers drive the other two counters; ``change_kind``
# is the 5-value enum {profile, preferences, address_added,
# address_updated, address_removed}.
USERS_REGISTERED_TOTAL: Final[Counter] = Counter(
    "users_registered_total",
    "Total count of users created via the user.registered Kafka event.",
)

USERS_UPDATED_TOTAL: Final[Counter] = Counter(
    "users_updated_total",
    "Total count of user-record mutations grouped by change kind.",
    labelnames=("change_kind",),
)

USERS_DELETED_TOTAL: Final[Counter] = Counter(
    "users_deleted_total",
    "Total count of user soft-delete operations.",
)


# ---- Kafka consumer lag (AAP R-17) ----------------------------------------
# ``KafkaConsumerRunner`` periodically samples high-water-mark minus
# last-committed offset per subscribed topic.
KAFKA_CONSUMER_LAG: Final[Gauge] = Gauge(
    "kafka_consumer_lag",
    (
        "Number of messages between the high-water mark and the last "
        "committed offset for each subscribed topic."
    ),
    labelnames=("topic",),
)


# ---- Outbox dispatcher metrics --------------------------------------------
# ``OutboxDispatcher`` updates these each poll: pending rows in
# ``events_outbox`` and counters for successful/failed Kafka publishes.
OUTBOX_PENDING: Final[Gauge] = Gauge(
    "outbox_pending",
    "Number of events_outbox rows pending publish, grouped by event_type.",
    labelnames=("event_type",),
)

OUTBOX_PUBLISHED_TOTAL: Final[Counter] = Counter(
    "outbox_published_total",
    "Total count of outbox events successfully published to Kafka.",
    labelnames=("event_type",),
)

OUTBOX_FAILED_TOTAL: Final[Counter] = Counter(
    "outbox_failed_total",
    "Total count of outbox events that failed to publish to Kafka.",
    labelnames=("event_type",),
)


# ---- Dead-letter queue depth (AAP R-17) -----------------------------------
# Updated when a DLQ produce is observed or via periodic admin-API
# poll. The ``topic`` label carries the full ``*.dlq`` topic name
# (e.g., ``user.registered.dlq``). ANY nonzero depth is operationally
# significant.
DLQ_DEPTH: Final[Gauge] = Gauge(
    "dlq_depth",
    "Approximate depth of dead-letter Kafka topics.",
    labelnames=("topic",),
)


# ---- JWKS fetcher metrics (AAP R-22) --------------------------------------
# ``JWKSFetcher`` increments on each fetch attempt. ``status`` enum:
# success, failure, cb_open (short-circuited; circuit open).
JWKS_FETCH_TOTAL: Final[Counter] = Counter(
    "jwks_fetch_total",
    "Total count of JWKS fetch attempts grouped by outcome.",
    labelnames=("status",),
)


# ---- Repository concurrency metric ----------------------------------------
# ``UserRepository.update_with_version_check`` increments on a stale
# ``version`` column under optimistic-concurrency UPDATE.
OPTIMISTIC_LOCK_CONFLICTS_TOTAL: Final[Counter] = Counter(
    "optimistic_lock_conflicts_total",
    (
        "Total count of optimistic-concurrency conflicts encountered "
        "while updating user records."
    ),
)


# ---- Error metrics --------------------------------------------------------
# ``ErrorHandlerMiddleware`` increments on every caught exception.
# ``exception_type`` is the Python class name; ``status`` is the HTTP
# status code as a string.
ERRORS_TOTAL: Final[Counter] = Counter(
    "errors_total",
    "Total count of errors handled by the global exception handler.",
    labelnames=("exception_type", "status"),
)


# ---- Database pool metrics ------------------------------------------------
# Sampled from psycopg_pool / SQLAlchemy pool stats. Saturation
# (``in_use`` near ``size``) indicates pool starvation.
DB_CONNECTION_POOL_IN_USE: Final[Gauge] = Gauge(
    "db_connection_pool_in_use",
    "Number of database connections currently checked out from the pool.",
)

DB_CONNECTION_POOL_SIZE: Final[Gauge] = Gauge(
    "db_connection_pool_size",
    "Configured maximum size of the database connection pool.",
)


# ---- Circuit breaker state ------------------------------------------------
# Updated by the ``pybreaker`` listener on each named breaker
# (``jwks``, ``kafka``, ``auth_introspect``) on every state
# transition. Encoded as closed=0, half-open=1, open=2 -- exposed via
# the constants below for self-documenting call sites.
CIRCUIT_BREAKER_STATE: Final[Gauge] = Gauge(
    "circuit_breaker_state",
    "State of named circuit breakers; 0=closed, 1=half-open, 2=open.",
    labelnames=("name",),
)
CIRCUIT_BREAKER_STATE_CLOSED: Final[int] = 0
CIRCUIT_BREAKER_STATE_HALF_OPEN: Final[int] = 1
CIRCUIT_BREAKER_STATE_OPEN: Final[int] = 2


# ---- Helpers --------------------------------------------------------------
def mount_prometheus_endpoint(
    app: "FastAPI",
    *,
    metrics_path: str = "/metrics",
) -> None:
    """Mount the Prometheus exposition ASGI app on the given FastAPI app.

    The endpoint exposes the default ``prometheus_client.REGISTRY`` in
    Prometheus text format with the Content-Type advertised by
    :data:`CONTENT_TYPE_LATEST` (re-exported here). The Metricbeat
    scrape config in ``infrastructure/elk/metricbeat/metricbeat.yml``
    targets this path on every User Service replica per AAP R-27.

    Parameters
    ----------
    app:
        The FastAPI application instance returned by ``create_app()``.
    metrics_path:
        Mount path. Defaults to ``/metrics``. Pass
        ``settings.observability.prometheus.metrics_path`` for a
        configurable path.

    Notes
    -----
    Thin wrapper around :func:`prometheus_client.make_asgi_app`. NOT
    idempotent at the FastAPI level: Starlette's ``Router.mount`` will
    raise on duplicate paths, so callers MUST invoke this exactly once
    per app construction. The recommended call site is inside
    ``create_app()`` in ``src/main.py``.
    """
    app.mount(metrics_path, make_asgi_app())


# ---- Public surface -------------------------------------------------------
__all__: list[str] = [
    "HTTP_LATENCY_BUCKETS_MS",
    "HTTP_REQUESTS_TOTAL",
    "HTTP_REQUEST_LATENCY_MS",
    "USERS_REGISTERED_TOTAL",
    "USERS_UPDATED_TOTAL",
    "USERS_DELETED_TOTAL",
    "KAFKA_CONSUMER_LAG",
    "OUTBOX_PENDING",
    "OUTBOX_PUBLISHED_TOTAL",
    "OUTBOX_FAILED_TOTAL",
    "DLQ_DEPTH",
    "JWKS_FETCH_TOTAL",
    "OPTIMISTIC_LOCK_CONFLICTS_TOTAL",
    "ERRORS_TOTAL",
    "DB_CONNECTION_POOL_IN_USE",
    "DB_CONNECTION_POOL_SIZE",
    "CIRCUIT_BREAKER_STATE",
    "CIRCUIT_BREAKER_STATE_CLOSED",
    "CIRCUIT_BREAKER_STATE_HALF_OPEN",
    "CIRCUIT_BREAKER_STATE_OPEN",
    "mount_prometheus_endpoint",
    "CONTENT_TYPE_LATEST",
    "REGISTRY",
]
