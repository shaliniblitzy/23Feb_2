"""Prometheus metric registry — single source of truth for all metrics
emitted by the Inventory Service.

This module materialises the entire Prometheus metric catalogue used by
this service.  Every metric is instantiated against the default global
``prometheus_client.REGISTRY``; the ASGI endpoint mounted at ``/metrics``
in :func:`src.main.create_app` (via :func:`prometheus_client.make_asgi_app`)
scrapes that registry directly, so callers gain visibility by importing
the relevant constant — there is no separate registration step.

Metric grouping
---------------
* **README authoritative metrics (8).**  Defined verbatim per the parent
  service ``README.md`` section 1.17 *Observability* — these names form
  the canonical service-level contract that the Kibana inventory
  dashboard, alerts, and Elasticsearch/Grafana queries reference.  They
  populate the request-volume, p95-latency, error-rate, Kafka
  consumer-lag, and DLQ-depth panels mandated by AAP R-28.

  - ``reservations_created_total``   — Counter ``{warehouse, outcome}``
  - ``reservations_released_total``  — Counter ``{warehouse, reason}``
  - ``reservation_latency_ms``       — Histogram ``{operation}``
  - ``low_stock_events_total``       — Counter ``{warehouse}``
  - ``stock_optimistic_lock_retries_total`` — Counter ``{operation}``
  - ``expired_reservations_total``   — Counter (no labels; alertable)
  - ``kafka_consumer_lag``           — Gauge ``{topic}``
  - ``dlq_depth``                    — Gauge ``{topic}``

* **Folder-spec supplementary metric (1).**  Adds operational visibility
  into the in-process expiry scheduler (AAP R-20 deadline-driven safety
  net) so operators can distinguish "scheduler is healthy and there is
  no work" from "scheduler is stuck and silently producing zero output".

  - ``expiry_scheduler_runs_total``  — Counter ``{result}``

Side-effect contract
--------------------
This module performs ZERO side effects at import time beyond metric
instantiation (which, by design, registers each metric on the default
``REGISTRY``).  In particular this module:

* does NOT read environment variables (``os.environ`` / ``os.getenv``),
* does NOT acquire loggers (``logging.getLogger(...)``),
* does NOT perform any file I/O,
* does NOT issue network calls,
* does NOT call ``print(...)``,
* does NOT define an ``if __name__ == "__main__":`` block,
* does NOT eagerly bind labels via ``metric.labels(...)`` at module level
  — callers bind labels lazily at the call site.

Each metric singleton is created exactly once (Python caches the module
after the first import); subsequent imports are free.

Cardinality discipline (forbidden labels)
-----------------------------------------
Prometheus's TSDB explodes when label cardinality is unbounded; the
following labels are **never** added to any metric in this module:
``order_id``, ``user_id``, ``reservation_id``, ``correlation_id``,
``product_id``.  Those identifiers belong on log lines and traces (the
ELK pipeline plus OpenTelemetry trace context) — never on metrics.

The labels that ARE used here are bounded by inherent business
cardinality:

* ``warehouse``  — bounded by rows in the ``warehouses`` table (typ. <100).
* ``outcome``    — fixed enum: ``success``, ``insufficient_stock``.
* ``reason``     — fixed enum: ``order_cancelled``, ``order_fulfilled``, ``expired``.
* ``operation``  — fixed enum: ``reserve``, ``release``, ``finalize``, ``decrement``, ``increment``.
* ``topic``      — bounded by the small set of Kafka topics this service
                   consumes/produces (incl. ``.retry`` and ``.dlq``
                   companions per AAP R-17).
* ``result``     — fixed enum: ``success``, ``failure``.

Naming conventions
------------------
* ``snake_case`` metric names.
* ``_total`` suffix on every Counter (Prometheus convention).
* ``_ms`` suffix when units are milliseconds (the README mandates ms,
  NOT seconds — mixing units across services breaks Kibana queries).
* No ``service`` label — the Prometheus scrape config supplies
  ``service.name="inventory-service"`` as an external dimension.

AAP cross-references
--------------------
* AAP R-15 / R-16 — Resilience: retry counters and circuit-breaker state
  observability are surfaced through ``stock_optimistic_lock_retries_total``
  and (indirectly, via consumer DLQ routing) ``dlq_depth``.
* AAP R-17        — Every consumed topic has a ``.retry`` and ``.dlq``
  companion; ``kafka_consumer_lag`` and ``dlq_depth`` expose them.
* AAP R-20        — Reservation deadline safety net surfaced through
  ``expired_reservations_total`` (alertable) and the supplementary
  ``expiry_scheduler_runs_total`` health counter.
* AAP R-27        — Metricbeat / Prometheus scrapes the ``/metrics``
  ASGI endpoint exposed by ``src.main.create_app`` and forwards through
  Logstash to Elasticsearch.
* AAP R-28        — Per-domain Kibana dashboards consume these metrics.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram


# ---------------------------------------------------------------------------
# Histogram bucket constants
# ---------------------------------------------------------------------------
# Reservation operations are sub-100 ms in steady state on warm Postgres
# connections; buckets extend to 10 seconds to capture pathological cases
# (cold-start, network blip, optimistic-lock retry storm).  Units:
# milliseconds.  The constant is a tuple (NOT a list) so it cannot be
# mutated by a caller — accidental mutation would silently break metric
# registration on subsequent imports.
RESERVATION_LATENCY_BUCKETS_MS: tuple[float, ...] = (
    1.0,
    5.0,
    10.0,
    25.0,
    50.0,
    100.0,
    250.0,
    500.0,
    1000.0,
    2500.0,
    5000.0,
    10000.0,
)


# ===========================================================================
# README authoritative metrics (section 1.17 Observability) — 8 metrics
# ===========================================================================

# --- Reservation Lifecycle Counters ----------------------------------------
# Counts reservation creation outcomes per warehouse.  Emitted by
# ``src/events/handlers/order_created.py`` and the warehouse adapter layer.
# Labels:
#   - warehouse: warehouse identifier (e.g. ``wh_us_east_1``).  LOW
#                cardinality, bounded by rows in the ``warehouses`` table.
#   - outcome:   ``success`` (reservation row inserted) or
#                ``insufficient_stock`` (request rejected because
#                ``available_qty < requested_qty``).
reservations_created_total: Counter = Counter(
    "reservations_created_total",
    "Reservations created or rejected, by warehouse and outcome.",
    ["warehouse", "outcome"],
)

# Counts reservation release outcomes per warehouse and reason.  Emitted
# by ``src/events/handlers/order_cancelled.py``,
# ``src/events/handlers/order_fulfilled.py``, and the expiry scheduler in
# ``src/scheduler/expiry_scheduler.py``.
# Labels:
#   - warehouse: warehouse identifier.  LOW cardinality.
#   - reason:    ``order_cancelled`` (saga compensation),
#                ``order_fulfilled`` (reservation converted to commitment),
#                or ``expired`` (deadline reached without saga progression
#                — AAP R-20 fallback path).
reservations_released_total: Counter = Counter(
    "reservations_released_total",
    "Reservations released, by warehouse and release reason.",
    ["warehouse", "reason"],
)

# --- Reservation Latency Histogram -----------------------------------------
# Tracks end-to-end latency of reservation operations.  Sampled around the
# repository transaction boundary (acquire connection → execute SQL →
# commit), so it captures the dominant time component for both the
# happy path and optimistic-lock retry paths.  Used by the Kibana
# dashboard to compute p50 / p95 / p99 latency.
# Labels:
#   - operation: ``reserve`` (``order.created`` handler), ``release``
#                (``order.cancelled`` / scheduler), or ``finalize``
#                (``order.fulfilled``).
# Units: milliseconds — convert ``time.perf_counter()`` deltas via
# ``* 1000.0`` before ``observe(...)``.
reservation_latency_ms: Histogram = Histogram(
    "reservation_latency_ms",
    "Latency of reservation operations in milliseconds, by operation type.",
    ["operation"],
    buckets=RESERVATION_LATENCY_BUCKETS_MS,
)

# --- Low-Stock Signal Counter ----------------------------------------------
# Counts emissions of ``inventory.low-stock`` events to Kafka.  Each
# emission represents a SKU dropping below its ``low_stock_threshold``
# inside the named warehouse.  Emitted by the warehouse adapter
# immediately after a stock decrement that crosses the threshold.
# Labels:
#   - warehouse: warehouse identifier.  LOW cardinality.
low_stock_events_total: Counter = Counter(
    "low_stock_events_total",
    "Low-stock events emitted to Kafka, by warehouse.",
    ["warehouse"],
)

# --- Stock Optimistic-Lock Retry Counter (Alertable) -----------------------
# Counts optimistic-lock conflict retries against the
# ``stock_items.version`` discriminator.  A small steady-state value is
# normal under any contention; a sustained nonzero rate indicates hot
# rows that may need partition tuning, hashing across more
# ``(product_id, warehouse_id)`` partitions, or upgrading to pessimistic
# row locking.  The README marks this metric as alertable.
# Labels:
#   - operation: ``decrement`` (reserve / fulfill — taking stock) or
#                ``increment`` (release / restock — returning stock).
stock_optimistic_lock_retries_total: Counter = Counter(
    "stock_optimistic_lock_retries_total",
    "Optimistic-lock retry attempts on stock_items rows, by operation.",
    ["operation"],
)

# --- Expired Reservations Counter (Alertable) ------------------------------
# Counts reservations released by the expiry scheduler because the saga
# never progressed past the reservation step within the configured
# ``RESERVATION_EXPIRY_MS`` deadline.  Sustained nonzero indicates
# upstream saga failures (typically Order Service or Payment Service
# stuck) that the deadline-driven release is silently masking.  The
# README marks this metric as alertable per section 1.17.  No labels —
# this is a global service-level counter; root-cause attribution lives
# in logs/traces, not in metric labels.
expired_reservations_total: Counter = Counter(
    "expired_reservations_total",
    "Reservations released by the expiry scheduler due to deadline reached "
    "without saga progression (AAP R-20 fallback path).",
)

# --- Kafka Consumer Lag Gauge ----------------------------------------------
# Tracks per-topic consumer-group lag in messages.  Sampled by the Kafka
# consumer's main loop in ``src/events/consumer.py`` using
# ``Consumer.committed()`` / ``Consumer.position()`` deltas (or the
# AdminClient end-offset query when committed offsets are unavailable).
# Powers the Kibana dashboard's consumer-lag panel.
# Labels:
#   - topic: Kafka topic name (e.g. ``order.created``,
#            ``order.cancelled``, ``order.fulfilled``, plus their
#            ``.retry`` companions per AAP R-17).
kafka_consumer_lag: Gauge = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag in messages, by topic.",
    ["topic"],
)

# --- DLQ Depth Gauge -------------------------------------------------------
# Tracks the size of dead-letter and retry topics that this service
# produces to or owns.  Sampled by the consumer's loop or by an admin
# job using the Kafka AdminClient end-offset metadata.  Powers the
# Kibana dashboard's DLQ-depth panel; alertable on first nonzero
# arrival per the operational runbook.
# Labels:
#   - topic: DLQ topic name (e.g. ``order.created.dlq``, ``inventory.dlq``).
dlq_depth: Gauge = Gauge(
    "dlq_depth",
    "Dead-letter queue depth in messages, by DLQ topic name.",
    ["topic"],
)


# ===========================================================================
# Folder-spec supplementary metric — 1 metric
# ===========================================================================

# --- Expiry Scheduler Run Counter ------------------------------------------
# Counts expiry scheduler tick executions and their outcomes.  Tracked
# separately from ``expired_reservations_total`` (which counts the
# RESERVATIONS that the scheduler released): this metric tracks scheduler
# HEALTH (did the loop fire?  did the tick complete cleanly?).  Useful
# for alerting on the "scheduler stuck or crashed" failure mode where
# ``expired_reservations_total`` would silently flatline because the
# scheduler is no longer running, masking the underlying outage.
# Labels:
#   - result: ``success`` (tick completed normally — including ticks that
#             found zero expired rows) or ``failure`` (tick raised an
#             exception that the scheduler logged and recovered from).
expiry_scheduler_runs_total: Counter = Counter(
    "expiry_scheduler_runs_total",
    "Expiry scheduler tick executions, by outcome.",
    ["result"],
)


# ---------------------------------------------------------------------------
# Public surface — only metric singletons and the bucket constant are
# re-exported.  Imports remain implementation details.
# ---------------------------------------------------------------------------
__all__: list[str] = [
    # Histogram bucket constants
    "RESERVATION_LATENCY_BUCKETS_MS",
    # README metrics (8)
    "reservations_created_total",
    "reservations_released_total",
    "reservation_latency_ms",
    "low_stock_events_total",
    "stock_optimistic_lock_retries_total",
    "expired_reservations_total",
    "kafka_consumer_lag",
    "dlq_depth",
    # Folder-spec supplementary metric (1)
    "expiry_scheduler_runs_total",
]
