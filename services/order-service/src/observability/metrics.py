"""Prometheus metric definitions for the Order Service.

SINGLE SOURCE OF TRUTH for every Prometheus metric emitted by the
Order Service. Each metric registers with the default
``prometheus_client.REGISTRY`` at import time so the FastAPI
``/metrics`` endpoint mounted in :mod:`src.main` via
``prometheus_client.make_asgi_app()`` exposes them automatically.

AAP rules satisfied:
    * R-17 — ``kafka_consumer_lag`` and ``dlq_depth`` surface the
      retry/DLQ topology per topic.
    * R-18 — Five saga metrics operationalize the saga coordinator;
      UNIQUE to the Order Service (no sibling runs a saga state machine).
    * R-27 — Metrics scraped by Metricbeat / Prometheus from
      ``/metrics`` and shipped to Elasticsearch via Logstash.
    * R-28 — Drives the Kibana Order-domain dashboard (request volume,
      p95 latency, error rate, consumer lag, DLQ depth, saga rates).

Conventions: ``snake_case``; ``_total`` for counters, ``_ms`` for ms
histograms, no suffix for gauges. Bounded label cardinality only — NO
``order_id``, ``user_id``, ``saga_id``, or ``correlation_id`` (those
belong in logs/traces). Module-level singletons; importing twice is
safe (Python caches). ZERO side effects: no logging, no env reads, no
I/O, no ``print``, no ``__main__`` block.
"""

from __future__ import annotations

from typing import Final

from prometheus_client import Counter, Gauge, Histogram

# Histogram buckets (in ms). ORDER_PLACEMENT_BUCKETS_MS — POST /orders
# end-to-end (5000.0 aligns with POSTGRES_STATEMENT_TIMEOUT_MS=5000).
# SAGA_STEP_BUCKETS_MS — async saga step via Kafka (10000.0 aligns with
# SAGA_STEP_TIMEOUT_MS=10000; 30000/60000 capture tail before
# SAGA_COMPENSATION_TIMEOUT_MS=30000 fires).
ORDER_PLACEMENT_BUCKETS_MS: Final[tuple[float, ...]] = (
    5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
)
SAGA_STEP_BUCKETS_MS: Final[tuple[float, ...]] = (
    10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0, 60000.0,
)


# =============================================================================
# Order metrics — HTTP surface (used by src.controllers.orders_controller)
# =============================================================================

orders_placed_total: Final[Counter] = Counter(
    "orders_placed_total",
    "Total POST /orders outcomes, partitioned by status and currency.",
    labelnames=("status", "currency"),
)
"""POST /orders outcome counter. Labels: status (``CREATED`` |
``REJECTED`` | ``DUPLICATE``; cardinality 3); currency (ISO-4217
``USD``/``EUR``/``GBP``/``INR``/``AUD``/``CAD``; cardinality 6).
Total: 18. Error rate derived as
``rate(orders_placed_total{status="REJECTED"}[5m])``; a separate
``errors_total`` is intentionally NOT defined (would duplicate this).
"""

order_placement_latency_ms: Final[Histogram] = Histogram(
    "order_placement_latency_ms",
    "End-to-end POST /orders latency in ms (validation + DB insert + saga init + first Kafka emit).",
    buckets=ORDER_PLACEMENT_BUCKETS_MS,
)
"""POST /orders end-to-end latency. User-facing latency only — does NOT
cover async saga progression (see :data:`saga_step_latency_ms`).
Buckets: :data:`ORDER_PLACEMENT_BUCKETS_MS` (5 ms .. 10 s).
Observations MUST be in ms — convert ``time.perf_counter()`` deltas
via ``* 1000.0`` before ``.observe(...)``.
"""


# =============================================================================
# Saga metrics — AAP R-18 (UNIQUE to Order Service)
# =============================================================================
# State machine (see ``src.saga.coordinator``): CREATED ->
# INVENTORY_RESERVED -> PAYMENT_TAKEN -> FULFILLED on the happy path;
# CREATED -> COMPENSATING_(INVENTORY|PAYMENT) -> CANCELLED on rollback;
# any path can terminate in FAILED on DLQ-exhausted manual intervention.

saga_state_transitions_total: Final[Counter] = Counter(
    "saga_state_transitions_total",
    "Total saga state transitions executed by the saga coordinator.",
    labelnames=("from_state", "to_state"),
)
"""Saga state transition counter. Labels: from_state, to_state — one of
``CREATED``, ``INVENTORY_RESERVED``, ``PAYMENT_TAKEN``,
``COMPENSATING_INVENTORY``, ``COMPENSATING_PAYMENT``, ``FULFILLED``,
``CANCELLED``, ``FAILED``. Only ~12 ``(from, to)`` pairs reachable.
Spikes to ``COMPENSATING_*`` correlate with downstream issues; spikes
to ``FAILED`` correlate with poison messages or DLQ exhaustion.
"""

saga_compensation_total: Final[Counter] = Counter(
    "saga_compensation_total",
    "Total compensating actions executed (release inventory, refund payment).",
    labelnames=("reason",),
)
"""Compensating action counter. Labels: reason —
``inventory_reservation_failed`` | ``payment_failed`` |
``saga_timeout`` | ``manual_cancel`` | ``downstream_unavailable``
(cardinality 5). Operators alert when rate > 10/min sustained over 5
min; spikes indicate downstream service problems, poisoned Kafka
messages, or misconfigured saga deadlines.
"""

saga_manual_intervention_total: Final[Counter] = Counter(
    "saga_manual_intervention_total",
    "Total sagas that exhausted retries and require human/operator intervention.",
    labelnames=("reason",),
)
"""Manual-intervention escalation counter — operationally CRITICAL.
Labels: reason — ``dlq_exhausted`` |
``compensation_max_attempts_exceeded`` | ``unrecoverable_error``
(cardinality 3). Any nonzero increment means a saga is durably stuck.
Alerts SHOULD fire on the FIRST increment, NOT on a sustained rate —
the alarm bell against silent loss of customer orders.
"""

saga_in_flight: Final[Gauge] = Gauge(
    "saga_in_flight",
    "In-flight (non-terminal) sagas currently being coordinated, sampled per state.",
    labelnames=("state",),
)
"""In-flight saga gauge. Labels: state — ``CREATED`` |
``INVENTORY_RESERVED`` | ``PAYMENT_TAKEN`` |
``COMPENSATING_INVENTORY`` | ``COMPENSATING_PAYMENT`` (terminal
states NOT sampled; cardinality 5). Sampled by ``src.saga.scheduler``
via a periodic SELECT against ``saga_state`` filtering out terminal
states. A persistently large value is a backlog signal.
"""

saga_step_latency_ms: Final[Histogram] = Histogram(
    "saga_step_latency_ms",
    "Per-step saga latency in ms (from step entry to completion event received).",
    labelnames=("step",),
    buckets=SAGA_STEP_BUCKETS_MS,
)
"""Per-step saga latency histogram. Labels: step — ``inventory_reserve``
| ``payment_capture`` | ``order_fulfill`` (forward) |
``inventory_release`` | ``payment_refund`` (compensating);
cardinality 5. Buckets: :data:`SAGA_STEP_BUCKETS_MS` (10 ms .. 60 s).
The p99 drives the saga deadline; when it approaches
``SAGA_STEP_TIMEOUT_MS=10000``, increase the deadline or fix upstream
slowness (right-shift correlates with elevated
:data:`saga_compensation_total`). Observations MUST be in ms.
"""


# =============================================================================
# Kafka metrics — AAP R-17 retry + DLQ topology (used by src.events.consumer)
# =============================================================================

kafka_consumer_lag: Final[Gauge] = Gauge(
    "kafka_consumer_lag",
    "Consumer-group lag (uncommitted messages) per topic for the Order Service consumer group.",
    labelnames=("topic",),
)
"""Kafka consumer-lag gauge. Labels: topic — Order Service consumes
``inventory.reserved``, ``inventory.reservation_failed``,
``payment.succeeded``, ``payment.failed`` plus ``.retry`` companions
per AAP R-17 (cardinality ~8). Computed via Kafka admin API (committed
vs end offsets). Sustained nonzero lag means the consumer cannot keep
up; alerts fire on threshold or monotonic growth. Deploy spikes are
typically rebalance-related and self-heal.
"""

dlq_depth: Final[Gauge] = Gauge(
    "dlq_depth",
    "Total messages currently waiting in dead-letter topics for the Order Service.",
    labelnames=("topic",),
)
"""DLQ topic depth gauge. Labels: topic — ``order.dlq``,
``inventory.reserved.dlq``, ``inventory.reservation_failed.dlq``,
``payment.succeeded.dlq``, ``payment.failed.dlq`` (cardinality 5).
Per AAP R-17 every consumed/produced topic has a ``.dlq`` companion;
ANY nonzero depth is operationally significant — operators page on
first arrival, with entries usually correlated to
:data:`saga_manual_intervention_total`.
"""


# -----------------------------------------------------------------------------
# Public surface — only the 9 metric singletons are re-exported via __all__.
# Bucket constants and imports are implementation details (NOT re-exported).
# -----------------------------------------------------------------------------
__all__: list[str] = [
    "orders_placed_total",            # Order metrics — HTTP surface
    "order_placement_latency_ms",
    "saga_state_transitions_total",   # Saga metrics — AAP R-18 (UNIQUE)
    "saga_compensation_total",
    "saga_manual_intervention_total",
    "saga_in_flight",
    "saga_step_latency_ms",
    "kafka_consumer_lag",             # Kafka metrics — AAP R-17
    "dlq_depth",
]
