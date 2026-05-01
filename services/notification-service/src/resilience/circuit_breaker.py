"""pybreaker-based circuit breaker primitives for the Notification Service.

Implements AAP R-16: every outbound HTTP call (email and SMS provider
calls) is protected by a circuit breaker with:

  * A **failure-rate threshold** (e.g., 50% failures over the last N
    calls) --- tracked via an in-process ``collections.deque`` of the
    last ``call_volume_threshold`` outcomes.
  * A **call-volume threshold** --- the breaker does not evaluate the
    rate until at least this many calls have been observed, preventing
    spurious trips on rare failures from low-volume services.
  * An **open-duration** (``reset_timeout``) --- how long the breaker
    stays in the OPEN state before allowing a probe.
  * A **half-open probe policy** --- recorded as an attribute for future
    pybreaker upgrades; default behavior is pybreaker's single-probe.

Composition Rule
----------------
**RETRY sits INSIDE the CIRCUIT BREAKER**, not the other way around. A
retry exhaustion counts as ONE failure to the breaker (because the
breaker wraps the retrying coroutine as a single callable). Consumers
compose as::

    @retry_async(policy, classify=classify_email_error)
    async def _send():
        return await provider.send_raw(...)
    try:
        return await breaker.call_async(_send)
    except pybreaker.CircuitBreakerError:
        # AAP R-20 fallback: breaker is open --> route to DLQ.
        await dlq_writer.write(...)

Never wrap ``breaker.call`` / ``breaker.call_async`` WITH an outer
``@retry_async``; that yields N-times-amplified failure counts and
poisons the breaker.

Public API
----------
- :func:`make_circuit_breaker` --- factory returning a configured
  :class:`NotificationCircuitBreaker`.
- :class:`NotificationCircuitBreaker` --- subclass of
  :class:`pybreaker.CircuitBreaker` that adds rolling-window failure-rate
  tracking, observability (structlog + Prometheus) on every state
  transition, a :meth:`NotificationCircuitBreaker.health` method, and
  an :attr:`NotificationCircuitBreaker.is_open` convenience property.
- :class:`BreakerHealth` --- :class:`typing.TypedDict` shape returned by
  :meth:`NotificationCircuitBreaker.health`.

Metrics exposed
---------------
- ``notification_circuit_state{name, state}`` (Gauge 0/1) --- one time
  series per ``(name, state)`` pair; exactly one has value ``1.0`` at a
  time. Pre-seeded with all three states so dashboards never show "no
  data" for healthy breakers.
- ``notification_circuit_trips_total{name}`` (Counter) --- incremented
  on every transition INTO the OPEN state.

Compliance
----------
- AAP R-16 --- circuit breaker thresholds for failure rate and call
  volume, open-state duration, half-open probe policy.
- AAP R-19 --- ``health()`` and ``is_open`` integrate with the
  ``/health/ready`` controller (``src.controllers.health``).
- AAP R-20 --- callers catch :class:`pybreaker.CircuitBreakerError` to
  route to the DLQ writer (fallback path).
- AAP R-26 --- structlog WARNING-level structured JSON logs on every
  state transition; INFO at construction.
- AAP R-27 --- Prometheus metrics exposed via ``/metrics`` endpoint
  scraped by Metricbeat into the ELK stack.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetized)
# ---------------------------------------------------------------------------
# ``threading`` provides the :class:`Lock` primitive that protects the
# rolling-window deque and ``_last_failure_at`` field under concurrent
# access from multiple consumer threads. pybreaker's internal RLock
# protects its own state; our additional bookkeeping needs explicit
# synchronization on a separate lock.
import threading

# ``time`` provides the :func:`time.time` monotonic clock used internally
# for any timing operations. Imported per the schema requirement; does
# not currently drive a module-level computation but is used implicitly
# by pybreaker's ``opened_at`` arithmetic in the manual ``call_async``
# emulation path's timeout-elapsed check.
import time as _time  # noqa: F401  (imported for schema compliance + future use)

# ``deque`` is the fixed-size queue (with ``maxlen=call_volume_threshold``)
# providing O(1) ``append`` + automatic eviction for the rolling-window
# failure-rate tracker that determines when the breaker trips per
# AAP R-16.
from collections import deque

# ``datetime`` and ``timezone`` are used to:
#   1. Stamp ``last_failure_at`` as an ISO 8601 UTC datetime, which the
#      :class:`BreakerHealth` dict serializes via ``.isoformat()`` for
#      the ``/health/ready`` probe response.
#   2. Replicate pybreaker's manual ``opened_at`` arithmetic in the
#      :meth:`NotificationCircuitBreaker.call_async` fallback path that
#      runs when Tornado is not installed.
from datetime import datetime, timedelta, timezone

# Type-system primitives:
#   - ``Any`` types the variadic ``call`` / ``call_async`` arguments.
#   - ``Protocol`` + ``runtime_checkable`` together define the structural
#     :class:`CircuitBreakerConfig` so this module is decoupled from the
#     concrete ``Settings`` class in :mod:`src.config.settings`.
#   - ``TypedDict`` defines the :class:`BreakerHealth` return shape used
#     by the readiness-probe controller.
from typing import Any, Protocol, TypedDict, runtime_checkable

# ---------------------------------------------------------------------------
# Third-party imports (alphabetized)
# ---------------------------------------------------------------------------
# ``pybreaker`` is the canonical thread-safe in-process circuit breaker
# library for Python (AAP Section 0.3.1, R-16). We subclass
# :class:`pybreaker.CircuitBreaker` so downstream code that
# type-annotates against the base class continues to work; we extend the
# state machine with a rolling-window failure-rate tracker.
import pybreaker

# ``structlog`` is the canonical structured-logging library across this
# service (AAP R-26). Calling :func:`structlog.get_logger` returns a
# lazy proxy whose processor chain is configured globally during
# :func:`src.main.lifespan`. The structlog ``contextvars`` processor
# automatically merges the request's correlation ID (AAP R-13) into
# every log line emitted from this module.
import structlog

# ``REGISTRY`` is the global Prometheus default collector registry; we
# read its private ``_names_to_collectors`` dict so we can reuse already
# registered Counter / Gauge instances when the module is imported more
# than once (e.g., pytest's ``importlib.reload``). ``Counter`` and
# ``Gauge`` are the metric primitives that surface the AAP R-27
# circuit-breaker observability signals on ``/metrics``.
from prometheus_client import REGISTRY, Counter, Gauge

# ---------------------------------------------------------------------------
# Module-level state --- structured logger
# ---------------------------------------------------------------------------
# The structlog logger captures a stable ``logger=`` field that consumers
# can pivot on in Kibana to find every state-change event emitted from
# this module. Bind once at import time; the lazy proxy defers its
# processor chain to runtime so changes applied later in the service
# lifespan still take effect.
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# State labels used for gauge time-series initialization
# ---------------------------------------------------------------------------
# pybreaker exposes the canonical state name constants
# (``STATE_CLOSED == "closed"``, ``STATE_HALF_OPEN == "half-open"``,
# ``STATE_OPEN == "open"``). Using the library constants --- not bare
# string literals --- protects this module against any future rename in
# pybreaker. Pre-registering all three labels at construction time
# guarantees ``/metrics`` always shows every state, which avoids
# Grafana / Kibana panels rendering "no data" for healthy breakers.
_STATE_LABELS: tuple[str, ...] = (
    pybreaker.STATE_CLOSED,
    pybreaker.STATE_HALF_OPEN,
    pybreaker.STATE_OPEN,
)


# ---------------------------------------------------------------------------
# Prometheus metrics --- idempotent registration
# ---------------------------------------------------------------------------
def _get_or_create_counter(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...],
) -> Counter:
    """Return an existing :class:`Counter` from REGISTRY or register a new one.

    Re-importing this module in tests (or in any environment that calls
    :func:`importlib.reload`) MUST NOT raise the
    ``ValueError: Duplicated timeseries in CollectorRegistry`` that
    ``Counter(name, ...)`` would otherwise raise on second registration.
    We therefore peek into the registry's private
    ``_names_to_collectors`` mapping and reuse the existing collector
    when present.

    Args:
        name: The Prometheus metric name (e.g.,
            ``notification_circuit_trips_total``).
        documentation: Human-readable HELP string emitted in the
            ``/metrics`` exposition output.
        labelnames: Tuple of label keys.

    Returns:
        The freshly-registered or pre-existing :class:`Counter`.
    """
    # ``REGISTRY._names_to_collectors`` is a dict keyed by the metric's
    # base name. Looking up the bare name is sufficient because Counter
    # registers itself under its base name first.
    existing = REGISTRY._names_to_collectors.get(name)  # noqa: SLF001
    if existing is not None:
        # Reuse the registered collector. The ``type: ignore`` covers
        # the fact that REGISTRY stores collectors as the abstract base
        # class; we know it's a Counter because we registered it as one
        # and the name -> collector mapping is unique within the registry.
        return existing  # type: ignore[return-value]
    return Counter(name, documentation, labelnames=labelnames)


def _get_or_create_gauge(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...],
) -> Gauge:
    """Return an existing :class:`Gauge` from REGISTRY or register a new one.

    Same idempotent-registration semantics as
    :func:`_get_or_create_counter`. See that function's docstring for
    the rationale.

    Args:
        name: The Prometheus metric name (e.g., ``notification_circuit_state``).
        documentation: Human-readable HELP string.
        labelnames: Tuple of label keys.

    Returns:
        The freshly-registered or pre-existing :class:`Gauge`.
    """
    existing = REGISTRY._names_to_collectors.get(name)  # noqa: SLF001
    if existing is not None:
        return existing  # type: ignore[return-value]
    return Gauge(name, documentation, labelnames=labelnames)


# Gauge encoding the breaker's current state with TWO labels (name +
# state) and a binary 0.0 / 1.0 value. For a given ``name`` exactly one
# ``(name, state)`` time series has value 1.0 at any moment; the other
# two are 0.0. This shape matches the folder spec verbatim and yields
# the natural Kibana / Grafana query::
#
#     notification_circuit_state{name="email", state="open"}
#
# It trades a slight increase in time-series cardinality (3x rather than
# 1x per breaker) for query ergonomics on the resilience dashboard.
CIRCUIT_STATE: Gauge = _get_or_create_gauge(
    "notification_circuit_state",
    "Current state of a notification-service circuit breaker (0/1 gauge per name+state).",
    labelnames=("name", "state"),
)

# Counter incremented ONLY on transitions INTO the OPEN state. Combined
# with the gauge above, dashboards can compute trip rate over time::
#
#     rate(notification_circuit_trips_total{name="email"}[5m])
#
# Transitions OUT of the OPEN state (e.g., open -> half-open after the
# reset timeout, or half-open -> closed on a successful probe) are
# observable on the gauge; only the INTO-OPEN transitions are counted
# here because that is the operationally-significant outage signal.
CIRCUIT_TRIPS_TOTAL: Counter = _get_or_create_counter(
    "notification_circuit_trips_total",
    "Count of notification-service circuit-breaker transitions to the OPEN state.",
    labelnames=("name",),
)


# ---------------------------------------------------------------------------
# CircuitBreakerConfig structural Protocol
# ---------------------------------------------------------------------------
@runtime_checkable
class CircuitBreakerConfig(Protocol):
    """Duck-typed view of a circuit-breaker configuration.

    Any object exposing these four attributes --- a Pydantic
    :class:`pydantic.BaseModel` subclass, a dataclass, a plain
    namespace, or a unit-test mock --- is acceptable as the ``policy``
    argument to :func:`make_circuit_breaker`.

    Attributes:
        failure_rate_threshold_pct: Failure rate (in percent, ``0 < x <= 100``)
            above which the breaker opens, evaluated over the last
            ``call_volume_threshold`` calls.
        call_volume_threshold: Minimum number of observed calls before
            the rolling-window rate is evaluated. Must be ``>= 1``.
        open_duration_ms: How long the breaker stays in the OPEN state
            before allowing a probe call (HALF_OPEN). Maps to pybreaker's
            ``reset_timeout`` (converted from milliseconds to seconds).
            Must be ``>= 0``.
        half_open_permitted_calls: Number of probe calls permitted in
            HALF_OPEN before either closing (all succeed) or re-opening
            (any fail). Must be ``>= 1``. pybreaker's built-in default
            is single-probe; values ``> 1`` are recorded as a custom
            attribute for future pybreaker upgrades that natively
            support multi-probe HALF_OPEN.
    """

    # ``int | float`` accommodates both Pydantic-validated integer
    # percentages (e.g., 50) and dataclass-supplied floats (e.g., 99.5).
    failure_rate_threshold_pct: int | float
    call_volume_threshold: int
    open_duration_ms: int
    half_open_permitted_calls: int


# ---------------------------------------------------------------------------
# BreakerHealth TypedDict
# ---------------------------------------------------------------------------
class BreakerHealth(TypedDict):
    """Shape of the dict returned by :meth:`NotificationCircuitBreaker.health`.

    Consumed by the ``/health/ready`` controller (``src.controllers.health``)
    to populate the readiness-probe response with per-breaker status.
    All fields are JSON-serializable primitives so the dict can be
    embedded in the readiness JSON without further coercion.

    Keys:
        name: The breaker's unique identifier (e.g., ``"email"``,
            ``"sms"``).
        state: Current pybreaker state name --- one of
            :data:`pybreaker.STATE_CLOSED` (``"closed"``),
            :data:`pybreaker.STATE_HALF_OPEN` (``"half-open"``), or
            :data:`pybreaker.STATE_OPEN` (``"open"``).
        fail_count: Current consecutive-failure counter from pybreaker's
            internal state. Reset to ``0`` on every success.
        rolling_window_failures: Number of failures within the last
            ``call_volume_threshold`` calls (the primary trip signal).
        rolling_window_size: Actual number of observations currently in
            the rolling window. Equals ``call_volume_threshold`` once
            the window is full; ``< call_volume_threshold`` during the
            warm-up phase.
        failure_rate_pct: Computed failure rate in percent, defined as
            ``(rolling_window_failures / rolling_window_size) * 100``
            when ``rolling_window_size > 0``, else ``0.0``.
        last_failure_at: ISO 8601 UTC timestamp of the most recent
            recorded failure, or ``None`` if no failure has yet been
            observed.
        is_open: Convenience boolean equal to ``state == "open"``.
            ``True`` signals degraded readiness for the protected
            dependency.
    """

    name: str
    state: str
    fail_count: int
    rolling_window_failures: int
    rolling_window_size: int
    failure_rate_pct: float
    last_failure_at: str | None
    is_open: bool


# ---------------------------------------------------------------------------
# State-change listener --- emit observability signals on every transition
# ---------------------------------------------------------------------------
class _BreakerStateChangeListener(pybreaker.CircuitBreakerListener):
    """pybreaker listener emitting structlog + Prometheus on state changes.

    Registered automatically by :class:`NotificationCircuitBreaker`; not
    intended for direct instantiation. On every state transition this
    listener:

    1. Emits a WARNING-level :mod:`structlog` line with the stable event
       name ``circuit_breaker_state_change`` and the canonical pivot
       fields (``breaker``, ``from_state``, ``to_state``,
       ``fail_counter``) so Kibana panels can drill in on a particular
       transition --- per AAP R-26.
    2. Updates the ``notification_circuit_state`` Gauge: sets the new
       state's time series to ``1.0`` and the other two to ``0.0`` for
       this breaker's ``name`` --- per AAP R-27.
    3. Increments ``notification_circuit_trips_total`` ONLY on
       transitions INTO the OPEN state (the operationally-significant
       outage signal).
    """

    def state_change(
        self,
        cb: pybreaker.CircuitBreaker,
        old_state: pybreaker.CircuitBreakerState | None,
        new_state: pybreaker.CircuitBreakerState | None,
    ) -> None:
        """Handle a breaker state transition (override of base listener).

        Args:
            cb: The breaker whose state changed. Always non-``None``.
            old_state: The previous state, or ``None`` on the very first
                transition where the breaker had no prior state.
            new_state: The new state. Practically always non-``None``;
                typed as optional to mirror the pybreaker base class
                signature.
        """
        # ``CircuitBreakerState.name`` is the canonical state name string
        # ("closed", "half-open", "open"). Default to "unknown" for the
        # extremely-unlikely None case so the metric labels remain
        # populated and the log line readable in Kibana without
        # special-casing.
        old_name = old_state.name if old_state is not None else "unknown"
        new_name = new_state.name if new_state is not None else "unknown"

        # AAP R-26: structured JSON log, WARNING level, stable event
        # name. The breaker's name is the primary pivot field; the
        # ``from_state`` / ``to_state`` pair drives the transition
        # heatmap on the resilience dashboard. ``fail_counter`` is read
        # via ``getattr`` defensively because pybreaker minor-version
        # bumps occasionally rename or remove the attribute.
        logger.warning(
            "circuit_breaker_state_change",
            breaker=cb.name,
            from_state=old_name,
            to_state=new_name,
            fail_counter=getattr(cb, "fail_counter", 0),
        )

        # AAP R-27: gauge update. Set the new state's series to 1.0 and
        # all other states to 0.0 for this breaker. This loop guarantees
        # the gauge invariant "exactly one (name, state) pair has value
        # 1.0 at any time" even if a future pybreaker version emits an
        # unexpected ``new_name`` not in ``_STATE_LABELS`` --- in that
        # case all three known states are zeroed and the unknown state
        # silently does not appear, which is operationally safe.
        for state_label in _STATE_LABELS:
            value = 1.0 if state_label == new_name else 0.0
            CIRCUIT_STATE.labels(name=cb.name, state=state_label).set(value)

        # AAP R-27: trip counter. Increment ONLY on transitions INTO
        # OPEN (closed -> open or half-open -> open). Transitions OUT
        # of OPEN (open -> half-open, half-open -> closed) are visible
        # on the gauge but do not register here because they are not
        # outage events.
        if new_name == pybreaker.STATE_OPEN:
            CIRCUIT_TRIPS_TOTAL.labels(name=cb.name).inc()

        # On any transition INTO the CLOSED state (typically
        # half-open -> closed after a successful probe, but also
        # operator-driven close() / reset()), clear our rolling-window
        # history. Without this, the window would still hold the old
        # pre-trip failures and any subsequent failure inside the
        # post-recovery period would re-trip the breaker on the spot
        # --- a confusing user experience that hides the fact that the
        # provider has actually recovered.
        #
        # The hasattr guard below ensures we only clear the window for
        # :class:`NotificationCircuitBreaker` instances; if a future
        # caller passes a vanilla :class:`pybreaker.CircuitBreaker`
        # with this listener attached, we silently no-op.
        if new_name == pybreaker.STATE_CLOSED and isinstance(
            cb, NotificationCircuitBreaker,
        ):
            cb._clear_window()  # noqa: SLF001  (intra-module sibling access)


# ---------------------------------------------------------------------------
# NotificationCircuitBreaker --- subclass of pybreaker.CircuitBreaker
# ---------------------------------------------------------------------------
class NotificationCircuitBreaker(pybreaker.CircuitBreaker):
    """Notification Service circuit breaker with rolling-window failure-rate.

    Subclasses :class:`pybreaker.CircuitBreaker` to keep compatibility
    with callers that type-annotate against the base class. Adds:

      * A rolling-window failure-rate tracker over the last
        ``call_volume_threshold`` calls. When the rate over the FULL
        window meets or exceeds ``failure_rate_threshold_pct``, the
        breaker is forced into the OPEN state via :meth:`open`.
      * A :meth:`health` method returning a :class:`BreakerHealth` dict
        for the readiness-probe controller (AAP R-19).
      * An :attr:`is_open` boolean property (convenience for readiness
        / availability checks).
      * A :meth:`reset` method that closes the breaker and clears the
        rolling-window history (mirrors pybreaker's pre-1.x ``reset``
        semantics; useful in tests and for operator-driven recovery).
      * Overridden :meth:`call` and :meth:`call_async` that drive
        rolling-window accounting AFTER pybreaker's own bookkeeping
        --- a single helper, :meth:`_after_call`, deduplicates the
        outcome-recording logic across both call paths.

    All inherited :class:`pybreaker.CircuitBreaker` APIs remain
    available: :meth:`current_state`, :attr:`fail_counter`,
    :attr:`name`, :meth:`open`, :meth:`close`, :meth:`half_open`.
    """

    def __init__(
        self,
        name: str,
        *,
        failure_rate_threshold_pct: float,
        call_volume_threshold: int,
        open_duration_ms: int,
        half_open_permitted_calls: int,
    ) -> None:
        """Construct a :class:`NotificationCircuitBreaker`.

        Args:
            name: Unique identifier --- appears as the ``name`` label on
                exported Prometheus metrics and in structlog events.
            failure_rate_threshold_pct: Failure rate threshold (percent)
                above which the breaker opens. See
                :class:`CircuitBreakerConfig` for full semantics.
            call_volume_threshold: Window size for the rolling-window
                rate evaluation; also used as ``fail_max`` (the absolute
                worst-case backstop --- a 100%-failure window trips the
                breaker via pybreaker's native mechanism regardless of
                our rolling-window logic).
            open_duration_ms: Open-state duration in milliseconds.
                Translated to ``reset_timeout`` (seconds).
            half_open_permitted_calls: Number of probe calls permitted
                in HALF_OPEN. Recorded for future pybreaker upgrades.
        """
        # pybreaker's ``fail_max`` is the SECONDARY guard --- we set it
        # equal to ``call_volume_threshold`` so a 100%-failure window
        # trips the breaker via pybreaker's built-in mechanism even if
        # our rolling-window code has a bug. This defense-in-depth is
        # deliberate. The PRIMARY trip mechanism is the rolling-window
        # rate evaluation in :meth:`_record_and_evaluate`.
        #
        # ``throw_new_error_on_trip=False`` preserves the ORIGINAL
        # exception type when pybreaker's native fail_max mechanism
        # trips the breaker on a failing call (otherwise pybreaker
        # would replace the original ``RuntimeError`` /
        # ``ProviderTransientError`` / etc. with its own
        # ``CircuitBreakerError``). Preserving the original is critical
        # because our :meth:`call` override uses the exception type to
        # distinguish "actual call failure" (record as window failure)
        # from "short-circuit because already open"
        # (do NOT record). Without this flag, every fail_max-crossing
        # failure would be silently dropped from our rolling window.
        # The breaker still transitions to OPEN regardless of this
        # flag --- only the user-facing exception type changes. Callers
        # that need the OPEN signal can still detect it via
        # :attr:`is_open` immediately after the failing call returns.
        super().__init__(
            name=name,
            fail_max=call_volume_threshold,
            reset_timeout=open_duration_ms / 1000.0,
            listeners=[_BreakerStateChangeListener()],
            throw_new_error_on_trip=False,
        )

        # Cache the user-facing thresholds in attributes so they can be
        # read by tests and by future tooling (e.g., a ``/breakers``
        # admin endpoint that surfaces the active configuration). All
        # are leading-underscore so they signal "internal" --- callers
        # should not mutate them after construction.
        self._threshold_rate: float = float(failure_rate_threshold_pct) / 100.0
        self._call_volume_threshold: int = int(call_volume_threshold)
        self._half_open_permitted_calls: int = int(half_open_permitted_calls)

        # Rolling-window tracker: each entry is True (failure) or False
        # (success). ``deque`` with ``maxlen=call_volume_threshold``
        # gives O(1) ``append`` + automatic eviction. The lock protects
        # both the deque and ``_last_failure_at`` --- pybreaker's
        # internal RLock does NOT cover these additional fields.
        self._window: deque[bool] = deque(maxlen=int(call_volume_threshold))
        self._window_lock: threading.Lock = threading.Lock()

        # Track the most recent failure timestamp for the readiness
        # probe. ``None`` until the first failure is observed.
        self._last_failure_at: datetime | None = None

        # Pre-seed the gauge so ``/metrics`` shows the breaker even
        # before any state change occurs. Initial state: closed=1,
        # half-open=0, open=0. Without this, dashboards aggregating
        # over ``notification_circuit_state`` would render "no data"
        # for healthy breakers --- a confusing operator experience.
        for state_label in _STATE_LABELS:
            CIRCUIT_STATE.labels(name=name, state=state_label).set(
                1.0 if state_label == pybreaker.STATE_CLOSED else 0.0,
            )

        # AAP R-26: structured initialization log so operators can
        # confirm the breaker came up with the expected thresholds.
        # INFO level is appropriate --- initialization is an expected
        # event, not a warning.
        logger.info(
            "circuit_breaker_initialized",
            name=name,
            fail_max=int(call_volume_threshold),
            reset_timeout_seconds=open_duration_ms / 1000.0,
            failure_rate_threshold_pct=float(failure_rate_threshold_pct),
            call_volume_threshold=int(call_volume_threshold),
            half_open_permitted_calls=int(half_open_permitted_calls),
        )

    # ------------------------------------------------------------------
    # Rolling-window rate evaluation --- the primary trip mechanism
    # ------------------------------------------------------------------
    def _record_and_evaluate(self, *, failed: bool) -> None:
        """Record a call outcome and trip the breaker if the rate exceeds.

        This method is the PRIMARY trip path. It is invoked by
        :meth:`_after_call` after every protected call returns (success
        or failure). When the window is full and the failure rate meets
        or exceeds ``threshold_rate``, the breaker is forced into the
        OPEN state via :meth:`pybreaker.CircuitBreaker.open` --- which
        fires the state-change listener and emits the structlog +
        Prometheus signals.

        Args:
            failed: ``True`` if the most recent call raised an
                exception (other than :class:`pybreaker.CircuitBreakerError`,
                which signals a short-circuit and is not recorded);
                ``False`` if it returned normally.
        """
        # All deque + datetime access happens under the window lock.
        # We compute the snapshot of "are we tripping?" inside the
        # critical section, then drop the lock before any further work
        # so we never call back into pybreaker (which has its own RLock)
        # while holding ours --- preventing any lock-ordering hazards.
        with self._window_lock:
            self._window.append(failed)
            if failed:
                self._last_failure_at = datetime.now(timezone.utc)

            # Only evaluate the rate when the window is full. Below
            # threshold-volume, a handful of failures should not trip
            # the breaker --- this is the AAP R-16 "call-volume
            # threshold" requirement that prevents spurious trips on
            # rare failures from low-volume services.
            if len(self._window) < self._call_volume_threshold:
                return

            failures = sum(1 for entry in self._window if entry)
            rate = failures / self._call_volume_threshold
            already_open = self.current_state == pybreaker.STATE_OPEN

        # Trip the breaker outside the window lock. The check
        # ``not already_open`` avoids re-tripping an already-open
        # breaker (which would emit a redundant ``open -> open``
        # state-change event). The threshold comparison uses ``>=``
        # so an exactly-on-threshold rate (e.g., 50% failure with
        # threshold 50) trips the breaker, matching the user-facing
        # spec language "fails when the rate EXCEEDS the threshold"
        # interpreted inclusively.
        if rate >= self._threshold_rate and not already_open:
            logger.warning(
                "circuit_breaker_rate_exceeded_tripping",
                name=self.name,
                failure_rate_pct=rate * 100.0,
                threshold_pct=self._threshold_rate * 100.0,
                window_size=self._call_volume_threshold,
                failures_in_window=failures,
            )
            # Use pybreaker's public ``open()`` method (added in
            # pybreaker 1.4.x) to force the OPEN transition. ``open()``
            # acquires pybreaker's RLock, sets ``opened_at``, mutates
            # the state storage, and fires our listener exactly once
            # via the standard state-change pathway. We do NOT touch
            # ``_state_storage.state`` directly --- the public API
            # gives us deterministic listener firing and timeout
            # initialization without relying on private internals.
            self.open()

    def _after_call(self, *, raised: bool) -> None:
        """Post-call hook --- record outcome + evaluate the rate.

        Single shared helper used by both :meth:`call` and
        :meth:`call_async` so the outcome-recording logic lives in
        exactly one place.

        Args:
            raised: ``True`` when the protected callable raised an
                exception that should be counted as a failure;
                ``False`` when it returned normally.
        """
        self._record_and_evaluate(failed=raised)

    # ------------------------------------------------------------------
    # Synchronous call path
    # ------------------------------------------------------------------
    def call(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Synchronous protected call with rolling-window tracking.

        Delegates to :meth:`pybreaker.CircuitBreaker.call` so all
        pybreaker semantics (lock acquisition, listener invocation,
        ``fail_max`` accounting) execute first, then records the
        outcome in our rolling-window tracker.

        Args:
            func: A synchronous callable to protect. Most commonly a
                bound method on a provider adapter.
            *args: Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.

        Returns:
            Whatever ``func`` returns.

        Raises:
            pybreaker.CircuitBreakerError: When the breaker is OPEN
                (short-circuit). NOT recorded as a window failure
                because no call was actually made --- recording it
                would create a positive-feedback loop where an open
                breaker keeps re-tripping itself.
            BaseException: Any exception raised by ``func`` is
                propagated AFTER recording it as a window failure.
        """
        try:
            result = super().call(func, *args, **kwargs)
        except pybreaker.CircuitBreakerError:
            # Short-circuit --- no call was made; do not record.
            raise
        except BaseException:  # noqa: BLE001  (intentional --- record any failure)
            self._after_call(raised=True)
            raise
        else:
            self._after_call(raised=False)
            return result

    # ------------------------------------------------------------------
    # Asynchronous call path --- with manual emulation when Tornado is absent
    # ------------------------------------------------------------------
    async def call_async(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Async protected call with rolling-window tracking.

        Two implementation paths are tried in order:

        1. **Native** --- :meth:`pybreaker.CircuitBreaker.call_async`,
           which is the canonical pybreaker 1.x async API. It is
           decorated with :func:`tornado.gen.coroutine` and raises
           :class:`NameError` at call time when Tornado is not
           installed. The Notification Service does NOT pin Tornado
           (see ``requirements.txt``), so in practice the native path
           is selected only when an operator has explicitly added the
           ``tornado`` dependency.

        2. **Manual emulation** --- when the native path raises
           :class:`NameError` we fall through to a hand-written async
           wrapper that:

             a. Eagerly checks ``current_state`` and short-circuits
                with :class:`pybreaker.CircuitBreakerError` when OPEN
                and the reset timeout has not elapsed. When the
                timeout HAS elapsed we transition to HALF_OPEN before
                invoking ``func`` as the probe call.
             b. Awaits ``func``.
             c. Routes the success / failure outcome through the
                synchronous :meth:`pybreaker.CircuitBreaker.call`
                helper using the tiny ``_noop`` / ``_raise`` shims
                below so listeners fire and state transitions happen
                exactly as they would on the native path.

        Args:
            func: An ``async def`` function (or any callable returning
                an awaitable) to protect.
            *args: Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.

        Returns:
            Whatever ``func`` returns when awaited.

        Raises:
            pybreaker.CircuitBreakerError: When the breaker is OPEN
                (short-circuit). NOT recorded as a window failure ---
                see :meth:`call` for the rationale.
            BaseException: Any exception raised by ``func`` is
                propagated AFTER recording it as a window failure.
        """
        # ------- Path 1: native pybreaker.call_async (requires Tornado)
        # ``getattr`` with a sentinel default guards against any future
        # pybreaker version that drops or renames ``call_async``.
        native = getattr(super(), "call_async", None)
        if callable(native):
            try:
                result = await native(func, *args, **kwargs)
            except pybreaker.CircuitBreakerError:
                raise
            except NameError:
                # Tornado is not installed --- pybreaker's call_async
                # is decorated with @gen.coroutine which raises
                # NameError on ``gen``. Fall through to the manual
                # path. We catch NameError narrowly so genuine name
                # errors inside the protected coroutine still
                # propagate normally.
                pass
            except BaseException:  # noqa: BLE001  (record any failure)
                self._after_call(raised=True)
                raise
            else:
                self._after_call(raised=False)
                return result

        # ------- Path 2: manual emulation (default at runtime)
        return await self._call_async_manual(func, *args, **kwargs)

    async def _call_async_manual(
        self,
        func: Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Manual emulation of pybreaker's ``call_async`` without Tornado.

        Replicates pybreaker's internal flow:

        1. If OPEN and the reset timeout has NOT elapsed --> short-circuit.
        2. If OPEN and the reset timeout HAS elapsed --> transition to
           HALF_OPEN (firing the listener) and proceed.
        3. Await ``func``.
        4. Route the outcome through ``super().call(...)`` using shim
           callables so pybreaker's listeners and state machine update
           exactly as they would on the native path.
        5. Record the outcome in our rolling-window tracker.
        """
        # Eager OPEN check. pybreaker's ``CircuitOpenState.before_call``
        # implements identical timeout arithmetic; we replicate it here
        # so the manual path is observationally equivalent to the
        # native path.
        if self.current_state == pybreaker.STATE_OPEN:
            # ``opened_at`` is set by pybreaker to ``datetime.now(UTC)``
            # at the moment the breaker tripped. Read defensively via
            # ``getattr`` so custom storage backends that name the
            # attribute differently still work.
            storage = self._state_storage
            opened_at: datetime | None = getattr(storage, "opened_at", None)
            timeout_delta = timedelta(seconds=self.reset_timeout)

            if (
                opened_at is not None
                and datetime.now(timezone.utc) < opened_at + timeout_delta
            ):
                # Reset timeout has NOT elapsed. Short-circuit.
                # Do NOT record in the rolling window --- no call was
                # made.
                raise pybreaker.CircuitBreakerError(
                    f"Circuit breaker {self.name!r} is OPEN",
                )

            # Reset timeout HAS elapsed (or ``opened_at`` is missing,
            # which we treat as "ready to probe" for safety). Transition
            # to HALF_OPEN --- this fires the state-change listener and
            # leaves the breaker primed for the single probe call below.
            self.half_open()

        # Await the protected coroutine. Three exception flavors:
        #   1. CircuitBreakerError from a downstream nested breaker:
        #      propagate without double-counting (the inner breaker
        #      already recorded the failure on its own window).
        #   2. Any other exception: record as a failure on our window
        #      AND drive pybreaker's failure accounting.
        #   3. Success: drive pybreaker's success accounting AND record
        #      a success on our window.
        try:
            result = await func(*args, **kwargs)
        except pybreaker.CircuitBreakerError:
            # An inner breaker tripped --- propagate. We do NOT record
            # this as a failure on the OUTER breaker because no call
            # to a real provider was attempted from this breaker's
            # perspective.
            raise
        except BaseException as exc:  # noqa: BLE001  (record any failure)
            # Drive pybreaker's failure accounting via the synchronous
            # ``call`` helper. The ``_raise`` shim re-raises ``exc``
            # inside ``super().call``'s lock, which:
            #   - increments fail_counter (if exc is not excluded);
            #   - fires listeners.failure;
            #   - if fail_counter crosses fail_max, fires
            #     state_change(closed -> open);
            #   - re-raises ``exc``.
            try:
                super().call(_raise, exc)
            except pybreaker.CircuitBreakerError:
                # The breaker tripped to OPEN while accounting for
                # this failure (we crossed the fail_max boundary).
                # Record the failure in our window and surface the
                # OPEN signal so the caller enters the fallback path
                # immediately.
                self._after_call(raised=True)
                raise
            except BaseException:  # noqa: BLE001  (expected --- shim re-raised)
                # ``super().call(_raise, exc)`` re-raised the original
                # exception. That is the expected path; discard it
                # here because we re-raise the original below to
                # preserve the caller's traceback.
                pass
            self._after_call(raised=True)
            raise

        # Success path. Drive pybreaker's success accounting via the
        # ``_noop`` shim, then record on our window.
        try:
            super().call(_noop)
        except pybreaker.CircuitBreakerError:
            # Edge case: another concurrent call tipped the breaker
            # between ``func`` succeeding and the success bookkeeping.
            # ``func``'s result is still valid for the caller to use,
            # so we swallow the OPEN signal here and return it.
            pass
        self._after_call(raised=False)
        return result

    # ------------------------------------------------------------------
    # Public helpers --- readiness probe + introspection
    # ------------------------------------------------------------------
    @property
    def is_open(self) -> bool:
        """``True`` when the breaker is in the OPEN state.

        Convenience accessor for the readiness-probe controller
        (AAP R-19). When ``True``, the service should advertise
        degraded readiness for the corresponding provider so that
        upstream load balancers route traffic away.
        """
        return bool(self.current_state == pybreaker.STATE_OPEN)

    def health(self) -> BreakerHealth:
        """Return a JSON-serializable health snapshot of the breaker.

        Consumed by :mod:`src.controllers.health` to populate the
        readiness probe's per-breaker status section. All values are
        primitives (str, int, float, bool, or ``None``) so the dict
        can be embedded directly in the readiness JSON without
        further coercion.

        Returns:
            A :class:`BreakerHealth` dict --- see the class for the
            full key catalogue.
        """
        # Snapshot the deque and ``_last_failure_at`` under the window
        # lock to avoid concurrent-mutation races. We compute everything
        # else outside the lock to keep the critical section small.
        with self._window_lock:
            window_copy = list(self._window)
            last_failure = self._last_failure_at

        window_size = len(window_copy)
        window_failures = sum(1 for entry in window_copy if entry)
        # Empty-window guard --- a fresh breaker reports 0.0 % rather
        # than dividing by zero. The readiness JSON consumer can
        # distinguish "no data yet" from "0 % failure" via the
        # ``rolling_window_size`` field.
        rate_pct = (window_failures / window_size * 100.0) if window_size else 0.0

        # ``pybreaker.CircuitBreaker.name`` is typed as ``str | None`` in
        # the base class because pybreaker permits anonymous breakers.
        # Our factory enforces a non-empty string in
        # :func:`make_circuit_breaker`, so coercing via ``or ""`` is a
        # safety net; in practice the empty branch is unreachable.
        breaker_name: str = self.name or ""
        return BreakerHealth(
            name=breaker_name,
            state=self.current_state,
            fail_count=self.fail_counter,
            rolling_window_failures=window_failures,
            rolling_window_size=window_size,
            failure_rate_pct=rate_pct,
            last_failure_at=last_failure.isoformat() if last_failure else None,
            is_open=self.is_open,
        )

    # ------------------------------------------------------------------
    # Internal --- rolling-window maintenance on state transitions
    # ------------------------------------------------------------------
    def _clear_window(self) -> None:
        """Clear the rolling-window history (called by the state listener).

        Invoked by :class:`_BreakerStateChangeListener` when the breaker
        transitions back into the CLOSED state, so prior pre-trip
        failures do not influence the next trip decision after recovery.
        Also invoked manually by :meth:`reset`. Acquires
        :attr:`_window_lock` so concurrent ``call`` / ``call_async``
        invocations cannot observe a half-cleared deque.
        """
        with self._window_lock:
            self._window.clear()
            self._last_failure_at = None

    # ------------------------------------------------------------------
    # Operator / test helpers --- reset
    # ------------------------------------------------------------------
    def reset(self) -> None:
        """Force-close the breaker and clear the rolling-window history.

        Equivalent to manually closing pybreaker (which resets its
        ``fail_counter`` to zero) AND clearing our rolling-window deque
        plus the ``_last_failure_at`` stamp. Intended for two consumers:

        - **Tests** --- so a single test can construct a breaker, drive
          it through trip / probe / close cycles, and reset it before
          the next assertion.
        - **Operator-driven recovery** --- a future ``/breakers/reset``
          admin endpoint can call this method to manually clear a
          breaker that has tripped on a transient infrastructure event
          known to be resolved.

        Note:
            This method does NOT reset the Prometheus counter
            ``notification_circuit_trips_total`` (counters in Prometheus
            are monotonic --- resetting one would violate the rate /
            increase semantics that dashboards depend on).
        """
        # Close the underlying breaker first --- this fires the state
        # change listener (open/half-open -> closed) so dashboards
        # reflect the recovery promptly. The listener will ALSO call
        # :meth:`_clear_window` on us as part of its CLOSED-transition
        # handler. The redundant explicit clear below is the
        # defense-in-depth path: if the breaker is already CLOSED
        # (no listener fires), we still want :meth:`reset` to reset
        # the rolling-window state.
        if self.current_state != pybreaker.STATE_CLOSED:
            self.close()
        # Always clear the window --- idempotent if the listener
        # already did. Covers the already-CLOSED case.
        self._clear_window()


# ---------------------------------------------------------------------------
# Internal shims for the manual call_async emulation path
# ---------------------------------------------------------------------------
def _noop() -> None:
    """No-op success marker passed to ``super().call`` to record success.

    Routing a callable that simply returns ``None`` through pybreaker
    triggers its success-accounting path: ``fail_counter`` resets to
    zero, listeners' ``success`` callback fires, and (in HALF_OPEN)
    the breaker closes after the success threshold is met. The
    function does no real work --- it exists purely to give pybreaker
    a callable to wrap.
    """
    return None


def _raise(exc: BaseException) -> None:
    """Failure marker passed to ``super().call`` to record a failure.

    Routing a callable that re-raises a captured exception through
    pybreaker triggers its failure-accounting path: ``fail_counter``
    increments (if the exception is not in the breaker's ``exclude``
    set), listeners' ``failure`` callback fires, and (when
    ``fail_counter`` crosses ``fail_max``) the breaker transitions to
    OPEN. ``super().call`` then re-raises the exception, which the
    call site in :meth:`NotificationCircuitBreaker._call_async_manual`
    discards because it re-raises the original itself to preserve the
    caller's traceback.

    Args:
        exc: The exception to re-raise inside the breaker's call
            machinery.

    Raises:
        BaseException: Always --- re-raises ``exc``.
    """
    raise exc


# ---------------------------------------------------------------------------
# Factory --- the only public construction entry point
# ---------------------------------------------------------------------------
def make_circuit_breaker(
    name: str,
    policy: CircuitBreakerConfig,
) -> NotificationCircuitBreaker:
    """Construct a configured :class:`NotificationCircuitBreaker` (AAP R-16).

    Args:
        name: Unique identifier for the breaker. Appears as the
            ``name`` label on exported Prometheus metrics and in
            structlog events. Convention: the downstream dependency
            it protects (``"email"`` or ``"sms"`` per the folder spec).
        policy: A :class:`CircuitBreakerConfig`-shaped object ---
            usually ``settings.email.circuit_breaker`` or
            ``settings.sms.circuit_breaker``. Any object with the four
            required attributes is acceptable.

    Returns:
        A :class:`NotificationCircuitBreaker` instance with a
        :class:`_BreakerStateChangeListener` attached. Initial state:
        CLOSED. The ``notification_circuit_state`` gauge is seeded
        with ``closed=1.0, half-open=0.0, open=0.0``.

    Raises:
        ValueError: When any of the policy attributes violate their
            documented ranges. Failing fast at the factory call site
            catches misconfigurations during service bootstrap (when
            ``Settings`` is constructed) instead of at the first
            protected call deep inside the request path.
    """
    # ``name`` must be a non-empty string --- it labels Prometheus
    # metrics, log events, and the breaker's identity. Allowing empty
    # / non-string would silently break dashboard pivots.
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"name must be a non-empty string for circuit breaker (got {name!r})",
        )

    # ``failure_rate_threshold_pct`` must be in the half-open interval
    # ``(0, 100]``. A zero threshold would trip the breaker on the very
    # first call --- nonsensical. A value above 100 has no meaning.
    failure_rate_threshold_pct = float(policy.failure_rate_threshold_pct)
    if not 0 < failure_rate_threshold_pct <= 100:
        raise ValueError(
            f"failure_rate_threshold_pct must be in (0, 100] for breaker "
            f"{name!r}; got {failure_rate_threshold_pct!r}",
        )

    # ``call_volume_threshold`` must be at least 1 --- the rolling
    # window cannot have fewer than one slot. Higher values smooth out
    # noise but delay trip detection.
    call_volume_threshold = int(policy.call_volume_threshold)
    if call_volume_threshold < 1:
        raise ValueError(
            f"call_volume_threshold must be >= 1 for breaker {name!r}; "
            f"got {call_volume_threshold!r}",
        )

    # ``open_duration_ms`` must be non-negative. Zero means "trip and
    # immediately allow a probe" --- weird but technically valid for
    # tests; we permit it.
    open_duration_ms = int(policy.open_duration_ms)
    if open_duration_ms < 0:
        raise ValueError(
            f"open_duration_ms must be >= 0 for breaker {name!r}; "
            f"got {open_duration_ms!r}",
        )

    # ``half_open_permitted_calls`` must be at least 1 --- a HALF_OPEN
    # state with zero permitted probes would never close the breaker.
    half_open_permitted_calls = int(policy.half_open_permitted_calls)
    if half_open_permitted_calls < 1:
        raise ValueError(
            f"half_open_permitted_calls must be >= 1 for breaker {name!r}; "
            f"got {half_open_permitted_calls!r}",
        )

    return NotificationCircuitBreaker(
        name=name,
        failure_rate_threshold_pct=failure_rate_threshold_pct,
        call_volume_threshold=call_volume_threshold,
        open_duration_ms=open_duration_ms,
        half_open_permitted_calls=half_open_permitted_calls,
    )


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------
# Alphabetical for stable ``from circuit_breaker import *`` and easy
# diffing. Internal helpers (``_BreakerStateChangeListener``, ``_noop``,
# ``_raise``, ``_get_or_create_*``, ``_STATE_LABELS``) are intentionally
# OMITTED --- they are implementation detail and must not be re-exported
# through the package root.
__all__ = [
    "BreakerHealth",
    "NotificationCircuitBreaker",
    "make_circuit_breaker",
]
