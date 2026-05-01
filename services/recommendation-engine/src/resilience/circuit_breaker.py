"""pybreaker-based circuit breaker primitives (AAP R-16).

This module provides a thin, observability-aware wrapper around
``pybreaker.CircuitBreaker`` that:

  - Translates the AAP R-16 vocabulary ("failure RATE threshold over N
    calls") into pybreaker's native ``fail_max`` / ``reset_timeout`` model.
  - Emits structlog WARNING logs and Prometheus metrics on every state
    transition via :class:`BreakerStateChangeListener`.
  - Exposes :func:`call_through` (``breaker, coro, *args, **kwargs``) to
    wrap async calls — on OPEN, it raises
    :class:`pybreaker.CircuitBreakerError` so callers can invoke their
    fallback path (AAP R-20).

Composition rule
----------------
**RETRIES sit INSIDE the breaker.** A retry exhaustion counts as ONE
failure to the breaker, not N. Consumers should compose as::

    await call_through(breaker, retrying_coro, *args, **kwargs)

and NEVER wrap :func:`call_through` with ``retry_async`` from the
sibling :mod:`retry` module — doing so would poison the breaker because
each in-flight retry attempt would count as a separate failure.

Metrics exposed
---------------
- ``circuit_breaker_state{name}`` (Gauge: 0=closed, 1=half_open, 2=open).
- ``circuit_breaker_transitions_total{name, from, to}`` (Counter).

Compliance notes
----------------
- AAP R-16 — every outbound call has an explicit, configurable circuit
  breaker with thresholds for failure rate and call volume, an
  open-state duration, and a half-open probe policy.
- AAP R-20 — every dependency declares a fallback; callers catch
  :class:`pybreaker.CircuitBreakerError` to trigger that fallback.
- AAP R-26 — every state transition emits a structured JSON WARNING
  log with stable fields (``breaker``, ``from_state``, ``to_state``,
  ``fail_counter``, ``failure_rate``).
- AAP R-27 — Prometheus metrics ``circuit_breaker_state`` and
  ``circuit_breaker_transitions_total`` are exposed via the service's
  ``/metrics`` endpoint, scraped by Metricbeat into the ELK stack.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------
# ``datetime`` and ``timedelta`` power the OPEN-state timeout check in
# :func:`call_through`'s manual emulation path: pybreaker stores
# ``opened_at`` as a timezone-aware UTC datetime, and we replicate its
# native ``before_call`` logic to determine whether the OPEN duration has
# elapsed (allowing a HALF_OPEN probe) or whether the call must be
# short-circuited.
from datetime import datetime, timedelta, timezone

# ``Any`` types the variadic forwarding parameters of :func:`call_through`
# (the protected coroutine's argument list is intentionally opaque to the
# breaker layer). ``Awaitable`` and ``Callable`` together type the
# protected coroutine — ``Callable[..., Awaitable[T]]`` is the canonical
# spelling for "any callable that returns an awaitable" and matches
# ``async def`` functions, lambdas wrapping ``asyncio.ensure_future``,
# bound methods of async classes, and partial applications. ``Mapping``
# parameterizes the read-only ``_STATE_VALUE`` constant. ``TypeVar`` is
# the generic return type forwarded through :func:`call_through` so
# static checkers preserve the inner coroutine's return type. ``cast``
# is used in :func:`call_through` to preserve ``T`` across pybreaker's
# untyped ``call_async`` shim so callers get a precisely-typed return
# value rather than ``Any`` even under ``mypy --strict``.
from typing import Any, Awaitable, Callable, Mapping, TypeVar, cast

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
# ``pybreaker`` is the canonical thread-safe in-process circuit breaker
# library for Python (AAP Section 0.3.1). It implements the closed /
# half-open / open state machine with explicit transitions and provides
# the :class:`pybreaker.CircuitBreakerListener` extension point for
# observability side-effects.
import pybreaker

# ``structlog`` is the canonical structured-logging library across this
# service (AAP R-26). Calling ``structlog.get_logger(__name__)`` returns
# a lazy proxy whose processor chain is configured globally in
# ``src.main.lifespan``; log lines emitted from this module before that
# configuration is applied are buffered against the default processor
# chain and remain valid JSON.
import structlog

# ``REGISTRY`` is the global Prometheus default collector registry; we
# read its private ``_names_to_collectors`` dict so we can reuse already
# registered Counter / Gauge instances when the module is imported more
# than once (e.g., pytest's ``importlib.reload``). ``Counter`` and
# ``Gauge`` are the metric primitives used to expose the AAP R-27
# circuit-breaker observability surface.
from prometheus_client import REGISTRY, Counter, Gauge


# ---------------------------------------------------------------------------
# Module-level state — logger
# ---------------------------------------------------------------------------
# The structlog logger captures a stable ``logger=`` field that consumers
# can pivot on in Kibana to find every state-change event emitted from
# this module across the fleet. Bind once at import time; the lazy proxy
# defers its processor chain to runtime so changes applied later in the
# service lifespan still take effect.
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Re-exported state constants (AAP R-16 — half-open probe policy)
# ---------------------------------------------------------------------------
# pybreaker exposes the canonical state name constants
# (``"closed"``, ``"half-open"``, ``"open"``). Re-export them under the
# package's public namespace so consumers do not have to import
# ``pybreaker`` directly just to compare against breaker state strings.
# Using the library constants — not bare string literals — protects
# downstream code from any future rename in pybreaker.
STATE_CLOSED: str = pybreaker.STATE_CLOSED
STATE_HALF_OPEN: str = pybreaker.STATE_HALF_OPEN
STATE_OPEN: str = pybreaker.STATE_OPEN


# ---------------------------------------------------------------------------
# State-to-int encoding for the Prometheus gauge
# ---------------------------------------------------------------------------
# The ``circuit_breaker_state`` gauge encodes the breaker's current state
# as a small integer (closed=0, half_open=1, open=2). This avoids the
# high-cardinality / dropped-time-series problems that come with
# encoding the state as a Prometheus label, and matches Prometheus best
# practice for enum-like fields with a small, closed-universe mapping.
# Kibana / Grafana panels can reverse-map the integer back to a state
# name via a lookup table or a regex panel transformation.
_STATE_VALUE: Mapping[str, int] = {
    pybreaker.STATE_CLOSED: 0,
    pybreaker.STATE_HALF_OPEN: 1,
    pybreaker.STATE_OPEN: 2,
}


# ---------------------------------------------------------------------------
# Prometheus metrics — idempotent registration
# ---------------------------------------------------------------------------
def _get_or_create_counter(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...],
) -> Counter:
    """Return an existing Counter from REGISTRY or register a new one.

    Re-importing this module in tests (or in any environment that calls
    ``importlib.reload``) MUST NOT raise the
    ``ValueError: Duplicated timeseries in CollectorRegistry`` that
    ``Counter(name, ...)`` would otherwise raise on second registration.
    We therefore peek into the registry's private
    ``_names_to_collectors`` mapping and reuse the existing collector
    when present.

    Args:
        name: The Prometheus metric name (e.g.,
            ``circuit_breaker_transitions_total``).
        documentation: Human-readable HELP string emitted in the
            ``/metrics`` exposition output.
        labelnames: Tuple of label keys.

    Returns:
        The freshly-registered or pre-existing :class:`Counter` instance.
    """
    # ``REGISTRY._names_to_collectors`` is a dict keyed by the metric's
    # base name (and its ``_total`` / ``_created`` suffixed variants).
    # Looking up the bare name is sufficient because Counter registers
    # itself under its base name first.
    existing = REGISTRY._names_to_collectors.get(name)  # noqa: SLF001
    if existing is not None:
        # Reuse the registered collector. The ``type: ignore`` covers
        # the fact that REGISTRY stores collectors as their abstract
        # base class; we know it's a Counter because we registered it
        # as one and the name -> collector mapping is unique.
        return existing  # type: ignore[return-value]
    return Counter(name, documentation, labelnames=labelnames)


def _get_or_create_gauge(
    name: str,
    documentation: str,
    labelnames: tuple[str, ...],
) -> Gauge:
    """Return an existing Gauge from REGISTRY or register a new one.

    Same idempotent-registration semantics as
    :func:`_get_or_create_counter`. See that function's docstring for
    the rationale.

    Args:
        name: The Prometheus metric name (e.g., ``circuit_breaker_state``).
        documentation: Human-readable HELP string.
        labelnames: Tuple of label keys.

    Returns:
        The freshly-registered or pre-existing :class:`Gauge` instance.
    """
    existing = REGISTRY._names_to_collectors.get(name)  # noqa: SLF001
    if existing is not None:
        return existing  # type: ignore[return-value]
    return Gauge(name, documentation, labelnames=labelnames)


# Gauge exposing each breaker's current state as a small integer.
#   name (label) — the breaker's unique identifier (e.g., "product_service").
# Value semantics: 0=closed, 1=half_open, 2=open.
# The state itself is the VALUE of the gauge, not a label, to keep
# cardinality bounded — one time series per breaker, regardless of how
# many state transitions occur.
CIRCUIT_BREAKER_STATE: Gauge = _get_or_create_gauge(
    "circuit_breaker_state",
    "Current state of a circuit breaker (0=closed, 1=half_open, 2=open).",
    labelnames=("name",),
)

# Counter exposing the cumulative count of state transitions per breaker.
#   name (label) — the breaker's unique identifier.
#   from (label) — the previous state name ("closed", "half-open", "open",
#                  or "none" on the first transition where there was no
#                  prior state).
#   to   (label) — the new state name (same vocabulary as ``from``).
# The label keys ``from`` and ``to`` are valid Prometheus label names
# (Prometheus has no reserved-keyword restriction on label names; only
# the metric name and label values are subject to character constraints).
# In Python, however, ``from`` is a reserved keyword and cannot be used
# as a kwarg directly — call sites use ``**{"from": ..., "to": ...}``.
CIRCUIT_BREAKER_TRANSITIONS_TOTAL: Counter = _get_or_create_counter(
    "circuit_breaker_transitions_total",
    "Count of circuit-breaker state transitions.",
    labelnames=("name", "from", "to"),
)


# ---------------------------------------------------------------------------
# State-change listener — observability side-effects
# ---------------------------------------------------------------------------
class BreakerStateChangeListener(pybreaker.CircuitBreakerListener):
    """pybreaker listener emitting structlog + Prometheus on state changes.

    Registered automatically by :func:`build_circuit_breaker`. Not
    intended for direct instantiation by consumers — callers obtain a
    fully-wired breaker from the factory and never need to touch
    listeners directly.

    On every state transition (CLOSED -> OPEN -> HALF_OPEN -> CLOSED),
    this listener:

    1. Emits a WARNING-level structured log line with the stable event
       name ``circuit_breaker_state_change`` and the canonical fields
       (``breaker``, ``from_state``, ``to_state``, ``fail_counter``,
       ``failure_rate``) so Kibana panels can pivot per-breaker on
       the AAP R-26 / R-28 dashboards.
    2. Increments the ``circuit_breaker_transitions_total`` Counter
       with labels ``name``, ``from``, ``to``.
    3. Updates the ``circuit_breaker_state`` Gauge to the new state's
       integer encoding.

    Note:
        ``failure_rate`` is read defensively via :func:`getattr` because
        pybreaker 1.x does not expose it as a top-level attribute on the
        breaker; logging ``None`` is acceptable and signals "not
        available" to dashboard queries that handle missing fields.
    """

    def state_change(
        self,
        cb: pybreaker.CircuitBreaker,
        old_state: pybreaker.CircuitBreakerState | None,
        new_state: pybreaker.CircuitBreakerState,
    ) -> None:
        """Handle a breaker state transition (override of base listener).

        Args:
            cb: The breaker whose state changed.
            old_state: The previous state, or ``None`` on the very first
                transition where the breaker had no prior state.
            new_state: The new state. Always non-``None``.
        """
        # ``CircuitBreakerState.name`` is the canonical state-name
        # string ("closed", "half-open", "open"). Defaulting to "none"
        # for the no-prior-state case keeps the metric labels populated
        # and the log line readable in Kibana without special-casing.
        old_name = old_state.name if old_state is not None else "none"
        new_name = new_state.name

        # AAP R-26: structured JSON log, WARNING level, stable event
        # name. The breaker's name is the primary pivot field; the
        # ``from_state`` / ``to_state`` pair drives the transition
        # heatmap on the resilience dashboard.
        logger.warning(
            "circuit_breaker_state_change",
            breaker=cb.name,
            from_state=old_name,
            to_state=new_name,
            fail_counter=cb.fail_counter,
            failure_rate=getattr(cb, "failure_rate", None),
        )

        # AAP R-27: Prometheus counter increment. ``from`` and ``to``
        # must be passed via ``**{...}`` because ``from`` is a reserved
        # Python keyword and cannot be used as a bare kwarg.
        CIRCUIT_BREAKER_TRANSITIONS_TOTAL.labels(
            name=cb.name,
            **{"from": old_name, "to": new_name},
        ).inc()

        # AAP R-27: Prometheus gauge update. Defaulting to -1 for any
        # unrecognized state name (forward-compat against future
        # pybreaker additions) lets Grafana/Kibana visualize the
        # anomaly rather than dropping the time series silently.
        CIRCUIT_BREAKER_STATE.labels(name=cb.name).set(
            _STATE_VALUE.get(new_name, -1),
        )


# ---------------------------------------------------------------------------
# Factory — build a configured circuit breaker (AAP R-16)
# ---------------------------------------------------------------------------
def build_circuit_breaker(
    name: str,
    *,
    failure_rate_threshold_pct: int,
    call_volume_threshold: int,
    open_duration_ms: int,
    half_open_permitted_calls: int,
    exclude_exception_types: tuple[type[BaseException], ...] = (),
) -> pybreaker.CircuitBreaker:
    """Construct a configured :class:`pybreaker.CircuitBreaker` (AAP R-16).

    The factory translates the AAP R-16 user-facing vocabulary
    ("failure RATE threshold over N calls") into pybreaker's native
    ``fail_max`` (absolute failure count that opens the breaker) by
    computing::

        fail_max = max(1, round(call_volume_threshold *
                                failure_rate_threshold_pct / 100.0))

    For the common config (50% threshold, volume 20) this yields
    ``fail_max == 10`` — exactly half of the call volume must fail
    before the breaker opens, matching the user-facing intent.

    Args:
        name: Unique identifier for the breaker (also the ``name``
            label on exported metrics). Convention: the downstream
            dependency it protects, e.g., ``"product_service"``.
        failure_rate_threshold_pct: Failure rate (1..100) above which
            the breaker OPENS, evaluated over the last
            ``call_volume_threshold`` calls. pybreaker approximates
            this via ``fail_max`` (see formula above).
        call_volume_threshold: Minimum number of calls used to compute
            ``fail_max``. Must be ``>= 1``.
        open_duration_ms: How long the breaker stays OPEN before
            allowing probe calls (HALF_OPEN). Maps to pybreaker's
            ``reset_timeout`` (converted from milliseconds to seconds).
        half_open_permitted_calls: Number of probe calls permitted in
            the HALF_OPEN state before either closing (all succeed) or
            re-opening (any fails). Recorded on the breaker as a
            custom attribute for downstream inspection; pybreaker's
            built-in default is single-probe, so values ``> 1`` do not
            affect the underlying state machine — a future pybreaker
            upgrade that natively supports multi-probe HALF_OPEN can
            be wired up here without changing the factory's signature.
        exclude_exception_types: Exceptions that should NOT count as
            failures (e.g., ``httpx.HTTPStatusError`` wrapping a 4xx
            client error — the upstream is healthy, the caller sent
            bad input). Passed directly to pybreaker's ``exclude``.

    Returns:
        A :class:`pybreaker.CircuitBreaker` with a
        :class:`BreakerStateChangeListener` attached. Initial state:
        CLOSED. The ``circuit_breaker_state{name=...}`` gauge is
        seeded to 0 so the metric appears in ``/metrics`` output
        before any state change occurs.

    Raises:
        ValueError: If any of the threshold parameters violate their
            documented ranges (negative durations, zero volumes,
            out-of-range percentages, etc.).
    """
    # AAP R-16: validate every tunable on the way in. Failing fast at
    # the factory call site catches misconfigurations during service
    # bootstrap (when ``Settings`` is constructed) instead of at the
    # first protected call deep inside the request path.
    if failure_rate_threshold_pct <= 0 or failure_rate_threshold_pct > 100:
        raise ValueError(
            f"failure_rate_threshold_pct must be in (0, 100] for breaker "
            f"{name!r}; got {failure_rate_threshold_pct!r}",
        )
    if call_volume_threshold < 1:
        raise ValueError(
            f"call_volume_threshold must be >= 1 for breaker {name!r}; "
            f"got {call_volume_threshold!r}",
        )
    if open_duration_ms < 0:
        raise ValueError(
            f"open_duration_ms must be >= 0 for breaker {name!r}; "
            f"got {open_duration_ms!r}",
        )
    if half_open_permitted_calls < 1:
        raise ValueError(
            f"half_open_permitted_calls must be >= 1 for breaker "
            f"{name!r}; got {half_open_permitted_calls!r}",
        )

    # Translate "failure rate over N calls" into pybreaker's absolute
    # ``fail_max``. The ``max(1, ...)`` floor guarantees the breaker
    # always trips on at least one failure even when the rate * volume
    # rounds to zero (e.g., 1% of 20 = 0.2 -> 0 -> floored to 1).
    fail_max = max(
        1,
        round(call_volume_threshold * failure_rate_threshold_pct / 100.0),
    )

    # ``reset_timeout`` is in seconds; convert from milliseconds with a
    # plain float division so partial-second values (e.g. 500 ms) are
    # preserved precisely.
    reset_timeout_seconds = open_duration_ms / 1000.0

    # Construct the breaker. ``exclude=None`` is pybreaker's "no
    # exclusions" sentinel — passing an empty list also works but
    # ``None`` is the canonical choice when no exclusions are desired.
    breaker = pybreaker.CircuitBreaker(
        name=name,
        fail_max=fail_max,
        reset_timeout=reset_timeout_seconds,
        exclude=list(exclude_exception_types) if exclude_exception_types else None,
        listeners=[BreakerStateChangeListener()],
    )

    # Record the half-open policy on the breaker for downstream
    # inspection. pybreaker's built-in HALF_OPEN model permits a
    # single probe; this attribute reserves the public surface so any
    # future pybreaker upgrade that natively supports multi-probe can
    # be wired up here without breaking consumers that already read
    # ``breaker.half_open_permitted_calls``.
    breaker.half_open_permitted_calls = half_open_permitted_calls  # type: ignore[attr-defined]

    # Seed the state gauge so the metric is present in ``/metrics``
    # output BEFORE any state change occurs. Without this, dashboards
    # that aggregate over ``circuit_breaker_state`` would show "no
    # data" for healthy breakers — a confusing operator experience.
    CIRCUIT_BREAKER_STATE.labels(name=name).set(_STATE_VALUE[pybreaker.STATE_CLOSED])

    # AAP R-26: structured initialization log so operators can confirm
    # the breaker came up with the expected thresholds. INFO level is
    # appropriate here — initialization is an expected event, not a
    # warning.
    logger.info(
        "circuit_breaker_initialized",
        breaker=name,
        fail_max=fail_max,
        reset_timeout_seconds=reset_timeout_seconds,
        failure_rate_threshold_pct=failure_rate_threshold_pct,
        call_volume_threshold=call_volume_threshold,
        half_open_permitted_calls=half_open_permitted_calls,
        exclude_exception_types=[c.__name__ for c in exclude_exception_types],
    )

    return breaker


# ---------------------------------------------------------------------------
# Async call-through wrapper
# ---------------------------------------------------------------------------
# Generic type variable for the protected coroutine's return type.
# Declared at module scope so static type checkers can infer the awaited
# type without re-parameterizing on each call.
T = TypeVar("T")


async def call_through(
    breaker: pybreaker.CircuitBreaker,
    func: Callable[..., Awaitable[T]],
    *args: Any,
    **kwargs: Any,
) -> T:
    """Route an async call through a circuit breaker.

    The breaker observes the OUTCOME of ``func`` (success or failure)
    and updates its internal state machine accordingly. When the
    breaker is OPEN, the call is short-circuited with
    :class:`pybreaker.CircuitBreakerError` and ``func`` is NOT
    invoked — the caller catches this exception to trigger their
    AAP R-20 fallback path.

    Two implementation paths are supported in priority order:

    1. **Native** — pybreaker exposes ``CircuitBreaker.call_async``,
       which is the canonical 1.x async API. When invokable
       end-to-end (i.e., the optional Tornado runtime is available),
       we delegate to it directly so success/failure accounting and
       state transitions happen inside pybreaker's lock under a
       single critical section.

    2. **Manual emulation** — pybreaker 1.x's ``call_async`` is
       implemented atop ``tornado.gen.coroutine`` and raises
       :class:`NameError` at call time when Tornado is not installed
       (the recommendation engine does NOT pin Tornado, so this is
       the de-facto runtime path). The fallback path:
         a. Eagerly checks ``breaker.current_state`` and raises
            :class:`pybreaker.CircuitBreakerError` if OPEN, mirroring
            pybreaker's own short-circuit semantics.
         b. Awaits the protected coroutine.
         c. Routes the success/failure outcome through the breaker's
            synchronous ``call`` helper (using the tiny ``_noop`` /
            ``_raise`` shims below) so listeners fire and state
            transitions happen exactly as they would on the native
            path.

    Args:
        breaker: The breaker returned by :func:`build_circuit_breaker`.
        func: An awaitable callable (typically an ``async def``
            function or a bound async method) to protect.
        *args: Positional arguments forwarded to ``func``.
        **kwargs: Keyword arguments forwarded to ``func``.

    Returns:
        Whatever ``func`` returns.

    Raises:
        pybreaker.CircuitBreakerError: When the breaker is OPEN
            (short-circuit) or when the breaker transitions to OPEN
            while accounting for ``func``'s failure. Callers catch
            EXACTLY this type to trigger their fallback (AAP R-20).
        Exception: Any exception raised by ``func`` is propagated to
            the caller AFTER being recorded as a failure on the
            breaker (unless the exception type is in the breaker's
            ``exclude`` set, in which case it is recorded as a
            success and still re-raised).
    """
    # ---- Path 1: native call_async (preferred when Tornado is wired) ----
    # ``getattr`` with default ``None`` guards forward-compat against
    # any future pybreaker version that drops or renames call_async.
    # The cast preserves the caller's return type ``T`` through
    # pybreaker's untyped ``call_async`` shim — the value is the awaited
    # result of ``func``, which is genuinely of type ``T``.
    call_async = getattr(breaker, "call_async", None)
    if callable(call_async):
        try:
            return cast("T", await call_async(func, *args, **kwargs))
        except NameError:
            # pybreaker 1.x's call_async is decorated with
            # ``@tornado.gen.coroutine``; invoking it without the
            # ``tornado`` package installed raises a NameError on
            # ``gen``. Fall through to the manual path. We catch
            # NameError narrowly so genuine name errors inside the
            # protected coroutine still propagate normally.
            pass

    # ---- Path 2: manual emulation (default at runtime) ----
    # Eagerly check the breaker's current state. This mirrors
    # pybreaker's :meth:`CircuitOpenState.before_call`:
    #
    #   - If OPEN and the ``reset_timeout`` window has NOT elapsed,
    #     short-circuit by raising :class:`CircuitBreakerError` —
    #     ``func`` is NOT invoked.
    #   - If OPEN and the timeout HAS elapsed, transition the breaker
    #     to HALF_OPEN (firing the state-change listener) and proceed
    #     to invoke ``func`` as a probe call. A successful probe will
    #     close the breaker via the ``breaker.call(_noop)`` success
    #     accounting below; a failed probe will reopen it via the
    #     ``breaker.call(_raise, exc)`` failure accounting below.
    #   - If CLOSED or HALF_OPEN, fall through and invoke ``func``.
    if breaker.current_state == pybreaker.STATE_OPEN:
        # ``opened_at`` is set by pybreaker to ``datetime.now(UTC)`` at
        # the moment the breaker tripped. Reading the attribute via
        # ``getattr`` defends against custom storage backends that may
        # not implement it identically.
        storage = breaker._state_storage  # noqa: SLF001
        opened_at: datetime | None = getattr(storage, "opened_at", None)
        timeout_delta = timedelta(seconds=breaker.reset_timeout)

        if opened_at is not None and datetime.now(timezone.utc) < opened_at + timeout_delta:
            # Reset timeout window has NOT elapsed. Short-circuit.
            raise pybreaker.CircuitBreakerError(
                f"Circuit breaker {breaker.name!r} is OPEN",
            )

        # Reset timeout window has elapsed (or ``opened_at`` is missing,
        # which we treat as "ready to probe" for safety). Transition to
        # HALF_OPEN — this fires the state-change listener and leaves
        # the breaker primed for a single probe call. The probe call's
        # outcome is recorded by the success/failure accounting below.
        breaker.half_open()

    try:
        result = await func(*args, **kwargs)
    except pybreaker.CircuitBreakerError:
        # If the protected coroutine itself raised CircuitBreakerError
        # (e.g., an inner call_through fanned out to a downstream
        # breaker), propagate without double-accounting the failure on
        # the outer breaker — the inner breaker has already recorded
        # the outage upstream.
        raise
    except BaseException as exc:  # noqa: BLE001
        # Drive pybreaker's failure accounting via its synchronous
        # ``call`` helper. The ``_raise`` shim simply re-raises the
        # captured exception inside breaker.call's lock, which:
        #   1. Triggers ``before_call`` on listeners (no-op by default).
        #   2. Calls ``_handle_error(exc)`` which:
        #      - increments fail_counter (if exc is NOT excluded);
        #      - fires listeners.failure (which may include our
        #        BreakerStateChangeListener if the threshold trips);
        #      - re-raises ``exc``.
        try:
            breaker.call(_raise, exc)
        except pybreaker.CircuitBreakerError:
            # The breaker tripped to OPEN while accounting for this
            # failure (we crossed the fail_max boundary). Surface the
            # OPEN signal to the caller so they enter their fallback
            # path immediately rather than continuing to stack
            # additional failures.
            raise
        except BaseException:  # noqa: BLE001
            # ``breaker.call(_raise, exc)`` re-raised our original
            # exception (or the excluded-exception fast path's
            # success-record-then-reraise). Discard it here because
            # we re-raise the original below.
            pass
        # Re-raise the original exception with its original traceback
        # intact. The breaker has already done its accounting.
        raise

    # Record success by routing a no-op through the breaker. This:
    #   - resets the fail_counter back to 0 (closed-state behavior);
    #   - increments the success_counter (half-open-state behavior),
    #     which closes the breaker after success_threshold probes;
    #   - fires listeners.success (no-op by default).
    try:
        breaker.call(_noop)
    except pybreaker.CircuitBreakerError:
        # Edge case: the breaker transitioned to OPEN between
        # ``func`` succeeding and the success bookkeeping (e.g.,
        # another concurrent call failed and tipped the threshold).
        # ``func``'s result is still valid for the caller to use, so
        # we swallow the OPEN signal here and return the result.
        pass
    return result


# ---------------------------------------------------------------------------
# Internal shims for the manual call_through emulation path
# ---------------------------------------------------------------------------
def _noop() -> None:
    """No-op success marker passed to ``breaker.call`` to record success.

    Routing a callable that simply returns ``None`` through the breaker
    triggers its success-accounting path: the fail_counter is reset, the
    success_counter is incremented (in HALF_OPEN), and listeners' success
    callback is invoked. The function does no real work — it exists
    purely to give pybreaker a callable to wrap.
    """
    return None


def _raise(exc: BaseException) -> None:
    """Failure marker passed to ``breaker.call`` to record a failure.

    Routing a callable that re-raises a captured exception through the
    breaker triggers its failure-accounting path: the fail_counter is
    incremented (if the exception is not in the breaker's ``exclude``
    set), listeners' failure callback is invoked, and the breaker may
    transition to OPEN. ``breaker.call`` then re-raises the exception,
    which we discard at the call site because we re-raise the original
    ourselves to preserve the traceback.

    Args:
        exc: The exception to re-raise inside the breaker's call
            machinery.

    Raises:
        BaseException: Always — re-raises ``exc``.
    """
    raise exc


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------
# Alphabetical ordering for stable ``from circuit_breaker import *`` and
# easy diffing. Internal helpers (``_noop``, ``_raise``,
# ``_get_or_create_*``) are intentionally OMITTED — they are
# implementation detail and must not be re-exported through
# ``src.resilience``.
__all__ = [
    "BreakerStateChangeListener",
    "CIRCUIT_BREAKER_STATE",
    "CIRCUIT_BREAKER_TRANSITIONS_TOTAL",
    "STATE_CLOSED",
    "STATE_HALF_OPEN",
    "STATE_OPEN",
    "_STATE_VALUE",
    "build_circuit_breaker",
    "call_through",
]
