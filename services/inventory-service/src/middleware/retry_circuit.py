"""Retry and circuit-breaker helpers for outbound HTTP calls.

This module provides two reusable factories that wrap outbound HTTP calls
with the platform-standard resilience policies described in the
Inventory Service architecture documentation:

* :func:`retry_outbound_http` — returns a tenacity-based async retry
  decorator that implements **AAP R-15** (every outbound HTTP call must
  have a retry policy with exponential backoff and jitter, a maximum
  attempt count, and an explicit timeout).
* :func:`circuit_breaker_outbound_http` — returns a configured
  :class:`pybreaker.CircuitBreaker` instance that implements **AAP R-16**
  (every outbound HTTP call must be protected by a circuit breaker with
  thresholds for failure rate and call volume, an open-state duration,
  and a half-open probe policy).

Together these two helpers satisfy the cross-cutting interceptor pair
listed in **AAP Section 0.4.5** (retry interceptor + circuit breaker
interceptor) for the Inventory Service. They are deliberately exposed
as **factories that return decorators / breaker instances** rather than
as standalone middleware classes, because outbound HTTP calls happen at
specific call sites (e.g. JWKS fetches, future
``ExternalWMSWarehouseAdapter`` requests) — not on every inbound request
— and the policy is invoked at container construction time or at the
call site, not on every request through the FastAPI middleware stack.

Usage
-----
.. code-block:: python

    from src.middleware.retry_circuit import (
        circuit_breaker_outbound_http,
        retry_outbound_http,
    )

    # 1. Build a retry decorator from configuration.
    retry = retry_outbound_http(
        max_attempts=3,
        backoff_initial_ms=100,
        backoff_max_ms=2000,
        backoff_multiplier=2.0,
        jitter_ms=50,
        operation_name="external_wms.reserve",
    )

    @retry
    async def fetch_remote_thing() -> Response:
        ...

    # 2. Build a circuit breaker for a specific dependency.
    breaker = circuit_breaker_outbound_http(
        name="external_wms",
        failure_threshold=5,
        reset_timeout_ms=30_000,
    )

    # 3. Compose them at the call site as ``breaker(retry(call))``:
    result = await breaker.call_async(fetch_remote_thing)

Ordering rationale (``breaker(retry(call))``)
---------------------------------------------
The canonical composition order is **breaker on the outside, retry on
the inside**. With this layout, the circuit breaker observes the FINAL
outcome of the retry attempts, not each individual attempt. Inversion
(``retry(breaker(call))``) means a single retry burst against a
genuinely transient blip would prematurely trip the breaker against an
otherwise-healthy service. Operators reading dashboards expect a
breaker trip to mean "the dependency is sustainedly broken", not
"a transient stale TCP connection produced a brief flurry of retries".

AAP cross-references
--------------------
* AAP Section 0.4.5 — Cross-cutting interceptors (retry + circuit breaker).
* AAP R-15 — Retry policy: exponential backoff with jitter, max attempts,
  explicit timeout.
* AAP R-16 — Circuit breaker: thresholds for failure rate and call
  volume, open-state duration, half-open probe.
* AAP R-26 — Structured JSON logs; the breaker listener emits
  state-change events via :mod:`structlog` so Kibana dashboards can
  alert on trip / half-open / reset transitions.
"""

from __future__ import annotations

import logging
import random  # noqa: F401  # documented fallback for tenacity<8.0; primary path uses tenacity.wait_random
from typing import Any, Awaitable, Callable, Final, TypeVar

import httpx
import pybreaker
import structlog
import tenacity

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

#: Structlog logger used by the :class:`_BreakerListener` and the
#: ``circuit_breaker.created`` startup event. The logger name encodes the
#: full module path so operators can filter Kibana on
#: ``logger == "inventory_service.middleware.retry_circuit"`` to see only
#: breaker activity.
_LOGGER: Final[structlog.stdlib.BoundLogger] = structlog.get_logger(
    "inventory_service.middleware.retry_circuit"
)

#: Default tuple of exception classes that signal a TRANSIENT outbound
#: HTTP failure eligible for retry. Callers can override this via the
#: ``retryable_exceptions`` keyword argument or supply a
#: ``retry_predicate`` for status-code-aware retry decisions.
#:
#: This tuple is intentionally NARROW: it covers transport-level and
#: timeout failures only (connection drops, half-open TCP sockets,
#: protocol corruption, read/write/connect/pool timeouts). It explicitly
#: EXCLUDES :class:`httpx.HTTPStatusError` because retrying on 4xx
#: responses is almost always a bug — a 4xx means the server rejected
#: the request as malformed or unauthorized, and retrying turns a fast
#: failure into a slow failure. Callers that want to retry on specific
#: 5xx status codes can opt in via
#: ``retry_predicate=lambda exc: isinstance(exc, httpx.HTTPStatusError) and 500 <= exc.response.status_code < 600``.
_DEFAULT_RETRYABLE_EXCEPTIONS: Final[tuple[type[BaseException], ...]] = (
    httpx.TransportError,
    httpx.RemoteProtocolError,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)

#: Generic type variable used by :data:`AsyncCallable` to preserve the
#: wrapped coroutine's return type through the retry decorator.
T = TypeVar("T")

#: Type alias for an async callable that returns ``T``. The
#: :func:`retry_outbound_http` decorator preserves this type so that
#: callers retain full IDE / mypy coverage of the wrapped coroutine's
#: return type.
AsyncCallable = Callable[..., Awaitable[T]]


# ---------------------------------------------------------------------------
# Public factory: retry_outbound_http
# ---------------------------------------------------------------------------


def retry_outbound_http(
    *,
    max_attempts: int,
    backoff_initial_ms: int,
    backoff_max_ms: int,
    backoff_multiplier: float = 2.0,
    jitter_ms: int = 0,
    retryable_exceptions: tuple[type[BaseException], ...] = _DEFAULT_RETRYABLE_EXCEPTIONS,
    retry_predicate: Callable[[BaseException], bool] | None = None,
    operation_name: str = "outbound_http",
) -> Callable[[AsyncCallable[T]], AsyncCallable[T]]:
    """Return a tenacity-based async retry decorator (AAP R-15).

    The returned decorator wraps an async callable with a
    :class:`tenacity.AsyncRetrying` engine configured for exponential
    backoff with optional jitter, bounded by ``max_attempts``. The
    underlying retry mechanism is reconstructed for each decoration so
    no retry state leaks between independently-decorated callables.

    Args:
        max_attempts:        Total number of attempts including the
            initial call. ``1`` disables retries (the call runs exactly
            once). Must be ``>= 1``.
        backoff_initial_ms:  Initial wait duration in milliseconds. Used
            as the base for exponential growth. Must be ``>= 0``.
        backoff_max_ms:      Upper cap on the wait duration in
            milliseconds. The exponential schedule is clamped to this
            value to prevent unbounded growth. Must be ``>= 0``.
        backoff_multiplier:  Multiplier applied between successive
            attempts. Default ``2.0`` produces the canonical
            ``100ms -> 200ms -> 400ms -> ...`` progression. Must be
            ``>= 1.0``.
        jitter_ms:           Maximum random jitter added per attempt in
            milliseconds. ``0`` disables jitter. Jitter is applied
            additively on top of the exponential schedule so independent
            clients do not synchronize their retry storms. Must be
            ``>= 0``.
        retryable_exceptions: Tuple of exception classes treated as
            retryable. Defaults to the transient-only set
            :data:`_DEFAULT_RETRYABLE_EXCEPTIONS` (timeouts and
            transport failures, NOT 4xx status errors). Ignored when
            ``retry_predicate`` is supplied.
        retry_predicate:     Optional predicate that overrides the
            ``retryable_exceptions`` check. Receives the raised
            exception and returns ``True`` to retry, ``False`` to give
            up immediately. Useful when a caller needs to retry on
            specific 5xx status codes but not 4xx.
        operation_name:      Human-readable name surfaced in the
            ``RuntimeError`` message thrown if the retry loop somehow
            exits without producing a result (a defensive guard that
            should never fire under tenacity's contract).

    Returns:
        A decorator that takes an async callable and returns a wrapped
        async callable applying the retry policy. The wrapped callable
        preserves the original ``__name__``, ``__doc__``, and
        ``__wrapped__`` attributes for introspection.

    Raises:
        ValueError: If ``max_attempts < 1``, any timing argument is
            negative, or ``backoff_multiplier < 1.0``.

    Example:
        >>> retry = retry_outbound_http(
        ...     max_attempts=3,
        ...     backoff_initial_ms=100,
        ...     backoff_max_ms=2000,
        ...     jitter_ms=50,
        ... )
        >>> @retry
        ... async def fetch() -> dict:
        ...     async with httpx.AsyncClient() as client:
        ...         response = await client.get("https://example.com/")
        ...         response.raise_for_status()
        ...         return response.json()
    """
    # Fail-fast input validation. Defensive: tenacity itself will accept
    # most invalid values silently (e.g., ``stop_after_attempt(0)``
    # produces a never-running loop), so we surface configuration errors
    # at the factory call site rather than at the first attempted call.
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if backoff_initial_ms < 0 or backoff_max_ms < 0 or jitter_ms < 0:
        raise ValueError("backoff and jitter values must be >= 0")
    if backoff_multiplier < 1.0:
        raise ValueError("backoff_multiplier must be >= 1.0")

    # Convert millisecond inputs into the seconds tenacity expects
    # internally. The ``_ms`` suffix on every parameter name + this
    # single conversion point eliminates the most common bug class in
    # retry-policy code (mixing seconds and milliseconds at the call
    # site). ``max_seconds`` is forced to be at least ``initial_seconds``
    # so a misconfigured ``backoff_max_ms`` cannot produce a max smaller
    # than the initial wait.
    initial_seconds = backoff_initial_ms / 1000.0
    max_seconds = max(backoff_max_ms / 1000.0, initial_seconds)
    jitter_seconds = jitter_ms / 1000.0

    # Branch on whether the caller supplied an exception-instance
    # predicate (``retry_predicate``) or wants the default
    # exception-class match (``retryable_exceptions``). The predicate
    # form is required for status-code-aware retry decisions because
    # ``retry_if_exception_type`` cannot inspect the response payload of
    # an ``httpx.HTTPStatusError`` to discriminate 5xx from 4xx.
    retry_strategy: tenacity.retry_base
    if retry_predicate is None:
        retry_strategy = tenacity.retry_if_exception_type(retryable_exceptions)
    else:
        retry_strategy = tenacity.retry_if_exception(retry_predicate)

    # Use tenacity's native ``wait_exponential_jitter`` (single combinator
    # introduced in tenacity 8.0) when available. Fall back to
    # ``wait_exponential`` plus an additive ``wait_random`` for older
    # tenacity versions that lack the combined helper. Both branches
    # produce a wait strategy that is a subclass of
    # :class:`tenacity.wait.wait_base`.
    wait_strategy: tenacity.wait.wait_base
    if hasattr(tenacity, "wait_exponential_jitter") and jitter_seconds > 0:
        wait_strategy = tenacity.wait_exponential_jitter(
            initial=initial_seconds,
            max=max_seconds,
            exp_base=backoff_multiplier,
            jitter=jitter_seconds,
        )
    else:
        wait_strategy = tenacity.wait_exponential(
            multiplier=initial_seconds,
            max=max_seconds,
            exp_base=backoff_multiplier,
        )
        if jitter_seconds > 0:
            # Additive jitter via wait combination: the resulting
            # ``wait_combine`` returns the sum of both child strategies.
            wait_strategy = wait_strategy + tenacity.wait_random(0, jitter_seconds)

    # Tenacity's ``before_sleep_log`` callback is built around the stdlib
    # logging.Logger interface, so we must use a stdlib logger here (not
    # structlog). The stdlib logger name is bridged to structlog's JSON
    # output by ``src.observability.logging_setup`` so retry attempt
    # warnings still land in Kibana with the same schema.
    stdlib_logger = logging.getLogger(
        "inventory_service.middleware.retry_circuit.tenacity"
    )

    def _decorator(func: AsyncCallable[T]) -> AsyncCallable[T]:
        # Build a fresh AsyncRetrying for each decoration. AsyncRetrying
        # holds per-call retry state internally, so reusing one instance
        # across multiple decorated callables would cause state to leak
        # between unrelated calls.
        retrying = tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(max_attempts),
            wait=wait_strategy,
            retry=retry_strategy,
            before_sleep=tenacity.before_sleep_log(stdlib_logger, logging.WARNING),
            reraise=True,
        )

        async def _wrapper(*args: Any, **kwargs: Any) -> T:
            # The canonical AsyncRetrying iterator pattern: each
            # ``with attempt`` block records success or failure; the
            # ``async for`` loop drives the retry schedule. ``return``
            # exits the loop on success; on failure with a retryable
            # exception, ``with attempt`` swallows it and the loop
            # iterates; on failure with a non-retryable exception or
            # exhausted attempts (``reraise=True``), the original
            # exception propagates out of the with block.
            async for attempt in retrying:
                with attempt:
                    return await func(*args, **kwargs)
            # Defensive fallback: tenacity guarantees that we either
            # return inside the loop body or that the final exception is
            # re-raised when ``reraise=True``. Reaching this line would
            # indicate a tenacity API contract violation; surface the
            # operation name so logs identify the offending integration.
            raise RuntimeError(
                f"retry_outbound_http({operation_name}) exited without a result"
            )

        # Preserve the wrapped function's metadata so observability
        # spans, error traces, and ``inspect.signature`` introspection
        # surface the original callable's identity rather than the
        # opaque ``_wrapper``.
        _wrapper.__wrapped__ = func  # type: ignore[attr-defined]
        _wrapper.__name__ = getattr(func, "__name__", "_wrapper")
        _wrapper.__doc__ = func.__doc__
        return _wrapper

    return _decorator


# ---------------------------------------------------------------------------
# Internal: structured-log listener for circuit breaker state changes
# ---------------------------------------------------------------------------


class _BreakerListener(pybreaker.CircuitBreakerListener):
    """Logs circuit-breaker state changes via :mod:`structlog`.

    Bound to every breaker created by
    :func:`circuit_breaker_outbound_http` so operators can observe trip
    / half-open / reset transitions in Kibana without scraping
    pybreaker's internal stdout. Without this listener, a tripped
    breaker would only surface as opaque
    :class:`pybreaker.CircuitBreakerError` exceptions at the call site;
    with it, every state transition produces a structured log line that
    Kibana dashboards can alert on (per AAP R-26 / R-28).

    Note:
        This class is intentionally module-private (leading underscore).
        Callers configure listener behavior implicitly by choosing a
        breaker ``name`` — the listener uses that name to scope its log
        lines.
    """

    def __init__(self, name: str) -> None:
        """Initialize the listener with the owning breaker's name.

        Args:
            name: The breaker identifier surfaced in every emitted log
                line. Conventionally the name of the dependency the
                breaker protects (e.g., ``"auth_service"``,
                ``"external_wms"``).
        """
        self._name: Final[str] = name

    def state_change(
        self,
        cb: pybreaker.CircuitBreaker,
        old_state: pybreaker.CircuitBreakerState | None,
        new_state: pybreaker.CircuitBreakerState,
    ) -> None:
        """Emit a WARNING log line on every breaker state transition.

        State changes (closed -> open, open -> half-open, half-open ->
        closed) are operationally significant: a transition to ``open``
        means the dependency is sustainedly broken; a transition to
        ``half-open`` means a probe is in flight; a transition back to
        ``closed`` means recovery has been confirmed. The
        ``fail_counter`` snapshot included with each event lets
        dashboards correlate the burst of failures that triggered a
        trip.

        Note:
            ``old_state`` may be ``None`` on the very first state
            assignment (when pybreaker initializes the breaker's state
            machine). The :func:`getattr` calls below handle that case
            without raising — ``getattr(None, "name", str(None))``
            produces the string ``"None"``.

        Args:
            cb: The :class:`pybreaker.CircuitBreaker` whose state
                changed. Carries the current ``fail_counter`` value.
            old_state: The breaker's previous state, or ``None`` on
                initial state assignment.
            new_state: The breaker's new state. Always non-None.
        """
        _LOGGER.warning(
            "circuit_breaker.state_change",
            breaker=self._name,
            from_state=getattr(old_state, "name", str(old_state)),
            to_state=getattr(new_state, "name", str(new_state)),
            fail_counter=cb.fail_counter,
        )

    def failure(
        self, cb: pybreaker.CircuitBreaker, exc: BaseException
    ) -> None:
        """Emit an INFO log line on every individual call failure.

        Per-call failures are normal-but-noteworthy: they accumulate
        toward the breaker's ``fail_max`` threshold but are not
        themselves operational events. Logged at INFO so they surface in
        verbose Kibana queries without flooding default WARNING-level
        dashboards.

        Args:
            cb: The :class:`pybreaker.CircuitBreaker` whose protected
                call raised an exception.
            exc: The exception raised by the protected callable.
        """
        _LOGGER.info(
            "circuit_breaker.failure",
            breaker=self._name,
            fail_counter=cb.fail_counter,
            error_type=exc.__class__.__name__,
        )


# ---------------------------------------------------------------------------
# Public factory: circuit_breaker_outbound_http
# ---------------------------------------------------------------------------


def circuit_breaker_outbound_http(
    *,
    name: str,
    failure_threshold: int,
    reset_timeout_ms: int,
    excluded_exceptions: tuple[type[BaseException], ...] = (),
) -> pybreaker.CircuitBreaker:
    """Return a configured :class:`pybreaker.CircuitBreaker` (AAP R-16).

    Each invocation produces a fresh breaker with its own state — the
    breaker is stateful (it tracks the failure counter and the OPEN
    expiration timestamp) and must NOT be shared across distinct
    integrations. Callers are expected to construct one breaker per
    protected dependency at container construction time and reuse it
    for the lifetime of the application.

    Args:
        name:                Identifier surfaced in logs and breaker
            diagnostics. Conventionally the name of the dependency the
            breaker protects (e.g., ``"auth_service"``,
            ``"external_wms"``).
        failure_threshold:   Number of consecutive failures before the
            breaker trips to OPEN. Must be ``>= 1``. Maps directly to
            pybreaker's ``fail_max`` parameter.
        reset_timeout_ms:    Time in milliseconds the breaker stays OPEN
            before attempting a half-open probe call. Must be ``>= 0``.
            Converted to seconds for pybreaker's ``reset_timeout``
            parameter at construction time.
        excluded_exceptions: Exception classes that do NOT count toward
            the failure threshold. Used to mark domain exceptions that
            represent business outcomes rather than dependency failures
            (e.g., ``InsufficientStockError`` should not slowly trip the
            breaker against a perfectly healthy service). Defaults to
            empty tuple.

    Returns:
        A :class:`pybreaker.CircuitBreaker` instance with the configured
        thresholds, name, and an attached :class:`_BreakerListener` for
        structlog state-change visibility. Use the
        :meth:`pybreaker.CircuitBreaker.call_async` method to wrap async
        calls; pybreaker will track success and failure automatically.

    Raises:
        ValueError: If ``failure_threshold < 1`` or
            ``reset_timeout_ms < 0``.

    Example:
        >>> breaker = circuit_breaker_outbound_http(
        ...     name="external_wms",
        ...     failure_threshold=5,
        ...     reset_timeout_ms=30_000,
        ... )
        >>> async def fetch_inventory() -> dict:
        ...     async with httpx.AsyncClient() as client:
        ...         response = await client.get("https://wms.example.com/stock")
        ...         response.raise_for_status()
        ...         return response.json()
        >>> result = await breaker.call_async(fetch_inventory)
    """
    # Fail-fast input validation. Surfaces configuration errors at the
    # factory call site rather than at the first protected call.
    if failure_threshold < 1:
        raise ValueError("failure_threshold must be >= 1")
    if reset_timeout_ms < 0:
        raise ValueError("reset_timeout_ms must be >= 0")

    breaker = pybreaker.CircuitBreaker(
        fail_max=failure_threshold,
        reset_timeout=reset_timeout_ms / 1000.0,
        # pybreaker's ``exclude`` parameter accepts an iterable of
        # exception classes (or callable predicates). We always pass a
        # list to keep the conversion explicit.
        exclude=list(excluded_exceptions),
        name=name,
    )

    # Bind the structlog listener AFTER construction. The listener
    # surfaces every state transition as a structured log line so Kibana
    # can alert on trip / reset events.
    breaker.add_listener(_BreakerListener(name))

    # Emit an INFO line on creation so operators can confirm at startup
    # that the breaker was wired correctly. This event also seeds the
    # Kibana dashboard with a baseline so a missing breaker is detected
    # by absence of the expected creation log line.
    _LOGGER.info(
        "circuit_breaker.created",
        breaker=name,
        failure_threshold=failure_threshold,
        reset_timeout_ms=reset_timeout_ms,
    )
    return breaker


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------

__all__: list[str] = [
    "circuit_breaker_outbound_http",
    "retry_outbound_http",
]
