"""Tenacity-based retry primitives for async calls (AAP R-15).

This module provides two public primitives that the Recommendation Engine
(and any sibling service that copies this scaffolding) uses everywhere an
outbound call must be bounded, retried with exponential backoff and jitter,
and instrumented for observability:

    retry_async(...)
        A decorator that wraps an async function with a tenacity retry
        policy (exponential backoff + jitter, bounded attempts,
        configurable retryable exception classes). Used on Kafka handler
        coroutines, internal pipeline stages, and any place where
        declarative retry semantics are desired.

    build_http_retrying_transport(...)
        Returns an ``httpx.AsyncBaseTransport`` that retries every request
        on 5xx, ``httpx.TransportError``, and ``httpx.TimeoutException``.
        Non-idempotent methods (POST / PATCH) are retried ONLY when the
        request carries an ``X-Idempotency-Key`` header (AAP R-12).

Both primitives:

* Honor bounded retry counts (AAP R-15 — every outbound call has a
  configurable maximum attempt count).
* Use exponential backoff with symmetric multiplicative jitter so the
  retry storm a hot upstream sees is distributed in time.
* Emit Prometheus metrics (``http_retry_total``, ``retry_attempts_total``)
  scraped by Metricbeat per AAP R-27.
* Emit OpenTelemetry spans (``retry.<name>`` / ``http.retry_attempt``)
  when the OT SDK is installed and configured, so distributed traces
  show retries as discrete child spans.
* Log each retry / give-up decision via structlog at WARNING with the
  endpoint, HTTP method, attempt number, scheduled sleep, and the
  underlying error type per AAP R-26.

Composition rule
----------------
Retries sit INSIDE circuit breakers (see ``circuit_breaker.py``). A
retry exhaustion is ONE failure to the breaker, not N. The standard
call site is::

    # Correct: breaker observes only the *final* outcome.
    await call_through(breaker, retrying_coro, ...)

    # Incorrect: every individual retry is a breaker failure, which
    # poisons the breaker prematurely.
    @retry_async(...)
    async def _wrapped():
        return await call_through(breaker, ...)

Idempotency guard
-----------------
Per RFC 9110, ``GET``, ``HEAD``, ``OPTIONS``, ``PUT``, and ``DELETE`` are
idempotent and safe to retry. ``POST`` and ``PATCH`` are NOT — a
partially-completed request that timed out on the server side may have
committed, so retrying could produce duplicate work. The transport
therefore retries non-idempotent methods ONLY when the caller asserts
idempotency by attaching the ``X-Idempotency-Key`` header (typically a
client-generated UUID v4). This matches AAP R-12 (Payment webhooks must
be idempotent based on the provider's event ID) and the broader
industry pattern.

Compliance notes
----------------
* AAP R-15 — every outbound HTTP call has a retry policy with
  exponential backoff and jitter, a maximum attempt count, and an
  explicit timeout (timeout is enforced by ``timeout.http_timeout`` at
  the ``httpx.AsyncClient`` level; this module supplies the retry
  layer).
* AAP R-17 — Kafka consumers route to ``<topic>.retry`` and
  ``<topic>.dlq``; the ``retry_async`` decorator is the in-memory retry
  primitive applied BEFORE the message is routed to a retry topic.
* AAP R-26 — every retry / give-up decision is logged as structured
  JSON at WARNING with stable event names (``retry_attempt_scheduled``,
  ``http_response_retry``, ``http_transport_error_retry``,
  ``http_transport_error_give_up``).
* AAP R-27 — Prometheus counters ``http_retry_total`` and
  ``retry_attempts_total`` are exposed via the service's ``/metrics``
  endpoint.
* AAP R-13 — correlation IDs propagate transparently because httpx
  transports forward request headers untouched and tenacity preserves
  the contextvar context across awaits.

Cross-references
----------------
* Consumed by ``src/container.py`` — builds the retrying transport for
  the Product Service ``httpx.AsyncClient``.
* Consumed by ``src/events/consumer.py`` — decorates the per-message
  Kafka handler with ``retry_async``.
* Re-exported from the package root via ``src.resilience.__init__``.
* Depends on (destination repo): NOTHING in ``src.*``. ``depends_on_files``
  is intentionally empty so the module is importable during early
  bootstrap (logging configuration, container construction, test
  fixtures) without dragging in any service-specific state.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``asyncio`` provides ``asyncio.sleep`` for the inter-attempt pause inside
# the transport's manual retry loop. The decorator delegates sleeping to
# tenacity's ``AsyncRetrying`` engine, but the transport rolls its own loop
# because it must honor ``Retry-After`` overrides that tenacity has no
# built-in primitive for. We deliberately do NOT wrap ``asyncio.sleep`` in
# try/except: cancellation must propagate cleanly so that a shutting-down
# consumer task is cancelled promptly even mid-retry.
import asyncio

# ``random.uniform`` powers the symmetric multiplicative jitter applied to
# the transport's exponential backoff base value. Using ``random`` (rather
# than ``secrets``) is correct here: jitter is for traffic shaping, not
# cryptographic isolation.
import random

# ``time.time()`` converts the parsed HTTP-date in a ``Retry-After`` header
# into a relative seconds delta. The wall clock is acceptable here because
# server-issued HTTP dates are wall-clock times by definition; using
# ``time.monotonic()`` would compare against a different epoch and produce
# nonsensical deltas.
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    ParamSpec,
    TypeVar,
)

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``httpx`` supplies both the transport base class (``AsyncBaseTransport``)
# we subclass and the retryable exception classes we catch
# (``TransportError`` covers DNS failures, connection refused, and broken
# pipes; ``TimeoutException`` is technically a subclass of ``TransportError``
# in modern httpx but we list both explicitly so the retryable set remains
# self-documenting).
import httpx

# ``structlog.get_logger`` yields a module-scoped logger configured by the
# service's central ``src.config.logging`` setup at process start. The
# logger's ``warning`` method emits structured JSON consumed by Filebeat
# and indexed in Elasticsearch (AAP R-26 / R-27).
import structlog

# ``REGISTRY`` is the global Prometheus default collector registry; we read
# its private ``_names_to_collectors`` dict to make ``Counter`` registration
# idempotent — re-importing the module in tests must NOT raise the
# ``Duplicated timeseries`` ``ValueError`` that ``Counter(...)`` would
# otherwise raise on second registration. ``Counter`` is the metric type
# used for monotonically-increasing retry attempt tallies.
from prometheus_client import REGISTRY, Counter

# Tenacity primitives — chosen over ``@retry`` decorators because the
# ``AsyncRetrying`` iterator gives us full control over the surrounding
# ``with attempt:`` block and lets us instrument each iteration with
# OpenTelemetry spans without monkeypatching.
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
# A bound logger captured once at import time. structlog's lazy proxy means
# this does NOT freeze the global processor chain: configuration changes
# applied later in ``src.main.lifespan`` still apply to log lines emitted
# after that point.
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------
# Status codes that SHOULD be retried for idempotent requests.
#   408 (Request Timeout)        — request was not completed; safe to retry.
#   425 (Too Early)              — client should retry after a delay.
#   429 (Too Many Requests)      — Retry-After header typically present.
#   500/502/503/504              — server-side transient errors (5xx).
# 501 (Not Implemented) and 505 (HTTP Version Not Supported) are NOT
# retryable: they signal a permanent server-side incapacity.
_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})

# Idempotent HTTP methods per RFC 9110 §9.2.2. Methods outside this set
# (POST, PATCH, CONNECT, TRACE) are retried only when the caller attaches
# an ``X-Idempotency-Key`` header to assert the request is safe to retry.
_IDEMPOTENT_METHODS: frozenset[str] = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})

# The opt-in idempotency assertion header. Exported so other modules
# (notably the Product Service client and any future Payment Service
# adapter) can reference it without string duplication. The value matches
# the de-facto industry convention (Stripe, GitHub, Square all use
# ``Idempotency-Key`` or a vendor-prefixed variant; we use the
# ``X-`` prefix for clarity, matching the convention adopted by most
# internal Blitzy services).
IDEMPOTENCY_KEY_HEADER: str = "X-Idempotency-Key"


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
        name: The Prometheus metric name (e.g., ``http_retry_total``).
        documentation: Human-readable HELP string.
        labelnames: Tuple of label keys.

    Returns:
        The freshly-registered or pre-existing ``Counter`` instance.
    """
    # ``REGISTRY._names_to_collectors`` is a dict keyed by both the bare
    # metric name AND the suffixed variants Prometheus generates ("_total",
    # "_created", ...). Looking up the bare name is sufficient because
    # ``Counter`` registers itself under its base name first.
    existing = REGISTRY._names_to_collectors.get(name)  # noqa: SLF001
    if existing is not None:
        # Reuse the registered collector. The ``type: ignore`` covers the
        # fact that REGISTRY stores collectors as the abstract base class;
        # we know it's a Counter because we registered it as one and the
        # name -> collector mapping is unique.
        return existing  # type: ignore[return-value]
    return Counter(name, documentation, labelnames=labelnames)


# Counter exposed by the retrying httpx transport. Labels:
#   endpoint — request URL path only (NOT query string), to bound cardinality.
#   result   — one of: "retry" (an attempt failed and another is scheduled),
#              "give_up" (max attempts reached or non-idempotent + retryable),
#              "success" (a non-retryable status was returned, including 2xx
#              and non-retryable 4xx like 404).
HTTP_RETRY_TOTAL: Counter = _get_or_create_counter(
    "http_retry_total",
    "HTTP retry attempts by endpoint and outcome.",
    labelnames=("endpoint", "result"),
)

# Counter exposed by the ``retry_async`` decorator. Labels:
#   name   — the decorated function's qualified name (or the explicit
#            ``name`` argument when supplied), used to disambiguate
#            similarly-shaped retry loops in dashboards.
#   result — same vocabulary as ``http_retry_total``: "retry" / "success".
#            (The decorator does not emit "give_up" because tenacity
#            re-raises the original exception on exhaustion and the
#            caller's exception handler can attribute the failure.)
RETRY_ATTEMPTS_TOTAL: Counter = _get_or_create_counter(
    "retry_attempts_total",
    "Generic retry attempts for `retry_async`-decorated coroutines.",
    labelnames=("name", "result"),
)


# ---------------------------------------------------------------------------
# Lazy OpenTelemetry tracer
# ---------------------------------------------------------------------------
# Sentinel ``None`` means "not yet probed"; ``False`` means "probed and
# unavailable" (the OT SDK is not installed); a tracer object means
# "probed and available". This three-state encoding lets us probe exactly
# once even if the module is hot-imported by many short-lived workers.
_tracer: Any = None


def _get_tracer() -> Any:
    """Return an OpenTelemetry tracer if the SDK is installed, else None.

    The OT SDK is an OPTIONAL dependency: development environments and
    pure unit tests routinely run without it installed. Importing
    ``opentelemetry`` lazily inside this helper (rather than at module
    top-level) lets ``retry.py`` load cleanly in those environments.
    The probe runs at most once; the result is cached in ``_tracer``.

    Returns:
        A ``trace.Tracer`` instance when ``opentelemetry-api`` is
        importable, otherwise ``None``.
    """
    global _tracer
    if _tracer is not None:
        # Already probed. Translate the False sentinel back to None
        # for callers; everything else is a real tracer.
        return _tracer if _tracer is not False else None
    try:
        # Lazy import keeps the module loadable without the OT SDK.
        from opentelemetry import trace

        _tracer = trace.get_tracer(__name__)
    except ImportError:  # pragma: no cover  (covered only in pure-stdlib environments)
        # Cache the negative result so subsequent calls don't re-probe.
        _tracer = False
    return _tracer if _tracer is not False else None


# ---------------------------------------------------------------------------
# Helper functions (internal)
# ---------------------------------------------------------------------------
def _is_idempotent_request(request: httpx.Request) -> bool:
    """Return True when the request is safe to retry.

    A request is considered idempotent when EITHER its method is in the
    RFC 9110 idempotent set OR the caller has attached an
    ``X-Idempotency-Key`` header to assert that retrying is safe.

    Args:
        request: The outbound httpx request.

    Returns:
        True when retrying is safe, False otherwise.
    """
    # ``request.method`` is already an uppercase string in httpx, but we
    # normalise defensively in case a custom transport up the stack has
    # injected a request with a non-standard method casing.
    if request.method.upper() in _IDEMPOTENT_METHODS:
        return True
    # ``httpx.Headers.__contains__`` is case-insensitive per RFC 7230,
    # so a caller passing the header as ``x-idempotency-key`` still
    # matches.
    return IDEMPOTENCY_KEY_HEADER in request.headers


def _is_retryable_response(response: httpx.Response) -> bool:
    """Return True when the response status code is in the retryable set.

    Args:
        response: The HTTP response received from the inner transport.

    Returns:
        True when the status code indicates a transient failure that
        warrants a retry attempt; False otherwise (including for all
        2xx success codes and non-retryable 4xx client errors).
    """
    return response.status_code in _RETRYABLE_STATUS_CODES


def _parse_retry_after(response: httpx.Response) -> float | None:
    """Parse the ``Retry-After`` header. Return delay in seconds or None.

    Per RFC 9110 §10.2.3, the ``Retry-After`` header may take one of two
    forms:

    * An integer number of seconds (``"120"``) — a relative delay.
    * An HTTP-date (``"Wed, 21 Oct 2026 07:28:00 GMT"``) — an absolute
      time after which the request may be retried.

    This helper handles both formats and returns the relative delay in
    seconds, clamped to ``[0, +inf)`` so a server returning a date in
    the past collapses to "retry immediately" rather than producing a
    negative sleep.

    Args:
        response: The HTTP response carrying (or lacking) the header.

    Returns:
        The retry delay in seconds, or ``None`` when the header is
        absent or malformed.
    """
    header = response.headers.get("Retry-After")
    if not header:
        return None
    # First try: integer-seconds form. ``float()`` accepts integers too,
    # so ``"120"`` and ``"120.5"`` both parse. We then clamp negatives to
    # zero so a deliberately-negative server response doesn't make
    # ``asyncio.sleep`` raise.
    try:
        seconds = float(header)
        return max(0.0, seconds)
    except ValueError:
        # Not numeric — fall through to HTTP-date parsing below.
        pass
    # Second try: HTTP-date form. ``email.utils.parsedate_to_datetime``
    # is the canonical RFC 5322 parser and accepts the obsolete RFC 850
    # and asctime formats too, matching the RFC 9110 requirements for
    # accepting all three forms.
    try:
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(header)
        # Convert to relative seconds via wall clock subtraction. Using
        # ``time.time()`` (wall clock) is correct here because the server
        # issued an absolute wall-clock timestamp.
        delta = dt.timestamp() - time.time()
        return max(0.0, delta)
    except (TypeError, ValueError):
        # Malformed date — surface ``None`` so callers fall back to
        # exponential backoff rather than waiting indefinitely.
        return None


def _compute_backoff_seconds(
    attempt: int,
    *,
    initial_delay_ms: int,
    multiplier: float,
    max_delay_ms: int,
    jitter_pct: float,
) -> float:
    """Compute the inter-attempt sleep with exponential backoff + jitter.

    Formula::

        base_ms = min(max_delay_ms, initial_delay_ms * multiplier**attempt)
        if jitter_pct > 0:
            base_ms += U(-base_ms * jitter_pct, +base_ms * jitter_pct)
        return max(0.0, base_ms / 1000.0)

    Note that ``attempt`` is **0-indexed**: the first retry uses
    ``attempt=0`` so the first sleep equals ``initial_delay_ms``. This
    matches tenacity's internal accounting for ``wait_exponential_jitter``.

    Args:
        attempt: The 0-indexed retry number.
        initial_delay_ms: The base delay (milliseconds) for ``attempt=0``.
        multiplier: The exponential growth factor (must be >= 1.0).
        max_delay_ms: The upper bound on any single sleep
            (the exponential growth is clamped to this cap).
        jitter_pct: Symmetric multiplicative jitter in ``[0, 1]``;
            0.2 means each sleep is multiplied by a uniform sample from
            ``[0.8, 1.2]``.

    Returns:
        The sleep duration in seconds, clamped to ``[0.0, +inf)``.
    """
    # Compute the deterministic exponential base. ``max(0, attempt)``
    # guards against a negative ``attempt`` argument (which would produce
    # a fractional multiplier value and a tiny initial sleep) — defensive
    # only; the transport always passes 0-indexed non-negative values.
    base_ms = min(
        float(max_delay_ms),
        initial_delay_ms * (multiplier ** max(0, attempt)),
    )
    # Apply symmetric jitter so a fleet of clients hitting the same
    # upstream after a transient outage spread their retries across the
    # backoff window. AAP R-15 mandates jitter explicitly.
    if jitter_pct > 0:
        spread = base_ms * jitter_pct
        base_ms = base_ms + random.uniform(-spread, spread)
    # Final clamp converts ms -> s and ensures we never return a
    # negative duration (would otherwise raise from ``asyncio.sleep``).
    return max(0.0, base_ms / 1000.0)


def _make_before_sleep(name: str) -> Callable[[RetryCallState], None]:
    """Return a tenacity ``before_sleep`` hook bound to ``name``.

    The returned hook fires just before ``AsyncRetrying`` sleeps between
    retry attempts. It increments the retry counter and emits a WARNING
    log line carrying the attempt number, the scheduled sleep, and the
    underlying exception class / message. Tenacity invokes the hook
    exactly once per scheduled retry (i.e., never on the terminal
    success and never on exhaustion).

    Args:
        name: The label value used for the ``retry_attempts_total``
            counter and the ``name=`` log field.

    Returns:
        A ``Callable[[RetryCallState], None]`` suitable for the
        ``before_sleep=`` parameter of ``AsyncRetrying``.
    """

    def _hook(retry_state: RetryCallState) -> None:
        # Increment the retry counter exactly once per scheduled retry.
        RETRY_ATTEMPTS_TOTAL.labels(name=name, result="retry").inc()

        attempt_number = retry_state.attempt_number
        # ``next_action`` carries the scheduled sleep duration that
        # tenacity computed via the configured ``wait`` callable.
        next_wait = retry_state.next_action.sleep if retry_state.next_action else None
        # ``outcome`` is a Future-like wrapping the call's exception
        # (because the call failed — that's why ``before_sleep`` fires).
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            "retry_attempt_scheduled",
            name=name,
            attempt=attempt_number,
            next_sleep_seconds=next_wait,
            exception=type(exc).__name__ if exc else None,
            exception_message=str(exc) if exc else None,
        )

    return _hook


def _make_after(name: str) -> Callable[[RetryCallState], None]:
    """Return a tenacity ``after`` hook bound to ``name``.

    The hook is included for completeness so callers / future code can
    plug additional post-attempt observability without re-implementing
    the metric increment. The decorator itself does NOT use this hook —
    it accounts for successes inline within the ``async for`` loop so
    the success increment fires exactly once even when the wrapped
    function returns ``None``.

    Args:
        name: The label value used for the ``retry_attempts_total``
            counter.

    Returns:
        A ``Callable[[RetryCallState], None]`` that increments the
        success counter when the most recent attempt succeeded.
    """

    def _hook(retry_state: RetryCallState) -> None:
        if retry_state.outcome and retry_state.outcome.failed:
            # Hook fires after each attempt; only count successes here so
            # retries don't double-count via ``before_sleep`` + ``after``.
            return
        RETRY_ATTEMPTS_TOTAL.labels(name=name, result="success").inc()

    return _hook


# ---------------------------------------------------------------------------
# Null span context manager — used when OpenTelemetry is unavailable
# ---------------------------------------------------------------------------
class _null_span_cm:
    """A no-op context manager standing in for an OpenTelemetry span.

    Returned by ``_get_tracer() -> None`` paths so the decorator and
    transport can use a uniform ``with span_cm:`` block whether or not
    OT is installed. The class deliberately mirrors the minimal surface
    of an OT span context manager — ``__enter__`` returns ``None``,
    ``__exit__`` returns ``False`` to avoid swallowing exceptions.
    """

    def __enter__(self) -> None:
        return None

    def __exit__(self, *args: Any) -> None:
        # Return ``None`` (equivalent to returning ``False``) so any
        # in-flight exception propagates. Returning a truthy value
        # would mask exceptions, which is never the right behavior
        # for a tracing span. Annotated as ``-> None`` rather than
        # ``-> bool`` so static checkers don't flag the constant
        # ``False`` return as an exception-swallow risk.
        return None


# ---------------------------------------------------------------------------
# Type variables used by retry_async
# ---------------------------------------------------------------------------
# ``ParamSpec`` (PEP 612, Python 3.10+) preserves the wrapped function's
# parameter list through the decorator so ``retry_async()(my_func)`` exposes
# ``my_func``'s exact signature to static checkers and IDE autocomplete.
P = ParamSpec("P")
T = TypeVar("T")


# ---------------------------------------------------------------------------
# Public API — retry_async decorator
# ---------------------------------------------------------------------------
def retry_async(
    *,
    max_attempts: int = 3,
    initial_delay_ms: int = 100,
    multiplier: float = 2.0,
    max_delay_ms: int = 2000,
    jitter_pct: float = 0.2,
    retryable_exceptions: Iterable[type[BaseException]] = (Exception,),
    name: str | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Wrap an async function with exponential-backoff retry (AAP R-15).

    Args:
        max_attempts: Total attempts including the first call (must be
            ``>= 1``). The default of 3 means: one initial attempt plus
            up to two retries.
        initial_delay_ms: Initial sleep between attempts (milliseconds).
            Must be ``>= 0``. The first retry waits this long; each
            subsequent retry waits ``previous * multiplier`` (clamped
            to ``max_delay_ms``).
        multiplier: Exponential growth factor. Must be ``>= 1.0``. A
            value of 2.0 doubles the wait between retries; 1.0 disables
            growth (constant backoff).
        max_delay_ms: Upper bound on any single sleep (milliseconds).
            Must be ``>= initial_delay_ms``. Once growth hits this cap
            it stays clamped.
        jitter_pct: Symmetric multiplicative jitter in ``[0, 1]``. 0.2
            means each sleep is multiplied by ``U(0.8, 1.2)``. Set to 0
            to disable jitter (NOT recommended in production — AAP R-15
            mandates jitter).
        retryable_exceptions: Iterable of exception classes that trigger
            a retry; all other exceptions propagate immediately.
            Defaults to ``(Exception,)`` so callers who do not narrow
            the set retry every failure (which is rarely what you want
            — narrow this to the specific transient failure modes of
            the wrapped function).
        name: Label used on the ``retry_attempts_total`` counter and
            in log lines. Defaults to the decorated function's
            ``__qualname__``.

    Returns:
        A decorator that preserves the wrapped function's signature
        (PEP 612 ``ParamSpec``).

    Raises:
        ValueError: On invalid arguments — surfaced eagerly at
            decoration time so misconfigurations fail at import rather
            than under load.
        Exception: On attempt exhaustion the original exception
            propagates (because ``reraise=True``) — callers see the
            real failure, not a ``tenacity.RetryError`` wrapper.

    Example:
        >>> @retry_async(  # doctest: +SKIP
        ...     max_attempts=4,
        ...     initial_delay_ms=200,
        ...     retryable_exceptions=(httpx.TransportError,),
        ... )
        ... async def fetch_user(uid: str) -> dict:
        ...     return (await client.get(f"/users/{uid}")).json()
    """
    # Eager argument validation — these checks fire at decoration time
    # (i.e., at import) so a typo in a config-driven retry policy fails
    # fast rather than mid-incident.
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if initial_delay_ms < 0:
        raise ValueError("initial_delay_ms must be >= 0")
    if multiplier < 1.0:
        raise ValueError("multiplier must be >= 1.0")
    if max_delay_ms < initial_delay_ms:
        raise ValueError("max_delay_ms must be >= initial_delay_ms")
    if not 0.0 <= jitter_pct <= 1.0:
        raise ValueError("jitter_pct must be in [0, 1]")

    # Coerce the retryable iterable to a tuple eagerly so a generator
    # passed in does not get exhausted on the second decoration call.
    retryable_tuple = tuple(retryable_exceptions)
    if not retryable_tuple:
        raise ValueError("retryable_exceptions must contain at least one class")

    # ``wait_exponential_jitter`` accepts ``jitter`` in ABSOLUTE seconds.
    # We translate the percent-based spec into the maximum jitter in
    # seconds so the absolute spread matches the configured percent of
    # the eventual ceiling. Tenacity's jitter is uniform over
    # ``[-jitter, +jitter]`` and added to the deterministic base.
    jitter_seconds = (max_delay_ms / 1000.0) * jitter_pct

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        # Default the log/metric label to the function's qualified name
        # (e.g., ``module.ClassName.method``), which is stable across
        # imports and unique within a process. Falling back here means
        # callers can override per-call site without touching the source.
        effective_name = name or func.__qualname__

        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # Probe OT lazily on each call (cheap after the first probe
            # because the result is cached in ``_tracer``). Doing this on
            # each call rather than at decoration time means a service
            # that initialises OT *after* importing this module still
            # benefits from tracing on subsequent calls.
            tracer = _get_tracer()

            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential_jitter(
                    initial=initial_delay_ms / 1000.0,
                    max=max_delay_ms / 1000.0,
                    exp_base=multiplier,
                    jitter=jitter_seconds,
                ),
                retry=retry_if_exception_type(retryable_tuple),
                before_sleep=_make_before_sleep(effective_name),
                reraise=True,
            ):
                # ``with attempt:`` is tenacity's signal that the code
                # inside is the retryable unit. An exception raised here
                # is captured by tenacity and stored in the outcome;
                # tenacity then either re-attempts the call (if the
                # retry predicate matches and the stop condition is
                # not yet met) or re-raises on the next loop iteration
                # (because of ``reraise=True``). The ``with attempt:``
                # block does NOT propagate the exception out of itself;
                # subsequent code in this iteration runs even on
                # failure, so we MUST guard the success bookkeeping
                # behind an explicit ``outcome.failed`` check.
                with attempt:
                    if tracer is not None:
                        # Each attempt becomes a child span so traces
                        # can render the retry sequence. The attribute
                        # vocabulary follows OpenTelemetry's semantic
                        # conventions for retry-related telemetry.
                        span_cm = tracer.start_as_current_span(
                            f"retry.{effective_name}",
                            attributes={
                                "retry.attempt": attempt.retry_state.attempt_number,
                                "retry.name": effective_name,
                            },
                        )
                    else:
                        span_cm = _null_span_cm()
                    with span_cm:
                        result = await func(*args, **kwargs)
                # Inspect tenacity's outcome BEFORE assuming success.
                # When the ``with attempt:`` block above caught an
                # exception, ``outcome.failed`` is True and we MUST
                # continue to the next iteration so tenacity can
                # decide to retry (predicate match) or re-raise
                # (predicate miss + ``reraise=True``). Reading
                # ``result`` here would raise ``UnboundLocalError``
                # because it was never assigned.
                outcome = attempt.retry_state.outcome
                if outcome is not None and outcome.failed:
                    # Hand control back to AsyncRetrying so the next
                    # iteration either schedules a retry or re-raises.
                    continue
                # Reaching this line means the call succeeded (no
                # exception escaped the ``with attempt:`` block). The
                # ``async for`` loop will exit naturally on the next
                # iteration because the outcome is non-failed and
                # tenacity stops iterating.
                RETRY_ATTEMPTS_TOTAL.labels(
                    name=effective_name,
                    result="success",
                ).inc()
                return result

            # Unreachable in practice: tenacity with ``reraise=True``
            # re-raises the last exception from inside the ``async for``
            # loop on exhaustion. The ``RetryError`` raise exists so
            # static checkers see a definite return / raise path even
            # though the loop never falls off naturally. ``RetryError``
            # is the canonical tenacity exhaustion sentinel; using it
            # here (rather than a generic Python exception) keeps the
            # exception vocabulary consistent should the unreachable
            # branch ever be reached due to a future tenacity version
            # change. The ``# type: ignore[arg-type]`` is required
            # because ``RetryError.__init__`` formally types
            # ``last_attempt`` as ``Future``; we have no Future to
            # supply on this never-reached path.
            raise RetryError(last_attempt=None)  # type: ignore[arg-type]  # pragma: no cover

        # Preserve introspection metadata so stack traces, logging, and
        # tools like ``functools.wraps``-aware decorators see the wrapped
        # function's identity. We do this manually rather than via
        # ``@functools.wraps`` because ``ParamSpec`` interacts cleanly
        # with attribute assignment but ``@wraps`` would drop the
        # parameter typing information.
        wrapper.__wrapped__ = func  # type: ignore[attr-defined]
        wrapper.__name__ = func.__name__
        wrapper.__qualname__ = func.__qualname__
        wrapper.__doc__ = func.__doc__
        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Public API — build_http_retrying_transport
# ---------------------------------------------------------------------------
class _RetryingAsyncHTTPTransport(httpx.AsyncBaseTransport):
    """httpx transport that retries on 5xx + transport errors (AAP R-15).

    A subclass of ``httpx.AsyncBaseTransport`` that wraps an inner
    transport (defaulting to ``httpx.AsyncHTTPTransport``) and retries
    every request on:

    * ``httpx.TransportError`` — connection / DNS / network failures.
    * ``httpx.TimeoutException`` — connect / read / write / pool
      timeout (a subclass of ``TransportError`` in modern httpx, but
      listed explicitly so the retryable set is self-documenting).
    * Any HTTP response whose status code is in
      ``retryable_status_codes`` (default: 408, 425, 429, 500, 502,
      503, 504) — the ``Retry-After`` header is honored on these
      responses when present.

    Idempotency guard: non-idempotent methods (anything not in
    ``_IDEMPOTENT_METHODS``) are retried ONLY when the request carries
    an ``X-Idempotency-Key`` header. This prevents the well-known
    "retry creates duplicate side effects" anti-pattern (AAP R-12).

    Behavior on the LAST attempt: a retryable response is returned to
    the caller AS-IS rather than retried further. The caller (or its
    surrounding circuit breaker) sees the actual 503 / 504 and can
    trigger fallback semantics. A transport exception on the last
    attempt is re-raised after the final ``give_up`` metric increment.
    """

    # ``ClassVar`` because the tuple is shared by every instance — keeps
    # the type checker happy without inflating per-instance memory.
    _TRANSPORT_EXCEPTIONS: ClassVar[tuple[type[BaseException], ...]] = (
        httpx.TransportError,
        httpx.TimeoutException,
    )

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport,
        *,
        max_attempts: int,
        initial_delay_ms: int,
        multiplier: float,
        max_delay_ms: int,
        jitter_pct: float,
        retryable_status_codes: frozenset[int],
    ) -> None:
        # Store the configured policy. All fields are private (``_``-prefix)
        # because the transport's public surface is ``handle_async_request``
        # and ``aclose`` — both inherited from ``AsyncBaseTransport``.
        self._inner = inner
        self._max_attempts = max_attempts
        self._initial_delay_ms = initial_delay_ms
        self._multiplier = multiplier
        self._max_delay_ms = max_delay_ms
        self._jitter_pct = jitter_pct
        self._retryable_status_codes = retryable_status_codes

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Send ``request`` through the inner transport with retries.

        Args:
            request: The outbound httpx request, fully prepared by the
                client (URL, headers, content, extensions).

        Returns:
            The successful (non-retryable status) ``httpx.Response``,
            OR the final retryable response when retries are exhausted
            (so the caller sees the 5xx and can decide on fallback).

        Raises:
            httpx.TransportError: If the inner transport raises a
                transport exception on the last allowed attempt.
        """
        # Bound the cardinality of the ``endpoint`` Prometheus label by
        # using the URL PATH ONLY (no query string, no host). This
        # prevents per-user-id query parameters from creating a unique
        # label series for every request — a classic Prometheus
        # cardinality blow-up. Empty-path requests collapse to "/".
        endpoint = request.url.path or "/"
        idempotent = _is_idempotent_request(request)
        # Holds the most recent transport exception so we can re-raise
        # it after exhaustion. Type-annotated as a union so mypy knows
        # the post-loop ``raise last_exc`` is well-typed.
        last_exc: BaseException | None = None

        tracer = _get_tracer()

        for attempt in range(self._max_attempts):
            if tracer is not None:
                # OpenTelemetry semantic conventions for HTTP retry
                # spans: name "http.retry_attempt", attributes for
                # method, route, and the 1-indexed attempt number.
                span_cm = tracer.start_as_current_span(
                    "http.retry_attempt",
                    attributes={
                        "http.request.method": request.method,
                        "http.route": endpoint,
                        "retry.attempt": attempt + 1,
                    },
                )
            else:
                span_cm = _null_span_cm()

            with span_cm:
                try:
                    # Delegate to the inner transport. This is where
                    # connection pooling, TLS, HTTP/2, and proxy
                    # handling all live — we don't reimplement any of
                    # it; we only layer retry semantics on top.
                    response = await self._inner.handle_async_request(request)
                except self._TRANSPORT_EXCEPTIONS as exc:
                    # Capture for potential re-raise after exhaustion.
                    last_exc = exc
                    if not idempotent or attempt == self._max_attempts - 1:
                        # Either:
                        #   1. Method is not idempotent and the caller
                        #      did not assert idempotency via header —
                        #      retrying might duplicate side effects.
                        #   2. We're on the final attempt — exhausted.
                        # In both cases we surface the failure.
                        HTTP_RETRY_TOTAL.labels(
                            endpoint=endpoint,
                            result="give_up",
                        ).inc()
                        logger.warning(
                            "http_transport_error_give_up",
                            endpoint=endpoint,
                            method=request.method,
                            attempt=attempt + 1,
                            error=type(exc).__name__,
                        )
                        # Plain ``raise`` preserves the original
                        # exception context (no ``from None``); the
                        # caller's traceback shows the underlying
                        # transport failure verbatim.
                        raise
                    # Transient transport failure on a retryable
                    # request — record, log, sleep, retry.
                    HTTP_RETRY_TOTAL.labels(
                        endpoint=endpoint,
                        result="retry",
                    ).inc()
                    delay = _compute_backoff_seconds(
                        attempt,
                        initial_delay_ms=self._initial_delay_ms,
                        multiplier=self._multiplier,
                        max_delay_ms=self._max_delay_ms,
                        jitter_pct=self._jitter_pct,
                    )
                    logger.warning(
                        "http_transport_error_retry",
                        endpoint=endpoint,
                        method=request.method,
                        attempt=attempt + 1,
                        next_attempt_in_seconds=delay,
                        error=type(exc).__name__,
                    )
                    # No try/except around sleep: cancellation must
                    # propagate cleanly so a shutting-down task is
                    # cancelled promptly even mid-retry.
                    await asyncio.sleep(delay)
                    continue

                # ---- Got a response. Decide whether to retry. ----
                if response.status_code not in self._retryable_status_codes:
                    # Non-retryable status (2xx, most 4xx) — return.
                    # Note: 404, 401, 403 etc. fall through here as
                    # SUCCESS for retry-counter purposes because the
                    # transport delivered them faithfully; whether the
                    # caller treats them as logical errors is a
                    # higher-level concern.
                    HTTP_RETRY_TOTAL.labels(
                        endpoint=endpoint,
                        result="success",
                    ).inc()
                    return response

                if not idempotent or attempt == self._max_attempts - 1:
                    # Retryable status, but either:
                    #   1. Caller did not assert idempotency on a
                    #      non-idempotent method — return the 5xx so
                    #      the caller's circuit breaker / fallback
                    #      can handle it.
                    #   2. We're on the final attempt — surface the
                    #      response as-is rather than retrying again.
                    HTTP_RETRY_TOTAL.labels(
                        endpoint=endpoint,
                        result="give_up",
                    ).inc()
                    return response

                # ---- Retryable response on a retryable request. ----
                # Honor ``Retry-After`` when the server provides it;
                # otherwise fall back to our exponential backoff.
                # Honoring ``Retry-After`` is non-negotiable for 429
                # responses — ignoring it leads to thundering-herd
                # amplification during upstream incidents.
                retry_after = _parse_retry_after(response)
                delay = (
                    retry_after
                    if retry_after is not None
                    else _compute_backoff_seconds(
                        attempt,
                        initial_delay_ms=self._initial_delay_ms,
                        multiplier=self._multiplier,
                        max_delay_ms=self._max_delay_ms,
                        jitter_pct=self._jitter_pct,
                    )
                )
                HTTP_RETRY_TOTAL.labels(
                    endpoint=endpoint,
                    result="retry",
                ).inc()
                logger.warning(
                    "http_response_retry",
                    endpoint=endpoint,
                    method=request.method,
                    attempt=attempt + 1,
                    status_code=response.status_code,
                    next_attempt_in_seconds=delay,
                    retry_after_honored=retry_after is not None,
                )
                # Close the response body BEFORE retrying so the
                # underlying connection is released back to the pool.
                # Otherwise httpx would hold the connection open until
                # the response is fully consumed, blocking subsequent
                # retries on the same pool.
                await response.aclose()
                await asyncio.sleep(delay)
                continue

        # Loop exit due to exhaustion on transport exceptions (the
        # response-status branches all ``return`` from inside the loop).
        # The ``assert`` exists for type checkers; it can never trip
        # because we either set ``last_exc`` and ``continue`` or we
        # ``return``/``raise`` from inside the loop.
        assert last_exc is not None  # noqa: S101  (type-checker assist)
        raise last_exc

    async def aclose(self) -> None:
        """Close the inner transport.

        Called by ``httpx.AsyncClient.aclose`` (or its async context
        manager exit). Forwarding to the inner transport ensures the
        underlying connection pool releases its sockets cleanly.
        """
        await self._inner.aclose()


def build_http_retrying_transport(
    *,
    max_attempts: int = 3,
    initial_delay_ms: int = 100,
    multiplier: float = 2.0,
    max_delay_ms: int = 2000,
    jitter_pct: float = 0.2,
    retryable_status_codes: frozenset[int] = _RETRYABLE_STATUS_CODES,
    inner_transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncBaseTransport:
    """Build an httpx.AsyncBaseTransport that retries on 5xx + transport errors.

    The returned transport retries on:

    * ``httpx.TransportError`` (connection / DNS failures).
    * ``httpx.TimeoutException``.
    * HTTP 5xx and 408 / 425 / 429 (for idempotent requests only).

    Non-idempotent methods (POST, PATCH) are retried ONLY when the
    request carries the ``X-Idempotency-Key`` header.

    Args:
        max_attempts: Total attempts including the first (must be ``>= 1``).
        initial_delay_ms: Initial sleep between attempts (milliseconds).
            See ``retry_async`` for semantics.
        multiplier: Exponential growth factor (must be ``>= 1.0``).
        max_delay_ms: Upper bound on any single sleep (milliseconds).
        jitter_pct: Symmetric multiplicative jitter in ``[0, 1]``.
        retryable_status_codes: Override the default retryable status
            set; the ``Retry-After`` header is always honored when
            present on a retryable response.
        inner_transport: The underlying transport to wrap. Defaults to
            ``httpx.AsyncHTTPTransport()`` with httpx's own defaults.

    Returns:
        An ``httpx.AsyncBaseTransport`` suitable for passing as the
        ``transport=`` argument to ``httpx.AsyncClient(...)``.

    Raises:
        ValueError: On invalid arguments — same validation rules as
            ``retry_async``.

    Example:
        >>> import httpx  # doctest: +SKIP
        >>> transport = build_http_retrying_transport(  # doctest: +SKIP
        ...     max_attempts=4,
        ...     initial_delay_ms=200,
        ... )
        >>> async with httpx.AsyncClient(transport=transport) as client:
        ...     response = await client.get("https://api.example.com/x")
    """
    # Mirror ``retry_async``'s validation so the two primitives accept
    # the same configuration vocabulary. Centralizing the defaults in
    # this signature also keeps the call sites in ``src.container``
    # terse.
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    if initial_delay_ms < 0:
        raise ValueError("initial_delay_ms must be >= 0")
    if multiplier < 1.0:
        raise ValueError("multiplier must be >= 1.0")
    if max_delay_ms < initial_delay_ms:
        raise ValueError("max_delay_ms must be >= initial_delay_ms")
    if not 0.0 <= jitter_pct <= 1.0:
        raise ValueError("jitter_pct must be in [0, 1]")

    # Default the inner transport to a fresh ``AsyncHTTPTransport`` —
    # httpx's defaults are sensible, and callers that want HTTP/2,
    # custom proxy, or socket options pass their own.
    inner = inner_transport or httpx.AsyncHTTPTransport()
    return _RetryingAsyncHTTPTransport(
        inner,
        max_attempts=max_attempts,
        initial_delay_ms=initial_delay_ms,
        multiplier=multiplier,
        max_delay_ms=max_delay_ms,
        jitter_pct=jitter_pct,
        retryable_status_codes=retryable_status_codes,
    )


# ---------------------------------------------------------------------------
# Public surface declaration
# ---------------------------------------------------------------------------
# ``__all__`` is intentionally narrow: it lists the public primitives
# and the canonical idempotency-key header constant. The retryable
# status set and idempotent method set are exposed (without underscore-
# stripping) so unit tests and dependency-injection wiring can reference
# them, but they are deliberately UNDERSCORE-PREFIXED to communicate
# that they are implementation defaults — callers should use the
# ``retryable_status_codes`` argument to ``build_http_retrying_transport``
# rather than mutating these constants in place.
__all__ = [
    "IDEMPOTENCY_KEY_HEADER",
    "_IDEMPOTENT_METHODS",
    "_RETRYABLE_STATUS_CODES",
    "build_http_retrying_transport",
    "retry_async",
]
