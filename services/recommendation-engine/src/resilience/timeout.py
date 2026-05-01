"""Asyncio- and httpx-native timeout helpers.

This module provides two tiny, foundational helpers for bounding outbound
work in the Recommendation Engine:

    with_timeout(coro, seconds, *, name=None)
        Await an arbitrary coroutine with a hard upper bound. Raises
        :class:`asyncio.TimeoutError` with a descriptive message on expiry,
        including the ``name`` tag (when provided) for log correlation.

    http_timeout(connect_ms, read_ms, pool_ms, *, write_ms=None)
        Convenience builder for :class:`httpx.Timeout` from per-phase
        millisecond integers. Matches the configuration vocabulary used by
        ``services/recommendation-engine/config/default.yaml`` (e.g.
        ``connect_timeout_ms``, ``read_timeout_ms``, ``pool_timeout_ms``).

Both helpers comply with **AAP R-15** — every outbound call (synchronous
HTTP or asynchronous primitive) MUST have an explicit, configurable
timeout. Callers SHOULD wrap any coroutine whose upper bound is
operationally meaningful with :func:`with_timeout` so hangs surface early
rather than silently consuming worker capacity. The :func:`http_timeout`
builder ensures every :class:`httpx.AsyncClient` constructed in this
service is given all four phase timeouts (``connect``, ``read``,
``write``, ``pool``) explicitly — defaulting to a single scalar would
mask which phase actually exceeded the budget when latency anomalies
appear in Kibana.

Design constraints
------------------
- **No business-logic imports.** This module is foundational and SHOULD be
  importable during early bootstrap (logging configuration, container
  construction, test fixtures) without dragging in any service-specific
  state. ``depends_on_files`` is therefore intentionally empty.
- **No environment-variable reads.** Configuration discovery belongs in
  ``src.config.settings``; this file only operates on the values its
  callers pass in.
- **No runtime side effects** beyond obtaining a structlog logger. The
  module is safe to import many times concurrently without risk of
  duplicate state.

Compliance notes
----------------
- AAP R-15 — every outbound call has an explicit timeout boundary.
- AAP R-19 — fail-fast behavior at the call-site level (a hung coroutine
  that respects ``with_timeout`` cannot keep a readiness probe green
  forever).
- AAP R-26 — timeouts emit structured JSON WARNING logs with
  ``operation_timeout`` event name, the operation name, and the configured
  timeout in seconds, so Kibana dashboards can aggregate timeout activity
  across the fleet.

Cross-references
----------------
- Consumed by ``src/container.py`` when constructing the httpx
  ``AsyncClient`` for the Product Service (passes the result of
  :func:`http_timeout` to the ``timeout=`` constructor parameter).
- Consumed by ``src/repository/product_client.py`` (and any other future
  module needing an explicit async upper bound) via :func:`with_timeout`
  to guarantee bounded await semantics.
- Re-exported from the package root via ``src.resilience.__init__``.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------
# ``asyncio`` provides the canonical ``wait_for`` primitive used to bound a
# coroutine's runtime, and the ``TimeoutError`` exception class re-raised on
# expiry. Note: since Python 3.11 ``asyncio.TimeoutError`` is an *alias* for
# the built-in ``TimeoutError`` (PEP-678 / cpython issue #91987); both names
# resolve to the same class object at runtime, so callers that ``except
# TimeoutError`` will catch the exception this helper raises.
import asyncio

# ``Awaitable`` types the coroutine parameter precisely (any object with
# ``__await__`` is acceptable, including coroutines, futures, and tasks).
# ``TypeVar`` parameterizes the return type so static checkers preserve the
# inner coroutine's type through the helper. ``Any`` is reserved for fallback
# typing in places where a precise type would over-constrain the signature.
from typing import Any, Awaitable, TypeVar  # noqa: F401  ``Any`` retained for downstream parity

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
# ``httpx`` is the async-first HTTP client used throughout this service to
# call the Product Service (see AAP Section 0.4.2). ``httpx.Timeout`` exposes
# four independent phases — connect, read, write, pool — each of which can
# trip in isolation, so building a Timeout from per-phase values is more
# surgical than the single-scalar shorthand.
import httpx

# ``structlog`` powers the structured JSON logs mandated by AAP R-26. The
# module-level logger captures a stable ``logger=`` field that consumers can
# pivot on in Kibana to find every timeout fired anywhere in this module.
import structlog


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
# The structlog logger is bound once at import time. structlog's lazy proxy
# means this does NOT freeze the global processor chain; configuration
# changes applied later in ``src.main.lifespan`` still take effect for log
# lines emitted after that point.
logger = structlog.get_logger(__name__)

# Generic type variable parameterizing :func:`with_timeout`'s return value.
# Declared at module scope (not inside the function) so static checkers can
# infer the awaited type without re-parameterizing on each call.
T = TypeVar("T")


# ---------------------------------------------------------------------------
# Public API — coroutine timeout
# ---------------------------------------------------------------------------


async def with_timeout(
    coro: Awaitable[T],
    seconds: float,
    *,
    name: str | None = None,
) -> T:
    """Await ``coro`` with a bounded timeout.

    This is a thin wrapper over :func:`asyncio.wait_for` that adds:

    1. **Strict input validation.** ``seconds`` must be a positive, finite
       number; ``None``, zero, negatives, NaN, and ``+inf`` are rejected
       eagerly with :class:`ValueError` so misconfigurations surface at the
       call site instead of producing an unbounded await.
    2. **A structured WARNING log** emitted on timeout with the operation
       name and the configured budget — feeding the AAP R-26 / R-28 Kibana
       dashboards that surface elevated timeout activity per operation.
    3. **A clearer re-raised error message** that names the operation
       (when provided) and the expired budget in seconds, so the message is
       immediately actionable in observability tooling without having to
       walk the exception chain.

    The original ``asyncio.TimeoutError`` is preserved as ``__cause__`` via
    ``raise ... from exc`` so traceback rendering, Sentry-style cause
    chains, and Jaeger span tags retain the underlying primitive's context.

    Args:
        coro: The awaitable to await. May be a coroutine, a
            :class:`asyncio.Task`, a :class:`asyncio.Future`, or any other
            object with an ``__await__`` method.
        seconds: The upper-bound duration in seconds. MUST be a strictly
            positive, finite ``int`` or ``float``. Zero, negatives, NaN,
            and infinities are rejected.
        name: Optional operation name. When set it is included in the
            re-raised exception message and emitted as a ``name=`` field
            on the WARNING log line, making it easy to attribute timeouts
            to specific call sites in Kibana.

    Returns:
        Whatever ``coro`` returns when it completes within the budget.

    Raises:
        ValueError: If ``seconds`` is not a positive, finite number.
        asyncio.TimeoutError: If ``coro`` does not complete within
            ``seconds``. The exception's message has the form
            ``"operation timed out after <s>s"`` (with a trailing
            ``" operation=<name>"`` segment when ``name`` was supplied),
            and its ``__cause__`` is the underlying
            :class:`asyncio.TimeoutError` raised by
            :func:`asyncio.wait_for`. Note that since Python 3.11
            ``asyncio.TimeoutError`` is an alias for the built-in
            :class:`TimeoutError`, so callers may catch either name.
        Exception: Any exception raised by ``coro`` itself propagates
            unchanged — the helper does not catch or wrap non-timeout
            errors, preserving the principle that
            ``with_timeout(failing_coro, ...)`` raises the *original*
            failure rather than masking it as a timeout.

    Example:
        >>> async def fetch_product(pid):  # doctest: +SKIP
        ...     return await client.get(f"/products/{pid}")
        >>> async def main():  # doctest: +SKIP
        ...     try:
        ...         result = await with_timeout(
        ...             fetch_product("p-42"),
        ...             seconds=2.5,
        ...             name="product_lookup",
        ...         )
        ...     except asyncio.TimeoutError:
        ...         # Fall back to cached / popularity-based recommendations
        ...         result = popularity_fallback()
    """
    # Type guard: ``bool`` is a subclass of ``int`` and would otherwise pass
    # the ``isinstance`` check below, but treating ``True``/``False`` as
    # timeout values is almost certainly a programmer error. Reject them
    # explicitly with a clear message so callers don't get the surprising
    # behavior of "1 second" or "0 seconds" from a stray boolean expression.
    if isinstance(seconds, bool):
        raise ValueError("seconds must be a number, not a bool")
    # Reject everything that isn't a real number (str, None, list, etc.).
    # Note: this MUST happen before the comparison checks below, because
    # ``"abc" <= 0`` raises a confusing :class:`TypeError` in Python 3.
    if not isinstance(seconds, (int, float)):
        raise ValueError("seconds must be a number")
    # Reject NaN early — every comparison involving NaN is False, so the
    # ``seconds <= 0`` guard below would silently accept NaN otherwise. The
    # canonical NaN test is ``x != x`` (NaN is the only value not equal to
    # itself); we use that rather than ``math.isnan`` to avoid pulling in
    # ``math`` for a single check.
    if seconds != seconds:
        raise ValueError("seconds must be a finite positive number")
    # Reject zero and negatives — both would make the wrapped coroutine
    # appear to "fail before it started" without any chance to make
    # progress, which is rarely the caller's intent.
    if seconds <= 0:
        raise ValueError("seconds must be > 0")
    # Reject ``+inf`` (and ``-inf``, though ``seconds <= 0`` already caught
    # the negative case). ``asyncio.wait_for`` accepts ``None`` to mean "no
    # timeout", but we intentionally do not expose that affordance — if
    # the caller wants no timeout, they should not call this helper at all.
    if seconds == float("inf"):
        raise ValueError("seconds must be a finite positive number")

    try:
        # ``asyncio.wait_for`` is the canonical primitive: it cancels the
        # awaitable and re-raises ``asyncio.TimeoutError`` if the budget is
        # exceeded. The newer ``asyncio.timeout`` context manager (Python
        # 3.11+) offers identical semantics, but ``wait_for`` is the
        # broader-compat choice and reads well at the call site.
        return await asyncio.wait_for(coro, timeout=seconds)
    except asyncio.TimeoutError as exc:
        # Build the human-readable suffix only when ``name`` was supplied,
        # so the unnamed case stays terse: ``"operation timed out after
        # 2.500s"`` rather than ``"operation timed out after 2.500s
        # operation=None"``. ``!r`` quoting on ``name`` mirrors the format
        # used by stdlib :class:`Exception` reprs and is helpful when the
        # operation name contains whitespace or punctuation.
        label = f" operation={name!r}" if name else ""
        # AAP R-26: structured JSON WARNING with stable event name
        # ``operation_timeout`` so Kibana queries / alerts can pivot on
        # ``event=operation_timeout`` to spot regressions.
        logger.warning(
            "operation_timeout",
            name=name,
            timeout_seconds=seconds,
        )
        # Re-raise with a clearer message AND preserve the original
        # exception via ``from exc`` so traceback rendering shows both the
        # helpful summary and the underlying primitive's context.
        # Format budget to milliseconds resolution (``.3f`` -> ``2.500``),
        # which is more readable than a bare ``2.5`` and stays bounded for
        # long timeouts (``300.000``).
        raise asyncio.TimeoutError(
            f"operation timed out after {seconds:.3f}s{label}",
        ) from exc


# ---------------------------------------------------------------------------
# Public API — httpx Timeout builder
# ---------------------------------------------------------------------------


def http_timeout(
    connect_ms: int,
    read_ms: int,
    pool_ms: int,
    *,
    write_ms: int | None = None,
) -> httpx.Timeout:
    """Build an :class:`httpx.Timeout` from per-phase millisecond values.

    httpx exposes four independent timeout phases:

    - ``connect`` — TCP connect + TLS handshake.
    - ``read`` — server takes too long to send the next response chunk.
    - ``write`` — client takes too long to send the next request chunk.
    - ``pool`` — the connection pool is fully utilized and no slot
      becomes available within the configured wait.

    The httpx default of a single scalar for all four phases makes it easy
    to accidentally raise the overall timeout because connect was slow on
    one host — only to discover later that reads are now allowed to hang
    for the same elevated duration. Building Timeout objects from per-phase
    values prevents that class of regression entirely.

    All arguments are integers in **milliseconds** to match the
    Settings-file vocabulary
    (``connect_timeout_ms``, ``read_timeout_ms``, ``pool_timeout_ms``,
    ``write_timeout_ms``). The conversion to httpx's float-seconds
    representation happens here so callers never juggle mixed units.

    Args:
        connect_ms: Maximum milliseconds allowed for TCP connect plus the
            TLS handshake. MUST be ``>= 0``.
        read_ms: Maximum milliseconds allowed for the server to send each
            response chunk after the request was issued. MUST be ``>= 0``.
        pool_ms: Maximum milliseconds the client will wait to acquire a
            connection from the pool when all connections are in use.
            MUST be ``>= 0``.
        write_ms: Maximum milliseconds allowed to send each request chunk.
            Defaults to ``read_ms`` when omitted — the common case for
            symmetric I/O. MUST be ``>= 0`` when explicitly provided.

    Returns:
        An :class:`httpx.Timeout` ready to be passed to the ``timeout=``
        keyword argument of :class:`httpx.AsyncClient` (or any other httpx
        client constructor).

    Raises:
        ValueError: If any millisecond value is negative.

    Example:
        >>> from httpx import AsyncClient
        >>> client = AsyncClient(  # doctest: +SKIP
        ...     timeout=http_timeout(
        ...         connect_ms=1000,
        ...         read_ms=2000,
        ...         pool_ms=500,
        ...     ),
        ... )
    """
    # Validate the three required arguments together for a single,
    # actionable error message. We deliberately do NOT enumerate which one
    # was negative — operators read the call site to know which is which.
    if connect_ms < 0 or read_ms < 0 or pool_ms < 0:
        raise ValueError("all millisecond values must be >= 0")
    # ``write_ms`` is validated separately because its default is "fall
    # back to ``read_ms``", which is itself already known to be non-negative
    # at this point.
    if write_ms is not None and write_ms < 0:
        raise ValueError("write_ms must be >= 0")

    # Default the write phase to mirror the read phase. Most outbound
    # request payloads are small enough that the read budget is also a
    # reasonable upper bound for sending the request — and a single value
    # avoids the configuration sprawl of carrying a separate ``write_ms``
    # in every settings file when symmetric I/O is fine.
    effective_write_ms: int = write_ms if write_ms is not None else read_ms

    # ``httpx.Timeout`` requires EITHER a single scalar timeout OR all four
    # phase values; mixing the two raises a :class:`ValueError` at httpx's
    # construction time. We always pass all four phases for explicitness so
    # the returned object's ``str()`` / ``repr()`` shows the full breakdown
    # in logs and tests.
    return httpx.Timeout(
        connect=connect_ms / 1000.0,
        read=read_ms / 1000.0,
        write=effective_write_ms / 1000.0,
        pool=pool_ms / 1000.0,
    )


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------
# ``__all__`` is sorted alphabetically (per project style — see
# ``src.domain.errors``) so re-exports and ``from src.resilience.timeout
# import *`` stay stable and self-documenting.
__all__ = [
    "http_timeout",
    "with_timeout",
]
