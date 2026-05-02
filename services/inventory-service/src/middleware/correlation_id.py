"""Correlation-ID propagation middleware for the Inventory Service.

This module implements the **outermost** middleware in the Inventory
Service's middleware stack — the foundational observability primitive
that every other middleware and outbound call depends on.

It satisfies three architectural rules from the Agent Action Plan (AAP):

* **AAP R-13** — *Every external call must propagate a correlation ID
  (generated at the API Gateway) through to external providers where
  supported, and must be present on every log line.* This middleware is
  the inbound entry point for that propagation: when the API Gateway
  forwards a request with ``X-Correlation-ID``, we adopt it verbatim;
  otherwise we mint a fresh 32-character hex UUID so downstream Kafka
  events, structured logs, and outbound HTTP calls (warehouse-adapter
  HTTP requests in the future, JWKS fetches today) carry a non-empty
  value.
* **AAP R-26** — *All logs must be structured JSON with* ``correlation_id``
  *as a mandatory field.* We bind the resolved value into
  ``structlog.contextvars`` so the ``merge_contextvars`` processor
  configured in :mod:`src.observability.logging_setup` automatically
  pulls ``correlation_id`` into every JSON log line emitted during the
  request scope, with no per-call-site work required by the application
  code that emits log lines.
* **AAP Section 0.4.5** — *The correlation-ID middleware is a first-class
  cross-cutting interceptor that must inject or propagate*
  ``X-Correlation-ID`` *across HTTP calls and Kafka message headers.*

Public API
----------
* :class:`CorrelationIdMiddleware` — the Starlette
  :class:`~starlette.middleware.base.BaseHTTPMiddleware` implementation
  that runs the per-request correlation-ID lifecycle on every inbound
  HTTP call. Construction takes a single keyword-only argument
  ``header_name`` (default ``"X-Correlation-ID"``). The class exposes
  exactly one method, ``dispatch``, which wraps each call to the
  downstream ASGI app with the six-step lifecycle described below.
* :func:`get_correlation_id` — module-level accessor that returns the
  current task's correlation ID (or ``None`` outside an HTTP request
  scope). Consumed by sibling modules that need to enrich outbound
  payloads with the inbound correlation ID:

  - ``src.container``'s outbound httpx event-hook attaches the value as
    the ``X-Correlation-ID`` header on every downstream HTTP request,
    satisfying AAP R-13's "propagate through every outbound call"
    clause.
  - The Kafka event producer (``src.events.producer``) attaches the
    value as the ``X-Correlation-ID`` Kafka message header on every
    published event so consumers (Order Service, Recommendation Engine,
    Notification Service) can correlate consumed events back to the
    inbound request that triggered them.
  - The auth middleware's error-body builder and the structured-logging
    middleware's error-response builder include the value in JSON
    error envelopes so clients can quote it in support tickets.

ContextVar lifecycle (for every inbound HTTP request)
-----------------------------------------------------
The middleware runs the following six-step protocol on every request,
guaranteed by the ``try/finally`` block in :meth:`CorrelationIdMiddleware.dispatch`:

1. **Extract** the inbound ``X-Correlation-ID`` header
   (case-insensitive lookup via Starlette's ``Headers`` mapping).
2. **Validate** the value via :func:`_coerce_correlation_id` — invalid
   or missing values are replaced with a freshly generated 32-character
   hex UUID. Validation rules:

   * ``None`` or empty after strip → generate UUID.
   * Length > :data:`_MAX_CORRELATION_ID_LENGTH` (128 chars) → generate
     UUID.
   * Any character outside printable ASCII (codepoints 32..126) →
     generate UUID. This rejects control characters, raw bytes, and
     non-Latin-1 content that some clients may accidentally send. It
     also defends against header / log injection attacks where a
     malicious ``\\r\\n`` could split log lines or response headers.
   * Otherwise → return the stripped value verbatim.

3. **Bind** the resolved value into the module-level
   :data:`_correlation_id_var` ``ContextVar`` so any downstream code
   running in the same asyncio task can read it via
   :func:`get_correlation_id` without threading the value through
   every function signature; AND into ``structlog.contextvars`` so
   every log line emitted during the request automatically includes
   the ``correlation_id`` field.
4. **Attach** the value to the active OpenTelemetry span (when OTel is
   installed and a span is active) so distributed-trace systems can
   join Elasticsearch logs to traces by the same identifier.
5. **Invoke** the downstream ASGI chain via ``await call_next(request)``
   and **echo** the resolved correlation ID back on the response's
   ``X-Correlation-ID`` header so clients can quote it in support
   tickets.
6. **Reset** the ``ContextVar`` token in a ``finally`` block (and unbind
   the structlog contextvar) so the value never leaks across requests
   handled by the same asyncio task.

Module-level invariants
-----------------------
* **No imports** from any other ``src.middleware.*`` module — this file
  is foundational, and any internal cross-import would create a circular
  dependency. Sibling middlewares (``auth``, ``structured_logging``,
  ``error_handler``) and other consumers (``src.container``,
  ``src.events.producer``) import :func:`get_correlation_id` from THIS
  module, never the reverse.
* **No I/O** at import time — only the ``ContextVar`` is created; no
  app instantiation, no logger construction, no environment lookup.
* **No** ``print()`` statements (AAP R-26 forbids them).
* **OpenTelemetry is optional** — the import is wrapped in
  ``try/except ImportError`` so this middleware works in minimal test
  environments that strip the OTel package out, even though
  ``opentelemetry-api`` is declared in ``requirements.txt`` and is
  available in production.

Pattern conformance
-------------------
This file mirrors the pattern of ``services/order-service/src/middleware/
correlation_id.py`` deliberately, with a single intentional adaptation:
the Inventory Service does NOT need ``set_saga_context`` /
``clear_saga_context`` helpers because it is a saga *participant*
(consumer of ``order.*`` events) rather than the saga *coordinator*. The
two helpers exist solely on the order-service module where saga step
processing happens; here the only structlog binding is
``correlation_id`` itself.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------
import uuid
from contextvars import ContextVar
from typing import Awaitable, Callable, Final

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

# ---------------------------------------------------------------------------
# Optional OpenTelemetry import — guarded so the middleware works even if
# the OTel package isn't installed in a minimal test environment. The
# ``opentelemetry-api`` package IS declared in
# ``services/inventory-service/requirements.txt`` and is expected to be
# present in production / CI; the fallback path exists exclusively for
# focused unit-test rigs that intentionally strip the observability
# stack out to keep the test surface small.
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace as _otel_trace

    # NOTE: ``_HAS_OTEL`` is intentionally NOT annotated ``Final[bool]``
    # here. ``Final`` cannot be reassigned in the ``except`` branch
    # below, but a try/except import gate inherently requires
    # assignment in both branches. The variable is treated as a
    # de-facto module-level constant by every consumer; reassignment
    # is structurally impossible past the try/except block since no
    # other code path touches it.
    _HAS_OTEL: bool = True
except ImportError:  # pragma: no cover - exercised only without OTel installed
    _otel_trace = None  # type: ignore[assignment]
    _HAS_OTEL = False


# =============================================================================
# Module-level constants and state
# =============================================================================

#: Maximum permitted length of an inbound correlation-ID header value.
#: Generous (cloud-platform request IDs typically max at 64) but bounded
#: to defend against header-bloat abuse: a multi-megabyte
#: ``X-Correlation-ID`` would inflate every log line in Elasticsearch
#: AND every Kafka message header sent for the duration of the request.
#: 128 is comfortably larger than ``uuid.uuid4().hex`` (32 chars) and a
#: hyphenated UUID4 (36 chars), leaving headroom for upstream gateways
#: that concatenate trace IDs while still firmly rejecting unbounded
#: blobs.
_MAX_CORRELATION_ID_LENGTH: Final[int] = 128

#: Default header name used by :class:`CorrelationIdMiddleware` when no
#: explicit ``header_name`` is passed to the constructor. ``"X-Correlation-ID"``
#: is the AAP R-13 canonical header name (the value the API Gateway
#: sets before forwarding to this service); the constructor accepts an
#: override so tests / specialized deployments can rebind it (for
#: example, a deployment fronted by a CDN that prefers
#: ``X-Request-ID``).
_DEFAULT_HEADER_NAME: Final[str] = "X-Correlation-ID"

#: ``ContextVar`` holding the current request's correlation ID across
#: ``await`` boundaries.
#:
#: * ``None`` outside an inbound HTTP request scope (e.g., FastAPI
#:   startup / shutdown hooks, Kafka consumer poll loops, the
#:   reservation-expiry scheduler tick). Callers in those contexts
#:   that require a non-None value MUST set the ContextVar themselves
#:   (typically before kicking off a unit of work) or fall back to a
#:   freshly generated ``uuid.uuid4().hex``.
#: * Set by :meth:`CorrelationIdMiddleware.dispatch` for the duration
#:   of each inbound request, then RESET via the token returned from
#:   :meth:`ContextVar.set` so the value never leaks across requests
#:   handled on the same asyncio task.
#:
#: ``ContextVar`` (rather than ``request.state``) is the primitive of
#: choice because it propagates across ``asyncio`` ``await`` boundaries
#: WITHOUT requiring the FastAPI ``Request`` object to be threaded
#: through every call site. The ``src.container``'s outbound httpx
#: event-hook does NOT have access to the inbound request — it only
#: has access to the outbound request being made — so an ambient
#: storage primitive that flows through async tasks is exactly what
#: AAP R-13 ("propagate ``correlation_id`` through ALL outbound calls")
#: requires.
_correlation_id_var: ContextVar[str | None] = ContextVar(
    "_correlation_id_var",
    default=None,
)


# =============================================================================
# Public exports
# =============================================================================

#: Public API surface; everything else in this module is module-private.
#: ``_correlation_id_var``, ``_coerce_correlation_id``,
#: ``_attach_to_otel_span``, ``_MAX_CORRELATION_ID_LENGTH``,
#: ``_DEFAULT_HEADER_NAME``, ``_HAS_OTEL``, ``_otel_trace`` are NOT in
#: ``__all__`` because they are implementation details that may change
#: without notice.
__all__: list[str] = [
    "CorrelationIdMiddleware",
    "get_correlation_id",
]


# =============================================================================
# Public functions
# =============================================================================


def get_correlation_id() -> str | None:
    """Return the correlation ID for the current task, or ``None``.

    The function is a thin wrapper around :meth:`ContextVar.get`; it is
    intentionally NOT raising when the value is ``None`` so callers can
    decide whether to fall back to ``uuid.uuid4().hex`` (typical for
    background tasks that lack a triggering request) or to short-circuit
    (typical for outbound hooks that simply omit the header when no
    correlation ID is in scope).

    Used by every consumer that needs to enrich an outbound payload
    with the inbound correlation ID:

    * :func:`src.container._build_http_client._correlation_hook` — sets
      ``X-Correlation-ID`` on every outbound httpx request (AAP R-13).
    * :func:`src.middleware.auth._error_body` — populates the
      ``correlation_id`` field of 401 / 503 error responses so the JSON
      envelope sent to the client carries the same correlation ID that
      appears in the service's structured logs.
    * The structured-logging middleware's error-response builders —
      populate ``correlation_id`` on every error envelope.
    * Kafka producer wrappers (``src.events.producer``) — set the
      ``X-Correlation-ID`` Kafka message header on every published
      event so downstream consumers can correlate consumed events back
      to the inbound HTTP request that triggered them.

    Returns:
        The string correlation ID set by
        :class:`CorrelationIdMiddleware` for the current request, or
        ``None`` when called outside an active request (e.g., during
        container construction, FastAPI startup / shutdown hooks, or
        scheduler ticks not driven by an inbound HTTP call).
    """
    return _correlation_id_var.get()


# =============================================================================
# Module-private helpers
# =============================================================================


def _coerce_correlation_id(raw: str | None) -> str:
    """Validate and normalize an inbound correlation-ID header value.

    The "regenerate on invalid" pattern is intentional: we never want a
    malformed inbound value to pollute logs or break Elasticsearch
    queries. Generating a fresh UUID is safer than rejecting the
    request because the API Gateway in front of this service may
    already be relying on us to *always* produce a clean
    ``correlation_id`` — rejecting requests because the gateway sent a
    malformed header would break that contract. Coercing (regenerating)
    is the safer pattern; the malformed value is silently dropped, the
    system continues serving traffic.

    The printable-ASCII filter is defense against log and header
    injection. A correlation ID of ``"\\nfake_user=admin\\n"`` could
    fool a poorly parsed log scraper into believing a separate log line
    followed; a correlation ID of ``"x\\r\\nSet-Cookie: session=evil"``
    could split the outbound response into two HTTP messages on a
    misconfigured proxy. Restricting to printable ASCII (codepoints
    32..126) eliminates both classes of attack and keeps the value
    safe to emit verbatim in JSON logs and HTTP headers without
    escaping.

    The fallback identifier is ``uuid.uuid4().hex`` — a 32-character
    lowercase hexadecimal string with no hyphens. This compact form
    matches the platform-wide convention for correlation IDs and is
    safely transportable in HTTP headers and Kafka message headers
    without quoting.

    Validation rules:
        * ``None`` or empty after strip → generate UUID.
        * Length > :data:`_MAX_CORRELATION_ID_LENGTH` (128 chars) →
          generate UUID.
        * Any character outside printable ASCII (codepoints 32..126) →
          generate UUID. Rejects control characters, raw bytes, and
          non-Latin-1 content that some clients may accidentally send.
        * Otherwise → return the stripped value verbatim.

    Args:
        raw: Header value from the inbound request (may be ``None``).

    Returns:
        A clean, bounded, printable-ASCII correlation ID — either the
        sanitized inbound value or a freshly minted ``uuid.uuid4().hex``
        when the inbound value is missing or invalid.
    """
    if raw is None:
        return uuid.uuid4().hex
    stripped = raw.strip()
    if not stripped:
        return uuid.uuid4().hex
    if len(stripped) > _MAX_CORRELATION_ID_LENGTH:
        return uuid.uuid4().hex
    for char in stripped:
        codepoint = ord(char)
        if codepoint < 32 or codepoint > 126:
            return uuid.uuid4().hex
    return stripped


def _attach_to_otel_span(correlation_id: str) -> None:
    """Set the correlation ID as an attribute on the current OTel span.

    No-op when OpenTelemetry is not installed (``_HAS_OTEL`` is
    ``False``) OR when there is no active span (e.g., during a
    synchronous test request that doesn't pass through an
    OTel-instrumented entry point, or when the OTel SDK has been
    configured but no tracer provider is yet active).

    The attribute name follows OTel's snake_case convention so that
    Kibana / Jaeger queries on ``correlation_id`` work consistently
    across services.

    Defensive design:
        * The ``_HAS_OTEL`` flag short-circuits before touching the
          OTel module so a missing import cannot raise here.
        * The ``span is None`` guard handles the case where OTel is
          installed but no span context exists yet.
        * The ``try/except Exception`` is belt-and-braces: an OTel
          SDK bug (e.g., span has been ended on another thread, or a
          custom span implementation raises on ``set_attribute``)
          MUST NOT break request handling. We swallow any exception
          rather than letting it propagate up through dispatch.

    Args:
        correlation_id: The validated correlation ID to attach.
    """
    if not _HAS_OTEL or _otel_trace is None:  # pragma: no cover - configured-out branch
        return
    span = _otel_trace.get_current_span()
    if span is None:
        return
    try:
        span.set_attribute("correlation_id", correlation_id)
    except Exception:  # pragma: no cover - belt-and-braces guard
        # Never let OpenTelemetry failures affect request handling. The
        # correlation ID is already in the ContextVar and structlog
        # contextvars; the OTel attachment is best-effort.
        pass


# =============================================================================
# CorrelationIdMiddleware
# =============================================================================


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Per-request correlation-ID extraction, propagation, and emission.

    Behavior on every inbound HTTP request:

    1. **Extract** the ``X-Correlation-ID`` header (case-insensitive
       lookup is provided by Starlette's ``Headers`` mapping which
       lower-cases keys for storage) from the inbound request.
    2. **Validate** the value via :func:`_coerce_correlation_id`:
       non-empty, length <= :data:`_MAX_CORRELATION_ID_LENGTH`,
       printable ASCII only. If absent or invalid, *generate* a
       ``uuid.uuid4().hex`` instead (defense against header injection
       that could pollute logs or break Elasticsearch queries).
    3. **Bind** the resolved correlation ID into:

       * the module-level :data:`_correlation_id_var` ``ContextVar``,
         capturing the reset token so the value never leaks across
         requests handled on the same asyncio task.
       * ``structlog.contextvars`` under the key ``correlation_id`` so
         the ``merge_contextvars`` processor automatically includes it
         on every JSON log line emitted during the request scope
         (AAP R-26).

    4. **Attach** the value to the active OpenTelemetry server span as
       an attribute named ``correlation_id`` (when OTel is installed
       and a span is active) so distributed-trace systems can join
       Elasticsearch logs to traces by the same identifier.
    5. **Invoke** the downstream ASGI chain via
       ``await call_next(request)``.
    6. **Echo** the resolved correlation ID back on the response as
       the ``X-Correlation-ID`` header so clients can quote it in
       support tickets — operators paste the ID into Kibana to find
       every log line related to the request. Direct assignment
       overwrites any value the application may have set, ensuring the
       header is always present and consistent with the value used in
       logs / spans.
    7. **Reset** the ``ContextVar`` token in a ``finally`` block (and
       unbind the structlog contextvar) so the value never leaks
       across requests handled on the same asyncio task.

    Construction
    ------------
    ``__init__(self, app, *, header_name="X-Correlation-ID")``

    The ``header_name`` keyword arg lets tests / specialized deployments
    override the inbound header name (for example, a deployment fronted
    by a CDN that uses ``X-Request-ID``). The same value is used for
    both the inbound read AND the outbound response echo so the two
    directions never drift.

    Runtime middleware order
    ------------------------
    This middleware MUST be the OUTERMOST in the chain so the
    correlation ID is in scope for every other middleware and the
    route handler. Per the inventory-service folder spec the canonical
    order is::

        CorrelationId (outermost) -> StructuredLogging -> JWTAuth ->
        ErrorHandler -> route handler

    so this middleware's request hook runs FIRST, ensuring every
    downstream log line and every span created by inner middlewares
    already carries the ``correlation_id`` field.

    See also AAP R-13 (correlation-ID propagation), AAP R-26
    (structured logs).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        header_name: str = _DEFAULT_HEADER_NAME,
    ) -> None:
        """Construct the middleware.

        Args:
            app: The downstream ASGI application — the next middleware
                in the chain or the FastAPI router itself. Required by
                :class:`~starlette.middleware.base.BaseHTTPMiddleware`'s
                contract.
            header_name: HTTP header to read from the request and write
                back on the response. Keyword-only (``*`` separator)
                so misordered positional args at the call site can't
                silently swap ``app`` and the header name. Defaults
                to :data:`_DEFAULT_HEADER_NAME` (``"X-Correlation-ID"``)
                per the AAP R-13 canonical convention.
        """
        super().__init__(app)
        # ``Final`` annotation makes the immutability of the per-instance
        # header name explicit to mypy --strict and prevents accidental
        # rebinding inside dispatch.
        self._header_name: Final[str] = header_name

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Run the correlation-ID lifecycle around a single request.

        Implements the seven-step protocol documented on the class
        docstring: extract, validate-or-generate, bind, attach,
        invoke, echo, reset.

        Args:
            request: The inbound Starlette :class:`Request`.
                Case-insensitive header access is provided by
                ``request.headers`` (Starlette's :class:`Headers`
                mapping lower-cases keys for storage; HTTP/2 lower-
                cases header names on the wire).
            call_next: Coroutine factory that, when awaited, drives the
                rest of the middleware chain and the route handler and
                returns the resulting :class:`Response`.

        Returns:
            The downstream :class:`Response`, with the configured
            ``X-Correlation-ID`` header set to the resolved correlation
            ID.

        Note:
            The ``ContextVar`` token is reset in a ``finally`` block so
            the value is removed even if ``call_next`` raises. The
            downstream error-handler middleware is responsible for
            converting exceptions into responses; this middleware does
            NOT swallow exceptions.
        """
        # 1-2. Extract + validate (or generate). ``request.headers.get``
        # is case-insensitive: Starlette stores headers in a
        # ``Headers`` mapping that normalizes keys to lowercase.
        raw_inbound = request.headers.get(self._header_name)
        correlation_id = _coerce_correlation_id(raw_inbound)

        # 3. Bind into the ContextVar and structlog contextvars. The
        # order is intentional: ContextVar first so any inner
        # middleware that calls ``get_correlation_id()`` sees the
        # value; structlog second so the very next log line already
        # carries the ``correlation_id`` field.
        token = _correlation_id_var.set(correlation_id)
        # Tracks whether the structlog bind succeeded. If
        # ``bind_contextvars`` itself raised, we do NOT call
        # ``unbind_contextvars`` in the finally block (defensive: the
        # bind never happened and an unmatched unbind on a never-bound
        # key is a no-op in practice but the explicit guard keeps the
        # control flow auditable).
        bound_structlog = False
        try:
            structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
            bound_structlog = True

            # 4. Attach to the active OpenTelemetry server span. The
            # helper is a no-op when OTel is not installed or no span
            # is active; failures are swallowed defensively so OTel
            # bugs cannot break request handling.
            _attach_to_otel_span(correlation_id)

            # 5. Drive the downstream middleware chain and route
            # handler. Exceptions raised here propagate out of the
            # try block; the ``finally`` block below still runs to
            # reset the ContextVar.
            response = await call_next(request)

            # 6. Echo the resolved correlation ID back on the
            # response. Direct assignment OVERWRITES any value the
            # application or inner middleware may have set, ensuring
            # the header is always present and consistent with the
            # value used in logs / spans. Using ``self._header_name``
            # (NOT the literal ``"X-Correlation-ID"``) means custom
            # configurations are honored on both directions.
            response.headers[self._header_name] = correlation_id
            return response
        finally:
            # 7. Reset on EVERY exit path (normal return, exception,
            # cancellation). This is the safety mechanism that
            # guarantees the correlation ID does NOT leak into
            # subsequent requests handled on the same asyncio task.
            if bound_structlog:
                structlog.contextvars.unbind_contextvars("correlation_id")
            _correlation_id_var.reset(token)
