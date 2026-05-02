"""Correlation-ID middleware and saga-domain context helpers (AAP R-13, R-26).

This module is the **single source of truth** for correlation-ID propagation
inside the Order Service. It implements two related but independent concerns
so that all saga / request context plumbing lives in one place and cannot
drift between consumers:

1. **Per-request correlation-ID** — :class:`CorrelationIdMiddleware` extracts
   (or, when missing/invalid, generates) the ``X-Correlation-ID`` header on
   every inbound HTTP request and exposes the resolved value through:

       * a module-level :data:`_correlation_id_var` ``ContextVar`` (read via
         :func:`get_correlation_id`) — used by ``src/container.py``'s httpx
         outbound-request hook to inject the same header on every downstream
         HTTP call (AAP R-13: correlation-ID propagated through ALL
         outbound calls).
       * ``structlog.contextvars`` (key: ``correlation_id``) — the
         ``merge_contextvars`` processor configured in
         ``src/observability/logger.py`` makes the field appear automatically
         on every JSON log line emitted during the request scope (AAP R-26:
         structured JSON logs include ``correlation_id``).
       * the active OpenTelemetry server span (attribute: ``correlation_id``)
         — distributed-trace systems can join Elasticsearch logs to traces by
         the same identifier.
       * the response ``X-Correlation-ID`` header — clients can copy the
         value into support tickets so operators paste it into Kibana to
         retrieve every log line related to the request.

2. **Saga-domain log enrichment** — :func:`set_saga_context` and
   :func:`clear_saga_context` bind / unbind ``saga_id``, ``order_id``,
   ``current_step``, and ``awaiting_event`` into ``structlog.contextvars``
   so that subsequent log calls during saga step processing (in
   ``src/saga/coordinator.py``, ``src/saga/scheduler.py``) automatically
   include those fields per the format string in ``config/log_config.json``
   and AAP R-18 (saga pattern with explicit compensation).

Architectural rules satisfied
-----------------------------
* **AAP R-13** — Correlation ID propagated through ALL outbound calls and
  ALL log lines. Generated at the API Gateway and passed through every
  service. This module is the propagation primitive on the inbound side.
* **AAP R-26** — Structured JSON logs include the ``correlation_id`` field
  on every line emitted within the request scope.
* **AAP R-18** — Saga-domain fields enrich log lines so operators can
  follow a single saga across services and steps in Kibana.

Module-level invariants
-----------------------
* **No imports** from any other ``src.middleware.*`` module — keeps the
  middleware DAG flat and prevents accidental import cycles.
* **No imports** from ``src.config.settings`` at module level — settings
  are read at request time via the FastAPI app's wiring, NOT forced on
  module import (so unit tests can import this file without instantiating
  the global Settings object).
* **No I/O** at import time — only the ``ContextVar`` is created.
* **No** ``print()`` statements (AAP R-26 forbids them).

Runtime middleware order (per the folder spec)::

    CorrelationId (outermost) -> StructuredLogging -> JWTAuth ->
    ErrorHandler -> route handler

The ``CorrelationId`` hook runs FIRST so every inner middleware and every
route handler observes a fully populated correlation context.

See also
--------
* ``src/container.py`` — late-imports :func:`get_correlation_id` for the
  httpx outbound-request hook.
* ``src/events/producer.py`` — calls :func:`get_correlation_id` to attach
  ``correlation_id`` to Kafka message headers on every published event.
* ``src/observability/logger.py`` — configures structlog with
  ``merge_contextvars`` so bound fields automatically appear on log
  events.
* ``src/observability/tracing.py`` — documents that this middleware
  attaches the ``correlation_id`` attribute to every server-side span.
* ``src/saga/coordinator.py`` and ``src/saga/scheduler.py`` — call
  :func:`set_saga_context` / :func:`clear_saga_context` around saga step
  processing.
"""

from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Awaitable, Callable

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

# OpenTelemetry import is guarded with try/except as defensive
# belt-and-braces. ``opentelemetry-api`` is declared in
# ``requirements.txt`` so the import should always succeed in production
# and CI; the fallback path exists exclusively for hypothetical
# lightweight test environments that strip OTEL out (e.g., a focused
# unit test rig that imports this module without installing the full
# observability stack).
try:
    from opentelemetry import trace as _otel_trace

    _HAS_OTEL: bool = True
except ImportError:  # pragma: no cover - tracing is a hard dep but stay defensive
    _otel_trace = None  # type: ignore[assignment]
    _HAS_OTEL = False


# =============================================================================
# Module-level state
# =============================================================================

#: Maximum length of a correlation ID. Mirrors the per-message-id
#: contract across the platform: bounded to keep log payloads compact
#: and to defend against header-injection abuse (a 1MB
#: ``X-Correlation-ID`` would bloat every log line in Elasticsearch and
#: every Kafka message header). 128 is comfortably larger than a hex
#: UUID4 (32 chars) and a hyphenated UUID4 (36 chars), leaving headroom
#: for upstream gateways that concatenate trace IDs while still firmly
#: rejecting unbounded blobs.
_MAX_CORRELATION_ID_LENGTH: int = 128

#: Default header name used by :class:`CorrelationIdMiddleware` when no
#: explicit ``header_name`` is passed to the constructor. The FastAPI
#: app wires ``settings.observability.correlation_id_header`` (which
#: defaults to the same value) so the production binding goes through
#: the Settings object; this default exists so the class can be
#: instantiated in unit tests and isolated benchmarks without the full
#: Settings dependency tree.
_DEFAULT_HEADER_NAME: str = "X-Correlation-ID"

#: ContextVar holding the current request's correlation ID across
#: ``await`` boundaries.
#:
#: * ``None`` when not inside an HTTP request (e.g., Kafka consumer poll
#:   loops, scheduler ticks, FastAPI startup / shutdown hooks). Callers
#:   that want a non-None value in those contexts MUST set the ContextVar
#:   themselves (typically before kicking off a unit of work) or fall
#:   back to a fresh UUID via :func:`uuid.uuid4`.
#: * Set by :meth:`CorrelationIdMiddleware.dispatch` for the duration of
#:   each HTTP request, then RESET via the token returned from
#:   ``ContextVar.set`` so the value never leaks across requests handled
#:   by the same task.
#:
#: ``ContextVar`` (not ``request.state``) is the primitive of choice
#: because it propagates across ``asyncio`` ``await`` boundaries WITHOUT
#: requiring the FastAPI ``Request`` object to be threaded through every
#: call site. The httpx outbound-request hook in ``container.py`` does
#: NOT have access to the inbound request; it only has access to the
#: outbound request being made. ContextVars provide ambient context
#: that flows through async tasks, which is exactly what AAP R-13
#: ("propagate correlation_id through ALL outbound calls") requires.
_correlation_id_var: ContextVar[str | None] = ContextVar(
    "_correlation_id_var", default=None
)


__all__ = [
    "CorrelationIdMiddleware",
    "get_correlation_id",
    "set_saga_context",
    "clear_saga_context",
]


# =============================================================================
# Public functions
# =============================================================================


def get_correlation_id() -> str | None:
    """Return the current request's correlation ID, or ``None`` outside a request.

    Used by:
        * ``src/container.py``'s httpx event hook to inject
          ``X-Correlation-ID`` on every outbound HTTP request (AAP R-13).
        * The Kafka producer (via the event producer wrapper in
          ``src/events/producer.py``) to attach ``correlation_id`` to
          message headers on every published event.
        * Any application code that needs to read the correlation ID
          without depending on the FastAPI ``Request`` object (e.g., a
          background task spawned from a request handler that should
          inherit the correlation ID for log correlation).

    The function is a thin wrapper around :meth:`ContextVar.get`; it is
    intentionally NOT raising when the value is ``None`` so callers can
    decide whether to fall back to ``uuid.uuid4().hex`` (typical for
    background tasks that lack a triggering request) or to short-circuit
    (typical for outbound hooks that simply omit the header when the
    value is unavailable).

    Returns:
        The string correlation ID set by :class:`CorrelationIdMiddleware`,
        or ``None`` when called outside an HTTP request scope (e.g.,
        background poll loops, scheduler ticks, startup / shutdown hooks).
    """
    return _correlation_id_var.get()


def set_saga_context(
    *,
    saga_id: str | None = None,
    order_id: str | None = None,
    current_step: str | None = None,
    awaiting_event: str | None = None,
) -> None:
    """Bind saga-domain fields into structlog contextvars for log enrichment.

    Called by the saga coordinator / event handlers when a saga step is
    being processed, so that subsequent log lines automatically include
    these fields (per ``config/log_config.json`` format string and
    AAP R-26 / R-18).

    The fields are propagated through ``structlog.contextvars`` and end
    up in every JSON log line emitted from the same task. They do NOT
    automatically flow into the OpenTelemetry span — callers that want
    span attributes should call
    ``trace.get_current_span().set_attribute(...)`` explicitly.

    All four parameters are keyword-only (``*`` separator) and all are
    optional. Passing ``None`` for any parameter SKIPS that key — the
    caller can therefore set just the saga_id at the start of saga
    processing and refine ``current_step`` / ``awaiting_event`` later
    without unbinding-and-rebinding the whole set.

    Args:
        saga_id: UUID of the saga being processed (string form).
        order_id: UUID of the order being processed (string form).
        current_step: Current ``SagaStep`` value (e.g., ``"AWAIT_PAYMENT"``).
        awaiting_event: Topic name being awaited (e.g.,
            ``"payment.succeeded"``); ``None`` for terminal steps that
            wait for nothing.

    Note:
        This helper does NOT enforce that all four fields are set
        together — partial binds are valid. The complementary
        :func:`clear_saga_context` unbinds the same set of keys
        atomically.

        Calling :func:`set_saga_context` with all four arguments left at
        ``None`` is a no-op; ``structlog.contextvars.bind_contextvars``
        is NOT invoked in that case.
    """
    bind_kwargs: dict[str, str] = {}
    if saga_id is not None:
        bind_kwargs["saga_id"] = saga_id
    if order_id is not None:
        bind_kwargs["order_id"] = order_id
    if current_step is not None:
        bind_kwargs["current_step"] = current_step
    if awaiting_event is not None:
        bind_kwargs["awaiting_event"] = awaiting_event
    if bind_kwargs:
        structlog.contextvars.bind_contextvars(**bind_kwargs)


def clear_saga_context() -> None:
    """Remove saga-domain context fields from structlog contextvars.

    Called at the end of saga step processing (typically in a ``finally``
    block in ``src/saga/coordinator.py`` and ``src/saga/scheduler.py``)
    to prevent field bleed into subsequent log lines that are not part
    of the same saga step.

    The unbind is unconditional and idempotent: keys that were never
    bound are silently ignored by ``structlog.contextvars.unbind_contextvars``,
    so calling :func:`clear_saga_context` without a matching
    :func:`set_saga_context` is safe.

    Symmetric counterpart to :func:`set_saga_context`.
    """
    structlog.contextvars.unbind_contextvars(
        "saga_id", "order_id", "current_step", "awaiting_event"
    )


# =============================================================================
# Private helpers
# =============================================================================


def _coerce_correlation_id(raw: str | None) -> str:
    """Validate and normalize an inbound correlation-ID header value.

    Rules:
        * If ``raw`` is ``None`` or empty after strip -> generate fresh UUID4.
        * If ``raw`` is longer than :data:`_MAX_CORRELATION_ID_LENGTH`
          (128 chars) -> generate fresh UUID4.
        * If ``raw`` contains non-printable or non-ASCII characters
          (anything outside codepoints 32..126) -> generate fresh UUID4.
        * Otherwise return the stripped value verbatim.

    The "regenerate on invalid" pattern is intentional: we never want a
    malformed inbound value to pollute logs or break Elasticsearch
    queries. Generating a fresh UUID is safer than rejecting the
    request because gateways may already be relying on us to "always
    produce a clean correlation_id" — rejecting requests because the
    gateway sent a malformed header would break that contract.
    Coercing (regenerating) is the safer pattern; the malformed value
    is silently dropped, the system continues.

    The printable-ASCII filter is defense against log injection. A
    correlation_id of ``"\\nfake_user=admin\\n"`` could fool a poorly
    parsed log scraper into believing a separate log line followed.
    Restricting to printable ASCII (codepoints 32..126) eliminates this
    entire class of attack and keeps the value safe to emit verbatim
    in JSON logs and HTTP headers without escaping.

    The fallback identifier is ``uuid.uuid4().hex`` — a 32-character
    lowercase hexadecimal string with no hyphens. This compact form
    matches the platform-wide convention for correlation IDs and is
    safely transportable in HTTP headers and Kafka message headers
    without quoting.

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
    # Reject non-printable / non-ASCII to defend against log/header injection.
    # Codepoints 32..126 cover all printable ASCII characters: SP through ~.
    if not all(32 <= ord(c) < 127 for c in stripped):
        return uuid.uuid4().hex
    return stripped


# =============================================================================
# CorrelationIdMiddleware
# =============================================================================


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Per-request correlation-ID extraction, propagation, and emission.

    Behavior:
        1. **Extract** the ``X-Correlation-ID`` header (case-insensitive
           lookup is provided by Starlette's ``Headers`` mapping) from
           the inbound request.
        2. **Validate** the value: non-empty, length <= 128 chars,
           printable ASCII only. If absent or invalid, **generate** a
           ``uuid.uuid4().hex`` instead (defense against header injection
           that could pollute logs or break Elasticsearch queries).
        3. **Bind** the resolved correlation ID into:

               * the module-level :data:`_correlation_id_var`
                 ``ContextVar``, capturing the reset token so the value
                 never leaks across requests handled by the same task.
               * ``structlog.contextvars`` under the key
                 ``correlation_id`` so the ``merge_contextvars``
                 processor automatically includes it on every JSON log
                 line emitted during the request (AAP R-26).
               * the active OpenTelemetry server span as an attribute
                 named ``correlation_id`` when tracing is enabled and
                 the span is recording (NoOp / non-recording spans are
                 skipped to avoid wasted attribute-set calls).

        4. **Invoke** the next handler in the middleware chain.
        5. **Echo** the resolved correlation ID back on the response as
           ``X-Correlation-ID`` (overwrites any value the application
           may have set so the header is always present and consistent
           with the value used in logs / spans). Clients can use this
           value for support tickets — operators paste the ID into
           Kibana to find every log line related to the request.
        6. **Reset** the ``ContextVar`` token in a ``finally`` block so
           the value never leaks across requests handled by the same
           task. ``structlog.contextvars.unbind_contextvars`` is also
           called to remove the key from the structlog context.

    Per the folder spec, the runtime middleware order is::

        CorrelationId (outermost) -> StructuredLogging -> JWTAuth ->
        ErrorHandler -> route handler

    so this middleware's request hook runs FIRST, ensuring every
    downstream log line and every span created by inner middlewares
    already carries the ``correlation_id`` field.

    Configuration:
        ``header_name`` defaults to ``"X-Correlation-ID"``. The FastAPI
        app SHOULD pass
        ``settings.observability.correlation_id_header`` to the
        ``add_middleware`` call if that setting differs from the
        default (e.g., to align with an upstream gateway that uses a
        non-standard header name).

    See AAP R-13 (correlation-ID propagation), AAP R-26 (structured
    logs).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        header_name: str = _DEFAULT_HEADER_NAME,
    ) -> None:
        """Construct the middleware.

        Args:
            app: The downstream ASGI application (next middleware or
                the FastAPI router).
            header_name: HTTP header to read from the request and write
                back on the response. Keyword-only so misordered
                positional args at the call site can't silently swap
                ``app`` and the header name. Defaults to
                ``"X-Correlation-ID"`` per the IETF tracing convention.
        """
        super().__init__(app)
        self._header_name = header_name

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Run the correlation-ID lifecycle around a single request.

        Implements the six-step protocol documented on the class
        docstring: extract, validate-or-generate, bind, invoke, echo,
        reset.

        Args:
            request: The inbound Starlette ``Request`` (case-insensitive
                header access is provided by ``request.headers``).
            call_next: Coroutine factory that, when awaited, drives the
                rest of the middleware chain and the route handler and
                returns the resulting ``Response``.

        Returns:
            The downstream ``Response``, with the ``X-Correlation-ID``
            header set to the resolved correlation ID.

        Note:
            The ``ContextVar`` token is reset in a ``finally`` block so
            the value is removed even if ``call_next`` raises. The
            downstream ``ErrorHandler`` middleware is responsible for
            converting exceptions into responses; this middleware does
            NOT swallow exceptions.
        """
        # 1-2. Extract + validate (or generate).
        # ``request.headers.get`` is case-insensitive — Starlette stores
        # headers in a ``Headers`` mapping that lower-cases keys.
        raw = request.headers.get(self._header_name)
        correlation_id = _coerce_correlation_id(raw)

        # 3. Bind into the ContextVar, structlog contextvars, and the
        #    active span. The order is intentional: ContextVar first so
        #    inner middleware that reads ``get_correlation_id()`` sees
        #    the value; structlog second so the very next log line
        #    already has the field; OTEL last because it is the most
        #    expensive of the three and we want to guarantee the
        #    cheaper bindings always succeed.
        token = _correlation_id_var.set(correlation_id)
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

        if _HAS_OTEL:
            # ``trace.get_current_span()`` returns the current active
            # span from the OTEL context. When tracing is not configured
            # this is a NoOp span whose ``is_recording()`` returns
            # ``False`` — the guard avoids wasted attribute-set calls.
            span = _otel_trace.get_current_span()
            if span is not None and span.is_recording():
                span.set_attribute("correlation_id", correlation_id)

        try:
            # 4. Process the request through the rest of the middleware
            #    stack and the route handler.
            response = await call_next(request)

            # 5. Echo the correlation ID back on the response.
            #    Direct assignment overwrites any value the application
            #    may have set, ensuring the header is always present
            #    and consistent with the value used in logs / spans.
            response.headers[self._header_name] = correlation_id
            return response
        finally:
            # 6. Always reset, even on exceptions (the ErrorHandler
            #    middleware downstream will handle the response). This
            #    guarantees that the correlation ID does NOT leak into
            #    subsequent requests scheduled on the same asyncio task.
            _correlation_id_var.reset(token)
            structlog.contextvars.unbind_contextvars("correlation_id")
