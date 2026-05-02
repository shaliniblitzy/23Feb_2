"""Correlation-ID middleware and accessor for the User Service.

This module is the **single source of truth** for the correlation-ID
primitive used throughout the User Service. It implements:

* :class:`CorrelationIdMiddleware` — pure-ASGI middleware. Extracts
  ``X-Correlation-ID`` from the inbound request (case-insensitive) OR
  generates a fresh ``uuid.uuid4().hex`` when the inbound value is
  absent or malformed. Sanitizes malformed values (>128 chars or any
  character outside the ``[A-Za-z0-9._-]`` whitelist) by regenerating.
  Binds the resolved value into:

      * a module-level :class:`contextvars.ContextVar`
        (:data:`_correlation_id_var`) so non-logging consumers can read
        it without depending on structlog or the FastAPI ``Request``
        object,
      * structlog's contextvars store (key: ``correlation_id``) so the
        ``merge_contextvars`` processor configured FIRST in
        ``src/observability/logging.py`` automatically merges it into
        every emitted JSON log line during the request,
      * the active OpenTelemetry server span as an attribute named
        ``correlation_id`` (when tracing is enabled and the span is
        recording),
      * ``scope["state"]["correlation_id"]`` for downstream middleware
        and route handlers that prefer reading from the ASGI scope.

  Echoes the resolved value back on the response as the configured
  header so clients can stitch their client-side logs to our
  server-side logs end-to-end (operators paste the ID into Kibana to
  retrieve every log line related to the request).

* :func:`get_correlation_id` — public accessor. Returns the current
  request's correlation ID or ``None`` when called outside a request
  scope. **MUST NOT raise** — the httpx event hook in
  ``src/container.py`` calls it on every outbound HTTP request and
  any exception there would break every outbound call. The Kafka
  producer plumbing in ``src/events/correlation.py`` and the
  ``ErrorHandlerMiddleware`` in ``src/middleware/error_handler.py``
  also rely on the no-raise contract.

Architectural rules satisfied
-----------------------------
* **AAP R-13** — Every external call must propagate a correlation ID
  (generated at the API Gateway) through to external providers where
  supported, and must be present on every log line. This module is
  the propagation primitive on the inbound side; outbound propagation
  is performed by the httpx event hook in ``src/container.py`` and
  the Kafka producer plumbing in ``src/events/correlation.py``, both
  of which call :func:`get_correlation_id`.
* **AAP Section 0.4.5** — Correlation-ID middleware injects or
  propagates ``X-Correlation-ID`` across HTTP calls and Kafka message
  headers for distributed tracing.
* **AAP R-26** — Structured JSON logs include the ``correlation_id``
  field on every log line emitted within the request scope (auto-merged
  via ``merge_contextvars``).

Runtime middleware order (per the User Service folder spec)::

    CorrelationIdMiddleware (outermost)
        -> RequestLoggingMiddleware
            -> ErrorHandlerMiddleware (innermost)
                -> route handler

Registration in ``src/main.py`` is in REVERSE order so this middleware
is added LAST and therefore wraps OUTERMOST at runtime (Starlette wraps
later-added middlewares around earlier-added ones).

Module-level invariants
-----------------------
* **No imports** from any other ``src.middleware.*`` module — keeps the
  middleware DAG flat and prevents accidental import cycles.
* **No imports** from ``src.config.settings`` at module level — the
  header name is passed via the constructor so unit tests can import
  this file without instantiating the global Settings object.
* **No imports** from ``src.observability.logging`` — structlog is
  imported directly; the ``merge_contextvars`` processor is configured
  by ``configure_logging(settings)`` at startup, not by this file.
* **No I/O** at import time — only the ContextVar is created.
* **No** ``print()`` statements (AAP R-26 forbids them).

See also
--------
* ``src/main.py`` — registers
  ``CorrelationIdMiddleware(header_name=settings.observability.correlation_id_header)``
  as the OUTERMOST middleware via ``app.add_middleware(...)`` LAST in
  the reverse-order block.
* ``src/container.py`` — late-imports :func:`get_correlation_id` for
  the httpx outbound-request hook.
* ``src/events/correlation.py`` — imports :func:`get_correlation_id`
  to attach ``correlation_id`` to Kafka message headers.
* ``src/middleware/error_handler.py`` — imports
  :func:`get_correlation_id` to embed the correlation ID in RFC 7807
  problem+json error bodies.
* ``src/middleware/request_logging.py`` — does NOT import directly;
  ``correlation_id`` flows in via structlog ``merge_contextvars``.
* Sibling references (canonical patterns adopted here):
  ``services/notification-service/src/middleware/correlation_id.py``
  (pure ASGI, dual-binding, granular unbind) and
  ``services/order-service/src/middleware/correlation_id.py``
  (OpenTelemetry span attribute, ``uuid.uuid4().hex`` form, coerce
  helper).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``contextvars.ContextVar`` provides the asyncio-safe per-task storage
# slot that backs :data:`_correlation_id_var`. ContextVars are isolated
# per ``asyncio.Task``, so concurrent in-flight requests cannot read or
# overwrite each other's correlation IDs even though the var is
# module-global. We deliberately maintain BOTH a module-level
# ``ContextVar`` and the structlog contextvars binding so non-logging
# consumers (httpx outbound hook in ``container.py``, Kafka producer in
# ``events/correlation.py``) can read the active correlation ID without
# depending on structlog's private storage layout.
from contextvars import ContextVar

# ``re`` is used to compile and apply :data:`_ALLOWED_CHARS_RE` — the
# strict whitelist regex that rejects malformed inbound correlation-ID
# values (whitespace, quotes, control characters, non-ASCII bytes).
# Compiling the regex once at module-load time amortizes the cost
# across the millions of requests this hot-path middleware will see.
import re

# ``uuid.uuid4().hex`` produces a 32-character lowercase hexadecimal
# identifier with no hyphens. ``uuid4`` (random) is preferred over
# ``uuid1`` (timestamp + MAC) because the correlation ID is exposed in
# response headers and log lines visible to clients; ``uuid1`` would
# leak the host MAC address and a high-resolution wall-clock value,
# neither of which is appropriate to expose externally. The ``.hex``
# form is preferred over ``str(uuid4())`` for compactness in log
# payloads and consistency with sibling order-service middleware.
import uuid

# ``typing.Final`` is used to annotate module-level constants (PEP 591).
# This is mandated by the file's style rules (Phase 9: "``Final`` is
# used for module-level constants"). It tells mypy --strict that the
# value must not be reassigned, providing a stronger guarantee than a
# plain type annotation.
from typing import Final

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``starlette.types`` provides the canonical ASGI type aliases. We
# import them rather than declaring our own so the middleware's
# signatures are SOURCE-COMPATIBLE with the rest of the Starlette /
# FastAPI middleware ecosystem (e.g., ``CORSMiddleware``,
# ``Middleware``, ``add_middleware``):
#   - ``ASGIApp``  — the inner application reference (``self._app``).
#   - ``Scope``    — ASGI request scope (dict) passed to ``__call__``.
#   - ``Receive``  — async callable yielding incoming ``Message`` events.
#   - ``Send``     — async callable accepting outgoing ``Message`` events.
#   - ``Message``  — dict shape observed in the wrapping
#                    ``send_with_header`` closure when injecting the
#                    ``X-Correlation-ID`` response header into the
#                    ``http.response.start`` event.
# We deliberately do NOT import ``BaseHTTPMiddleware`` or any FastAPI /
# Starlette framework primitives (``Request``, ``Response``); pure ASGI
# gives us everything we need with zero message-buffering overhead on
# hot-path routes (``/health/live``, ``/health/ready``, ``/metrics``).
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# ``structlog`` is the primary structured-logging library configured
# by ``src/observability/logging.py``. We import the top-level module
# (rather than ``from structlog.contextvars import ...``) so it is
# explicit at every call site that we are reaching into the
# ``contextvars`` submodule — both ``bind_contextvars`` and
# ``unbind_contextvars`` are called from this module. The
# ``merge_contextvars`` processor (installed FIRST in the structlog
# processor chain) auto-merges the ``correlation_id`` key bound here
# into every log record emitted within the request scope, satisfying
# AAP R-26 without requiring every log call site to pass the ID
# explicitly.
import structlog

# ---------------------------------------------------------------------------
# Optional OpenTelemetry import (defensive belt-and-braces)
# ---------------------------------------------------------------------------
# OpenTelemetry is a hard dependency in ``services/user-service/requirements.txt``
# (``opentelemetry-api~=1.24``), so the import succeeds in production
# and CI. The ``try/except ImportError`` exists exclusively for
# hypothetical lightweight test environments that strip OTEL out — and
# satisfies the agent prompt's "MUST NOT raise" contract for
# :func:`get_correlation_id` (the module must load even if OTEL is
# missing). The ``# pragma: no cover`` directive tells coverage tools
# to ignore the fallback branch since CI always exercises the import-
# success path.
try:
    from opentelemetry import trace as _otel_trace

    _HAS_OTEL: Final[bool] = True
except ImportError:  # pragma: no cover - opentelemetry-api is in requirements.txt
    _otel_trace = None  # type: ignore[assignment]
    # ``Final`` declared on the success branch above. mypy --strict flags
    # this re-assignment as ``misc`` ("Cannot assign to final name") even
    # though the two branches are mutually exclusive at runtime; the
    # ``type: ignore[misc]`` documents that we accept the conditional-
    # assignment pattern as the cost of the ``Final`` annotation on the
    # primary path. The branch is unreachable in production because
    # ``opentelemetry-api`` is declared in
    # ``services/user-service/requirements.txt``; this exists only to
    # satisfy the agent-prompt contract that ``get_correlation_id`` MUST
    # NOT raise even if OTEL is stripped from a hypothetical lightweight
    # test environment.
    _HAS_OTEL = False  # type: ignore[misc]


# ===========================================================================
# Module-level constants
# ===========================================================================

#: Default header name read from the inbound request and written to the
#: outbound response. Overridable per-instance via the constructor's
#: ``header_name`` kwarg, which the FastAPI app wires from
#: ``settings.observability.correlation_id_header`` so the header name
#: can be overridden via configuration without code changes.
_DEFAULT_HEADER_NAME: Final[str] = "X-Correlation-ID"

#: Maximum length of an inbound correlation ID. Bounded to keep log
#: payloads compact and to defend against header-injection abuse — a
#: 1MB ``X-Correlation-ID`` would bloat every log line in
#: Elasticsearch and every Kafka message header. 128 chars is
#: comfortably larger than a hex UUID4 (32 chars) and a hyphenated
#: UUID4 (36 chars), leaving headroom for upstream gateways that
#: concatenate trace IDs while still firmly rejecting unbounded blobs.
_MAX_CORRELATION_ID_LENGTH: Final[int] = 128

#: Whitelist regex applied to inbound correlation-ID values after
#: ASCII decode and whitespace strip. Permits alphanumerics, hyphens,
#: underscores, and dots — the union of characters used by UUID4 hex
#: representations, hyphenated service-prefix tokens, dotted
#: namespace conventions (``svc.req.42``), and underscore-separated
#: identifiers. Deliberately EXCLUDES whitespace, quotes, control
#: characters, and unicode codepoints — all of which would either
#: break log-format round-trips, introduce JSON-escape bugs, or
#: expose non-ASCII PII. The expression is anchored with ``^...$``
#: so partial matches are also rejected. Defense against log
#: injection: a correlation_id of ``"\nfake_user=admin\n"`` could
#: fool a poorly-parsed log scraper into believing a separate log
#: line followed.
_ALLOWED_CHARS_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._\-]+$")

#: ASGI scope state key where downstream middleware/controllers can
#: read the correlation ID without re-parsing headers. Starlette
#: exposes this slot as ``request.state.correlation_id`` via FastAPI;
#: raw-ASGI middleware reads it directly from
#: ``scope["state"]["correlation_id"]``.
_CORRELATION_SCOPE_KEY: Final[str] = "correlation_id"

#: Structlog contextvars key. Auto-merged into every emitted log event
#: via the ``merge_contextvars`` processor configured FIRST in the
#: chain by ``src/observability/logging.py``. Identical to
#: :data:`_CORRELATION_SCOPE_KEY` by convention so callers reading
#: from either location get the same field name.
_CORRELATION_LOG_KEY: Final[str] = "correlation_id"


# ===========================================================================
# Per-task correlation-ID storage (ContextVar)
# ===========================================================================

#: Module-level :class:`contextvars.ContextVar` holding the correlation
#: ID for the currently-executing asyncio task.
#:
#: * **Default value of None**: A consumer that reads the var outside
#:   any active request context (e.g., during process startup, from a
#:   background task spawned without ``contextvars.copy_context``,
#:   from a Kafka consumer poll loop, or from a scheduler tick) gets
#:   ``None`` and can decide whether to mint a placeholder, log
#:   without the field, or skip entirely. A non-None default would
#:   mask such bugs by always returning a stale value.
#:
#: * **Set by the middleware**: :meth:`CorrelationIdMiddleware.__call__`
#:   sets this slot at the beginning of each HTTP request and resets
#:   it via the captured :class:`contextvars.Token` in a ``finally:``
#:   block so the value never leaks across requests handled by the
#:   same task.
#:
#: * **Read by non-logging consumers**: The httpx outbound-request hook
#:   in ``src/container.py`` calls :func:`get_correlation_id` (which
#:   wraps ``ContextVar.get``) on every outbound request to inject the
#:   same ``X-Correlation-ID`` header. The Kafka producer in
#:   ``src/events/correlation.py`` reads the same slot to attach
#:   ``correlation_id`` to message headers on every published event.
#:   The error handler in ``src/middleware/error_handler.py`` reads
#:   the slot to embed the ID in RFC 7807 problem+json bodies.
#:
#: * **Why not ``request.state``?** ContextVars propagate across
#:   ``await`` boundaries WITHOUT requiring the FastAPI ``Request``
#:   object to be threaded through every call site. The httpx
#:   outbound hook in ``container.py`` does NOT have access to the
#:   inbound request; it only has access to the outbound request
#:   being made. ContextVars provide ambient context that flows
#:   through async tasks (httpx, Kafka producer, etc.).
#:
#: The leading underscore signals that this is a module-private
#: identifier — public access goes through :func:`get_correlation_id`.
_correlation_id_var: ContextVar[str | None] = ContextVar(
    "_correlation_id_var", default=None
)


# ===========================================================================
# Public exports
# ===========================================================================

# ``__all__`` declares the public symbols exported by this module that
# participate in ``from src.middleware.correlation_id import *``.
#
# - :class:`CorrelationIdMiddleware` is the primary export consumed by
#   ``src/main.py`` via ``app.add_middleware(...)``.
# - :func:`get_correlation_id` is the secondary export consumed by
#   the httpx outbound-request hook in ``src/container.py``, by the
#   Kafka producer in ``src/events/correlation.py``, and by the
#   error handler in ``src/middleware/error_handler.py``.
#
# The module-level ContextVar (``_correlation_id_var``), the helper
# (``_coerce_correlation_id``), the constants (``_DEFAULT_HEADER_NAME``,
# ``_MAX_CORRELATION_ID_LENGTH``, ``_ALLOWED_CHARS_RE``,
# ``_CORRELATION_SCOPE_KEY``, ``_CORRELATION_LOG_KEY``), and the
# ``_HAS_OTEL`` flag are deliberately NOT exported (leading underscore
# signals private). Tests can still access them via direct import.
__all__: list[str] = [
    "CorrelationIdMiddleware",
    "get_correlation_id",
]


# ===========================================================================
# Public accessor
# ===========================================================================


def get_correlation_id() -> str | None:
    """Return the current request's correlation ID, or ``None`` outside a request.

    Used by:
        * ``src/container.py``'s httpx event hook to inject the
          ``X-Correlation-ID`` header on every outbound HTTP request
          (AAP R-13).
        * The Kafka producer (``src/events/producer.py`` via
          ``src/events/correlation.py``) to attach ``correlation_id``
          to message headers on every published event.
        * :class:`src.middleware.error_handler.ErrorHandlerMiddleware`
          to embed the correlation ID in RFC 7807 problem+json error
          responses when a domain error did not carry one.
        * Any application code that needs to read the correlation ID
          without depending on the FastAPI ``Request`` object (e.g.,
          a background task spawned from a request handler that should
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
        background poll loops, scheduler ticks, startup / shutdown
        hooks).

    Notes:
        This function MUST NOT raise. The httpx event hook in
        ``container.py`` calls it on every outbound request; raising
        would break every outbound HTTP call. ``ContextVar.get()``
        with a default never raises, so the function is automatically
        safe.
    """
    return _correlation_id_var.get()


# ===========================================================================
# Private helpers
# ===========================================================================


def _coerce_correlation_id(raw: str | None) -> str:
    """Validate and normalize an inbound correlation-ID header value.

    Rules:
        * If ``raw`` is ``None`` or empty after strip -> generate fresh
          ``uuid.uuid4().hex``.
        * If the stripped value is longer than
          :data:`_MAX_CORRELATION_ID_LENGTH` (128 chars) -> generate fresh.
        * If the stripped value contains any character outside the
          :data:`_ALLOWED_CHARS_RE` whitelist (alphanumerics, hyphens,
          underscores, dots) -> generate fresh.
        * Otherwise return the stripped value verbatim.

    The "regenerate on invalid" pattern is intentional: we never want a
    malformed inbound value to pollute logs or break Elasticsearch
    queries. Generating a fresh UUID is safer than rejecting the
    request because gateways may already be relying on us to "always
    produce a clean correlation_id" — rejecting requests because the
    gateway sent a malformed header would break that contract.
    Coercing (regenerating) is the safer pattern; the malformed value
    is silently dropped, the system continues.

    The whitelist regex is defense against log injection. A
    correlation_id of ``"\\nfake_user=admin\\n"`` could fool a poorly
    parsed log scraper into believing a separate log line followed.
    Restricting to alphanumerics + hyphens + underscores + dots
    eliminates this entire class of attack and keeps the value safe to
    emit verbatim in JSON logs and HTTP headers without escaping.

    The fallback identifier is ``uuid.uuid4().hex`` — a 32-character
    lowercase hexadecimal string with no hyphens. This compact form
    matches the platform-wide convention for correlation IDs (sibling
    order-service middleware uses the same form) and is safely
    transportable in HTTP headers and Kafka message headers without
    quoting.

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
    if not _ALLOWED_CHARS_RE.match(stripped):
        return uuid.uuid4().hex
    return stripped


# ===========================================================================
# CorrelationIdMiddleware
# ===========================================================================


class CorrelationIdMiddleware:
    """Pure-ASGI middleware that injects or propagates ``X-Correlation-ID``.

    Behaviour:
        1. **Extract** the configured correlation header (default
           ``X-Correlation-ID``, case-insensitive) from the inbound
           request headers.
        2. **Validate** the value: non-empty, <= 128 chars, characters
           restricted to ``[A-Za-z0-9._\\-]+``. If absent or invalid,
           **generate** a fresh ``uuid.uuid4().hex`` instead (defense
           against header injection that could pollute logs or break
           Elasticsearch queries).
        3. **Bind** the resolved correlation ID into:

               * the module-level :data:`_correlation_id_var` ContextVar
                 (capturing the reset Token for safe cleanup),
               * structlog contextvars (key: ``correlation_id``) so
                 every log line during the request carries it via the
                 ``merge_contextvars`` processor configured in
                 ``src/observability/logging.py``,
               * the active OpenTelemetry server span (attribute:
                 ``correlation_id``) when tracing is enabled and the
                 span is recording (NoOp / non-recording spans are
                 skipped to avoid wasted attribute-set calls; server-
                 span creation is handled by
                 ``opentelemetry-instrumentation-fastapi`` applied via
                 ``src/observability/tracing.instrument_fastapi``),
               * ``scope["state"]["correlation_id"]`` for downstream
                 middleware/controllers that prefer to read from scope.

        4. **Invoke** the next ASGI app in the chain (typically the
           ``RequestLoggingMiddleware``).
        5. **Echo** the resolved correlation ID back on the response by
           intercepting the ``http.response.start`` event and inserting
           the header. Strips any pre-existing value the inner app may
           have set so our value is authoritative — clients can use this
           value for support tickets (operators paste the ID into Kibana
           to find every log line related to the request).
        6. **Reset** the ContextVar Token AND unbind the structlog
           contextvar in a ``finally`` block so the value never leaks
           across requests handled by the same task. Other startup-bound
           keys (``service``, ``version``, ``environment``) are preserved
           because we use ``unbind_contextvars("correlation_id")`` rather
           than ``clear_contextvars()``.

    Pure ASGI vs ``BaseHTTPMiddleware``:
        ``starlette.middleware.base.BaseHTTPMiddleware`` buffers the
        entire response body into a ``Message`` queue to pass it
        through Starlette's ``dispatch()`` callback; that buffering is
        observable overhead on hot-path routes (``/health/live``,
        ``/health/ready``, ``/metrics``). Pure ASGI wires ``send``
        directly through the ``send_with_header`` closure, avoiding
        the buffer entirely. This middleware runs on EVERY request, so
        the lighter pure-ASGI implementation matches the canonical
        pattern established by sibling notification-service middleware
        (the closer architectural match for the User Service's 4-file
        layout). The User Service folder spec mentions
        ``BaseHTTPMiddleware`` but the canonical sibling pattern is
        adopted for performance.

    Args:
        app: The inner ASGI application (FastAPI / Starlette instance
            or the next middleware in the chain). Stored as
            ``self._app`` and invoked unchanged for non-HTTP scopes,
            or with the wrapped ``send_with_header`` for HTTP scopes.
        header_name: The HTTP header name to read on input and emit on
            output. Defaults to ``"X-Correlation-ID"``. The User
            Service ``main.py`` passes
            ``settings.observability.correlation_id_header`` here so
            the header name can be overridden via configuration.

    Example:
        >>> from fastapi import FastAPI
        >>> from src.middleware.correlation_id import CorrelationIdMiddleware
        >>> app = FastAPI()
        >>> # Register LAST so this middleware is OUTERMOST at runtime
        >>> # (Starlette wraps later-added middlewares around earlier
        >>> # ones).
        >>> app.add_middleware(CorrelationIdMiddleware)

    See AAP R-13 (correlation-ID propagation) and AAP R-26 (structured
    JSON logs).
    """

    # ----------------------------------------------------------------------
    # Construction
    # ----------------------------------------------------------------------
    def __init__(
        self,
        app: ASGIApp,
        header_name: str = _DEFAULT_HEADER_NAME,
    ) -> None:
        """Initialize the middleware.

        Args:
            app: The inner ASGI application to wrap.
            header_name: HTTP header to read from the request and write
                back on the response. Defaults to ``"X-Correlation-ID"``
                per the IETF tracing convention. The User Service's
                ``main.py`` passes
                ``settings.observability.correlation_id_header`` here
                so the header name can be overridden via configuration.
        """
        # Hold the inner app reference verbatim. We do not modify or
        # re-wrap it at construction time; the wrapping happens
        # per-request in ``__call__`` via the ``send_with_header``
        # closure. Storing the reference once here keeps the
        # per-request overhead at a single attribute lookup
        # (``self._app``) rather than re-resolving the app from a
        # registry.
        self._app: ASGIApp = app

        # Keep the original (case-preserving) header-name string so
        # tests that verify the configured header name see exactly
        # what was passed in. Production code paths exclusively use
        # ``self._header_name_lower`` for the actual header matching
        # and emission.
        self._header_name_str: str = header_name

        # Normalize the header name to lowercase ASCII bytes EXACTLY
        # ONCE here so the per-request comparison in
        # :meth:`_extract_inbound` and the response-header-injection
        # path in :meth:`__call__` stay cheap ``bytes == bytes``
        # checks. Per the ASGI spec, headers in ``scope["headers"]``
        # are delivered as a list of ``(lowercase-bytes, bytes)``
        # tuples, so comparing against the lowercase form is correct
        # even when clients emit the header in mixed case (RFC 7230
        # §3.2 declares header names case-insensitive). Encoding to
        # ASCII is safe because HTTP header names per RFC 7230 §3.2.6
        # are restricted to a subset of ASCII (the token grammar).
        self._header_name_lower: bytes = header_name.lower().encode("ascii")

    # ----------------------------------------------------------------------
    # Inbound extraction (static for testability)
    # ----------------------------------------------------------------------
    @staticmethod
    def _extract_inbound(scope: Scope, header_name_lower: bytes) -> str | None:
        """Return the inbound correlation ID if the header is present and valid.

        ASGI delivers headers as a list of ``(name_bytes, value_bytes)``
        tuples in ``scope["headers"]``, with header names canonicalized
        to lowercase. We iterate looking for an exact match on the
        lowercase header name (defensive ``.lower()`` is also applied
        on each element to support stubs / mocks that may pass
        mixed-case headers in unit tests).

        Validation pipeline (defense-in-depth):
            1. **ASCII decode.** ``value_bytes.decode("ascii", errors="strict")``
               — non-ASCII codepoints (potential PII or homograph
               attack) raise ``UnicodeDecodeError`` and we return
               ``None``.
            2. **Whitespace strip.** ``.strip()`` removes leading /
               trailing horizontal whitespace some misbehaving proxies
               emit.
            3. **Non-empty check.** An empty (or whitespace-only) value
               after the strip is treated as absent.

        Note that :func:`_coerce_correlation_id` performs the FINAL
        length and whitelist validation — this method only handles
        binary-level rejection (non-ASCII bytes / empty-after-strip).
        Returning the stripped string here defers regex / length checks
        to the coercion helper which has the symmetric "generate fresh
        on invalid" semantics; this avoids duplicating validation
        logic across two layers.

        Multiple header occurrences:
            RFC 7230 §3.2.2 permits multiple occurrences of the same
            header name only for fields whose grammar explicitly
            defines a comma-separated list. ``X-Correlation-ID`` is a
            custom scalar header; multiple occurrences are a client
            misuse. We accept the FIRST valid match and ignore the
            rest — this mirrors typical proxy behavior and avoids any
            ambiguity about which value to bind.

        Args:
            scope: The ASGI request scope dict.
            header_name_lower: The lowercase ``bytes`` form of the
                correlation header name (pre-normalized in
                ``__init__``).

        Returns:
            The inbound correlation-ID string (after ASCII decode and
            strip), or ``None`` if the header is missing, empty, or
            contains non-ASCII bytes. The caller (``__call__``) hands
            the result to :func:`_coerce_correlation_id` which
            applies final length and whitelist validation, generating
            a fresh ID when those checks fail.
        """
        # Default to an empty list when ``headers`` is absent — non-HTTP
        # scopes are already short-circuited in ``__call__``, but
        # ``scope.get`` is the safer access pattern for unit tests
        # that may pass minimal scopes.
        headers: list[tuple[bytes, bytes]] = scope.get("headers", [])

        for name_bytes, value_bytes in headers:
            # ``.lower()`` defensively in case a unit-test stub passes
            # mixed-case header names (the ASGI spec mandates
            # lowercase, but we don't want unit tests to crash on a
            # technicality).
            if name_bytes.lower() != header_name_lower:
                continue

            # ----- Step 1: ASCII decode --------------------------------
            # ``errors="strict"`` is essential here — using
            # ``"replace"`` would silently substitute the U+FFFD
            # replacement character on bad bytes and we'd propagate a
            # corrupted ID. Strict mode raises ``UnicodeDecodeError``
            # which we catch below and convert into a None return.
            try:
                value: str = value_bytes.decode("ascii", errors="strict").strip()
            except UnicodeDecodeError:
                # Non-ASCII bytes in the header value — could be a
                # mis-encoded UTF-8 string or random binary garbage.
                # Either way, treat as malformed and fall back to
                # generation via the caller. Returning ``None`` here
                # (rather than continuing to scan) is correct because
                # a single malformed value taints the whole header
                # line and we should not silently use a duplicate.
                return None

            # ----- Step 2: non-empty after strip -----------------------
            # The strip handles leading/trailing whitespace; an empty
            # result means the client sent ``X-Correlation-ID:`` with
            # no value (or only whitespace). Treat as absent and fall
            # back to generation.
            if not value:
                return None

            # All binary-level validations passed — return the stripped
            # string. The caller hands this to
            # :func:`_coerce_correlation_id` which applies length and
            # whitelist regex validation.
            return value

        # No matching header found in the entire list — the inbound
        # request did not carry the correlation header.
        return None

    # ----------------------------------------------------------------------
    # ASGI entry point
    # ----------------------------------------------------------------------
    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        """ASGI entry point — invoked by the ASGI server on each event.

        Dispatch:
            * **Non-HTTP scopes** (``lifespan``, ``websocket``) — pass
              through to the inner app untouched. Correlation IDs are
              an HTTP concept in this service; binding them at
              lifespan would produce misleading log lines at process
              startup / shutdown, and websockets do not have request /
              response semantics that map cleanly onto a single ID.
            * **HTTP scopes** — extract or generate a correlation ID,
              stash it on ``scope["state"]``, set
              :data:`_correlation_id_var`, bind it into structlog
              context vars, set it on the active OpenTelemetry span,
              wrap ``send`` with the header injector, and invoke the
              inner app in a ``try / finally`` block that guarantees
              cleanup of all bound context vars.

        Exception propagation:
            Any exception raised by the inner app propagates upward
            unchanged — this middleware has no business catching errors
            (that is ``ErrorHandlerMiddleware``'s job). The
            ``finally:`` block still runs and unbinds the
            ``correlation_id`` context var, so an exception path does
            not leave a stale ID bound for subsequent requests.

        Args:
            scope: The ASGI scope dict.
            receive: The ASGI receive callable; passed through unchanged.
            send: The ASGI send callable; replaced with
                ``send_with_header`` for HTTP scopes so the outbound
                response carries our ``X-Correlation-ID`` header.
        """
        # ------------------------------------------------------------------
        # Non-HTTP scopes: pass through untouched.
        #
        # The ASGI spec defines three scope types: ``http``,
        # ``websocket``, and ``lifespan``. Lifespan messages
        # (``startup``, ``shutdown``) arrive once per process;
        # websockets carry their own message semantics. Neither maps
        # onto a per-request correlation ID, so we forward them
        # verbatim. Importantly, we do NOT bind a correlation ID at
        # lifespan because the startup hook in ``src/main.py`` binds
        # ``service``, ``version``, and ``environment`` and we must
        # not pollute that long-lived context with a per-event ID.
        # ------------------------------------------------------------------
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        # ------------------------------------------------------------------
        # 1-2. Resolve the effective correlation ID for this request.
        #
        # ``_extract_inbound`` returns ``None`` when the header is
        # missing, empty after strip, or contains non-ASCII bytes.
        # ``_coerce_correlation_id`` applies the final length and
        # whitelist regex validation and falls back to
        # ``uuid.uuid4().hex`` when any check fails. The combined
        # pipeline guarantees ``correlation_id`` is always a
        # non-empty, bounded, ASCII-printable string.
        # ------------------------------------------------------------------
        inbound_raw = self._extract_inbound(scope, self._header_name_lower)
        correlation_id = _coerce_correlation_id(inbound_raw)

        # ------------------------------------------------------------------
        # 3a. Bind into the module-level :data:`_correlation_id_var`
        #     ContextVar so non-logging consumers (httpx outbound hook
        #     in container.py, Kafka producer in events/correlation.py,
        #     error handler) can read the active correlation ID without
        #     reaching into structlog's private contextvars storage or
        #     re-parsing the ASGI scope.
        #
        # ``ContextVar.set`` returns a :class:`contextvars.Token` we
        # capture for the matching ``reset`` in the ``finally:``
        # block. Using ``reset(token)`` (rather than ``set(None)``)
        # restores the variable to whatever value it held before
        # this request — which, for a request entering at the
        # outermost middleware, is the default ``None`` slot. The
        # Token-based pattern is the canonical asyncio-safe way to
        # scope a ContextVar to a single block of work, and it
        # handles the (currently impossible but future-proof) case
        # of nested set calls.
        # ------------------------------------------------------------------
        cv_token = _correlation_id_var.set(correlation_id)

        # ------------------------------------------------------------------
        # 3b. Bind into structlog contextvars so ``merge_contextvars``
        #     auto-includes the field on every log line emitted during
        #     the request.
        #
        # ``structlog.contextvars.bind_contextvars`` writes to
        # ``contextvars.ContextVar`` slots which are isolated per
        # ``asyncio.Task``, so concurrent requests do not interfere
        # with each other's bindings. The ``merge_contextvars``
        # processor (installed FIRST in the structlog chain by
        # ``src/observability/logging.py``) auto-merges the bound
        # value into every emitted log record without requiring call
        # sites to re-pass it. Satisfies AAP R-26.
        #
        # Why bind here as well as in step 3a:
        # The structlog binding feeds the log emission path. The
        # :data:`_correlation_id_var` set in step 3a feeds non-logging
        # code paths. Maintaining both decouples our non-logging code
        # (httpx hook, Kafka producer, error formatters) from
        # structlog's internal storage layout — which is private to
        # structlog and could change between minor releases.
        # ------------------------------------------------------------------
        structlog.contextvars.bind_contextvars(**{_CORRELATION_LOG_KEY: correlation_id})

        # ------------------------------------------------------------------
        # 3c. Set as OpenTelemetry span attribute (if tracing is
        #     enabled and a recording server span is active).
        #
        # Server-span creation itself is handled by
        # ``opentelemetry-instrumentation-fastapi`` applied via
        # ``src/observability/tracing.instrument_fastapi``. This
        # middleware only ATTACHES the correlation_id attribute so
        # distributed-trace systems can join Elasticsearch logs to
        # traces by the same identifier.
        #
        # ``trace.get_current_span()`` returns the current active
        # span from the OTEL context. When tracing is not configured
        # this is a NoOp span whose ``is_recording()`` returns
        # ``False`` — the guard avoids wasted attribute-set calls.
        # The outer ``_HAS_OTEL`` guard handles the (unlikely)
        # hypothetical test environment where opentelemetry-api is
        # missing entirely.
        # ------------------------------------------------------------------
        if _HAS_OTEL and _otel_trace is not None:
            span = _otel_trace.get_current_span()
            if span is not None and span.is_recording():
                span.set_attribute(_CORRELATION_LOG_KEY, correlation_id)

        # ------------------------------------------------------------------
        # 3d. Stash on ``scope["state"]`` so downstream middleware /
        #     controllers can read it without re-parsing headers.
        #
        # ``scope.setdefault("state", {})`` creates the slot if
        # absent — Starlette normally initializes it for HTTP scopes,
        # but raw-ASGI tests may not. ``setdefault`` is idempotent:
        # if ``state`` already exists it is returned as-is, so we
        # preserve any keys upstream middleware may have placed
        # there. (No upstream middleware exists in our stack since
        # we are the OUTERMOST, but this remains correct behavior if
        # the registration order ever changes.)
        # ------------------------------------------------------------------
        state = scope.setdefault("state", {})
        state[_CORRELATION_SCOPE_KEY] = correlation_id

        # ------------------------------------------------------------------
        # 4-5. Pre-compute the response-header tuple and build the
        #      ``send_with_header`` closure that injects our
        #      ``X-Correlation-ID`` header into the outbound
        #      ``http.response.start`` message.
        #
        # ``encode("ascii")`` is safe here because ``correlation_id``
        # has either passed the whitelist regex (ASCII-only by
        # construction) or was generated from ``uuid.uuid4().hex``
        # (hex chars only, also ASCII). Computing the tuple once
        # outside the closure avoids rebuilding it on every
        # ``http.response.start`` event the closure observes. (In
        # practice each HTTP request sends exactly one
        # ``http.response.start``, so the optimization is symbolic —
        # but it documents the invariant.)
        # ------------------------------------------------------------------
        encoded_value: bytes = correlation_id.encode("ascii")
        header_tuple: tuple[bytes, bytes] = (
            self._header_name_lower,
            encoded_value,
        )

        async def send_with_header(message: Message) -> None:
            """Inject the correlation header into the outbound response.

            Per the ASGI spec, an HTTP response is conveyed as a
            sequence of messages: exactly one ``http.response.start``
            (with ``status`` and ``headers``) followed by one or more
            ``http.response.body`` messages (carrying body bytes and
            an optional ``more_body`` flag). We mutate ONLY the
            ``http.response.start`` message — body messages pass
            through verbatim so streaming responses retain their
            streaming behavior.

            Args:
                message: An outbound ASGI message dict.
            """
            if message["type"] == "http.response.start":
                # Clone the existing headers list so we don't mutate
                # the inner app's data structure (the inner app may
                # reuse it across requests in some edge cases). The
                # explicit ``list(...)`` copy is cheap (a few tuples)
                # compared to the rest of the request lifecycle.
                headers_list: list[tuple[bytes, bytes]] = list(
                    message.get("headers", [])
                )

                # Strip any existing correlation-ID headers the inner
                # app may have set — our value is AUTHORITATIVE. This
                # handles the (rare but observable) case where a
                # controller manually echoes a request header into
                # the response, which would otherwise produce two
                # ``X-Correlation-ID`` headers in the wire response.
                # RFC 7230 permits duplicates for list-grammar fields
                # but ``X-Correlation-ID`` is scalar; one
                # authoritative value is cleaner.
                headers_list = [
                    h for h in headers_list if h[0].lower() != self._header_name_lower
                ]

                # Append our pre-computed header tuple. Append (not
                # prepend) keeps the rest of the headers in the
                # order the inner app emitted them — preserving any
                # ordering invariants downstream middleware or
                # observers may depend on (e.g., security headers
                # set early).
                headers_list.append(header_tuple)

                # Build a new message dict with the patched headers
                # rather than mutating ``message`` in place. The
                # ``{**message, "headers": headers_list}`` idiom
                # creates a shallow copy — the inner app's references
                # stay untouched — and is the canonical ASGI-message-
                # mutation pattern used throughout Starlette / FastAPI
                # internals.
                message = {**message, "headers": headers_list}

            # Forward the (possibly-patched) message to the underlying
            # send. The await yields control to the ASGI server which
            # writes bytes onto the wire and resumes our coroutine
            # when ready.
            await send(message)

        # ------------------------------------------------------------------
        # 6. Invoke the inner app inside a ``try / finally`` block.
        #
        # The ``finally:`` block runs on EVERY exit path — successful
        # response, observed 4xx/5xx, raised exception, even
        # ``CancelledError`` from a client disconnect. This guarantees
        # the bound ``correlation_id`` is always cleared so the next
        # request seen by this task starts with a clean context.
        # (Without this, a long-running asyncio task pool would
        # accumulate stale-ID bleed-through.)
        #
        # We deliberately do NOT catch the exception here — that is
        # the responsibility of ``ErrorHandlerMiddleware`` (innermost).
        # Any exception propagates up and out, while ``finally`` still
        # cleans the bound context vars (both the structlog binding
        # and the :data:`_correlation_id_var` ContextVar).
        # ------------------------------------------------------------------
        try:
            await self._app(scope, receive, send_with_header)
        finally:
            # ----- Cleanup step A ----------------------------------------
            # Reset the :data:`_correlation_id_var` ContextVar to the
            # value it held before ``__call__`` ran. For a request
            # entering at the OUTERMOST middleware that is the
            # ``default=None`` slot, but using the ``reset(token)``
            # idiom (rather than ``set(None)``) future-proofs the
            # cleanup against the (currently impossible) case of
            # nested ``set`` calls and matches the canonical
            # ContextVar usage pattern documented in PEP 567.
            _correlation_id_var.reset(cv_token)

            # ----- Cleanup step B ----------------------------------------
            # Unbind ONLY the ``correlation_id`` key from structlog's
            # contextvars store, NOT the entire bound context. The
            # startup hook in ``src/main.py`` binds ``service``,
            # ``version``, and ``environment`` once per process via
            # ``bind_contextvars``; calling ``clear_contextvars()``
            # here would also drop those keys, leaving subsequent
            # log lines missing the ``service`` field — a direct
            # AAP R-26 violation because every log line is required
            # to carry ``service``.
            structlog.contextvars.unbind_contextvars(_CORRELATION_LOG_KEY)
