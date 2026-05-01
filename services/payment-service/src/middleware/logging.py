"""Structured per-request logging middleware for the Payment Service.

Pure-ASGI middleware that emits exactly one structured JSON log line
per HTTP request, populating the AAP R-26 required field set
(timestamp, level, service, correlation_id, user_id, route, method,
status, latency_ms, message) with PCI-DSS-flavored redaction layered
on the request/response metadata.

This middleware is registered SECOND-OUTERMOST by ``main.create_app``
so the runtime order is::

    CorrelationId → StructuredLogging → JWTAuth → ErrorHandler → route

Behavioral contract
-------------------
* Captures ``method``, ``route``, ``status``, ``latency_ms``,
  ``user_id``, ``request_size_bytes``, and ``response_size_bytes`` and
  emits them in a single log call at the end of every HTTP request.
* Reads ``correlation_id`` IMPLICITLY via
  :func:`structlog.contextvars.merge_contextvars` (bound by
  :class:`CorrelationIdMiddleware` per AAP R-13). Never passes it
  explicitly — that would create a duplicate-key conflict in the
  emitted JSON record.
* Reads ``user_id`` from ``scope["state"]["user"]["sub"]`` if
  :class:`JWTAuthMiddleware` authenticated the caller; otherwise logs
  the canonical ``"anonymous"`` sentinel.
* Selects log level by status code: 5xx → ``error``, 4xx → ``warning``,
  else ``info``. Unhandled exceptions ALWAYS escalate to ``error`` and
  re-tag the event as ``request.failed`` for operator-friendly Kibana
  filtering.
* NEVER logs the request body, the response body, the ``Authorization``
  header, the webhook signature headers (``Stripe-Signature``,
  ``X-Razorpay-Signature``), or any cookie headers (PCI-DSS adjacency
  + AAP R-25 defense in depth). The :meth:`_redact_headers` helper is
  available for future debug-mode features that may need to log a
  small bounded subset of headers; it MUST be used wherever any header
  value crosses the log boundary.
* Latency is measured with :func:`time.monotonic_ns` (NOT
  :func:`time.time`) to immunize the measurement against wall-clock
  adjustments (NTP slew, DST transitions, container clock drift).

Pure-ASGI rationale
-------------------
This middleware deliberately does NOT inherit from
``starlette.middleware.base.BaseHTTPMiddleware``. ``BaseHTTPMiddleware``
buffers the entire request/response body into a ``Message`` queue to
pass through Starlette's ``dispatch()`` helper; that buffering would
break the webhook security model in the Payment Service — the
``src/controllers/webhook_*.py`` controllers compute HMAC over the
unbuffered request body bytes for Stripe / Razorpay signature
verification (AAP R-12). Buffering would also incur unnecessary
overhead on hot-path probes (``/health/live``, ``/health/ready``,
``/metrics``).

PCI-DSS posture
---------------
The Payment Service handles payment metadata adjacent to PCI-DSS-
regulated data (the Stripe / Razorpay tokenization at the client side
keeps raw PANs OUT of this service entirely — see
``services/payment-service/README.md``). This middleware enforces the
**second line of defense** against accidental PAN leakage:

* The default log payload contains ONLY metadata (method, route,
  status, latency, sizes, user_id, exception_type) — none of which
  can plausibly carry a PAN.
* The :data:`_PAN_PATTERN` regex is exposed as a module-level
  constant so the ``PanRedactionFilter`` configured in
  ``src/config/logging_config.py`` can apply the SAME canonical
  pattern as a structlog processor, scanning every dict value across
  the entire process. The pattern catches 13–19 contiguous digits
  with optional single-character spaces or dashes (the canonical
  credit-card numbering range per ISO/IEC 7812-1).
* The :data:`_REDACTED_HEADER_NAMES` allowlist enumerates every
  header name whose VALUE must be replaced with ``"***REDACTED***"``
  before any log emission. Using a frozenset of lowercase names
  keeps the lookup O(1) and immutable across the process lifetime.
"""

# ``from __future__ import annotations`` enables PEP 563 postponed
# evaluation of annotations — type hints are stored as strings rather
# than evaluated at runtime. This permits modern PEP 604 union syntax
# (``int | None``, ``BaseException | None``) and forward-referenced
# generic aliases (``re.Pattern[str]``, ``list[tuple[bytes, bytes]]``)
# on every supported Python version, and keeps the import-time cost of
# this module to a minimum (annotations would otherwise force eager
# evaluation of every type referenced).
from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``re`` compiles the :data:`_PAN_PATTERN` constant — the credit-card
# PAN regex used by :meth:`StructuredLoggingMiddleware._redact_headers`
# for PCI-DSS defense-in-depth (AAP R-25). The same pattern is exposed
# to :mod:`src.config.logging_config` so the structlog
# ``PanRedactionFilter`` processor can apply it uniformly.
import re

# ``time.monotonic_ns()`` is a nanosecond-resolution monotonic clock
# used to measure HTTP request handling latency. Capturing
# ``start_ns = time.monotonic_ns()`` at request entry and computing
# ``(time.monotonic_ns() - start_ns) / 1_000_000`` in the ``finally:``
# block produces the AAP R-26 ``latency_ms`` field with sub-millisecond
# precision. The monotonic clock immunizes the measurement against NTP
# slew, DST transitions, and container clock drift — all of which can
# produce NEGATIVE deltas with ``time.time()`` and poison SLO
# percentile aggregations downstream.
import time

# ``typing.Any`` annotates two heterogeneous shapes:
#   1. The :meth:`_emit_log` ``payload`` dict, whose values mix
#      ``str`` (method, route, user_id, exception_type), ``int``
#      (status, sizes), and ``float`` (latency_ms).
#   2. The return type of :meth:`_select_level`, which yields a
#      structlog bound-logger METHOD reference (``_LOGGER.info``,
#      ``_LOGGER.warning``, or ``_LOGGER.error``). Importing
#      structlog's ``BoundLogger`` type would pull in structlog's full
#      type stack; ``Any`` keeps the file ``mypy --strict`` clean while
#      preserving runtime correctness.
from typing import Any

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``starlette.types`` exposes the canonical ASGI type aliases. We
# import them directly rather than declaring local aliases so this
# middleware's signature is SOURCE-COMPATIBLE with the rest of the
# Starlette/FastAPI middleware ecosystem (``app.add_middleware``,
# ``Middleware``):
#   * ``ASGIApp`` — the inner application reference (``self._app``).
#   * ``Scope``   — ASGI request scope dict; we read ``scope["type"]``,
#                   ``scope["method"]``, ``scope["headers"]``,
#                   ``scope["path"]``, ``scope["route"]``,
#                   ``scope["state"]`` for user_id resolution and the
#                   correlation_id auto-merge contract.
#   * ``Receive`` — async callable yielding incoming ``Message``
#                   events; wrapped via :func:`receive_tracking` to
#                   accumulate request body bytes when the
#                   ``Content-Length`` header is absent.
#   * ``Send``    — async callable accepting outgoing ``Message``
#                   events; wrapped via :func:`send_tracking` to
#                   capture status code and response body bytes.
#   * ``Message`` — dict shape observed by the wrappers
#                   (``http.request`` for body bytes accumulation,
#                   ``http.response.start`` for status capture, and
#                   ``http.response.body`` for byte counting).
#
# We deliberately do NOT import
# ``starlette.middleware.base.BaseHTTPMiddleware``: that base class
# buffers the request body into a ``Message`` queue, which would
# break the Payment Service's webhook HMAC verification path (AAP
# R-12) and inflate latency on hot-path probes.
import structlog
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
# ``__all__`` declares the SOLE public symbol exported by this module.
# The class is named :class:`StructuredLoggingMiddleware` (NOT
# ``LoggingMiddleware`` — the consumer contract from
# ``services/payment-service/src/main.py`` imports
# ``from src.middleware.logging import StructuredLoggingMiddleware``,
# so renaming this symbol — even subtly — would break service startup).
# Module-level constants are deliberately PRIVATE (single-underscore
# prefix) and excluded from ``__all__`` to discourage external
# coupling on sentinel values.
__all__: list[str] = ["StructuredLoggingMiddleware"]


# ---------------------------------------------------------------------------
# Module-level logger (cached at import — idempotent)
# ---------------------------------------------------------------------------
# ``structlog.get_logger`` returns a lazy proxy that defers actual
# binding until the first call site. Capturing it once at module
# import keeps per-request overhead at a single attribute lookup
# rather than re-resolving the proxy on every request — and ASGI
# middleware is invoked on EVERY request, so every cycle counts.
#
# IMPORTANT: this is the ONLY module-level "object" in the file. No
# environment reads, metric registrations, DB/Kafka connections, or
# settings access happens at import time — that hygiene rule keeps
# the file safely importable from unit tests, ``mypy``, and the
# top-level ``src/__init__.py`` package without triggering side
# effects.
_LOGGER = structlog.get_logger("payment_service.middleware.logging")


# ---------------------------------------------------------------------------
# Module-level constants (private — sentinels and security policy)
# ---------------------------------------------------------------------------
#: Sentinel value emitted as ``user_id`` when the request was not
#: authenticated by :class:`JWTAuthMiddleware` (public routes:
#: ``/health/live``, ``/health/ready``, ``/metrics``,
#: ``/webhooks/stripe``, ``/webhooks/razorpay``). ``"anonymous"`` is
#: the canonical RFC 4519 / RFC 8141 anonymous-subject sentinel —
#: widely understood by Kibana operators and amenable to filtering
#: (``user_id != "anonymous"`` selects authenticated traffic).
_UNKNOWN_USER_ID: str = "anonymous"

#: Sentinel value emitted as ``route`` when the URL path cannot be
#: determined from the ASGI scope (extremely rare — the ASGI spec
#: guarantees ``scope["path"]`` is set for HTTP scopes, but
#: defensive sentinels keep downstream Kibana parsers from breaking
#: on missing fields). The literal ``"unknown"`` is preferred over
#: ``""`` because empty strings can be silently dropped by some log
#: shippers / parsers.
_UNKNOWN_ROUTE: str = "unknown"

#: Sentinel value emitted as ``status`` when the inner app crashed
#: BEFORE emitting an ``http.response.start`` ASGI message (i.e.,
#: the response status was never set). ``0`` is unambiguous as a
#: "not observed" marker because real HTTP statuses are 100–599;
#: integer typing is preserved so Elasticsearch indexes the field
#: as a long (mixing strings and integers would force the mapping
#: to a less-efficient text type).
_UNKNOWN_STATUS: int = 0

#: Event name emitted on the success path (and on any path where the
#: inner middleware chain returned cleanly, including 4xx/5xx
#: responses produced by :class:`ErrorHandlerMiddleware`). The
#: ``EventRenamer(to="message")`` processor configured in
#: ``src/config/logging_config.py`` renames the structlog ``event``
#: key to ``message`` before JSON rendering, so this string ends up
#: in the AAP R-26 ``message`` field of the final log record.
_REQUEST_COMPLETED_EVENT: str = "request.completed"

#: Event name emitted when the inner middleware chain raised an
#: unhandled exception that escaped past :class:`ErrorHandlerMiddleware`.
#: Operators searching Kibana for ``message:"request.failed"`` find
#: bugs that bypassed the normal error-response path.
_REQUEST_FAILED_EVENT: str = "request.failed"

#: PAN regex: 13–19 contiguous digits with optional single-character
#: spaces or dashes between them. Anchored to word boundaries
#: (``\b``) to avoid matching long tracking IDs that happen to
#: contain digit runs. The bounds (``{12,18}`` plus the trailing
#: ``\d``) yield 13–19 total digits — the canonical credit-card
#: numbering range per ISO/IEC 7812-1.
#:
#: NOTE: this is BELT-AND-SUSPENDERS — providers (Stripe Elements,
#: Razorpay Checkout) tokenize at the client so raw PANs should
#: NEVER reach this service. This regex catches accidental leakage
#: in headers or future-extension log fields; the structlog
#: ``PanRedactionFilter`` processor reuses this exact pattern.
_PAN_PATTERN: re.Pattern[str] = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")

#: Replacement string substituted for any matched PAN run. Chosen as
#: a literal, non-numeric string so a downstream operator immediately
#: recognizes the redaction; Kibana queries can filter
#: ``*"***REDACTED-PAN***"*`` to find log lines that crossed the
#: redaction boundary.
_PAN_REDACTION: str = "***REDACTED-PAN***"

#: Header names whose VALUE must NEVER appear in any log line
#: emitted by the Payment Service. Stored as a frozenset of
#: lowercase strings (ASGI headers are lowercase per RFC 7230) for
#: O(1) membership lookup and immutability.
#:
#: Rationale per entry:
#:   * ``authorization`` — Bearer tokens (AAP R-21) leak session
#:     identity if logged.
#:   * ``proxy-authorization`` — same risk via a different header
#:     RFC 7235 §4.4.
#:   * ``cookie`` — session cookies enable account takeover.
#:   * ``set-cookie`` — outbound session cookies on response.
#:   * ``stripe-signature`` — leaks HMAC values, enabling replay
#:     against Stripe's webhook validator if combined with the
#:     event body (AAP R-12).
#:   * ``x-razorpay-signature`` — equivalent risk for Razorpay.
#:   * ``x-api-key`` — generic API-key shorthand used by some
#:     internal tooling (rate-limit bypass risk if leaked).
#:   * ``x-auth-token`` — alternative auth header used by some
#:     legacy clients; same risk as ``authorization``.
_REDACTED_HEADER_NAMES: frozenset[str] = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "stripe-signature",
        "x-razorpay-signature",
        "x-api-key",
        "x-auth-token",
    }
)

#: Substring/suffix hints (lowercase) that mark a field name as
#: carrying secret material. Defined for cross-module use by the
#: structlog ``PanRedactionFilter`` processor; this middleware's
#: own log payload contains only metadata fields, none of which match
#: these hints, so the constant is currently unused inside
#: :class:`StructuredLoggingMiddleware`. It is preserved as a single
#: source of truth for "what looks like a secret" so future
#: extensions stay consistent with the structlog filter.
_SECRET_KEY_HINTS: frozenset[str] = frozenset(
    {
        "_secret",
        "_token",
        "_key",
        "password",
        "api_key",
        "webhook_secret",
    }
)

#: Replacement string for redacted header values and for fields
#: whose name matches :data:`_SECRET_KEY_HINTS`. Distinct from
#: :data:`_PAN_REDACTION` so log analysis can distinguish between
#: "this was a known-secret header" and "this was a PAN-pattern
#: substring inside an otherwise free-form value".
_SECRET_REDACTION: str = "***REDACTED***"


# ---------------------------------------------------------------------------
# Middleware class
# ---------------------------------------------------------------------------
class StructuredLoggingMiddleware:
    """Pure-ASGI middleware emitting one structured log line per request.

    The log line is produced in the ``finally:`` block of
    :meth:`__call__` so it is emitted whether the inner middleware
    chain succeeded, returned an error response, or crashed with an
    unhandled exception. The emitted record carries the AAP R-26
    minimum field set::

        timestamp | level | service | correlation_id | user_id |
        route | method | status | latency_ms | message

    plus optional request/response size metrics
    (``request_size_bytes``, ``response_size_bytes``) and
    ``exception_type`` on the failure path. Many AAP R-26 fields are
    NOT explicitly passed by this middleware — they are auto-merged
    into every log record by the structlog processor chain configured
    in ``src/config/logging_config.py``:

    +-------------------+-------------------------------------------+
    | Field             | Source                                    |
    +===================+===========================================+
    | ``timestamp``     | ``structlog.processors.TimeStamper``      |
    |                   | (ISO-8601 UTC, RFC 3339 compatible)       |
    +-------------------+-------------------------------------------+
    | ``level``         | ``structlog.processors.add_log_level``    |
    +-------------------+-------------------------------------------+
    | ``message``       | First positional arg of the                |
    |                   | ``logger.<level>`` call                    |
    |                   | (``"request.completed"`` /                 |
    |                   | ``"request.failed"``) renamed by           |
    |                   | ``EventRenamer(to="message")``             |
    +-------------------+-------------------------------------------+
    | ``service``       | Bound at startup by ``main.lifespan``     |
    |                   | via ``bind_contextvars(service=...)``     |
    +-------------------+-------------------------------------------+
    | ``correlation_id``| Bound per-request by                      |
    |                   | :class:`CorrelationIdMiddleware`          |
    +-------------------+-------------------------------------------+

    Args:
        app: The inner ASGI application (next middleware in the
            chain, or ultimately the FastAPI router). Stored as
            ``self._app`` and invoked unchanged for non-HTTP scopes;
            for HTTP scopes ``receive`` and ``send`` are wrapped via
            inner async closures to capture request body bytes,
            response status, and response body bytes without
            buffering or mutating the message stream.

    Example:
        >>> from fastapi import FastAPI
        >>> from src.middleware.logging import StructuredLoggingMiddleware
        >>> app = FastAPI()
        >>> # Register SECOND-TO-LAST so this middleware runs
        >>> # SECOND-OUTERMOST at runtime (CorrelationIdMiddleware
        >>> # is added LAST so it is OUTERMOST and binds the
        >>> # correlation_id BEFORE this middleware emits its log).
        >>> app.add_middleware(StructuredLoggingMiddleware)
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def __init__(self, app: ASGIApp) -> None:
        """Initialize the middleware.

        Args:
            app: The inner ASGI application to wrap. This is the next
                middleware in the chain (added BEFORE this one via
                :meth:`fastapi.FastAPI.add_middleware`) or the
                FastAPI / Starlette router itself if this is the
                innermost middleware. Stored verbatim as
                ``self._app``; we never modify or re-wrap it at
                construction time. The wrapping happens per-request
                in :meth:`__call__` via ``send_tracking`` and
                ``receive_tracking`` closures.
        """
        # Hold the inner app reference. Storing it once here keeps the
        # per-request overhead at a single attribute lookup
        # (``self._app``) rather than re-resolving the app from a
        # registry on every invocation. ASGI middleware is invoked on
        # EVERY request so every cycle counts; this micro-optimization
        # is also the conventional ASGI 3.0 pattern.
        self._app: ASGIApp = app

    # ------------------------------------------------------------------
    # ASGI entry point
    # ------------------------------------------------------------------
    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        """ASGI entry point — invoked by the ASGI server on each event.

        Dispatch:

        * Non-HTTP scopes (``lifespan``, ``websocket``) — pass through
          to the inner app untouched. Per-request logging does not
          apply: lifespan events are once-per-process and handled by
          ``main.lifespan``; websockets are not in scope for the
          Payment Service's HTTP-only public surface.
        * HTTP scopes — capture status / size metrics by observing
          the inbound and outbound message streams, then emit a
          single structured log line in the ``finally:`` block whether
          the inner chain succeeded, returned an error response, or
          raised an unhandled exception.

        The ``finally:`` block is the heart of the
        exactly-once-log-per-request guarantee: Python's
        ``try / finally`` semantics ensure the block runs on EVERY
        exit path including normal coroutine return,
        :class:`asyncio.CancelledError` from a client disconnect,
        ``SystemExit`` / ``KeyboardInterrupt`` (caught implicitly by
        ``except BaseException``), and any other exception subclass.
        Without this guarantee, error rates computed in Kibana would
        be skewed because failed requests would silently produce no
        log line.

        Args:
            scope: The ASGI scope dict.
            receive: The ASGI receive callable; wrapped with
                ``receive_tracking`` for HTTP scopes so this
                middleware can accumulate request body byte counts as
                a fallback when ``Content-Length`` is absent (e.g.,
                chunked transfer encoding).
            send: The ASGI send callable; wrapped with
                ``send_tracking`` for HTTP scopes so this middleware
                can capture the outbound status code from
                ``http.response.start`` and accumulate response body
                bytes from ``http.response.body``.
        """
        # Non-HTTP scopes (``lifespan``, ``websocket``) are forwarded
        # verbatim. The ASGI spec defines three scope types; lifespan
        # messages arrive once per process and are handled by
        # ``main.lifespan``, websockets are not in scope for the
        # Payment Service's HTTP-only public surface. Neither maps
        # onto the per-request logging contract this middleware
        # implements.
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        # Capture the start timestamp using ``time.monotonic_ns()``.
        # The monotonic clock cannot go backward; using ``time.time()``
        # would expose latency calculations to NTP slew, DST changes,
        # and manual clock adjustments — any of which can produce
        # negative deltas that poison SLO percentile aggregations.
        start_ns = time.monotonic_ns()

        # Capture the HTTP method once at request entry. The ASGI spec
        # guarantees ``scope["method"]`` is set for HTTP scopes, but
        # the empty-string fallback protects against malformed test
        # scopes (we still emit a log line even if method is missing).
        method: str = scope.get("method", "")

        # Read the ``Content-Length`` header up front as the primary
        # source for ``request_size_bytes``. Most production clients
        # set this header for non-streaming requests; for chunked
        # transfer encoding it is absent and we fall back to summing
        # the body bytes observed via ``receive_tracking``.
        request_size_bytes: int | None = self._extract_content_length(scope)

        # Counter for body bytes seen on ``http.request`` messages.
        # Used as the fallback when ``Content-Length`` is missing.
        request_body_seen: int = 0

        async def receive_tracking() -> Message:
            """Wrap ``receive`` to count incoming request body bytes.

            Forwards the message UNCHANGED to the caller; this
            wrapper does NOT buffer, mutate, or replay the body —
            doing so would break the Payment Service's webhook HMAC
            verification (AAP R-12) and inflate memory on large
            uploads.
            """
            # ``nonlocal`` is required so we mutate the enclosed
            # ``request_body_seen`` counter rather than shadowing it
            # with a new local variable (the canonical Python
            # closure-scoping bug).
            nonlocal request_body_seen
            message = await receive()
            if message["type"] == "http.request":
                # ``message.get("body") or b""`` guards against
                # messages that omit the field (rare; the ASGI spec
                # says ``body`` is required) and against ``None``
                # values (some test stubs).
                body = message.get("body") or b""
                # ``len()`` on bytes is O(1) — we never iterate over
                # body content, only count it.
                request_body_seen += len(body)
            return message

        # Status code captured from ``http.response.start``. Defaults
        # to the ``_UNKNOWN_STATUS`` sentinel; remains 0 if the inner
        # app crashed before emitting any response.
        status_code: int = _UNKNOWN_STATUS

        # Accumulator for response body bytes observed across one or
        # more ``http.response.body`` messages. Sum of byte counts is
        # accurate for both Content-Length and chunked encodings.
        response_size_bytes: int = 0

        # Flag indicating whether ``http.response.start`` was ever
        # observed. Used to distinguish "crash before any response
        # bytes" from "crash mid-response". Currently informational —
        # status_code remains 0 in either case — but the flag is
        # available for future log-line discrimination.
        response_started: bool = False

        async def send_tracking(message: Message) -> None:
            """Wrap ``send`` to capture status and response body bytes.

            Forwards each message UNCHANGED to the caller; this
            wrapper does NOT modify response status, headers, or body
            bytes. The ASGI message stream is preserved bit-for-bit
            so streaming responses (and Stripe / Razorpay webhook
            redirects) continue to function as if this middleware
            were not present.
            """
            nonlocal status_code, response_size_bytes, response_started
            if message["type"] == "http.response.start":
                # ``http.response.start`` arrives once per response
                # carrying the status code and headers list. We
                # capture only the integer status; response headers
                # may carry sensitive values (``Set-Cookie``,
                # ``Location`` with query-string tokens) and are
                # deliberately not inspected.
                status_code = int(message.get("status") or _UNKNOWN_STATUS)
                response_started = True
            elif message["type"] == "http.response.body":
                # ``http.response.body`` may arrive 1+ times per
                # response. Summing ``len(body)`` is correct for
                # both Content-Length and chunked encodings.
                body = message.get("body") or b""
                response_size_bytes += len(body)
            # ALWAYS forward the message — even types we do not
            # currently observe — so future ASGI spec extensions
            # continue to work transparently.
            await send(message)

        # Captured-exception pattern: ``exception`` records any
        # exception caught from the inner app for use during
        # log-level selection in the ``finally:`` block. We then
        # re-raise so the caller (Starlette's protocol handler, or
        # :class:`ErrorHandlerMiddleware` if it is INSIDE us) can
        # handle the exception appropriately. Without the re-raise,
        # callers would receive empty responses and the request
        # would silently fail.
        exception: BaseException | None = None
        try:
            await self._app(scope, receive_tracking, send_tracking)
        except BaseException as exc:  # noqa: BLE001 — re-raised after log
            # Catch ``BaseException`` (not just ``Exception``) so we
            # also observe ``SystemExit`` / ``KeyboardInterrupt`` /
            # ``GeneratorExit`` — none of which inherit from
            # ``Exception`` but all of which can crash the inner app.
            # We do NOT swallow these; we capture the reference for
            # log-level selection and re-raise immediately below.
            exception = exc
            # Bare ``raise`` re-raises the currently active exception
            # preserving the original traceback. Using ``raise exc``
            # would also work but appends a redundant frame.
            raise
        finally:
            # Compute latency in milliseconds. ``/ 1_000_000`` is
            # integer-safe in Python 3 (always returns float), and
            # ``round(..., 3)`` keeps microsecond precision while
            # avoiding the noisy nanosecond tail that would inflate
            # log-line size for no operational benefit.
            latency_ms = (time.monotonic_ns() - start_ns) / 1_000_000

            # Resolve user_id and route AT LOG TIME (i.e., here in
            # ``finally:``) — both rely on state populated by INNER
            # middleware (JWT auth populates ``state["user"]``;
            # FastAPI router populates ``scope["route"]``) which is
            # only present after ``self._app(...)`` returns.
            user_id = self._resolve_user_id(scope)
            route = self._resolve_route(scope)

            # Prefer Content-Length when available; fallback to the
            # bytes observed by ``receive_tracking``. Both branches
            # produce a non-negative ``int`` for type stability.
            req_size = (
                request_size_bytes
                if request_size_bytes is not None
                else request_body_seen
            )

            # Emit the per-request log line. The event name flips
            # between ``request.completed`` and ``request.failed``
            # based on whether an exception escaped the inner app
            # — operators can filter by ``message:"request.failed"``
            # to find logic bugs that bypassed
            # :class:`ErrorHandlerMiddleware`.
            self._emit_log(
                event=(
                    _REQUEST_FAILED_EVENT
                    if exception is not None
                    else _REQUEST_COMPLETED_EVENT
                ),
                method=method,
                route=route,
                status=status_code,
                latency_ms=round(latency_ms, 3),
                user_id=user_id,
                request_size_bytes=req_size,
                response_size_bytes=response_size_bytes,
                exception=exception,
            )

    # ------------------------------------------------------------------
    # Helper: read Content-Length from ASGI scope headers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_content_length(scope: Scope) -> int | None:
        """Read ``Content-Length`` from the raw ASGI scope headers.

        ASGI delivers headers as ``list[tuple[bytes, bytes]]`` with
        names normalized to lowercase. We compare against the literal
        ``b"content-length"`` byte string for an exact match. If the
        header is absent or unparseable, returns ``None`` so the
        caller can fall back to the chunk-sum produced by
        ``receive_tracking``.

        Why ``None`` (not ``0``)
        ------------------------
        Returning ``None`` lets the caller distinguish "header
        absent — fall back to body bytes seen" from "header present
        and equal to zero — request had no body". The downstream
        ``request_size_bytes`` field is always coerced to a
        non-negative integer before logging, so ``None`` never
        leaks into the JSON log record.

        Args:
            scope: The ASGI request scope dict.

        Returns:
            The integer value of the ``Content-Length`` header, or
            ``None`` if absent / malformed / non-decodable.
        """
        # ``scope.get("headers") or []`` defensively handles malformed
        # test scopes that omit the headers key entirely. The ``or``
        # also handles the case where ``headers`` is explicitly
        # ``None`` (rare but possible in raw-ASGI test stubs).
        for raw_name, raw_value in scope.get("headers") or []:
            # Direct bytes comparison: the ASGI spec mandates lowercase
            # header names so no ``.lower()`` normalization is needed
            # in the steady state. Test stubs that violate the spec
            # will simply not match — and we will fall back to the
            # ``receive_tracking`` chunk sum, which is the correct
            # behavior under uncertainty.
            if raw_name == b"content-length":
                try:
                    # ``decode("ascii")`` is correct: HTTP header
                    # values are ASCII per RFC 7230 §3.2.4.
                    # ``int(...)`` raises ValueError on non-numeric
                    # input (e.g., ``Content-Length: chunked``) and
                    # we return None so the caller falls back.
                    return int(raw_value.decode("ascii"))
                except (UnicodeDecodeError, ValueError):
                    # Malformed value — treat as absent.
                    return None
        # Header not found — caller falls back to the chunk-sum
        # accumulator.
        return None

    # ------------------------------------------------------------------
    # Helper: resolve user_id from ASGI scope state
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_user_id(scope: Scope) -> str:
        """Extract the JWT ``sub`` claim from the ASGI scope state.

        :class:`JWTAuthMiddleware` (running INSIDE this middleware in
        the runtime stack) populates
        ``scope["state"]["user"] = {"sub": ..., "scope": [...]}`` on
        successful authentication. For public routes
        (``/health/live``, ``/health/ready``, ``/metrics``,
        ``/webhooks/stripe``, ``/webhooks/razorpay``) the JWT
        middleware bypasses validation entirely and never sets the
        state slot — this helper returns ``_UNKNOWN_USER_ID`` in
        that case.

        Defensive type checks at every nesting level prevent a
        downstream type error from crashing the ``finally:`` block
        and suppressing the per-request log line. Logging
        ``user_id="anonymous"`` on a malformed state is strictly
        better than logging nothing.

        Args:
            scope: The ASGI request scope dict.

        Returns:
            The JWT ``sub`` claim if present and non-empty as a
            string, otherwise the constant :data:`_UNKNOWN_USER_ID`
            (``"anonymous"``).
        """
        # ``scope.get("state") or {}`` defensively handles raw-ASGI
        # test stubs that omit ``state``. Starlette / FastAPI always
        # initialize the state dict for HTTP requests, so this branch
        # is exercised primarily by unit tests.
        state = scope.get("state") or {}
        # ``state.get("user")`` reads the JWT claim payload set by
        # :class:`JWTAuthMiddleware` on successful authentication.
        # Public routes that bypass JWT validation never reach the
        # auth middleware, so this slot stays unset.
        user = state.get("user") if isinstance(state, dict) else None
        if isinstance(user, dict):
            # ``user.get("sub")`` reads the standard JWT subject
            # claim per RFC 7519 §4.1.2 — the canonical identity for
            # the authenticated principal (UUID, email, or numeric
            # user ID depending on the Auth Service's identity
            # scheme).
            sub = user.get("sub")
            # Require a non-empty string. An empty ``sub`` would
            # render as ``user_id=""`` which is technically valid
            # JSON but operationally confusing.
            if isinstance(sub, str) and sub:
                return sub
        return _UNKNOWN_USER_ID

    # ------------------------------------------------------------------
    # Helper: resolve route from ASGI scope (template preferred)
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_route(scope: Scope) -> str:
        """Return the request's route, preferring the matched template.

        Why the template (not the raw path)
        -----------------------------------
        Elasticsearch / Kibana aggregate metrics by exact field
        value. If we logged the raw concrete path
        (``/payments/abc-123``), every UUID-bearing payment ID would
        produce a unique ``route`` value PER REQUEST — inflating the
        Elasticsearch field-value cardinality, exhausting mapping
        slots, and making Kibana's "top routes by p95 latency"
        dashboards unusable because every row would be a unique
        path. Logging the TEMPLATE (``/payments/{id}``) aggregates
        all variants of a route into a single dimension, which is
        what the dashboards actually need.

        Fallback chain:
        1. ``scope["route"].path`` (FastAPI / Starlette router
           populates this AFTER matching the request).
        2. ``scope["path"]`` (raw concrete path — used for 404s
           that did not match any route).
        3. :data:`_UNKNOWN_ROUTE` (defensive sentinel for
           malformed scopes).

        Args:
            scope: The ASGI request scope dict.

        Returns:
            The matched route template if available, else the raw
            path, else :data:`_UNKNOWN_ROUTE`.
        """
        # Step 1: prefer the matched route template. FastAPI /
        # Starlette populates ``scope["route"]`` with a ``Route`` or
        # ``APIRoute`` object once the router has matched the request.
        route = scope.get("route")
        # ``getattr(route, "path", None)`` defensively probes — both
        # ``starlette.routing.Route`` and ``fastapi.routing.APIRoute``
        # expose ``.path``, but custom user-defined route classes
        # might not.
        template = getattr(route, "path", None)
        if isinstance(template, str) and template:
            return template
        # Step 2: fall back to the raw concrete path. ``scope["path"]``
        # is guaranteed to be a string for HTTP scopes per the ASGI
        # spec, but ``isinstance`` checks defensively.
        raw_path = scope.get("path")
        if isinstance(raw_path, str) and raw_path:
            return raw_path
        # Step 3: ultimate fallback. Should be unreachable for
        # well-formed HTTP scopes; included so the log line is valid
        # even for malformed scopes.
        return _UNKNOWN_ROUTE

    # ------------------------------------------------------------------
    # Helper: emit the per-request log line
    # ------------------------------------------------------------------
    def _emit_log(
        self,
        *,
        event: str,
        method: str,
        route: str,
        status: int,
        latency_ms: float,
        user_id: str,
        request_size_bytes: int,
        response_size_bytes: int,
        exception: BaseException | None,
    ) -> None:
        """Emit the per-request log line at the level chosen by status.

        NEVER passes ``correlation_id`` explicitly — structlog's
        ``merge_contextvars`` processor (installed FIRST in the chain
        by ``src/config/logging_config.py``) auto-merges the value
        bound by :class:`CorrelationIdMiddleware` (AAP R-13) into
        every emitted record. Passing it explicitly would create a
        duplicate-key conflict that structlog would either raise on
        or silently drop — neither outcome is desirable.

        Likewise, ``service`` / ``version`` / ``environment`` are NOT
        passed explicitly: the application lifespan startup hook
        binds them once via
        ``structlog.contextvars.bind_contextvars(...)`` and they
        flow through ``merge_contextvars`` for the lifetime of the
        process.

        The payload contains ONLY metadata (method, route, status,
        latency, sizes, user_id, exception_type) — none of which
        plausibly carries PAN or secret material. The
        :data:`_PAN_PATTERN` and :data:`_REDACTED_HEADER_NAMES`
        constants are exposed for cross-module use by the
        ``PanRedactionFilter`` structlog processor; this middleware
        does not need to apply them to its own payload because no
        free-form value reaches the log call here.

        Args:
            event: The structlog event name that becomes the
                ``message`` field after ``EventRenamer``.
            method: HTTP method verb.
            route: Resolved route template or raw path.
            status: HTTP status code (``0`` if response never
                started).
            latency_ms: Request handling latency in milliseconds,
                pre-rounded to 3 decimal places.
            user_id: JWT ``sub`` or ``"anonymous"``.
            request_size_bytes: ``Content-Length`` or chunk-sum
                fallback.
            response_size_bytes: Sum of ``http.response.body`` byte
                counts.
            exception: The captured exception if the inner chain
                raised, else ``None``.
        """
        # Resolve the structlog method (info / warning / error) by
        # status code and exception presence.
        log_method = self._select_level(status, exception)
        # Build the payload as a plain dict so structlog renders it
        # as a flat JSON object after the processor chain. ``int(...)``
        # coerces defensively — values are already int by
        # construction but the explicit cast documents the wire-shape
        # contract for downstream Elasticsearch mapping.
        payload: dict[str, Any] = {
            "method": method,
            "route": route,
            "status": int(status),
            "latency_ms": latency_ms,
            "user_id": user_id,
            "request_size_bytes": int(request_size_bytes or 0),
            "response_size_bytes": int(response_size_bytes or 0),
        }
        if exception is not None:
            # ``type(exception).__name__`` yields the bare class name
            # (e.g., ``"RuntimeError"``, ``"ValueError"``,
            # ``"PaymentProviderError"``). We do NOT log the exception
            # message or traceback here — those may contain PII or
            # provider tokens; they belong on the
            # :class:`ErrorHandlerMiddleware` log line which has
            # access to the full context for safe redaction.
            payload["exception_type"] = type(exception).__name__
        # Final dispatch. ``log_method(event, **payload)`` is the
        # canonical structlog API: the first positional arg becomes
        # the event/message; keyword args become flat JSON keys.
        log_method(event, **payload)

    # ------------------------------------------------------------------
    # Helper: select log level from status code and exception state
    # ------------------------------------------------------------------
    @staticmethod
    def _select_level(status: int, exception: BaseException | None) -> Any:
        """Choose the structlog log method based on outcome.

        Mapping:

        * Caught exception (regardless of status) → ``error``.
        * 5xx response → ``error``.
        * 4xx response → ``warning``.
        * Otherwise (1xx / 2xx / 3xx / unobserved) → ``info``.

        Returns the BOUND logger method itself (not a level name) so
        the caller can invoke it directly with
        ``log_method(event, **payload)``. This trades a tiny bit of
        type opacity (``Any`` instead of a precise ``Callable``) for
        simpler call sites and one fewer ``getattr`` dispatch per
        log emission.

        Args:
            status: HTTP status code.
            exception: The captured exception if the inner chain
                raised, else ``None``.

        Returns:
            One of ``_LOGGER.info`` / ``_LOGGER.warning`` /
            ``_LOGGER.error`` (typed as :class:`Any` because
            structlog's ``BoundLogger`` is generic over the
            processor chain).
        """
        # Order matters: exceptions ALWAYS escalate to error, even on
        # paths where a partial response was emitted before the crash
        # (a 502/503 from the inner chain plus an exception is still
        # an error). Then 5xx, then 4xx, then info as the default.
        if exception is not None:
            return _LOGGER.error
        if status >= 500:
            return _LOGGER.error
        if status >= 400:
            return _LOGGER.warning
        return _LOGGER.info

    # ------------------------------------------------------------------
    # Helper: redact a list of ASGI headers (defense-in-depth)
    # ------------------------------------------------------------------
    @staticmethod
    def _redact_headers(
        headers: list[tuple[bytes, bytes]],
    ) -> list[tuple[str, str]]:
        """Return a redacted, decoded copy of an ASGI headers list.

        Headers in :data:`_REDACTED_HEADER_NAMES` have their VALUE
        replaced with :data:`_SECRET_REDACTION` (``"***REDACTED***"``).
        Surviving values are then scanned for PAN-pattern substrings
        and substituted with :data:`_PAN_REDACTION`
        (``"***REDACTED-PAN***"``) when matched.

        Usage policy
        ------------
        :class:`StructuredLoggingMiddleware` does NOT log headers in
        the default log payload — the metadata it emits (method,
        route, status, latency_ms, sizes, user_id, exception_type)
        cannot plausibly carry secrets. This helper exists for
        FUTURE extensions (e.g., a one-off operator-enabled
        "log-headers-for-this-trace" debug flag, or a dedicated
        webhook-receipt log line that captures non-sensitive headers
        for auditing). Any such future extension MUST funnel the
        headers through this helper BEFORE they reach the log call.

        Decoding
        --------
        We use ``latin-1`` for the bytes-to-str conversion (NOT
        ``ascii`` and NOT ``utf-8``):

        * RFC 7230 §3.2.4 restricts header values to a subset of
          ASCII, but real-world clients sometimes emit non-ASCII
          bytes in custom headers (``X-Filename`` carrying a
          non-ASCII filename, ``Content-Disposition`` with a
          UTF-8-encoded name).
        * ``latin-1`` (a.k.a. ISO-8859-1) is a bijective 1-byte
          encoding that NEVER fails on any 8-bit input — it maps
          each byte 0-255 to a Unicode code point of the same
          numeric value. This guarantees the helper does not raise
          ``UnicodeDecodeError`` on hostile input.
        * Any non-ASCII bytes that would otherwise confuse Kibana's
          JSON parser are still subject to the PAN regex scan and
          will be redacted if they happen to match — and otherwise
          are passed through as-is (with the same byte values).

        Args:
            headers: ASGI scope/message headers list — a list of
                ``(name_bytes, value_bytes)`` tuples with names
                lowercased per the ASGI spec.

        Returns:
            A new list of ``(name_str, value_str)`` tuples with all
            sensitive values replaced. The input is NOT mutated.
        """
        # Build the result incrementally rather than via a list
        # comprehension because the redaction logic per element is
        # multi-step (decode → check name → maybe redact → scan PAN).
        # An explicit for-loop is clearer here than nesting the
        # branches into a comprehension.
        out: list[tuple[str, str]] = []
        for raw_name, raw_value in headers:
            try:
                # ``latin-1`` is bijective 1-byte → never raises on
                # 8-bit bytes. ``.lower()`` normalizes the name for
                # the redacted-set membership check.
                name = raw_name.decode("latin-1").lower()
                value = raw_value.decode("latin-1")
            except (UnicodeDecodeError, AttributeError):
                # ``latin-1`` cannot raise UnicodeDecodeError on
                # bytes input, but we keep the except clause for
                # defense against test stubs that pass non-bytes
                # types (where ``.decode`` would raise
                # AttributeError). Skip the malformed entry rather
                # than emit garbage.
                continue
            if name in _REDACTED_HEADER_NAMES:
                # The header is on the redaction allowlist — replace
                # the value entirely. We DO NOT preserve any prefix
                # / suffix because the entire value is considered
                # secret (e.g., the entire Bearer token in
                # ``Authorization``, the entire HMAC in
                # ``Stripe-Signature``).
                out.append((name, _SECRET_REDACTION))
                continue
            # Not on the allowlist — scan the value for PAN-pattern
            # substrings and substitute matches. ``re.sub`` returns
            # the original string unchanged when there is no match,
            # so this is safe and cheap on the common case.
            if _PAN_PATTERN.search(value):
                value = _PAN_PATTERN.sub(_PAN_REDACTION, value)
            out.append((name, value))
        return out
