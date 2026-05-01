"""Pure-ASGI structured-logging middleware for the Notification Service.

This module implements :class:`StructuredLoggingMiddleware` — the
**second-outermost** middleware in the Notification Service's middleware
stack. It runs immediately AFTER :class:`CorrelationIdMiddleware` (so
``correlation_id`` is already bound into structlog's contextvars when
this middleware emits a log line) and immediately BEFORE
:class:`JWTAuthMiddleware` (so we capture log lines for requests that
were rejected by JWT validation, too — auth failures show up as
``status=401`` log lines just like any other 4xx response).

Authoritative requirements implemented here
-------------------------------------------
* **AAP R-26** — All logs must be structured JSON with the required
  fields ``timestamp`` (RFC 3339), ``level``, ``service``,
  ``correlation_id``, ``user_id`` (when known), ``route``, ``method``,
  ``status``, ``latency_ms``, ``message``. This middleware emits
  exactly one such log line per HTTP request, populating every
  required field for which it has authoritative state. The remaining
  required fields are auto-merged into every log record by
  ``structlog.contextvars.merge_contextvars`` (the FIRST processor
  installed by ``src/config/logging_config.py``):

  * ``timestamp`` — added by ``structlog.processors.TimeStamper``
    (configured with ``fmt="iso", utc=True`` for RFC 3339).
  * ``level``     — populated by ``structlog.processors.add_log_level``.
  * ``message``   — the first positional arg passed to ``logger.info``
    / ``logger.warning`` / ``logger.error`` becomes the ``event`` /
    ``message`` field after ``EventRenamer``.
  * ``service``   — bound at process startup by ``main.lifespan`` via
    ``structlog.contextvars.bind_contextvars(service=...)``.
  * ``correlation_id`` — bound per-request by
    :class:`CorrelationIdMiddleware`.
  * ``user_id``   — populated by this middleware in ``finally:`` from
    ``scope["state"]["user"]["sub"]`` if :class:`JWTAuthMiddleware`
    successfully validated a JWT, otherwise the sentinel
    ``"anonymous"``.
  * ``route``, ``method``, ``status``, ``latency_ms`` — populated
    explicitly per-request by this middleware.

* **AAP R-27** — Services ship logs via Filebeat / Metricbeat to
  Logstash, which forwards them to Elasticsearch. This middleware's
  output is the conveyor belt: structured JSON written to stdout that
  Filebeat tails and ships unmodified.

* **AAP Section 0.4.5** — Logging middleware is enumerated as one of
  the first-class cross-cutting interceptors that must produce one
  log line per request (route, method, latency, status, user_id,
  correlation_id) without leaking sensitive material from headers or
  bodies.

Design philosophy
-----------------
* **Pure ASGI, NOT** ``starlette.middleware.base.BaseHTTPMiddleware``.
  ``BaseHTTPMiddleware`` buffers the entire response body into a
  ``Message`` queue to pass it through Starlette's ``dispatch()``
  helper; that buffering is observable overhead on hot-path routes
  (``/health/live``, ``/health/ready``, ``/metrics``) and on any
  notification-status streaming responses. Pure ASGI observes
  individual ``http.response.start`` and ``http.response.body``
  events and forwards them through ``send()`` immediately,
  preserving streaming behavior and skipping the buffer. The folder
  spec for ``services/notification-service/src/middleware/`` calls
  out this choice as non-negotiable for the two hot-path middlewares
  (this one and ``correlation_id.py``).

* **Exactly one log line per request** — emitted in the ``finally:``
  block so the line is produced whether the inner chain succeeded,
  returned a 4xx / 5xx response, or raised an unhandled exception.
  Dual logs (start + end) double the log volume with no operational
  benefit when ``latency_ms`` already encodes the duration; we
  deliberately AVOID the dual-log pattern that some frameworks
  default to.

* **Privacy by construction** — request bodies, response bodies, and
  individual headers (especially ``Authorization``, ``Cookie``, and
  ``Set-Cookie``) are NEVER captured into the log record. The
  middleware reads only the ``Content-Length`` request header (a
  scalar integer carrying no PII) and observes the byte count of
  outbound ``http.response.body`` messages without buffering them.

* **Level selection drives Kibana queries** — INFO for 1xx / 2xx /
  3xx, WARNING for 4xx, ERROR for 5xx and unhandled exceptions.
  Operators can filter Kibana by ``level=error`` to pull every 5xx
  in a time window without parsing status codes.

* **Monotonic clock for latency** — ``time.monotonic_ns()`` (NOT
  ``time.time()``) guarantees no negative latencies from wall-clock
  adjustments (NTP slew, DST transitions, manual ``date`` changes).
  Negative latencies poison SLO calculations and percentile
  aggregations downstream.

* **Route cardinality discipline** — when the matched FastAPI route
  template is available on ``scope["route"]``, we log the template
  (``/notifications/{id}``) rather than the raw path
  (``/notifications/abc-123``). This keeps the ``route`` dimension
  bounded in Elasticsearch / Kibana so dashboards aggregating by
  route do not explode on unique path-parameter values or query
  strings.

Cross-references
----------------
* **Consumed by**: ``services/notification-service/src/main.py`` via
  ``app.add_middleware(StructuredLoggingMiddleware)`` registered
  SECOND-TO-LAST so it runs SECOND-OUTERMOST at runtime (FastAPI /
  Starlette wraps later-added middlewares around earlier-added ones,
  so the LAST-added is the OUTERMOST).
* **Indirectly depends on**:

  * ``src/config/logging_config.py`` — must be called once during the
    startup lifespan so structlog's processor chain (including
    ``merge_contextvars``, ``TimeStamper``, ``add_log_level``,
    ``JSONRenderer``) is configured before the first request.
  * ``src/middleware/correlation_id.py`` — runs OUTSIDE this
    middleware and binds ``correlation_id`` into structlog's
    contextvars BEFORE this middleware emits its log line.
  * ``src/middleware/jwt_auth.py`` — runs INSIDE this middleware and
    sets ``scope["state"]["user"] = {"sub": ..., "scope": [...]}`` on
    successful JWT validation; this middleware reads ``sub`` for the
    ``user_id`` log field in its ``finally:`` block.
* **Sibling middleware**:

  * ``src/middleware/error_handler.py`` catches domain errors and
    emits an HTTP response — this middleware sees that response's
    ``http.response.start`` and picks the appropriate log level from
    its status code.
"""

# ``from __future__ import annotations`` enables PEP 563 postponed
# evaluation of annotations so type hints are stored as strings at
# runtime. This permits modern PEP 604 union syntax (e.g.,
# ``Any | None``, ``BaseException | None``) and forward-referenced
# generic aliases (``list[tuple[bytes, bytes]]``) on every supported
# Python version without explicit string quoting. It also keeps the
# style consistent with the sibling ``correlation_id.py`` module.
from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``time.monotonic_ns()`` provides a nanosecond-resolution monotonic
# clock that we use to measure request handling latency. We capture
# ``start_ns`` at request entry and compute
# ``latency_ms = (time.monotonic_ns() - start_ns) / 1_000_000.0`` in
# the ``finally:`` block. The monotonic clock is critical here because
# wall-clock adjustments (NTP slew, DST transitions, manual ``date``
# changes) can produce negative deltas with ``time.time()`` — and
# negative latencies poison SLO calculations and percentile
# aggregations downstream.
import time

# ``typing.Any`` annotates the lazily-cached structlog bound logger
# reference. We deliberately use ``Any`` rather than importing
# structlog's ``BoundLogger`` because structlog's typed loggers depend
# on the configured processor chain at runtime; ``Any`` keeps the file
# clean under ``mypy --strict`` while preserving runtime correctness.
# This mirrors the typing approach used in the sibling
# ``recommendation-engine`` reference middleware.
from typing import Any

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``starlette.types`` exposes the canonical ASGI type aliases. We
# import them rather than declaring our own so the middleware's
# signatures are SOURCE-COMPATIBLE with the rest of the Starlette /
# FastAPI middleware ecosystem (``add_middleware``, ``Middleware``):
#   - ``ASGIApp`` — the inner application reference (``self._app``).
#   - ``Scope``   — ASGI request scope dict passed to ``__call__``.
#                   We read ``scope["type"]``, ``scope["method"]``,
#                   ``scope["headers"]``, ``scope["path"]``,
#                   ``scope["route"]``, and ``scope["state"]``.
#   - ``Receive`` — async callable yielding incoming ``Message`` events.
#   - ``Send``    — async callable accepting outgoing ``Message`` events.
#   - ``Message`` — dict shape observed in the ``send_observing``
#                   closure; specifically ``http.response.start`` for
#                   status capture and ``http.response.body`` for
#                   byte counting.
# We deliberately do NOT import ``BaseHTTPMiddleware`` or any FastAPI /
# Starlette framework primitives (``Request``, ``Response``,
# ``HTTPException``); pure ASGI gives us everything we need with zero
# message-buffering overhead on hot-path routes.
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# ``structlog`` is the SOLE logging primitive used by this middleware
# (no stdlib ``logging`` imports per the agent_prompt's Phase 7
# hygiene rules). The middleware obtains a cached bound logger via
# ``structlog.get_logger(__name__)`` (lazily, on first request) and
# emits exactly one structured JSON log record per request through
# level-selected calls (``logger.info``, ``logger.warning``,
# ``logger.error``). The structlog ``merge_contextvars`` processor
# (installed FIRST in the processor chain by
# ``src/config/logging_config.py``) auto-merges the ``correlation_id``
# bound by :class:`CorrelationIdMiddleware` AND the startup-bound
# ``service`` / ``version`` / ``environment`` keys into every emitted
# record, satisfying AAP R-26's required field set without explicit
# per-call binding.
import structlog


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
# ``__all__`` declares the ONLY public symbol exported by this module
# that participates in ``from src.middleware.structured_logging import *``.
# The naming is critical: per the agent_prompt's Phase 8.7 contract,
# ``src/main.py`` imports the class as ``StructuredLoggingMiddleware``
# (NOT ``LoggingMiddleware`` as in the sibling recommendation-engine
# reference). Renaming this symbol — even subtly — would break the
# main.py import line and the entire service would fail to start.
#
# Module-level constants (``_UNKNOWN_USER_ID``, ``_UNKNOWN_ROUTE``,
# ``_UNKNOWN_STATUS``) are deliberately PRIVATE (single-underscore
# prefix) and excluded from ``__all__``. They are sentinel values
# internal to this middleware; production code outside this module
# should never depend on their values.
__all__: list[str] = ["StructuredLoggingMiddleware"]


# ---------------------------------------------------------------------------
# Module-level constants (private)
# ---------------------------------------------------------------------------
#: Sentinel value used for the AAP R-26 ``user_id`` log field when the
#: request is not authenticated. Populated for:
#:
#:   * Public routes that bypass JWT validation entirely
#:     (``/health/live``, ``/health/ready``, ``/metrics``).
#:   * Unauthenticated requests rejected by :class:`JWTAuthMiddleware`
#:     before it had a chance to populate ``scope["state"]["user"]``.
#:   * Requests where the JWT was structurally invalid and the auth
#:     middleware short-circuited with a 401 response.
#:
#: ``"anonymous"`` is the canonical RFC 4519 / RFC 8141 anonymous-
#: subject sentinel and is widely understood by Kibana operators.
#: Using a non-empty string (rather than ``None`` / null) keeps the
#: ``user_id`` field present on every log record — matching the AAP
#: R-26 requirement that the field be reported "when known" — and
#: lets dashboards filter on ``user_id != "anonymous"`` to count
#: authenticated traffic.
_UNKNOWN_USER_ID: str = "anonymous"

#: Sentinel value for the AAP R-26 ``route`` field when the URL path
#: cannot be determined from the ASGI scope. In practice this is
#: extremely rare — the ASGI spec guarantees ``scope["path"]`` is set
#: for HTTP scopes — but defensive sentinels keep downstream Kibana
#: parsers from breaking on missing fields. The literal string
#: ``"unknown"`` is preferred over an empty string because empty
#: strings can be silently dropped by some log shippers / parsers.
_UNKNOWN_ROUTE: str = "unknown"

#: Sentinel value for the AAP R-26 ``status`` field when the inner
#: app crashed BEFORE emitting an ``http.response.start`` ASGI
#: message (i.e., the response status was never set). The integer
#: ``0`` is chosen because:
#:
#:   * Real HTTP statuses are 100 - 599 — ``0`` is unambiguous as a
#:     "not observed" marker.
#:   * Integer typing is preserved (Elasticsearch indexes the field
#:     as a long; mixing strings and integers would force the index
#:     mapping to a less-efficient text type).
#:
#: Logic in ``__call__`` later promotes this sentinel to ``500`` when
#: an exception path is detected, since by convention an unhandled
#: exception manifests externally as a 500 Internal Server Error
#: when the upstream Starlette server emits its fallback response.
_UNKNOWN_STATUS: int = 0


# ---------------------------------------------------------------------------
# Middleware class
# ---------------------------------------------------------------------------
class StructuredLoggingMiddleware:
    """Pure-ASGI middleware that emits one structured JSON log line per request.

    The log line is produced in the ``finally:`` block of
    :meth:`__call__` so it is emitted whether the inner middleware
    chain succeeded, returned an error response, or crashed with an
    unhandled exception. The emitted record carries the AAP R-26
    minimum field set:

        timestamp | level | service | correlation_id | user_id |
        route | method | status | latency_ms | message

    plus optional request / response size metrics
    (``request_size_bytes``, ``response_size_bytes``) used by Kibana
    payload-volume dashboards.

    Field provenance
    ----------------
    Many of the AAP R-26 required fields are NOT explicitly passed to
    the log call by this middleware — they are auto-merged into every
    log record by the structlog processor chain configured in
    ``src/config/logging_config.py``:

    +-----------------+----------------------------------------------+
    | Field           | Source                                       |
    +=================+==============================================+
    | ``timestamp``   | ``structlog.processors.TimeStamper`` (ISO   |
    |                 | 8601 UTC, RFC 3339 compatible)              |
    +-----------------+----------------------------------------------+
    | ``level``       | ``structlog.processors.add_log_level``      |
    +-----------------+----------------------------------------------+
    | ``message``     | First positional arg of the ``logger.<lvl>``|
    |                 | call (``"request.completed"`` /             |
    |                 | ``"request.failed"``)                       |
    +-----------------+----------------------------------------------+
    | ``service``     | Bound at startup by ``main.lifespan`` via   |
    |                 | ``structlog.contextvars.bind_contextvars``  |
    +-----------------+----------------------------------------------+
    | ``correlation_  | Bound per-request by                        |
    | id``            | :class:`CorrelationIdMiddleware`            |
    +-----------------+----------------------------------------------+
    | ``user_id``     | Populated by this middleware from           |
    |                 | ``scope["state"]["user"]["sub"]``           |
    +-----------------+----------------------------------------------+
    | ``route``       | Populated by this middleware (FastAPI       |
    |                 | route template, falls back to raw path)     |
    +-----------------+----------------------------------------------+
    | ``method``      | Populated by this middleware from           |
    |                 | ``scope["method"]``                         |
    +-----------------+----------------------------------------------+
    | ``status``      | Populated by this middleware from the       |
    |                 | observed ``http.response.start`` status     |
    +-----------------+----------------------------------------------+
    | ``latency_ms``  | Computed by this middleware as              |
    |                 | ``(monotonic_ns_end - start_ns) / 1e6``     |
    +-----------------+----------------------------------------------+

    Level selection
    ---------------
    * INFO    for 1xx / 2xx / 3xx responses (and the ``0`` sentinel
              for unobserved-start success paths, which is impossible
              in practice but defended-against).
    * WARNING for 4xx responses (client errors, including auth
              rejections from :class:`JWTAuthMiddleware`).
    * ERROR   for 5xx responses (server errors).
    * ERROR   for exceptions that propagate past
              :class:`ErrorHandlerMiddleware` without an
              ``http.response.start`` ever having been emitted —
              treated as an implicit 500.

    Privacy guarantees
    ------------------
    The log record produced by this middleware contains NO:

    * Request body bytes — the body is never read or buffered.
    * Response body bytes — only the byte count via ``len(body)``.
    * Request headers — only the ``Content-Length`` numeric value.
    * Response headers — completely opaque; never inspected.
    * JWT tokens — we read only the ``sub`` claim from
      ``scope["state"]["user"]`` populated by JWT middleware.
    * ``Authorization`` / ``Cookie`` / ``Set-Cookie`` header values.

    Rationale for pure-ASGI (vs ``BaseHTTPMiddleware``)
    ---------------------------------------------------
    The log middleware runs on EVERY request including
    ``/health/live``, ``/health/ready``, and ``/metrics``.
    ``starlette.middleware.base.BaseHTTPMiddleware`` buffers the
    entire response body into a ``Message`` queue for its
    ``dispatch()`` callback; that buffering is observable overhead
    on high-traffic probe endpoints and on any
    notification-status streaming responses. Pure ASGI
    (implementing ``__call__(scope, receive, send)`` directly)
    observes individual ``http.response.start`` and
    ``http.response.body`` events and forwards them through
    ``send()`` immediately — preserving streaming behavior and
    skipping the buffer.

    Args:
        app: The inner ASGI application (next middleware in the
            chain, or ultimately the FastAPI router). Stored as
            ``self._app`` and invoked unchanged for non-HTTP
            scopes; for HTTP scopes the ``send`` callable is
            wrapped with ``send_observing`` to capture status and
            body-size metrics without mutating the message stream.

    Example:
        >>> from fastapi import FastAPI
        >>> from src.middleware.structured_logging import (
        ...     StructuredLoggingMiddleware,
        ... )
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
                ``app.add_middleware``) or the FastAPI / Starlette
                router itself if this is the innermost middleware.
                Stored as ``self._app`` and invoked unchanged for
                non-HTTP scopes; for HTTP scopes the ``send`` callable
                is wrapped with ``send_observing``.
        """
        # Hold the inner app reference verbatim. We do not modify or
        # re-wrap it at construction time; the wrapping happens
        # per-request in ``__call__`` via the ``send_observing``
        # closure. Storing the reference once here keeps the
        # per-request overhead at a single attribute lookup
        # (``self._app``) rather than re-resolving the app from a
        # registry on every invocation — and ASGI middleware is
        # invoked on EVERY request so every cycle counts.
        self._app: ASGIApp = app

        # Lazy-created structlog bound logger reference. We
        # deliberately do NOT call ``structlog.get_logger(...)`` at
        # construction time because:
        #
        # 1. ``configure_logging(settings)`` (in
        #    ``src/config/logging_config.py``) is called ONCE during
        #    the FastAPI lifespan startup hook. Until that runs,
        #    structlog uses its default (non-JSON) renderer.
        #    Capturing a bound logger before configuration could
        #    snapshot the wrong renderer.
        # 2. The middleware class may be instantiated at module-import
        #    time when FastAPI's ``add_middleware(...)`` constructs
        #    the ASGI graph. Module imports must be side-effect-free
        #    per the project's hygiene rules; reaching into structlog
        #    at import time would violate that contract.
        #
        # The first request handles initialization via
        # :meth:`_get_logger`, after which the reference is cached
        # for the lifetime of the middleware instance (i.e., the
        # process lifetime).
        self._logger: Any | None = None

    # ------------------------------------------------------------------
    # Lazy logger accessor
    # ------------------------------------------------------------------
    def _get_logger(self) -> Any:
        """Return a cached structlog bound logger for this middleware.

        Lazy initialization on first request ensures the logger picks
        up the structlog processor chain configured by ``configure_
        logging`` during the application's lifespan startup hook.
        Capturing the bound logger before that point would snapshot
        structlog's default (non-JSON) renderer, producing log lines
        that Filebeat / Logstash cannot parse.

        Returns:
            The cached structlog bound logger. Typed as :class:`Any`
            because structlog's ``BoundLogger`` is generic over the
            processor chain; importing the precise type would pull
            structlog's full type stack into this module's surface
            without a clear benefit. ``Any`` keeps the file clean
            under ``mypy --strict`` while preserving runtime
            correctness.
        """
        # Cache check is a single attribute access plus an ``is None``
        # comparison — well under the cost of ``structlog.get_logger``
        # itself, which copies the configured processor chain into a
        # new ``BoundLogger`` instance. This makes the second-and-
        # subsequent-request overhead negligible.
        if self._logger is None:
            # ``structlog.get_logger(__name__)`` returns a logger
            # named with the dotted module path
            # (``src.middleware.structured_logging``). The name is
            # added to every emitted record under the ``logger`` key
            # by ``structlog.stdlib.add_logger_name`` if that
            # processor is installed; even if it is not, the name
            # serves as a debug aid.
            self._logger = structlog.get_logger(__name__)
        return self._logger

    # ------------------------------------------------------------------
    # Helper: parse Content-Length request header (static for testability)
    # ------------------------------------------------------------------
    @staticmethod
    def _read_content_length(headers: list[tuple[bytes, bytes]]) -> int:
        """Return the ``Content-Length`` request header as an integer.

        ASGI delivers headers as a list of ``(name_bytes, value_bytes)``
        tuples in ``scope["headers"]`` with header names normalized
        to lowercase. We iterate looking for an exact match on the
        lowercase header name. The match is defensively case-folded
        on each iteration to support unit-test stubs that may pass
        mixed-case header names (the spec mandates lowercase but
        defensive normalization protects against test-stub
        misconfiguration).

        If the header is absent or malformed, returns 0. Does NOT
        attempt to compute the actual body size — doing so would
        require buffering the body, which the middleware deliberately
        avoids to preserve streaming semantics on hot-path routes.

        Why fall back to 0 instead of -1 / None
        ---------------------------------------
        * **Type stability**: keeping the field a non-negative integer
          lets Elasticsearch index ``request_size_bytes`` as a numeric
          field uniformly. Mixing integer and null values in the same
          field would force the mapping to a less-efficient type.
        * **Honest semantics**: 0 means "we did not observe a value"
          (which is true for chunked-transfer-encoded requests with
          no Content-Length header). Downstream Kibana dashboards can
          filter ``request_size_bytes > 0`` to exclude unobserved
          requests from payload-volume aggregations.

        Args:
            headers: The raw ASGI ``scope["headers"]`` list, where
                each element is a ``(name_bytes, value_bytes)``
                tuple with the header name in lowercase per the
                ASGI spec.

        Returns:
            The non-negative integer value of the ``Content-Length``
            header, or 0 if the header is absent, empty, malformed,
            or contains a negative value.
        """
        for name, value in headers:
            # ``.lower()`` defensively in case a unit-test stub passes
            # mixed-case header names. The ASGI spec mandates that
            # ``scope["headers"]`` already contains lowercase names,
            # but unit tests may not respect that — and an extra
            # ``.lower()`` on a short bytes object is essentially
            # free (a few CPU cycles).
            if name.lower() == b"content-length":
                # ``decode("ascii")`` is the correct codec — header
                # values are restricted to ASCII per RFC 7230. The
                # ``.strip()`` removes any leading / trailing
                # whitespace some misbehaving proxies emit. The
                # ``int(...)`` raises ValueError on non-numeric
                # values; we catch and return 0.
                #
                # ``max(..., 0)`` clamps negative values (which are
                # nonsensical for a byte count but technically
                # accepted by ``int()`` if a client sends
                # ``Content-Length: -1``) to 0 — keeping the field a
                # non-negative integer for downstream Elasticsearch.
                try:
                    return max(int(value.decode("ascii").strip()), 0)
                except (ValueError, UnicodeDecodeError):
                    # Non-numeric value (e.g., a client sending
                    # ``Content-Length: chunked``) or non-ASCII bytes
                    # in the value (a protocol violation). Either
                    # way, treat as malformed and report 0.
                    return 0
        # Header not present in the iteration — common case for
        # chunked-transfer-encoded requests (HTTP/1.1) and some
        # streaming HTTP/2 requests. Reporting 0 is honest: we don't
        # know the body size without buffering it, and we won't
        # buffer it.
        return 0

    # ------------------------------------------------------------------
    # Helper: select log level from HTTP status (static for testability)
    # ------------------------------------------------------------------
    @staticmethod
    def _select_level(status: int) -> str:
        """Map an HTTP status code to a structlog level name.

        Mapping
        -------
        +-----------+--------------+--------------------------------+
        | Status    | Level        | Rationale                      |
        +===========+==============+================================+
        | >= 500    | ``"error"``  | Server-side faults: bugs,      |
        |           |              | dependency outages, panics.    |
        +-----------+--------------+--------------------------------+
        | >= 400    | ``"warning"``| Client-side faults: invalid    |
        |           | (and < 500)  | input, auth rejection, missing |
        |           |              | resource. Distinguished from   |
        |           |              | 5xx so on-call alerts only     |
        |           |              | fire on server errors.         |
        +-----------+--------------+--------------------------------+
        | otherwise | ``"info"``   | 1xx, 2xx, 3xx, and the ``0``   |
        |           |              | sentinel — all considered      |
        |           |              | normal request handling.       |
        +-----------+--------------+--------------------------------+

        The ``0`` sentinel (``_UNKNOWN_STATUS``) routes to ``info``
        only on the success path — the ``__call__`` method overrides
        this selection to ``error`` when an exception was caught
        with no observed ``http.response.start``, treating that case
        as an implicit 500.

        Args:
            status: The HTTP status code observed from the inner
                app's ``http.response.start`` ASGI message, or the
                ``_UNKNOWN_STATUS`` (0) sentinel if no start message
                was observed.

        Returns:
            One of ``"info"``, ``"warning"``, or ``"error"`` —
            matching the names of the structlog logger methods
            (``logger.info``, ``logger.warning``, ``logger.error``)
            so the caller can use ``getattr(logger, level)`` to
            dispatch the call dynamically.
        """
        # Order matters: check 5xx FIRST, because every 5xx is also
        # >= 400. We intentionally use sequential ``if`` returns
        # (rather than a chained ``elif``) for readability — each
        # branch is short and the early-return pattern documents
        # the priority unambiguously.
        if status >= 500:
            return "error"
        if status >= 400:
            return "warning"
        # 1xx / 2xx / 3xx — all "normal" handling. The ``0`` sentinel
        # also lands here on the success path; the ``__call__``
        # method intercepts the exception path separately so a 0
        # sentinel paired with a caught exception escalates to
        # ``error`` regardless of what this helper would say.
        return "info"

    # ------------------------------------------------------------------
    # Helper: resolve user_id from scope state (static for testability)
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_user_id(scope: Scope) -> str:
        """Extract the JWT subject (``sub`` claim) from the ASGI scope.

        Reads ``scope["state"]["user"]["sub"]`` if present and returns
        it; otherwise returns :data:`_UNKNOWN_USER_ID` (``"anonymous"``).

        The :class:`JWTAuthMiddleware` populates
        ``scope["state"]["user"]`` with the validated JWT claims on
        successful authentication. For public routes
        (``/health/live``, ``/health/ready``, ``/metrics``) JWT
        validation is skipped and ``scope["state"]["user"]`` is never
        set — this helper returns ``"anonymous"`` in that case.

        Defensive type checks
        ---------------------
        At every step we verify the expected type before accessing
        the next nesting level. This is intentional: ``scope["state"]``
        is a free-form dict that any middleware may populate with
        non-dict values; a downstream type error here would crash the
        ``finally:`` block and prevent the log line from being
        emitted at all — which is worse than logging
        ``user_id="anonymous"``.

        Args:
            scope: The ASGI request scope dict.

        Returns:
            The JWT ``sub`` claim if present and non-empty as a
            string, otherwise the constant :data:`_UNKNOWN_USER_ID`
            (``"anonymous"``).
        """
        # ``scope.get("state")`` rather than ``scope["state"]`` so a
        # malformed scope (e.g., a unit-test stub) doesn't raise
        # KeyError and crash the log path. State is initialized by
        # Starlette for HTTP requests but raw-ASGI tests may omit it.
        state = scope.get("state")
        if not isinstance(state, dict):
            # No state dict at all — request was never processed by
            # any middleware that populates state, or scope is a
            # minimal test stub. Return anonymous; absent a state
            # dict there is no JWT claim to read.
            return _UNKNOWN_USER_ID

        # ``state.get("user")`` reads the JWT claim payload set by
        # :class:`JWTAuthMiddleware` on successful authentication.
        # Public routes that bypass JWT validation never reach the
        # auth middleware, so this slot stays ``None`` (default for
        # ``.get``) and we fall through to the anonymous sentinel.
        user = state.get("user")
        if isinstance(user, dict):
            # ``user.get("sub")`` reads the standard JWT subject
            # claim per RFC 7519 §4.1.2. The subject is the
            # canonical identity for the authenticated principal —
            # typically a UUID, an email address, or a numeric user
            # ID, depending on the Auth Service's identity scheme.
            sub = user.get("sub")
            # ``isinstance(sub, str) and sub`` ensures we have a
            # non-empty string. An empty string ``""`` would render
            # as ``user_id=""`` in the log line, which is
            # technically valid JSON but operationally confusing —
            # Kibana operators expect a non-empty value when
            # ``user_id != "anonymous"``.
            if isinstance(sub, str) and sub:
                return sub

        # Default fallthrough: any path that didn't extract a valid
        # ``sub`` claim returns the anonymous sentinel. This includes:
        #
        # * Public-route requests (no JWT validation attempted).
        # * Auth-rejected requests (JWT middleware short-circuited
        #   with a 401 before populating ``state["user"]``).
        # * Test stubs that populate ``state["user"]`` with a non-
        #   dict value (defensive against fuzz inputs).
        # * Non-string ``sub`` values (defensive against malformed
        #   JWTs that somehow bypassed the auth middleware).
        return _UNKNOWN_USER_ID

    # ------------------------------------------------------------------
    # Helper: resolve route from scope (static for testability)
    # ------------------------------------------------------------------
    @staticmethod
    def _resolve_route(scope: Scope) -> str:
        """Return the request's route, preferring template over raw path.

        FastAPI populates ``scope["route"]`` with the matched
        :class:`starlette.routing.Route` (or ``APIRoute``) AFTER its
        router has resolved the request. The route's ``path``
        attribute is the path TEMPLATE — e.g., ``/notifications/{id}``
        — which we prefer over the raw concrete path
        (``/notifications/abc-123``) because templates have BOUNDED
        cardinality.

        Why route cardinality matters
        ------------------------------
        Elasticsearch and Kibana aggregate metrics by field value;
        unique values consume mapping slots and aggregation memory.
        If we logged the raw path, a request with a UUID path
        parameter (or a query string we accidentally include) would
        produce a unique ``route`` value PER REQUEST, which would:

        * Inflate the Elasticsearch field-value cardinality and
          potentially exceed the ``index.mapping.total_fields.limit``
          on long-tailed traffic.
        * Make Kibana's "top routes by p95 latency" dashboards
          unusable because every row would be a unique path.
        * Skew p99 percentiles toward the long-tail of unique paths
          rather than the actual route templates.

        Logging the template aggregates all variants of a route into
        a single dimension, which is what dashboards actually need.

        Fallback chain
        --------------
        1. If ``scope["route"]`` exists and has a non-empty
           ``.path`` attribute → use it.
        2. Otherwise fall back to ``scope["path"]`` (raw path).
        3. Otherwise fall back to :data:`_UNKNOWN_ROUTE`
           (``"unknown"``).

        Step 2 is hit for requests that did not match any FastAPI
        route — typically 404 responses generated by Starlette's
        default ``NotFound`` handler. In that case the raw path is
        the best we have, and the cardinality concern doesn't apply
        because 404s for non-existent paths are typically rare and
        operators want to see the exact path that was attempted.

        Args:
            scope: The ASGI request scope dict.

        Returns:
            The matched route template if available, else the raw
            path, else :data:`_UNKNOWN_ROUTE`.
        """
        # Step 1: prefer the matched route template. FastAPI / Starlette
        # populates ``scope["route"]`` with a ``Route`` (or
        # ``APIRoute``) object once the router has matched the
        # request. The route's ``.path`` attribute carries the
        # template string with placeholders (``/items/{item_id}``).
        route = scope.get("route")
        if route is not None:
            # ``getattr(route, "path", None)`` defensively probes for
            # the attribute — both ``starlette.routing.Route`` and
            # ``fastapi.routing.APIRoute`` expose ``.path``, but
            # custom user-defined route classes might not. We accept
            # only non-empty strings to avoid logging an empty
            # ``route=""`` field.
            path = getattr(route, "path", None)
            if isinstance(path, str) and path:
                return path

        # Step 2: fall back to the raw concrete path. ``scope["path"]``
        # is guaranteed to be a string for HTTP scopes per the ASGI
        # spec, but we ``isinstance`` check defensively in case a
        # malformed scope (e.g., a unit-test stub) violates the
        # contract.
        raw_path = scope.get("path")
        if isinstance(raw_path, str) and raw_path:
            return raw_path

        # Step 3: ultimate fallback. We should never reach here for
        # well-formed HTTP scopes — the ASGI spec guarantees
        # ``scope["path"]`` is set — but defensive sentinels keep the
        # log line valid even for malformed scopes.
        return _UNKNOWN_ROUTE

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

        Dispatch
        --------
        * **Non-HTTP scopes** (``lifespan``, ``websocket``) — pass
          through to the inner app untouched. Per-request logging
          does not apply: lifespan events are once-per-process and
          handled by ``main.lifespan``; websockets are not in scope
          for the Notification Service's HTTP-only public surface.
        * **HTTP scopes** — capture status / size metrics by
          observing the outbound message stream, then emit a single
          structured log line in the ``finally:`` block whether the
          inner chain succeeded, returned an error response, or
          raised an unhandled exception.

        The ``finally:`` block is the heart of the exactly-once-log
        guarantee: Python's ``try / finally`` semantics ensure the
        block runs on EVERY exit path including:

        * Normal coroutine return (success).
        * Inner-app exceptions (we catch, stash, and re-raise).
        * ``asyncio.CancelledError`` from a client disconnect
          (we don't catch this explicitly but ``finally:`` still
          runs before propagation).
        * Any other ``BaseException`` subclass.

        Exception handling
        ------------------
        We catch ``BaseException`` to capture EVERY exception path —
        including ``SystemExit`` and ``KeyboardInterrupt`` which are
        ``BaseException`` subclasses outside the normal ``Exception``
        hierarchy. After capturing the exception in
        ``error_exception`` for log-level selection, we re-raise so
        the caller (Starlette's protocol handler, or
        :class:`ErrorHandlerMiddleware` if it is INSIDE us) can
        handle it appropriately. Without the re-raise, callers would
        receive empty responses and the request would silently fail.

        Why ``user_id`` is read in ``finally:`` (not at request entry)
        --------------------------------------------------------------
        :class:`JWTAuthMiddleware` runs INSIDE this middleware. By
        the time the inner chain returns and we reach ``finally:``,
        ``scope["state"]["user"]`` has either been:

        * Populated by JWT middleware on successful authentication,
          OR
        * Left unset because JWT middleware raised (and
          :class:`ErrorHandlerMiddleware` converted to a 401 / 403),
          OR
        * Left unset because the request hit a public-route bypass.

        In ALL cases, reading ``scope["state"]`` in ``finally:`` is
        the correct pattern: we get the authenticated identity if
        any, and the anonymous sentinel otherwise — without ever
        racing the JWT middleware.

        Why ``correlation_id`` is NOT explicitly logged here
        ----------------------------------------------------
        :class:`CorrelationIdMiddleware` runs OUTSIDE this middleware
        and binds ``correlation_id`` into structlog's contextvars
        BEFORE we emit our log line. The structlog
        ``merge_contextvars`` processor (installed FIRST in the
        chain by ``src/config/logging_config.py``) auto-merges all
        bound contextvars into every log record. Passing
        ``correlation_id=...`` explicitly would create a duplicate
        key conflict that structlog's processor chain would either
        raise on or silently drop — neither outcome is desirable.

        Args:
            scope: The ASGI scope dict.
            receive: The ASGI receive callable; passed through
                unchanged to the inner app.
            send: The ASGI send callable; replaced with
                ``send_observing`` for HTTP scopes so this middleware
                can observe the outbound status code and response
                body byte count.
        """
        # --------------------------------------------------------------
        # Non-HTTP scopes: pass through untouched.
        #
        # The ASGI spec defines three scope types: ``http``,
        # ``websocket``, and ``lifespan``. Lifespan messages
        # (``startup``, ``shutdown``) arrive once per process and are
        # handled by ``main.lifespan``; websockets are not in scope
        # for the Notification Service's HTTP-only public surface.
        # Neither maps onto the per-request logging contract this
        # middleware implements, so we forward them verbatim.
        # --------------------------------------------------------------
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        # --------------------------------------------------------------
        # State captured via closure inside ``send_observing``.
        # Initialized to defensive defaults so that even if the inner
        # app crashes before emitting any response messages, the
        # ``finally:`` block has well-typed values to log.
        # --------------------------------------------------------------
        # ``status_code`` holds the HTTP status from the observed
        # ``http.response.start`` message. Defaults to the
        # ``_UNKNOWN_STATUS`` sentinel (0); will be promoted to 500
        # on the exception path if it remains unset.
        status_code: int = _UNKNOWN_STATUS

        # ``response_size_bytes`` is incremented by the byte count of
        # each observed ``http.response.body`` message. We sum
        # actual byte counts (rather than reading any
        # Content-Length response header) because Content-Length
        # may be absent on chunked responses and observing the
        # actual stream is correct for both encodings.
        response_size_bytes: int = 0

        # ``observed_start`` flags whether we ever observed an
        # ``http.response.start`` message. Used in the ``finally:``
        # block to distinguish:
        #
        # * Exception WITHOUT observed start → emit ERROR with
        #   status promoted to 500 (the inner app crashed before
        #   emitting any response).
        # * Exception WITH observed start → use the observed status
        #   for level selection (some bytes already went out — the
        #   client got a partial response).
        observed_start: bool = False

        # --------------------------------------------------------------
        # Per-request immutable values captured at request entry.
        # These do not change during the request lifecycle and are
        # read once here to avoid repeated dict lookups in the
        # ``finally:`` block.
        # --------------------------------------------------------------
        # ``scope.get("method", "UNKNOWN")`` — the HTTP method verb
        # ("GET", "POST", "PUT", etc.). The ASGI spec guarantees
        # this is set for HTTP scopes, but the ``"UNKNOWN"`` fallback
        # protects against malformed test scopes.
        method: str = scope.get("method", "UNKNOWN")

        # ``scope.get("headers", [])`` — the raw inbound headers
        # list. We read it ONCE here to compute
        # ``request_size_bytes``; subsequent middleware that needs
        # headers will read them from ``scope`` again.
        request_headers: list[tuple[bytes, bytes]] = scope.get("headers", [])

        # Compute the request body size from ``Content-Length`` (or
        # 0 if absent / malformed). This is a synchronous,
        # CPU-only operation — no I/O — so it is cheap to call at
        # request entry rather than deferring to the ``finally:``
        # block.
        request_size_bytes = self._read_content_length(request_headers)

        # --------------------------------------------------------------
        # Capture the start timestamp using the MONOTONIC clock.
        #
        # ``time.monotonic_ns()`` returns the value of a monotonic
        # nanosecond clock that:
        #   * Cannot go backward (NTP slew, DST, manual ``date``
        #     changes do NOT affect it).
        #   * Has nanosecond resolution on most platforms.
        #   * Is suitable for measuring elapsed time between two
        #     points in the same process.
        #
        # We deliberately do NOT use ``time.time()`` (wall clock) for
        # latency measurement. Wall-clock adjustments can produce
        # NEGATIVE deltas with ``time.time()`` — and negative
        # latencies poison SLO percentile aggregations downstream.
        # --------------------------------------------------------------
        start_ns = time.monotonic_ns()

        # --------------------------------------------------------------
        # Build the ``send_observing`` closure that captures status
        # and body-size metrics WITHOUT buffering or mutating the
        # outbound message stream.
        #
        # Per the ASGI spec, an HTTP response is conveyed as a
        # sequence of messages:
        #   * Exactly ONE ``http.response.start`` (with ``status``
        #     and ``headers``), followed by
        #   * One or more ``http.response.body`` messages (each
        #     carrying body bytes and an optional ``more_body`` flag).
        #
        # We observe each message, update our captured state, and
        # forward the message UNCHANGED to the outer ``send``. This
        # preserves streaming behavior — the inner app can still
        # write a response one chunk at a time and the client sees
        # bytes as they are produced, without us buffering them
        # into a single in-memory blob.
        # --------------------------------------------------------------
        async def send_observing(message: Message) -> None:
            """Observe outbound ASGI messages and forward them to ``send``.

            Captures the HTTP status code from ``http.response.start``
            messages and accumulates the response body byte count
            from ``http.response.body`` messages. Messages are
            forwarded UNCHANGED — this closure does NOT mutate
            response headers, status, or body bytes.

            Args:
                message: An outbound ASGI message dict.
            """
            # ``nonlocal`` declarations enable us to mutate the
            # captured closure variables (vs. reading them, which
            # would not require the declaration). Without
            # ``nonlocal``, an assignment would create a new local
            # variable shadowing the outer one — the very subtle
            # Python scoping bug that plagues callbacks and
            # closures.
            nonlocal status_code, response_size_bytes, observed_start

            if message["type"] == "http.response.start":
                # ``http.response.start`` arrives once per response
                # and carries the status code plus the response
                # headers list. We capture only the integer status
                # for the AAP R-26 ``status`` field. We DO NOT
                # capture the headers list — response headers may
                # carry sensitive values like ``Set-Cookie`` (auth
                # tokens, session identifiers) and ``Location``
                # (redirect URLs that may include query-string
                # tokens). Logging them would violate the privacy
                # contract.
                observed_start = True

                # ``int(message.get("status", _UNKNOWN_STATUS))``
                # coerces the status to int defensively — the spec
                # mandates int but unit-test stubs sometimes pass
                # strings. Passing a non-coercible value would raise
                # ValueError; we do NOT catch that here because a
                # malformed ASGI message indicates a bug worth
                # surfacing rather than silently swallowing.
                status_code = int(message.get("status", _UNKNOWN_STATUS))
            elif message["type"] == "http.response.body":
                # ``http.response.body`` may arrive 1+ times per
                # response. Each carries a ``body`` field (bytes)
                # and an optional ``more_body`` flag (bool). We sum
                # the byte counts to produce ``response_size_bytes``
                # — accurate for both Content-Length and
                # chunked-encoded responses.
                #
                # ``message.get("body", b"")`` defaults to empty
                # bytes for messages that omit the field (rare —
                # the spec says ``body`` is required — but
                # defensive). The ``isinstance`` check guards
                # against test stubs that pass strings or other
                # non-bytes types.
                body = message.get("body", b"")
                if isinstance(body, (bytes, bytearray)):
                    response_size_bytes += len(body)

            # ALWAYS forward the message — even messages we don't
            # care about (no current ASGI HTTP message types fall in
            # this category, but the pattern future-proofs the code
            # against new message types added in future ASGI spec
            # revisions). The outer ``send`` is the ASGI server's
            # callable; awaiting it yields control to the server
            # which writes bytes onto the wire.
            await send(message)

        # --------------------------------------------------------------
        # Invoke the inner app inside a ``try / finally`` block.
        #
        # Captured-exception pattern:
        #   ``error_exception`` records any exception caught from the
        #   inner app for use during log-level selection in the
        #   ``finally:`` block. We then re-raise so the caller
        #   (Starlette's protocol handler, or
        #   :class:`ErrorHandlerMiddleware` if INSIDE us) can convert
        #   it to an appropriate response. Without the re-raise,
        #   callers would receive empty responses and the request
        #   would silently fail.
        # --------------------------------------------------------------
        error_exception: BaseException | None = None
        try:
            await self._app(scope, receive, send_observing)
        except BaseException as exc:  # noqa: BLE001 - we log and re-raise
            # Catch ``BaseException`` (not just ``Exception``) so we
            # also observe ``SystemExit`` / ``KeyboardInterrupt`` /
            # ``GeneratorExit`` — none of which inherit from
            # ``Exception`` but all of which can crash the inner
            # app. We do NOT swallow these; we capture the reference
            # for log-level selection and re-raise immediately
            # below.
            error_exception = exc
            # ``raise`` (no argument) re-raises the currently active
            # exception, preserving the original traceback. Using
            # ``raise exc`` would also work but appends a redundant
            # frame. The bare ``raise`` is the canonical Python
            # idiom for "I observed this exception but I am not
            # handling it; let it continue propagating."
            raise
        finally:
            # ----- Compute latency in milliseconds (float) -----------
            # ``(end - start) / 1_000_000.0`` converts nanoseconds to
            # milliseconds with float precision. Using ``1_000_000.0``
            # (with the trailing ``.0``) forces float division so we
            # don't truncate sub-millisecond timings. Most requests
            # in this service are 1-100 ms; sub-ms precision matters
            # for the fastest health-check probes.
            latency_ms = (time.monotonic_ns() - start_ns) / 1_000_000.0

            # ----- Resolve route and user_id at log time -------------
            # Reading these in ``finally:`` (rather than at request
            # entry) ensures we capture state populated by INNER
            # middleware. Specifically:
            #
            #   * ``scope["route"]`` is set by the FastAPI router
            #     AFTER it matches the request, which happens
            #     INSIDE this middleware's ``await self._app(...)``
            #     call. So we must read it AFTER that call returns.
            #   * ``scope["state"]["user"]`` is set by
            #     :class:`JWTAuthMiddleware` on successful auth,
            #     also INSIDE our ``await self._app(...)`` call.
            route = self._resolve_route(scope)
            user_id = self._resolve_user_id(scope)

            # ----- Determine the log level ---------------------------
            # Level selection logic:
            #
            # 1. If an exception was caught AND no ``http.response.
            #    start`` was observed → escalate to ``error``. The
            #    inner app crashed before emitting any response;
            #    treat as an implicit 500 regardless of what the
            #    sentinel ``status_code=0`` would map to via
            #    :meth:`_select_level`.
            # 2. Otherwise (exception with observed start, OR no
            #    exception at all) → use the status-based mapping.
            #    For exceptions WITH observed start, the inner app
            #    sent SOME response bytes before crashing; the
            #    observed status is the truth on the wire, so we
            #    log it directly.
            level = (
                "error"
                if error_exception is not None and not observed_start
                else self._select_level(status_code)
            )

            # ----- Promote sentinel status to 500 on exception path --
            # If an exception was caught and we never observed an
            # ``http.response.start``, the upstream ASGI server will
            # synthesize a 500 response for the client. Logging
            # ``status=0`` would be misleading — by convention
            # operators expect the log to reflect what the client
            # actually saw (a 500). This keeps log records and HTTP
            # access logs aligned.
            if error_exception is not None and status_code == _UNKNOWN_STATUS:
                status_code = 500

            # ----- Resolve the bound logger lazily -------------------
            # First request triggers ``structlog.get_logger(__name__)``
            # via the cached helper; subsequent requests hit the
            # cached instance. See :meth:`_get_logger` for the
            # rationale behind lazy initialization.
            logger = self._get_logger()

            # ----- Dispatch to the level-appropriate logger method ---
            # ``getattr(logger, level)`` resolves to one of
            # ``logger.info``, ``logger.warning``, or ``logger.error``
            # — the structlog API surface exposed on every bound
            # logger. The dynamic dispatch keeps the call path tight
            # and avoids a chained ``if / elif / else`` block for
            # the common case.
            log_fn = getattr(logger, level)

            # ----- Emit the structured log record --------------------
            # The first positional argument is the EVENT NAME; the
            # structlog ``EventRenamer`` processor renames the
            # ``event`` key to ``message`` for AAP R-26 compliance.
            # We use TWO distinct event names so Kibana operators
            # can filter cleanly:
            #
            #   * ``"request.completed"`` — request finished
            #     normally (whether 2xx, 4xx, or 5xx). Use
            #     ``message:"request.completed" AND status>=500`` to
            #     find server errors.
            #   * ``"request.failed"`` — an unhandled exception
            #     bubbled past the inner middleware chain. Use
            #     ``message:"request.failed"`` to find logic bugs.
            #
            # NOTE — we deliberately do NOT pass ``correlation_id`` or
            # ``service`` to the logger call. Those fields are
            # auto-merged by ``merge_contextvars`` (the FIRST
            # processor in the structlog chain) from the contextvars
            # bound by :class:`CorrelationIdMiddleware` and the
            # startup hook respectively. Passing them explicitly
            # would create duplicate-key conflicts.
            log_fn(
                "request.completed" if error_exception is None else "request.failed",
                method=method,
                route=route,
                status=int(status_code),
                # ``round(latency_ms, 3)`` keeps microsecond
                # precision (3 decimal places of milliseconds =
                # 1 microsecond resolution). Avoids noisy
                # nanosecond-tail fractions that would inflate
                # log-line size for no operational benefit.
                latency_ms=round(latency_ms, 3),
                user_id=user_id,
                # ``int(...)`` coerces defensively — these values
                # are already int by construction but the explicit
                # cast documents the wire-shape contract.
                request_size_bytes=int(request_size_bytes),
                response_size_bytes=int(response_size_bytes),
            )
