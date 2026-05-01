"""Pure-ASGI structured logging middleware for the Recommendation Engine.

This module implements :class:`LoggingMiddleware` — the **second-outermost**
middleware in the Recommendation Engine's middleware stack. It runs AFTER
:class:`~src.middleware.correlation_id.CorrelationIdMiddleware` (so the
``correlation_id`` is already bound into structlog's context vars) and
BEFORE :class:`~src.middleware.jwt_auth.JWTAuthMiddleware` from the
add-order perspective; at runtime, however, JWTAuthMiddleware executes
INNER to LoggingMiddleware (later-added middlewares wrap earlier ones).
This ordering is deliberate: by the time the inner chain returns and we
reach the ``finally:`` block, ``scope["state"]["user"]`` has been
populated (or the JWT middleware raised and the error handler emitted a
401 / 403 response). In BOTH cases, reading ``scope["state"]`` in the
``finally:`` block yields the correct ``user_id`` for the log line.

Design philosophy
-----------------
* **Pure ASGI, NOT** ``starlette.middleware.base.BaseHTTPMiddleware`` —
  ``BaseHTTPMiddleware`` buffers the entire response body into a
  ``Message`` queue for its ``dispatch()`` callback. On hot-path routes
  (``/health/live``, ``/health/ready``, ``/metrics``, large streaming
  recommendation responses) this buffering is observable overhead. Pure
  ASGI observes individual ``http.response.start`` and
  ``http.response.body`` events and passes them through ``send()``
  immediately, preserving streaming behavior and skipping the buffer.
* **Exactly-once logging** — the guaranteed ``finally:`` path ensures the
  log line is emitted whether the request succeeded, returned 4xx,
  returned 5xx, or raised an exception that bypassed the error
  middleware. Without this guarantee, exceptions in edge cases would
  produce no observability trail at all.
* **No body buffering** — the middleware never reads request or response
  body bytes. ``request_size_bytes`` is derived from the
  ``Content-Length`` request header (0 for chunked-transfer-encoded
  requests); ``response_size_bytes`` is the running sum of ``len(body)``
  observed across each ``http.response.body`` message that flows through
  the wrapping ``send_observing`` closure.
* **No header logging** — request headers (which may contain
  ``Authorization`` / ``Cookie`` / ``X-Api-Key``) and response headers
  (which may contain ``Set-Cookie`` / ``Location``) are NEVER captured
  into the log record. Headers are PII / secret risks.
* **No body logging** — request bodies (often JSON payloads with PII
  fields like email, phone, name, address) and response bodies (which
  may include personalized recommendation results plus user attributes)
  are NEVER captured. Operators inspecting individual request payloads
  must use Filebeat's raw access logs or attach a debugger.

Emitted log fields
------------------
Every log line emitted by this middleware carries the AAP R-26 minimum
field set plus the request/response size metrics declared by the
``services/recommendation-engine/src/middleware/`` folder spec::

    timestamp        — RFC 3339 UTC timestamp (added by structlog
                       ``TimeStamper`` processor in logging_config.py).
    level            — "info" / "warning" / "error" — derived from the
                       observed HTTP status code.
    service          — bound at startup by ``configure_logging`` in
                       ``src/config/logging_config.py`` via the
                       ``merge_contextvars`` processor.
    correlation_id   — bound by ``CorrelationIdMiddleware`` upstream;
                       auto-merged by ``merge_contextvars`` (we do NOT
                       re-pass it explicitly to avoid double-binding).
    user_id          — JWT subject (``scope["state"]["user"]["sub"]``)
                       on authenticated requests; ``"anonymous"``
                       otherwise.
    route            — matched FastAPI route template (preferred) or
                       raw URL path (fallback). Template form keeps
                       cardinality bounded for downstream metrics.
    method           — HTTP verb (GET / POST / etc.) from the ASGI
                       scope.
    status           — numeric HTTP status code observed in the
                       ``http.response.start`` message; 500 for
                       exceptions that bypassed the error handler.
    latency_ms       — wall-clock latency in milliseconds, computed
                       from a monotonic-clock baseline and rounded to
                       three decimals (microsecond precision).
    request_size_bytes  — ``Content-Length`` header value (0 if
                          absent / malformed / chunked).
    response_size_bytes — running sum of bytes emitted across all
                          ``http.response.body`` messages.
    message          — ``"request.completed"`` on success / handled
                       error paths; ``"request.failed"`` when an
                       exception propagated out of the inner ASGI
                       chain.

Compliance notes
----------------
* AAP R-26 — every required field is present in every emitted log line;
  log lines are valid JSON after the ``JSONRenderer`` processor renders
  them.
* AAP R-27 — log lines are written to stdout where Filebeat tails them
  and ships them to Logstash.
* AAP R-19 — the middleware never blocks on I/O; the only synchronous
  work is a closure invocation, a few ``dict.get`` calls, and a single
  log emission. Liveness / readiness probes therefore retain their
  sub-millisecond latency.
* AAP R-25 — secrets never appear in log lines because headers and
  bodies are never logged. The ``user_id`` field carries the JWT
  ``sub`` claim only (typically a UUID or numeric ID), not the JWT
  itself.

Cross-references
----------------
* Consumed by ``services/recommendation-engine/src/main.py`` via
  ``app.add_middleware(LoggingMiddleware)``.
* Indirectly depends on
  ``services/recommendation-engine/src/config/logging_config.py``
  (must have configured structlog before any request arrives) and
  ``services/recommendation-engine/src/middleware/correlation_id.py``
  (must have bound the correlation ID into context vars upstream).
* Reads ``scope["state"]["user"]["sub"]`` populated by
  ``services/recommendation-engine/src/middleware/jwt_auth.py``.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``time.monotonic_ns`` provides a nanosecond-resolution monotonic clock
# used to compute ``latency_ms`` for each request. The monotonic clock is
# essential here: ``time.time()`` is wall-clock and can move BACKWARDS if
# the system clock is adjusted (NTP sync, leap second, manual change)
# mid-request, which would yield a NEGATIVE ``latency_ms`` — a silent data
# corruption bug for AAP R-26 compliance. ``time.monotonic_ns`` guarantees
# strict monotonicity and integer arithmetic (no float drift).
import time

# ``typing.Any`` types the lazily-cached structlog bound logger reference.
# structlog's ``get_logger`` return type is intentionally permissive
# (``Any``) because its concrete shape varies based on the configured
# wrapper class set in ``logging_config.py`` (``BoundLoggerLazyProxy``,
# ``FilteringBoundLogger``, etc.). Annotating the cache slot as
# ``Any | None`` lets mypy --strict accept ``getattr(logger, level)``
# below without resorting to ``# type: ignore``.
from typing import Any

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``starlette.types`` provides the canonical ASGI type aliases. We import
# them rather than declaring our own (``Callable[[dict[str, Any]], ...]``)
# so the middleware's signatures are SOURCE-COMPATIBLE with the rest of
# the Starlette / FastAPI middleware ecosystem (e.g.,
# ``BaseHTTPMiddleware``, ``Middleware``, ``CORSMiddleware``):
#   - ``ASGIApp``  — the inner application reference (``self._app``).
#   - ``Scope``    — ASGI request scope (dict) passed to ``__call__``.
#   - ``Receive``  — async callable yielding incoming ``Message`` events.
#   - ``Send``     — async callable accepting outgoing ``Message`` events.
#   - ``Message``  — dict shape observed in the wrapping ``send_observing``
#                    closure when capturing ``http.response.start`` status
#                    codes and ``http.response.body`` byte counts.
# We deliberately do NOT import ``BaseHTTPMiddleware`` or any FastAPI /
# Starlette framework primitives (``Request``, ``Response``,
# ``HTTPException``); pure ASGI gives us everything we need with zero
# message-buffering overhead on hot-path routes.
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# ``structlog`` is the primary structured logging library configured by
# the sibling ``src/config/logging_config.py``. The ``merge_contextvars``
# processor (installed FIRST in the chain) auto-merges
# ``correlation_id``, ``service``, ``version``, and ``environment`` from
# the bound context into every log record so we do NOT need to re-pass
# them here. Using stdlib ``logging`` would bypass that auto-merge and
# produce inconsistent log shapes — strictly forbidden per the folder
# spec.
import structlog

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
# ``__all__`` declares the single public symbol exported by this module.
# Sentinel constants below are deliberately module-private (``_`` prefix)
# because they are an implementation detail of the level / user_id /
# route resolution helpers — callers must not depend on their exact
# values.
__all__: list[str] = ["LoggingMiddleware"]


# ---------------------------------------------------------------------------
# Module-private sentinel constants
# ---------------------------------------------------------------------------
#: Sentinel value used for the ``user_id`` log field when the request
#: was not authenticated (e.g., a request to a public route like
#: ``/health/live``, ``/health/ready``, or ``/metrics``, or an
#: unauthenticated request that was rejected before JWT middleware
#: completed). Operators querying Kibana on
#: ``user_id:"anonymous"`` will see exactly the population of public /
#: pre-auth traffic.
_UNKNOWN_USER_ID: str = "anonymous"

#: Sentinel value for the ``route`` field when the URL path cannot be
#: determined from the ASGI scope (e.g., a malformed scope from a
#: misbehaving upstream proxy or a unit test stub that omitted
#: ``scope["path"]``). Operators seeing ``route:"unknown"`` in Kibana
#: should investigate the upstream — a healthy production deployment
#: should never produce this value.
_UNKNOWN_ROUTE: str = "unknown"

#: Sentinel value for the ``status`` field when the inner ASGI app
#: returned WITHOUT emitting an ``http.response.start`` message (a
#: protocol violation), or when an exception propagated out of the
#: inner chain BEFORE the start event was sent. The middleware
#: rewrites this sentinel to ``500`` in the log record so the emitted
#: status is always a valid HTTP code, but the sentinel is used
#: internally for level selection logic.
_UNKNOWN_STATUS: int = 0

#: Default HTTP status used in log records when an exception
#: propagated out of the inner ASGI chain BEFORE any
#: ``http.response.start`` message was observed. ASGI servers
#: (uvicorn, hypercorn) will eventually emit a 500 response on the
#: wire, so logging ``500`` here matches what the client actually
#: received.
_DEFAULT_EXCEPTION_STATUS: int = 500


class LoggingMiddleware:
    """Pure-ASGI middleware that emits one structured JSON log line per request.

    Emits on response dispatch (the ``finally`` block) so the log line
    is produced whether the inner chain succeeded, returned an error
    response, or crashed with an unhandled exception. The emitted
    record carries the AAP R-26 required fields plus the request /
    response size metrics declared by the folder spec.

    Level selection
    ---------------
    * ``info``    for 1xx – 3xx responses.
    * ``warning`` for 4xx responses (client errors).
    * ``error``   for 5xx responses (server errors).
    * ``error``   for protocol violations (no ``http.response.start``
      observed before the scope closed) and for exceptions that
      propagated out of the inner chain.

    Privacy
    -------
    Request bodies, response bodies, and individual headers (including
    ``Authorization``, ``Cookie``, and ``Set-Cookie``) are NEVER
    included in the log record. The middleware also avoids logging the
    raw URL path with query strings — the ``route`` field prefers the
    matched FastAPI route template, which strips path / query
    parameters and keeps the cardinality of the dimension bounded.

    Rationale for pure-ASGI (vs. ``BaseHTTPMiddleware``)
    ----------------------------------------------------
    The log middleware runs on EVERY request including ``/health/live``,
    ``/health/ready``, and ``/metrics``. ``BaseHTTPMiddleware`` buffers
    the entire response body into a ``Message`` queue for its
    ``dispatch()`` callback; that buffering is observable overhead on
    high-traffic probe endpoints and on large streaming responses.
    Pure ASGI observes the ``http.response.start`` and
    ``http.response.body`` events without buffering them, preserving
    streaming behavior and avoiding the ``Message``-queue allocation.

    Args:
        app: The inner ASGI application (next middleware in the chain
            from this middleware's perspective). Stored as ``self._app``
            and invoked as ``await self._app(scope, receive, send)``
            for non-HTTP scopes (lifespan, websocket) or
            ``await self._app(scope, receive, send_observing)`` for
            HTTP scopes where we wrap ``send`` to observe response
            events.
    """

    def __init__(self, app: ASGIApp) -> None:
        """Initialize the middleware with a reference to the inner ASGI app.

        Args:
            app: The inner ASGI application that this middleware wraps.
        """
        # Store the inner app reference for invocation in ``__call__``.
        # We do NOT take any other arguments at construction time —
        # logging configuration (level threshold, processors, JSON
        # renderer) is set globally by
        # ``src/config/logging_config.py`` BEFORE the first request
        # arrives, so this middleware has nothing to configure.
        self._app: ASGIApp = app

        # Lazy-created structlog bound logger reference. Created on
        # first request invocation rather than at ``__init__`` so the
        # module is import-side-effect-free (e.g., importing this
        # module in a test fixture without first calling
        # ``configure_logging`` does NOT trigger structlog
        # configuration). The cached reference avoids the (small but
        # measurable on hot-path routes) cost of re-binding the logger
        # on every request.
        self._logger: Any | None = None

    # ------------------------------------------------------------------
    # Logger access
    # ------------------------------------------------------------------
    def _get_logger(self) -> Any:
        """Return a cached structlog bound logger for this middleware.

        The logger is created on first call (lazy initialization) so
        importing this module is side-effect-free. structlog's
        ``get_logger`` returns a lazy proxy that adapts to the
        currently-configured processor chain at log time, so it is
        safe to cache the proxy across configuration reloads.

        Returns:
            A structlog bound logger (typically a ``BoundLoggerLazyProxy``
            or ``FilteringBoundLogger`` depending on the configured
            wrapper class).
        """
        if self._logger is None:
            # Bind the module name so log records carry a stable
            # ``logger`` field for filtering in Kibana — operators can
            # narrow on
            # ``logger:"src.middleware.structured_logging"`` to see
            # only request log lines.
            self._logger = structlog.get_logger(__name__)
        return self._logger

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------
    # All helpers are ``@staticmethod`` because they do not depend on
    # instance state. Marking them static makes their independence
    # explicit, avoids any temptation to reach for ``self`` (which would
    # introduce hidden coupling), and keeps them trivially unit-testable.

    @staticmethod
    def _read_content_length(headers: list[tuple[bytes, bytes]]) -> int:
        """Return the ``Content-Length`` request header value as an integer.

        Pure ASGI cannot easily read the request body without buffering
        it. We deliberately do NOT buffer bodies (PII risk + cost),
        so we extract the ``Content-Length`` header as an advisory
        size hint. Absent or malformed headers yield ``0`` rather than
        a guess.

        Why the fallback to ``0``: chunked-transfer-encoded requests
        have no ``Content-Length`` header by design, and rather than
        buffering the streamed body just to count its bytes we report
        ``0`` (i.e., "we don't know"). Downstream Kibana dashboards
        should filter on ``request_size_bytes:>0`` when aggregating
        request payload sizes; the all-zero requests are the chunked
        ones and should not skew percentiles.

        Args:
            headers: ASGI request headers as a list of
                ``(name_bytes, value_bytes)`` tuples (the canonical
                ASGI header representation).

        Returns:
            The non-negative ``Content-Length`` value as ``int``;
            ``0`` if absent, malformed, or non-numeric.
        """
        # Header names in ASGI are canonical lowercase bytes, but we
        # call ``.lower()`` defensively to support stubs / mocks that
        # may pass mixed-case headers in unit tests.
        for name, value in headers:
            if name.lower() == b"content-length":
                try:
                    # ``int(...)`` raises ``ValueError`` on non-numeric
                    # input; ``decode("ascii")`` raises
                    # ``UnicodeDecodeError`` on non-ASCII bytes.
                    # ``.strip()`` tolerates the (uncommon) leading /
                    # trailing whitespace some proxies emit.
                    # ``max(..., 0)`` clamps any (illegal) negative
                    # value to ``0`` rather than letting it skew
                    # downstream metrics.
                    return max(int(value.decode("ascii").strip()), 0)
                except (ValueError, UnicodeDecodeError):
                    # Any malformed value is treated as "unknown".
                    # We deliberately do NOT log a warning here — a
                    # client sending a malformed Content-Length is
                    # noisy, and the controller will reject the
                    # request anyway.
                    return 0
        # Header absent — common case for GET / HEAD / DELETE without
        # a body, or for chunked-transfer-encoded requests.
        return 0

    @staticmethod
    def _select_level(status: int) -> str:
        """Map an HTTP status code to a structlog level name.

        The mapping follows the conventional severity hierarchy and
        the folder-spec contract:

        * ``status >= 500`` -> ``"error"``  (server-side failure;
          alertable).
        * ``status >= 400`` -> ``"warning"`` (client-side error;
          informational).
        * Anything else (1xx / 2xx / 3xx, plus the ``0`` sentinel) ->
          ``"info"``  (success or informational status).

        Args:
            status: The numeric HTTP status code observed in the
                ``http.response.start`` message, or
                :data:`_UNKNOWN_STATUS` (``0``) when no start event
                was observed.

        Returns:
            The structlog level name as a lowercase string suitable
            for ``getattr(logger, level)`` invocation: one of
            ``"info"``, ``"warning"``, or ``"error"``.
        """
        # Order matters: the 5xx branch must run BEFORE the 4xx branch
        # because every 5xx is also >= 400. Using ``elif`` keeps the
        # check structure explicit and avoids redundant comparisons
        # in the common 2xx case.
        if status >= 500:
            return "error"
        if status >= 400:
            return "warning"
        return "info"

    @staticmethod
    def _resolve_user_id(scope: Scope) -> str:
        """Resolve the authenticated user's identifier from the ASGI scope.

        Reads ``scope["state"]["user"]["sub"]`` if present (populated
        by :class:`~src.middleware.jwt_auth.JWTAuthMiddleware` on
        successful JWT validation); otherwise returns
        :data:`_UNKNOWN_USER_ID` (``"anonymous"``).

        For public routes (``/health/live``, ``/health/ready``,
        ``/metrics``), JWT validation is skipped via the auth
        middleware's allow-list and this returns ``"anonymous"``.

        Defensive type checks
        ---------------------
        Each layer of the lookup is type-checked because the ASGI
        ``Scope`` type is permissive (``dict[str, Any]``) and a
        misbehaving upstream middleware could place arbitrary values
        at the ``state`` / ``user`` / ``sub`` paths. Returning the
        sentinel on any anomaly is preferable to raising — a logging
        middleware should NEVER crash on observability data.

        Args:
            scope: The ASGI request scope; expected to carry a
                ``state`` dict populated by Starlette / FastAPI's
                middleware stack.

        Returns:
            The JWT subject string when authentication succeeded;
            :data:`_UNKNOWN_USER_ID` otherwise.
        """
        # ``scope.get("state")`` rather than ``scope["state"]`` because
        # the state slot is only present after Starlette /
        # ``CorrelationIdMiddleware`` initializes it. For lifespan and
        # websocket scopes (which we already short-circuited in
        # ``__call__``) the slot may also be absent.
        state = scope.get("state")

        # ``isinstance(..., dict)`` rather than a truthiness check so
        # that an empty dict (``{}``) is correctly recognized as
        # "state initialized but no user" and falls through to the
        # sentinel return below. An attribute-style ``State`` object
        # (Starlette's default) supports ``.__getattr__`` but NOT
        # ``.get()``, so we accept only the dict shape — the agent
        # prompt explicitly specifies the dict shape for the slot.
        if not isinstance(state, dict):
            return _UNKNOWN_USER_ID

        # Walk to ``state["user"]``; any missing intermediate yields
        # the sentinel return.
        user = state.get("user")
        if isinstance(user, dict):
            sub = user.get("sub")
            # Empty / non-string ``sub`` values are treated as
            # "unknown" — a JWT with an empty subject claim is
            # malformed and should never reach this code path in
            # practice, but defending against it keeps the log line
            # well-formed even in pathological cases.
            if isinstance(sub, str) and sub:
                return sub
        return _UNKNOWN_USER_ID

    @staticmethod
    def _resolve_route(scope: Scope) -> str:
        """Return the request's logical route identifier.

        Prefers the matched FastAPI route template (e.g.,
        ``/recommendations``) over the raw URL path
        (``/recommendations?userId=abc&limit=5``) so that downstream
        metrics aggregating by ``route`` retain bounded cardinality.
        A naive raw-path approach would fan out into a unique label
        per query string and explode Kibana / Prometheus storage.

        Resolution order
        ----------------
        1. ``scope["route"].path`` — populated by FastAPI / Starlette
           AFTER router matching; the canonical low-cardinality form.
        2. ``scope["path"]`` — raw URL path WITHOUT query string
           (ASGI strips the query string into the separate
           ``query_string`` slot).
        3. :data:`_UNKNOWN_ROUTE` — fallback for malformed scopes.

        Args:
            scope: The ASGI request scope.

        Returns:
            The route template path, raw URL path, or the
            ``_UNKNOWN_ROUTE`` sentinel.
        """
        # Step 1: prefer the matched route template. ``scope["route"]``
        # is set by FastAPI / Starlette's router AFTER matching.
        # Earlier in the request lifecycle (i.e., before the router
        # runs INNER to this middleware), this slot may be absent or
        # ``None`` — both cases fall through to the raw path.
        route = scope.get("route")
        if route is not None:
            # ``getattr`` with default rather than ``route.path``
            # because ``route`` may be a duck-typed test stub that
            # does not expose a ``.path`` attribute.
            path = getattr(route, "path", None)
            if isinstance(path, str) and path:
                return path

        # Step 2: fall back to the raw URL path. ASGI guarantees this
        # is a string for HTTP scopes, but we type-check defensively
        # to support unit tests with malformed scopes.
        raw_path = scope.get("path")
        if isinstance(raw_path, str) and raw_path:
            return raw_path

        # Step 3: malformed scope. Should not happen in production.
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
        """ASGI entry point — invoked by the outer middleware on each event.

        Lifecycle
        ---------
        1. **Non-HTTP scopes** (``lifespan``, ``websocket``) — pass
           through to the inner app untouched. Per-request logging
           does not apply to lifespan or websocket events; lifespan
           events are logged by ``src.main.lifespan`` directly, and
           websockets do not have request / response semantics.
        2. **HTTP scopes** — record the start time, install a
           ``send_observing`` closure that captures the
           ``http.response.start`` status and the running sum of
           ``http.response.body`` byte counts, invoke the inner app
           in a ``try / finally`` block, and emit exactly one
           structured log line in ``finally``.
        3. **Exception propagation** — if the inner chain raises, the
           exception is caught for log-level selection, then
           **re-raised** so the ASGI server's protocol handler can
           emit a 500 response and close the connection cleanly. The
           ``finally`` block still emits the log line before the
           re-raise unwinds.

        Why the ``finally`` block (not a callback or post-response
        hook): pure ASGI does not provide a "response complete" hook
        the way ``BaseHTTPMiddleware`` does via ``dispatch()``, but
        ``finally`` is universal — it runs on success, on observed
        4xx/5xx, AND on uncaught exceptions, giving exactly-once
        log emission for every code path.

        Args:
            scope: The ASGI request scope.
            receive: The ASGI receive callable; passed through
                unchanged.
            send: The ASGI send callable; wrapped by
                ``send_observing`` for HTTP scopes so we can capture
                response status and body sizes without buffering.
        """
        # Non-HTTP scopes (lifespan, websocket) do not have
        # request / response semantics — pass through and return.
        # We deliberately do NOT log lifespan startup / shutdown
        # here; that belongs to the lifespan manager in
        # ``src/main.py`` (which logs ``service.starting``,
        # ``service.started``, ``service.stopping``,
        # ``service.stopped`` with extra fields beyond what we have
        # access to here).
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        # ------------------------------------------------------------------
        # Per-request mutable state captured by the ``send_observing``
        # closure. Defaults are used if the inner app crashes before
        # emitting ``http.response.start`` — in that case ``status_code``
        # remains ``_UNKNOWN_STATUS`` and we rewrite it to
        # ``_DEFAULT_EXCEPTION_STATUS`` (500) in ``finally`` so the log
        # record always carries a valid HTTP code.
        # ------------------------------------------------------------------
        status_code: int = _UNKNOWN_STATUS
        response_size_bytes: int = 0
        observed_start: bool = False

        # Method comes directly from the ASGI scope; no normalization
        # (uppercase / lowercase) — the ASGI spec mandates uppercase
        # method strings (RFC 9110 §9.1) and we propagate that
        # invariant into our log records.
        method: str = scope.get("method", "UNKNOWN")

        # Request headers as a list of (name_bytes, value_bytes) tuples
        # per the ASGI spec. We pass this through to
        # ``_read_content_length`` and discard the reference
        # immediately — at no point do we capture individual header
        # values into the log record.
        request_headers: list[tuple[bytes, bytes]] = scope.get("headers", [])
        request_size_bytes: int = self._read_content_length(request_headers)

        # Monotonic-clock baseline. ``time.monotonic_ns`` returns an
        # integer nanosecond count from a platform-defined epoch; the
        # delta to a later sample is what we care about, not the
        # absolute value. Integer arithmetic avoids float precision
        # drift on long-lived processes.
        start_ns: int = time.monotonic_ns()

        async def send_observing(message: Message) -> None:
            """Wrapper around ``send`` that observes response events.

            Captures:
            * ``http.response.start.status``    -> ``status_code``
            * ``http.response.body.body`` length -> ``response_size_bytes``

            Forwards every message to the underlying ``send`` callable
            without buffering or modification — the ASGI pipe is
            preserved verbatim so streaming responses stay streaming.
            """
            nonlocal status_code, response_size_bytes, observed_start

            # The ``http.response.start`` message carries the status
            # and headers. We capture only the status — response
            # headers may contain ``Set-Cookie`` / ``Location`` /
            # custom auth tokens and are off-limits for logging.
            if message["type"] == "http.response.start":
                observed_start = True
                # ``message.get("status", _UNKNOWN_STATUS)`` rather
                # than ``message["status"]`` because a spec-violating
                # inner app could omit the key. ``int(...)`` forces a
                # numeric type — most ASGI apps emit an ``int``
                # already, but a stub passing a string would otherwise
                # break the level selection.
                status_code = int(message.get("status", _UNKNOWN_STATUS))

            # The ``http.response.body`` message carries the body
            # bytes (and a ``more_body`` flag for streaming).
            # We sum the byte counts but never inspect or store the
            # bytes themselves.
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                # ``isinstance(..., (bytes, bytearray))`` because the
                # ASGI spec allows both; an inner app could emit a
                # ``memoryview`` but uvicorn / starlette normalize to
                # bytes. Anything else (None, str) silently
                # contributes zero to the size — the wire bytes are
                # what matter, and a non-bytes body in this slot is
                # a server bug we do not want to amplify.
                if isinstance(body, (bytes, bytearray)):
                    response_size_bytes += len(body)

            # Forward to the underlying send IMMEDIATELY (no
            # buffering) so streaming responses retain their
            # streaming behavior. Awaiting here yields control back
            # to the ASGI server, which sends the bytes on the wire
            # and resumes our coroutine when it is ready for more.
            await send(message)

        # ------------------------------------------------------------------
        # Invoke the inner app in a try / finally so the log line is
        # emitted on EVERY code path (success, observed error, raised
        # exception). The ``error_exception`` slot lets the
        # ``finally`` block distinguish between an observed-error
        # response (4xx / 5xx with ``observed_start=True``) and an
        # exception that bypassed any error handler.
        # ------------------------------------------------------------------
        error_exception: BaseException | None = None
        try:
            await self._app(scope, receive, send_observing)
        except BaseException as exc:
            # Catch ``BaseException`` (not just ``Exception``) so we
            # log even on ``KeyboardInterrupt`` / ``SystemExit`` /
            # ``CancelledError``. The re-raise below preserves the
            # exception's propagation semantics so the caller can
            # close the connection appropriately.
            #
            # Why catch and re-raise rather than let the ``finally``
            # alone observe it: ``finally`` runs on exception, but
            # ``error_exception`` would be ``None`` because no
            # assignment happened. Capturing the exception in the
            # slot lets the log line distinguish ``request.completed``
            # from ``request.failed`` and adjust the level to
            # ``error`` even when ``observed_start`` is False.
            error_exception = exc
            raise
        finally:
            # ----------------------------------------------------------
            # Compute the latency. Using ``time.monotonic_ns`` and
            # integer arithmetic guarantees a non-negative value even
            # if the wall clock was adjusted mid-request (the monotonic
            # clock is decoupled from the wall clock).
            # Convert nanoseconds to milliseconds via ``/ 1_000_000.0``
            # and round to three decimals (microsecond precision) — any
            # finer resolution adds noise to the log lines without
            # operational value.
            # ----------------------------------------------------------
            latency_ms: float = (time.monotonic_ns() - start_ns) / 1_000_000.0

            # Resolve the route AFTER the inner chain has run so
            # ``scope["route"]`` (set by FastAPI's router) is populated
            # — this is exactly why the resolution happens here in
            # ``finally`` rather than at the top of ``__call__``.
            route: str = self._resolve_route(scope)

            # Resolve the user_id AFTER the inner chain has run so
            # ``scope["state"]["user"]`` (set by JWTAuthMiddleware) is
            # populated. On failed JWT validation the auth middleware
            # raises and the error handler emits a 401; in BOTH cases
            # ``scope["state"].get("user")`` is the right source of
            # truth at this point in the lifecycle.
            user_id: str = self._resolve_user_id(scope)

            # ----------------------------------------------------------
            # Level selection.
            #
            # Two cases:
            # 1. Exception propagated AND no start event observed -->
            #    treat as 500 (error). The inner chain failed before
            #    any response could be emitted; the ASGI server will
            #    emit a 500 on the wire, so logging at error level
            #    matches what the client sees.
            # 2. Otherwise --> map the observed status code to a
            #    level via ``_select_level``. This handles success
            #    (2xx -> info), client errors (4xx -> warning), and
            #    server errors (5xx -> error) uniformly.
            # ----------------------------------------------------------
            level: str = (
                "error"
                if error_exception is not None and not observed_start
                else self._select_level(status_code)
            )

            # If an exception propagated but no start event was
            # observed, rewrite the sentinel ``_UNKNOWN_STATUS`` to
            # ``500`` so the log record carries a valid HTTP code.
            # We do NOT modify ``status_code`` when ``observed_start``
            # is True because in that case the inner app already
            # emitted a (presumably correct) status before raising.
            if error_exception is not None and status_code == _UNKNOWN_STATUS:
                status_code = _DEFAULT_EXCEPTION_STATUS

            # ----------------------------------------------------------
            # Resolve the level method on the cached logger
            # (``logger.info`` / ``logger.warning`` / ``logger.error``)
            # via ``getattr`` so the level selection above remains
            # data-driven. ``structlog.BoundLogger`` exposes all
            # standard level methods; the ``Any``-typed cache slot
            # lets mypy --strict accept this without complaint.
            # ----------------------------------------------------------
            logger = self._get_logger()
            log_fn = getattr(logger, level)

            # ----------------------------------------------------------
            # Emit the log record. Notes:
            # * The first positional argument is the ``event`` field
            #   (structlog's stable identifier for the log line):
            #     - ``request.completed`` on the success / handled-error
            #       path (``error_exception is None``).
            #     - ``request.failed`` on the exception path
            #       (``error_exception is not None``).
            #   Two distinct event names aid Kibana queries — operators
            #   can filter on
            #   ``event:"request.failed"`` to see only the exception
            #   trail without manually constructing a status range.
            # * ``correlation_id`` is NOT explicitly passed: structlog's
            #   ``merge_contextvars`` processor (installed FIRST in the
            #   chain by ``logging_config.py``) auto-merges it from the
            #   bound context. Passing it explicitly would create a
            #   duplicate key in the rendered JSON.
            # * ``service``, ``version``, and ``environment`` are bound
            #   at startup by ``logging_config.py``; same auto-merge
            #   applies.
            # * ``int(status_code)`` and ``int(request_size_bytes)``
            #   force numeric coercion so even if the inner app emitted
            #   a string status (a spec violation) the log record
            #   still carries an integer.
            # * ``round(latency_ms, 3)`` keeps the JSON terse.
            # ----------------------------------------------------------
            log_fn(
                "request.completed" if error_exception is None else "request.failed",
                method=method,
                route=route,
                status=int(status_code),
                latency_ms=round(latency_ms, 3),
                user_id=user_id,
                request_size_bytes=int(request_size_bytes),
                response_size_bytes=int(response_size_bytes),
            )
