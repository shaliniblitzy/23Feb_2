"""Pure-ASGI correlation-ID middleware for the Product Service.

This module implements :class:`CorrelationIdMiddleware` — the **outermost**
middleware in the Product Service's middleware stack (first to see each
incoming request and last to touch each outgoing response). It guarantees
that every HTTP exchange handled by this service carries a stable
``X-Correlation-ID`` value that operators can use to stitch together log
lines, distributed traces, and Kafka event headers across the eleven-
service e-commerce fabric described in the Agent Action Plan (AAP).

Authoritative requirements implemented here
-------------------------------------------
* **AAP R-13** — Every external call must propagate a correlation ID
  (generated at the API Gateway) through to external providers where
  supported, and must be present on every log line. This middleware is
  the entry point for that propagation: when the API Gateway forwards a
  request with ``X-Correlation-ID``, we adopt it verbatim; otherwise we
  mint a fresh ID so downstream Kafka events (``product.created`` /
  ``product.updated``), structured logs, and outbound HTTP calls (JWKS
  fetch, Schema Registry REST calls) can carry a non-empty value.
* **AAP Section 0.4.5** — The Correlation-ID middleware is enumerated as
  a first-class cross-cutting interceptor that must inject or propagate
  ``X-Correlation-ID`` across HTTP calls and Kafka message headers for
  distributed tracing. While Kafka-side propagation lives in the Kafka
  producer wrapper under ``src/events/producer.py``, that module reads
  the same correlation-ID value from the :data:`_correlation_id_var`
  ContextVar exported here via :func:`get_correlation_id`.
* **AAP R-26** — All logs must be structured JSON with the
  ``correlation_id`` field present on every line. We satisfy this by
  binding the ID into structlog's ``contextvars`` once per request; the
  ``merge_contextvars`` processor (installed FIRST in the chain by
  ``src/observability/logging_config.py``) auto-merges that key into
  every emitted log record.

Design philosophy
-----------------
* **Pure ASGI, NOT** ``starlette.middleware.base.BaseHTTPMiddleware``.
  ``BaseHTTPMiddleware`` buffers the entire response body into a
  ``Message`` queue to pass it through Starlette's ``dispatch()``
  helper; that buffering is observable overhead on hot-path routes
  (``/health/live``, ``/health/ready``, ``/metrics``) and on the catalog
  GET responses which can carry hundreds of products with embedded
  variants and media URLs. Pure ASGI observes individual
  ``http.response.start`` and ``http.response.body`` events and
  forwards them through ``send()`` immediately, preserving streaming
  behavior and skipping the buffer.
* **Defense-in-depth on inbound IDs.** Inbound correlation-ID values
  are untrusted: a malicious or misconfigured client could send values
  containing whitespace, control characters, JSON quotes, or PII that
  would corrupt log lines or break downstream JSON rendering. We apply
  a strict whitelist (``^[A-Za-z0-9._\\-]+$``) plus length cap to
  reject malformed values and fall back to a freshly-generated UUID —
  protecting the integrity of the structured log output (AAP R-26)
  without dropping the request.
* **Header replacement, not append.** If an inner controller
  accidentally sets its own ``X-Correlation-ID`` in the response (e.g.
  by echoing a request header back), we REPLACE rather than append.
  Two response headers with the same name is technically valid per
  RFC 7230 but confuses many clients; one authoritative value is
  cleaner.
* **Granular unbind, not clear.** The startup lifespan hook in
  ``src/main.py`` binds ``service``, ``version``, and ``environment``
  via ``structlog.contextvars.bind_contextvars`` once at process
  start. We deliberately call ``unbind_contextvars("correlation_id")``
  in the ``finally:`` block — NOT ``clear_contextvars()`` — so those
  permanently-bound startup keys survive across requests. Clearing the
  entire bound context would leave subsequent log lines missing the
  ``service`` field, violating AAP R-26.
* **Dual-binding (ContextVar + structlog contextvars).** In addition
  to binding into structlog's contextvars (which feed log emission),
  the middleware also stores the active correlation ID in a
  module-level :data:`_correlation_id_var` ContextVar so non-logging
  consumers — notably the Kafka producer under
  ``src/events/producer.py`` and the httpx event hook in
  ``src/container.py`` — can read the value via
  :func:`get_correlation_id` without coupling to structlog's internal
  storage layout.

Cross-references
----------------
* **Consumed by**: ``services/product-service/src/main.py`` via
  ``app.add_middleware(CorrelationIdMiddleware)`` registered LAST so it
  executes OUTERMOST at runtime (FastAPI/Starlette wraps later-added
  middlewares around earlier-added ones).
* **Sibling middleware** (all authored to read the bound state):
  - ``src/middleware/logging.py`` — emits one structured log line per
    request; the ``correlation_id`` is automatically merged via the
    structlog context vars bound here.
  - ``src/middleware/jwt_auth.py`` — returns 401/403 responses whose
    JSON error body includes ``correlation_id`` (read from
    ``scope["state"]["correlation_id"]`` or :func:`get_correlation_id`).
  - ``src/middleware/error_handler.py`` — embeds ``correlation_id`` in
    every JSON error envelope.
* **Late-imported by**: ``src/container.py`` inside the httpx
  ``_correlation_hook`` (deferred to first call to break a known
  startup-order cycle between ``container``, ``middleware/jwt_auth``,
  and this module).
* **Late-imported by**: ``src/observability/logging_config.py``'s
  structlog processor that decorates every log record with the
  current correlation ID — also deferred to break a cycle, since
  ``logging_config`` is imported during package bootstrap before
  ``CorrelationIdMiddleware`` is constructed.
* **Read by Kafka plumbing**: ``src/events/producer.py`` calls
  :func:`get_correlation_id` to attach ``X-Correlation-ID`` to Kafka
  message headers on every published event, propagating the request
  context across the async boundary per AAP Section 0.4.5.

Why the public surface is shaped the way it is
----------------------------------------------
* :func:`get_correlation_id` is a function (not a direct
  ``ContextVar`` re-export) so callers cannot accidentally call
  ``.set()`` on it — only the middleware should ever set the value.
  The function also gives a stable accessor that future
  implementations could re-route (for example, to read from
  ``contextvars.copy_context()`` in a background-task helper) without
  breaking the call sites.
* Module-level constants (``CORRELATION_HEADER_NAME``,
  ``CORRELATION_SCOPE_KEY``, ``DEFAULT_SERVICE_PREFIX``,
  ``MAX_CORRELATION_ID_LENGTH``) are exported so the unit-test suite
  AND the operational tooling (e.g., the OpenAPI spec generator that
  documents the response header) can read the same values used in
  production code without re-declaring them.
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
# consumers (the Kafka producer, the httpx outbound hook) can read the
# active correlation ID without depending on structlog's private
# storage layout.
from contextvars import ContextVar

# ``re`` is used to compile and apply ``_ALLOWED_CHARS_RE`` — the
# strict whitelist regex that rejects malformed inbound correlation-ID
# values (whitespace, quotes, control characters, non-ASCII bytes).
# Compiling the regex once at module-load time amortizes the cost
# across the millions of requests this hot-path middleware will see.
import re

# ``uuid.uuid4`` produces cryptographically-random 128-bit UUIDs.
# ``uuid4`` (random) is preferred over ``uuid1`` (timestamp + MAC)
# here because the correlation ID is exposed in response headers and
# log lines visible to clients; ``uuid1`` would leak the host MAC
# address and a high-resolution wall-clock value, neither of which is
# appropriate to expose externally.
import uuid

# ``typing.Optional`` is used explicitly for the ``Optional[str]``
# return type on :func:`get_correlation_id` and the ``Optional[str]``
# parameterization of :data:`_correlation_id_var`. We keep the
# explicit ``Optional[...]`` form (rather than ``str | None``) at
# declaration sites that must communicate "value may be absent" — a
# style the schema's external-imports declaration calls out as the
# canonical idiom for this codebase.
from typing import Optional

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``starlette.types`` provides the canonical ASGI type aliases. We
# import them rather than declaring our own so the middleware's
# signatures are SOURCE-COMPATIBLE with the rest of the Starlette /
# FastAPI middleware ecosystem (e.g., ``CORSMiddleware``,
# ``Middleware``, ``add_middleware``):
#   - ``ASGIApp``  — the inner application reference (``self.app``).
#   - ``Scope``    — ASGI request scope (dict) passed to ``__call__``.
#   - ``Receive``  — async callable yielding incoming ``Message`` events.
#   - ``Send``     — async callable accepting outgoing ``Message`` events.
#   - ``Message``  — dict shape observed in the wrapping
#                    ``send_with_correlation`` closure when injecting
#                    the ``X-Correlation-ID`` response header into the
#                    ``http.response.start`` event.
# We deliberately do NOT import ``BaseHTTPMiddleware`` or any FastAPI /
# Starlette framework primitives (``Request``, ``Response``,
# ``HTTPException``); pure ASGI gives us everything we need with zero
# message-buffering overhead on hot-path routes.
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# ``structlog`` is the primary structured-logging library configured
# by ``src/observability/logging_config.py``. We import the top-level
# module (rather than ``from structlog.contextvars import ...``) so it
# is explicit at every call site that we are reaching into the
# ``contextvars`` submodule — both ``bind_contextvars`` and
# ``unbind_contextvars`` are called from this module. The
# ``merge_contextvars`` processor (installed FIRST in the structlog
# processor chain) auto-merges the ``correlation_id`` key bound here
# into every log record emitted within the request scope, satisfying
# AAP R-26 without requiring every log call site to pass the ID
# explicitly.
import structlog


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
#: Canonical lowercase byte-string spelling of the correlation header
#: name. Per the ASGI spec, header names are stored as lowercase bytes
#: in ``scope["headers"]`` regardless of the on-the-wire casing the
#: client sent (RFC 7230 §3.2 declares header names case-insensitive),
#: so comparing against the lowercase form is correct even when
#: clients emit ``X-Correlation-ID`` in mixed case. We expose this as
#: ``bytes`` (not ``str``) so the response-injection path can append
#: it directly into the ``http.response.start`` headers list — which
#: also stores tuples of ``(bytes, bytes)``.
CORRELATION_HEADER_NAME: bytes = b"x-correlation-id"

#: Name of the ASGI ``scope["state"]`` key where downstream middleware
#: and controllers read the correlation ID without re-parsing the
#: request headers. Starlette exposes this slot as
#: ``request.state.correlation_id`` via FastAPI; raw-ASGI middleware
#: reads it directly from ``scope["state"]["correlation_id"]``. The
#: sibling ``jwt_auth`` and ``error_handler`` middlewares both rely on
#: this contract for their JSON error envelopes.
CORRELATION_SCOPE_KEY: str = "correlation_id"

#: Default prefix prepended to ``uuid.uuid4()`` when minting a fresh
#: correlation ID. Matches the ``service.name`` setting declared in
#: ``services/product-service/config/default.yaml`` and the
#: structured-log ``service`` field, so logs from this service are
#: always identifiable by either the ``service`` field or the
#: correlation_id prefix.
#:
#: We hard-code the default here (rather than importing settings at
#: module load) to keep this module import-side-effect-free and
#: trivially unit-testable; ``src/main.py`` may override this default
#: by passing ``service_prefix=settings.service.name`` to
#: ``add_middleware`` if a future deployment needs to tag IDs with a
#: deployment-specific suffix.
#:
#: NOTE — Service-prefix invariant for THIS file: the value MUST be
#: ``"product-service"`` (NOT ``"notification-service"`` or any other
#: sibling service name); this is the one material divergence from
#: the notification-service reference middleware at this file level.
DEFAULT_SERVICE_PREFIX: str = "product-service"

#: Maximum permitted character length of an inbound
#: ``X-Correlation-ID`` value. 128 chars is generous enough to
#: accommodate the service prefix (``product-service-``, 16 chars)
#: plus a UUID4 hex with dashes (36 chars) plus an optional namespace
#: token, while still bounding the size of the field as it flows into
#: Elasticsearch (AAP R-29 ILM policies cap individual log fields).
#: Anything longer is treated as malformed and replaced with a
#: freshly-generated value.
MAX_CORRELATION_ID_LENGTH: int = 128

#: Whitelist regex applied to inbound correlation-ID values after
#: latin-1 decode and whitespace strip. Permits alphanumerics,
#: hyphens, underscores, and dots — the union of characters used by
#: UUID4 hex representations, hyphenated service-prefix tokens, dotted
#: namespace conventions (``svc.req.42``), and underscore-separated
#: identifiers. Deliberately EXCLUDES whitespace, quotes, control
#: characters, and unicode codepoints — all of which would either
#: break log-format round-trips (whitespace splits log fields in some
#: legacy parsers), introduce JSON-escape bugs (quotes), or expose
#: non-ASCII PII (unicode codepoints). The expression is anchored
#: with ``^...$`` so partial matches are also rejected. This is a
#: defense-in-depth measure against log-pollution attacks where an
#: attacker submits a correlation ID containing newlines or ANSI
#: escape sequences hoping to corrupt the log stream.
_ALLOWED_CHARS_RE: re.Pattern[str] = re.compile(r"^[A-Za-z0-9._\-]+$")


# ---------------------------------------------------------------------------
# Per-task correlation-ID storage (ContextVar)
# ---------------------------------------------------------------------------
#: Module-level :class:`contextvars.ContextVar` holding the correlation
#: ID for the currently-executing asyncio task. The middleware sets
#: this slot at the beginning of each HTTP request and resets it in a
#: ``finally:`` block so the next request starts clean.
#:
#: Rationale for keeping a separate ContextVar in addition to the
#: structlog contextvars binding
#: -----------------------------------------------------------------
#: * **Decoupling from structlog internals.** ``structlog.contextvars``
#:   stores bound keys in a single dict-shaped ContextVar
#:   (``structlog_contextvars``) whose layout is private to structlog.
#:   Reading the correlation ID directly from there would tie our
#:   non-logging code to structlog's internal storage representation.
#:   Exposing a dedicated ContextVar via :func:`get_correlation_id`
#:   gives downstream consumers a stable, public, typed slot that is
#:   independent of the logging library.
#: * **Non-logging consumers.** Code paths that need to read the
#:   correlation ID but do NOT emit logs include:
#:     - ``src/events/producer.py`` — Kafka producer adds the
#:       correlation ID to outbound message headers (AAP Section 0.4.5
#:       / R-13).
#:     - ``src/container.py`` — httpx event hook injects
#:       ``X-Correlation-ID`` on every outbound HTTP call (AAP R-13).
#:     - ``src/middleware/error_handler.py`` — embeds the ID in JSON
#:       error envelopes returned to clients.
#:     - ``src/middleware/jwt_auth.py`` — embeds the ID in 401 / 403
#:       error envelopes.
#: * **Asyncio-safety.** :class:`contextvars.ContextVar` is isolated
#:   per ``asyncio.Task``, so concurrent in-flight requests cannot
#:   read or overwrite each other's correlation IDs. The same
#:   per-task isolation holds when uvicorn runs with multiple workers,
#:   multiple event-loop tasks per worker, or async test harnesses
#:   driving the middleware directly.
#:
#: Default value
#: -------------
#: ``default=None`` is intentional. A consumer that reads the var
#: outside any active request context (e.g., during process startup,
#: from a background task spawned without ``contextvars.copy_context``)
#: gets ``None`` and can decide whether to mint a placeholder, log
#: without the field, or skip entirely. A non-None default would mask
#: such bugs by always returning a stale value.
#:
#: The leading-underscore name marks the ContextVar as
#: module-private — callers should reach the value via the public
#: :func:`get_correlation_id` accessor, NEVER by importing the var
#: directly. This convention prevents downstream code from calling
#: ``.set()`` on the var and accidentally bypassing the middleware's
#: lifecycle management (token capture / reset).
_correlation_id_var: ContextVar[Optional[str]] = ContextVar(
    "correlation_id", default=None
)


# ---------------------------------------------------------------------------
# Public API — module symbols
# ---------------------------------------------------------------------------
#: ``__all__`` declares the public symbols exported by this module that
#: participate in ``from src.middleware.correlation_id import *``.
#:
#: Keeping the list explicit (rather than relying on the leading-
#: underscore convention to hide private names) gives static analyzers
#: and the IDE's auto-completion an unambiguous view of the module
#: surface. The order is alphabetical for readability.
__all__: list[str] = [
    "CORRELATION_HEADER_NAME",
    "CORRELATION_SCOPE_KEY",
    "CorrelationIdMiddleware",
    "DEFAULT_SERVICE_PREFIX",
    "MAX_CORRELATION_ID_LENGTH",
    "get_correlation_id",
]


# ---------------------------------------------------------------------------
# Public accessor function
# ---------------------------------------------------------------------------
def get_correlation_id() -> Optional[str]:
    """Return the current request's correlation ID (or ``None`` outside a request).

    This is the canonical accessor used by:

    * ``src.container._build_http_client._correlation_hook`` —
      late-imports this function inside the request hook to avoid a
      circular import at module load time. Injects ``X-Correlation-ID``
      on outbound HTTP calls (AAP R-13).
    * ``src.events.producer`` — reads the value to inject into Kafka
      message headers, enabling distributed tracing across async
      events (AAP Section 0.4.5).
    * ``src.observability.logging_config`` — structlog processor that
      attaches the value to every log line emitted in the current
      async task or its descendants (AAP R-26).

    The function is a thin wrapper around :meth:`ContextVar.get`; it is
    intentionally NOT raising when the value is ``None`` so callers
    can decide whether to fall back to ``uuid.uuid4().hex`` (typical
    for background tasks that lack a triggering request) or to
    short-circuit (typical for outbound hooks that simply omit the
    header when the value is unavailable).

    Returns:
        The current correlation ID (a non-empty string conforming to
        :data:`_ALLOWED_CHARS_RE` and at most
        :data:`MAX_CORRELATION_ID_LENGTH` chars long), or ``None`` when
        called outside an HTTP request scope (e.g., application
        startup, shutdown, background tasks where no
        :class:`CorrelationIdMiddleware` has run).
    """
    return _correlation_id_var.get()


# ---------------------------------------------------------------------------
# Middleware class
# ---------------------------------------------------------------------------
class CorrelationIdMiddleware:
    """Pure-ASGI middleware that injects or propagates ``X-Correlation-ID``.

    Position in the middleware stack: OUTERMOST. Registered LAST in
    ``main.create_app`` so that Starlette's inside-out wrapping makes
    it the outermost layer at runtime.

    Responsibilities (AAP R-13 / AAP Section 0.4.5)
    -----------------------------------------------
    * **Extract or generate** the request correlation ID:
      - If the inbound HTTP request carries a valid
        ``X-Correlation-ID`` header (case-insensitive per RFC 7230),
        adopt that value verbatim after sanitization.
      - Otherwise, mint a fresh value in the form
        ``<service-prefix>-<uuid4>``, e.g.
        ``product-service-550e8400-e29b-41d4-a716-446655440000``.

    * **Bind** the value into the structlog context
      (:func:`structlog.contextvars.bind_contextvars`) so every log
      line emitted within the request scope carries
      ``correlation_id=<value>`` — satisfying AAP R-26.

    * **Set** the value on :data:`_correlation_id_var` so non-logging
      consumers (Kafka producer, httpx outbound hook, error
      formatters) can read the ID via :func:`get_correlation_id`
      without depending on structlog's private contextvars layout —
      satisfying AAP Section 0.4.5 propagation to Kafka message
      headers and outbound HTTP calls.

    * **Expose** the value on ``scope["state"]["correlation_id"]`` for
      downstream middleware (``jwt_auth``, ``error_handler``,
      ``logging``) and FastAPI controllers to consume without
      re-parsing the request headers.

    * **Append** ``X-Correlation-ID`` to the outgoing HTTP response
      headers so callers can correlate their client-side logs with
      our server-side logs end-to-end.

    Lifecycle of a single HTTP request
    ----------------------------------
    1. ``__call__(scope, receive, send)`` is invoked by the ASGI server
       (uvicorn) for each new HTTP exchange.
    2. Non-HTTP scopes (``lifespan``, ``websocket``) short-circuit to
       the inner app; correlation IDs are an HTTP concept here and
       binding them at lifespan would produce misleading log lines at
       process-startup / shutdown.
    3. For HTTP scopes, the middleware extracts an inbound ID
       (:meth:`_read_header`) or generates a fresh one
       (:meth:`_generate_id`).
    4. The ID is stashed on ``scope["state"]``, set on the
       :data:`_correlation_id_var` ContextVar (capturing the reset
       :class:`contextvars.Token`), and bound to the structlog
       context vars.
    5. The inner app is invoked with a wrapped ``send`` callable
       (``send_with_correlation``) that appends ``X-Correlation-ID``
       to the ``http.response.start`` event's headers list — replacing
       any value the inner app may have set, to keep our value
       authoritative.
    6. The structlog context binding for ``correlation_id`` is
       removed and the :data:`_correlation_id_var` ContextVar is
       reset using the captured Token in a ``finally:`` block so the
       next request starts clean. Other startup-bound keys
       (``service``, ``version``, ``environment``) are preserved.

    Header invasion safety
    ----------------------
    The validation regex disallows control characters, whitespace,
    and non-ASCII codepoints. This prevents log injection attacks
    where an attacker submits a correlation ID containing newlines or
    ANSI escape sequences hoping to corrupt the log stream.

    Echo policy
    -----------
    The middleware always SETS ``X-Correlation-ID`` on the response,
    REPLACING any value the inner app emits. This guarantees the
    response header value always matches the value used internally
    for logs and outbound calls.

    Rationale for pure-ASGI (vs. ``BaseHTTPMiddleware``)
    ----------------------------------------------------
    This middleware runs on EVERY request including ``/health/live``,
    ``/health/ready``, and ``/metrics``.
    ``starlette.middleware.base.BaseHTTPMiddleware`` buffers the
    entire response body into a ``Message`` queue to pass it through
    its ``dispatch()`` callback; that buffering is observable
    overhead on high-traffic probe endpoints and on the catalog GET
    responses (which can carry hundreds of products with embedded
    variants and media URLs). Pure ASGI (implementing
    ``__call__(scope, receive, send)`` directly) avoids the buffer
    entirely.

    Args:
        app: The inner ASGI application (FastAPI / Starlette instance
            or the next middleware in the chain). Stored as
            ``self.app`` and invoked unchanged for non-HTTP scopes,
            or with the wrapped ``send_with_correlation`` for HTTP
            scopes.
        service_prefix: Prefix prepended to the random UUID4 when
            minting a fresh correlation ID. Defaults to
            :data:`DEFAULT_SERVICE_PREFIX` (``"product-service"``).
            ``src/main.py`` may override this with
            ``settings.service.name`` so the value stays consistent
            with the service identity declared in
            ``services/product-service/config/default.yaml``.
        header_name: Lowercase ``bytes`` form of the correlation
            header name. Defaults to :data:`CORRELATION_HEADER_NAME`
            (``b"x-correlation-id"``). Exposed primarily for tests
            that need to verify case-insensitive matching against an
            alternate header spelling without re-mocking ASGI.

    Example:
        >>> from fastapi import FastAPI
        >>> from src.middleware.correlation_id import CorrelationIdMiddleware
        >>> app = FastAPI()
        >>> # Register LAST so this middleware is OUTERMOST at runtime
        >>> # (Starlette wraps later-added middlewares around earlier
        >>> # ones).
        >>> app.add_middleware(CorrelationIdMiddleware)
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def __init__(
        self,
        app: ASGIApp,
        *,
        service_prefix: str = DEFAULT_SERVICE_PREFIX,
        header_name: bytes = CORRELATION_HEADER_NAME,
    ) -> None:
        """Initialize the middleware.

        Args:
            app: The inner ASGI application to wrap.
            service_prefix: Prefix used when generating a fresh
                correlation ID. Should match the service's logical
                name so operators can tell the originating service
                from the ID alone. Keyword-only to make call sites
                self-documenting (``CorrelationIdMiddleware(app,
                service_prefix="product-service")``). Defaults to
                ``"product-service"``.
            header_name: ``bytes`` form of the correlation header
                name. Will be normalized to lowercase exactly once
                here so the per-request comparison stays a cheap
                ``bytes == bytes`` check. Keyword-only for the same
                self-documentation reason.
        """
        # Hold the inner app reference verbatim. We do not modify or
        # re-wrap it at construction time; the wrapping happens
        # per-request in ``__call__`` via the ``send_with_correlation``
        # closure. Storing the reference once here keeps the
        # per-request overhead at a single attribute lookup
        # (``self.app``) rather than re-resolving the app from a
        # registry. The attribute name ``app`` (without underscore
        # prefix) matches the canonical Starlette middleware
        # convention and what ``add_middleware`` callers expect to
        # see.
        self.app: ASGIApp = app

        # Service prefix is stored as-is; ``_generate_id`` formats it
        # into the ``<prefix>-<uuid>`` shape on each new-ID path. We
        # deliberately do NOT validate or strip this string — the
        # caller (``src/main.py``) is responsible for passing a
        # well-formed value. Validating here would require coupling
        # to ``_ALLOWED_CHARS_RE`` and complicate the constructor;
        # invalid prefixes manifest as malformed correlation IDs
        # downstream and are caught in integration tests.
        self._service_prefix: str = service_prefix

        # Normalize the header name to lowercase bytes EXACTLY ONCE
        # here so the per-request comparison in ``_read_header`` and
        # ``send_with_correlation`` stays a cheap ``bytes == bytes``
        # check. ASGI canonicalizes header names to lowercase, but
        # defensive normalization protects against caller mistakes
        # (e.g., passing ``b"X-Correlation-ID"`` from a misconfigured
        # deployment or a unit-test stub).
        self._header_name: bytes = header_name.lower()

    # ------------------------------------------------------------------
    # ASGI entry point
    # ------------------------------------------------------------------
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """ASGI entry point — invoked by the ASGI server on each event.

        Dispatch
        --------
        * **Non-HTTP scopes** (``lifespan``, ``websocket``) — pass
          through to the inner app untouched. Correlation IDs are an
          HTTP concept in this service; binding them at lifespan
          would produce misleading log lines at process startup /
          shutdown, and websockets do not have request / response
          semantics that map cleanly onto a single ID.
        * **HTTP scopes** — extract or generate a correlation ID,
          stash it on ``scope["state"]``, set
          :data:`_correlation_id_var`, bind it into structlog
          context vars, wrap ``send`` with the header injector, and
          invoke the inner app in a ``try / finally`` block that
          guarantees cleanup of all bound context vars.

        Exception propagation
        ---------------------
        Any exception raised by the inner app propagates upward
        unchanged — this middleware has no business catching errors
        (that is ``error_handler``'s job). The ``finally:`` block
        still runs and unbinds the ``correlation_id`` context var,
        so an exception path does not leave a stale ID bound for
        subsequent requests.

        Args:
            scope: The ASGI scope dict.
            receive: The ASGI receive callable; passed through
                unchanged.
            send: The ASGI send callable; replaced with
                ``send_with_correlation`` for HTTP scopes so the
                outbound response carries our ``X-Correlation-ID``
                header.
        """
        # --------------------------------------------------------------
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
        # --------------------------------------------------------------
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # --------------------------------------------------------------
        # 1) Resolve the effective correlation ID for this request.
        #
        # ``_resolve_correlation_id`` returns the validated inbound ID
        # if present and well-formed, or a freshly-generated ID
        # otherwise. The request is never rejected on the basis of a
        # malformed correlation ID — rejecting would break clients
        # that send well-formed values from the API Gateway and
        # accidentally trip our whitelist if we ever tightened it.
        # --------------------------------------------------------------
        correlation_id: str = self._resolve_correlation_id(scope)

        # --------------------------------------------------------------
        # 2) Stash on ``scope["state"]`` so downstream middleware and
        #    controllers can read it without re-parsing headers.
        #
        # ``scope.setdefault("state", {})`` creates the slot if absent
        # — Starlette normally initializes it for HTTP scopes, but
        # raw-ASGI tests may pass a minimal scope that lacks the key
        # (or, in a misuse case, sets it to a non-dict value). The
        # defensive ``isinstance`` check guards against that case.
        # ``setdefault`` is idempotent: if ``state`` already exists
        # AND is a dict, it is returned as-is, so we preserve any
        # keys upstream middleware may have placed there. (No
        # upstream middleware exists in our stack since we are the
        # OUTERMOST, but this remains correct behavior if the
        # registration order ever changes.)
        # --------------------------------------------------------------
        state = scope.setdefault("state", {})
        if not isinstance(state, dict):
            state = {}
            scope["state"] = state
        state[CORRELATION_SCOPE_KEY] = correlation_id

        # --------------------------------------------------------------
        # 3) Set on the module-level :data:`_correlation_id_var`
        #    ContextVar so non-logging consumers (Kafka producer
        #    under ``src/events/producer.py``, the httpx outbound
        #    hook in ``src/container.py``, error formatters) can
        #    read the active correlation ID via
        #    :func:`get_correlation_id` without reaching into
        #    structlog's private contextvars storage or re-parsing
        #    the ASGI scope.
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
        # --------------------------------------------------------------
        token = _correlation_id_var.set(correlation_id)

        # --------------------------------------------------------------
        # 4) Bind to structlog contextvars so every log line within
        #    this request scope carries ``correlation_id=<value>``.
        #
        # ``structlog.contextvars.bind_contextvars`` writes to
        # ``contextvars.ContextVar`` slots which are isolated per
        # ``asyncio.Task``, so concurrent requests do not interfere
        # with each other's bindings. The ``merge_contextvars``
        # processor (installed FIRST in the structlog chain by
        # ``src/observability/logging_config.py``) auto-merges the
        # bound value into every emitted log record without
        # requiring call sites to re-pass it.
        #
        # Why bind here as well as in step 3:
        # The structlog binding feeds the log emission path. The
        # :data:`_correlation_id_var` set in step 3 feeds non-logging
        # code paths. Maintaining both decouples our non-logging code
        # (Kafka producer, httpx outbound hook, error formatters)
        # from structlog's internal storage layout — which is private
        # to structlog and could change between minor releases.
        # --------------------------------------------------------------
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

        # --------------------------------------------------------------
        # 5) Build the ``send_with_correlation`` closure that injects
        #    our ``X-Correlation-ID`` header into the outbound
        #    ``http.response.start`` message.
        #
        # We capture ``self._header_name`` and ``correlation_id`` by
        # closure rather than re-deriving them per call to keep the
        # hot path tight. The closure is rebuilt per request
        # (necessary because ``correlation_id`` changes per request)
        # but contains no per-event allocation other than the new
        # ``message`` dict and the ``headers`` list copy.
        # --------------------------------------------------------------
        async def send_with_correlation(message: Message) -> None:
            """Inject ``X-Correlation-ID`` into the outbound response.

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
                # Filter out any pre-existing ``X-Correlation-ID``
                # headers the inner app may have set — our value is
                # AUTHORITATIVE. This handles the (rare but
                # observable) case where a controller manually echoes
                # a request header into the response, which would
                # otherwise produce two ``X-Correlation-ID`` headers
                # in the wire response. RFC 7230 permits duplicates
                # for list-grammar fields but ``X-Correlation-ID`` is
                # scalar; one authoritative value is cleaner.
                #
                # The ``.lower()`` defensively normalizes the inner
                # app's header name in case it uses mixed case
                # (the ASGI spec mandates lowercase, but we don't
                # want unit tests or third-party middleware to crash
                # on a technicality).
                headers: list[tuple[bytes, bytes]] = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower() != self._header_name
                ]

                # Append our header. ``encode("ascii")`` is safe here
                # because ``correlation_id`` has either passed the
                # whitelist regex (ASCII-only by construction) or was
                # generated from a service prefix (ASCII) plus
                # ``str(uuid4)`` (hex + hyphens, also ASCII). Append
                # (not prepend) keeps the rest of the headers in the
                # order the inner app emitted them — preserving any
                # ordering invariants downstream middleware or
                # observers may depend on (e.g., security headers
                # set early).
                headers.append((self._header_name, correlation_id.encode("ascii")))

                # Build a new message dict with the patched headers
                # rather than mutating ``message`` in place. The
                # ``{**message, "headers": headers}`` idiom creates a
                # shallow copy — the inner app's references stay
                # untouched — and is the canonical ASGI-message-
                # mutation pattern used throughout Starlette /
                # FastAPI internals.
                message = {**message, "headers": headers}

            # Forward the (possibly-patched) message to the
            # underlying send. The await yields control to the ASGI
            # server which writes bytes onto the wire and resumes
            # our coroutine when ready.
            await send(message)

        # --------------------------------------------------------------
        # 6) Invoke the inner app inside a ``try / finally`` block.
        #
        # The ``finally:`` block runs on EVERY exit path — successful
        # response, observed 4xx/5xx, raised exception, even
        # ``CancelledError`` from a client disconnect. This
        # guarantees the bound ``correlation_id`` is always cleared
        # so the next request seen by this task starts with a clean
        # context. (Without this, a long-running asyncio task pool
        # would accumulate stale-ID bleed-through.)
        #
        # We deliberately do NOT catch the exception here — that is
        # the responsibility of the ``error_handler`` middleware. Any
        # exception propagates up and out, while ``finally`` still
        # cleans the bound context vars (both the structlog binding
        # and the :data:`_correlation_id_var` ContextVar).
        # --------------------------------------------------------------
        try:
            await self.app(scope, receive, send_with_correlation)
        finally:
            # ----- Cleanup step A ------------------------------------
            # Unbind ONLY the ``correlation_id`` key from structlog's
            # contextvars store, NOT the entire bound context. The
            # startup hook in ``src/main.py`` binds ``service``,
            # ``version``, and ``environment`` once per process via
            # ``bind_contextvars``; calling ``clear_contextvars()``
            # here would also drop those keys, leaving subsequent log
            # lines missing the ``service`` field — a direct AAP R-26
            # violation because every log line is required to carry
            # ``service``.
            structlog.contextvars.unbind_contextvars(CORRELATION_SCOPE_KEY)

            # ----- Cleanup step B ------------------------------------
            # Reset the :data:`_correlation_id_var` ContextVar to the
            # value it held before ``__call__`` ran. For a request
            # entering at the OUTERMOST middleware that is the
            # ``default=None`` slot, but using the ``reset(token)``
            # idiom (rather than ``set(None)``) future-proofs the
            # cleanup against the (currently impossible) case of
            # nested ``set`` calls and matches the canonical
            # ContextVar usage pattern documented in PEP 567.
            _correlation_id_var.reset(token)

    # ------------------------------------------------------------------
    # Helpers (private)
    # ------------------------------------------------------------------
    def _resolve_correlation_id(self, scope: Scope) -> str:
        """Return the canonical correlation ID for the current request.

        Reads the inbound header; if absent or invalid, returns a
        fresh ``f"{service_prefix}-{uuid.uuid4()}"`` value.

        Args:
            scope: The ASGI scope dict for the current HTTP request.

        Returns:
            A non-empty string conforming to :data:`_ALLOWED_CHARS_RE`
            and at most :data:`MAX_CORRELATION_ID_LENGTH` characters
            in length. Never returns ``None`` — the fall-back to
            :meth:`_generate_id` ensures every request has a valid
            correlation ID.
        """
        inbound = self._read_header(scope)
        if inbound is not None and self._is_valid(inbound):
            return inbound
        return self._generate_id()

    def _read_header(self, scope: Scope) -> Optional[str]:
        """Read the ``X-Correlation-ID`` header (case-insensitive).

        ASGI delivers headers as a list of ``(name_bytes, value_bytes)``
        tuples in ``scope["headers"]``, with header names canonicalized
        to lowercase. We iterate looking for an exact match on the
        lowercase header name (defensive ``.lower()`` is also applied
        on each element to support stubs / mocks that may pass
        mixed-case headers in unit tests).

        Header value decoding uses ``latin-1`` per RFC 7230 §3.2.4:
        HTTP/1.1 historically permits ISO-8859-1 (latin-1) in header
        values for backwards compatibility with legacy proxies. A
        ``utf-8`` decode would raise on bytes ``>= 0x80`` even though
        such bytes are technically valid on the wire. The whitelist
        regex applied later via :meth:`_is_valid` rejects any
        non-ASCII characters anyway, so latin-1 decode is a permissive
        gate that defers strict validation to the regex step.

        Multiple header occurrences
        ---------------------------
        RFC 7230 §3.2.2 permits multiple occurrences of the same
        header name only for fields whose grammar explicitly defines a
        comma-separated list. ``X-Correlation-ID`` is a custom scalar
        header; multiple occurrences are a client misuse. We accept
        the FIRST match and ignore the rest — this mirrors typical
        proxy behavior and avoids any ambiguity about which value to
        bind.

        Args:
            scope: The ASGI request scope dict.

        Returns:
            The raw (post-decode, post-strip) header value as a
            string, or ``None`` if the header is missing, the
            ``headers`` slot is malformed (not a list), or the value
            failed to decode. The caller (:meth:`_resolve_correlation_id`)
            applies the whitelist regex / length cap separately via
            :meth:`_is_valid` so this method's job is purely to
            extract the raw candidate string.
        """
        # ``Scope`` is a ``MutableMapping[str, Any]`` per the
        # Starlette ASGI typing, so ``scope.get(...)`` returns ``Any``.
        # We narrow it locally to the canonical ASGI shape — a list of
        # ``(bytes, bytes)`` tuples — both for runtime safety (the
        # ``isinstance`` check) and for mypy --strict typing on the
        # decoded return value below.
        raw_headers = scope.get("headers", [])

        # Defensive type check: ASGI guarantees ``headers`` is a list,
        # but raw-ASGI unit-test scopes occasionally pass a tuple,
        # generator, or ``None``. Returning ``None`` for non-list
        # values is more robust than raising.
        if not isinstance(raw_headers, list):
            return None

        # Bind a typed alias so mypy --strict can infer ``bytes`` for
        # the iteration tuple and verify the return type of
        # ``value.decode("latin-1")`` is ``str`` (not ``Any``).
        headers: list[tuple[bytes, bytes]] = raw_headers

        for name, value in headers:
            # ``.lower()`` defensively in case a unit-test stub passes
            # mixed-case header names (the ASGI spec mandates
            # lowercase, but we don't want unit tests to crash on a
            # technicality).
            if name.lower() == self._header_name:
                try:
                    # ``latin-1`` decode is the RFC 7230 §3.2.4
                    # contract for HTTP/1.1 header values. The
                    # ``.strip()`` removes leading / trailing
                    # whitespace some misbehaving proxies emit; the
                    # whitelist regex applied in :meth:`_is_valid`
                    # later will then either accept the cleaned value
                    # or reject it as malformed.
                    decoded: str = value.decode("latin-1")
                    return decoded.strip()
                except (UnicodeDecodeError, AttributeError):
                    # Non-decodable bytes or a non-bytes ``value``
                    # (the latter only possible from a misbehaving
                    # test stub). Either way, treat as malformed and
                    # fall through to ``None`` — the caller will
                    # mint a fresh ID. We do NOT continue scanning
                    # for another occurrence: a single malformed
                    # value taints the whole header line.
                    return None

        # No matching header found in the entire list — the inbound
        # request did not carry an ``X-Correlation-ID``.
        return None

    @staticmethod
    def _is_valid(value: str) -> bool:
        """Return ``True`` if ``value`` is a well-formed correlation ID.

        Validation pipeline (defense-in-depth)
        --------------------------------------
        1. **Non-empty check.** An empty string (or whitespace-only
           value the caller already stripped) is rejected as the
           "absent" case.
        2. **Length cap.** Values longer than
           :data:`MAX_CORRELATION_ID_LENGTH` (128 chars) are rejected
           to bound downstream Elasticsearch field sizes (AAP R-29).
        3. **Whitelist regex.** :data:`_ALLOWED_CHARS_RE` rejects
           values containing whitespace, quotes, control characters,
           or any character outside the alphanumeric-plus-``._-`` set
           — protecting log-line integrity (AAP R-26) and JSON
           rendering downstream.

        Args:
            value: The candidate correlation-ID string (already
                latin-1-decoded and whitespace-stripped by the
                caller).

        Returns:
            ``True`` if ``value`` passes all three validation gates,
            ``False`` otherwise. ``False`` signals to the caller that
            a fresh ID must be generated via :meth:`_generate_id`.
        """
        if not value:
            return False
        if len(value) > MAX_CORRELATION_ID_LENGTH:
            return False
        # ``bool(...)`` makes the return type narrowing explicit; the
        # ``Match`` object returned by ``re.match`` is truthy on hit
        # and ``None`` on miss, but mypy --strict prefers an
        # explicit cast to bool here.
        return bool(_ALLOWED_CHARS_RE.match(value))

    def _generate_id(self) -> str:
        """Generate a fresh correlation ID prefixed by the service name.

        Format: ``<service-prefix>-<uuid4>``, e.g.,
        ``product-service-550e8400-e29b-41d4-a716-446655440000``.

        The service prefix makes the originating service immediately
        recognizable from the ID alone — useful when correlation IDs
        propagate across services and operators see them in logs from
        any of the eleven microservices in the e-commerce platform.

        Why ``uuid.uuid4`` (random) and not ``uuid1`` (timestamp+MAC):
        ``uuid1`` would leak the host MAC address and a high-resolution
        wall-clock value in the ID — both of which would be exposed in
        client-visible response headers. ``uuid4`` produces 122 bits
        of cryptographic randomness with no observable host
        information, satisfying both privacy and the "globally unique"
        property a correlation ID needs.

        Returns:
            A new correlation ID in the form
            ``"<service_prefix>-<uuid4>"``. The result is guaranteed
            to match :data:`_ALLOWED_CHARS_RE` because the service
            prefix is ASCII alphanumeric-plus-hyphen and
            ``str(uuid4)`` emits hexadecimal-plus-hyphen, both of
            which are subsets of the whitelist character class.
        """
        # ``str(uuid.uuid4())`` produces a 36-character lowercase
        # hex-with-hyphens representation (e.g.,
        # ``"550e8400-e29b-41d4-a716-446655440000"``). Concatenating
        # with the service prefix and a separating hyphen yields a
        # well-formed ID that round-trips cleanly through the
        # whitelist regex. ``uuid.uuid4()`` returns a ``UUID`` object;
        # the f-string interpolation calls ``__str__`` implicitly,
        # which is the canonical Python idiom and avoids the small
        # overhead of an explicit ``str(...)`` call.
        return f"{self._service_prefix}-{uuid.uuid4()}"
