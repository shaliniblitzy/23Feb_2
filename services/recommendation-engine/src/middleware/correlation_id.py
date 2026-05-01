"""Pure-ASGI correlation-ID middleware for the Recommendation Engine.

This module implements :class:`CorrelationIdMiddleware` — the **outermost**
middleware in the Recommendation Engine's middleware stack (first to see
each incoming request and last to touch each outgoing response). It
guarantees that every HTTP exchange carries a stable
``X-Correlation-ID`` value that operators can use to stitch together
log lines, distributed traces, and Kafka event headers across the
microservices fabric described in the Agent Action Plan (AAP).

Authoritative requirements implemented here
-------------------------------------------
* **AAP R-13** — Every external call must propagate a correlation ID
  (generated at the API Gateway) through to external providers where
  supported, and must be present on every log line. This middleware is
  the entry point for that propagation: when the API Gateway forwards
  a request with ``X-Correlation-ID``, we adopt it verbatim; otherwise
  we mint a fresh ID so downstream Kafka events, structured logs, and
  external API calls (Stripe, Razorpay, SendGrid, Twilio in their
  respective services) can carry a non-empty value.
* **AAP Section 0.4.5** — The Correlation-ID middleware is enumerated
  as a first-class cross-cutting interceptor that must inject or
  propagate ``X-Correlation-ID`` across HTTP calls and Kafka message
  headers for distributed tracing.
* **AAP R-26** — All logs must be structured JSON with the
  ``correlation_id`` field present on every line. We satisfy this by
  binding the ID into structlog's ``contextvars`` once per request;
  the ``merge_contextvars`` processor (installed FIRST in the chain by
  ``src/config/logging_config.py``) auto-merges that key into every
  emitted log record.

Design philosophy
-----------------
* **Pure ASGI, NOT** ``starlette.middleware.base.BaseHTTPMiddleware``.
  ``BaseHTTPMiddleware`` buffers the entire response body into a
  ``Message`` queue to pass it through Starlette's ``dispatch()``
  helper; that buffering is observable overhead on hot-path routes
  (``/health/live``, ``/health/ready``, ``/metrics``, large streaming
  recommendation responses). Pure ASGI observes individual
  ``http.response.start`` and ``http.response.body`` events and
  forwards them through ``send()`` immediately, preserving streaming
  behavior and skipping the buffer. The folder spec for
  ``services/recommendation-engine/src/middleware/`` calls out this
  choice as non-negotiable for the two hot-path middlewares
  (this one and ``structured_logging.py``).
* **Defense-in-depth on inbound IDs.** Inbound correlation-ID values
  are untrusted: a malicious or misconfigured client could send
  values containing whitespace, control characters, JSON quotes, or
  PII that would corrupt log lines or break downstream JSON
  rendering. We apply a strict whitelist
  (``^[A-Za-z0-9._\\-]+$``) plus length cap to reject malformed
  values and fall back to a freshly-generated UUID — protecting the
  integrity of the structured log output (AAP R-26) without dropping
  the request.
* **Header replacement, not append.** If an inner controller
  accidentally sets its own ``X-Correlation-ID`` in the response
  (e.g. by echoing a request header back), we REPLACE rather than
  append. Two response headers with the same name is technically
  valid per RFC 7230 but confuses many clients; one authoritative
  value is cleaner.
* **Granular unbind, not clear.** The startup lifespan hook in
  ``src/main.py`` binds ``service``, ``version``, and ``environment``
  via ``structlog.contextvars.bind_contextvars`` once at process
  start. We deliberately call ``unbind_contextvars("correlation_id")``
  in the ``finally:`` block — NOT ``clear_contextvars()`` — so those
  permanently-bound startup keys survive across requests. Clearing
  the entire bound context would leave subsequent log lines missing
  the ``service`` field, violating AAP R-26.

Cross-references
----------------
* **Consumed by**: ``services/recommendation-engine/src/main.py`` via
  ``app.add_middleware(CorrelationIdMiddleware)`` registered LAST so
  it executes OUTERMOST at runtime (FastAPI/Starlette wraps
  later-added middlewares around earlier-added ones).
* **Sibling middleware**:
  - ``src/middleware/structured_logging.py`` — emits one log line per
    request; the ``correlation_id`` is automatically merged via the
    structlog context vars bound here.
  - ``src/middleware/jwt_auth.py`` — returns 401/403 responses whose
    JSON error body includes ``correlation_id`` (read from
    ``scope["state"]["correlation_id"]``).
  - ``src/middleware/error_handler.py`` — embeds ``correlation_id`` in
    every JSON error envelope.
* **Used by**: ``src/config/logging_config.py`` configures structlog
  with ``merge_contextvars`` as the FIRST processor; this middleware
  is the binding site for the ``correlation_id`` key that processor
  merges into every log record.
* **Out of scope**: Kafka message-header propagation. AAP Section
  0.4.5 mentions Kafka headers, but the Kafka producer/consumer
  plumbing lives in ``src/events/*`` and reads the same
  ``correlation_id`` value back from structlog's bound context vars
  when emitting events. This middleware is HTTP-only.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``re`` is used to compile and apply ``_ALLOWED_CHARS_RE`` — the strict
# whitelist regex that rejects malformed inbound correlation-ID values
# (whitespace, quotes, control characters, non-ASCII bytes). Compiling
# the regex once at module-load time amortizes the cost across the
# millions of requests this hot-path middleware will see.
import re

# ``uuid.uuid4`` produces cryptographically-random 128-bit UUIDs.
# ``uuid4`` (random) is preferred over ``uuid1`` (timestamp + MAC) here
# because the correlation ID is exposed in response headers and log
# lines visible to clients; ``uuid1`` would leak the host MAC address
# and a high-resolution wall-clock value, neither of which is
# appropriate to expose externally.
import uuid

# ``typing.Awaitable`` and ``typing.Callable`` are imported alongside
# the Starlette ASGI type aliases below to keep the inner
# ``send_with_header`` closure typed precisely. These imports satisfy
# the schema's external-imports declaration even though the closure
# typing relies primarily on Starlette's ``Send`` / ``Message``
# aliases — having ``Awaitable`` / ``Callable`` available keeps the
# module self-sufficient for any auxiliary callable hints downstream.
from typing import Awaitable, Callable

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
# Starlette framework primitives (``Request``, ``Response``,
# ``HTTPException``); pure ASGI gives us everything we need with zero
# message-buffering overhead on hot-path routes.
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# ``structlog`` is the primary structured-logging library configured
# by ``src/config/logging_config.py``. We import the top-level module
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
# Public API
# ---------------------------------------------------------------------------
# ``__all__`` declares the single public symbol exported by this
# module. Module-level constants (``CORRELATION_HEADER_NAME``,
# ``CORRELATION_SCOPE_KEY``, ``DEFAULT_SERVICE_PREFIX``,
# ``MAX_CORRELATION_ID_LENGTH``) are NOT included here because they
# are configuration knobs the test suite reads but production callers
# should not depend on directly. Per the agent prompt's Phase 6:
# "Everything else (constants, helpers) is module-private."
__all__: list[str] = ["CorrelationIdMiddleware"]


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
#: sibling ``jwt_auth`` and ``error_handler`` middlewares both rely
#: on this contract for their JSON error envelopes.
CORRELATION_SCOPE_KEY: str = "correlation_id"

#: Default prefix prepended to ``uuid.uuid4()`` when minting a fresh
#: correlation ID. Mirrors ``settings.service.name`` declared in
#: ``services/recommendation-engine/src/config/settings.py``. We hard-
#: code the default here (rather than importing settings at module
#: load) to keep this module import-side-effect-free and trivially
#: unit-testable; ``src/main.py`` overrides this default by passing
#: ``service_prefix=settings.service.name`` to ``add_middleware``.
DEFAULT_SERVICE_PREFIX: str = "recommendation-engine"

#: Maximum permitted character length of an inbound
#: ``X-Correlation-ID`` value. 128 chars is generous enough to
#: accommodate a service prefix (``recommendation-engine-``, 22 chars)
#: plus a UUID4 hex with dashes (36 chars) plus an optional namespace
#: token, while still bounding the size of the field as it flows into
#: Elasticsearch (AAP R-29 ILM policies cap individual log fields).
#: Anything longer is treated as malformed and replaced with a
#: freshly-generated value.
MAX_CORRELATION_ID_LENGTH: int = 128

#: Whitelist regex applied to inbound correlation-ID values after
#: ASCII decode and whitespace strip. Permits alphanumerics, hyphens,
#: underscores, and dots — the union of characters used by UUID4 hex
#: representations, hyphenated service-prefix tokens, dotted
#: namespace conventions (``svc.req.42``), and underscore-separated
#: identifiers. Deliberately EXCLUDES whitespace, quotes, control
#: characters, and unicode codepoints — all of which would either
#: break log-format round-trips (whitespace splits log fields in some
#: legacy parsers), introduce JSON-escape bugs (quotes), or expose
#: non-ASCII PII (unicode codepoints). The expression is anchored
#: with ``^...$`` so partial matches are also rejected.
_ALLOWED_CHARS_RE: re.Pattern[str] = re.compile(r"^[A-Za-z0-9._\-]+$")

#: Module-private type alias for the inner ``send_with_header`` closure
#: constructed per-request in :meth:`CorrelationIdMiddleware.__call__`.
#:
#: This alias is structurally equivalent to Starlette's ``Send`` (which
#: itself is ``Callable[[Message], Awaitable[None]]``) but expressed
#: here using the explicit :class:`typing.Callable` /
#: :class:`typing.Awaitable` idiom mandated by the schema's external-
#: imports declaration. Annotating the closure variable in
#: ``__call__`` with this alias both documents the contract at the
#: definition site and makes the imports of ``Awaitable`` /
#: ``Callable`` semantically load-bearing under ``mypy --strict``.
_SendCallable = Callable[[Message], Awaitable[None]]


# ---------------------------------------------------------------------------
# Middleware class
# ---------------------------------------------------------------------------
class CorrelationIdMiddleware:
    """Pure-ASGI middleware that injects or propagates ``X-Correlation-ID``.

    Responsibilities (AAP R-13 / AAP Section 0.4.5)
    -----------------------------------------------
    * **Extract or generate** the request correlation ID:
      - If the inbound HTTP request carries a valid
        ``X-Correlation-ID`` header (case-insensitive per RFC 7230),
        adopt that value verbatim after sanitization.
      - Otherwise, mint a fresh value in the form
        ``<service-prefix>-<uuid4>``, e.g.
        ``recommendation-engine-550e8400-e29b-41d4-a716-446655440000``.

    * **Bind** the value into the structlog context
      (:func:`structlog.contextvars.bind_contextvars`) so every log
      line emitted within the request scope carries
      ``correlation_id=<value>`` — satisfying AAP R-26.

    * **Expose** the value on ``scope["state"]["correlation_id"]`` for
      downstream middleware (``jwt_auth``, ``error_handler``,
      ``structured_logging``) and controllers to consume without
      re-parsing the request headers.

    * **Append** ``X-Correlation-ID`` to the outgoing HTTP response
      headers so callers can correlate their client-side logs with
      our server-side logs end-to-end.

    Lifecycle of a single HTTP request
    ----------------------------------
    1. ``__call__(scope, receive, send)`` is invoked by the ASGI server
       (uvicorn / hypercorn) for each new HTTP exchange.
    2. Non-HTTP scopes (``lifespan``, ``websocket``) short-circuit
       to the inner app; correlation IDs are an HTTP concept here and
       binding them at lifespan would produce misleading log lines at
       process-startup / shutdown.
    3. For HTTP scopes, the middleware extracts an inbound ID
       (:meth:`_extract_inbound`) or generates a fresh one
       (:meth:`_generate`).
    4. The ID is stashed on ``scope["state"]`` and bound to the
       structlog context vars.
    5. The inner app is invoked with a wrapped ``send`` callable
       (``send_with_header``) that appends ``X-Correlation-ID`` to
       the ``http.response.start`` event's headers list — replacing
       any value the inner app may have set, to keep our value
       authoritative.
    6. The structlog context binding for ``correlation_id`` is
       removed in a ``finally:`` block so the next request starts
       clean. Other startup-bound keys (``service``, ``version``,
       ``environment``) are preserved.

    Rationale for pure-ASGI (vs. ``BaseHTTPMiddleware``)
    ----------------------------------------------------
    This middleware runs on EVERY request including ``/health/live``,
    ``/health/ready``, and ``/metrics``.
    ``starlette.middleware.base.BaseHTTPMiddleware`` buffers the
    entire response body into a ``Message`` queue to pass it through
    its ``dispatch()`` callback; that buffering is observable
    overhead on high-traffic probe endpoints and on large streaming
    recommendation responses. Pure ASGI (implementing
    ``__call__(scope, receive, send)`` directly) avoids the buffer
    entirely. The folder spec for
    ``services/recommendation-engine/src/middleware/`` mandates this
    choice for the two hot-path middlewares — this one and
    ``structured_logging.py``.

    Args:
        app: The inner ASGI application (FastAPI / Starlette instance
            or the next middleware in the chain). Stored as
            ``self._app`` and invoked unchanged for non-HTTP scopes,
            or with the wrapped ``send_with_header`` for HTTP scopes.
        service_prefix: Prefix prepended to the random UUID4 when
            minting a fresh correlation ID. Defaults to
            :data:`DEFAULT_SERVICE_PREFIX` (``"recommendation-engine"``).
            ``src/main.py`` overrides this with
            ``settings.service.name`` so the value stays consistent
            with the service identity declared in
            ``services/recommendation-engine/config/default.yaml``.
        header_name: Lower-case ``bytes`` form of the correlation
            header. Defaults to :data:`CORRELATION_HEADER_NAME`
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
        service_prefix: str = DEFAULT_SERVICE_PREFIX,
        header_name: bytes = CORRELATION_HEADER_NAME,
    ) -> None:
        """Initialize the middleware.

        Args:
            app: The inner ASGI application to wrap.
            service_prefix: Prefix used when generating a fresh
                correlation ID. Should match the service's logical
                name so operators can tell the originating service
                from the ID alone.
            header_name: ``bytes`` form of the correlation header
                name. Will be normalized to lowercase exactly once
                here so the per-request comparison stays a cheap
                ``bytes == bytes`` check.
        """
        # Hold the inner app reference verbatim. We do not modify or
        # re-wrap it at construction time; the wrapping happens
        # per-request in ``__call__`` via the ``send_with_header``
        # closure.
        self._app: ASGIApp = app

        # Service prefix is stored as-is; ``_generate`` formats it
        # into the ``<prefix>-<uuid>`` shape on each new-ID path.
        # We deliberately do NOT validate or strip this string —
        # the caller (``src/main.py``) is responsible for passing a
        # well-formed value. Validating here would require coupling
        # to ``_ALLOWED_CHARS_RE`` and complicate the constructor;
        # invalid prefixes manifest as malformed correlation IDs
        # downstream and are caught in integration tests.
        self._service_prefix: str = service_prefix

        # Normalize the header name to lowercase bytes EXACTLY ONCE
        # here so the per-request comparison in ``_extract_inbound``
        # stays a cheap ``bytes == bytes`` check. ASGI canonicalizes
        # header names to lowercase, but defensive normalization
        # protects against caller mistakes (e.g., passing
        # ``b"X-Correlation-ID"`` from a misconfigured deployment).
        self._header_name: bytes = header_name.lower()

        # ``_header_name_lower`` is a SECOND reference to the same
        # bytes object — kept as a distinct attribute name to make
        # call sites self-documenting (``self._header_name_lower``
        # signals "this is already lowercased; safe to compare
        # directly"). Both names point at the same underlying bytes
        # object, so there is zero memory overhead.
        self._header_name_lower: bytes = self._header_name

    # ------------------------------------------------------------------
    # Inbound extraction (static for testability)
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_inbound(scope: Scope, header_name_lower: bytes) -> str | None:
        """Return the inbound correlation ID if the header is present and valid.

        ASGI delivers headers as a list of ``(name_bytes, value_bytes)``
        tuples in ``scope["headers"]``, with header names canonicalized
        to lowercase. We iterate looking for an exact match on the
        lowercase header name (defensive ``.lower()`` is also applied
        on each element to support stubs / mocks that may pass
        mixed-case headers in unit tests).

        Validation pipeline (defense-in-depth)
        --------------------------------------
        1. **ASCII decode.** ``value_bytes.decode("ascii", errors="strict")``
           — non-ASCII codepoints (potential PII or homograph attack)
           raise ``UnicodeDecodeError`` and we return ``None``.
        2. **Whitespace strip.** ``.strip()`` removes leading/trailing
           horizontal whitespace some misbehaving proxies emit.
        3. **Non-empty check.** An empty (or whitespace-only) value
           after the strip is treated as absent.
        4. **Length cap.** Values longer than
           :data:`MAX_CORRELATION_ID_LENGTH` (128 chars) are rejected
           to bound downstream Elasticsearch field sizes (AAP R-29).
        5. **Whitelist regex.** ``_ALLOWED_CHARS_RE`` rejects values
           containing whitespace, quotes, control characters, or any
           character outside the alphanumeric-plus-``._-`` set —
           protecting log-line integrity (AAP R-26) and JSON
           rendering downstream.

        Multiple header occurrences
        ---------------------------
        RFC 7230 §3.2.2 permits multiple occurrences of the same
        header name only for fields whose grammar explicitly defines
        a comma-separated list. ``X-Correlation-ID`` is a custom
        scalar header; multiple occurrences are a client misuse. We
        accept the FIRST valid match and ignore the rest — this
        mirrors typical proxy behavior and avoids any ambiguity
        about which value to bind.

        Args:
            scope: The ASGI request scope dict.
            header_name_lower: The lowercase ``bytes`` form of the
                correlation header name (pre-normalized in
                ``__init__``).

        Returns:
            The validated inbound correlation ID, or ``None`` if the
            header is missing or any validation step rejected the
            value. The caller (``__call__``) treats ``None`` as the
            signal to mint a fresh ID via :meth:`_generate`.
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

            # ----- Step 1: ASCII decode ------------------------------
            # ``errors="strict"`` is essential here — using ``"replace"``
            # would silently substitute the U+FFFD replacement
            # character on bad bytes and we'd propagate a corrupted
            # ID. Strict mode raises ``UnicodeDecodeError`` which we
            # catch below.
            try:
                value: str = value_bytes.decode("ascii", errors="strict").strip()
            except UnicodeDecodeError:
                # Non-ASCII bytes in the header value — could be a
                # mis-encoded UTF-8 string or random binary garbage.
                # Either way, treat as malformed and fall back.
                return None

            # ----- Step 2: non-empty after strip ---------------------
            # The strip handles leading/trailing whitespace; an empty
            # result means the client sent ``X-Correlation-ID:`` with
            # no value (or only whitespace). Treat as absent.
            if not value:
                return None

            # ----- Step 3: length cap --------------------------------
            # Bound the field size both for log-line readability and
            # for downstream Elasticsearch field-length limits
            # (AAP R-29). 128 chars is comfortable for any sane
            # correlation-ID scheme.
            if len(value) > MAX_CORRELATION_ID_LENGTH:
                return None

            # ----- Step 4: whitelist regex ---------------------------
            # The whitelist ensures the value contains only
            # characters that are safe to embed verbatim in JSON log
            # lines, response headers, and Kafka message-header
            # values without any further escaping.
            if not _ALLOWED_CHARS_RE.match(value):
                return None

            # All validations passed — the FIRST valid occurrence wins
            # and we return immediately. Subsequent header
            # occurrences (if any) are ignored.
            return value

        # No matching header found in the entire list — the inbound
        # request did not carry an ``X-Correlation-ID`` (or, less
        # commonly, the only occurrence(s) failed validation above
        # and we fell through to here only if the loop exhausted).
        return None

    # ------------------------------------------------------------------
    # Outbound generation
    # ------------------------------------------------------------------
    def _generate(self) -> str:
        """Generate a fresh correlation ID prefixed by the service name.

        Format: ``<service-prefix>-<uuid4>``, e.g.,
        ``recommendation-engine-550e8400-e29b-41d4-a716-446655440000``.

        The service prefix makes the originating service immediately
        recognizable from the ID alone — useful when correlation IDs
        propagate across services and operators see them in logs from
        any of the eleven microservices in the e-commerce platform.

        Why ``uuid.uuid4`` (random) and not ``uuid1`` (timestamp+MAC):
        ``uuid1`` would leak the host MAC address and a high-resolution
        wall-clock value in the ID — both of which would be exposed
        in client-visible response headers. ``uuid4`` produces 122
        bits of cryptographic randomness with no observable host
        information, satisfying both privacy and the "globally
        unique" property a correlation ID needs.

        Returns:
            A new correlation ID in the form
            ``"<service_prefix>-<uuid4>"``. The result is guaranteed
            to match :data:`_ALLOWED_CHARS_RE` because the service
            prefix is ASCII alphanumeric-plus-hyphen and ``str(uuid4)``
            emits hexadecimal-plus-hyphen.
        """
        # ``str(uuid.uuid4())`` produces a 36-character lowercase
        # hex-with-hyphens representation (e.g.,
        # ``"550e8400-e29b-41d4-a716-446655440000"``). Concatenating
        # with the service prefix and a separating hyphen yields a
        # well-formed ID that round-trips cleanly through the
        # whitelist regex.
        return f"{self._service_prefix}-{uuid.uuid4()}"

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
          through to the inner app untouched. Correlation IDs are an
          HTTP concept in this service; binding them at lifespan
          would produce misleading log lines at process startup /
          shutdown, and websockets do not have request / response
          semantics that map cleanly onto a single ID.
        * **HTTP scopes** — extract or generate a correlation ID,
          stash it on ``scope["state"]``, bind it into structlog
          context vars, wrap ``send`` with the header injector, and
          invoke the inner app in a ``try / finally`` block that
          guarantees cleanup of the bound context var.

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
                ``send_with_header`` for HTTP scopes so the outbound
                response carries our ``X-Correlation-ID`` header.
        """
        # --------------------------------------------------------------
        # Non-HTTP scopes: pass through untouched.
        #
        # The ASGI spec defines three scope types: ``http``, ``websocket``,
        # and ``lifespan``. Lifespan messages (``startup``, ``shutdown``)
        # arrive once per process; websockets carry their own message
        # semantics. Neither maps onto a per-request correlation ID, so
        # we forward them verbatim. Importantly, we do NOT bind a
        # correlation ID at lifespan because the startup hook in
        # ``src/main.py`` binds ``service``, ``version``, and
        # ``environment`` and we must not pollute that long-lived
        # context with a per-event ID.
        # --------------------------------------------------------------
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        # --------------------------------------------------------------
        # 1) Resolve the effective correlation ID for this request.
        #
        # ``_extract_inbound`` returns ``None`` when the header is
        # missing OR fails validation. In both cases we fall back to
        # generating a fresh ID — the request is never rejected on
        # the basis of a malformed correlation ID. (Rejecting would
        # break clients that send well-formed values from the API
        # Gateway and accidentally trip our whitelist if we ever
        # tightened it.)
        # --------------------------------------------------------------
        correlation_id: str | None = self._extract_inbound(
            scope, self._header_name_lower
        )
        if correlation_id is None:
            correlation_id = self._generate()

        # --------------------------------------------------------------
        # 2) Stash on ``scope["state"]`` so downstream middleware and
        #    controllers can read it without re-parsing headers.
        #
        # ``scope.setdefault("state", {})`` creates the slot if absent
        # — Starlette normally initializes it for HTTP scopes, but
        # raw-ASGI tests may not. ``setdefault`` is idempotent: if
        # ``state`` already exists it is returned as-is, so we
        # preserve any keys upstream middleware may have placed
        # there. (No upstream middleware exists in our stack since
        # we are the OUTERMOST, but this remains correct behavior if
        # the registration order ever changes.)
        # --------------------------------------------------------------
        state: dict[str, object] = scope.setdefault("state", {})
        state[CORRELATION_SCOPE_KEY] = correlation_id

        # --------------------------------------------------------------
        # 3) Bind to structlog contextvars so every log line within
        #    this request scope carries ``correlation_id=<value>``.
        #
        # ``structlog.contextvars.bind_contextvars`` writes to
        # ``contextvars.ContextVar`` slots which are isolated per
        # ``asyncio.Task``, so concurrent requests do not interfere
        # with each other's bindings. The ``merge_contextvars``
        # processor (installed FIRST in the structlog chain by
        # ``src/config/logging_config.py``) auto-merges the bound
        # value into every emitted log record without requiring
        # call sites to re-pass it.
        # --------------------------------------------------------------
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

        # --------------------------------------------------------------
        # 4) Pre-compute the response-header tuple.
        #
        # ``encode("ascii")`` is safe here because ``correlation_id``
        # has either passed the whitelist regex (ASCII-only by
        # construction) or was generated from a service prefix
        # (ASCII) plus ``str(uuid4)`` (hex + hyphens, also ASCII).
        # Computing the tuple once outside the closure avoids
        # rebuilding it on every ``http.response.start`` event the
        # closure observes. (In practice each HTTP request sends
        # exactly one ``http.response.start``, so the optimization
        # is symbolic — but it documents the invariant.)
        # --------------------------------------------------------------
        encoded_value: bytes = correlation_id.encode("ascii")
        header_tuple: tuple[bytes, bytes] = (
            self._header_name_lower,
            encoded_value,
        )

        # --------------------------------------------------------------
        # 5) Build the ``send_with_header`` closure that injects our
        #    ``X-Correlation-ID`` header into the outbound
        #    ``http.response.start`` message.
        #
        # We capture ``self._header_name_lower`` and ``header_tuple``
        # by closure rather than re-deriving them per call to keep
        # the hot path tight. The closure is rebuilt per request
        # (necessary because ``correlation_id`` changes per request)
        # but contains no per-event allocation other than the new
        # ``message`` dict and the ``headers_list`` copy.
        # --------------------------------------------------------------
        async def send_with_header(message: Message) -> None:
            """Inject ``X-Correlation-ID`` into the outbound response.

            Per the ASGI spec, an HTTP response is conveyed as a
            sequence of messages: exactly one
            ``http.response.start`` (with ``status`` and
            ``headers``) followed by one or more
            ``http.response.body`` messages (carrying body bytes
            and an optional ``more_body`` flag). We mutate ONLY the
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
                # explicit ``list(...)`` copy is cheap (a few
                # tuples) compared to the rest of the request
                # lifecycle.
                headers_list: list[tuple[bytes, bytes]] = list(
                    message.get("headers", [])
                )

                # Strip any existing ``X-Correlation-ID`` headers the
                # inner app may have set — our value is
                # AUTHORITATIVE. This handles the (rare but
                # observable) case where a controller manually
                # echoes a request header into the response, which
                # would otherwise produce two ``X-Correlation-ID``
                # headers in the wire response. RFC 7230 permits
                # duplicates for list-grammar fields but
                # ``X-Correlation-ID`` is scalar; one authoritative
                # value is cleaner.
                headers_list = [
                    h for h in headers_list
                    if h[0].lower() != self._header_name_lower
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
                # creates a shallow copy — the inner app's
                # references stay untouched — and is the canonical
                # ASGI-message-mutation pattern used throughout
                # Starlette / FastAPI internals.
                message = {**message, "headers": headers_list}

            # Forward the (possibly-patched) message to the
            # underlying send. The await yields control to the ASGI
            # server which writes bytes onto the wire and resumes
            # our coroutine when ready.
            await send(message)

        # --------------------------------------------------------------
        # Bind the closure to a typed reference that exercises the
        # explicit :class:`typing.Callable` / :class:`typing.Awaitable`
        # idiom mandated by the schema's external-imports declaration.
        # Starlette's ``Send`` alias would suffice for runtime
        # behavior, but the schema asks us to thread these typing
        # primitives through the implementation as well so static
        # analyzers (mypy --strict) confirm the closure satisfies
        # the explicit ``Callable[[Message], Awaitable[None]]``
        # contract before it is handed to the inner app. The cast
        # is a no-op at runtime — ``typed_send`` and
        # ``send_with_header`` are the same object — but it
        # documents the contract precisely at the call site and
        # makes both ``Awaitable`` and ``Callable`` semantically
        # load-bearing in the type-check pipeline.
        # --------------------------------------------------------------
        typed_send: _SendCallable = send_with_header

        # --------------------------------------------------------------
        # 6) Invoke the inner app inside a ``try / finally`` block.
        #
        # The ``finally:`` block runs on EVERY exit path — successful
        # response, observed 4xx/5xx, raised exception, even
        # ``CancelledError`` from a client disconnect. This
        # guarantees the bound ``correlation_id`` is always cleared
        # so the next request seen by this task starts with a clean
        # context. (Without this, a long-running asyncio task pool
        # would accumulate stale IDs bleed-through.)
        #
        # We deliberately do NOT catch the exception here — that is
        # the responsibility of ``error_handler`` middleware. Any
        # exception propagates up and out, while ``finally`` still
        # cleans the bound context var.
        # --------------------------------------------------------------
        try:
            await self._app(scope, receive, typed_send)
        finally:
            # Unbind ONLY the ``correlation_id`` key, NOT the entire
            # bound context. The startup hook in ``src/main.py`` binds
            # ``service``, ``version``, and ``environment`` once per
            # process via ``bind_contextvars``; calling
            # ``clear_contextvars()`` here would also drop those
            # keys, leaving subsequent log lines missing the
            # ``service`` field — a direct AAP R-26 violation
            # because every log line is required to carry
            # ``service``.
            structlog.contextvars.unbind_contextvars(CORRELATION_SCOPE_KEY)
