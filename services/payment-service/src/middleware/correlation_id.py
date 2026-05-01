"""Correlation ID middleware for the Payment Service.

Implements AAP R-13 by ensuring every HTTP request carries an
``X-Correlation-ID`` (or has one generated), binds the value to a
structlog contextvar so it auto-flows to all log lines (AAP R-26)
and downstream HTTP/Kafka calls, and echoes the value in the
response header.

This is the OUTERMOST middleware in the Payment Service stack ---
registered LAST in ``main.py`` (Starlette wraps inside-out: last-
added runs first at request entry, last at response exit). Other
middleware (logging, auth, error_handler) and downstream code
(controllers, providers, Kafka producers) read the correlation_id
either from ``scope["state"]["correlation_id"]`` or from
``structlog.contextvars`` --- this module is the SOURCE OF TRUTH;
everything else is a SINK.

Per-request behavior (AAP R-13 / R-26 / Section 0.4.5): extract
``X-Correlation-ID`` (case-insensitive); validate against
``^[A-Za-z0-9._\\-]+$`` plus the 128-char cap (silent rejection);
on failure generate ``<service_prefix>-<uuid4-hex>``; bind via
``structlog.contextvars.bind_contextvars`` and stash on
``scope["state"]``; wrap ``send`` to inject the header on
``http.response.start``; in ``finally:`` unbind the SPECIFIC key
(NOT ``clear_contextvars()``) to preserve startup-bound vars
(``service`` / ``version`` / ``environment``).

Pure-ASGI: no Starlette / FastAPI dependencies in the signature.
Defense-in-depth on inbound IDs prevents whitespace, quotes,
control chars, or non-ASCII bytes from corrupting log JSON or
Kafka header serialization.
"""

from __future__ import annotations

import re
import uuid
from typing import Awaitable, Callable

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

# ---------------------------------------------------------------------------
# Local ASGI type aliases (Pure-ASGI; avoids ``starlette.types`` import
# to keep the middleware framework-agnostic). Mirrors ASGI 3.0 spec.
# ---------------------------------------------------------------------------
Scope = dict
Message = dict
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

# Module logger; instantiated at import for test monkey-patching.
# Currently silent on the hot path (per-request rejection logs would
# generate unacceptable noise on a foundational middleware).
_LOGGER = structlog.get_logger("payment_service.middleware.correlation_id")


# ---------------------------------------------------------------------------
# Module-level constants (public API)
# ---------------------------------------------------------------------------
#: Wire-format header name in lowercase ``bytes``. ASGI canonicalizes
#: header names to lowercase regardless of on-the-wire casing (HTTP
#: names are case-insensitive per RFC 7230). Stored as ``bytes`` so
#: the response-injection path appends directly into the
#: ``http.response.start`` headers list (``list[tuple[bytes, bytes]]``).
CORRELATION_HEADER_NAME: bytes = b"x-correlation-id"

#: Key in ``scope["state"]`` where the correlation_id is stored for
#: downstream middleware/handlers to read via
#: ``request.state.correlation_id``.
CORRELATION_SCOPE_KEY: str = "correlation_id"

#: Prefix prepended to ``uuid4().hex`` for fallback IDs. Format:
#: ``payment-service-<uuid4-hex>``. The service-specific prefix lets
#: operators identify the originating service from the ID alone.
DEFAULT_SERVICE_PREFIX: str = "payment-service"

#: Maximum permitted length of an inbound correlation_id. Longer
#: values are rejected and a fresh ID is generated. Bounds memory
#: consumption from header-flood attacks; 128 chars accommodates any
#: realistic format (UUID, ULID, KSUID, opaque token).
MAX_CORRELATION_ID_LENGTH: int = 128

# Whitelist regex: alphanumerics + ``._-``. Excludes whitespace,
# quotes, control chars, and any character that would need quoting
# in HTTP headers, embed unsafely in Kafka header bytes, log JSON,
# or Kibana queries. Anchored ``^...$``: partial matches rejected.
_ALLOWED_CHARS_RE: re.Pattern[str] = re.compile(r"^[A-Za-z0-9._\-]+$")


# ---------------------------------------------------------------------------
# Helper functions (module-private)
# ---------------------------------------------------------------------------
def _extract_header_value(scope: Scope, header_name: bytes) -> str | None:
    """Read a header from an ASGI scope, returning the decoded value or ``None``.

    ASGI scope headers are ``list[tuple[bytes, bytes]]`` with names
    lowercased. We compare case-insensitively (defensive against
    non-conforming servers passing mixed case) and decode the first
    match using latin-1 (the canonical safe encoder for HTTP header
    bytes per RFC 7230 §3.2.4). On any decoding failure we return
    ``None`` so the caller can fall back to ID generation rather
    than emitting a 500 from the middleware.
    """
    headers = scope.get("headers") or []
    name_lower = header_name.lower()
    for raw_name, raw_value in headers:
        if raw_name.lower() == name_lower:
            try:
                return raw_value.decode("latin-1").strip()
            except (UnicodeDecodeError, AttributeError):
                # Malformed bytes / non-bytes value --- the FIRST
                # matching header wins per RFC 7230 for scalar
                # fields, so do not scan further.
                return None
    return None


def _is_valid_correlation_id(candidate: str) -> bool:
    """Validate a candidate correlation ID against length and charset rules.

    Returns ``False`` for empty, over-long
    (> :data:`MAX_CORRELATION_ID_LENGTH`), or values containing any
    character outside :data:`_ALLOWED_CHARS_RE`. The middleware
    silently substitutes a fresh ID on ``False``.
    """
    if not candidate:
        return False
    if len(candidate) > MAX_CORRELATION_ID_LENGTH:
        return False
    return bool(_ALLOWED_CHARS_RE.match(candidate))


def _generate_correlation_id(service_prefix: str) -> str:
    """Build a fresh correlation_id of the form ``<prefix>-<uuid4-hex>``.

    Uses :func:`uuid.uuid4` (122 bits of cryptographic randomness;
    no host MAC / wall-clock leakage like ``uuid1``) and the
    ``.hex`` short form (32 lowercase hex chars, no hyphens). With
    the default prefix the total length is 48 chars --- comfortably
    under :data:`MAX_CORRELATION_ID_LENGTH` and matching the
    whitelist regex.
    """
    return f"{service_prefix}-{uuid.uuid4().hex}"


# ---------------------------------------------------------------------------
# Middleware class
# ---------------------------------------------------------------------------
class CorrelationIdMiddleware:
    """Pure ASGI middleware that propagates ``X-Correlation-ID``.

    For every HTTP request the middleware extracts (or generates) a
    correlation ID, binds it as a structlog contextvar so every log
    line in the request scope carries it (AAP R-26), sets it on
    ``scope["state"]["correlation_id"]``, wraps ``send`` to inject
    the header on the response, and in ``finally:`` unbinds ONLY
    the correlation_id key (NOT ``clear_contextvars()``) so
    startup-bound vars are preserved.

    Args:
        app: The next ASGI app/middleware in the chain.
        service_prefix: Prefix for generated fallback IDs. Defaults
            to :data:`DEFAULT_SERVICE_PREFIX` (``"payment-service"``).
        header_name: Wire-format header name (``str`` or ``bytes``).
            Normalized to lowercase ``bytes`` in ``__init__``.
            Defaults to :data:`CORRELATION_HEADER_NAME`.

    Example:
        >>> from fastapi import FastAPI
        >>> app = FastAPI()
        >>> # Register LAST so this middleware runs OUTERMOST
        >>> # (Starlette wraps later-added around earlier ones).
        >>> app.add_middleware(CorrelationIdMiddleware)
    """

    def __init__(
        self,
        app: ASGIApp,
        service_prefix: str = DEFAULT_SERVICE_PREFIX,
        header_name: bytes = CORRELATION_HEADER_NAME,
    ) -> None:
        """Initialize the middleware.

        ``header_name`` is normalized to lowercase ``bytes`` exactly
        once here so the per-request comparison stays a cheap
        ``bytes == bytes`` check. ``str`` is accepted for caller
        convenience and encoded with latin-1 (the RFC 7230 canonical
        safe encoder for HTTP header names).
        """
        self._app: ASGIApp = app
        self._service_prefix: str = service_prefix
        if isinstance(header_name, str):
            header_name_bytes = header_name.encode("latin-1").lower()
        else:
            header_name_bytes = header_name.lower()
        self._header_name: bytes = header_name_bytes

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        """ASGI entry point --- invoked by the ASGI server per event.

        Non-HTTP scopes (``lifespan``, ``websocket``) pass through
        untouched: modifying lifespan scope can break uvicorn's
        startup/shutdown signaling. Exceptions raised by the inner
        app are NOT caught --- the inner ``error_handler``
        middleware owns that responsibility; we only ensure the
        contextvar is unbound on every exit path.
        """
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        correlation_id = self._resolve_correlation_id(scope)

        # Ensure ``scope["state"]`` exists. Starlette pre-populates it
        # for HTTP scopes; raw-ASGI servers / unit-test stubs may not.
        # ``setdefault`` is idempotent and preserves any keys upstream
        # middleware placed there.
        state = scope.setdefault("state", {})
        state[CORRELATION_SCOPE_KEY] = correlation_id

        # Bind BEFORE the inner app runs so every log line emitted
        # during the request carries the field via the
        # ``merge_contextvars`` processor.
        bind_contextvars(correlation_id=correlation_id)

        wrapped_send = self._make_send_wrapper(correlation_id, send)

        try:
            await self._app(scope, receive, wrapped_send)
        finally:
            # Unbind ONLY our key --- ``clear_contextvars()`` would
            # drop startup-bound ``service``/``version``/
            # ``environment``, violating AAP R-26.
            unbind_contextvars(CORRELATION_SCOPE_KEY)

    def _resolve_correlation_id(self, scope: Scope) -> str:
        """Return a valid correlation_id from the request or generate one."""
        candidate = _extract_header_value(scope, self._header_name)
        if candidate is not None and _is_valid_correlation_id(candidate):
            return candidate
        return _generate_correlation_id(self._service_prefix)

    def _make_send_wrapper(
        self,
        correlation_id: str,
        send: Send,
    ) -> Send:
        """Build a ``send`` wrapper that injects the response header.

        The header is injected on the ``http.response.start`` message
        only; other message types pass through unchanged. IDEMPOTENT:
        if a handler has already set the header we preserve their
        value. Header-list mutation builds a NEW list (never mutates
        the original) because some ASGI servers reuse message dicts.
        """
        header_name = self._header_name
        header_value = correlation_id.encode("latin-1")

        async def _send(message: Message) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers") or [])
                already_present = any(
                    raw_name.lower() == header_name for raw_name, _ in headers
                )
                if not already_present:
                    headers.append((header_name, header_value))
                    # Shallow-copy idiom for ASGI message mutation:
                    # does NOT mutate ``message`` in place.
                    message = {**message, "headers": headers}
            await send(message)

        return _send
