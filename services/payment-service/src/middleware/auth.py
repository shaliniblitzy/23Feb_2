"""ASGI JWT validation middleware for the Payment Service.

Validates Bearer tokens issued by the Auth Service (AAP R-21) using
JWKS public keys with bounded-TTL caching (AAP R-22). Allow-listed
paths (health probes, metrics, webhooks) bypass JWT validation ---
webhooks are authenticated by HMAC at the controller boundary
(AAP R-12).

Behavioral contract:

* Pass-through for allow-listed paths and non-HTTP scopes.
* Returns 401 with ``WWW-Authenticate: Bearer realm="payment-service"``
  on missing/invalid tokens (RFC 6750); 403 on insufficient scopes
  (RFC 7235).
* Reads ``settings.auth.algorithms``, ``audience``, ``issuer``,
  ``clock_skew_seconds``, ``required_scopes``, and ``public_routes``
  from runtime configuration; supports both RS256 and ES256.
* On success, binds the validated user to
  ``scope["state"]["user"] = {"sub": ..., "scope": [...], "_claims": {...}}``
  for downstream handlers.

This is a **Pure ASGI** middleware. It deliberately AVOIDS
``starlette.middleware.base.BaseHTTPMiddleware`` because that base
class buffers the request body, which would break webhook HMAC
verification (AAP R-12). The PyJWKClient instance is owned by the
DI container so its bounded-TTL cache (AAP R-22) is shared by every
request. No raw token contents or ``Authorization`` header values
are EVER emitted to logs (PCI-DSS adjacency + AAP R-25).
"""

from __future__ import annotations

import json
from typing import Any, cast

import jwt
import structlog
from jwt.exceptions import PyJWTError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
# Structured logger; correlation_id is auto-merged via
# ``structlog.contextvars.merge_contextvars`` (configured globally in
# ``logging_config.py`` and bound per-request by
# :class:`CorrelationIdMiddleware`), so this module never explicitly
# passes the correlation_id field.
_LOGGER = structlog.get_logger("payment_service.middleware.auth")

#: Wire-format authorization header in lowercase ``bytes``. ASGI
#: canonicalizes header names to lowercase regardless of on-the-wire
#: casing (RFC 7230); we store ``bytes`` so per-request lookup stays
#: a cheap ``bytes == bytes`` check.
_AUTHORIZATION_HEADER: bytes = b"authorization"

#: Required prefix on the Authorization header value (RFC 6750 §2.1).
_BEARER_PREFIX: str = "Bearer "

#: ``WWW-Authenticate`` header value emitted on every 401 response.
#: The realm parameter is an RFC 6750 §3 requirement and identifies
#: which service rejected the request (useful for Kibana 401 dashboards).
_WWW_AUTHENTICATE_REALM: bytes = b'Bearer realm="payment-service"'

#: ``Content-Type`` header value for JSON error envelopes.
_JSON_CONTENT_TYPE: bytes = b"application/json; charset=utf-8"

#: Standard JWT-decoding options. AAP R-21 requires every protected
#: token to carry ``exp``, ``iat``, ``iss``, and ``aud``; we enforce
#: presence via ``require`` rather than swallowing missing claims
#: silently (a missing ``aud`` would otherwise let a token minted for
#: a different audience validate).
_DECODE_OPTIONS: dict[str, bool | list[str]] = {
    "verify_signature": True,
    "verify_exp": True,
    "verify_nbf": True,
    "verify_iat": True,
    "verify_iss": True,
    "verify_aud": True,
    "require": ["exp", "iat", "iss", "aud"],
}


# ---------------------------------------------------------------------------
# Middleware class
# ---------------------------------------------------------------------------
class JWTAuthMiddleware:
    """Pure ASGI middleware that validates JWTs issued by Auth Service.

    Auth Service is the SOLE issuer (AAP R-21); this service only
    validates. Public keys come from a JWKS endpoint with a
    bounded-TTL cache owned by the DI container (AAP R-22), so key
    rotation does not require service restart. The middleware is
    typically registered between :class:`StructuredLoggingMiddleware`
    (outer) and :class:`ErrorHandlerMiddleware` (inner).

    Args:
        app: The next ASGI app/middleware in the chain.
        allow_paths: Paths to bypass JWT validation. When ``None``
            (the default), the effective allow list at request time
            is :attr:`settings.auth.public_routes`. When a tuple is
            supplied (e.g., from ``main.py``), it REPLACES the
            settings-derived list --- ``main.py`` typically passes a
            superset that includes dev-only endpoints (``/docs``,
            ``/redoc``, ``/openapi.json``).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        allow_paths: tuple[str, ...] | None = None,
    ) -> None:
        """Initialize the middleware.

        ``allow_paths`` is keyword-only (via ``*,``) to match the
        ``app.add_middleware(JWTAuthMiddleware, allow_paths=...)``
        registration in ``main.py``. We snapshot the tuple at
        construction time, or store ``None`` so per-request resolution
        falls back to ``settings.auth.public_routes``.
        """
        self._app: ASGIApp = app
        self._explicit_allow_paths: tuple[str, ...] | None = (
            tuple(allow_paths) if allow_paths is not None else None
        )

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        """ASGI entry point --- invoked by the ASGI server per event.

        Non-HTTP scopes (``lifespan``, ``websocket``) pass through
        untouched: lifespan mutation breaks uvicorn startup signaling
        and websockets do not carry Bearer tokens here.
        """
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        # Settings resolution can raise RuntimeError if the DI
        # container is not initialized; that is a misconfiguration
        # (NOT a per-request auth failure), so we do NOT catch it ---
        # it propagates to ErrorHandlerMiddleware which serializes
        # to 500.
        settings = self._resolve_settings(scope)
        allow_paths = self._resolve_allow_paths(settings)
        path = scope.get("path") or ""

        # Allow-list check happens BEFORE token extraction so health
        # probes and webhooks NEVER incur token-validation work.
        if path in allow_paths:
            await self._app(scope, receive, send)
            return

        token = self._extract_bearer_token(scope)
        if token is None:
            await self._unauthorized_response(
                scope, send, reason="missing_bearer_token"
            )
            return

        # Two-tier exception handling: PyJWTError (JWT validation
        # failure --- attacker-controllable, expected, WARNING) vs.
        # generic Exception (JWKS network failure --- operationally
        # significant, ERROR). Both fail-closed to 401 with distinct
        # ``reason`` values for observability.
        try:
            claims = self._validate_token(scope, token, settings=settings)
        except PyJWTError as exc:
            await self._unauthorized_response(
                scope, send, reason=f"jwt_{type(exc).__name__}"
            )
            return
        except Exception as exc:  # noqa: BLE001
            _LOGGER.error(
                "payment_service.auth.jwks_failure",
                exception_type=type(exc).__name__,
                path=path,
                method=scope.get("method"),
                exc_info=exc,
            )
            await self._unauthorized_response(
                scope, send, reason="jwks_unavailable"
            )
            return

        # Enforce required scopes per the matched path. The required
        # set comes from ``settings.auth.required_scopes`` so
        # operators can change scope strings without code changes.
        scopes = self._parse_scopes(claims)
        required = self._required_scopes_for_path(
            path, settings, scope.get("method")
        )
        if required and not required.issubset(set(scopes)):
            _LOGGER.warning(
                "payment_service.auth.insufficient_scope",
                required_scopes=sorted(required),
                presented_scopes=sorted(scopes),
                path=path,
                method=scope.get("method"),
            )
            await self._forbidden_response(
                scope, send, reason="insufficient_scope"
            )
            return

        # Bind the validated user to scope state for downstream
        # handlers. ``scope.setdefault`` is defensive --- Starlette
        # pre-populates ``scope["state"]`` for HTTP scopes, but raw
        # ASGI servers / unit-test stubs may not. The claims dict is
        # COPIED so handlers cannot mutate the cached claims.
        state = scope.setdefault("state", {})
        state["user"] = {
            "sub": str(claims.get("sub", "")),
            "scope": scopes,
            "_claims": dict(claims),
        }

        await self._app(scope, receive, send)

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def _resolve_settings(self, scope: Scope) -> Any:
        """Resolve runtime ``Settings`` from the FastAPI app state.

        Reads ``scope["app"].state.container.settings``, which is set
        by ``main.lifespan`` after :func:`build_container`. Raises
        ``RuntimeError`` if any link in the chain is missing --- a
        misconfiguration that should fail loudly rather than be
        masked as a 401.
        """
        app = scope.get("app")
        if app is None:
            raise RuntimeError(
                "ASGI scope missing 'app' --- cannot resolve settings."
            )
        state = getattr(app, "state", None)
        if state is None:
            raise RuntimeError(
                "ASGI app missing .state --- container not initialized."
            )
        container = getattr(state, "container", None)
        if container is None:
            raise RuntimeError(
                "ASGI app.state missing .container --- "
                "JWTAuthMiddleware requires the DI container to be "
                "built before requests are served."
            )
        settings = getattr(container, "settings", None)
        if settings is None:
            raise RuntimeError("Container missing .settings.")
        return settings

    def _resolve_allow_paths(self, settings: Any) -> frozenset[str]:
        """Compose the effective allow-path set.

        An explicit constructor tuple REPLACES the settings-derived
        list (it does not augment it). When the constructor was
        ``None``, :attr:`settings.auth.public_routes` is used so
        operators can add paths via env reload.
        """
        if self._explicit_allow_paths is not None:
            return frozenset(self._explicit_allow_paths)
        public_routes = getattr(settings.auth, "public_routes", None)
        return frozenset(public_routes or ())

    @staticmethod
    def _extract_bearer_token(scope: Scope) -> str | None:
        """Extract the Bearer token from the Authorization header.

        Returns the token string, or ``None`` if the header is
        missing, malformed, or empty. Case-insensitive lookup guards
        against non-conforming servers; latin-1 is the canonical
        safe encoder for HTTP header bytes (RFC 7230 §3.2.4).
        """
        for raw_name, raw_value in scope.get("headers") or []:
            if raw_name.lower() == _AUTHORIZATION_HEADER:
                try:
                    value = raw_value.decode("latin-1")
                except (UnicodeDecodeError, AttributeError):
                    return None
                if not value.startswith(_BEARER_PREFIX):
                    return None
                token = value[len(_BEARER_PREFIX):].strip()
                return token or None
        return None

    def _validate_token(
        self,
        scope: Scope,
        token: str,
        *,
        settings: Any,
    ) -> dict[str, Any]:
        """Validate the JWT signature and claims; return decoded claims.

        Two-step validation:

        1. **Algorithm pre-check** via :func:`jwt.get_unverified_header`
           rejects unsupported algorithms BEFORE any JWKS network
           round trip --- prevents DoS amplification (attacker spamming
           garbage tokens to force JWKS fetches) and information
           disclosure (the JWKS URL does not appear in the failure
           path).
        2. **Full signature + claims verification** via
           :func:`jwt.decode` using the public key fetched from the
           container's :class:`jwt.PyJWKClient`. The same algorithm
           allowlist is passed to ``jwt.decode`` to defeat the classic
           "signed-with-HS256-using-the-public-key" attack.

        Raises any :class:`PyJWTError` subclass on validation failure
        and a generic :class:`Exception` on JWKS-network failure.
        """
        # Resolve algorithm whitelist from settings (per AAP R-21/R-22 ---
        # never hardcode; rotation may add a new alg without code change).
        algorithms = list(
            getattr(settings.auth, "algorithms", None) or ["RS256"]
        )

        # Algorithm pre-check --- fail fast before the JWKS round trip.
        unverified_header = jwt.get_unverified_header(token)
        alg = unverified_header.get("alg")
        if alg not in algorithms:
            raise jwt.InvalidAlgorithmError(
                f"JWT alg '{alg}' is not in the configured allowlist."
            )

        # PyJWKClient --- bounded TTL cache per AAP R-22 --- owned by
        # the DI container.
        container = scope["app"].state.container
        signing_key = container.jwks_client.get_signing_key_from_jwt(token)

        decoded = jwt.decode(
            token,
            key=signing_key.key,
            algorithms=algorithms,
            audience=settings.auth.audience,
            issuer=settings.auth.issuer,
            leeway=int(settings.auth.clock_skew_seconds),
            # ``_DECODE_OPTIONS`` is a plain dict by design --- typing
            # it as the ``jwt.types.Options`` TypedDict would couple
            # this module to a non-public PyJWT module path. Cast at
            # the call site instead of importing the TypedDict.
            options=cast("Any", _DECODE_OPTIONS),
        )
        return dict(decoded)

    @staticmethod
    def _parse_scopes(claims: dict[str, Any]) -> list[str]:
        """Extract the scopes claim from a JWT.

        Honors both OAuth 2.0 (RFC 6749) and RFC 8693 conventions:
        ``scope`` (singular) may be a space-separated string OR a
        list; ``scopes`` (plural) is sometimes used by older issuers
        and is accepted as a fallback.
        """
        raw = claims.get("scope")
        if raw is None:
            raw = claims.get("scopes")
        if raw is None:
            return []
        if isinstance(raw, str):
            return [s for s in raw.split(" ") if s]
        if isinstance(raw, list):
            return [str(s) for s in raw if s]
        return []

    @staticmethod
    def _required_scopes_for_path(
        path: str,
        settings: Any,
        method: str | None,
    ) -> set[str]:
        """Compute the required scope set for the given path + method.

        Reads :attr:`settings.auth.required_scopes` (a flat dict
        mapping operation -> scope), then applies pragmatic defaults:

        * any path containing ``/refund`` -> ``payments:refund``
        * any path ending with ``/attempts`` -> ``payments:admin``
        * ``POST /payments`` -> ``payments:charge``
        * ``GET /payments`` or ``GET /payments/...`` -> ``payments:read``
        * default -> empty set (any authenticated user)

        Path matching uses prefixes / HTTP method so it stays robust
        to FastAPI's path templating without requiring a second
        routing pass at this layer.
        """
        scopes_map_raw = getattr(settings.auth, "required_scopes", None)
        scopes_map = dict(scopes_map_raw or {})
        charge_scope = scopes_map.get("charge", "payments:charge")
        refund_scope = scopes_map.get("refund", "payments:refund")
        admin_scope = scopes_map.get("admin", "payments:admin")
        read_scope = scopes_map.get("read", "payments:read")

        method = (method or "").upper()

        # Order matters: refund and admin sub-resources MUST match
        # before the generic read branch.
        if "/refund" in path:
            return {refund_scope}
        if path.endswith("/attempts"):
            return {admin_scope}
        if method == "POST" and path == "/payments":
            return {charge_scope}
        if method == "GET" and path.startswith("/payments"):
            return {read_scope}

        # Default: any authenticated user.
        return set()

    async def _unauthorized_response(
        self,
        scope: Scope,
        send: Send,
        *,
        reason: str,
    ) -> None:
        """Send a 401 response with WWW-Authenticate header.

        The response shape matches the canonical Payment Service
        error envelope used by :class:`ErrorHandlerMiddleware`. The
        ``correlation_id`` is read from
        ``scope["state"]["correlation_id"]`` (set by
        :class:`CorrelationIdMiddleware`).
        """
        correlation_id = self._scope_correlation_id(scope)
        _LOGGER.warning(
            "payment_service.auth.unauthorized",
            reason=reason,
            path=scope.get("path"),
            method=scope.get("method"),
        )
        payload: dict[str, Any] = {
            "error": "UNAUTHORIZED",
            "message": "Authentication required.",
            "correlation_id": correlation_id,
            "details": {"reason": reason},
        }
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", _JSON_CONTENT_TYPE),
            (b"content-length", str(len(body)).encode("ascii")),
            (b"www-authenticate", _WWW_AUTHENTICATE_REALM),
        ]
        start_message: Message = {
            "type": "http.response.start",
            "status": 401,
            "headers": headers,
        }
        body_message: Message = {
            "type": "http.response.body",
            "body": body,
            "more_body": False,
        }
        await send(start_message)
        await send(body_message)

    async def _forbidden_response(
        self,
        scope: Scope,
        send: Send,
        *,
        reason: str,
    ) -> None:
        """Send a 403 response (authenticated but lacks required scopes).

        RFC 7235 distinguishes 401 (missing/invalid auth) from 403
        (authenticated user with insufficient permissions); 403 is
        correct here since the token validates but lacks scopes.
        """
        correlation_id = self._scope_correlation_id(scope)
        payload: dict[str, Any] = {
            "error": "FORBIDDEN",
            "message": "Insufficient permissions.",
            "correlation_id": correlation_id,
            "details": {"reason": reason},
        }
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers: list[tuple[bytes, bytes]] = [
            (b"content-type", _JSON_CONTENT_TYPE),
            (b"content-length", str(len(body)).encode("ascii")),
        ]
        start_message: Message = {
            "type": "http.response.start",
            "status": 403,
            "headers": headers,
        }
        body_message: Message = {
            "type": "http.response.body",
            "body": body,
            "more_body": False,
        }
        await send(start_message)
        await send(body_message)

    @staticmethod
    def _scope_correlation_id(scope: Scope) -> str:
        """Read correlation_id bound by :class:`CorrelationIdMiddleware`.

        Falls back to ``"unknown"`` only when the upstream middleware
        is missing or hasn't run --- which should only happen in
        unit-test stubs. A literal ``"unknown"`` keeps the JSON
        envelope shape uniform across success and failure paths.
        """
        state = scope.get("state") or {}
        cid = state.get("correlation_id")
        if isinstance(cid, str) and cid:
            return cid
        return "unknown"
