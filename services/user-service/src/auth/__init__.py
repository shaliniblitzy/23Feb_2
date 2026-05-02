"""User Service authentication (JWT validation) package.

This package centralizes the **JWT-validation-only** subsystem for the User
Service. It NEVER issues, signs, mints, or rotates JWTs — that responsibility
belongs exclusively to the Auth Service (AAP R-21).

Concretely, this package provides:

* :class:`src.auth.principal.Principal` — frozen value object representing
  the authenticated caller (subject, scopes, claims, raw token).
* :class:`src.auth.jwks_fetcher.JWKSFetcher` — async client that fetches
  and caches the Auth Service's JWKS document with bounded TTL (AAP R-22),
  protected by a tenacity retry policy (R-15) and a pybreaker circuit
  breaker (R-16). Exposes ``prime()`` (fail-fast at startup, R-19),
  ``get_signing_key(kid)`` (refresh-on-miss), and ``aclose()``.
* :class:`src.auth.jwt_validator.JWTValidator` — validates JWTs against
  the JWKS via ``pyjwt[crypto]``; enforces issuer / audience / algorithm
  / expiry / nbf with ``leeway_seconds`` (RFC 6749 / RFC 6750 — AAP R-23).
  Raises typed exceptions from :mod:`src.domain.errors` mapped by the
  ``error_handler`` middleware to 401/403/503 responses.
* ``src.auth.jwt_validator.get_current_user`` — FastAPI dependency
  factory that reads the ``Authorization: Bearer <token>`` header,
  delegates to ``JWTValidator.validate``, and returns a ``Principal``.
* ``src.auth.scopes.require_scope`` — FastAPI dependency factory
  asserting the validated ``Principal`` carries a required scope; raises
  :class:`src.domain.errors.InsufficientScopeError` (mapped to 403).

Submodules
----------
``principal``
    The :class:`Principal` value object: a frozen dataclass holding
    ``subject``, ``scopes`` (frozenset), ``claims`` (read-only mapping),
    and ``raw_token`` (used by service-to-service forwarding).
``jwks_fetcher``
    The :class:`JWKSFetcher` async client (cachetools + pybreaker + tenacity
    + pyjwt JWK parsing).
``jwt_validator``
    The :class:`JWTValidator` plus the ``get_current_user`` FastAPI
    dependency.
``scopes``
    The ``require_scope`` FastAPI dependency factory and the canonical
    User Service scope name constants.

Consumption rules
-----------------
This package's ``__init__.py`` is intentionally **side-effect-free** and
**does not re-export submodule symbols** (per the folder specification).
Downstream code MUST import directly from the submodule::

    # CORRECT — used by src.container, src.controllers.*, etc.:
    from src.auth.principal import Principal
    from src.auth.jwks_fetcher import JWKSFetcher
    from src.auth.jwt_validator import JWTValidator, get_current_user
    from src.auth.scopes import require_scope

    # INCORRECT — this package does not re-export:
    from src.auth import JWTValidator  # NO — symbol not exported here

    # INCORRECT — relative imports break tooling (pytest --import-mode=importlib,
    # mypy plugins, ruff). Always use absolute imports across the monorepo:
    from .jwt_validator import JWTValidator  # NO

AAP traceability
----------------
- AAP Section 0.1.1 Component #3 — User Service.
- AAP Section 0.4.5 — Authentication middleware (validate JWT signature,
  check expiry, enforce scopes; fall back to public endpoint whitelist).
- AAP Section 0.5.2.2 bullet 3 — User Service implementation directive.
- AAP R-21 — Auth Service is the sole issuer of JWT tokens; this package
  exclusively VALIDATES tokens issued by Auth Service.
- AAP R-22 — JWKS validation keys cached with bounded TTL.
- AAP R-23 — OAuth 2.0 / RFC 6749 / RFC 6750 compliance.
- AAP R-25 — Secrets supplied exclusively via environment variables;
  never present in source files.
"""

from __future__ import annotations

__all__: list[str] = []
