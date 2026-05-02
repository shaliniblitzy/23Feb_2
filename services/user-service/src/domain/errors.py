"""Typed domain exceptions for the User Service.

The hierarchy is rooted at :class:`DomainError`, a direct subclass of
:class:`Exception` (NOT :class:`RuntimeError` — bare ``RuntimeError`` is too
generic for "expected, modeled" failure modes). Every subclass carries:

* ``error_code: ClassVar[str]`` — stable machine identifier used by
  ``error_handler.py`` to construct the RFC 7807 ``type`` URI.
* ``http_status: ClassVar[int]`` — default HTTP response status the
  middleware returns when this exception is unhandled at the controller.
* ``correlation_id: UUID | None`` (instance) — optional caller-supplied
  correlation id propagated through logs and the error response body
  (AAP R-13, R-26, R-28).
* ``detail: dict[str, Any]`` (instance) — structured context for logging
  and the error response body. Always copied in the constructor for
  immutability.
* ``cause: BaseException | None`` (instance) — optional underlying cause
  for traceback chaining; constructors that accept it use ``raise
  SomeError(...) from exc`` at the call site to set ``__cause__``.

Constructors take ``correlation_id`` and ``cause`` as KEYWORD-ONLY
arguments (after ``*``). Subclasses do NOT override ``__init__`` —
they customize behavior via class-attribute overrides only (matching the
notification-service flat-hierarchy pattern at
``services/notification-service/src/domain/errors.py``).

Design principles
-----------------
1. **Foundational module** — this file is the most foundational in the
   ``services/user-service/src/domain`` package. It MUST NOT import
   from any other ``src/*`` module. Every other domain file imports
   from it; circularity is avoided trivially by depending only on the
   Python standard library.
2. **Subclass of** :class:`Exception`, **not** :class:`RuntimeError`
   — domain errors are EXPECTED outcomes of business-rule evaluation
   (a missing user, a duplicate email, a malformed locale), not
   unexpected runtime failures.
3. **Class-attribute customization only** — subclasses override
   ``error_code`` and ``http_status`` and add no other behavior. This
   keeps the taxonomy uniform and lets ``error_handler.py`` use a
   single ``except DomainError as e: ...`` branch followed by a read
   of ``type(e).error_code`` / ``type(e).http_status``.
4. **No I/O, no logging, no side effects at import time** — keeps unit
   test startup cheap and prevents the module from accidentally
   pulling in framework-specific code paths.

Cross-references
----------------
This module is consumed by every other ``src/*`` package in the User
Service:

* ``src/middleware/error_handler.py`` — consumes
  :attr:`DomainError.error_code` and :attr:`DomainError.http_status`
  to construct RFC 7807 problem+json responses; walks ``__cause__``
  for the optional debug ``chained_cause`` field when
  ``LOGGING_LEVEL=DEBUG``.
* ``src/domain/validators.py`` — raises :class:`InvalidEmail`,
  :class:`InvalidLocale`, :class:`InvalidCurrency`,
  :class:`InvalidCountry`, :class:`InvalidTimezone`,
  :class:`InvalidPhone`, :class:`InvalidDateOfBirth`.
* ``src/domain/profile.py`` — field validators raise
  :class:`InvalidLocale`, :class:`InvalidTimezone`,
  :class:`InvalidPhone`, :class:`InvalidDateOfBirth`.
* ``src/domain/preferences.py`` — field validators raise
  :class:`InvalidLocale`, :class:`InvalidCurrency`.
* ``src/domain/address.py`` — field validators raise
  :class:`InvalidCountry`, :class:`InvalidPhone`.
* ``src/domain/user.py`` — field validators raise :class:`InvalidEmail`;
  aggregate methods raise :class:`MaxAddressesExceeded`,
  :class:`AddressNotFound`.
* ``src/repository/user_repository.py`` — raises :class:`UserNotFound`,
  :class:`EmailAlreadyExists`, :class:`OptimisticConcurrencyError`.
* ``src/middleware/auth.py`` — raises :class:`InvalidTokenError`,
  :class:`ExpiredTokenError`, :class:`InvalidIssuerError`,
  :class:`InvalidAudienceError`, :class:`InvalidAlgorithmError`,
  :class:`InsufficientScopeError`, :class:`JWKSUnavailableError`.

Compliance notes
----------------
* AAP R-13 — correlation-ID propagation across HTTP and Kafka headers.
* AAP R-19 — fail-fast on missing critical dependencies; configuration
  errors surface as :class:`DomainError` subclasses where appropriate.
* AAP R-21 / R-22 — JWT validation; the auth-error sub-hierarchy
  rooted at :class:`AuthError` covers the full set of token-validation
  failure modes plus the JWKS unavailability case.
* AAP R-26 — structured JSON logs; :meth:`DomainError.to_response_dict`
  produces the canonical JSON-serializable error payload.
"""
from __future__ import annotations

from typing import Any, ClassVar
from uuid import UUID


# =============================================================================
# Base class
# =============================================================================


class DomainError(Exception):
    """Root of the User Service domain exception hierarchy.

    Subclasses customize behavior via class-attribute overrides:

    * ``error_code`` — stable machine identifier (SCREAMING_SNAKE_CASE,
      e.g. ``"USER_NOT_FOUND"``).
    * ``http_status`` — HTTP status the API surface returns by default.

    Subclasses MUST NOT override ``__init__``. The standard constructor
    accepts a human-readable ``message``, optional structured ``detail``,
    optional ``correlation_id``, and optional ``cause``.

    Example:
        >>> from uuid import uuid4
        >>> raise UserNotFound(  # doctest: +SKIP
        ...     "user with id ... was not found",
        ...     detail={"user_id": "00000000-0000-0000-0000-000000000000"},
        ...     correlation_id=uuid4(),
        ... )

    Attributes:
        detail: Structured context dict (always a fresh copy of the
            caller-supplied dict, or ``{}`` when ``None`` was passed).
            JSON-serializable values only — callers stringify UUIDs,
            :class:`datetime.datetime`, and :class:`decimal.Decimal`
            before passing.
        correlation_id: Request-scoped UUID propagated by the
            correlation-ID middleware (AAP R-13). ``None`` for errors
            raised outside a request scope (e.g., startup validation,
            scheduled background work).
        cause: Underlying exception that triggered this error, when
            applicable. Stored as a domain-level attribute alongside the
            stdlib ``__cause__`` dunder (which the call site sets via
            ``raise X(...) from exc``) so log formatters and the
            ``error_handler.py`` middleware can access it without
            inspecting the dunders.

    Class attributes:
        error_code: Stable machine-readable code used to construct the
            RFC 7807 ``type`` URI in the JSON error body. Defaults to
            ``"DOMAIN_ERROR"``; subclasses override for stable identity
            across refactors that rename the Python class.
        http_status: HTTP status code returned by the global FastAPI
            exception handler when this exception propagates. Defaults
            to ``500``; subclasses override.
    """

    # Defaults that subclasses override.
    error_code: ClassVar[str] = "DOMAIN_ERROR"
    http_status: ClassVar[int] = 500

    def __init__(
        self,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        # Copy details to insulate from caller mutation.
        self.detail: dict[str, Any] = dict(detail) if detail else {}
        self.correlation_id: UUID | None = correlation_id
        # Mirror Python's ``__cause__`` semantics. Callers should still use
        # ``raise X(...) from exc`` so that traceback chaining works
        # correctly; ``self.cause`` is a parallel domain-level attribute
        # the error_handler middleware can read without consulting dunders.
        self.cause: BaseException | None = cause

    def to_response_dict(self) -> dict[str, Any]:
        """Render the exception as the JSON body of an RFC 7807 problem.

        Returns:
            A dict with the following keys (the ``type`` URI is added by
            ``error_handler.py``):

            * ``code`` — the class-level :attr:`error_code`.
            * ``status`` — the class-level :attr:`http_status`.
            * ``message`` — the message passed at construction.
            * ``correlation_id`` — string form of the UUID, or ``None``
              when no correlation id was supplied.
            * ``detail`` — a fresh copy of the structured context dict.
        """
        return {
            "code": self.error_code,
            "status": self.http_status,
            "message": str(self),
            "correlation_id": (
                str(self.correlation_id) if self.correlation_id else None
            ),
            "detail": dict(self.detail),
        }


# =============================================================================
# Resource / Identity errors
# =============================================================================


class UserNotFound(DomainError):
    """The requested user does not exist."""

    error_code: ClassVar[str] = "USER_NOT_FOUND"
    http_status: ClassVar[int] = 404


class EmailAlreadyExists(DomainError):
    """A user with this email already exists.

    Raised on UNIQUE-constraint violations against ``users.email`` —
    typically from :class:`UserRepository.create` after Postgres returns
    SQLSTATE ``23505`` (unique_violation).
    """

    error_code: ClassVar[str] = "USER_EMAIL_ALREADY_EXISTS"
    http_status: ClassVar[int] = 409


class OptimisticConcurrencyError(DomainError):
    """The user row was modified concurrently; retry with the latest version.

    Raised by ``UserRepository.update_with_version_check`` when the
    UPDATE affected 0 rows (the supplied version did not match the row's
    current version). The controller layer renders this as 409 Conflict
    with a Retry-After-aligned hint so clients can re-fetch and retry.
    """

    error_code: ClassVar[str] = "USER_OPTIMISTIC_CONCURRENCY"
    http_status: ClassVar[int] = 409


# =============================================================================
# Address errors
# =============================================================================


class AddressNotFound(DomainError):
    """The requested address does not belong to this user (or does not exist)."""

    error_code: ClassVar[str] = "USER_ADDRESS_NOT_FOUND"
    http_status: ClassVar[int] = 404


# =============================================================================
# Validation errors (HTTP 422)
# =============================================================================


class InvalidLocale(DomainError):
    """The supplied locale is not a valid BCP-47 tag."""

    error_code: ClassVar[str] = "USER_INVALID_LOCALE"
    http_status: ClassVar[int] = 422


class InvalidCountry(DomainError):
    """The supplied country code is not a valid ISO-3166-1 alpha-2 value."""

    error_code: ClassVar[str] = "USER_INVALID_COUNTRY"
    http_status: ClassVar[int] = 422


class InvalidCurrency(DomainError):
    """The supplied currency code is not a valid ISO-4217 value."""

    error_code: ClassVar[str] = "USER_INVALID_CURRENCY"
    http_status: ClassVar[int] = 422


class InvalidTimezone(DomainError):
    """The supplied timezone is not a valid IANA tz identifier."""

    error_code: ClassVar[str] = "USER_INVALID_TIMEZONE"
    http_status: ClassVar[int] = 422


class InvalidPhone(DomainError):
    """The supplied phone number could not be parsed or is not a valid number."""

    error_code: ClassVar[str] = "USER_INVALID_PHONE"
    http_status: ClassVar[int] = 422


class InvalidEmail(DomainError):
    """The supplied email is empty, malformed, or fails RFC 5321/5322 checks."""

    error_code: ClassVar[str] = "USER_INVALID_EMAIL"
    http_status: ClassVar[int] = 422


class InvalidDateOfBirth(DomainError):
    """The supplied date of birth is in the future, under min age, or over max age."""

    error_code: ClassVar[str] = "USER_INVALID_DATE_OF_BIRTH"
    http_status: ClassVar[int] = 422


class MaxAddressesExceeded(DomainError):
    """The user has reached the maximum allowed number of addresses.

    The threshold is configured via ``USER_MAX_ADDRESSES_PER_USER``
    (default 20) and supplied to ``User.add_address`` by the command
    handler.
    """

    error_code: ClassVar[str] = "USER_MAX_ADDRESSES_EXCEEDED"
    http_status: ClassVar[int] = 422


# =============================================================================
# Auth error sub-hierarchy
# =============================================================================


class AuthError(DomainError):
    """Base for authentication / authorization failures.

    These are surfaced primarily by the JWT validation path in
    ``src/middleware/auth.py``, but are defined here so the domain
    layer has a single canonical hierarchy. Subclasses override
    ``error_code`` and ``http_status`` to map to 401 (authentication)
    vs. 403 (authorization) — see AAP R-21 / R-22.
    """

    error_code: ClassVar[str] = "USER_AUTH_ERROR"
    http_status: ClassVar[int] = 401


class InvalidTokenError(AuthError):
    """The bearer token failed signature, format, or claim validation."""

    error_code: ClassVar[str] = "USER_AUTH_INVALID_TOKEN"
    http_status: ClassVar[int] = 401


class ExpiredTokenError(AuthError):
    """The bearer token's ``exp`` claim is in the past."""

    error_code: ClassVar[str] = "USER_AUTH_EXPIRED_TOKEN"
    http_status: ClassVar[int] = 401


class InvalidIssuerError(AuthError):
    """The bearer token's ``iss`` claim does not match the configured issuer."""

    error_code: ClassVar[str] = "USER_AUTH_INVALID_ISSUER"
    http_status: ClassVar[int] = 401


class InvalidAudienceError(AuthError):
    """The bearer token's ``aud`` claim does not include the User Service audience."""

    error_code: ClassVar[str] = "USER_AUTH_INVALID_AUDIENCE"
    http_status: ClassVar[int] = 401


class InvalidAlgorithmError(AuthError):
    """The bearer token's ``alg`` header is not in the configured allowed set."""

    error_code: ClassVar[str] = "USER_AUTH_INVALID_ALGORITHM"
    http_status: ClassVar[int] = 401


class InsufficientScopeError(AuthError):
    """The token authenticated successfully but lacks the required scope."""

    error_code: ClassVar[str] = "USER_AUTH_INSUFFICIENT_SCOPE"
    http_status: ClassVar[int] = 403


class JWKSUnavailableError(AuthError):
    """The Auth Service JWKS endpoint is unreachable or returned a malformed payload.

    Renders 503 Service Unavailable to signal a transient upstream
    dependency failure (the API Gateway and clients can retry). See
    AAP R-22 (bounded-TTL JWKS cache mitigates short outages).
    """

    error_code: ClassVar[str] = "USER_AUTH_JWKS_UNAVAILABLE"
    http_status: ClassVar[int] = 503


# =============================================================================
# Public API — strictly alphabetized
# =============================================================================


__all__ = [
    "AddressNotFound",
    "AuthError",
    "DomainError",
    "EmailAlreadyExists",
    "ExpiredTokenError",
    "InsufficientScopeError",
    "InvalidAlgorithmError",
    "InvalidAudienceError",
    "InvalidCountry",
    "InvalidCurrency",
    "InvalidDateOfBirth",
    "InvalidEmail",
    "InvalidIssuerError",
    "InvalidLocale",
    "InvalidPhone",
    "InvalidTimezone",
    "InvalidTokenError",
    "JWKSUnavailableError",
    "MaxAddressesExceeded",
    "OptimisticConcurrencyError",
    "UserNotFound",
]
