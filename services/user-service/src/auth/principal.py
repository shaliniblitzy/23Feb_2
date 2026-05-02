"""``Principal`` value object — the authenticated caller in the User Service.

A :class:`Principal` is constructed exclusively by
:class:`src.auth.jwt_validator.JWTValidator` after a JWT has been
fully verified. It carries the minimum information that the rest of
the application needs to make authorization decisions and to forward
the token to downstream services:

* :attr:`subject` — the JWT ``sub`` claim, which is the external auth
  identity used to look up the user's local record in the User Service
  (see :class:`src.domain.user.User.external_auth_id`).
* :attr:`scopes` — the parsed OAuth 2.0 scopes (RFC 6749 §3.3) as a
  ``frozenset[str]``.
* :attr:`claims` — a read-only mapping of every verified JWT claim,
  available to handlers that need additional fields (e.g.,
  ``email``, ``name``, ``preferred_username``).
* :attr:`raw_token` — the original compact-serialized JWT, used by the
  outbound HTTP client to forward authentication on service-to-service
  calls (e.g., User Service → Auth Service /admin/users introspection).

Immutability
------------
The class is a frozen dataclass with ``slots=True``: assigning to any
attribute after construction raises :class:`dataclasses.FrozenInstanceError`.
The ``claims`` mapping is wrapped in :class:`types.MappingProxyType`
to prevent in-place mutation (e.g., ``principal.claims["sub"] = "x"``
raises :class:`TypeError`).

Equality and hashing
--------------------
Two ``Principal`` instances are equal iff all four fields are equal.
The auto-generated ``__hash__`` from ``frozen=True`` works for the
common case where ``claims`` is empty or not used as a hash component
because ``frozenset`` and ``str`` are both hashable. Because the
underlying dict wrapped by :class:`MappingProxyType` is not hashable,
hashing a Principal whose claims contain mutable values will raise
:class:`TypeError` at hash-time — that is acceptable; Principals are
not used as dict keys in production.

AAP traceability
----------------
* AAP Section 0.4.5 — Authentication middleware emits a Principal.
* AAP R-21 — The Principal is the *only* artifact carrying user
  identity into the request — never the raw token, never user input.
* AAP R-23 — Scope parsing follows RFC 6749 §3.3.

This module has **no internal dependencies**. It must remain
import-cheap so that any module in the auth subsystem can depend on it
without circular-import risk.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

# Per RFC 6749 §3.3, scope is a space-delimited string. We split on any
# ASCII whitespace per the standard `str.split()` default behavior, which
# also handles tabs/newlines defensively (some IDPs are lax).
_SCOPE_SEPARATOR_NOTE: Final[str] = (
    "RFC 6749 §3.3: scope = scope-token *( SP scope-token )"
)

# An empty mapping proxy used as the default for ``claims`` when callers
# construct a Principal without claims (rare; primarily for tests).
_EMPTY_CLAIMS: Final[Mapping[str, Any]] = MappingProxyType({})


# ---------------------------------------------------------------------------
# Principal — the immutable authenticated-caller value object
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Principal:
    """Immutable representation of the authenticated caller.

    Attributes:
        subject: The JWT ``sub`` claim. This is the external auth
            identity (e.g., a UUID issued by Auth Service) used by the
            User Service to look up the local
            :class:`src.domain.user.User` record via
            ``external_auth_id``. Never empty.
        scopes: The frozen set of OAuth 2.0 scopes parsed from the
            verified JWT ``scope`` claim (RFC 6749 §3.3) or the
            ``scp`` claim (Microsoft / Auth0 convention). Tested for
            membership by :func:`src.auth.scopes.require_scope`.
        claims: A read-only mapping of every verified JWT claim. Stored
            as a :class:`types.MappingProxyType` so handlers cannot
            mutate the underlying dict. Includes ``sub``, ``iss``,
            ``aud``, ``exp``, ``iat``, ``nbf`` (if present), and any
            custom claims (e.g., ``email``, ``name``).
        raw_token: The original compact-serialized JWT (no ``Bearer``
            prefix). Used by outbound HTTP clients to forward
            authentication to downstream services (e.g., to query the
            Auth Service's introspection endpoint).

    Example:
        >>> principal = Principal(
        ...     subject="auth0|abc123",
        ...     scopes=frozenset({"users:read", "users:self"}),
        ...     claims={
        ...         "sub": "auth0|abc123",
        ...         "iss": "https://auth.example.com",
        ...         "aud": "user-service",
        ...         "exp": 1700000000,
        ...         "iat": 1699999000,
        ...     },
        ...     raw_token="eyJhbGciOiJSUzI1NiIs...",
        ... )
        >>> "users:read" in principal.scopes
        True

    Notes:
        * ``__post_init__`` normalizes ``claims`` into a
          :class:`types.MappingProxyType` to enforce immutability and
          accepts any ``Mapping[str, Any]`` from the validator (which
          always passes a freshly-constructed ``dict``).
        * ``__post_init__`` also coerces ``scopes`` to a
          ``frozenset[str]`` if a plain set, list, or tuple is passed
          (defensive parameter handling for tests).
        * Frozen dataclass means assigning to any field raises
          :class:`dataclasses.FrozenInstanceError`.
        * ``slots=True`` means assigning a new attribute raises
          :class:`AttributeError`.
    """

    subject: str
    scopes: frozenset[str] = field(default_factory=frozenset)
    claims: Mapping[str, Any] = field(default_factory=lambda: _EMPTY_CLAIMS)
    raw_token: str = ""

    # ------------------------------------------------------------------
    # Normalization & validation
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        """Validate and normalize fields immediately after construction.

        Because the dataclass is ``frozen=True``, direct attribute
        assignment raises :class:`dataclasses.FrozenInstanceError`. The
        documented escape hatch for one-time normalization is
        :func:`object.__setattr__`, which bypasses the frozen check.

        Raises:
            TypeError: ``subject``, ``scopes``, ``claims``, or
                ``raw_token`` has the wrong type.
            ValueError: ``subject`` is an empty string.
        """
        # ---- subject -------------------------------------------------
        # JWT validator guarantees a non-empty string, but the
        # constructor is public, so we re-validate here defensively.
        if not isinstance(self.subject, str):
            raise TypeError(
                f"subject must be str, got {type(self.subject).__name__}"
            )
        if not self.subject:
            raise ValueError("subject must be non-empty")

        # ---- scopes --------------------------------------------------
        # Coerce any iterable of non-empty strings to a ``frozenset[str]``.
        # We use ``object.__setattr__`` because the dataclass is frozen.
        if not isinstance(self.scopes, frozenset):
            if isinstance(self.scopes, str) or not isinstance(
                self.scopes, Iterable
            ):
                # Strings ARE iterable but iterating over a string yields
                # individual characters — which is virtually never what a
                # caller intends for a scopes parameter. Reject explicitly
                # to surface the usage error at construction time.
                raise TypeError(
                    f"scopes must be an iterable of strings, got "
                    f"{type(self.scopes).__name__}"
                )
            coerced: list[str] = []
            for s in self.scopes:
                if not isinstance(s, str):
                    raise TypeError(
                        f"every scope must be str, got "
                        f"{type(s).__name__}"
                    )
                if s:
                    # Filter out empty strings defensively so the set
                    # never contains an empty token (which would surprise
                    # readers querying ``"" in principal.scopes``).
                    coerced.append(s)
            object.__setattr__(self, "scopes", frozenset(coerced))
        else:
            # Already a frozenset — validate every element is a string.
            for s in self.scopes:
                if not isinstance(s, str):
                    raise TypeError(
                        f"every scope must be str, got "
                        f"{type(s).__name__}"
                    )

        # ---- claims --------------------------------------------------
        # Wrap claims in a ``MappingProxyType`` to prevent in-place
        # mutation. Any caller-provided dict is copied first so that
        # subsequent caller mutations cannot leak through the proxy.
        if not isinstance(self.claims, Mapping):
            raise TypeError(
                f"claims must be a Mapping, got "
                f"{type(self.claims).__name__}"
            )
        if not isinstance(self.claims, MappingProxyType):
            # Defensive copy: isolates the principal from later
            # mutations of the caller's original dict.
            object.__setattr__(
                self, "claims", MappingProxyType(dict(self.claims))
            )

        # ---- raw_token -----------------------------------------------
        # Must be a string. May be empty in tests / synthetic
        # constructions where a forwardable token is not required.
        if not isinstance(self.raw_token, str):
            raise TypeError(
                f"raw_token must be str, got "
                f"{type(self.raw_token).__name__}"
            )

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def has_token(self) -> bool:
        """Whether this principal carries a forwardable raw token.

        Returns:
            ``True`` iff :attr:`raw_token` is a non-empty string.
        """
        return bool(self.raw_token)

    # ------------------------------------------------------------------
    # Scope-membership helpers
    # ------------------------------------------------------------------

    def has_scope(self, scope: str) -> bool:
        """Test whether the principal carries the given scope.

        Args:
            scope: The scope string to test (e.g., ``"users:admin"``).

        Returns:
            ``True`` iff the scope is in :attr:`scopes`.
        """
        return scope in self.scopes

    def has_any_scope(self, scopes: Iterable[str]) -> bool:
        """Test whether the principal carries at least one of the given scopes.

        Args:
            scopes: An iterable of scope strings to test.

        Returns:
            ``True`` iff at least one element of ``scopes`` is in
            :attr:`scopes`. ``False`` if ``scopes`` is empty.
        """
        return any(s in self.scopes for s in scopes)

    def has_all_scopes(self, scopes: Iterable[str]) -> bool:
        """Test whether the principal carries every one of the given scopes.

        Args:
            scopes: An iterable of scope strings to test.

        Returns:
            ``True`` iff every element of ``scopes`` is in
            :attr:`scopes`. ``True`` (vacuously) if ``scopes`` is empty.
        """
        return all(s in self.scopes for s in scopes)

    # ------------------------------------------------------------------
    # Safe representations — never include sensitive material
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        """Return a redacted string representation safe for logs.

        The representation deliberately excludes :attr:`raw_token`
        (credential material) and :attr:`claims` (potentially PII such
        as email, name, picture URL). Only :attr:`subject` and a sorted
        view of :attr:`scopes` are emitted so that log lines remain
        useful for authorization debugging without leaking secrets.
        """
        return (
            f"Principal(subject={self.subject!r}, "
            f"scopes={sorted(self.scopes)!r})"
        )

    def __str__(self) -> str:
        """Mirror :meth:`__repr__` so ``str(principal)`` is also redacted."""
        return self.__repr__()

    # ------------------------------------------------------------------
    # Alternate constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_claims(
        cls,
        *,
        claims: Mapping[str, Any],
        raw_token: str = "",
    ) -> Principal:
        """Build a :class:`Principal` from a verified JWT claims mapping.

        This is a convenience constructor used by tests and by callers
        that prefer to delegate scope parsing to ``Principal``. Production
        code in :class:`src.auth.jwt_validator.JWTValidator` may use this
        helper OR construct via the direct keyword form
        (``Principal(subject=..., scopes=..., claims=..., raw_token=...)``)
        — both are supported.

        Scope parsing follows RFC 6749 §3.3 (space-delimited ``scope``)
        with fallback to the Microsoft Entra / Auth0 convention of
        ``scp`` (which may be either a string or an array of strings).

        Args:
            claims: The verified JWT payload mapping (output of
                :func:`jwt.decode`). Must contain a ``sub`` claim of
                type ``str``.
            raw_token: The original compact-serialized JWT, optional
                (default ``""``).

        Returns:
            A new :class:`Principal`.

        Raises:
            ValueError: ``claims`` does not contain a non-empty ``sub``
                string claim.
            TypeError: ``claims`` is not a mapping; ``raw_token`` is
                not a string.
        """
        if not isinstance(claims, Mapping):
            raise TypeError(
                f"claims must be a Mapping, got {type(claims).__name__}"
            )
        sub = claims.get("sub")
        if not isinstance(sub, str) or not sub:
            raise ValueError(
                "claims must contain a non-empty 'sub' string claim"
            )
        scopes = cls._parse_scopes(claims)
        return cls(
            subject=sub,
            scopes=scopes,
            claims=claims,
            raw_token=raw_token,
        )

    @staticmethod
    def _parse_scopes(claims: Mapping[str, Any]) -> frozenset[str]:
        """Extract scopes from a JWT claims mapping per RFC 6749 §3.3.

        Resolution order:

        1. ``scope`` — a single string, space-delimited per the RFC.
        2. ``scp``  — a list of strings (Microsoft Entra / Auth0 style).
        3. ``scp``  — a single string, space-delimited (fallback).
        4. Empty ``frozenset`` if no recognized scope claim is present.

        Empty tokens (resulting from leading, trailing, or consecutive
        whitespace) are filtered out via the truthiness guard ``if s``.

        Args:
            claims: The verified JWT payload mapping.

        Returns:
            A ``frozenset[str]`` of scope tokens. Empty if no scope
            claim is present or all candidates are empty.
        """
        scope_claim = claims.get("scope")
        if isinstance(scope_claim, str) and scope_claim.strip():
            # ``str.split()`` with no separator splits on runs of any
            # ASCII whitespace and drops empty leading/trailing tokens —
            # matching RFC 6749 §3.3's space-delimited grammar while
            # tolerating IDPs that emit tabs or newlines.
            return frozenset(s for s in scope_claim.split() if s)

        scp_claim = claims.get("scp")
        if isinstance(scp_claim, list):
            return frozenset(
                s for s in scp_claim if isinstance(s, str) and s
            )
        if isinstance(scp_claim, str) and scp_claim.strip():
            return frozenset(s for s in scp_claim.split() if s)

        return frozenset()


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------

__all__ = [
    "Principal",
]
