"""Custom exception hierarchy for the Recommendation Engine service.

This module defines the **closed** exception taxonomy used throughout the
Recommendation Engine. Every runtime exception raised by this service SHOULD
be a subclass of :class:`DomainError`. Standard Python exceptions
(``ValueError``, ``TypeError``, ``KeyError``, etc.) are acceptable at the
lowest levels but MUST NOT escape past the domain boundary — repositories,
integrations, and inference code wrap them as appropriate subclasses of
:class:`DomainError` before letting them bubble up.

Design principles
-----------------
1. **Import-graph root** — this module is the most foundational package in
   ``src/*``. It MUST NOT import from any other ``src/*`` module, from
   Pydantic, from any HTTP framework, or from the standard ``logging``
   library. Errors carry structured context as a plain ``dict[str, Any]`` —
   serialization is the caller's responsibility.
2. **Five top-level families map 1:1 to HTTP statuses**:

   ============================== ====================
   Family                         ``default_http_status``
   ============================== ====================
   :class:`NotFoundError`         404
   :class:`ValidationError`       400
   :class:`InferenceError`        503 (degraded)
   :class:`IntegrationError`      502 (upstream failure)
   :class:`ConfigError`           500 (startup / config)
   ============================== ====================

   The HTTP error-handling middleware (``src/middleware/error_handler.py``)
   uses this attribute as a simple dispatch table over ``isinstance`` checks
   instead of a sprawling mapping.
3. **Stable machine-readable error codes** — every concrete subclass exposes
   an ``error_code`` SCREAMING_SNAKE_CASE string. Consumers of the JSON
   response (mobile clients, CLI tools, dashboards) should key off the
   ``code`` field rather than the Python class name (``type``), so the
   ``error_code`` survives refactors that rename classes.
4. **Single inheritance throughout** — no class has multiple bases. Each
   family is a strict tree, simplifying reasoning about ``isinstance``
   dispatch and ``except`` clause ordering.

Compliance notes
----------------
- AAP R-13 — every ``DomainError`` carries a ``correlation_id`` so the same
  request identifier propagated by middleware and Kafka headers is preserved
  in error logs and JSON error bodies.
- AAP R-17 — :class:`InvalidEventError` and :class:`SchemaRegistryError` are
  raised by Kafka consumers and trigger routing to ``<topic>.dlq``.
- AAP R-19 — :class:`MissingConfigValueError` enforces fail-fast on missing
  critical dependencies during ``build_container``.
- AAP R-20 — :class:`InferenceError` and :class:`IntegrationError`
  subclasses trigger fallback chains (cache → popularity-based recs).
- AAP R-26 — :meth:`DomainError.to_dict` produces the JSON body shape used
  by the structured-logging error pipeline; values are JSON-serializable
  primitives only.

Cross-references
----------------
This module is imported by every other ``src/*`` package:

- ``src/middleware/error_handler.py`` — catches :class:`DomainError`, calls
  :meth:`DomainError.to_dict`, and emits the response with HTTP status from
  :attr:`DomainError.default_http_status`.
- ``src/repository/product_client.py`` — raises :class:`ProductServiceError`
  on HTTP failures and :class:`ProductNotFoundError` on 404.
- ``src/repository/embeddings_repo.py`` — raises
  :class:`UserEmbeddingMissing` for cold-start users.
- ``src/inference/runtime.py`` — raises :class:`LowConfidenceError`,
  :class:`ModelNotLoadedError`, :class:`EmbeddingDimensionMismatchError`.
- ``src/inference/model_loader.py`` — raises
  :class:`InvalidModelArtifactError` and
  :class:`EmbeddingDimensionMismatchError` on artifact load.
- ``src/events/consumer.py`` — catches :class:`InvalidEventError` and routes
  to ``<topic>.dlq``.
- ``src/config/settings.py`` and ``src/container.py`` — raise
  :class:`MissingConfigValueError` (fail-fast per AAP R-19).
- ``src/fallback/chain.py`` — catches :class:`InferenceError` and
  :class:`IntegrationError` to trigger the next fallback tier.
"""

from __future__ import annotations

from typing import Any


# =============================================================================
# Base class
# =============================================================================


class DomainError(Exception):
    """Root of the Recommendation Engine domain exception hierarchy.

    Every exception raised by this service SHOULD be a subclass of
    :class:`DomainError`. Standard Python exceptions are acceptable at the
    lowest levels but MUST be wrapped into a :class:`DomainError` subclass
    before escaping the module that produced them.

    Attributes:
        message: Short, human-readable error message.
        correlation_id: Request-scoped correlation identifier (propagated via
            middleware / Kafka headers per AAP R-13). ``None`` for errors
            raised outside a request scope (e.g., startup validation).
        details: Structured context included in logs and error responses.
            MUST be JSON-serializable (primitives, dicts, lists). Callers
            are responsible for stringifying values of non-primitive types
            (e.g., :class:`uuid.UUID`, :class:`datetime.datetime`,
            :class:`decimal.Decimal`) BEFORE passing them — :meth:`to_dict`
            does NOT coerce.
        cause: The underlying exception that triggered this error, if any.
            Preserved separately from the standard ``__cause__`` so we can
            serialize it into :meth:`to_dict` without losing the chain. The
            constructor also wires ``__cause__`` so traceback chaining works
            with or without an explicit ``raise X from Y`` clause.

    Class attributes:
        default_http_status: HTTP status code emitted by the error
            middleware for this error family. Subclasses MAY override it.
            The default 500 is a safe server-error catch-all.
        error_code: Short machine-readable code surfaced in the JSON body's
            ``code`` field. Stable across refactors that rename the class;
            subclasses override to expose a distinct identity.
    """

    # Subclasses MAY override to tune the HTTP status emitted by the error
    # middleware; the default 500 is a safe server-error catch-all.
    default_http_status: int = 500
    # Short machine-readable error code used in the JSON body's ``code`` field.
    # Default is the generic family code; subclasses override for stability
    # across refactors (e.g., when a class is renamed).
    error_code: str = "DOMAIN_ERROR"

    def __init__(
        self,
        message: str,
        *,
        correlation_id: str | None = None,
        details: dict[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        # Pass the message up to ``Exception`` so ``args`` and the default
        # ``str`` behavior of stdlib loggers continue to work as expected.
        super().__init__(message)
        self.message: str = message
        self.correlation_id: str | None = correlation_id
        # Always store a fresh dict so callers can't mutate our state by
        # holding a reference to the dict they passed in. ``None`` becomes
        # ``{}`` so downstream logging code never has to special-case it.
        self.details: dict[str, Any] = dict(details) if details is not None else {}
        self.cause: BaseException | None = cause
        # Preserve Python's standard exception chaining alongside our explicit
        # ``cause`` attribute so tracebacks remain in sync with the JSON body.
        # This makes ``raise MyError(..., cause=underlying)`` produce the same
        # chained traceback as ``raise MyError(...) from underlying``.
        if cause is not None:
            self.__cause__ = cause

    # ------------------------------------------------------------------ repr

    def __str__(self) -> str:  # noqa: D401
        """Return a human-readable one-liner for stdlib logging and ``str``.

        The output is deterministic (details keys are sorted) so that test
        assertions on ``str(exc)`` are stable across Python implementations
        that differ in dict ordering for non-string keys (vacuously true for
        our ``dict[str, Any]`` payloads, but cheap insurance).
        """
        parts: list[str] = [f"{self.__class__.__name__}: {self.message}"]
        if self.correlation_id is not None:
            parts.append(f"correlation_id={self.correlation_id}")
        if self.details:
            # Sort for deterministic output (easier test assertions and
            # log-line stability across runs).
            kv = ", ".join(f"{k}={self.details[k]!r}" for k in sorted(self.details))
            parts.append(f"details={{{kv}}}")
        if self.cause is not None:
            parts.append(f"caused_by={type(self.cause).__name__}: {self.cause}")
        return " | ".join(parts)

    def __repr__(self) -> str:  # pragma: no cover - trivial repr
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"correlation_id={self.correlation_id!r}, "
            f"details={self.details!r})"
        )

    # ----------------------------------------------------------- serialization

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON body shape used by the error middleware.

        The HTTP error middleware (``src/middleware/error_handler.py``)
        wraps this dict in a top-level ``{"error": ...}`` envelope; this
        method returns ONLY the inner error object so it can be composed by
        callers that need different envelopes (e.g., Kafka DLQ payloads).

        The returned dict is guaranteed to contain only JSON-serializable
        values when the caller has populated ``details`` with primitives
        (``str``, ``int``, ``float``, ``bool``, ``None``) or nested
        structures of primitives. Non-primitive values in ``details`` (e.g.,
        :class:`uuid.UUID`, :class:`datetime.datetime`,
        :class:`decimal.Decimal`) are passed through unchanged — the caller
        is responsible for stringifying them BEFORE construction; this
        method does NOT coerce.

        Returns:
            A dict with stable keys:

            - ``code`` — machine-readable error code
              (:attr:`error_code`).
            - ``type`` — the Python class name (verbose but precise).
            - ``message`` — the human-readable string.
            - ``correlation_id`` — may be ``None``.
            - ``details`` — the structured context payload (always a dict;
              empty when none was provided).
            - ``cause`` — string repr of :attr:`cause` in the form
              ``"ExceptionType: message"``, present only when ``cause`` is
              not ``None``.
        """
        payload: dict[str, Any] = {
            "code": self.error_code,
            "type": self.__class__.__name__,
            "message": self.message,
            "correlation_id": self.correlation_id,
            "details": self.details,
        }
        if self.cause is not None:
            payload["cause"] = f"{type(self.cause).__name__}: {self.cause}"
        return payload


# =============================================================================
# NotFoundError family — HTTP 404
# =============================================================================


class NotFoundError(DomainError):
    """Requested resource or record was not found.

    The error middleware translates this family to HTTP 404. Subclasses MAY
    refine the meaning (e.g., :class:`UserEmbeddingMissing` for cold-start
    users, :class:`ProductNotFoundError` for missing catalog entries) but
    must keep the 404 semantics.
    """

    default_http_status = 404
    error_code = "NOT_FOUND"


class UserEmbeddingMissing(NotFoundError):
    """No embedding row exists for the specified user.

    Triggered when the inference path queries the ``embeddings`` store for a
    user that was never trained (cold-start). The fallback chain
    (cache → popularity) catches this and returns a degraded response per
    AAP R-20.
    """

    error_code = "USER_EMBEDDING_MISSING"


class ProductNotFoundError(NotFoundError):
    """Product Service returned 404 (or local cache returned no record).

    Raised by ``src/repository/product_client.py`` when the upstream Product
    Service cannot resolve a product ID. The recommendation pipeline filters
    such products out of the response and continues with the remaining
    candidates rather than failing the whole request.
    """

    error_code = "PRODUCT_NOT_FOUND"


# =============================================================================
# ValidationError family — HTTP 400
# =============================================================================


class ValidationError(DomainError):
    """Input data failed domain-level validation.

    Distinct from :class:`pydantic.ValidationError` — we wrap the latter at
    the boundary (controllers, event handlers) and raise our own so
    downstream handlers don't need to import pydantic and so error responses
    carry the same shape regardless of where validation happened.
    """

    default_http_status = 400
    error_code = "VALIDATION_ERROR"


class InvalidEventError(ValidationError):
    """A Kafka event failed schema or semantic validation.

    Raised by ``src/events/dispatcher.py`` for malformed payloads and
    caught by ``src/events/consumer.py``, which routes the offending message
    to the ``<topic>.dlq`` dead-letter topic per AAP R-17 instead of
    blocking the consumer with a poison message.
    """

    error_code = "INVALID_EVENT"


class InvalidModelArtifactError(ValidationError):
    """Model file exists but fails structural validation.

    Examples: missing ``model_metadata.json``, checksum mismatch, dimension
    mismatch declared inside the metadata itself. For embedding-dimension
    mismatches detected DURING inference (runtime), see
    :class:`EmbeddingDimensionMismatchError`. For artifact corruption, this
    error is raised at startup and prevents the service from passing the
    readiness probe (AAP R-19).
    """

    error_code = "INVALID_MODEL_ARTIFACT"


# =============================================================================
# InferenceError family — HTTP 503 (service degraded)
# =============================================================================


class InferenceError(DomainError):
    """ML inference failed or produced an unusable result.

    The fallback chain (``src/fallback/chain.py``) catches this family and
    falls back to the next tier (Redis cache → popularity-based
    recommendations) per AAP R-20. The 503 status is emitted only when the
    fallback chain itself is exhausted, signaling to clients that the
    recommendations subsystem is currently degraded.
    """

    default_http_status = 503
    error_code = "INFERENCE_ERROR"


class LowConfidenceError(InferenceError):
    """Inference completed but confidence is below the configured threshold.

    Triggers fallback to cached / popularity-based recommendations rather
    than returning a low-quality result. The caller SHOULD log
    ``details={"confidence": <float>, "threshold": <float>}`` so dashboards
    can plot the confidence distribution and tune the threshold.
    """

    error_code = "LOW_CONFIDENCE"


class ModelNotLoadedError(InferenceError):
    """Inference attempted but no model is currently loaded.

    Raised when ``ModelLoader`` is in a state where no model is ready —
    typically during startup before ``load()`` completes, or after a reload
    failure. We fail closed for safety: a service that cannot serve quality
    recommendations should explicitly degrade rather than silently emit
    arbitrary outputs.
    """

    error_code = "MODEL_NOT_LOADED"


# =============================================================================
# IntegrationError family — HTTP 502 (upstream failure)
# =============================================================================


class IntegrationError(DomainError):
    """A dependency on an external or sibling service failed.

    The caller SHOULD record ``details={"service": <name>, "status": <int>}``
    and let the fallback behavior (cached response, degraded mode, circuit
    breaker) take over per AAP R-20. The 502 status communicates that the
    upstream — not this service — is the root cause, allowing operators to
    triage faster.
    """

    default_http_status = 502
    error_code = "INTEGRATION_ERROR"


class ProductServiceError(IntegrationError):
    """Product Service HTTP call failed (non-2xx, timeout, or connection error).

    The circuit breaker on the Product Service client (AAP R-16) may be open
    when this error is raised; downstream code SHOULD NOT retry in a tight
    loop — the breaker's half-open probe is the correct retry surface.
    """

    error_code = "PRODUCT_SERVICE_ERROR"


class SchemaRegistryError(IntegrationError):
    """Confluent Schema Registry is unreachable or returned an error.

    Raised during Kafka produce/consume when the schema lookup fails. The
    consumer SHOULD route the affected message to ``<topic>.dlq`` per AAP
    R-17 rather than blocking the partition with a poison message.
    """

    error_code = "SCHEMA_REGISTRY_ERROR"


# =============================================================================
# ConfigError family — HTTP 500 (startup / config)
# =============================================================================


class ConfigError(DomainError):
    """Configuration is missing or invalid.

    Typically raised during startup (``build_container``, ``get_settings``)
    to enforce the fail-fast rule (AAP R-19). MAY also be raised at runtime
    when a previously-optional config value is read for the first time. The
    500 status is the safe default; readiness probes should NOT pass while a
    :class:`ConfigError` is unresolved.
    """

    default_http_status = 500
    error_code = "CONFIG_ERROR"


class MissingConfigValueError(ConfigError):
    """A required environment variable or config key is absent or empty.

    Raised during startup when a critical dependency (``POSTGRES_URL``,
    ``REDIS_URL``, ``KAFKA_BOOTSTRAP``, ``MODEL_PATH``,
    ``JWT_PUBLIC_KEY_URL``, etc.) cannot be resolved. Fail-fast per AAP
    R-19: better to crash on boot than to silently degrade in production.
    """

    error_code = "MISSING_CONFIG_VALUE"


class EmbeddingDimensionMismatchError(ConfigError):
    """An embedding vector's length does not match the configured model dim.

    Raised when an :class:`Embedding` object is constructed or persisted
    with a ``vector`` whose length differs from
    ``settings.model.embedding_dim`` (default 128 per the folder spec). This
    typically indicates a corrupted model artifact or an incompatible model
    version being served. Also raised at startup during model load if
    ``ModelMetadata.embedding_dim`` differs from the configured value, in
    which case the service refuses to boot (AAP R-19).
    """

    error_code = "EMBEDDING_DIMENSION_MISMATCH"


# =============================================================================
# Public API
# =============================================================================
# Ordered: base → top-level family → leaves grouped under each family. This
# preserves a readable mental model for developers auditing the hierarchy
# and matches the order in the folder-spec tree.
__all__ = [
    # Base
    "DomainError",
    # NotFoundError family
    "NotFoundError",
    "UserEmbeddingMissing",
    "ProductNotFoundError",
    # ValidationError family
    "ValidationError",
    "InvalidEventError",
    "InvalidModelArtifactError",
    # InferenceError family
    "InferenceError",
    "LowConfidenceError",
    "ModelNotLoadedError",
    # IntegrationError family
    "IntegrationError",
    "ProductServiceError",
    "SchemaRegistryError",
    # ConfigError family
    "ConfigError",
    "MissingConfigValueError",
    "EmbeddingDimensionMismatchError",
]
