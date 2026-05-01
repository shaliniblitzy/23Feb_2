"""Custom exception hierarchy for the Payment Service.

This module defines the **closed** exception taxonomy used throughout the
Payment Service. Every runtime exception raised by this service SHOULD be a
subclass of :class:`PaymentServiceError`. Standard Python exceptions
(``ValueError``, ``TypeError``, ``KeyError``, etc.) are acceptable at the
lowest levels but MUST NOT escape past the domain boundary --- providers,
repositories, controllers, the webhook pipeline, idempotency middleware,
and event handlers wrap them as appropriate subclasses of
:class:`PaymentServiceError` before letting them bubble up.

Design principles
-----------------
1. **Import-graph root** --- this module is the most foundational package
   in ``services/payment-service/src/*``. It MUST NOT import from any
   other ``src/*`` module, from Pydantic, from any HTTP framework, or
   from the standard ``logging`` library. Errors carry structured context
   as a plain ``dict[str, Any]`` --- serialization is the caller's
   responsibility. Keeping the module pure is a deliberate, defensive
   discipline: any side effect here pollutes every test and every
   downstream module.
2. **Flat hierarchy** --- the Payment Service's error taxonomy is
   intentionally flat (one base plus fifteen leaves). The leaves do not
   split cleanly into families (e.g., :class:`WebhookSignatureError` and
   :class:`IdempotencyMismatchError` are not really "validation" errors;
   :class:`PaymentDeniedError` is not really an "integration" error).
   A flat hierarchy is clearer here. Future growth can introduce family
   classes if needed, but premature grouping creates misplaced leaves.
3. **Per-leaf HTTP status mapping** --- each leaf overrides
   :attr:`PaymentServiceError.http_status` to communicate the appropriate
   client-facing status code. The HTTP error-handling middleware
   (``src/middleware/error_handler.py``) reads this attribute as a simple
   dispatch table over ``isinstance`` checks instead of a sprawling
   mapping.
4. **Stable machine-readable error codes** --- every concrete subclass
   exposes an ``error_code`` SCREAMING_SNAKE_CASE string. Consumers of
   the JSON response (mobile clients, CLI tools, dashboards) should key
   off the ``error`` field rather than the Python class name (``type``),
   so the ``error_code`` survives refactors that rename classes.
5. **Single inheritance throughout** --- no class has multiple bases.
   Each leaf inherits DIRECTLY from :class:`PaymentServiceError`, never
   from another leaf. This simplifies reasoning about ``isinstance``
   dispatch and ``except`` clause ordering.
6. **No per-subclass ``__init__``** --- the base class's ``__init__`` is
   sufficient for all subclasses; adding per-subclass ``__init__``
   methods would duplicate code and introduce subtle bugs. Subclasses
   customize via class attributes (``error_code``, ``http_status``)
   only.

Compliance notes
----------------
- AAP Section 0.1.1 Component #7 --- Payment Service (dual-provider
  Stripe + Razorpay).
- AAP Section 0.4.4 --- ``payment_db`` schema (encrypted at rest);
  :class:`EncryptionError` / :class:`DecryptionError` /
  :class:`DatabaseConnectionError` capture infra-level failures around
  the encrypted store.
- AAP Section 0.4.5 --- middleware / interceptor stack (the error
  handler consumes these errors).
- AAP R-8 --- idempotency keys + encryption at rest;
  :class:`IdempotencyMismatchError`, :class:`EncryptionError`,
  :class:`DecryptionError` enforce these contracts.
- AAP R-10 --- dual-provider concurrent integration;
  :class:`UnknownProviderError`, :class:`NoCompatibleProviderError`,
  :class:`ProviderUnavailableError` are the routing-layer signals.
- AAP R-12 --- webhook signature verification;
  :class:`WebhookSignatureError`, :class:`WebhookBodyTooLargeError`.
- AAP R-13 --- correlation-ID propagation --- every
  :class:`PaymentServiceError` carries a ``correlation_id`` so the same
  request identifier propagated by middleware and Kafka headers is
  preserved in error logs and JSON error bodies.
- AAP R-15 --- retry with exponential backoff + jitter; provider
  transient errors trigger retry.
- AAP R-16 --- circuit breaker; :class:`ProviderUnavailableError` is
  raised when the per-provider breaker is OPEN AND no fallback is
  available.
- AAP R-19 --- fail fast on missing critical dependencies at startup;
  :class:`ConfigurationError`, :class:`DatabaseConnectionError`.
- AAP R-20 --- fallback behavior declared for every external
  dependency.
- AAP R-25 --- secrets never in source; ``details`` MUST NOT contain
  secrets, full PANs, or provider API keys.
- AAP R-26 --- structured JSON logs;
  :meth:`PaymentServiceError.to_response_dict` produces the
  serializable error shape.

Cross-references
----------------
This module is imported by every other ``src/*`` package in the
Payment Service:

- ``src/middleware/error_handler.py`` --- catches
  :class:`PaymentServiceError`, calls
  :meth:`PaymentServiceError.to_response_dict` for the body and
  :meth:`PaymentServiceError.to_status_code` for the HTTP status.
- ``src/controllers/payments.py`` --- raises
  :class:`PaymentDeniedError`, :class:`ProviderUnavailableError`,
  :class:`NoCompatibleProviderError`,
  :class:`IdempotencyHeaderMissingError`.
- ``src/controllers/refunds.py`` --- raises
  :class:`OrderNotFoundError`, :class:`PaymentNotFoundError`,
  :class:`RefundIneligibleError`.
- ``src/controllers/webhooks.py`` --- raises
  :class:`WebhookSignatureError`, :class:`WebhookBodyTooLargeError`.
- ``src/providers/registry.py`` --- raises
  :class:`UnknownProviderError`.
- ``src/providers/routing.py`` --- raises
  :class:`NoCompatibleProviderError`,
  :class:`ProviderUnavailableError`.
- ``src/providers/stripe/stripe_provider.py`` and
  ``src/providers/razorpay/razorpay_provider.py`` --- translate provider
  SDK exceptions to :class:`PaymentDeniedError` /
  :class:`ProviderUnavailableError`.
- ``src/repository/payments_repo.py``,
  ``src/repository/refunds_repo.py`` --- raise
  :class:`PaymentNotFoundError`, :class:`OrderNotFoundError`,
  :class:`DatabaseConnectionError`.
- ``src/repository/encryption.py`` --- raises :class:`EncryptionError`
  on encrypt failure and :class:`DecryptionError` on decrypt failure.
- ``src/idempotency/middleware.py`` --- raises
  :class:`IdempotencyMismatchError`,
  :class:`IdempotencyHeaderMissingError`.
- ``src/idempotency/store.py`` --- raises
  :class:`IdempotencyMismatchError` on hash mismatch.
- ``src/webhook/verifier.py`` --- raises :class:`WebhookSignatureError`
  on HMAC mismatch.
- ``src/config/settings.py`` --- raises :class:`ConfigurationError` when
  required env vars are missing or invalid.
- ``src/container.py`` --- propagates :class:`ConfigurationError` at
  startup (fail-fast per AAP R-19).
"""

from __future__ import annotations

from typing import Any


# =============================================================================
# Base class
# =============================================================================


class PaymentServiceError(Exception):
    """Root of the Payment Service domain exception hierarchy.

    Every exception raised by this service SHOULD be a subclass of
    :class:`PaymentServiceError`. Standard Python exceptions are
    acceptable at the lowest levels but MUST be wrapped into a
    :class:`PaymentServiceError` subclass before escaping the module
    that produced them.

    Attributes:
        message: Short, human-readable error message. MUST NOT include
            PII, card data, secrets, or provider API keys.
        correlation_id: Request-scoped correlation identifier (propagated
            via middleware / Kafka headers per AAP R-13). ``None`` for
            errors raised outside a request scope (e.g., startup
            validation, background scheduler tick).
        details: Structured context included in logs and error responses.
            MUST be JSON-serializable (primitives, dicts, lists). MUST
            NOT contain PII / secrets / full PANs (AAP R-25). Callers
            are responsible for stringifying values of non-primitive
            types (e.g., :class:`uuid.UUID`,
            :class:`datetime.datetime`, :class:`decimal.Decimal`)
            BEFORE passing them --- :meth:`to_response_dict` does NOT
            coerce.
        cause: The underlying exception that triggered this error, if
            any. Preserved separately from the standard ``__cause__`` so
            we can serialize it into :meth:`to_response_dict` without
            losing the chain. The constructor also wires ``__cause__``
            so traceback chaining works with or without an explicit
            ``raise X from Y`` clause.

    Class attributes:
        http_status: HTTP status code emitted by the error middleware
            for this error. Subclasses MAY override it. The default
            ``500`` is a safe server-error catch-all.
        error_code: Short machine-readable code surfaced in the JSON
            body's ``error`` field. Stable across refactors that rename
            the class; subclasses override to expose a distinct
            identity.

    Example:
        >>> err = PaymentServiceError("oops", correlation_id="cid-123",
        ...                           details={"k": "v"})
        >>> err.message
        'oops'
        >>> err.correlation_id
        'cid-123'
        >>> err.details
        {'k': 'v'}
        >>> err.to_response_dict()["error"]
        'PAYMENT_SERVICE_ERROR'
        >>> err.to_status_code()
        500
    """

    # Subclasses MAY override to tune the HTTP status emitted by the
    # error middleware; the default 500 is a safe server-error catch-all.
    http_status: int = 500
    # Short machine-readable error code used in the JSON body's ``error``
    # field. Default is the generic family code; subclasses override for
    # stability across refactors (e.g., when a class is renamed).
    error_code: str = "PAYMENT_SERVICE_ERROR"

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
        # Preserve Python's standard exception chaining alongside our
        # explicit ``cause`` attribute so tracebacks remain in sync with
        # the JSON body. This makes ``raise MyError(..., cause=underlying)``
        # produce the same chained traceback as
        # ``raise MyError(...) from underlying``.
        if cause is not None:
            self.__cause__ = cause

    # --------------------------------------------------------- representations

    def __str__(self) -> str:
        """Return a human-readable one-liner for stdlib logging and ``str``.

        The output is deterministic (details keys are sorted) so that test
        assertions on ``str(exc)`` are stable across Python implementations
        that differ in dict ordering for non-string keys (vacuously true
        for our ``dict[str, Any]`` payloads, but cheap insurance).

        Returns:
            A pipe-separated one-liner: ``"<ClassName>: <message>"``
            optionally followed by ``correlation_id=<id>``,
            ``details={<sorted kv pairs>}``, and
            ``caused_by=<ExceptionType>: <message>`` segments when those
            attributes are populated.
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

    def to_response_dict(self) -> dict[str, Any]:
        """Return the JSON body shape used by the error middleware.

        Schema (per folder spec)::

            {
                "error": "<error_code>",
                "message": "<safe-description>",
                "correlation_id": "<id>" or null,
                "details": {...},
                "cause": "<ExceptionType>: <message>"  # only when set
            }

        The middleware emits this dict directly as the response body
        (no outer envelope) and sets the HTTP status from
        :meth:`to_status_code`.

        Returns:
            A JSON-serializable dict.

        Note:
            When ``details`` contains a value of a non-primitive type
            (e.g., a UUID), the caller is responsible for stringifying it
            BEFORE passing it. This method does NOT coerce non-primitives.
            ``details`` MUST NOT contain PII, full PANs, secrets, or
            provider API keys (AAP R-25).
        """
        payload: dict[str, Any] = {
            "error": self.error_code,
            "message": self.message,
            "correlation_id": self.correlation_id,
            "details": self.details,
        }
        if self.cause is not None:
            payload["cause"] = f"{type(self.cause).__name__}: {self.cause}"
        return payload

    def to_status_code(self) -> int:
        """Return the HTTP status code for this exception.

        Reads :attr:`http_status` (which subclasses override). The error
        middleware uses this value to set the HTTP status on the
        response.

        Returns:
            The HTTP status code as an integer.
        """
        return self.http_status


# =============================================================================
# Configuration / startup errors
# =============================================================================


class ConfigurationError(PaymentServiceError):
    """Startup-time misconfiguration (AAP R-19 --- fail fast).

    Raised during :class:`Settings` validation, ``build_container``, or
    provider construction when a required configuration value is missing
    or invalid (e.g., neither ``STRIPE_ENABLED`` nor ``RAZORPAY_ENABLED``
    is true; invalid KMS provider; missing JWKS URL; malformed Kafka
    bootstrap string).

    The recommended response to a :class:`ConfigurationError` raised at
    startup is to fail fast and exit the process with a non-zero status
    so the orchestrator (Kubernetes, Docker Compose) can surface the
    failure via standard health checks instead of starting an unhealthy
    instance.

    Recommended ``details`` fields:
        * ``setting`` (str) --- the name of the missing or invalid
          configuration variable.
        * ``reason`` (str) --- a brief explanation of why the value is
          unacceptable.

    NOTE:
        NEVER include the actual setting value if it could be a secret
        (e.g., API keys, DB passwords, KMS key IDs). Echoing a typo in a
        public hostname is fine; echoing a leaked credential is a
        security incident waiting to happen (AAP R-25).
    """

    http_status = 500
    error_code = "CONFIGURATION_ERROR"


# =============================================================================
# Provider routing / registry errors
# =============================================================================


class UnknownProviderError(PaymentServiceError):
    """Provider registry lookup miss.

    Raised by :class:`ProviderRegistry.get` when a caller requests a
    provider that is not currently registered (e.g., requesting
    ``"stripe"`` when ``STRIPE_ENABLED=false``). This typically
    indicates a routing-rule bug; operators should investigate
    immediately because the request is well-formed but the routing
    layer is asking for a provider the registry cannot supply.

    Returns HTTP 500 (Internal Server Error) because this represents a
    misconfiguration or routing bug, not a client error.

    Recommended ``details`` fields:
        * ``provider`` (str) --- the requested provider name.
        * ``available_providers`` (list[str]) --- the list of providers
          the registry currently serves.
    """

    http_status = 500
    error_code = "UNKNOWN_PROVIDER"


class NoCompatibleProviderError(PaymentServiceError):
    """No registered provider supports the requested currency / region.

    Raised by :class:`ProviderRouter.select` when neither Stripe nor
    Razorpay can process the requested currency (e.g., a request for
    ``INR`` with only Stripe registered, or for an exotic currency
    neither provider supports). Returns HTTP 422 (Unprocessable Entity)
    because the request itself is well-formed but cannot be satisfied
    in the current provider configuration.

    Recommended ``details`` fields:
        * ``currency`` (str) --- the requested ISO 4217 currency code.
        * ``region`` (str | None) --- the merchant region, if known.
        * ``available_providers`` (list[str]) --- the providers the
          router considered.
    """

    http_status = 422
    error_code = "NO_COMPATIBLE_PROVIDER"


class ProviderUnavailableError(PaymentServiceError):
    """Provider's circuit breaker is open AND no fallback is available.

    Raised when:

    * The provider's circuit breaker is OPEN (AAP R-16).
    * All retry attempts within the current request window have been
      exhausted (AAP R-15).
    * The fallback provider (if any) is also unavailable (AAP R-20).

    Callers MAY return HTTP 503 with a ``Retry-After`` header. The
    Order Service saga interprets this as a transient failure and may
    compensate (release inventory reservation, mark order as cancelled)
    rather than retrying immediately.

    Recommended ``details`` fields:
        * ``provider`` (str) --- the provider that became unavailable.
        * ``circuit_state`` (str) --- the breaker's current state
          (``"OPEN"`` / ``"HALF_OPEN"``).
        * ``retry_after_seconds`` (int | None) --- recommended retry
          delay; ``None`` if no estimate is available.
        * ``fallback_provider`` (str | None) --- the fallback that was
          attempted, or ``None`` if no fallback was configured.
    """

    http_status = 503
    error_code = "PROVIDER_UNAVAILABLE"


# =============================================================================
# Provider response / business errors
# =============================================================================


class PaymentDeniedError(PaymentServiceError):
    """Provider returned a hard denial (declined card, fraud signal, ...).

    Raised when the provider explicitly rejects the charge (e.g., Stripe
    ``card_declined``, Razorpay ``BAD_REQUEST_ERROR`` with a
    ``payment_failed`` code). NOT retryable --- the saga should NOT
    compensate-and-retry on this error; instead it cancels the order
    and emits a ``payment.failed`` event so the customer can be
    notified to update their payment method.

    Returns HTTP 402 (Payment Required), the precise semantic match for
    "the request is denied for payment reasons." RFC 7231 reserves 402
    for payment-related rejections; modern usage (Stripe, RFC drafts)
    is increasingly aligned with this interpretation.

    Recommended ``details`` fields:
        * ``provider`` (str) --- the provider that issued the denial.
        * ``decline_code`` (str) --- the provider-specific decline code
          (e.g., ``"card_declined"``, ``"insufficient_funds"``).
        * ``decline_message`` (str | None) --- the provider's
          human-readable decline reason; OK to surface to the user
          (NOT the raw provider error body, which may contain PII).
    """

    http_status = 402
    error_code = "PAYMENT_DENIED"


# =============================================================================
# Webhook errors
# =============================================================================


class WebhookSignatureError(PaymentServiceError):
    """Webhook signature mismatch or replay window violated.

    Per the folder spec, the controller responds with HTTP 200 OK to
    prevent provider retry storms. Stripe and Razorpay both treat
    4xx/5xx responses on webhook endpoints as retry triggers; returning
    a 401/403/500 floods our endpoint with retries, potentially
    creating a self-DoS. By returning 200 OK while logging the failure,
    we suppress retries and rely on monitoring (Kibana dashboard for
    ``WEBHOOK_SIGNATURE_INVALID`` count) to surface the issue.

    The middleware's :meth:`PaymentServiceError.to_status_code` reports
    200 here, and the ``error_handler`` middleware MUST NOT log this as
    a server error --- it is a security-relevant signal but expected to
    occur occasionally (replays, misconfigured webhook endpoints,
    provider testing).

    Recommended ``details`` fields:
        * ``provider`` (str) --- the provider whose webhook was rejected.
        * ``reason`` (str) --- the verification failure reason
          (e.g., ``"hmac_mismatch"``, ``"timestamp_outside_tolerance"``).

    NOTE:
        DO NOT include the raw signature, the body, or any header
        values that could leak secrets or PII (AAP R-25).
    """

    http_status = 200
    error_code = "WEBHOOK_SIGNATURE_INVALID"


class WebhookBodyTooLargeError(PaymentServiceError):
    """Webhook body exceeds ``WEBHOOK_MAX_BODY_BYTES``.

    Raised by the webhook controller before reading the full body to
    prevent memory exhaustion attacks. Stripe and Razorpay's legitimate
    webhooks fit comfortably under any reasonable limit (typically
    <1KB), so a body that exceeds the configured ceiling is a strong
    signal of either misconfiguration or an attack.

    Returns HTTP 413 (Payload Too Large), the standard RFC 7231 status
    for over-sized requests.

    Recommended ``details`` fields:
        * ``provider`` (str) --- the provider whose webhook was rejected.
        * ``body_size_bytes`` (int) --- the actual body size in bytes
          (or the size at the point reading was aborted).
        * ``limit_bytes`` (int) --- the configured maximum body size.
    """

    http_status = 413
    error_code = "WEBHOOK_BODY_TOO_LARGE"


# =============================================================================
# Idempotency errors
# =============================================================================


class IdempotencyMismatchError(PaymentServiceError):
    """Same idempotency key, different request body.

    Raised by :class:`IdempotencyMiddleware` (or
    :class:`IdempotencyStore`) when a replayed request uses a
    previously-seen idempotency key but the canonical hash of its
    request body differs from the one cached against that key.
    Returning HTTP 409 (Conflict) tells the client they have a bug ---
    they are reusing an idempotency key for a different operation,
    which violates the idempotency contract (AAP R-8).

    Recommended ``details`` fields:
        * ``idempotency_key`` (str) --- the offending key.

    NOTE:
        DO NOT include either the original or the replayed request
        body in ``details`` --- they could leak PII (AAP R-25). The
        idempotency_key alone is sufficient for the client to
        investigate.
    """

    http_status = 409
    error_code = "IDEMPOTENCY_MISMATCH"


class IdempotencyHeaderMissingError(PaymentServiceError):
    """Required ``Idempotency-Key`` header missing on a protected endpoint.

    Raised by :class:`IdempotencyMiddleware` when a POST/PUT request to
    a payment-mutating endpoint (charge, refund) lacks the
    ``Idempotency-Key`` header. Per AAP R-8, all mutating payment
    requests MUST be idempotent so that a network-level retry does not
    produce a duplicate charge or refund.

    Returns HTTP 400 (Bad Request), the catch-all for "your request is
    malformed in a way I can't process." 411 (Length Required) and 412
    (Precondition Failed) are about HTTP-level semantics, not
    application-level contract violations, so 400 is the correct match.

    Recommended ``details`` fields:
        * ``endpoint`` (str) --- the path of the rejected request
          (e.g., ``"/v1/payments"``).
    """

    http_status = 400
    error_code = "IDEMPOTENCY_HEADER_MISSING"


# =============================================================================
# Resource not-found errors
# =============================================================================


class OrderNotFoundError(PaymentServiceError):
    """Refund requested for an order that does not exist in payments.

    Raised by ``RefundsController`` (or its repository layer) when a
    refund operation references an ``order_id`` that has no
    corresponding ``payments`` row. This may indicate a saga bug
    (e.g., the order was cancelled before any payment was created), a
    fraudulent refund attempt, or a stale client cache. Differs from
    :class:`PaymentNotFoundError` in that this is keyed on the order
    UUID rather than the payment UUID.

    Returns HTTP 404 (Not Found).

    Recommended ``details`` fields:
        * ``order_id`` (str) --- the requested order identifier.
    """

    http_status = 404
    error_code = "ORDER_NOT_FOUND"


class PaymentNotFoundError(PaymentServiceError):
    """Payment lookup miss.

    Raised by :class:`PaymentsRepository.get_by_id` (and its callers)
    when no ``payments`` row exists for the given ``payment_id``.
    Differs from :class:`OrderNotFoundError` in that this is keyed on
    the payment UUID rather than the order UUID. Distinct codes
    (``PAYMENT_NOT_FOUND`` vs ``ORDER_NOT_FOUND``) make Kibana
    dashboards and alerts trivially partitionable by lookup key.

    Returns HTTP 404 (Not Found).

    Recommended ``details`` fields:
        * ``payment_id`` (str) --- the requested payment identifier.
    """

    http_status = 404
    error_code = "PAYMENT_NOT_FOUND"


# =============================================================================
# Refund eligibility errors
# =============================================================================


class RefundIneligibleError(PaymentServiceError):
    """Payment status precludes a refund.

    Raised when a refund is attempted against a payment that is in an
    incompatible state (e.g., already fully ``REFUNDED``, in
    ``PROCESSING`` state, or terminally ``FAILED``). Returns HTTP 422
    (Unprocessable Entity) because the request is well-formed but the
    resource state forbids the operation.

    Recommended ``details`` fields:
        * ``payment_id`` (str) --- the payment that cannot be refunded.
        * ``current_status`` (str) --- the payment's current status
          (e.g., ``"REFUNDED"``, ``"FAILED"``).
        * ``reason`` (str) --- a brief explanation of why the refund
          was rejected (e.g., ``"already_fully_refunded"``,
          ``"payment_not_yet_succeeded"``).
    """

    http_status = 422
    error_code = "REFUND_INELIGIBLE"


# =============================================================================
# Persistence / encryption errors (AAP R-8 --- encryption at rest)
# =============================================================================


class DatabaseConnectionError(PaymentServiceError):
    """Database connection or query-level transport failure.

    Raised when the Payment Service cannot communicate with its
    PostgreSQL ``payment_db`` --- e.g., a connection pool exhaustion,
    a DNS failure, a TLS handshake failure, or a transient server-side
    error that the driver could not classify as a retryable statement
    error. AAP Section 0.4.4 mandates an isolated, encrypted-at-rest
    PostgreSQL instance for payments; this exception is the
    Payment-Service-level wrapper around any low-level
    ``psycopg`` / ``asyncpg`` / ``SQLAlchemy`` transport error so that
    callers (controllers, sagas) handle a single domain exception
    rather than a heterogeneous mess of driver errors.

    Returns HTTP 503 (Service Unavailable) because connection failures
    are typically transient infrastructure issues; the saga interprets
    this as a retry-then-compensate signal (AAP R-15, AAP R-20).

    Recommended ``details`` fields:
        * ``operation`` (str) --- the high-level operation that failed
          (e.g., ``"insert_payment"``, ``"select_payment_by_id"``).
        * ``retryable`` (bool) --- whether the saga should retry before
          compensating.

    NOTE:
        DO NOT include the connection string, password, or any other
        credential material in ``details`` (AAP R-25). The exception's
        ``cause`` attribute preserves the original driver error for
        log inspection without surfacing it in the JSON response body
        in a structured way.
    """

    http_status = 503
    error_code = "DATABASE_CONNECTION_ERROR"


class EncryptionError(PaymentServiceError):
    """Failure encrypting a payload before persisting it.

    Raised when the encryption layer (KMS / envelope encryption / DB
    column-level encryption) cannot encrypt a value before write ---
    e.g., the KMS endpoint is unreachable, the data key has been
    revoked, or the cipher initialization failed. Per AAP R-8, payment
    records are encrypted at rest, so any encryption failure MUST
    abort the write rather than allow plaintext to be persisted.

    Returns HTTP 500 (Internal Server Error). This is a server-side
    cryptographic failure, not a client error.

    Recommended ``details`` fields:
        * ``operation`` (str) --- the high-level operation that
          required encryption (e.g.,
          ``"persist_provider_metadata"``).
        * ``key_id`` (str | None) --- the (non-secret) identifier of
          the encryption key used (e.g., a KMS key ARN alias);
          ``None`` if the key was not yet selected.
        * ``reason`` (str) --- a brief, sanitized failure reason
          (e.g., ``"kms_unreachable"``, ``"data_key_revoked"``).

    NOTE:
        DO NOT include the plaintext or the encryption key material in
        ``details`` (AAP R-25). The whole point of this exception is
        that we MUST NOT persist or surface the unprotected value.
    """

    http_status = 500
    error_code = "ENCRYPTION_ERROR"


class DecryptionError(PaymentServiceError):
    """Failure decrypting a value previously stored under encryption.

    Raised when the encryption layer cannot decrypt a value previously
    written under :class:`EncryptionError`'s counterpart code path ---
    e.g., the ciphertext is corrupted, the data key is no longer
    available, the integrity tag mismatched, or the KMS endpoint is
    unreachable. Per AAP R-8 (encryption at rest), the Payment Service
    treats decryption failures as data-integrity events that MUST be
    surfaced loudly so operators can investigate.

    Returns HTTP 500 (Internal Server Error). A decryption failure on
    a record we own is a server-side problem, not a client problem.
    Distinct from :class:`EncryptionError` so dashboards and alerts
    can partition write-side vs. read-side cryptographic failures.

    Recommended ``details`` fields:
        * ``operation`` (str) --- the high-level operation that
          required decryption (e.g., ``"hydrate_provider_metadata"``).
        * ``record_id`` (str | None) --- the (non-secret) identifier of
          the row whose ciphertext could not be decrypted.
        * ``reason`` (str) --- a brief, sanitized failure reason
          (e.g., ``"integrity_check_failed"``,
          ``"data_key_unavailable"``).

    NOTE:
        DO NOT include the ciphertext or any partial plaintext in
        ``details`` (AAP R-25). Surfacing a partial decryption is a
        cryptographic anti-pattern --- treat the whole record as
        compromised and refuse to expose any of its bytes.
    """

    http_status = 500
    error_code = "DECRYPTION_ERROR"


# =============================================================================
# Public API
# =============================================================================
#
# Order: base first (``PaymentServiceError``), then leaves alphabetized for
# determinism.  Exactly 16 entries (1 base + 15 leaves).

__all__ = [
    # Base
    "PaymentServiceError",
    # Leaves (alphabetized for determinism)
    "ConfigurationError",
    "DatabaseConnectionError",
    "DecryptionError",
    "EncryptionError",
    "IdempotencyHeaderMissingError",
    "IdempotencyMismatchError",
    "NoCompatibleProviderError",
    "OrderNotFoundError",
    "PaymentDeniedError",
    "PaymentNotFoundError",
    "ProviderUnavailableError",
    "RefundIneligibleError",
    "UnknownProviderError",
    "WebhookBodyTooLargeError",
    "WebhookSignatureError",
]
