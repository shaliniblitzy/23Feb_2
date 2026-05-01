"""Custom exception hierarchy for the Notification Service.

This module defines the **closed** exception taxonomy used throughout the
Notification Service. Every runtime exception raised by this service SHOULD
be a subclass of :class:`DomainError`. Standard Python exceptions
(``ValueError``, ``TypeError``, ``KeyError``, etc.) are acceptable at the
lowest levels but MUST NOT escape past the domain boundary --- channels,
repositories, integrations, template renderers, and event handlers wrap
them as appropriate subclasses of :class:`DomainError` before letting them
bubble up.

Design principles
-----------------
1. **Import-graph root** --- this module is the most foundational package
   in ``services/notification-service/src/*``. It MUST NOT import from any
   other ``src/*`` module, from Pydantic, from any HTTP framework, or from
   the standard ``logging`` library. Errors carry structured context as a
   plain ``dict[str, Any]`` --- serialization is the caller's
   responsibility. Keeping the module pure is a deliberate, defensive
   discipline: any side effect here pollutes every test and every
   downstream module.
2. **Flat hierarchy** --- unlike the sibling ``recommendation-engine``
   service (which groups its 10 leaves under 5 family classes), the
   Notification Service's error taxonomy is intentionally flat (one base
   plus eleven leaves). The leaves do not split cleanly into families
   (e.g., :class:`TemplateNotFound` is a not-found, but
   :class:`TemplateRenderError` is not a validation error --- it is a
   rendering-engine failure). Premature grouping creates misplaced leaves;
   if future growth demands grouping, the change is coordinated with the
   ``ErrorHandlerMiddleware``.
3. **Per-leaf HTTP status mapping** --- each leaf overrides
   :attr:`DomainError.default_http_status` to communicate the appropriate
   client-facing status code. The HTTP error-handling middleware
   (``src/middleware/error_handler.py``) reads this attribute as a simple
   dispatch table over ``isinstance`` checks instead of a sprawling
   mapping.
4. **Stable machine-readable error codes** --- every concrete subclass
   exposes an ``error_code`` SCREAMING_SNAKE_CASE string. Consumers of the
   JSON response (mobile clients, CLI tools, dashboards) should key off
   the ``code`` field rather than the Python class name (``type``), so
   the ``error_code`` survives refactors that rename classes.
5. **Single inheritance throughout** --- no class has multiple bases. Each
   leaf inherits DIRECTLY from :class:`DomainError`, never from another
   leaf. This simplifies reasoning about ``isinstance`` dispatch and
   ``except`` clause ordering.
6. **No per-subclass ``__init__``** --- the base class's ``__init__`` is
   sufficient for all subclasses; adding per-subclass ``__init__`` methods
   would duplicate code and introduce subtle bugs. Subclasses customize
   via class attributes (``error_code``, ``default_http_status``) only.

Compliance notes
----------------
- AAP R-11 --- dual-channel concurrent integration ---
  :class:`ChannelUnavailable` reflects per-channel failure semantics
  (one channel may be open while another is healthy).
- AAP R-13 --- correlation-ID propagation --- every :class:`DomainError`
  carries a ``correlation_id`` so the same request identifier propagated
  by middleware and Kafka headers is preserved in error logs and JSON
  error bodies.
- AAP R-15 --- retry with exponential backoff + jitter ---
  :class:`ProviderTransientError` triggers scheduler retry.
- AAP R-16 --- circuit breaker --- :class:`ChannelUnavailable` is raised
  when the per-channel breaker is OPEN.
- AAP R-17 --- retry + DLQ topics --- :class:`DlqWriteFailed` captures
  the edge case where even the DLQ write fails; transient channel errors
  trigger ``<topic>.retry`` then ``<topic>.dlq`` routing.
- AAP R-19 --- fail-fast on missing critical dependencies at startup;
  configuration-related failures should bubble up as
  :class:`DomainError` subclasses or upstream Pydantic validation errors.
- AAP R-20 --- fallback behavior declared for every inter-service and
  external dependency; the error subclasses model the failure modes that
  trigger those fallbacks (:class:`ChannelUnavailable`,
  :class:`ProviderTransientError`, :class:`ProviderTerminalError`).
- AAP R-21 / R-22 --- JWT validation --- :class:`AuthenticationError`
  and :class:`PermissionDenied` reflect auth middleware failures.
- AAP R-26 --- structured JSON logs --- :meth:`DomainError.to_dict`
  produces the JSON body shape used by the structured-logging error
  pipeline; values are JSON-serializable primitives only when the caller
  has populated ``details`` correctly.

Cross-references
----------------
This module is imported by every other ``src/*`` package in the
Notification Service:

- ``src/middleware/error_handler.py`` --- catches :class:`DomainError`,
  calls :meth:`DomainError.to_dict`, and emits the response with HTTP
  status from :attr:`DomainError.default_http_status`.
- ``src/middleware/jwt_auth.py`` --- raises :class:`AuthenticationError`
  on invalid JWT; raises :class:`PermissionDenied` on insufficient scope.
- ``src/controllers/templates.py`` --- raises :class:`TemplateNotFound`
  on 404 lookups.
- ``src/controllers/preferences.py`` --- raises
  :class:`InvalidPreferences` on validation failure.
- ``src/channels/email/email_channel.py`` and
  ``src/channels/sms/sms_channel.py`` --- raise
  :class:`ProviderTransientError`, :class:`ProviderTerminalError`, or
  :class:`ChannelUnavailable` based on provider response semantics.
- ``src/channels/email/sendgrid_adapter.py``,
  ``src/channels/email/ses_adapter.py``,
  ``src/channels/sms/twilio_adapter.py``,
  ``src/channels/sms/sns_adapter.py`` --- translate provider SDK
  exceptions to :class:`ProviderTransientError` /
  :class:`ProviderTerminalError`.
- ``src/templates/renderer.py`` --- raises :class:`TemplateRenderError`
  on Jinja2 undefined-variable / syntax errors.
- ``src/repository/template_repo.py`` --- raises
  :class:`TemplateNotFound` when a lookup misses.
- ``src/scheduler/dlq_writer.py`` --- raises :class:`DlqWriteFailed`
  after exhausting internal DLQ-write retries.
- ``src/scheduler/scheduler.py`` --- catches :class:`ChannelUnavailable`
  and :class:`ProviderTransientError` to schedule a retry per AAP R-15.
- ``src/events/dispatcher.py`` --- catches :class:`DomainError`
  subclasses to route events to DLQ; raises
  :class:`UnsupportedEventError` when an event type does not match any
  registered handler.
"""

from __future__ import annotations

from typing import Any


# =============================================================================
# Base class
# =============================================================================


class DomainError(Exception):
    """Root of the Notification Service domain exception hierarchy.

    Every exception raised by this service SHOULD be a subclass of
    :class:`DomainError`. Standard Python exceptions are acceptable at the
    lowest levels but MUST be wrapped into a :class:`DomainError` subclass
    before escaping the module that produced them.

    Attributes:
        message: Short, human-readable error message.
        correlation_id: Request-scoped correlation identifier (propagated
            via middleware / Kafka headers per AAP R-13). ``None`` for
            errors raised outside a request scope (e.g., startup
            validation, background scheduler tick).
        details: Structured context included in logs and error responses.
            MUST be JSON-serializable (primitives, dicts, lists). Callers
            are responsible for stringifying values of non-primitive types
            (e.g., :class:`uuid.UUID`, :class:`datetime.datetime`,
            :class:`decimal.Decimal`) BEFORE passing them ---
            :meth:`to_dict` does NOT coerce.
        cause: The underlying exception that triggered this error, if any.
            Preserved separately from the standard ``__cause__`` so we can
            serialize it into :meth:`to_dict` without losing the chain.
            The constructor also wires ``__cause__`` so traceback chaining
            works with or without an explicit ``raise X from Y`` clause.

    Class attributes:
        default_http_status: HTTP status code emitted by the error
            middleware for this error. Subclasses MAY override it. The
            default ``500`` is a safe server-error catch-all.
        error_code: Short machine-readable code surfaced in the JSON
            body's ``code`` field. Stable across refactors that rename
            the class; subclasses override to expose a distinct identity.

    Example:
        >>> err = DomainError("oops", correlation_id="cid-123",
        ...                   details={"k": "v"})
        >>> err.message
        'oops'
        >>> err.correlation_id
        'cid-123'
        >>> err.details
        {'k': 'v'}
        >>> err.to_dict()["code"]
        'DOMAIN_ERROR'
    """

    # Subclasses MAY override to tune the HTTP status emitted by the error
    # middleware; the default 500 is a safe server-error catch-all.
    default_http_status: int = 500
    # Short machine-readable error code used in the JSON body's ``code``
    # field. Default is the generic family code; subclasses override for
    # stability across refactors (e.g., when a class is renamed).
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

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON body shape used by the error middleware.

        The HTTP error middleware (``src/middleware/error_handler.py``)
        wraps this dict in a top-level ``{"error": ...}`` envelope; this
        method returns ONLY the inner error object so it can be composed
        by callers that need different envelopes (e.g., Kafka DLQ
        payloads, structured log records).

        The returned dict is guaranteed to contain only JSON-serializable
        values when the caller has populated ``details`` with primitives
        (``str``, ``int``, ``float``, ``bool``, ``None``) or nested
        structures of primitives. Non-primitive values in ``details``
        (e.g., :class:`uuid.UUID`, :class:`datetime.datetime`,
        :class:`decimal.Decimal`) are passed through unchanged --- the
        caller is responsible for stringifying them BEFORE construction;
        this method does NOT coerce.

        Returns:
            A dict with stable keys:

            - ``code`` --- machine-readable error code
              (:attr:`error_code`).
            - ``type`` --- the Python class name (verbose but precise).
            - ``message`` --- the human-readable string.
            - ``correlation_id`` --- may be ``None``.
            - ``details`` --- the structured context payload (always a
              dict; empty when none was provided).
            - ``cause`` --- string repr of :attr:`cause` in the form
              ``"ExceptionType: message"``, present only when ``cause``
              is not ``None``.

        Note:
            ``code`` and ``type`` look redundant but serve different
            audiences. ``code`` is a stable SCREAMING_SNAKE_CASE
            identifier intended for programmatic consumption (clients
            should switch on it); ``type`` is the Python class name and
            is intended for humans reading logs.
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
# Authentication / Authorization leaves
# =============================================================================


class AuthenticationError(DomainError):
    """JWT missing, invalid, or expired.

    Raised by :class:`JWTAuthMiddleware` (AAP R-22) when:

      * The ``Authorization`` header is absent on a protected endpoint.
      * JWT signature verification fails against the JWKS public keys.
      * JWT is expired (``exp`` claim in the past).
      * JWT issuer (``iss``) or audience (``aud``) claim does not match
        the configured expectations.

    Recommended ``details`` fields: ``{"reason": <str>}`` --- do NOT
    include the raw token, its claims, or PII fields in ``details``;
    those would land in the structured-log Elasticsearch index and could
    leak credentials.

    HTTP status ``401 Unauthorized`` per RFC 7235; the middleware also
    sets a ``WWW-Authenticate`` header pointing to the Auth Service's
    ``/.well-known/oauth-authorization-server`` discovery document.
    """

    default_http_status = 401
    error_code = "AUTHENTICATION_ERROR"


class PermissionDenied(DomainError):
    """JWT is valid but lacks the scope required for the operation.

    Raised by the scope-check layer downstream of
    :class:`JWTAuthMiddleware` when a token authenticates successfully
    but the ``scope`` claim does not contain the required scope (e.g.,
    ``notifications:admin`` for template CRUD endpoints).

    Recommended ``details`` fields: ``{"required_scope": <str>,
    "token_scopes": <list[str]>}`` --- include only the scope claim, not
    the entire token.

    HTTP status ``403 Forbidden`` per RFC 7231: the request was
    authenticated, but the principal is not allowed to perform the
    operation. Distinct from :class:`AuthenticationError` (which is
    raised BEFORE identity is established).
    """

    default_http_status = 403
    error_code = "PERMISSION_DENIED"


# =============================================================================
# Channel / Provider leaves
# =============================================================================


class ChannelUnavailable(DomainError):
    """Channel cannot accept work right now.

    Raised when one of:

      * The channel's circuit breaker is OPEN (AAP R-16).
      * All retry attempts within the current dispatch window are
        exhausted (AAP R-15).
      * The channel's provider adapter is in a failed health-check
        state.

    Callers typically translate this to ``DeliveryOutcome.RETRYABLE`` so
    the scheduler picks the row up later --- the assumption is the
    circuit breaker will close before the retry budget is exhausted. If
    the breaker remains open past the retry budget, the message is
    routed to ``<topic>.dlq`` per AAP R-17.

    Recommended ``details`` fields: ``{"channel": <str>, "reason": <str>,
    "breaker_state": <str>}``.

    HTTP status ``503 Service Unavailable`` per RFC 7231; the middleware
    MAY also emit a ``Retry-After`` header derived from the breaker's
    open-state duration.
    """

    default_http_status = 503
    error_code = "CHANNEL_UNAVAILABLE"


class ProviderTransientError(DomainError):
    """External provider returned a RETRYABLE error (5xx or 429).

    Raised by :class:`EmailChannel` / :class:`SmsChannel` adapters when
    the provider's HTTP response indicates a temporary failure
    (``429 Too Many Requests``, ``500``--``599``). Callers MUST treat
    this as :class:`DeliveryOutcome.RETRYABLE` and let the scheduler
    retry per AAP R-15 with exponential backoff and jitter.

    Recommended ``details`` fields: ``{"provider": <str>, "status":
    <int>, "response_body": <str|None>, "attempt": <int>}``. Truncate
    long response bodies to a reasonable size (e.g., 1 KiB) before
    placing them in ``details`` to avoid bloating log volumes.

    HTTP status ``502 Bad Gateway`` from the external API's perspective
    --- both transient and terminal upstream failures surface as 502 to
    HTTP clients; the distinction is internal (it changes whether the
    scheduler retries, not how the error surfaces externally). The
    :attr:`error_code` field preserves the distinction for operator
    dashboards (DLQ depth broken down by ``PROVIDER_TRANSIENT_ERROR``
    vs ``PROVIDER_TERMINAL_ERROR`` is a useful metric).
    """

    default_http_status = 502
    error_code = "PROVIDER_TRANSIENT_ERROR"


class ProviderTerminalError(DomainError):
    """External provider returned a NON-RETRYABLE error (4xx non-429).

    Raised for ``400 Bad Request``, ``401 Unauthorized``,
    ``403 Forbidden``, ``404 Not Found`` (recipient does not exist),
    ``422 Unprocessable Entity``, and similar responses. Callers MUST
    treat this as :class:`DeliveryOutcome.TERMINAL` and route to the
    ``<topic>.dlq`` topic per AAP R-17 without further retries ---
    retrying would only burn quota without changing the outcome.

    Recommended ``details`` fields: ``{"provider": <str>, "status":
    <int>, "error_code": <str>, "error_message": <str>}``. The
    ``error_code`` here is the PROVIDER's error code (e.g., SendGrid's
    ``"invalid_email"``), distinct from this exception's
    :attr:`error_code` class attribute.

    HTTP status ``502 Bad Gateway`` --- same external-facing status as
    :class:`ProviderTransientError`; the distinction is internal.
    """

    default_http_status = 502
    error_code = "PROVIDER_TERMINAL_ERROR"


# =============================================================================
# DLQ leaf
# =============================================================================


class DlqWriteFailed(DomainError):
    """Kafka producer failed to write a message to a DLQ topic.

    This is an EDGE-CASE ERROR: we are already in an error-handling
    path (routing a failed message to the DLQ per AAP R-17), and even
    that path has failed. The scheduler retries the DLQ write internally
    with its own bounded retry policy; if all internal retries are
    exhausted, this error is raised and the ``notification_log`` row is
    left in ``PENDING_RETRY`` state for operator intervention.

    Recommended ``details`` fields: ``{"topic": <str>, "kafka_error":
    <str>, "attempt": <int>, "original_event_id": <str>}``.

    HTTP status ``500 Internal Server Error`` (inherited from
    :class:`DomainError`) --- the operator dashboard in Kibana should
    alert on any non-zero count of this code in the last 5 minutes;
    under healthy conditions this error MUST be zero. Kafka is designed
    to be highly available, so observing this error indicates a serious
    infrastructure incident that may require operator paging.
    """

    error_code = "DLQ_WRITE_FAILED"


# =============================================================================
# Event-handling leaves
# =============================================================================


class UnsupportedEventError(DomainError):
    """A consumed Kafka event has a type that no handler is registered for.

    Raised by ``src/events/dispatcher.py`` when a message arrives on a
    subscribed topic but the dispatcher's handler registry has no entry
    for the event's type. Possible causes:

      * The producer added a new event variant before the consumer was
        upgraded to handle it.
      * The topic mapping in ``infrastructure/kafka/topics.yaml`` was
        edited in a way that subscribed this service to an irrelevant
        topic.
      * A test fixture or migration injected a malformed event.

    Callers MUST route these messages to ``<topic>.dlq`` per AAP R-17
    rather than blocking the partition with a poison message; retrying
    will not help because the lack of a handler is a code-deployment
    issue, not a transient infrastructure failure.

    Recommended ``details`` fields: ``{"topic": <str>, "event_type":
    <str>, "offset": <int>, "partition": <int>}``.

    HTTP status ``400 Bad Request`` --- though events flow through
    Kafka rather than HTTP, the status communicates the contract
    violation when this error surfaces through any synchronous endpoint
    (e.g., a synthetic-event admin route used during incident drills).
    """

    default_http_status = 400
    error_code = "UNSUPPORTED_EVENT_ERROR"


# =============================================================================
# Preference / Validation leaves
# =============================================================================


class InvalidPreferences(DomainError):
    """User preference payload failed validation.

    Raised from the ``PUT /api/v1/preferences/{userId}`` admin endpoint
    when:

      * Unknown channel type appears in the request body (i.e., not in
        :class:`~src.domain.channel_types.ChannelType`).
      * A locale string fails the BCP-47 format check (e.g.,
        ``"en-XX-bad"``).
      * A quiet-hours span is malformed (start ``>=`` end without an
        explicit overnight wrap-around flag).
      * A required field is missing or has the wrong type.

    Distinct from Pydantic's :class:`pydantic.ValidationError` --- we
    wrap the latter at the controller boundary and raise this one so
    downstream handlers don't need to import Pydantic and so error
    responses carry the canonical shape regardless of where validation
    happened.

    Recommended ``details`` fields: ``{"field": <str>, "reason": <str>,
    "value": <Any>}``. Keep the ``value`` representation short --- avoid
    full payload echoing.

    HTTP status ``400 Bad Request``.
    """

    default_http_status = 400
    error_code = "INVALID_PREFERENCES"


# =============================================================================
# Rate-limit leaf
# =============================================================================


class RateLimitExceeded(DomainError):
    """Admin endpoint rate limit exceeded.

    Raised by the rate-limit middleware (if enforced per-service in
    addition to the API Gateway's global rate limit per AAP Section
    0.4.5) when a caller's request rate exceeds the configured token
    bucket --- typically on bursty admin operations like bulk template
    upload or per-user delivery-log lookups under traffic spikes.

    Recommended ``details`` fields: ``{"retry_after_seconds": <int>,
    "limit": <int>, "window_seconds": <int>, "principal": <str>}``. The
    error handler reads ``retry_after_seconds`` and copies it into a
    ``Retry-After`` HTTP header per RFC 6585; this turns a 429 into a
    polite, operable response instead of a generic failure.

    HTTP status ``429 Too Many Requests`` per RFC 6585.
    """

    default_http_status = 429
    error_code = "RATE_LIMIT_EXCEEDED"


# =============================================================================
# Template leaves
# =============================================================================


class TemplateNotFound(DomainError):
    """Requested template lookup key missed the ``templates`` table.

    Raised when ``TemplateRepository.get(event_type, channel, locale,
    version)`` returns no row. Typically indicates:

      * A new event type arrived before its template was provisioned by
        the admin endpoint.
      * A user's locale has no fallback (and the default-locale fallback
        also missed).
      * A template version was requested explicitly but no longer exists
        because it was archived.

    Callers MAY route this to ``<consumed-topic>.dlq`` after one retry
    (the provisioning race is usually resolved within seconds when a
    new template is being deployed alongside a new event type).

    Recommended ``details`` fields: ``{"event_type": <str>, "channel":
    <str>, "locale": <str>, "version": <str|None>}``.

    HTTP status ``404 Not Found``.
    """

    default_http_status = 404
    error_code = "TEMPLATE_NOT_FOUND"


class TemplateRenderError(DomainError):
    """Jinja2 (or equivalent) raised during template rendering.

    Typical causes:

      * Missing context variable --- the template requests
        ``{{ order.id }}`` but the consumed event payload has no
        ``order`` field (strict-undefined mode is enabled in the
        template engine, so missing variables fail loudly rather than
        silently shipping a malformed notification).
      * Malformed template after a bad update via the admin endpoint
        (e.g., unclosed Jinja block).
      * Filter / function reference not present in the sandboxed
        environment.
      * Recursive include resolution failure.

    NOT retryable --- the root cause is data or template content, not
    transient infrastructure. Route to DLQ immediately; retrying will
    not help. The DLQ payload SHOULD include the rendered context
    snapshot so an operator can reproduce the failure offline.

    Recommended ``details`` fields: ``{"template_id": <str>, "locale":
    <str>, "missing_variable": <str|None>, "render_error": <str>}``.

    HTTP status ``500 Internal Server Error`` (the default) --- the
    failure is internal: the request was well-formed, but our template
    or our event payload is broken.
    """

    error_code = "TEMPLATE_RENDER_ERROR"


# =============================================================================
# Public API
# =============================================================================
# Ordered: base first, then leaves alphabetized. Every name in this list is
# a class defined above; importers (e.g., ``src/domain/__init__.py``) can
# safely do ``from .errors import *`` and rely on this list for what's
# re-exported.
__all__ = [
    # Base
    "DomainError",
    # Leaves (alphabetized)
    "AuthenticationError",
    "ChannelUnavailable",
    "DlqWriteFailed",
    "InvalidPreferences",
    "PermissionDenied",
    "ProviderTerminalError",
    "ProviderTransientError",
    "RateLimitExceeded",
    "TemplateNotFound",
    "TemplateRenderError",
    "UnsupportedEventError",
]
