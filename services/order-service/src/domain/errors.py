"""Domain exception hierarchy for the Order Service.

This module defines the **closed taxonomy** of business / domain
exceptions raised by the Order Service. Every domain-layer exception
SHOULD be a subclass of :class:`DomainError`. Standard Python
exceptions (``ValueError``, ``KeyError``, ``TypeError``) are acceptable
at the lowest levels but MUST be wrapped into a :class:`DomainError`
subclass before crossing the domain boundary so that the FastAPI
exception handler, the structured logger (AAP R-26), and the Kafka
consumer error policy (AAP R-17) can route them deterministically.

Design principles
-----------------
1. **Foundational module** — this file is the most foundational in the
   ``services/order-service/src/domain`` package. It MUST NOT import
   from any other ``src/*`` module (not even peer domain modules such
   as ``order_status.py`` or ``saga_state.py``). The deliberate
   ``str`` typing of ``from_status`` / ``to_status`` on
   :class:`InvalidStateTransition` is the key enforcement point: the
   caller (the saga state machine) formats ``OrderStatus.value``
   (a ``str`` since ``OrderStatus`` is a ``StrEnum``) before raising.
   Any other choice would create a fragile circular import.

2. **Subclass of** :class:`Exception`, **not** :class:`RuntimeError`
   — the folder spec is explicit on this. Domain errors are EXPECTED
   outcomes of business-rule evaluation (e.g., a state-machine
   transition the spec disallows, a currency outside the configured
   allow-list), not unexpected runtime failures. ``RuntimeError`` is
   reserved for "errors that cannot be detected statically and arise
   during execution" — a different semantic class.

3. **Flat hierarchy** — one base class :class:`DomainError` plus 8
   leaves, all inheriting directly. No nested family classes (e.g.,
   no ``SagaError`` group for :class:`SagaTimeout` and
   :class:`SagaCompensationFailure`). This mirrors the Payment
   Service's ``services/payment-service/src/domain/exceptions.py``
   pattern, simplifies ``except DomainError:`` dispatch at the
   FastAPI exception-handler boundary, and avoids premature
   over-grouping for only 8 leaves.

4. **Strict types in constructors** — ``UUID`` for identifiers
   (``order_id``, ``saga_id``, ``existing_order_id``,
   ``correlation_id``), ``int`` for versions / counters, ``Decimal``
   for monetary amounts, ``str`` for free-form labels. The instance
   attributes are also typed for downstream consumers (FastAPI
   handler, structured logger).

5. **Ergonomic** :meth:`DomainError.to_response_dict` — produces a
   JSON-serializable dict the API exception handler returns verbatim
   as the body of a JSON error response. UUIDs become strings;
   :class:`Decimal` values in subclass details are pre-stringified to
   preserve exact representation across the JSON wire format (no
   ``json.JSONEncoder`` customization required).

6. **No** I/O, **no** logging, **no** side effects at import time —
   keeps unit-test startup cheap and prevents the module from
   accidentally pulling in framework-specific code paths.

Cross-references
----------------
This module is consumed by every other ``src/*`` package in the Order
Service:

* ``src/domain/order.py`` — raises :class:`UnsupportedCurrency` and
  :class:`InvalidOrderTotal` from Pydantic ``model_validator`` hooks.
* ``src/saga/state_machine.py`` — raises
  :class:`InvalidStateTransition` when an event is not encoded in the
  allowed-transitions table.
* ``src/saga/coordinator.py`` — raises
  :class:`SagaCompensationFailure` after retries are exhausted (AAP
  R-18).
* ``src/saga/scheduler.py`` — raises :class:`SagaTimeout` when a
  saga step's deadline elapses without the awaited Kafka event.
* ``src/repository/order_repository.py`` — raises
  :class:`OrderNotFound` and :class:`OptimisticLockFailure`.
* ``src/repository/idempotency_repository.py`` — raises
  :class:`IdempotencyConflict`.
* ``src/main.py`` — global FastAPI exception handler calls
  :meth:`DomainError.to_status_code` for the HTTP status and
  :meth:`DomainError.to_response_dict` for the JSON body.
* ``src/events/consumers.py`` — distinguishes transient
  (``http_status >= 500``, e.g., :class:`SagaTimeout` /
  :class:`SagaCompensationFailure`) from permanent (``< 500``)
  failures to choose retry-topic vs. DLQ routing (AAP R-17).
* ``src/observability/logger.py`` — logs ``error_code``,
  ``correlation_id``, ``details`` for every domain exception that
  propagates to a handler (AAP R-26).

Compliance notes
----------------
* AAP R-13 — correlation-ID propagation. Every :class:`DomainError`
  carries an optional ``correlation_id`` so the same identifier
  injected by the API Gateway / correlation-ID middleware survives
  into log lines and JSON error bodies for end-to-end Kibana
  tracing (AAP R-28).
* AAP R-18 — saga pattern with explicit compensation.
  :class:`SagaTimeout` and :class:`SagaCompensationFailure` are
  first-class exceptional conditions emitted by the saga scheduler
  and coordinator.
* AAP R-26 — structured JSON logs.
  :meth:`DomainError.to_response_dict` produces the canonical
  JSON-serializable error payload.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, ClassVar, Final
from uuid import UUID


# =============================================================================
# Module-level defaults
# =============================================================================

#: Default HTTP status code for any domain error not overriding it.
#: ``422 Unprocessable Entity`` is appropriate because most domain
#: errors indicate that the REQUEST was syntactically valid (parsed
#: successfully by Pydantic / framework decoders) but semantically
#: rejected by a business rule — exactly the meaning of ``422``.
_DEFAULT_HTTP_STATUS: Final[int] = 422

#: Default machine-readable error code for any domain error not
#: overriding it. Subclasses SHOULD override this to a specific code
#: (uppercase ``SCREAMING_SNAKE_CASE``) so JSON consumers can switch
#: on a stable identifier instead of the Python class name.
_DEFAULT_ERROR_CODE: Final[str] = "DOMAIN_ERROR"


# =============================================================================
# Base class
# =============================================================================


class DomainError(Exception):
    """Base class for all Order Service domain exceptions.

    Every domain exception carries:

      * ``message`` — human-readable summary (English; logs and dev
        consoles consume this).
      * ``correlation_id`` — request-scoped UUID propagated from the
        API Gateway (AAP R-13). Required for end-to-end tracing
        through Kibana (AAP R-26 / R-28).
      * ``details`` — optional structured context (machine-readable
        fields specific to each exception subclass).
      * ``cause`` — optional underlying ``BaseException`` (set when an
        infrastructure failure was wrapped into a domain failure).

    Subclasses override ``http_status`` and ``error_code`` class
    variables. The infrastructure layers translate domain exceptions:

      * FastAPI exception handler maps them to JSON 4xx / 5xx
        responses via :meth:`to_response_dict` and
        :meth:`to_status_code`.
      * Kafka consumer error policy routes them either to retry topics
        (for transient errors) or DLQs (for permanent errors). The
        consumer treats ``http_status >= 500`` as transient by default.
      * Structured logger logs ``cls.error_code``, ``correlation_id``,
        and ``details``, with the original ``cause`` chained.

    NOT for use as a generic catch-all — call sites should always
    raise the most specific subclass.

    See AAP R-13, R-26, R-28.

    Attributes:
        message: Human-readable error message. Surfaced to operators
            via logs and (depending on environment) to clients via
            the JSON error body.
        correlation_id: Request-scoped UUID propagated by the
            correlation-ID middleware. ``None`` for errors raised
            outside a request scope (e.g., startup validation,
            background scheduler tick before a request arrives).
        details: Structured context appended to logs and emitted in
            the JSON error body. Values must be JSON-serializable;
            UUIDs and ``Decimal`` instances are pre-stringified by
            subclasses' constructors so the FastAPI handler can
            ``json.dumps`` the dict directly.
        cause: Underlying exception that produced this error, when
            applicable. Stored as a domain-level attribute so the
            FastAPI handler and the structured logger can access it
            without inspecting the stdlib ``__cause__`` /
            ``__context__`` dunders.

    Class attributes:
        http_status: HTTP status code returned by the global
            FastAPI exception handler when this exception
            propagates. Defaults to ``422``; subclasses override.
        error_code: Stable machine-readable code emitted in the JSON
            body's ``error_code`` field. Stable across refactors that
            rename the Python class.

    Example:
        >>> err = DomainError("oops", details={"k": "v"})
        >>> err.message
        'oops'
        >>> err.details
        {'k': 'v'}
        >>> err.to_response_dict()["error_code"]
        'DOMAIN_ERROR'
        >>> err.to_status_code()
        422
    """

    #: Default HTTP status code; subclasses MAY override.
    http_status: ClassVar[int] = _DEFAULT_HTTP_STATUS

    #: Default machine-readable error code; subclasses SHOULD override.
    error_code: ClassVar[str] = _DEFAULT_ERROR_CODE

    def __init__(
        self,
        message: str,
        *,
        correlation_id: UUID | None = None,
        details: dict[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.message: str = message
        self.correlation_id: UUID | None = correlation_id
        # Always store a fresh dict so callers cannot mutate the
        # exception's state by holding a reference to the dict they
        # passed in. ``None`` becomes ``{}`` so downstream logging
        # code never has to special-case it.
        self.details: dict[str, Any] = dict(details) if details else {}
        self.cause: BaseException | None = cause

    def to_status_code(self) -> int:
        """Return the HTTP status code for this exception.

        The global FastAPI exception handler reads this value to set
        the HTTP status on the JSON error response. The Kafka
        consumer error policy uses it to classify the failure as
        transient (``>= 500``, route to ``<topic>.retry``) or
        permanent (``< 500``, route to ``<topic>.dlq``) per AAP
        R-17.

        Returns:
            The HTTP status code (an integer in the standard 4xx /
            5xx range) defined by the class attribute
            :attr:`http_status`.
        """
        return self.http_status

    def to_response_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable error payload.

        Shape (RFC 7807-inspired but trimmed to the essentials)::

            {
                "error_code": "ORDER_INVALID_STATE_TRANSITION",
                "message": "...",
                "correlation_id": "uuid-or-null",
                "details": {...}  # arbitrary subclass context
            }

        UUIDs are converted to strings; non-string keys / values
        inside ``details`` MUST already be JSON-safe (the subclass
        constructor's responsibility — see how :class:`OrderNotFound`
        and :class:`InvalidOrderTotal` pre-stringify ``UUID`` /
        ``Decimal`` values into their ``details`` dicts).

        Returns:
            A dict suitable for ``json.dumps``. The FastAPI exception
            handler returns it verbatim as the body of the JSON error
            response.
        """
        return {
            "error_code": self.error_code,
            "message": self.message,
            "correlation_id": (
                str(self.correlation_id)
                if self.correlation_id is not None
                else None
            ),
            "details": self.details,
        }

    def __repr__(self) -> str:  # pragma: no cover - dev ergonomics only
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, "
            f"correlation_id={self.correlation_id!r}, "
            f"details={self.details!r})"
        )


# =============================================================================
# Leaf exceptions — state-machine / saga family
# =============================================================================


class InvalidStateTransition(DomainError):
    """Raised when an OrderStatus transition is not in the allowed table.

    Raised by ``saga.state_machine.transition`` when an event would
    move the order from ``from_status`` to ``to_status`` but the
    transition is not encoded in
    :data:`src.domain.order_status._ALLOWED_TRANSITIONS`.

    The ``event`` argument is the saga-internal event name (e.g.,
    ``"payment.succeeded"`` or ``"step_timeout"``) that triggered the
    attempted transition.

    HTTP 409 Conflict — the request is structurally valid but the
    resource's current state forbids the operation. Maps to the
    ``ORDER_INVALID_STATE_TRANSITION`` machine-readable error code.

    NOTE: ``from_status`` and ``to_status`` are typed as ``str``
    (rather than ``OrderStatus``) DELIBERATELY to avoid a circular
    import between ``errors.py`` and ``order_status.py``. Callers
    pass ``OrderStatus.value`` (which IS a ``str`` since
    ``OrderStatus`` is a ``StrEnum``).

    Attributes:
        from_status: The current order status as a string (the
            ``OrderStatus.value`` of the source state).
        to_status: The attempted-but-disallowed target status as a
            string (the ``OrderStatus.value`` of the destination
            state).
        event: The saga-internal event name that triggered the
            attempted transition.
    """

    http_status: ClassVar[int] = 409
    error_code: ClassVar[str] = "ORDER_INVALID_STATE_TRANSITION"

    def __init__(
        self,
        from_status: str,
        to_status: str,
        event: str,
        *,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = (
            f"Order cannot transition from '{from_status}' to "
            f"'{to_status}' via event '{event}'"
        )
        super().__init__(
            message,
            correlation_id=correlation_id,
            details={
                "from_status": from_status,
                "to_status": to_status,
                "event": event,
            },
            cause=cause,
        )
        self.from_status: str = from_status
        self.to_status: str = to_status
        self.event: str = event


class SagaTimeout(DomainError):
    """Raised by the saga scheduler when a saga step's deadline has
    passed without a required Kafka event arriving.

    Raised by ``saga.scheduler.SagaScheduler.tick`` BEFORE triggering
    compensation. The scheduler caller catches this exception, logs
    it at WARN, and routes the saga into the appropriate
    ``COMPENSATING_*`` step.

    HTTP 504 Gateway Timeout — surfaces only when a synchronous
    request waits long enough to be rejected by the API Gateway's
    own timeout. The more common code path is internal: scheduler
    catches and converts to compensation.

    Attributes:
        saga_id: The UUID of the saga whose step deadline elapsed.
    """

    http_status: ClassVar[int] = 504
    error_code: ClassVar[str] = "ORDER_SAGA_TIMEOUT"

    def __init__(
        self,
        saga_id: UUID,
        *,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = f"Saga {saga_id} exceeded step deadline"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details={"saga_id": str(saga_id)},
            cause=cause,
        )
        self.saga_id: UUID = saga_id


class SagaCompensationFailure(DomainError):
    """Raised when compensation has been retried up to the configured
    maximum and is still failing.

    Raised by ``saga.coordinator.SagaCoordinator`` after
    ``saga.max_compensation_attempts`` retries fail. The order is
    transitioned to ``OrderStatus.FAILED`` and the saga to
    ``SagaStep.TERMINATED``.

    HTTP 500 Internal Server Error — operations team must intervene.
    A Kibana alert is wired on the count of saga compensation
    failures (AAP R-28).

    Attributes:
        saga_id: The UUID of the saga whose compensation failed.
        attempts: The number of compensation retries attempted before
            giving up (typically equal to
            ``saga.max_compensation_attempts``).
    """

    http_status: ClassVar[int] = 500
    error_code: ClassVar[str] = "ORDER_SAGA_COMPENSATION_FAILURE"

    def __init__(
        self,
        saga_id: UUID,
        attempts: int,
        *,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = (
            f"Saga {saga_id} compensation failed after {attempts} "
            f"attempts; order marked FAILED for manual recovery"
        )
        super().__init__(
            message,
            correlation_id=correlation_id,
            details={
                "saga_id": str(saga_id),
                "attempts": attempts,
            },
            cause=cause,
        )
        self.saga_id: UUID = saga_id
        self.attempts: int = attempts


# =============================================================================
# Leaf exceptions — repository / persistence family
# =============================================================================


class OrderNotFound(DomainError):
    """Raised when an order lookup yields no rows.

    Raised by ``OrderRepository.get_by_id`` (and similar query
    methods) when the requested order does not exist.

    HTTP 404 Not Found.

    Attributes:
        order_id: The UUID of the order that was not found.
    """

    http_status: ClassVar[int] = 404
    error_code: ClassVar[str] = "ORDER_NOT_FOUND"

    def __init__(
        self,
        order_id: UUID,
        *,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = f"Order not found: {order_id}"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details={"order_id": str(order_id)},
            cause=cause,
        )
        self.order_id: UUID = order_id


class OptimisticLockFailure(DomainError):
    """Raised when a versioned UPDATE returns 0 rows affected.

    Raised by ``OrderRepository.update`` and
    ``SagaRepository.update`` when ``WHERE version = expected``
    matches no rows — meaning another writer raced and incremented
    the version first.

    HTTP 409 Conflict — the client should re-read and retry. The
    Kafka consumer's retry policy classifies this as transient and
    routes the message to ``<topic>.retry`` for re-processing.

    Attributes:
        order_id: The UUID of the order on which the optimistic
            lock check failed.
        expected_version: The version the writer expected to find
            in the database (typically the version it had read
            during the initial query).
        actual_version: The version actually present in the
            database when the UPDATE executed (greater than
            ``expected_version`` because another writer committed
            first).
    """

    http_status: ClassVar[int] = 409
    error_code: ClassVar[str] = "ORDER_OPTIMISTIC_LOCK_FAILURE"

    def __init__(
        self,
        order_id: UUID,
        expected_version: int,
        actual_version: int,
        *,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = (
            f"Optimistic lock failure on order {order_id}: expected "
            f"version {expected_version}, found {actual_version}"
        )
        super().__init__(
            message,
            correlation_id=correlation_id,
            details={
                "order_id": str(order_id),
                "expected_version": expected_version,
                "actual_version": actual_version,
            },
            cause=cause,
        )
        self.order_id: UUID = order_id
        self.expected_version: int = expected_version
        self.actual_version: int = actual_version


class IdempotencyConflict(DomainError):
    """Raised when a POST /orders carries an idempotency key that has
    been seen before with a DIFFERENT request payload.

    The repository stores ``(idempotency_key, request_hash, order_id)``
    rows; a cache hit with a matching hash returns the existing order
    (idempotent re-play; NOT this exception). A cache hit with a
    differing hash raises this exception.

    HTTP 409 Conflict — the idempotency key is being re-used with an
    incompatible payload. The client must either re-use the original
    payload or generate a fresh idempotency key.

    Attributes:
        idempotency_key: The client-supplied ``Idempotency-Key`` header
            value that conflicted with a previously stored request
            hash.
        existing_order_id: The UUID of the order created on the
            ORIGINAL POST that first associated this idempotency key
            with a request hash. The client may inspect this order
            to reconcile state.
    """

    http_status: ClassVar[int] = 409
    error_code: ClassVar[str] = "ORDER_IDEMPOTENCY_CONFLICT"

    def __init__(
        self,
        idempotency_key: str,
        existing_order_id: UUID,
        *,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = (
            f"Idempotency key '{idempotency_key}' was previously used "
            f"for order {existing_order_id} with a different request "
            f"payload"
        )
        super().__init__(
            message,
            correlation_id=correlation_id,
            details={
                "idempotency_key": idempotency_key,
                "existing_order_id": str(existing_order_id),
            },
            cause=cause,
        )
        self.idempotency_key: str = idempotency_key
        self.existing_order_id: UUID = existing_order_id


# =============================================================================
# Leaf exceptions — aggregate / validation family
# =============================================================================


class UnsupportedCurrency(DomainError):
    """Raised by the Order aggregate's currency validator when a
    request specifies a currency outside the configured allow-list.

    The allow-list lives in ``config/default.yaml`` under
    ``order.supported_currencies`` (e.g., ``USD``, ``EUR``, ``GBP``,
    ``INR``, ``AUD``, ``CAD``). Adding a new currency requires
    payment-provider routing rules to be aware of it.

    HTTP 400 Bad Request — the client supplied invalid input.

    Attributes:
        currency: The currency code (typically an ISO 4217 string)
            that was rejected.
    """

    http_status: ClassVar[int] = 400
    error_code: ClassVar[str] = "ORDER_UNSUPPORTED_CURRENCY"

    def __init__(
        self,
        currency: str,
        *,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = f"Currency '{currency}' is not in the supported list"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details={"currency": currency},
            cause=cause,
        )
        self.currency: str = currency


class InvalidOrderTotal(DomainError):
    """Raised by the Order aggregate's total validator when
    ``total_amount`` does not equal the sum of ``items.line_total``.

    Raised at construction time of the ``Order`` aggregate (the
    ``_total_validation`` ``model_validator``). Because the aggregate
    is FROZEN, this exception only fires when an ``Order`` is being
    instantiated.

    HTTP 422 Unprocessable Entity — the client's request is
    structurally valid (parsed by Pydantic) but semantically
    inconsistent.

    Attributes:
        total: The ``total_amount`` claimed by the request.
        sum_of_lines: The sum of ``line_total`` across the order
            items, which did not match ``total``.
    """

    http_status: ClassVar[int] = 422
    error_code: ClassVar[str] = "ORDER_INVALID_TOTAL"

    def __init__(
        self,
        total: Decimal,
        sum_of_lines: Decimal,
        *,
        correlation_id: UUID | None = None,
        cause: BaseException | None = None,
    ) -> None:
        message = (
            f"Order total {total} does not equal the sum of line "
            f"totals {sum_of_lines}"
        )
        super().__init__(
            message,
            correlation_id=correlation_id,
            details={
                "total": str(total),
                "sum_of_lines": str(sum_of_lines),
            },
            cause=cause,
        )
        self.total: Decimal = total
        self.sum_of_lines: Decimal = sum_of_lines


# =============================================================================
# Public exports
# =============================================================================
#
# ``DomainError`` is listed first as the base class (consistent with
# the Payment Service's ``services/payment-service/src/domain/exceptions.py``
# convention); the eight leaf classes follow in alphabetical order
# for predictable diff hygiene.

__all__ = [
    "DomainError",
    "IdempotencyConflict",
    "InvalidOrderTotal",
    "InvalidStateTransition",
    "OptimisticLockFailure",
    "OrderNotFound",
    "SagaCompensationFailure",
    "SagaTimeout",
    "UnsupportedCurrency",
]
