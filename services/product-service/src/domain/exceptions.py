"""Domain exceptions for the Product Service.

This module defines the **closed taxonomy** of business / domain
exceptions raised by the Product Service. Every domain-layer exception
SHOULD be a subclass of :class:`DomainError`. Standard Python exceptions
(``ValueError``, ``KeyError``, ``TypeError``) are acceptable at the
lowest levels but MUST be wrapped into a :class:`DomainError` subclass
before crossing the domain boundary so that the FastAPI exception
handler (``src/middleware/error_handler.py``), the structured logger
(AAP R-26), and the Kafka producer error policy (AAP R-17) can route
them deterministically.

Design principles
-----------------
1. **Most foundational module of the domain layer.** This file MUST
   NOT import from any other ``src/*`` module — not even peer domain
   modules such as ``product.py`` or ``category.py``. It imports only
   from the Python standard library. The mapping from exception class
   to HTTP status code lives entirely in
   :mod:`src.middleware.error_handler` (declarative dispatch via
   :attr:`DomainError.http_status`); this module never imports from
   FastAPI / Starlette / any HTTP framework.

2. **Subclass of** :class:`Exception`, **not** :class:`RuntimeError`.
   Domain errors are EXPECTED outcomes of business-rule evaluation
   (e.g., a sku that already exists, a category-tree cycle, a missing
   product), not unexpected runtime failures. ``RuntimeError`` is
   reserved for "errors that cannot be detected statically and arise
   during execution" — a different semantic class.

3. **Flat hierarchy** — one base class :class:`DomainError` plus 10
   leaves, all inheriting DIRECTLY from :class:`DomainError`. No
   intermediate family classes (no ``NotFoundError`` group, no
   ``ConflictError`` group). This mirrors the Order Service's
   ``services/order-service/src/domain/errors.py`` pattern, simplifies
   ``except DomainError:`` dispatch at the FastAPI exception-handler
   boundary, and keeps the type taxonomy predictable for ``isinstance``
   checks in the error_handler.

4. **Per-leaf HTTP status mapping.** Each leaf overrides the
   :attr:`DomainError.http_status` :class:`typing.ClassVar` to
   communicate the appropriate client-facing status code:

   ============================ =====
   Class                        HTTP
   ============================ =====
   :class:`ProductNotFound`       404
   :class:`CategoryNotFound`      404
   :class:`MediaNotFound`         404
   :class:`DuplicateSku`          409
   :class:`DuplicateSlug`         409
   :class:`VersionConflict`       409
   :class:`InvalidCategoryHierarchy` 422
   :class:`InvalidProductState`   422
   :class:`UnauthorizedError`     401
   :class:`ForbiddenError`        403
   :class:`DomainError` (default) 422
   ============================ =====

   The error_handler middleware reads ``type(exc).http_status``
   directly — no per-instance state, no sprawling mapping table.

5. **Stable machine-readable** :attr:`DomainError.error_code`. Every
   concrete subclass exposes an ``error_code`` SCREAMING_SNAKE_CASE
   string. Consumers of the JSON response (mobile clients, CLI tools,
   dashboards) should key off the ``error_code`` field rather than the
   Python class name (``type``), so the ``error_code`` survives
   refactors that rename classes.

6. **Defensive** ``details`` **dict copying** — on construction AND on
   read. Callers cannot leak post-raise mutations into the response,
   and consumers of :meth:`to_response_dict` cannot mutate the
   exception's stored state by editing the returned dict. Subtle
   aliasing bugs are foreclosed by design.

7. **Ergonomic keyword-only constructors.** Each leaf accepts the
   relevant context fields as explicit keyword arguments (e.g.,
   ``ProductNotFound(product_id="...")``). Clients never need to
   assemble a generic ``details`` dict manually — the constructor
   builds it from the named fields. This mirrors the structured-log
   field names so the JSON error body and the log line carry
   consistent keys.

8. **No** I/O, **no** logging, **no** side effects at import time.
   Only the module-level constants and class definitions are
   evaluated. Keeps unit-test startup cheap and prevents the module
   from accidentally pulling in framework-specific code paths.

Cross-references
----------------
This module is consumed by every other ``src/*`` package in the Product
Service:

* ``src/domain/product.py`` — raises :class:`InvalidProductState` from
  state-transition guards.
* ``src/domain/category.py`` — raises
  :class:`InvalidCategoryHierarchy` when a tree operation would create
  a cycle or exceed maximum depth.
* ``src/repository/products_repo.py`` — raises
  :class:`ProductNotFound`, :class:`DuplicateSku`,
  :class:`DuplicateSlug`, and :class:`VersionConflict`.
* ``src/repository/categories_repo.py`` — raises
  :class:`CategoryNotFound`, :class:`DuplicateSlug`, and
  :class:`VersionConflict`.
* ``src/repository/media_repo.py`` — raises :class:`MediaNotFound` and
  :class:`VersionConflict`.
* ``src/middleware/auth.py`` — raises :class:`UnauthorizedError` /
  :class:`ForbiddenError` from JWT scope checks.
* ``src/middleware/error_handler.py`` — global FastAPI exception
  handler reads :attr:`DomainError.http_status` for the response
  status and calls :meth:`DomainError.to_response_dict` for the body.
* ``src/observability/logger.py`` — logs ``error_code``,
  ``correlation_id``, and ``details`` for every domain exception that
  propagates to a handler (AAP R-26).

Compliance notes
----------------
* AAP R-13 — correlation-ID propagation. Every :class:`DomainError`
  carries an optional ``correlation_id`` so the same identifier
  injected by the API Gateway / correlation-ID middleware survives
  into log lines and JSON error bodies for end-to-end Kibana tracing
  (AAP R-28).
* AAP R-21 / R-22 — JWT issuance and validation.
  :class:`UnauthorizedError` and :class:`ForbiddenError` are the
  domain-level signals from JWT validation and scope enforcement.
* AAP R-26 — structured JSON logs.
  :meth:`DomainError.to_response_dict` produces the canonical
  JSON-serializable error payload.
* AAP Section 0.4.5 — the error-handler middleware reads
  :attr:`DomainError.http_status` to map exceptions to HTTP status
  codes WITHOUT this module knowing anything about HTTP frameworks.

Class diagram
-------------
::

    DomainError                                    (HTTP 422)
        ├── ProductNotFound                        (HTTP 404)
        ├── CategoryNotFound                       (HTTP 404)
        ├── MediaNotFound                          (HTTP 404)
        ├── DuplicateSku                           (HTTP 409)
        ├── DuplicateSlug                          (HTTP 409)
        ├── VersionConflict                        (HTTP 409)
        ├── InvalidCategoryHierarchy               (HTTP 422)
        ├── InvalidProductState                    (HTTP 422)
        ├── UnauthorizedError                      (HTTP 401)
        └── ForbiddenError                         (HTTP 403)
"""

from __future__ import annotations

from typing import Any, ClassVar, Final

# =============================================================================
# Module-level defaults
# =============================================================================

#: Default HTTP status code for any domain error not overriding it.
#: ``422 Unprocessable Entity`` is appropriate because most domain errors
#: indicate that the REQUEST was syntactically valid (parsed successfully
#: by Pydantic / framework decoders) but semantically rejected by a
#: business rule — exactly the meaning of ``422``.
_DEFAULT_HTTP_STATUS: Final[int] = 422

#: Default machine-readable error code for any domain error not
#: overriding it. Subclasses SHOULD override this to a specific code
#: (uppercase ``SCREAMING_SNAKE_CASE``) so JSON consumers can switch on a
#: stable identifier instead of the Python class name.
_DEFAULT_ERROR_CODE: Final[str] = "DOMAIN_ERROR"


# =============================================================================
# Base class
# =============================================================================


class DomainError(Exception):
    """Base class for all Product Service domain-layer exceptions.

    Every domain exception carries:

      * ``message`` — human-readable summary (English; logs and dev
        consoles consume this).
      * ``correlation_id`` — request-scoped identifier propagated from
        the API Gateway (AAP R-13). Required for end-to-end tracing
        through Kibana (AAP R-26 / R-28). ``None`` for errors raised
        outside a request scope (e.g., startup validation).
      * ``details`` — optional structured context (machine-readable
        fields specific to each exception subclass) included in logs
        and emitted in the JSON error body. Values must be
        JSON-serializable.
      * ``__cause__`` — the standard Python attribute, set when an
        infrastructure failure is wrapped into a domain failure
        (preserved through ``raise X from Y`` semantics so the
        FastAPI handler and structured logger can chain the
        underlying driver / framework exception when present, without
        leaking it into the public response).

    Subclasses override the :class:`typing.ClassVar` attributes
    ``http_status`` and ``error_code`` ONLY. The infrastructure layers
    translate domain exceptions:

      * The FastAPI exception handler maps them to JSON 4xx / 5xx
        responses via :meth:`to_response_dict` and
        :meth:`to_status_code`.
      * The structured logger logs ``cls.error_code``,
        ``correlation_id``, and ``details``, with the underlying
        ``__cause__`` chained.

    NOT for use as a generic catch-all — call sites should always
    raise the most specific subclass.

    Parameters
    ----------
    message:
        Human-readable error message. Required positional argument.
    correlation_id:
        Optional request correlation id for log enrichment. The error
        handler stamps this onto the structured error response when
        present.
    details:
        Optional dict of additional structured context for the
        response body (e.g., ``{"product_id": "..."}``). The dict is
        COPIED defensively so caller mutations after construction do
        not leak into the response.
    cause:
        Optional underlying cause; preserved through ``__cause__`` so
        traceback chaining works with or without an explicit
        ``raise X from Y`` clause.

    Attributes
    ----------
    message: str
        Human-readable error message.
    correlation_id: str | None
        Request-scoped correlation id. ``None`` outside a request
        scope.
    details: dict[str, Any]
        Structured context dictionary. Always a fresh dict (never the
        caller's reference) so post-construction mutations do not
        bleed into the response.

    Class attributes
    ----------------
    http_status: ClassVar[int]
        HTTP status code returned by the global FastAPI exception
        handler when this exception propagates. Defaults to ``422``;
        subclasses override.
    error_code: ClassVar[str]
        Stable machine-readable code emitted in the JSON body's
        ``error_code`` field. Stable across refactors that rename the
        Python class.

    Example
    -------
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
        correlation_id: str | None = None,
        details: dict[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.message: str = message
        self.correlation_id: str | None = correlation_id
        # Defensive copy so callers cannot mutate the exception's state
        # by holding a reference to the dict they passed in. ``None``
        # becomes ``{}`` so downstream logging / serialization code
        # never has to special-case the absence of details.
        self.details: dict[str, Any] = dict(details) if details else {}
        if cause is not None:
            # Wire the standard Python ``__cause__`` so traceback
            # chaining works whether the caller used ``raise X from Y``
            # or just ``raise X(...)`` with ``cause=Y``.
            self.__cause__ = cause

    def to_status_code(self) -> int:
        """Return the HTTP status code for this exception.

        The global FastAPI exception handler reads this value to set
        the HTTP status on the JSON error response. Reads
        :attr:`http_status` from the runtime class (``type(self)``) so
        the correct override is picked up for every subclass without
        per-instance state.

        Returns
        -------
        int
            The HTTP status code (an integer in the standard 4xx /
            5xx range) defined by the class attribute
            :attr:`http_status`.
        """
        return type(self).http_status

    def to_response_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable error payload.

        Shape (RFC 7807-inspired but trimmed to the essentials)::

            {
                "error_code": "PRODUCT_NOT_FOUND",
                "message": "...",
                "correlation_id": "..." | None,
                "details": {...}
            }

        The returned dict is a fresh copy — mutating it does NOT
        affect the exception's :attr:`details` state. The FastAPI
        exception handler returns this dict verbatim as the body of
        the JSON error response.

        Returns
        -------
        dict[str, Any]
            A dict suitable for ``json.dumps``.
        """
        return {
            "error_code": type(self).error_code,
            "message": self.message,
            "correlation_id": self.correlation_id,
            # Defensive copy on read — mutating the returned dict
            # cannot affect the exception's stored ``details``.
            "details": dict(self.details),
        }

    def __repr__(self) -> str:  # pragma: no cover - dev ergonomics only
        return (
            f"{type(self).__name__}("
            f"message={self.message!r}, "
            f"correlation_id={self.correlation_id!r}, "
            f"details={self.details!r}"
            f")"
        )


# =============================================================================
# Leaf exceptions — Not Found (HTTP 404)
# =============================================================================


class ProductNotFound(DomainError):
    """Raised when a product cannot be located by id, sku, or slug.

    Raised by ``src/repository/products_repo.py`` when a lookup by
    ``product_id``, ``sku``, or ``slug`` returns no document, and by
    controllers that resolve a product before dispatching domain
    operations.

    HTTP 404 Not Found — the canonical mapping for missing resources.
    Maps to the ``PRODUCT_NOT_FOUND`` machine-readable error code.

    Parameters
    ----------
    product_id:
        The product's persistent identifier, when the lookup was by id.
    sku:
        The product's SKU, when the lookup was by SKU.
    slug:
        The product's URL slug, when the lookup was by slug.
    message:
        Optional override for the human-readable message; defaults to
        ``f"product not found: {product_id or sku or slug}"``.
    correlation_id:
        Optional request correlation id (AAP R-13).
    cause:
        Optional underlying exception (e.g., a stale cache miss
        wrapped here for the response).

    Notes
    -----
    All identifying fields are optional so callers can raise this
    exception at any granularity (most lookup paths know exactly one
    of the three). At least one identifier SHOULD be provided so the
    error log is actionable; a fallback ``<unspecified>`` is used in
    the default message when all are ``None``.
    """

    http_status: ClassVar[int] = 404
    error_code: ClassVar[str] = "PRODUCT_NOT_FOUND"

    def __init__(
        self,
        *,
        product_id: str | None = None,
        sku: str | None = None,
        slug: str | None = None,
        message: str | None = None,
        correlation_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        details: dict[str, Any] = {}
        if product_id is not None:
            details["product_id"] = product_id
        if sku is not None:
            details["sku"] = sku
        if slug is not None:
            details["slug"] = slug
        if message is None:
            ident = product_id or sku or slug or "<unspecified>"
            message = f"product not found: {ident}"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


class CategoryNotFound(DomainError):
    """Raised when a category cannot be located by id or slug.

    Raised by ``src/repository/categories_repo.py`` and by category
    tree operations that resolve a parent or sibling reference.

    HTTP 404 Not Found. Maps to the ``CATEGORY_NOT_FOUND``
    machine-readable error code.

    Parameters
    ----------
    category_id:
        The category's persistent identifier.
    slug:
        The category's URL slug (sibling-scoped).
    message:
        Optional override for the human-readable message; defaults to
        ``f"category not found: {category_id or slug}"``.
    correlation_id:
        Optional request correlation id (AAP R-13).
    cause:
        Optional underlying exception.
    """

    http_status: ClassVar[int] = 404
    error_code: ClassVar[str] = "CATEGORY_NOT_FOUND"

    def __init__(
        self,
        *,
        category_id: str | None = None,
        slug: str | None = None,
        message: str | None = None,
        correlation_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        details: dict[str, Any] = {}
        if category_id is not None:
            details["category_id"] = category_id
        if slug is not None:
            details["slug"] = slug
        if message is None:
            ident = category_id or slug or "<unspecified>"
            message = f"category not found: {ident}"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


class MediaNotFound(DomainError):
    """Raised when a product media reference cannot be located.

    Raised by ``src/repository/media_repo.py`` when a lookup by
    ``media_id`` (optionally scoped by ``product_id``) returns no
    document.

    HTTP 404 Not Found. Maps to the ``MEDIA_NOT_FOUND``
    machine-readable error code.

    Parameters
    ----------
    media_id:
        The media's persistent identifier.
    product_id:
        Optional owning product id, included for log correlation when
        the media reference was scoped to a particular product.
    message:
        Optional override for the human-readable message.
    correlation_id:
        Optional request correlation id (AAP R-13).
    cause:
        Optional underlying exception.
    """

    http_status: ClassVar[int] = 404
    error_code: ClassVar[str] = "MEDIA_NOT_FOUND"

    def __init__(
        self,
        *,
        media_id: str | None = None,
        product_id: str | None = None,
        message: str | None = None,
        correlation_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        details: dict[str, Any] = {}
        if media_id is not None:
            details["media_id"] = media_id
        if product_id is not None:
            details["product_id"] = product_id
        if message is None:
            ident = media_id or "<unspecified>"
            message = f"media not found: {ident}"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


# =============================================================================
# Leaf exceptions — Conflicts (HTTP 409)
# =============================================================================


class DuplicateSku(DomainError):
    """Raised on attempted creation/update of a product with a SKU
    that already exists in the catalog.

    Raised by ``src/repository/products_repo.py`` when a unique-index
    violation on the ``sku`` column is detected (MongoDB duplicate-key
    error code 11000).

    HTTP 409 Conflict — the request is structurally valid but the
    persisted state forbids the write. Maps to the
    ``DUPLICATE_SKU`` machine-readable error code.

    Parameters
    ----------
    sku:
        The duplicate SKU value (REQUIRED — surfacing it in the
        details payload makes the error self-describing for both
        operators and API consumers).
    existing_product_id:
        Optional id of the existing product that already owns this
        SKU; included when the repository looked it up to enrich the
        operator-facing error context.
    message:
        Optional override for the human-readable message.
    correlation_id:
        Optional request correlation id (AAP R-13).
    cause:
        Optional underlying exception (e.g., the original PyMongo
        ``DuplicateKeyError``).
    """

    http_status: ClassVar[int] = 409
    error_code: ClassVar[str] = "DUPLICATE_SKU"

    def __init__(
        self,
        *,
        sku: str,
        existing_product_id: str | None = None,
        message: str | None = None,
        correlation_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        details: dict[str, Any] = {"sku": sku}
        if existing_product_id is not None:
            details["existing_product_id"] = existing_product_id
        if message is None:
            message = f"sku already exists: {sku}"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


class DuplicateSlug(DomainError):
    """Raised on attempted creation/update of an aggregate with a slug
    that collides with an existing aggregate's slug.

    For products, slugs are service-wide unique. For categories, slugs
    are sibling-scoped (unique within the same parent). The repository
    signals both cases with this exception; the ``scope`` detail field
    distinguishes them so the error_handler / API consumer can tell
    service-wide product-slug collisions apart from sibling-scoped
    category-slug collisions without inspecting the slug shape.

    HTTP 409 Conflict. Maps to the ``DUPLICATE_SLUG`` machine-readable
    error code.

    Parameters
    ----------
    slug:
        The duplicate slug value (REQUIRED).
    scope:
        ``"product"`` for service-wide product-slug uniqueness;
        ``"category-sibling"`` for sibling-scoped category-slug
        uniqueness. Free-form ``str`` so future scopes (e.g., per
        store-front) can be added without an enum migration.
    parent_id:
        Optional parent category id when ``scope ==
        "category-sibling"``. Improves the actionability of the error
        for operators inspecting the category tree.
    message:
        Optional override for the human-readable message.
    correlation_id:
        Optional request correlation id (AAP R-13).
    cause:
        Optional underlying exception.
    """

    http_status: ClassVar[int] = 409
    error_code: ClassVar[str] = "DUPLICATE_SLUG"

    def __init__(
        self,
        *,
        slug: str,
        scope: str,  # "product" | "category-sibling"
        parent_id: str | None = None,
        message: str | None = None,
        correlation_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        details: dict[str, Any] = {"slug": slug, "scope": scope}
        if parent_id is not None:
            details["parent_id"] = parent_id
        if message is None:
            message = f"slug already in use ({scope}): {slug}"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


class VersionConflict(DomainError):
    """Raised when an optimistic-concurrency check fails because the
    current persisted version does not match the caller's expected
    version.

    Raised by ``src/repository/*.py`` when ``findOneAndUpdate`` with
    a ``version`` filter and ``$inc: { version: 1 }`` returns ``None``
    (no document matched the expected version), indicating that
    another writer has updated the aggregate in the meantime.

    HTTP 409 Conflict. Maps to the ``VERSION_CONFLICT``
    machine-readable error code. The caller (typically a controller)
    should retry the read-modify-write cycle or surface a 409 to the
    client so the user can resolve the conflict manually.

    Parameters
    ----------
    aggregate_type:
        Free-form aggregate type label (``"product"``, ``"category"``,
        or ``"product_media"``). Free-form ``str`` to avoid coupling
        this foundational module to an enum that might evolve.
    aggregate_id:
        The aggregate's persistent identifier (REQUIRED).
    expected_version:
        The version the caller believed was current (REQUIRED).
    actual_version:
        Optional — the version actually persisted at write time.
        Useful for log-based diagnosis when the repository can cheaply
        re-read the current value.
    message:
        Optional override for the human-readable message.
    correlation_id:
        Optional request correlation id (AAP R-13).
    cause:
        Optional underlying exception.
    """

    http_status: ClassVar[int] = 409
    error_code: ClassVar[str] = "VERSION_CONFLICT"

    def __init__(
        self,
        *,
        aggregate_type: str,  # "product" | "category" | "product_media"
        aggregate_id: str,
        expected_version: int,
        actual_version: int | None = None,
        message: str | None = None,
        correlation_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        details: dict[str, Any] = {
            "aggregate_type": aggregate_type,
            "aggregate_id": aggregate_id,
            "expected_version": expected_version,
        }
        if actual_version is not None:
            details["actual_version"] = actual_version
        if message is None:
            actual_clause = (
                f", actual {actual_version}"
                if actual_version is not None
                else ""
            )
            message = (
                f"version conflict on {aggregate_type} {aggregate_id}: "
                f"expected {expected_version}{actual_clause}"
            )
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


# =============================================================================
# Leaf exceptions — Unprocessable Entity (HTTP 422)
# =============================================================================


class InvalidCategoryHierarchy(DomainError):
    """Raised when a category-tree operation would violate a tree
    invariant.

    Raised by ``src/domain/category.py`` and
    ``src/repository/categories_repo.py`` when a proposed parent
    assignment would create a cycle, exceed the configured maximum
    depth, or otherwise violate the materialized-path tree contract.

    HTTP 422 Unprocessable Entity — the request was syntactically
    valid but rejected by a domain rule. Maps to the
    ``INVALID_CATEGORY_HIERARCHY`` machine-readable error code.

    Parameters
    ----------
    category_id:
        Optional id of the category being moved / created.
    parent_id:
        Optional id of the proposed parent that triggered the
        violation.
    reason:
        Short, free-form rationale (``"cycle"``, ``"max-depth"``,
        ``"self-parent"``, etc.) included in the response details so
        clients can render a specific message. Defaults to a generic
        invariant-violation phrase.
    message:
        Optional override for the human-readable message.
    correlation_id:
        Optional request correlation id (AAP R-13).
    cause:
        Optional underlying exception.
    """

    http_status: ClassVar[int] = 422
    error_code: ClassVar[str] = "INVALID_CATEGORY_HIERARCHY"

    def __init__(
        self,
        *,
        category_id: str | None = None,
        parent_id: str | None = None,
        reason: str = "category hierarchy invariant violated",
        message: str | None = None,
        correlation_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        details: dict[str, Any] = {"reason": reason}
        if category_id is not None:
            details["category_id"] = category_id
        if parent_id is not None:
            details["parent_id"] = parent_id
        if message is None:
            message = f"invalid category hierarchy: {reason}"
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


class InvalidProductState(DomainError):
    """Raised when an operation is incompatible with the product's
    current state.

    Examples include attempting to update a deprecated product,
    re-deprecating an already-deprecated product, or publishing a
    product that has not yet been validated. Raised by
    ``src/domain/product.py`` from state-transition guards.

    HTTP 422 Unprocessable Entity. Maps to the
    ``INVALID_PRODUCT_STATE`` machine-readable error code.

    Parameters
    ----------
    product_id:
        The product's persistent identifier (REQUIRED).
    current_status:
        The product's current status as a ``str`` (e.g.,
        ``"deprecated"``, ``"draft"``, ``"published"``). Free-form
        ``str`` to avoid coupling this foundational module to a
        product-status enum that might evolve.
    attempted_operation:
        Short, free-form name of the operation that was rejected
        (``"update"``, ``"deprecate"``, ``"publish"``, etc.).
    message:
        Optional override for the human-readable message.
    correlation_id:
        Optional request correlation id (AAP R-13).
    cause:
        Optional underlying exception.
    """

    http_status: ClassVar[int] = 422
    error_code: ClassVar[str] = "INVALID_PRODUCT_STATE"

    def __init__(
        self,
        *,
        product_id: str,
        current_status: str,
        attempted_operation: str,
        message: str | None = None,
        correlation_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        details: dict[str, Any] = {
            "product_id": product_id,
            "current_status": current_status,
            "attempted_operation": attempted_operation,
        }
        if message is None:
            message = (
                f"operation {attempted_operation!r} is not allowed on "
                f"product {product_id} in state {current_status!r}"
            )
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


# =============================================================================
# Leaf exceptions — Authentication / Authorization (HTTP 401 / 403)
# =============================================================================


class UnauthorizedError(DomainError):
    """Raised when a request lacks valid authentication credentials.

    Raised by ``src/middleware/auth.py`` when the JWT is missing,
    malformed, expired, or signature-invalid.

    HTTP 401 Unauthorized. Maps to the ``UNAUTHORIZED``
    machine-readable error code.

    Notes
    -----
    Despite the HTTP-flavored name, this is a DOMAIN exception —
    raised inside the domain / middleware layer when an authentication
    check fails, and translated into an HTTP 401 response by the
    error_handler. The name aligns with the standard HTTP semantics
    that operators expect to see in dashboards and logs.

    Parameters
    ----------
    message:
        Human-readable message; defaults to ``"unauthorized"``.
    correlation_id:
        Optional request correlation id (AAP R-13).
    details:
        Optional structured context (e.g., ``{"reason": "expired"}``).
        Copied defensively by the base class.
    cause:
        Optional underlying exception (e.g., the original
        ``jwt.InvalidTokenError``).
    """

    http_status: ClassVar[int] = 401
    error_code: ClassVar[str] = "UNAUTHORIZED"

    def __init__(
        self,
        message: str = "unauthorized",
        *,
        correlation_id: str | None = None,
        details: dict[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=details,
            cause=cause,
        )


class ForbiddenError(DomainError):
    """Raised when an authenticated request lacks the required scope or
    role for the requested operation.

    Raised by ``src/middleware/auth.py`` and admin endpoints when JWT
    introspection succeeds but the token's scopes do not include the
    one needed for the route (e.g., ``products:admin``).

    HTTP 403 Forbidden. Maps to the ``FORBIDDEN`` machine-readable
    error code.

    Parameters
    ----------
    message:
        Human-readable message; defaults to ``"forbidden"``.
    required_scope:
        Optional name of the scope the operation required (added to
        ``details`` so the response payload tells the caller exactly
        which scope they were missing). Operators should NEVER include
        secrets here.
    correlation_id:
        Optional request correlation id (AAP R-13).
    details:
        Optional additional structured context. Merged with
        ``required_scope`` (when provided); the caller's dict is
        copied defensively before merging.
    cause:
        Optional underlying exception.
    """

    http_status: ClassVar[int] = 403
    error_code: ClassVar[str] = "FORBIDDEN"

    def __init__(
        self,
        message: str = "forbidden",
        *,
        required_scope: str | None = None,
        correlation_id: str | None = None,
        details: dict[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        # Defensive copy of the caller's dict before mutation so the
        # caller's reference remains untouched.
        merged: dict[str, Any] = dict(details) if details else {}
        if required_scope is not None:
            merged["required_scope"] = required_scope
        super().__init__(
            message,
            correlation_id=correlation_id,
            details=merged,
            cause=cause,
        )


# =============================================================================
# Public API
# =============================================================================

#: Public re-export list. Base class is listed FIRST (mirrors the Order
#: Service ``errors.py`` convention); leaf exceptions follow in
#: alphabetical order so the eleven-element catalog is stable.
__all__ = [
    "DomainError",
    "CategoryNotFound",
    "DuplicateSku",
    "DuplicateSlug",
    "ForbiddenError",
    "InvalidCategoryHierarchy",
    "InvalidProductState",
    "MediaNotFound",
    "ProductNotFound",
    "UnauthorizedError",
    "VersionConflict",
]
