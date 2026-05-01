"""Pydantic v2 event models for inbound Kafka domain events.

This module is the **foundational schemas module** for the Recommendation
Engine's Kafka consumer pipeline. It declares every Pydantic v2 ``BaseModel``
that validates inbound payloads after Schema-Registry deserialization, and it
is imported by every other module in the ``src.events`` and ``src.features``
packages (dispatcher, consumer, handlers, feature pipeline, extractors).

Module responsibilities
-----------------------
1. **Type vocabulary** for the six inbound Kafka topics consumed by the
   Recommendation Engine:

   ============================  =====================
   Topic                         Pydantic class
   ----------------------------  ---------------------
   ``product.created``           :class:`ProductCreatedEvent`
   ``product.updated``           :class:`ProductUpdatedEvent`
   ``order.created``             :class:`OrderCreatedEvent`
   ``order.fulfilled``           :class:`OrderFulfilledEvent`
   ``user.registered``           :class:`UserRegisteredEvent`
   ``user.updated``              :class:`UserUpdatedEvent`
   ============================  =====================

2. **Validation enforcement** of three universal invariants on every inbound
   event payload:

   * ``occurred_at`` MUST be timezone-aware (rejects naive ``datetime`` per
     AAP R-26 RFC 3339 timestamps).
   * ``event_version`` MUST be a non-empty string of <= 64 characters.
   * ``category_path`` segments MUST match the injection-safe pattern
     ``^[A-Za-z0-9._/\\- ]{1,128}$`` (defense against SQL/log forging).

3. **Forward-compatible read semantics** via ``extra="ignore"`` on every
   model. Producers may add new fields in future schema versions without
   breaking the consumer (AAP R-31).

4. **Immutability** via ``frozen=True``: handlers cannot mutate events after
   validation. This prevents subtle cross-handler bugs where one handler
   silently rewrites a field that a later handler then observes.

Cross-folder consumer contracts honored by this module
------------------------------------------------------
* :mod:`src.events.dispatcher` uses ``type(event).__name__`` as the routing
  key. Class names MUST match exactly: ``ProductCreatedEvent``,
  ``ProductUpdatedEvent``, ``OrderCreatedEvent``, ``OrderFulfilledEvent``,
  ``UserRegisteredEvent``, ``UserUpdatedEvent``.
* :mod:`src.events.consumer` maps Kafka topics to class names (strings) and
  calls ``model_cls.model_validate(payload_dict)`` — Pydantic v2 supplies
  ``model_validate`` automatically for every ``BaseModel`` subclass.
* :mod:`src.features.pipeline` dispatches on ``type(event).__name__`` —
  same routing-key convention as the dispatcher.
* :mod:`src.features.extractors` reads ``event.line_items`` on order events.
  The :class:`OrderCreatedEvent` and :class:`OrderFulfilledEvent` classes
  declare ``line_items`` with ``validation_alias=AliasChoices("line_items",
  "items")`` so producers may emit either input key and the consumer's
  Python attribute access (``event.line_items``) always works.
* :mod:`src.events.schemas.__init__` re-exports all seven names (the six
  event classes plus :class:`OrderLineItem`).

Compliance with AAP rules
-------------------------
- **R-14** — Pydantic validation runs after Schema Registry deserialization
  in :mod:`src.events.consumer._deserialize_event`. This module supplies
  the schema enforcement layer.
- **R-26** — RFC 3339 timezone-aware timestamps enforced via the
  ``_validate_tz_aware`` helper used by every event class.
- **R-30** — Class names mirror the ``<domain>.<verb>`` Kafka topic naming
  convention (e.g. ``product.created`` -> ``ProductCreatedEvent``).
- **R-31** — ``extra="ignore"`` plus required ``event_version`` field on
  every class together support backward-compatible schema evolution.
- **R-33** — Models are self-contained; no model construction triggers an
  external lookup or side effect.

Side-effect freedom
-------------------
This module performs ZERO logging, ZERO I/O, and ZERO mutable global state.
Importing it is safe in any context — including pytest collection,
documentation tooling, and code-generation scripts.
"""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
# Defensive character pattern for category-path segments. Rejects anything
# outside letters, digits, '.', '_', '/', '-', and space. Length bounded to
# 1-128 characters per segment per the folder specification.
#
# Why this pattern?
# -----------------
# Category-path strings flow into two security-sensitive sinks downstream:
#
#   1. SQL — the ``rec_cache`` popularity-delta stored procedure uses
#      category-path segments as keyspace prefixes.
#   2. Log queries — Kibana log searches index category strings; a hostile
#      segment containing ``"\n[INFO] forged log entry"`` could pollute the
#      log stream and forge audit-trail entries.
#
# Constraining the character set to `[A-Za-z0-9._/\- ]` covers every legitimate
# product category (e.g. "electronics", "smart-phones", "Electronics 2.0",
# "home/kitchen") while rejecting the metacharacters that enable SQL injection,
# log forging, command injection, and HTML/JS injection.
#
# The 1-128 length bound prevents zero-length segments (which carry no
# information and confuse downstream rendering) and bounds the keyspace
# prefix size to a value PostgreSQL can index efficiently.
_CATEGORY_SEGMENT_PATTERN: re.Pattern[str] = re.compile(
    r"^[A-Za-z0-9._/\- ]{1,128}$"
)


# ---------------------------------------------------------------------------
# Private validator helpers
# ---------------------------------------------------------------------------
# These helpers are reused by every event class via ``@field_validator`` so
# that the validation logic lives in exactly one place. They are NOT exported
# in ``__all__`` and downstream modules should NOT import them directly.
def _validate_tz_aware(value: datetime) -> datetime:
    """Reject naive datetimes per the folder specification.

    Per AAP R-26 and the folder spec, all timestamps must be timezone-aware
    (RFC 3339 with explicit offset). Naive datetimes are silently dangerous
    because comparisons between naive and aware values raise ``TypeError``
    downstream — a class of bug that often surfaces only at peak load.

    The check covers both the common case (``tzinfo is None``) and the
    pathological case where a custom ``tzinfo`` subclass declares itself
    present but produces ``None`` from ``utcoffset()``. Both conditions
    indicate the datetime cannot be unambiguously rendered as RFC 3339
    and so must be rejected at the validation boundary.

    Args:
        value: A ``datetime`` potentially lacking proper ``tzinfo``.

    Returns:
        The same ``datetime``, unchanged, if it is genuinely
        timezone-aware.

    Raises:
        ValueError: If ``value.tzinfo`` is ``None`` or if
            ``value.tzinfo.utcoffset(value)`` returns ``None``. The
            error message is the exact string
            ``"occurred_at must be timezone-aware"`` mandated by the
            folder specification, so consumer logs can be matched on it
            for monitoring naive-datetime producer drift.
    """
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("occurred_at must be timezone-aware")
    return value


def _validate_category_path(value: list[str]) -> list[str]:
    """Validate every category-path segment against the injection-safe pattern.

    Each segment is checked against :data:`_CATEGORY_SEGMENT_PATTERN`. The
    function does NOT trim, lowercase, or otherwise rewrite segments — it
    is a pure guard that either accepts the input as-is or raises
    ``ValueError``. The validator returns the original list reference so
    that callers can chain it after Pydantic's type coercion.

    Empty input lists are explicitly accepted: ``ProductCreatedEvent``
    instances without any category breadcrumb are valid (the producer
    simply hasn't categorized the product yet).

    Args:
        value: A list of category-path segments. May be empty.

    Returns:
        The same list, unchanged, if every segment is a ``str`` that
        matches the pattern ``^[A-Za-z0-9._/\\- ]{1,128}$``.

    Raises:
        ValueError: If any segment is not a ``str`` (e.g. ``None`` or an
            ``int`` survived Pydantic's pre-validation), or if any
            segment fails the pattern. The error message identifies the
            offending segment by index so debugging messy producer
            payloads is straightforward.
    """
    for index, segment in enumerate(value):
        # Pydantic's type coercion should already reject non-strings, but
        # we double-check defensively. ``isinstance`` catches both ``None``
        # and exotic types that may slip through ``list[str]`` validation
        # in edge cases (e.g. when ``arbitrary_types_allowed`` is enabled
        # downstream — currently not, but the guard is cheap insurance).
        if not isinstance(segment, str):
            raise ValueError(
                f"category_path entry at index {index} is not a string: "
                f"{type(segment).__name__}"
            )
        if not _CATEGORY_SEGMENT_PATTERN.match(segment):
            raise ValueError(
                f"category_path entry at index {index} does not match the "
                f"allowed pattern '^[A-Za-z0-9._/\\- ]{{1,128}}$': {segment!r}"
            )
    return value


# ---------------------------------------------------------------------------
# Shared base class
# ---------------------------------------------------------------------------
class _EventBase(BaseModel):
    """Shared Pydantic v2 configuration for every event model in this module.

    Configuration rationale
    -----------------------
    * ``extra="ignore"`` (AAP R-31) — producers may add new fields in future
      schema versions; ignoring unknown fields preserves backward-compatible
      read semantics so this consumer never crashes when a forward-compatible
      producer upgrade lands first.
    * ``frozen=True`` — events are immutable; once validated, handlers
      cannot mutate them in place. This prevents subtle cross-handler bugs
      where one handler accidentally rewrites a field that a subsequent
      handler reads. Pydantic raises :class:`pydantic.ValidationError` on
      any attempted assignment after construction.
    * ``populate_by_name=True`` — allows Python attribute names to be used
      even when field aliases are configured. Required because the
      :attr:`OrderCreatedEvent.line_items` field carries a
      ``validation_alias=AliasChoices(...)``; without ``populate_by_name``
      the Python attribute name might be unreachable from certain code
      paths (e.g. ``model_construct`` calls).
    * ``str_strip_whitespace=True`` — defensive trimming on every string
      field. Combined with ``Field(min_length=1)`` on ``event_version``,
      this rejects whitespace-only inputs declaratively, without needing
      a custom validator.

    Privacy
    -------
    The leading underscore is **not** a strict access modifier — Python
    does not enforce private attributes — but it signals two things:

    1. The class is excluded from ``__all__`` and downstream code should
       not subclass it directly (subclass concrete event classes if you
       need event-class-specific behavior).
    2. The class exists ONLY to share ``ConfigDict`` between event models;
       it is not part of the public API.
    """

    model_config = ConfigDict(
        extra="ignore",
        frozen=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )


# ---------------------------------------------------------------------------
# OrderLineItem — nested model used by Order events
# ---------------------------------------------------------------------------
class OrderLineItem(_EventBase):
    """A single line item within an order event.

    The ``category_path`` and ``category`` fields are BOTH optional because
    different producers of ``order.created`` and ``order.fulfilled`` emit
    different shapes:

    * Some producers include the full hierarchical ``category_path`` inline
      to avoid a round-trip lookup to Product Service in the consumer.
    * Some producers include a flat ``category`` string instead.
    * Some producers include neither; the consumer's feature extractor
      falls back to the ``__unknown__`` bucket in that case (handled in
      :mod:`src.features.extractors`).

    The ``rating`` field is optional and is present only on
    ``order.fulfilled`` events that additionally carry a user-submitted
    post-delivery rating. The Order Service folds post-delivery ratings
    into the fulfillment event rather than emitting a separate
    ``order.rated`` topic — this keeps the recommendation feature pipeline
    simpler and avoids race conditions between fulfillment and rating
    events for the same order.

    All monetary values use :class:`decimal.Decimal` (NOT ``float``) to
    preserve precision and avoid IEEE-754 rounding errors when the value
    is ultimately stored to PostgreSQL ``NUMERIC`` columns or rendered to
    end users.

    Attributes:
        product_id: UUID of the catalog product being purchased.
        quantity: Number of units purchased; must be >= 1 (``Field(ge=1)``).
            Zero-quantity lines indicate a producer bug and are rejected.
        unit_price: Price per unit at the time of the order. Producers
            should send the order-time snapshot of the price even if the
            catalog price has since changed. No constraint is applied
            here because legitimate refund/credit flows may use negative
            values in test fixtures; production validation lives in the
            Order Service itself.
        category_path: Optional hierarchical category breadcrumb for the
            product. Each segment is validated against
            :data:`_CATEGORY_SEGMENT_PATTERN`.
        category: Alternative flat category string. No pattern validation
            (the producer-side schema bounds shape).
        rating: Optional post-fulfillment user rating in the inclusive
            range ``[0.0, 5.0]``. Pydantic's ``Field(ge=0, le=5)``
            constraints enforce the bounds.
    """

    product_id: UUID
    quantity: int = Field(..., ge=1)
    unit_price: Decimal
    category_path: list[str] | None = None
    category: str | None = None
    rating: float | None = Field(default=None, ge=0.0, le=5.0)

    @field_validator("category_path")
    @classmethod
    def _check_category_path(cls, v: list[str] | None) -> list[str] | None:
        """Run :func:`_validate_category_path` only when the field is set.

        Pydantic invokes field validators before defaults are applied, but
        ``None`` defaults are passed through as ``None``. We short-circuit
        on ``None`` so optional category paths skip pattern validation.

        Args:
            v: The proposed value for ``category_path``.

        Returns:
            ``None`` if ``v`` was ``None``, otherwise ``v`` unchanged
            after every segment passes the injection-safe pattern check.

        Raises:
            ValueError: Propagated from :func:`_validate_category_path`
                if any segment is invalid.
        """
        if v is None:
            return v
        return _validate_category_path(v)


# ---------------------------------------------------------------------------
# Product event models
# ---------------------------------------------------------------------------
# ``ProductCreatedEvent`` and ``ProductUpdatedEvent`` carry an identical
# field shape per the folder specification, but they are declared as TWO
# DISTINCT classes for three reasons:
#
#   1. The dispatcher uses ``type(event).__name__`` as its routing key; two
#      classes are required for distinct routing.
#   2. The handler methods diverge: ``handle_created`` writes a brand-new
#      embedding to pgvector, while ``handle_updated`` additionally
#      invalidates ``product_metadata:{id}`` in Redis (see the dispatcher's
#      Integration Map in ``docs/architecture/event-catalog.md``).
#   3. Future evolution may diverge their fields (e.g. ``ProductUpdatedEvent``
#      may grow a ``previous_price`` field for delta computation). Keeping
#      them separate today makes that future change a one-class edit
#      rather than an enum-tag schema migration.
#
# A common parent or single class with an ``EventType`` enum was rejected on
# the grounds that the established cross-folder dispatch pattern operates on
# class names, not on enum tags.
class ProductCreatedEvent(_EventBase):
    """Event emitted by Product Service when a new product is created.

    Topic: ``product.created``

    Consumer behavior (Recommendation Engine)
    -----------------------------------------
    On receipt of this event, the Recommendation Engine:

    1. Computes a product embedding via the model inference runtime
       (see :mod:`src.inference.scorer`).
    2. Upserts the embedding into pgvector's ``embeddings`` table
       (see :mod:`src.repository.embeddings_repo`).
    3. Records the event metadata (``occurred_at``, ``event_version``)
       in a small idempotency table so replays — common when Kafka
       consumer offsets reset — do not double-write the same embedding.

    Attributes:
        product_id: UUID of the product being created.
        category_path: Hierarchical category breadcrumb (e.g.
            ``["electronics", "phones", "smartphones"]``). Defaults to
            an empty list. Each segment is validated against
            :data:`_CATEGORY_SEGMENT_PATTERN`.
        price: Optional price; some producers emit catalog-only events
            without pricing context (price lookup happens later via the
            Pricing Service for those flows).
        tags: Optional product tags / keywords; defaults to ``[]``. No
            per-entry validation — tags are producer-controlled
            free-text and the producer's own schema bounds them.
        occurred_at: RFC 3339 timestamp of when the product was created.
            Must be timezone-aware; naive datetimes are rejected.
        event_version: Producer-side schema version (semver-ish string,
            1-64 chars). Required on every event so consumers can branch
            on schema evolution.
    """

    product_id: UUID
    category_path: list[str] = Field(default_factory=list)
    price: Decimal | None = None
    tags: list[str] = Field(default_factory=list)
    occurred_at: datetime
    event_version: str = Field(..., min_length=1, max_length=64)

    @field_validator("occurred_at")
    @classmethod
    def _check_tz_aware(cls, v: datetime) -> datetime:
        """Delegate to :func:`_validate_tz_aware` to enforce RFC 3339.

        Args:
            v: The proposed ``occurred_at`` value.

        Returns:
            The same datetime if ``tzinfo`` carries a real UTC offset.

        Raises:
            ValueError: With the canonical message
                ``"occurred_at must be timezone-aware"`` when the
                datetime is naive.
        """
        return _validate_tz_aware(v)

    @field_validator("category_path")
    @classmethod
    def _check_category_path(cls, v: list[str]) -> list[str]:
        """Delegate to :func:`_validate_category_path` for pattern checks.

        Args:
            v: The proposed list of category-path segments. Always a
                ``list[str]`` for product events because the field
                defaults to ``[]`` rather than ``None``.

        Returns:
            The same list unchanged when every segment passes the
            injection-safe pattern.

        Raises:
            ValueError: Propagated from
                :func:`_validate_category_path` if any segment is
                invalid.
        """
        return _validate_category_path(v)


class ProductUpdatedEvent(_EventBase):
    """Event emitted by Product Service when an existing product is updated.

    Topic: ``product.updated``

    Field shape is identical to :class:`ProductCreatedEvent`; the distinct
    class enables routing to a distinct handler (``handle_updated``) that
    additionally:

    1. Invalidates the ``product_metadata:{id}`` Redis cache entry.
    2. Re-computes the product embedding (price changes can shift
       neighbor relationships in pgvector).
    3. Optionally publishes a derived ``recommendation.refreshed``
       cache-invalidation message so downstream consumers can purge
       stale recommendation lists for affected users.

    Attributes:
        product_id: UUID of the product being updated.
        category_path: Hierarchical category breadcrumb; defaults to ``[]``.
            Each segment is validated against
            :data:`_CATEGORY_SEGMENT_PATTERN`.
        price: Optional updated price; absent when the update only
            changes non-pricing metadata (e.g. description, tags).
        tags: Updated tag set; defaults to ``[]``. Producer is
            responsible for emitting the full new tag set on every
            update — partial / delta tag updates are NOT supported in
            this event shape.
        occurred_at: RFC 3339 timestamp of the update.
        event_version: Producer-side schema version (1-64 chars).
    """

    product_id: UUID
    category_path: list[str] = Field(default_factory=list)
    price: Decimal | None = None
    tags: list[str] = Field(default_factory=list)
    occurred_at: datetime
    event_version: str = Field(..., min_length=1, max_length=64)

    @field_validator("occurred_at")
    @classmethod
    def _check_tz_aware(cls, v: datetime) -> datetime:
        """Delegate to :func:`_validate_tz_aware` to enforce RFC 3339.

        Args:
            v: The proposed ``occurred_at`` value.

        Returns:
            The same datetime if ``tzinfo`` carries a real UTC offset.

        Raises:
            ValueError: When the datetime is naive.
        """
        return _validate_tz_aware(v)

    @field_validator("category_path")
    @classmethod
    def _check_category_path(cls, v: list[str]) -> list[str]:
        """Delegate to :func:`_validate_category_path` for pattern checks.

        Args:
            v: The proposed list of category-path segments.

        Returns:
            The same list when every segment matches the pattern.

        Raises:
            ValueError: When any segment fails the pattern check.
        """
        return _validate_category_path(v)


# ---------------------------------------------------------------------------
# Order event models
# ---------------------------------------------------------------------------
# Both ``OrderCreatedEvent`` and ``OrderFulfilledEvent`` declare a
# ``line_items`` field with both ``alias="items"`` and
# ``validation_alias=AliasChoices("line_items", "items")``:
#
#   * ``alias="items"`` is the SERIALIZATION alias. If a downstream caller
#     ever round-trips an event with ``model_dump(by_alias=True)``, the
#     emitted JSON uses the ``items`` key — which matches what the older
#     order-event schemas in some producer services still emit. This is
#     belt-and-suspenders forward compatibility.
#   * ``validation_alias=AliasChoices("line_items", "items")`` is the
#     VALIDATION alias. Pydantic accepts payloads that key the field by
#     EITHER name. The first choice (``line_items``) wins when both are
#     present, but in practice producers send only one or the other.
#
# Combined with ``populate_by_name=True`` in :class:`_EventBase`, the Python
# attribute name is always ``line_items`` regardless of which input name
# was used. Feature extractors can rely on ``event.line_items`` everywhere.
class OrderCreatedEvent(_EventBase):
    """Event emitted by Order Service when a new order is placed.

    Topic: ``order.created``

    Consumer behavior (Recommendation Engine)
    -----------------------------------------
    On receipt of this event, the Recommendation Engine records a WEAK
    "view-intent" interaction signal per line item via the feature
    pipeline (``view_count_delta=1``). Strong purchase signals are only
    recorded once the order is FULFILLED — see :class:`OrderFulfilledEvent`.
    This split avoids inflating the purchase signal for orders that are
    later cancelled or returned.

    Attributes:
        order_id: UUID of the newly created order.
        user_id: UUID of the customer who placed the order.
        line_items: Per-product line items. Validated as
            ``list[OrderLineItem]``. Accepts payload key ``line_items``
            OR ``items``; Python attribute access is always
            ``event.line_items``.
        occurred_at: RFC 3339 timestamp of order creation.
        event_version: Producer-side schema version (1-64 chars).
    """

    order_id: UUID
    user_id: UUID
    line_items: list[OrderLineItem] = Field(
        ...,
        alias="items",
        validation_alias=AliasChoices("line_items", "items"),
    )
    occurred_at: datetime
    event_version: str = Field(..., min_length=1, max_length=64)

    @field_validator("occurred_at")
    @classmethod
    def _check_tz_aware(cls, v: datetime) -> datetime:
        """Delegate to :func:`_validate_tz_aware` to enforce RFC 3339.

        Args:
            v: The proposed ``occurred_at`` value.

        Returns:
            The same datetime if ``tzinfo`` carries a real UTC offset.

        Raises:
            ValueError: When the datetime is naive.
        """
        return _validate_tz_aware(v)


class OrderFulfilledEvent(_EventBase):
    """Event emitted by Order Service when an order is fulfilled.

    Topic: ``order.fulfilled``

    Consumer behavior (Recommendation Engine)
    -----------------------------------------
    On receipt of this event, the Recommendation Engine records a STRONG
    "purchase" interaction signal per line item via the feature pipeline:

    * ``purchase_count_delta = line_item.quantity``
    * If ``line_item.rating`` is set, the rating is folded into the
      feature delta (post-delivery satisfaction signal).

    The event also feeds the tier-3 popularity-based fallback path
    (:mod:`src.fallback.popularity`) via
    ``rec_cache_repo.apply_popularity_delta`` so degraded-mode
    recommendations stay current even when the ML inference service is
    unavailable (AAP R-20 — fallback declared for every dependency).

    Attributes:
        order_id: UUID of the fulfilled order.
        user_id: UUID of the customer.
        line_items: Per-product line items. Each may carry an optional
            ``rating`` reflecting post-delivery satisfaction. Accepts
            payload key ``line_items`` OR ``items``.
        occurred_at: RFC 3339 timestamp of fulfillment.
        event_version: Producer-side schema version (1-64 chars).
    """

    order_id: UUID
    user_id: UUID
    line_items: list[OrderLineItem] = Field(
        ...,
        alias="items",
        validation_alias=AliasChoices("line_items", "items"),
    )
    occurred_at: datetime
    event_version: str = Field(..., min_length=1, max_length=64)

    @field_validator("occurred_at")
    @classmethod
    def _check_tz_aware(cls, v: datetime) -> datetime:
        """Delegate to :func:`_validate_tz_aware` to enforce RFC 3339.

        Args:
            v: The proposed ``occurred_at`` value.

        Returns:
            The same datetime if ``tzinfo`` carries a real UTC offset.

        Raises:
            ValueError: When the datetime is naive.
        """
        return _validate_tz_aware(v)


# ---------------------------------------------------------------------------
# User event models
# ---------------------------------------------------------------------------
class UserRegisteredEvent(_EventBase):
    """Event emitted by User Service (via Auth Service) on user registration.

    Topic: ``user.registered``

    Consumer behavior (Recommendation Engine)
    -----------------------------------------
    On receipt of this event, the Recommendation Engine initializes the
    user's embedding row in pgvector. The initial embedding is either:

    * The zero vector — pure cold start, recommendations must come from
      the popularity-based fallback path until interaction signals
      accumulate.
    * A baseline derived from regional popularity, if ``region`` is
      provided — gives new users from underrepresented regions slightly
      better starting recommendations than zero.

    Distinct from :class:`UserUpdatedEvent` because user registration is
    ALWAYS a meaningful signal (a new user exists), whereas an update
    may be a no-op (e.g. the user merely refreshed their profile page
    without changing anything).

    Attributes:
        user_id: UUID of the newly registered user.
        preferences: Optional arbitrary key->value string map. Defaults
            to ``None`` (treated identically to an empty dict by the
            extractor). Common keys include ``"theme"``, ``"language"``,
            ``"currency"``.
        region: Optional ISO 3166-1 alpha-2 country code or similar
            region identifier (e.g. ``"IN"``, ``"US"``, ``"EU-DE"``).
            Used by the recommendation engine for region-weighted
            scoring. No pattern validation here; the producer-side
            schema bounds the shape.
        occurred_at: RFC 3339 timestamp of registration.
        event_version: Producer-side schema version (1-64 chars).
    """

    user_id: UUID
    preferences: dict[str, str] | None = None
    region: str | None = None
    occurred_at: datetime
    event_version: str = Field(..., min_length=1, max_length=64)

    @field_validator("occurred_at")
    @classmethod
    def _check_tz_aware(cls, v: datetime) -> datetime:
        """Delegate to :func:`_validate_tz_aware` to enforce RFC 3339.

        Args:
            v: The proposed ``occurred_at`` value.

        Returns:
            The same datetime if ``tzinfo`` carries a real UTC offset.

        Raises:
            ValueError: When the datetime is naive.
        """
        return _validate_tz_aware(v)


class UserUpdatedEvent(_EventBase):
    """Event emitted by User Service when a user's profile is updated.

    Topic: ``user.updated``

    Consumer behavior (Recommendation Engine)
    -----------------------------------------
    On receipt of this event, the Recommendation Engine consumes the
    update ONLY when ``preferences`` or ``region`` carry meaningful
    changes. The downstream feature extractor for this event returns
    ``None`` if both fields are empty (a no-op update that does not
    affect recommendations) — this is the correct separation of
    concerns: this Pydantic model enforces SHAPE; the extractor
    enforces SEMANTIC no-op detection.

    Attributes:
        user_id: UUID of the user whose profile changed.
        preferences: Updated preferences map (full replacement, not
            delta). Defaults to ``None``.
        region: Updated region identifier. Defaults to ``None``.
        occurred_at: RFC 3339 timestamp of the update.
        event_version: Producer-side schema version (1-64 chars).
    """

    user_id: UUID
    preferences: dict[str, str] | None = None
    region: str | None = None
    occurred_at: datetime
    event_version: str = Field(..., min_length=1, max_length=64)

    @field_validator("occurred_at")
    @classmethod
    def _check_tz_aware(cls, v: datetime) -> datetime:
        """Delegate to :func:`_validate_tz_aware` to enforce RFC 3339.

        Args:
            v: The proposed ``occurred_at`` value.

        Returns:
            The same datetime if ``tzinfo`` carries a real UTC offset.

        Raises:
            ValueError: When the datetime is naive.
        """
        return _validate_tz_aware(v)


# ---------------------------------------------------------------------------
# Public API surface
# ---------------------------------------------------------------------------
# ``__all__`` enumerates the seven names that ``src.events.schemas.__init__``
# re-exports. Listed in alphabetical order for deterministic diffs and
# matching the import order in the sibling ``__init__.py`` file.
#
# Excluded from the public API (intentionally):
#   * ``_EventBase`` — internal base for ConfigDict sharing only.
#   * ``_validate_tz_aware`` — private validator helper.
#   * ``_validate_category_path`` — private validator helper.
#   * ``_CATEGORY_SEGMENT_PATTERN`` — private compiled regex.
__all__: list[str] = [
    "OrderCreatedEvent",
    "OrderFulfilledEvent",
    "OrderLineItem",
    "ProductCreatedEvent",
    "ProductUpdatedEvent",
    "UserRegisteredEvent",
    "UserUpdatedEvent",
]

