"""Category aggregate root for the Product Service domain layer.

The :class:`Category` aggregate models a tree of categories where each
category has at most one parent and may have many children. Categories
are persisted as documents in the MongoDB ``categories`` collection
inside the Product Service's private ``product_db`` (per AAP Section
0.4.4 — database-per-service). The Product Service is the SOLE owner of
this collection (AAP R-6).

The :attr:`Category.path` field is a **materialized list of ancestor
ids** — the chain runs root-to-immediate-parent and is EXCLUSIVE of the
category itself (the category's own id is NEVER part of its own path).
Materialization enables fast subtree queries via a single equality /
prefix match on ``path`` rather than recursive lookups. The trade-off is
that moving a category between parents requires rewriting ``path`` for
the entire subtree (an O(N) operation handled by a dedicated repository
method, NOT exposed on the aggregate).

Cycle detection (a ``parent_id`` pointing into the subtree of an
ancestor) is performed at the repository layer using full-path
inspection — the aggregate alone cannot verify a non-trivial cycle
without I/O. This module enforces only **trivially-detectable** cycles:

  * ``parent_id == id``                  (self-cycle)
  * ``id in path``                       (id appears in its own ancestry)

Both checks happen at construction time inside the
:meth:`Category._validate_path_consistency` model validator and raise
:class:`pydantic.ValidationError` if violated.

Design principles
-----------------
1. **Aggregate root, not value object.** A :class:`Category` document
   has its own stable :attr:`id`, an :attr:`updated_at` lifecycle, and
   a private collection. It is fully self-contained. ``parent_id`` is a
   foreign-key-style reference to a peer document in the same
   ``product_db`` (NOT cross-service per AAP R-6). The aggregate
   mirrors :class:`src.domain.product_media.ProductMedia` in shape and
   :class:`src.domain.product.Product` in lifecycle conventions.

2. **Frozen / immutable.** ``model_config.frozen=True`` prevents
   accidental mutation in repositories or handlers. To "modify" a
   category, callers MUST use :meth:`apply_changes`, which returns a
   fresh instance via :meth:`pydantic.BaseModel.model_copy`. Direct
   field assignment after construction raises
   :class:`pydantic.ValidationError`.

3. **Strict typing & strict configuration.** ``extra="forbid"`` rejects
   unknown fields (defends against typos in API request bodies and
   Kafka event payloads decoded into this model). String fields are
   automatically stripped via ``str_strip_whitespace=True``. Naive
   datetimes are rejected by an explicit field validator
   (:meth:`Category._require_tzaware`).

4. **Optimistic-concurrency version field.** :attr:`version` mirrors
   the convention used by :class:`src.domain.product.Product` and
   :class:`src.domain.product_media.ProductMedia`: the field starts at
   ``1`` for new documents; the repository increments it via
   ``findOneAndUpdate`` filter + ``$inc`` on every update;
   :meth:`apply_changes` does NOT increment it (that is the
   repository's job).

5. **Materialized path with parent_id ↔ path consistency.** The
   following invariants are enforced at construction time inside
   :meth:`Category._validate_path_consistency`:

   * ``parent_id is None  ⇔  path == []``  (root categories have an
     empty path; non-root categories MUST have a non-empty path).
   * ``path[-1] == parent_id``  (when ``parent_id`` is set, the last
     element of ``path`` MUST be ``parent_id`` because ``path`` runs
     root-to-immediate-parent, exclusive of self).
   * ``id not in path``         (trivial cycle: self in own ancestry).
   * ``parent_id != id``        (trivial cycle: self-as-own-parent).
   * ``updated_at >= created_at``  (monotonic lifecycle timestamps).

6. **Slug uniqueness scope is sibling-only.** Per the folder spec key
   insight, slug uniqueness is **service-wide for products** but
   **scoped to siblings for categories** (since the slug forms part of
   the canonical URL ``/categories/{path...}/{slug}``). The aggregate
   itself does NOT enforce uniqueness — that is repository-layer I/O
   (a compound index on ``(parent_id, slug)``); the aggregate only
   enforces that the slug matches the URL-safe character class
   :data:`_SLUG_PATTERN` and does not begin with a reserved prefix
   (e.g., ``admin``, ``api``).

7. **No I/O, no logging, no module-level side effects.** Only the
   module-level constants, the precompiled regex pattern, the
   :class:`CategoryStatus` enum, and the :class:`Category` class are
   evaluated at import time. Keeps unit-test startup cheap and
   prevents the module from accidentally pulling in framework-specific
   code paths. NO MongoDB / Kafka / HTTP imports anywhere in this
   module — the aggregate is a leaf in the import graph.

Field summary
-------------
* ``id: str``                  — globally-unique aggregate identifier
                                 (UUID4, stored as string for
                                 JSON / MongoDB serialization).
* ``slug: str``                — URL-safe lowercase identifier
                                 matching ``^[a-z0-9]+(-[a-z0-9]+)*$``;
                                 must NOT begin with a reserved prefix.
* ``name: str``                — human-readable category name.
* ``parent_id: str | None``    — id of the immediate parent category;
                                 ``None`` ⇔ root category.
* ``path: list[str]``          — materialized list of ancestor ids,
                                 root-to-immediate-parent, EXCLUSIVE of
                                 self. Empty for root categories;
                                 ``path[-1] == parent_id`` for
                                 non-root.
* ``description: str | None``  — optional long-form description (SEO,
                                 landing pages).
* ``display_order: int``       — sort order within the same parent;
                                 lower values appear first; defaults
                                 to ``0``.
* ``is_visible: bool``         — whether the category is visible on
                                 the storefront; defaults to ``True``.
* ``version: int``             — optimistic-concurrency token; starts
                                 at ``1``; incremented by the
                                 repository on every update.
* ``created_at: datetime``     — UTC creation timestamp; immutable.
* ``updated_at: datetime``     — UTC most-recent-update timestamp;
                                 bumped by :meth:`apply_changes`.

Why the slug RESERVED-prefix list?
* The values in :data:`_RESERVED_SLUG_PREFIXES` (``admin``, ``api``,
  ``health``, ``metrics``) collide with operational paths the API
  Gateway exposes. A category slug ``admin-tools`` would generate the
  URL ``/categories/admin-tools`` which conflicts with the gateway's
  ``/admin/...`` route family and creates ambiguous routing. Rejecting
  these prefixes at the domain layer guarantees the conflict cannot
  propagate to persistence.

Why the path-depth bound (:data:`MAX_PATH_DEPTH`)?
* Real-world taxonomies rarely exceed 5-6 levels. A defensive bound of
  ``10`` accommodates edge cases (electronics > smartphones > apple >
  iphone > iphone-15 > iphone-15-pro) while preventing pathological
  trees that would inflate the embedded ``path`` array on every child
  document and slow subtree queries.

Authoritative references
------------------------
* AAP Section 0.4.4 — ``product_db`` MongoDB ``categories`` collection
  ownership.
* AAP Section 0.5.2.2 bullet 4 — Product Service implementation plan.
* AAP R-6 — Database per service: ``parent_id`` is a peer-document
  reference within ``product_db``, NOT a cross-service foreign key.
* AAP R-7 — MongoDB chosen for flexible schema; categories carry
  variable optional metadata (description, banner image, theme).
* AAP R-26 — Structured JSON-friendly fields (every primitive is
  trivially JSON-serializable).
* AAP R-30 / R-32 / R-33 — Event semantics. Categories do not
  currently emit dedicated Kafka events; :meth:`to_event_envelope` is
  provided for symmetry with :class:`src.domain.product.Product` and
  :class:`src.domain.product_media.ProductMedia` and for future use
  (e.g., a hypothetical ``category.created`` event).
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Annotated, Any, Final

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from slugify import slugify


# =============================================================================
# Module-level constants
# =============================================================================
#
# All length / range ceilings exposed on this module are typed
# :class:`typing.Final` so static analyzers (mypy) prevent accidental
# reassignment elsewhere in the codebase. Tests can import these symbols
# directly to verify boundary semantics without spinning up the
# configuration layer.

#: Maximum length of the ``name`` field (in characters).
#:
#: 200 chars accommodates verbose category labels (e.g., multi-language
#: variants joined together for catalog search) while bounding DoS-via-
#: oversized-document attacks. The MongoDB collection's JSON-Schema
#: validator (see ``migrations/jsonschema/category_validator.json``)
#: caps the same field at 256 to allow a small buffer for emoji and
#: combining characters; the application layer is the stricter bound.
MAX_NAME_LENGTH: Final[int] = 200

#: Maximum length of the ``slug`` field (in characters).
#:
#: 120 chars matches the conventional cap on URL path segments across
#: search-engine SEO recommendations and HTTP intermediaries (NGINX,
#: AWS ALB, CloudFront). Any single path segment beyond this length is
#: pathological and frequently triggers proxy-side truncation.
MAX_SLUG_LENGTH: Final[int] = 120

#: Maximum length of the ``description`` field (in characters).
#:
#: 5000 chars accommodates SEO-rich landing-page descriptions (a
#: meta description is at most 160 chars, but rendered category
#: landing pages can include several paragraphs of merchandising
#: copy). Bounded to prevent unbounded document growth that would
#: inflate the ``categories`` collection size.
MAX_DESCRIPTION_LENGTH: Final[int] = 5000

#: Maximum depth of the ``path`` array.
#:
#: Real-world taxonomies rarely exceed 5-6 levels. ``10`` is a
#: defensive bound that accommodates edge cases (electronics >
#: smartphones > apple > iphone > iphone-15 > iphone-15-pro > ...)
#: while preventing pathological trees that would inflate the
#: embedded ``path`` array on every child document and slow subtree
#: queries (each subtree query scans a ``path`` array of up to this
#: many elements).
MAX_PATH_DEPTH: Final[int] = 10

#: Minimum value for the ``version`` field.
#:
#: Aggregates start their lifecycle at version 1 (the
#: :meth:`Category.new` factory hard-codes this). The repository
#: increments via ``findOneAndUpdate`` filter ``{_id, version: N}`` +
#: ``$inc: {version: 1}`` on every update, so ``version`` is strictly
#: monotonically increasing. ``0`` and negative values would indicate
#: a corrupted document and are rejected at construction time.
MIN_VERSION: Final[int] = 1

#: Minimum value for the ``display_order`` field.
#:
#: ``display_order`` is a 0-indexed sort key within the same parent —
#: the first sibling has ``display_order=0``. Negative values are
#: nonsensical and are rejected at construction time. The
#: :meth:`Category.new` factory defaults to ``0`` which always
#: satisfies this lower bound.
MIN_DISPLAY_ORDER: Final[int] = 0


# =============================================================================
# Precompiled regex patterns and reserved-prefix set (private)
# =============================================================================
#
# Compiled once at module load so that field-level validation does not
# pay the regex compilation cost on every model construction.
# :data:`_SLUG_PATTERN` is referenced by the Pydantic
# ``Field(pattern=...)`` constraint on :attr:`Category.slug` (which
# requires the source string, accessed via the ``.pattern`` attribute).
# The compiled :class:`re.Pattern` object is also kept in case a
# downstream consumer needs to validate a candidate slug ahead of
# Category construction (e.g., a service-level pre-flight check).

#: Slug pattern: lowercase alphanumeric segments separated by single
#: hyphens. Anchored at both ends (``^...$``) so partial matches do
#: not pass.
#:
#: Examples accepted:  ``electronics``, ``mobile-phones``,
#:                     ``apple-iphone-15``, ``a1b2-c3``.
#: Examples rejected:  ``Electronics`` (uppercase),
#:                     ``mobile_phones`` (underscore),
#:                     ``-mobile`` (leading hyphen),
#:                     ``mobile-`` (trailing hyphen),
#:                     ``mobile--phones`` (double hyphen),
#:                     `` mobile`` (leading whitespace; would have
#:                     been stripped by Pydantic
#:                     ``str_strip_whitespace=True`` first), empty
#:                     string.
#:
#: The same source string is shared with the Pydantic
#: ``Field(pattern=...)`` constraint on :attr:`Category.slug` so the
#: rule is declared exactly once.
_SLUG_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")

#: Reserved slug prefixes that collide with operational paths exposed
#: by the API Gateway. Any slug whose first hyphen-separated segment
#: matches one of these values is rejected by
#: :meth:`Category._validate_slug_not_reserved`.
#:
#: The prefixes are:
#:   * ``admin``    — admin console / management API path family.
#:   * ``api``      — versioned API path (``/api/v1/...``).
#:   * ``health``   — Kubernetes liveness / readiness probes.
#:   * ``metrics``  — Prometheus metrics scrape endpoint.
#:
#: A category slug like ``admin-tools`` would generate the URL
#: ``/categories/admin-tools`` and downstream observers might confuse
#: it with the operational ``/admin/...`` family. Rejecting the prefix
#: at the domain layer guarantees the conflict cannot propagate to
#: persistence. Stored as a :class:`frozenset` so membership checks
#: are O(1) and the set itself is hashable / immutable.
_RESERVED_SLUG_PREFIXES: Final[frozenset[str]] = frozenset(
    {"admin", "api", "health", "metrics"}
)


# =============================================================================
# CategoryStatus enum
# =============================================================================


class CategoryStatus(StrEnum):
    """Lifecycle states for a category — advisory enum.

    The folder spec for the Category aggregate uses a boolean
    ``is_visible`` flag rather than an enum; that is the AUTHORITATIVE
    visibility field on :class:`Category`. This enum is exposed for
    symmetry with :class:`src.domain.product.ProductStatus` and for
    future expansion (e.g., a separate ``archived`` state that does
    not collapse into ``is_visible=False``).

    Subclasses :class:`enum.StrEnum` (Python 3.11+) so members are
    simultaneously :class:`str` instances AND enum members. This
    enables transparent serialization to MongoDB / JSON without a
    custom encoder, transparent comparison with raw strings
    (``CategoryStatus.ACTIVE == "active"``), and direct coercion from
    string values when constructing the enum from a stored or wire
    representation (``CategoryStatus(value)``).

    Values are lowercase strings (``"active"``, ``"inactive"``,
    ``"archived"``) to match the user-facing API representation. This
    deviates from the conventional UPPERCASE Python enum convention
    used by :class:`OrderStatus` / :class:`PaymentStatus` in the Order
    / Payment Services because those enums are NOT user-facing — they
    are internal state-machine tokens. :class:`CategoryStatus`, by
    contrast, is intended for direct use in API responses and admin
    reporting tools, so the canonical form is the lowercase string.

    Attributes:
        ACTIVE: Category is visible and selectable on the storefront.
            The most common state; corresponds to ``is_visible=True``
            on :class:`Category`.
        INACTIVE: Category is hidden but not soft-deleted; can be
            re-activated. Corresponds to ``is_visible=False`` on
            :class:`Category` for un-archived hidden categories.
        ARCHIVED: Category is soft-deleted; kept for historical
            product references but hidden from category listings.
            This state does NOT have a direct boolean equivalent on
            :class:`Category` and is reserved for future expansion.

    Example:
        >>> CategoryStatus.ACTIVE == "active"
        True
        >>> CategoryStatus("inactive") is CategoryStatus.INACTIVE
        True
        >>> CategoryStatus.ARCHIVED.value
        'archived'
        >>> isinstance(CategoryStatus.ACTIVE, str)
        True
    """

    ACTIVE = "active"
    INACTIVE = "inactive"
    ARCHIVED = "archived"


# =============================================================================
# Slug helper (private)
# =============================================================================


def _make_slug(name: str) -> str:
    """Generate a URL-safe ASCII slug from a category name.

    Wraps :func:`slugify.slugify` (from the ``python-slugify``
    package) with the parameters that are consistent across the
    catalog domain:

      * ``max_length=MAX_SLUG_LENGTH`` truncates overly-long slugs to
        the field's hard cap.
      * ``word_boundary=True`` trims at word boundaries (rather than
        mid-word) when truncation would otherwise occur.
      * ``save_order=True`` preserves word order (relevant only when
        truncation kicks in; without this flag the slug would re-sort
        words to maximize content fit).

    The function transliterates Unicode (Cyrillic, CJK, accented
    Latin, Devanagari, etc.) to ASCII so the slug column on the
    ``categories`` collection renders identically across locales and
    is safe to use in canonical URLs.

    If the input is empty, whitespace-only, or composed entirely of
    characters that ``slugify`` strips (punctuation, control codes),
    the function returns the literal string ``"category"`` to ensure
    the resulting slug is always non-empty and matches
    :data:`_SLUG_PATTERN`. Callers that want a stricter contract
    should validate the input themselves before invoking this helper.

    Args:
        name: The human-readable category name to slugify. May be
            any Unicode string.

    Returns:
        A non-empty ASCII slug suitable for use in
        :attr:`Category.slug`. Always matches :data:`_SLUG_PATTERN`
        because :mod:`slugify` produces only lowercase alphanumerics
        and single hyphens.

    Example:
        >>> _make_slug("Electronics")
        'electronics'
        >>> _make_slug("Mobile Phones")
        'mobile-phones'
        >>> _make_slug("Très Bon!")
        'tres-bon'
        >>> _make_slug("")
        'category'
        >>> _make_slug("!!!")
        'category'
    """
    base = slugify(
        name,
        max_length=MAX_SLUG_LENGTH,
        word_boundary=True,
        save_order=True,
    )
    if not base:
        # ``slugify`` returns an empty string for inputs that have no
        # alphanumeric characters after transliteration. Falling back
        # to the literal "category" preserves the field's contract
        # (non-empty, matches :data:`_SLUG_PATTERN`) and is preferable
        # to surfacing a downstream Pydantic ``min_length=1`` error
        # which would be opaque about the cause.
        base = "category"
    return base


# =============================================================================
# Category aggregate root
# =============================================================================


class Category(BaseModel):
    """The Category aggregate root.

    A frozen Pydantic v2 model representing a single node in the
    category tree of the e-commerce catalog. Persisted in the MongoDB
    ``categories`` collection inside the Product Service's private
    ``product_db`` (per AAP Section 0.4.4 — database-per-service);
    :attr:`parent_id` is a foreign-key-style reference to a peer
    document in the same ``product_db`` (NOT cross-service per AAP
    R-6 — database per service).

    Validators applied:
        * :attr:`slug` matches :data:`_SLUG_PATTERN`
          (``^[a-z0-9]+(-[a-z0-9]+)*$``) via Pydantic
          ``Field(pattern=...)``; length bounded
          ``[1, MAX_SLUG_LENGTH]``; first hyphen-separated segment
          must NOT be in :data:`_RESERVED_SLUG_PREFIXES` (enforced by
          :meth:`_validate_slug_not_reserved`).
        * :attr:`name` length bounded ``[1, MAX_NAME_LENGTH]``.
        * :attr:`description` length bounded
          ``[0, MAX_DESCRIPTION_LENGTH]``; optional (defaults to
          ``None``).
        * :attr:`display_order` bounded ``[MIN_DISPLAY_ORDER, +inf)``
          — non-negative.
        * :attr:`path` capped at :data:`MAX_PATH_DEPTH` entries; each
          entry must be a non-empty string; entries must be unique
          (no duplicates within a single path); enforced by
          :meth:`_validate_path_unique`.
        * :attr:`version` bounded ``[MIN_VERSION, +inf)``.
        * :attr:`created_at` and :attr:`updated_at` MUST be
          timezone-aware (UTC). Naive datetimes are rejected by the
          :meth:`_require_tzaware` field validator.
        * Cross-field invariants enforced by
          :meth:`_validate_path_consistency` (``mode="after"`` model
          validator):

            - ``parent_id is None  ⇔  path == []``  (root categories
              have an empty path; non-root categories have a non-
              empty path).
            - ``path[-1] == parent_id``             (when non-root,
              the last element of ``path`` is exactly ``parent_id``).
            - ``id not in path``                    (trivial cycle).
            - ``parent_id != id``                   (trivial self-
              cycle).
            - ``updated_at >= created_at``          (monotonic
              lifecycle timestamps).

    Attributes:
        id: Globally-unique aggregate identifier (UUID4, stored as
            string for JSON / MongoDB serialization compatibility).
            Immutable through :meth:`apply_changes`.
        slug: URL-safe lowercase slug
            (``^[a-z0-9]+(-[a-z0-9]+)*$``). Used in canonical URLs
            (``/categories/{slug}``); uniqueness scoped to siblings
            (per the folder-spec key insight) is enforced by a
            compound index on ``(parent_id, slug)`` at the
            repository layer, NOT by this aggregate. Auto-regenerated
            on a name change passed through :meth:`apply_changes`.
        name: Human-readable category name (e.g., ``"Electronics"``,
            ``"Mobile Phones"``). Bounded to
            :data:`MAX_NAME_LENGTH` chars.
        parent_id: Reference to the immediate parent category.
            ``None`` ⇔ this is a root category. Foreign-key-style
            reference to a peer document in the same ``product_db``
            (NOT cross-service per AAP R-6). Immutable through
            :meth:`apply_changes` — moving a category between
            parents requires the dedicated repository operation
            described in ``docs/architecture/data-stores.md`` because
            the move must rewrite ``path`` for every descendant.
        path: Materialized list of ancestor ids, root-to-immediate-
            parent, EXCLUSIVE of self. Empty for root categories;
            ``path[-1] == parent_id`` for non-root categories. Capped
            at :data:`MAX_PATH_DEPTH` entries. Immutable through
            :meth:`apply_changes` (see :attr:`parent_id`).
        description: Optional long-form description for SEO /
            landing-page rendering. Bounded to
            :data:`MAX_DESCRIPTION_LENGTH` chars.
        display_order: 0-indexed sort order within the same parent.
            Lower values appear first. Defaults to ``0``.
        is_visible: Whether the category is visible on the
            storefront. Defaults to ``True``. The authoritative
            visibility field; the :class:`CategoryStatus` enum is
            advisory.
        version: Optimistic-concurrency token. Starts at ``1`` for
            new documents; incremented by the repository on every
            update via ``findOneAndUpdate`` filter ``{_id, version:
            N}`` + ``$inc: {version: 1}``. :meth:`apply_changes`
            does NOT increment this field.
        created_at: UTC timestamp when the document was created;
            immutable through :meth:`apply_changes`.
        updated_at: UTC timestamp of the most recent update; bumped
            by :meth:`apply_changes` to ``datetime.now(UTC)``.

    Example:
        >>> from src.domain.category import Category
        >>> root = Category.new(name="Electronics")
        >>> root.is_root()
        True
        >>> root.path
        []
        >>> root.slug
        'electronics'
        >>> root.version
        1

        >>> child = Category.new(
        ...     name="Mobile Phones",
        ...     parent_id=root.id,
        ...     path=[root.id],
        ... )
        >>> child.is_root()
        False
        >>> child.path == [root.id]
        True

        Renaming auto-regenerates the slug:

        >>> renamed = root.apply_changes({"name": "Consumer Electronics"})
        >>> renamed.slug
        'consumer-electronics'
        >>> renamed.id == root.id
        True
        >>> renamed.version == root.version  # repo's job to bump
        True
    """

    # ------------------------------------------------------------------
    # Model configuration (frozen / strict)
    # ------------------------------------------------------------------
    #
    # ``frozen=True``                — immutability. Prevents accidental
    #                                  mutation in repositories or
    #                                  handlers. Direct field assignment
    #                                  after construction raises
    #                                  ``pydantic.ValidationError``.
    # ``str_strip_whitespace=True``  — strip surrounding whitespace
    #                                  from any string-typed field
    #                                  (``id``, ``slug``, ``name``,
    #                                  ``parent_id``, ``description``,
    #                                  and each ``path`` entry).
    #                                  Defends against copy-paste
    #                                  artifacts in API request bodies
    #                                  and stored documents.
    # ``extra="forbid"``             — reject unknown fields; protects
    #                                  against typos in API request
    #                                  bodies and Kafka event payloads
    #                                  decoded into this model.
    # ``populate_by_name=True``      — allow construction by either
    #                                  field name or alias; future-
    #                                  proofs schema evolution.
    # ``validate_assignment=True``   — even though ``frozen=True``
    #                                  blocks direct assignment, this
    #                                  flag ensures any framework path
    #                                  that bypasses ``__setattr__``
    #                                  (e.g., custom deserialization)
    #                                  is still validated.
    # ``arbitrary_types_allowed=False`` — refuse non-Pydantic-aware
    #                                     types; forces explicit
    #                                     handling of every field.
    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
        arbitrary_types_allowed=False,
    )

    # ------------------------------------------------------------------
    # Fields
    # ------------------------------------------------------------------

    id: Annotated[
        str,
        Field(
            min_length=1,
            description=(
                "Globally-unique aggregate identifier (UUID4, stored "
                "as string). Generated by :meth:`Category.new`. "
                "Immutable through :meth:`apply_changes`."
            ),
        ),
    ]

    slug: Annotated[
        str,
        Field(
            min_length=1,
            max_length=MAX_SLUG_LENGTH,
            pattern=_SLUG_PATTERN.pattern,
            description=(
                "URL-safe lowercase slug (alphanumeric + single "
                "hyphens). Used in canonical URLs (`/categories/"
                "{slug}`). Uniqueness is scoped to siblings — "
                "enforced by a compound index on `(parent_id, slug)` "
                "at the repository layer, NOT by this aggregate. "
                "Auto-regenerated on a name change passed through "
                ":meth:`apply_changes`."
            ),
        ),
    ]

    name: Annotated[
        str,
        Field(
            min_length=1,
            max_length=MAX_NAME_LENGTH,
            description=(
                "Human-readable category name (e.g., 'Electronics', "
                "'Mobile Phones'). Multi-language support is "
                "provided out-of-band (typically via a sibling "
                "`localizations` collection)."
            ),
        ),
    ]

    parent_id: str | None = Field(
        default=None,
        description=(
            "Reference to the immediate parent category. `None` ⇔ "
            "this is a root category. Foreign-key-style reference to "
            "a peer document in the same `product_db` (NOT cross-"
            "service per AAP R-6). Immutable through "
            ":meth:`apply_changes` — moving a category between "
            "parents requires a dedicated repository operation that "
            "rewrites `path` for the entire subtree."
        ),
    )

    path: Annotated[
        list[str],
        Field(
            default_factory=list,
            max_length=MAX_PATH_DEPTH,
            description=(
                "Materialized list of ancestor ids, root-to-"
                "immediate-parent, EXCLUSIVE of self. Empty for root "
                "categories; `path[-1] == parent_id` for non-root "
                "categories. Capped at MAX_PATH_DEPTH entries. "
                "Immutable through :meth:`apply_changes` (see "
                "`parent_id`)."
            ),
        ),
    ]

    description: Annotated[
        str | None,
        Field(
            default=None,
            min_length=0,
            max_length=MAX_DESCRIPTION_LENGTH,
            description=(
                "Optional long-form category description for SEO / "
                f"landing pages. Bounded to {MAX_DESCRIPTION_LENGTH} "
                "chars."
            ),
        ),
    ] = None

    display_order: Annotated[
        int,
        Field(
            ge=MIN_DISPLAY_ORDER,
            description=(
                "0-indexed sort order within the same parent. "
                "Lower values appear first. Defaults to 0."
            ),
        ),
    ] = 0

    is_visible: bool = Field(
        default=True,
        description=(
            "Whether the category is visible on the storefront. "
            "The AUTHORITATIVE visibility field; the "
            ":class:`CategoryStatus` enum is advisory only."
        ),
    )

    version: Annotated[
        int,
        Field(
            ge=MIN_VERSION,
            description=(
                "Optimistic-concurrency token. Starts at 1 for new "
                "documents; incremented by the repository on every "
                "update via `findOneAndUpdate` filter `{_id, "
                "version: N}` + `$inc: {version: 1}`. "
                ":meth:`apply_changes` does NOT increment this "
                "field — that is the repository's job."
            ),
        ),
    ]

    created_at: Annotated[
        datetime,
        Field(
            description=(
                "UTC timestamp when the document was created. MUST be "
                "timezone-aware (naive datetimes are rejected). "
                "Immutable through :meth:`apply_changes`."
            ),
        ),
    ]

    updated_at: Annotated[
        datetime,
        Field(
            description=(
                "UTC timestamp of the most recent update. MUST be "
                "timezone-aware (naive datetimes are rejected). "
                "Bumped by :meth:`apply_changes` to "
                "`datetime.now(UTC)`. Cross-field invariant: "
                "`updated_at >= created_at`."
            ),
        ),
    ]


    # ------------------------------------------------------------------
    # Field validators
    # ------------------------------------------------------------------

    @field_validator("created_at", "updated_at")
    @classmethod
    def _require_tzaware(cls, v: datetime) -> datetime:
        """Reject naive datetimes for ``created_at`` / ``updated_at``.

        AAP R-26 mandates that all timestamp fields are RFC 3339-
        encoded UTC; a naive :class:`datetime.datetime` (one with
        ``tzinfo is None`` or whose ``utcoffset()`` returns ``None``)
        cannot be unambiguously converted to UTC and is therefore
        rejected at construction time.

        :meth:`Category.new` always uses
        :func:`datetime.now(timezone.utc)` so it satisfies this rule
        by construction. Callers reconstructing a :class:`Category`
        from a database document or an API request body MUST ensure
        the timestamps carry a timezone (the MongoDB BSON date type
        and the FastAPI Pydantic decoder both produce timezone-aware
        datetimes by default).

        Args:
            v: The candidate :class:`datetime` value.

        Returns:
            The same value ``v`` if it is timezone-aware.

        Raises:
            ValueError: When ``v`` is naive (``tzinfo is None`` or
                ``utcoffset()`` returns ``None``). Pydantic wraps
                this in a :class:`pydantic.ValidationError` at the
                model boundary.
        """
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise ValueError("datetime fields must be timezone-aware (UTC)")
        return v

    @field_validator("slug")
    @classmethod
    def _validate_slug_not_reserved(cls, v: str) -> str:
        """Reject slugs whose first segment is in :data:`_RESERVED_SLUG_PREFIXES`.

        The first hyphen-separated segment of the slug is compared
        against the reserved-prefix set
        (:data:`_RESERVED_SLUG_PREFIXES`). A category slug whose
        first segment matches one of these values would generate a
        URL that conflicts with an operational path family exposed by
        the API Gateway:

          * ``admin``    — admin console / management API.
          * ``api``      — versioned API path prefix.
          * ``health``   — Kubernetes liveness / readiness probes.
          * ``metrics``  — Prometheus metrics scrape endpoint.

        Rejecting these prefixes at the domain layer guarantees the
        conflict cannot propagate to persistence, regardless of how
        the slug was produced (auto-generated by :func:`_make_slug`
        or supplied directly by an admin client).

        The Pydantic ``Field(pattern=_SLUG_PATTERN.pattern)``
        constraint has already validated that the value matches the
        URL-safe character class by the time this validator runs,
        and ``str_strip_whitespace=True`` has stripped surrounding
        whitespace — this validator therefore sees a clean, lowercase
        ASCII string.

        Args:
            v: The candidate slug string.

        Returns:
            The same value ``v`` if its first segment is NOT in
            :data:`_RESERVED_SLUG_PREFIXES`.

        Raises:
            ValueError: When the first segment matches a reserved
                prefix. Pydantic wraps this in a
                :class:`pydantic.ValidationError` at the model
                boundary.
        """
        # ``split("-", 1)`` returns at most a 2-element list; the
        # first element is the candidate first segment. Even for a
        # slug with no hyphen, this returns ``[v]`` and the index
        # ``[0]`` access is safe.
        first_segment = v.split("-", 1)[0]
        if first_segment in _RESERVED_SLUG_PREFIXES:
            raise ValueError(
                f"slug must not begin with a reserved prefix "
                f"({sorted(_RESERVED_SLUG_PREFIXES)}); got {v!r}"
            )
        return v

    @field_validator("path")
    @classmethod
    def _validate_path_unique(cls, v: list[str]) -> list[str]:
        """Validate per-element invariants on the ``path`` array.

        Enforces two simple rules on the list itself, without yet
        cross-referencing :attr:`id` or :attr:`parent_id` (those
        invariants are checked in
        :meth:`_validate_path_consistency`):

          1. **No duplicate ancestor ids.** A duplicate within a
             single ``path`` would imply a malformed ancestor chain
             (the same node appearing twice in the path from root to
             immediate-parent is impossible in a tree). Detected via
             ``len(v) != len(set(v))``.

          2. **No empty entries.** Each ancestor id must be a non-
             empty, non-whitespace string. Pydantic's
             ``str_strip_whitespace=True`` operates only on the top-
             level ``str`` field type; it does NOT recurse into the
             elements of a ``list[str]`` field, so this explicit
             check is required to reject e.g. ``[""]`` or
             ``["   "]``.

        Args:
            v: The candidate list of ancestor ids.

        Returns:
            The same list ``v`` if both invariants hold.

        Raises:
            ValueError: When ``v`` contains duplicates or empty
                entries. Pydantic wraps this in a
                :class:`pydantic.ValidationError`.
        """
        # Duplicate detection: a tree path from root to immediate-
        # parent visits each node at most once, so duplicates are
        # always malformed.
        if len(v) != len(set(v)):
            raise ValueError("path must contain unique ancestor ids")

        # Per-entry non-emptiness check. ``str.strip()`` is used to
        # also reject whitespace-only strings (e.g., "   ").
        for entry in v:
            if not entry or not entry.strip():
                raise ValueError("path must not contain empty entries")

        return v

    # ------------------------------------------------------------------
    # Model validators (cross-field invariants)
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def _validate_path_consistency(self) -> Category:
        """Enforce the cross-field invariants on parent_id ↔ path.

        The :attr:`path` field is a materialized list of ancestor
        ids that runs root-to-immediate-parent and is EXCLUSIVE of
        self. This validator ensures the four invariants that follow
        from that contract, plus the timestamp monotonicity check.

        Invariants enforced:

          1. **Trivial cycle (id-in-path):** ``self.id`` must NOT
             appear anywhere in ``self.path``. A non-trivial cycle
             (e.g., grandchild's ``parent_id`` pointing back to
             grandparent) cannot be detected at this layer because
             it requires reading the parent's path from the database;
             that check lives in the repository layer. The trivial
             check guarantees that AT LEAST self-in-own-ancestry is
             rejected at construction time.

          2. **Root has empty path:** ``parent_id is None  ⇒  path
             == []``. A root category by definition has no ancestors,
             so the materialized path must be empty.

          3. **Non-root has non-empty path:** ``parent_id is not
             None  ⇒  path != []``. A non-root category has at least
             its immediate parent in the path.

          4. **path[-1] == parent_id:** the last element of ``path``
             is exactly ``parent_id`` because ``path`` runs root-to-
             immediate-parent, exclusive of self. The immediate
             parent is therefore the LAST entry.

          5. **Trivial self-cycle:** ``parent_id != id`` (a category
             cannot be its own parent).

          6. **Monotonic timestamps:** ``updated_at >= created_at``.
             A document whose most-recent-update timestamp predates
             its creation timestamp is corrupted and is rejected at
             construction time.

        Returns:
            ``self`` if all invariants hold. Pydantic
            :func:`model_validator` with ``mode="after"`` requires the
            validator to return the model instance.

        Raises:
            ValueError: When any of the six invariants is violated.
                Pydantic wraps this in a
                :class:`pydantic.ValidationError`.
        """
        # Invariant 1: id must not appear in its own ancestry.
        if self.id in self.path:
            raise ValueError(
                "category id must not appear in its own path (cycle detected)"
            )

        # Invariant 2: root categories (parent_id is None) must have
        # an empty path.
        if self.parent_id is None and self.path:
            raise ValueError(
                "root categories (parent_id is None) must have an empty path"
            )

        # Invariants 3 + 4: non-root categories (parent_id is set)
        # must have a non-empty path whose last element is exactly
        # parent_id.
        if self.parent_id is not None:
            if not self.path:
                raise ValueError("non-root category must have a non-empty path")
            if self.path[-1] != self.parent_id:
                raise ValueError(
                    "path[-1] must equal parent_id "
                    "(path is root-to-immediate-parent, exclusive of self)"
                )

        # Invariant 5: trivial self-cycle.
        if self.parent_id == self.id:
            raise ValueError("parent_id must not equal id (self-cycle)")

        # Invariant 6: monotonic lifecycle timestamps.
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must not be before created_at")

        return self

    # ------------------------------------------------------------------
    # Class methods (factories) and instance methods
    # ------------------------------------------------------------------

    @classmethod
    def new(
        cls,
        *,
        name: str,
        parent_id: str | None = None,
        path: list[str] | None = None,
        description: str | None = None,
        display_order: int = 0,
        is_visible: bool = True,
    ) -> Category:
        """Create a brand-new :class:`Category` aggregate.

        Generates a fresh UUID4 :attr:`id`, slugifies :attr:`name`
        via :func:`_make_slug`, sets :attr:`version` to ``1``, and
        stamps :attr:`created_at` / :attr:`updated_at` to
        ``datetime.now(timezone.utc)``. All other fields are taken
        from the keyword arguments.

        The caller (typically the repository layer) is responsible
        for passing the correct ``path`` derived from the parent's
        path:

          * **Root categories**: pass ``parent_id=None`` and either
            omit ``path`` or pass ``path=[]``. The factory will
            normalize ``None`` to an empty list.
          * **Non-root categories**: pass the parent's ``id`` as
            ``parent_id`` AND pass the parent's path with
            ``parent_id`` appended as ``path``. For a depth-1 child
            of a root parent, ``path = [root_parent.id]``. For a
            depth-2 grandchild, ``path = [root_grandparent.id,
            parent.id]``.

        This factory does NOT perform any I/O or lookup — it does
        not read the parent's ``path`` from the database. That is
        the repository's responsibility. The aggregate's invariant
        checks (in :meth:`_validate_path_consistency`) catch trivial
        violations (root with non-empty path, non-root with empty
        path, ``path[-1] != parent_id``, etc.) so a malformed call
        site fails fast with a clear
        :class:`pydantic.ValidationError`.

        Keyword arguments are required (the leading ``*`` enforces
        keyword-only invocation) so callers cannot accidentally swap
        positional arguments.

        Args:
            name: Human-readable category name. Will be slugified
                via :func:`_make_slug` to produce :attr:`slug`.
            parent_id: Optional reference to the immediate parent
                category. ``None`` for root categories.
            path: Optional materialized path (root-to-immediate-
                parent, exclusive of self). Defaults to an empty
                list. For a non-root category, the LAST element MUST
                equal ``parent_id``.
            description: Optional long-form description.
            display_order: 0-indexed sort key within the same
                parent. Defaults to ``0``.
            is_visible: Whether the category is visible on the
                storefront. Defaults to ``True``.

        Returns:
            A fully-validated :class:`Category` instance with
            :attr:`id` populated, :attr:`version=1`, and
            :attr:`created_at` / :attr:`updated_at` set to the
            current UTC time.

        Raises:
            pydantic.ValidationError: When any field-level
                constraint or cross-field invariant fails (e.g.,
                slug derived from ``name`` begins with a reserved
                prefix, ``parent_id`` is set but ``path`` is empty,
                ``path[-1] != parent_id``).

        Example:
            Creating a root category:

            >>> root = Category.new(name="Electronics")
            >>> root.is_root()
            True
            >>> root.path
            []
            >>> root.slug
            'electronics'
            >>> root.version
            1

            Creating a depth-1 child:

            >>> child = Category.new(
            ...     name="Mobile Phones",
            ...     parent_id=root.id,
            ...     path=[root.id],
            ... )
            >>> child.is_root()
            False
            >>> child.parent_id == root.id
            True
            >>> child.path == [root.id]
            True
        """
        now = datetime.now(timezone.utc)
        return cls(
            id=str(uuid.uuid4()),
            slug=_make_slug(name),
            name=name,
            parent_id=parent_id,
            # ``list(path or [])`` defensively copies the caller's
            # list (so the aggregate never aliases a mutable list
            # the caller still holds a reference to) and normalizes
            # ``None`` to an empty list. Even though the model is
            # frozen and Pydantic v2 deep-copies the value into the
            # model, this defensive copy at construction time
            # prevents accidental mutation of the caller's input.
            path=list(path or []),
            description=description,
            display_order=display_order,
            is_visible=is_visible,
            version=1,
            created_at=now,
            updated_at=now,
        )

    def is_root(self) -> bool:
        """Return True iff this category has no parent (it is a tree root).

        A category is a tree root when its :attr:`parent_id` is
        ``None``. Equivalently (and enforced by
        :meth:`_validate_path_consistency`), a root category has an
        empty :attr:`path`. Either condition can be checked; this
        method uses the more direct ``parent_id is None`` check.

        Returns:
            ``True`` if :attr:`parent_id` is ``None``; ``False``
            otherwise.

        Example:
            >>> root = Category.new(name="Electronics")
            >>> root.is_root()
            True

            >>> child = Category.new(
            ...     name="Mobile Phones",
            ...     parent_id=root.id,
            ...     path=[root.id],
            ... )
            >>> child.is_root()
            False
        """
        return self.parent_id is None

    def apply_changes(
        self,
        changes: dict[str, Any],
        *,
        now: datetime | None = None,
    ) -> Category:
        """Return a new :class:`Category` with the given fields updated.

        Same semantics as the other aggregates in the Product Service
        (:class:`src.domain.product.Product`,
        :class:`src.domain.product_media.ProductMedia`):

          * :attr:`updated_at` is bumped to ``now`` (or
            ``datetime.now(timezone.utc)`` if ``now`` is ``None``).
          * :attr:`version` is NOT incremented — that is the
            repository's job (via ``findOneAndUpdate`` filter +
            ``$inc``).
          * :attr:`id`, :attr:`parent_id`, :attr:`path`,
            :attr:`version`, and :attr:`created_at` are immutable.
            Attempting to update any of them raises
            :class:`ValueError` BEFORE
            :meth:`pydantic.BaseModel.model_copy` runs, which keeps
            the error message clear and actionable.
          * If ``changes`` includes ``"name"`` but NOT ``"slug"``,
            the slug is auto-regenerated from the new name via
            :func:`_make_slug`. Callers that want to keep the slug
            stable across a rename MUST pass an explicit ``"slug"``
            entry in ``changes``.
          * The result is a fresh :class:`Category` instance produced
            via :meth:`pydantic.BaseModel.model_copy`. Per the
            documented Pydantic v2 semantics, ``model_copy`` does NOT
            re-run field and model validators on the merged field
            set — it merely constructs a copy with the patched
            attributes. The repository / command-handler layer is
            responsible for validating ``changes`` (typically by
            constructing a fresh :class:`Category` and asserting it
            succeeds) BEFORE invoking this method. This deliberate
            split keeps the aggregate's mutation API cheap (a single
            dict merge plus the optional slug regeneration) while
            leaving rich validation in the command-handler layer
            where domain-specific rules live.

        Why ``parent_id`` and ``path`` are immutable here:
          Moving a category between parents would invalidate the
          materialized ``path`` of every descendant. The repository
          layer exposes a dedicated ``move_subtree`` operation that
          rewrites ``path`` for every descendant in a single
          transactional pass; this aggregate-level API deliberately
          does NOT expose that operation because it is an O(N)
          batch write, not a pointwise mutation.

        Args:
            changes: Mapping of field name to new value. Only mutable
                fields may appear (``slug``, ``name``, ``description``,
                ``display_order``, ``is_visible``, ``updated_at``).
                Passing any of the immutable field names (``id``,
                ``parent_id``, ``path``, ``version``, ``created_at``)
                raises :class:`ValueError`.
            now: Optional override for the new :attr:`updated_at`
                value. If ``None`` (the default), the current UTC
                time is used. The override is primarily for testing
                determinism.

        Returns:
            A fresh :class:`Category` instance with the given
            fields applied and :attr:`updated_at` bumped. The
            original instance is unchanged (Pydantic v2
            ``model_copy`` does not mutate ``self``).

        Raises:
            ValueError: When ``changes`` contains any of the
                immutable field names (``id``, ``parent_id``,
                ``path``, ``version``, ``created_at``). The
                exception is raised eagerly with a clear message
                identifying the offending field.

        Example:
            Renaming auto-regenerates the slug:

            >>> c = Category.new(name="Old Name")
            >>> c.slug
            'old-name'
            >>> c2 = c.apply_changes({"name": "New Name"})
            >>> c2.slug
            'new-name'
            >>> c2.id == c.id  # id is preserved
            True
            >>> c2.version == c.version  # repo bumps version
            True

            Renaming with an explicit slug overrides auto-regeneration:

            >>> c3 = c.apply_changes({"name": "New Name",
            ...                       "slug": "kept-stable"})
            >>> c3.slug
            'kept-stable'

            Attempting to mutate an immutable field raises:

            >>> c.apply_changes({"parent_id": "y"})
            Traceback (most recent call last):
                ...
            ValueError: field 'parent_id' is immutable and cannot be ...
        """
        if now is None:
            now = datetime.now(timezone.utc)

        # Eager rejection of immutable-field mutation. Done before
        # ``model_copy`` so the error message points unambiguously
        # to the offending field rather than surfacing as a generic
        # downstream validation error.
        for forbidden in ("id", "parent_id", "path", "version", "created_at"):
            if forbidden in changes:
                raise ValueError(
                    f"field {forbidden!r} is immutable and cannot be updated "
                    "via apply_changes (use a dedicated repository operation "
                    "if needed)"
                )

        # Auto-regenerate slug on a name change unless the caller
        # supplied an explicit slug. Performed by constructing a
        # fresh dict (rather than mutating the caller's input) so
        # the original ``changes`` mapping is left untouched.
        if "name" in changes and "slug" not in changes:
            changes = {**changes, "slug": _make_slug(changes["name"])}

        # ``model_copy`` produces a NEW instance with the merged
        # field set. The merge order ({**changes, "updated_at":
        # now}) ensures that a caller-supplied "updated_at" entry
        # in ``changes`` is OVERRIDDEN by the bumped value, which
        # preserves the lifecycle invariant. Callers that want to
        # back-date the update for testing should use the ``now``
        # parameter instead of injecting "updated_at" into
        # ``changes``.
        return self.model_copy(update={**changes, "updated_at": now})

    def to_event_envelope(self) -> dict[str, Any]:
        """Produce a self-contained event envelope (per AAP R-33).

        Categories do not currently emit dedicated Kafka events, but
        this method exists for symmetry with
        :class:`src.domain.product.Product` and
        :class:`src.domain.product_media.ProductMedia` and for future
        use (e.g., a hypothetical ``category.created`` /
        ``category.updated`` event the events package may introduce
        later).

        Per AAP R-33 events must be self-contained — consumers
        should not need to call back to this service to interpret a
        category reference embedded in a product event. This method
        therefore returns a fully-resolved payload containing every
        field a consumer needs.

        Field encoding choices:
          * ``path`` is a defensive copy of the internal list (via
            ``list(self.path)``) so a downstream consumer mutating
            the returned envelope cannot accidentally mutate the
            aggregate's own ``path``. (The aggregate is frozen, so
            this is mostly a defense-in-depth measure for
            unexpected code paths that subclass ``BaseModel`` and
            relax the freeze; emitting a copy is cheap and
            unambiguously safe.)
          * ``created_at`` and ``updated_at`` are emitted as RFC 3339
            ISO 8601 strings via :meth:`datetime.isoformat`.
            Producing a string keeps the envelope JSON-serializable
            without a custom encoder; the timezone offset is
            preserved (e.g.,
            ``"2024-01-15T12:34:56.789012+00:00"``).
          * Optional fields (``description``) are included as
            ``None`` rather than omitted so consumers can reliably
            check ``if envelope.get("description"): ...`` without
            distinguishing "absent" from "explicit null".

        Returns:
            A dict with eleven keys (``id``, ``slug``, ``name``,
            ``parent_id``, ``path``, ``description``,
            ``display_order``, ``is_visible``, ``version``,
            ``created_at``, ``updated_at``) suitable for inclusion
            in a future ``category.*`` Kafka event payload (Schema
            Registry-validated upstream by the producer pipeline
            per AAP R-14).

        Example:
            >>> root = Category.new(name="Electronics")
            >>> env = root.to_event_envelope()
            >>> env["slug"]
            'electronics'
            >>> env["parent_id"] is None
            True
            >>> env["path"]
            []
            >>> isinstance(env["created_at"], str)
            True
            >>> env["version"]
            1
        """
        return {
            "id": self.id,
            "slug": self.slug,
            "name": self.name,
            "parent_id": self.parent_id,
            # Defensive copy so a downstream mutation of the
            # returned envelope never reaches back into the
            # aggregate's internal state.
            "path": list(self.path),
            "description": self.description,
            "display_order": self.display_order,
            "is_visible": self.is_visible,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


# =============================================================================
# Public surface
# =============================================================================
#
# Sorted alphabetically (the standard convention for ``__all__``) so
# diff hygiene is preserved on future additions. The list mirrors the
# ``exports`` schema declared for this module.

__all__ = [
    "Category",
    "CategoryStatus",
    "MAX_DESCRIPTION_LENGTH",
    "MAX_NAME_LENGTH",
    "MAX_PATH_DEPTH",
    "MAX_SLUG_LENGTH",
    "MIN_DISPLAY_ORDER",
    "MIN_VERSION",
]

