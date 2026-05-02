"""ProductMedia aggregate root for the Product Service domain layer.

A :class:`ProductMedia` document records metadata for a CDN-hosted media
asset (image, video, or thumbnail) attached to a :class:`Product`. The
asset bytes themselves live on the CDN; this aggregate only stores the
URL and descriptive metadata. Replacing the bytes is done out-of-band;
the :attr:`ProductMedia.url` field is updatable to support CDN URL
rotation.

Persisted in the MongoDB ``product_media`` collection inside the Product
Service's private ``product_db`` (per AAP Section 0.4.4 — database-per-
service). Lookups by :attr:`ProductMedia.product_id` are served by an
index on ``product_media.product_id`` declared in the migration scripts;
this module is purely the in-memory aggregate definition and contains no
persistence code.

Pure domain layer — no MongoDB / Kafka / HTTP imports. The aggregate
stands alone with no peer-domain imports, which keeps the import graph
acyclic and makes this module cheap to import in unit tests.

Design principles
-----------------
1. **Aggregate root, not value object.** Unlike :class:`Variant` (which
   is embedded inside :class:`Product` and has no identity of its own),
   a :class:`ProductMedia` document has a stable :attr:`id`, an
   :attr:`updated_at` lifecycle, and a private collection. It is fully
   self-contained. ``product_id`` is a foreign-key-style reference to a
   peer document in the same ``product_db`` (NOT cross-service per AAP
   R-6 — database per service).

2. **Frozen / immutable.** ``model_config.frozen=True`` prevents
   accidental mutation in repositories or handlers. To "modify" a
   :class:`ProductMedia`, callers MUST use :meth:`apply_changes`, which
   returns a fresh instance via :meth:`pydantic.BaseModel.model_copy`.

3. **Strict typing & strict configuration.** ``extra="forbid"`` rejects
   unknown fields (defends against typos in API request bodies and
   Kafka event payloads decoded into this model). String fields are
   automatically stripped via ``str_strip_whitespace=True``.

4. **Optimistic-concurrency version field.** :attr:`version` mirrors
   the convention used by :class:`Product` and :class:`Category`: the
   field starts at ``1`` for new documents; the repository increments
   it via ``findOneAndUpdate`` filter + ``$inc`` on every update;
   :meth:`apply_changes` does NOT increment it (that is the
   repository's job).

5. **No I/O, no logging, no module-level side effects.** Only the
   module-level constants, precompiled regex patterns, the
   :class:`MediaKind` enum, and the :class:`ProductMedia` class are
   evaluated at import time. Keeps unit-test startup cheap and
   prevents the module from accidentally pulling in framework-specific
   code paths.

Field summary
-------------
* ``id: str``                   — globally-unique aggregate identifier
                                  (UUID4, stored as string for
                                  JSON/MongoDB serialization).
* ``product_id: str``           — foreign-key-style reference to a
                                  :class:`Product` aggregate; immutable
                                  through :meth:`apply_changes` (moving
                                  a media asset to a different product
                                  would invalidate referential
                                  integrity).
* ``kind: MediaKind``           — classification of the asset
                                  (``image`` / ``video`` /
                                  ``thumbnail``).
* ``url: str``                  — HTTP(S) URL of the CDN-hosted asset;
                                  bytes themselves are NOT stored here.
* ``alt_text: str | None``      — accessibility / SEO text; optional
                                  (some asset kinds may not require
                                  it).
* ``position: int``             — display order within the product's
                                  media list (lower values appear
                                  first).
* ``width: int | None``         — pixel width (image / video); optional
                                  (may be populated asynchronously by
                                  an inspection job).
* ``height: int | None``        — pixel height (image / video);
                                  optional (same as ``width``).
* ``mime_type: str``            — RFC 6838 media type (e.g.,
                                  ``image/png``, ``video/mp4``,
                                  ``image/svg+xml``).
* ``version: int``              — optimistic-concurrency token; starts
                                  at ``1``; incremented by the
                                  repository on every update.
* ``created_at: datetime``      — UTC creation timestamp; immutable.
* ``updated_at: datetime``      — UTC most-recent-update timestamp;
                                  bumped by :meth:`apply_changes`.

Why ``url`` and ``mime_type`` are required but ``alt_text`` /
``width`` / ``height`` are optional?
* ``url`` and ``mime_type`` together identify the asset: the URL points
  at the bytes, the MIME type tells the renderer how to display them.
  Without both, a downstream consumer cannot present the asset.
* ``alt_text`` is conditionally required by the client (mandatory for
  images on the storefront UI for WCAG 2.1 / SEO, optional for
  thumbnails) — that conditional rule is enforced in the controller /
  command-handler layer, not in this aggregate, because the rule
  varies by API surface.
* ``width`` / ``height`` may be populated asynchronously by an
  inspection job that runs after the upload completes; they are
  optional at create-time and patched-in once the inspection job has
  run.

Why ``MediaKind`` values are lowercase strings?
* The values double as the user-facing API representation
  (``"image"``, ``"video"``, ``"thumbnail"``) and as the MongoDB stored
  enum value. Lowercase strings serialize transparently to both. This
  deviates from the conventional UPPERCASE Python enum convention used
  by :class:`OrderStatus` / :class:`PaymentStatus` in the Order /
  Payment Services because those enums are NOT user-facing — they are
  internal state machine tokens.

Why URL validation is shallow (scheme-only)?
* Deeper URL validation (domain allow-list, CDN-prefix matching,
  signed-URL-expiry parsing) belongs in middleware / repository where
  configuration settings are accessible. This aggregate enforces the
  bare minimum: the URL must use the ``http`` or ``https`` scheme.
  Plaintext ``http://`` is permitted at this layer to support local
  development against MinIO / fake CDNs; production deployments
  enforce HTTPS at the gateway / WAF level (AAP R-24).

Authoritative references
------------------------
* AAP Section 0.4.4 — ``product_db`` MongoDB ``product_media``
  collection ownership.
* AAP Section 0.5.2.2 bullet 4 — Product Service implementation plan.
* AAP R-6 — Database per service: ``product_id`` is a peer-document
  reference within ``product_db``, NOT a cross-service foreign key.
* AAP R-7 — MongoDB chosen for flexible schema: media metadata varies
  by ``kind`` (image dimensions, video duration, etc.) and the
  document model accommodates this without schema migrations.
* AAP R-26 — Structured JSON-friendly fields (every primitive is
  trivially JSON-serializable).
* AAP R-30 / R-32 / R-33 — Event semantics: media-update events are
  emitted as nested fields of ``product.created`` / ``product.updated``
  rather than dedicated topics (per AAP R-32 producers must not know
  consumers; per AAP R-33 events must be self-contained — hence
  :meth:`to_event_envelope` returning a fully-resolved payload).
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Annotated, Any, Final

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# =============================================================================
# Module-level constants
# =============================================================================
#
# All length / range ceilings exposed on this module are typed
# :class:`typing.Final` so static analyzers (mypy) prevent accidental
# reassignment elsewhere in the codebase. Tests can import these symbols
# directly to verify boundary semantics without spinning up the
# configuration layer.

#: Maximum length of the ``url`` field (in characters).
#:
#: 2048 chars matches the conventional URL-length cap supported by
#: common CDNs, browsers, and HTTP intermediaries (Apache HTTPD, NGINX,
#: AWS CloudFront, Akamai). Longer URLs are pathological and frequently
#: rejected by upstream proxies; capping the field at this layer
#: prevents unwieldy values from reaching the persistence layer.
MAX_URL_LENGTH: Final[int] = 2048

#: Maximum length of the ``alt_text`` field (in characters).
#:
#: 500 chars is generous enough to accommodate descriptive accessibility
#: text (WCAG 2.1 — alt text should describe the image's content and
#: function) yet bounded to prevent abuse. Real-world alt text rarely
#: exceeds 125 chars.
MAX_ALT_TEXT_LENGTH: Final[int] = 500

#: Maximum length of the ``mime_type`` field (in characters).
#:
#: 128 chars accommodates RFC 6838 media types including vendor-
#: prefixed and parameter-laden variants (e.g.,
#: ``application/vnd.api+json``, ``image/svg+xml``,
#: ``application/vnd.openxmlformats-officedocument.wordprocessingml.document``)
#: with comfortable headroom. The actual median MIME-type length is
#: under 20 chars.
MAX_MIME_TYPE_LENGTH: Final[int] = 128

#: Minimum value for the ``version`` field.
#:
#: Aggregates start their lifecycle at version 1 (the
#: :meth:`ProductMedia.new` factory hard-codes this). The repository
#: increments via ``findOneAndUpdate`` filter ``{_id, version: N}`` +
#: ``$inc: {version: 1}`` on every update, so ``version`` is strictly
#: monotonically increasing. ``0`` and negative values would indicate
#: a corrupted document and are rejected at construction time.
MIN_VERSION: Final[int] = 1

#: Minimum value for the ``position`` field.
#:
#: ``position`` is a 0-indexed display order within the parent product's
#: media list — the first media item has ``position=0``. Negative
#: values are nonsensical and are rejected at construction time. The
#: agent prompt's :class:`ProductMedia.new` factory defaults to ``0``
#: which always satisfies this lower bound.
MIN_POSITION: Final[int] = 0

#: Minimum value for the ``width`` / ``height`` dimension fields
#: (in pixels).
#:
#: A 0-or-negative dimension is meaningless for a real media asset.
#: ``1`` is the absolute minimum: a 1x1 pixel image is degenerate but
#: technically valid (e.g., transparent tracking pixels). Whether the
#: value is actually present is governed separately by the field
#: optionality — ``width`` / ``height`` may be ``None`` at create time
#: and patched in once the asynchronous inspection job determines
#: them.
MIN_DIMENSION: Final[int] = 1

#: Maximum value for the ``width`` / ``height`` dimension fields
#: (in pixels).
#:
#: 100,000 pixels is a defensive bound against pathological clients.
#: Real-world images rarely exceed 16,000 pixels on either axis (8K
#: video is 7680x4320). The cap admits arbitrarily-detailed digital
#: photography while rejecting accidental misconfiguration (e.g.,
#: a client passing ``2**31 - 1`` would otherwise blow up downstream
#: rendering / sizing logic). Symmetric on both axes.
MAX_DIMENSION: Final[int] = 100_000


# =============================================================================
# Precompiled regex patterns (private)
# =============================================================================
#
# Compiled once at module load so that field-level validation does not
# pay the regex compilation cost on every model construction.
# :data:`_MIME_TYPE_PATTERN` drives the :class:`pydantic.Field`
# ``pattern=...`` constraint on :attr:`ProductMedia.mime_type` (which
# requires the source string, accessed via the ``.pattern`` attribute);
# :data:`_URL_PATTERN` is referenced by the
# :meth:`ProductMedia._validate_url_scheme` field validator (which
# requires the compiled :class:`re.Pattern` object for the
# :meth:`re.Pattern.match` call).

#: MIME-type pattern: simple ``type/subtype`` matching the bulk of RFC
#: 6838 grammar — supports ``image/png``, ``video/mp4``,
#: ``application/json``, plus the ``+`` and ``.`` and ``-`` characters
#: in the subtype that show up in vendor-prefixed types
#: (``image/svg+xml``, ``application/vnd.api+json``,
#: ``application/x-www-form-urlencoded``).
#:
#: Anchored at both ends (``^...$``) so partial matches do not pass.
#: The same source string is shared with the Pydantic
#: ``Field(pattern=...)`` constraint on :attr:`ProductMedia.mime_type`
#: so the rule is declared exactly once.
#:
#: Examples accepted:  ``image/png``, ``video/mp4``,
#:                     ``application/json``, ``image/svg+xml``,
#:                     ``application/vnd.api+json``.
#: Examples rejected:  ``not-a-mime-type`` (no slash), ``image/`` (no
#:                     subtype), ``/png`` (no type), ``image png``
#:                     (space), empty string.
_MIME_TYPE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-zA-Z]+/[a-zA-Z0-9.+\-]+$"
)

#: URL scheme pattern: matches the leading ``http://`` or ``https://``
#: of a URL. Compiled with :data:`re.IGNORECASE` so ``HTTP://``,
#: ``Https://``, etc. are accepted (RFC 3986 section 3.1: scheme is
#: case-insensitive). The pattern is deliberately shallow — it does
#: NOT validate the rest of the URL (host, path, query). Deeper URL
#: validation (domain allow-list, CDN-prefix matching, signed-URL-
#: expiry parsing) belongs in middleware / repository where settings
#: are accessible. This aggregate enforces the bare minimum: only
#: HTTP and HTTPS schemes are permitted (no ``ftp://``,
#: ``data:``, ``javascript:``, etc.).
_URL_PATTERN: Final[re.Pattern[str]] = re.compile(r"^https?://", re.IGNORECASE)


# =============================================================================
# MediaKind enum
# =============================================================================


class MediaKind(StrEnum):
    """Kind of media asset attached to a :class:`Product`.

    Subclasses :class:`enum.StrEnum` (Python 3.11+) so members are
    simultaneously :class:`str` instances AND enum members. This
    enables:

      * Transparent serialization to MongoDB / JSON: storing
        :data:`MediaKind.IMAGE` writes the literal string ``"image"``
        without any custom encoder.
      * Transparent comparison with raw strings:
        ``MediaKind.IMAGE == "image"`` is ``True``.
      * Direct coercion from string values when
        :meth:`ProductMedia.new` receives a string ``kind`` argument
        (the factory calls ``MediaKind(kind)`` which constructs the
        enum member from its string value).

    Values are lowercase strings (``"image"``, ``"video"``,
    ``"thumbnail"``) to match the user-facing API representation. This
    deviates from the standard UPPERCASE Python enum convention used by
    :class:`OrderStatus` / :class:`PaymentStatus` in the Order / Payment
    Services because those enums are NOT user-facing — they are
    internal state-machine tokens. :class:`MediaKind`, by contrast,
    surfaces directly in API responses (``GET /products/{id}/media``)
    and in the :class:`product.updated` Kafka event envelope, so the
    canonical form is the lowercase string.

    Attributes:
        IMAGE: Static image asset (``image/png``, ``image/jpeg``,
            ``image/webp``, ``image/svg+xml``, etc.). The most common
            kind; rendered inline on the storefront product page.
        VIDEO: Video asset (``video/mp4``, ``video/webm``, etc.).
            Rendered via an HTML5 ``<video>`` element on the storefront.
        THUMBNAIL: Lower-resolution preview image suitable for product
            list / search-result rendering. A product may have one or
            more thumbnails alongside its full-resolution images;
            consumers select by ``position`` and aspect ratio.

    Example:
        >>> MediaKind.IMAGE == "image"
        True
        >>> MediaKind("image") is MediaKind.IMAGE
        True
        >>> MediaKind.VIDEO.value
        'video'
        >>> isinstance(MediaKind.IMAGE, str)
        True
    """

    IMAGE = "image"
    VIDEO = "video"
    THUMBNAIL = "thumbnail"


# =============================================================================
# ProductMedia aggregate root
# =============================================================================


class ProductMedia(BaseModel):
    """The ProductMedia aggregate root.

    A frozen Pydantic v2 model representing a single CDN-hosted media
    asset attached to a :class:`Product`. Persisted in the MongoDB
    ``product_media`` collection inside the Product Service's private
    ``product_db`` (per AAP Section 0.4.4); :attr:`product_id` is a
    foreign-key-style reference to a peer document in the same
    ``product_db`` (NOT cross-service per AAP R-6 — database per
    service).

    Validators applied:
        * ``url`` matches :data:`_URL_PATTERN` (must use the ``http`` or
          ``https`` scheme); length bounded ``[1, MAX_URL_LENGTH]``.
        * ``mime_type`` matches :data:`_MIME_TYPE_PATTERN` (RFC 6838
          ``type/subtype`` form); length bounded
          ``[1, MAX_MIME_TYPE_LENGTH]``.
        * ``alt_text`` length bounded ``[0, MAX_ALT_TEXT_LENGTH]``;
          optional (defaults to ``None``).
        * ``position`` bounded ``[MIN_POSITION, +inf)`` — non-negative.
        * ``width`` / ``height`` each bounded
          ``[MIN_DIMENSION, MAX_DIMENSION]`` (pixels); optional.
        * ``version`` bounded ``[MIN_VERSION, +inf)``.
        * ``created_at`` and ``updated_at`` MUST be timezone-aware
          (UTC). Naive datetimes are rejected by the
          :meth:`_require_tzaware` field validator.
        * ``updated_at >= created_at`` enforced by the
          :meth:`_updated_at_not_before_created_at` model validator.

    Attributes:
        id: Globally-unique aggregate identifier (UUID4, stored as
            string for JSON / MongoDB serialization compatibility).
            Immutable through :meth:`apply_changes`.
        product_id: Reference to the owning product
            (``products._id``). Foreign-key-style reference to a peer
            document in the same ``product_db``. Immutable through
            :meth:`apply_changes` — moving a media asset from one
            product to another is not supported (would invalidate
            referential integrity).
        kind: Classification of the media asset
            (``image`` / ``video`` / ``thumbnail``). See
            :class:`MediaKind`.
        url: HTTP(S) URL of the CDN-hosted asset; bytes themselves are
            stored on the CDN, NOT in this collection. Mutable through
            :meth:`apply_changes` to support CDN URL rotation.
        alt_text: Alternative text for accessibility (WCAG 2.1) and
            SEO. Optional; presence-for-images is enforced by the
            controller / command-handler layer, not by this aggregate.
        position: 0-indexed display order within the product's media
            list. Lower values appear first. Defaults to ``0``.
        width: Pixel width for image / video; absent for thumbnails
            that have not yet been inspected. Bounded
            ``[1, 100_000]``.
        height: Pixel height for image / video; absent for thumbnails
            that have not yet been inspected. Bounded
            ``[1, 100_000]``.
        mime_type: RFC 6838 media type
            (e.g., ``image/jpeg``, ``video/mp4``,
            ``application/pdf``).
        version: Optimistic-concurrency token. Starts at ``1`` for new
            documents; incremented by the repository on every update
            via ``findOneAndUpdate`` filter ``{_id, version: N}`` +
            ``$inc: {version: 1}``. :meth:`apply_changes` does NOT
            increment this field.
        created_at: UTC timestamp when the document was created;
            immutable through :meth:`apply_changes`.
        updated_at: UTC timestamp of the most recent update; bumped by
            :meth:`apply_changes` to ``datetime.now(UTC)``.

    Example:
        >>> from src.domain.product_media import ProductMedia, MediaKind
        >>> m = ProductMedia.new(
        ...     product_id="prod-123",
        ...     kind=MediaKind.IMAGE,
        ...     url="https://cdn.example.com/img.png",
        ...     mime_type="image/png",
        ... )
        >>> m.version
        1
        >>> m.position
        0
        >>> m.kind == MediaKind.IMAGE
        True
        >>> m2 = m.apply_changes({"alt_text": "Front view"})
        >>> m2.alt_text
        'Front view'
        >>> m2.id == m.id
        True
        >>> m2.version  # unchanged — repository's job
        1
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
    #                                  (``id``, ``product_id``,
    #                                  ``url``, ``alt_text``,
    #                                  ``mime_type``). Defends against
    #                                  copy-paste artifacts from API
    #                                  request bodies and Kafka event
    #                                  payloads.
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
                "as string). Generated by :meth:`ProductMedia.new`. "
                "Immutable through :meth:`apply_changes`."
            ),
        ),
    ]

    product_id: Annotated[
        str,
        Field(
            min_length=1,
            description=(
                "Reference to the owning product (`products._id`). "
                "Foreign-key-style reference to a peer document in "
                "the same `product_db` (NOT cross-service per AAP "
                "R-6). Immutable through :meth:`apply_changes` — "
                "moving a media asset from one product to another is "
                "not supported (would invalidate referential "
                "integrity)."
            ),
        ),
    ]

    kind: Annotated[
        MediaKind,
        Field(
            description=(
                "Classification of the media asset (image / video / "
                "thumbnail). See :class:`MediaKind`."
            ),
        ),
    ]

    url: Annotated[
        str,
        Field(
            min_length=1,
            max_length=MAX_URL_LENGTH,
            description=(
                "HTTP(S) URL of the CDN-hosted asset. The asset bytes "
                "themselves live on the CDN; this collection stores "
                "ONLY the metadata. Mutable through "
                ":meth:`apply_changes` to support CDN URL rotation. "
                f"Bounded to {MAX_URL_LENGTH} chars (matches common "
                "CDN / browser / proxy caps)."
            ),
        ),
    ]

    alt_text: Annotated[
        str | None,
        Field(
            default=None,
            min_length=0,
            max_length=MAX_ALT_TEXT_LENGTH,
            description=(
                "Alternative text for accessibility (WCAG 2.1) and "
                "SEO. Optional; presence-for-images is enforced by "
                "the controller / command-handler layer, not by this "
                "aggregate (the rule varies by API surface). "
                f"Bounded to {MAX_ALT_TEXT_LENGTH} chars."
            ),
        ),
    ] = None

    position: Annotated[
        int,
        Field(
            ge=MIN_POSITION,
            description=(
                "0-indexed display order within the product's media "
                "list. Lower values appear first. Defaults to 0."
            ),
        ),
    ] = 0

    width: Annotated[
        int | None,
        Field(
            default=None,
            ge=MIN_DIMENSION,
            le=MAX_DIMENSION,
            description=(
                "Pixel width for image / video. Optional — may be "
                "patched in by an asynchronous inspection job after "
                f"upload. Bounded [{MIN_DIMENSION}, {MAX_DIMENSION}] "
                "(defensive cap against pathological clients)."
            ),
        ),
    ] = None

    height: Annotated[
        int | None,
        Field(
            default=None,
            ge=MIN_DIMENSION,
            le=MAX_DIMENSION,
            description=(
                "Pixel height for image / video. Optional — may be "
                "patched in by an asynchronous inspection job after "
                f"upload. Bounded [{MIN_DIMENSION}, {MAX_DIMENSION}] "
                "(defensive cap against pathological clients)."
            ),
        ),
    ] = None

    mime_type: Annotated[
        str,
        Field(
            min_length=1,
            max_length=MAX_MIME_TYPE_LENGTH,
            pattern=_MIME_TYPE_PATTERN.pattern,
            description=(
                "RFC 6838 media type (e.g., `image/jpeg`, "
                "`video/mp4`, `application/pdf`). Must match the "
                "canonical `type/subtype` form. Vendor-prefixed types "
                "(e.g., `image/svg+xml`, `application/vnd.api+json`) "
                "are accepted via the `+`, `.`, `-` characters in "
                f"the subtype. Bounded to {MAX_MIME_TYPE_LENGTH} "
                "chars."
            ),
        ),
    ]

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

        :meth:`ProductMedia.new` always uses
        :func:`datetime.now(timezone.utc)` so it satisfies this rule
        by construction. Callers reconstructing a
        :class:`ProductMedia` from a database document or an API
        request body MUST ensure the timestamps carry a timezone (the
        MongoDB BSON date type and the FastAPI Pydantic decoder both
        produce timezone-aware datetimes by default).

        Args:
            v: The candidate :class:`datetime` value.

        Returns:
            The same value ``v`` if it is timezone-aware.

        Raises:
            ValueError: When ``v`` is naive (``tzinfo is None`` or
                ``utcoffset()`` returns ``None``). Pydantic wraps this
                in a :class:`pydantic.ValidationError` at the model
                boundary.
        """
        if v.tzinfo is None or v.tzinfo.utcoffset(v) is None:
            raise ValueError("datetime fields must be timezone-aware (UTC)")
        return v

    @field_validator("url")
    @classmethod
    def _validate_url_scheme(cls, v: str) -> str:
        """Enforce that ``url`` uses the ``http`` or ``https`` scheme.

        Deeper URL validation (domain allow-list, CDN-prefix matching,
        signed-URL-expiry parsing) belongs in middleware / repository
        where configuration settings are accessible. This aggregate
        enforces only the bare minimum — only HTTP and HTTPS schemes
        are permitted (no ``ftp://``, ``data:``, ``javascript:``,
        ``file:``, or other potentially-dangerous schemes).

        Pydantic's automatic whitespace stripping
        (``str_strip_whitespace=True``) and ``min_length=1`` /
        ``max_length=MAX_URL_LENGTH`` constraints have already run by
        the time this validator executes — it sees a stripped, length-
        bounded string and need only verify the leading scheme.

        The :data:`_URL_PATTERN` regex matches case-insensitively
        (RFC 3986 section 3.1: scheme is case-insensitive), so
        ``HTTP://``, ``Https://``, etc. are accepted.

        Args:
            v: The candidate URL string.

        Returns:
            The same value ``v`` if it matches the scheme pattern.

        Raises:
            ValueError: When ``v`` does not start with ``http://`` or
                ``https://``. Pydantic wraps this in a
                :class:`pydantic.ValidationError` at the model
                boundary, which the FastAPI exception middleware
                translates to an HTTP ``422 Unprocessable Entity``
                response at the controller boundary.
        """
        if not _URL_PATTERN.match(v):
            raise ValueError("url must use http or https scheme")
        return v

    # ------------------------------------------------------------------
    # Model validators
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def _updated_at_not_before_created_at(self) -> ProductMedia:
        """Cross-field invariant: ``updated_at >= created_at``.

        A document whose most-recent-update timestamp predates its
        creation timestamp is corrupted and is rejected at
        construction time. :meth:`ProductMedia.new` initializes both
        timestamps to the same value, satisfying this invariant by
        construction. :meth:`apply_changes` always bumps
        ``updated_at`` to a value greater than or equal to the
        previous ``updated_at`` (which is itself ``>= created_at`` by
        induction), so the invariant is preserved.

        Returns:
            ``self`` if the invariant holds. Pydantic
            :func:`model_validator` with ``mode="after"`` requires the
            validator to return the model instance.

        Raises:
            ValueError: When :attr:`updated_at` is strictly less than
                :attr:`created_at`. Pydantic wraps this in a
                :class:`pydantic.ValidationError`.
        """
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
        product_id: str,
        kind: MediaKind | str,
        url: str,
        mime_type: str,
        alt_text: str | None = None,
        position: int = 0,
        width: int | None = None,
        height: int | None = None,
    ) -> ProductMedia:
        """Create a brand-new :class:`ProductMedia` aggregate.

        Generates a fresh UUID4 :attr:`id`, sets :attr:`version` to
        ``1``, and stamps :attr:`created_at` / :attr:`updated_at` to
        ``datetime.now(timezone.utc)``. All other fields are taken
        from the keyword arguments, with the same default values as
        the field definitions: ``alt_text=None``, ``position=0``,
        ``width=None``, ``height=None``.

        ``kind`` accepts EITHER a :class:`MediaKind` instance OR its
        raw string value (``"image"``, ``"video"``, ``"thumbnail"``).
        Strings are coerced to the enum via ``MediaKind(kind)`` before
        construction; this is a lightweight sanity check that fails
        fast with a clear :class:`ValueError` if the caller passes an
        unknown string. (Pydantic itself would also coerce a string to
        the enum during model validation, but doing the coercion here
        normalizes the value for downstream code paths and produces
        a slightly clearer error message.)

        Keyword arguments are required (the leading ``*`` enforces
        keyword-only invocation) so callers cannot accidentally swap
        ``product_id`` and ``url``.

        Args:
            product_id: Reference to the owning product (the
                ``Product.id`` field). Must be a non-empty string.
            kind: Classification of the asset
                (``MediaKind.IMAGE`` / ``MediaKind.VIDEO`` /
                ``MediaKind.THUMBNAIL``) OR the equivalent raw string
                (``"image"`` / ``"video"`` / ``"thumbnail"``).
            url: HTTP(S) URL of the CDN-hosted asset. Must use the
                ``http`` or ``https`` scheme; bounded to
                :data:`MAX_URL_LENGTH` chars.
            mime_type: RFC 6838 media type
                (e.g., ``"image/png"``, ``"video/mp4"``).
            alt_text: Optional accessibility / SEO text. Defaults to
                ``None``.
            position: 0-indexed display order; defaults to ``0``.
            width: Optional pixel width; defaults to ``None``.
            height: Optional pixel height; defaults to ``None``.

        Returns:
            A fully-validated :class:`ProductMedia` instance with
            ``id`` populated, ``version=1``, and ``created_at`` /
            ``updated_at`` set to the current UTC time.

        Raises:
            pydantic.ValidationError: When any field-level constraint
                fails (e.g., URL scheme is not HTTP(S), MIME type
                does not match the canonical regex, dimensions out of
                range).
            ValueError: When ``kind`` is a string but does not match
                any :class:`MediaKind` member (raised directly by
                ``MediaKind(kind)`` before model construction).

        Example:
            >>> m = ProductMedia.new(
            ...     product_id="prod-123",
            ...     kind=MediaKind.IMAGE,
            ...     url="https://cdn.example.com/img.png",
            ...     mime_type="image/png",
            ... )
            >>> m.version
            1
            >>> m.position
            0
            >>> m.kind == MediaKind.IMAGE
            True
        """
        now = datetime.now(timezone.utc)
        return cls(
            id=str(uuid.uuid4()),
            product_id=product_id,
            kind=MediaKind(kind) if isinstance(kind, str) else kind,
            url=url,
            mime_type=mime_type,
            alt_text=alt_text,
            position=position,
            width=width,
            height=height,
            version=1,
            created_at=now,
            updated_at=now,
        )

    def apply_changes(
        self,
        changes: dict[str, Any],
        *,
        now: datetime | None = None,
    ) -> ProductMedia:
        """Return a new :class:`ProductMedia` with the given fields updated.

        Same semantics as the other aggregates in the Product Service
        (:class:`Product`, :class:`Category`):

          * :attr:`updated_at` is bumped to ``now`` (or
            ``datetime.now(timezone.utc)`` if ``now`` is ``None``).
          * :attr:`version` is NOT incremented — that is the
            repository's job (via ``findOneAndUpdate`` filter +
            ``$inc``).
          * :attr:`id`, :attr:`product_id`, :attr:`version`, and
            :attr:`created_at` are immutable. Attempting to update any
            of them raises :class:`ValueError` BEFORE
            :meth:`pydantic.BaseModel.model_copy` runs, which keeps
            the error message clear and actionable.
          * The result is a fresh :class:`ProductMedia` instance
            produced via :meth:`pydantic.BaseModel.model_copy`. Per
            the documented Pydantic v2 semantics, ``model_copy`` does
            NOT re-run field and model validators on the merged field
            set — it merely constructs a copy with the patched
            attributes. The repository / command-handler layer is
            responsible for validating ``changes`` (typically by
            constructing a fresh :class:`ProductMedia` and asserting
            it succeeds) BEFORE invoking this method. This deliberate
            split keeps the aggregate's mutation API cheap (a single
            dict merge) while leaving rich validation in the
            command-handler layer where domain-specific rules
            (e.g., "thumbnails must have width set") live.

        Why ``product_id`` is immutable here:
          Moving a media asset from one product to another would
          invalidate referential integrity (the storefront UI and the
          recommendation engine both query by ``product_id``). If a
          truly different product needs the same asset, the operator
          should create a new :class:`ProductMedia` document for the
          new product (the URL on the CDN is shared transparently).

        Args:
            changes: Mapping of field name to new value. Only mutable
                fields may appear (``kind``, ``url``, ``alt_text``,
                ``position``, ``width``, ``height``, ``mime_type``).
            now: Optional override for the new :attr:`updated_at`
                value. If ``None`` (the default), the current UTC
                time is used. The override is primarily for testing
                determinism.

        Returns:
            A fresh :class:`ProductMedia` instance with the given
            fields applied and :attr:`updated_at` bumped. The
            original instance is unchanged (Pydantic v2
            ``model_copy`` does not mutate ``self``).

        Raises:
            ValueError: When ``changes`` contains any of the immutable
                field names (``id``, ``product_id``, ``version``,
                ``created_at``). The exception is raised eagerly with
                a clear message identifying the offending field.

        Example:
            >>> m = ProductMedia.new(
            ...     product_id="prod-123",
            ...     kind=MediaKind.IMAGE,
            ...     url="https://cdn.example.com/img.png",
            ...     mime_type="image/png",
            ... )
            >>> m2 = m.apply_changes({"alt_text": "Front view"})
            >>> m2.alt_text
            'Front view'
            >>> m2.id == m.id
            True
            >>> m2.version == m.version  # unchanged — repo's job
            True
            >>> m2.updated_at > m.updated_at
            True

            Attempting to mutate an immutable field raises:

            >>> m.apply_changes({"product_id": "evil"})
            Traceback (most recent call last):
                ...
            ValueError: field 'product_id' is immutable and cannot be updated
        """
        if now is None:
            now = datetime.now(timezone.utc)

        for forbidden in ("id", "product_id", "version", "created_at"):
            if forbidden in changes:
                raise ValueError(
                    f"field {forbidden!r} is immutable and cannot be updated"
                )

        return self.model_copy(update={**changes, "updated_at": now})

    def to_event_envelope(self) -> dict[str, Any]:
        """Produce a self-contained event envelope (per AAP R-33).

        Used by the :class:`Product` aggregate / events package when
        serializing media references attached to a ``product.created``
        or ``product.updated`` Kafka event. Per AAP R-33 events must
        be self-contained — consumers should not need to call back to
        this service to interpret a media reference embedded in a
        product event. This method therefore returns a fully-resolved
        payload containing every field a consumer needs.

        Field encoding choices:
          * ``kind`` is emitted as its raw string value (``"image"`` /
            ``"video"`` / ``"thumbnail"``) via :attr:`MediaKind.value`
            for cross-language consumer compatibility (a Java consumer
            need not know about Python's :class:`StrEnum` — it sees a
            plain string).
          * ``created_at`` and ``updated_at`` are emitted as RFC 3339
            ISO 8601 strings via :meth:`datetime.isoformat`. Producing
            a string keeps the envelope JSON-serializable without a
            custom encoder; the timezone offset is preserved (e.g.,
            ``"2024-01-15T12:34:56.789012+00:00"``).
          * Optional fields (``alt_text``, ``width``, ``height``) are
            included as ``None`` rather than omitted so consumers can
            reliably check ``if envelope.get("alt_text"): ...``
            without distinguishing "absent" from "explicit null".

        Returns:
            A dict with twelve keys (``id``, ``product_id``, ``kind``,
            ``url``, ``alt_text``, ``position``, ``width``, ``height``,
            ``mime_type``, ``version``, ``created_at``, ``updated_at``)
            suitable for inclusion in a ``product.*`` Kafka event
            payload (Schema Registry-validated upstream by the
            producer pipeline per AAP R-14).

        Example:
            >>> m = ProductMedia.new(
            ...     product_id="prod-123",
            ...     kind=MediaKind.IMAGE,
            ...     url="https://cdn.example.com/img.png",
            ...     mime_type="image/png",
            ... )
            >>> env = m.to_event_envelope()
            >>> env["kind"]
            'image'
            >>> env["url"].startswith("https://")
            True
            >>> isinstance(env["created_at"], str)
            True
        """
        return {
            "id": self.id,
            "product_id": self.product_id,
            "kind": self.kind.value,
            "url": self.url,
            "alt_text": self.alt_text,
            "position": self.position,
            "width": self.width,
            "height": self.height,
            "mime_type": self.mime_type,
            "version": self.version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


# =============================================================================
# Public surface
# =============================================================================
#
# Sorted alphabetically (the standard convention for ``__all__``) so
# diff hygiene is preserved on future additions.

__all__ = [
    "MAX_ALT_TEXT_LENGTH",
    "MAX_DIMENSION",
    "MAX_MIME_TYPE_LENGTH",
    "MAX_URL_LENGTH",
    "MIN_DIMENSION",
    "MIN_POSITION",
    "MIN_VERSION",
    "MediaKind",
    "ProductMedia",
]
