"""Pure, dependency-free domain models for the Recommendation Engine.

This module is the **type vocabulary** of the Recommendation Engine. Every
domain concept — products, users, feature deltas, embeddings, recommendations,
model metadata — is represented by a frozen Pydantic v2 ``BaseModel`` with
strict validation.

Design principles (each enforced by tests in ``tests/unit/test_models.py``)
--------------------------------------------------------------------------
1. **Immutable by default** — every class sets ``frozen=True`` so accidental
   mutation raises ``ValidationError``. Updates go through
   :meth:`pydantic.BaseModel.model_copy` (``update={...}``).
2. **Strict types** — UUIDs are :class:`uuid.UUID` (never ``str``); money is
   :class:`decimal.Decimal` (never ``float``); timestamps are
   :class:`datetime.datetime` and MUST be timezone-aware (RFC 3339, AAP R-26).
3. **Pure data** — no behavior methods beyond what Pydantic provides and a
   single explicitly-documented helper (:meth:`Neighbor.from_distance`).
4. **JSON round-trip friendly** — ``model_dump_json``/``model_validate_json``
   produces an equal instance for every value reachable through normal use.
5. **Import-graph root** — only standard library + pydantic; no imports from
   ``src/*``. Every other ``src/*`` package may safely depend on this module.

Compliance highlights
---------------------
- AAP R-7  — Polyglot persistence: vector store + Redis (the data classes
  here are the canonical row representation for both stores).
- AAP R-19 — Fail-fast on startup: validators reject malformed inputs at the
  edge so dependents can rely on invariants without re-checking.
- AAP R-20 — Fallback declared for every dependency: ``Product.hydrated``,
  ``RecommendationsResult.degraded``, and ``Recommendation.source`` carry the
  fallback signal through the call graph.
- AAP R-26 — RFC 3339 timestamps: every ``datetime`` field is required to be
  timezone-aware so log lines and DB rows always serialize unambiguously.
- AAP R-30/31 — Event versioning: ``FeatureDelta.event_version`` and
  ``Features.last_event_version`` carry the event-schema version for
  consumer-side compatibility checks.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Module-level constants and shared configuration
# ---------------------------------------------------------------------------

#: Required length of every embedding vector (AAP Section 0.4.4, folder spec).
#:
#: A mismatch between an :class:`Embedding`'s ``vector`` length and this
#: constant raises :class:`ValueError` at construction time; the repository
#: layer re-raises that as ``EmbeddingDimensionMismatchError`` (defined in
#: :mod:`src.domain.errors`) so callers receive a domain-typed exception.
#:
#: This value MUST stay in lockstep with ``ModelMetadata.embedding_dim`` of
#: every loaded model artifact; the model loader compares the two at startup
#: and refuses to boot on a mismatch (fail-fast per AAP R-19).
EMBEDDING_DIM: int = 128

#: Shared Pydantic configuration applied to every frozen domain model.
#:
#: * ``frozen=True`` — instances are immutable; mutation raises
#:   :class:`pydantic.ValidationError`. Use :meth:`BaseModel.model_copy`
#:   (with ``update={...}``) to produce a modified copy.
#: * ``strict=True`` — disables Pydantic's lax-mode coercion (e.g., ``"42"``
#:   to :class:`int`); callers must pass values of the declared type.
#: * ``extra="forbid"`` — unexpected fields raise on construction. This is
#:   the right default for domain types: an unexpected field signals a
#:   contract drift between producer and consumer that we want to catch
#:   loudly in tests rather than silently drop.
#: * ``populate_by_name=True`` — supports both field names and aliases when
#:   parsing inbound payloads (no aliases are declared today, but enabling
#:   this keeps the door open without a future model_config refactor).
#: * ``ser_json_timedelta="iso8601"`` — durations serialize to ISO-8601 (no
#:   ``timedelta`` fields exist today; this is forward-compatible).
#: * ``ser_json_bytes="utf8"`` — byte fields serialize as UTF-8 strings (no
#:   ``bytes`` fields exist today; forward-compatible).
#: * ``arbitrary_types_allowed=False`` — refuse types that Pydantic does not
#:   know how to validate; this catches accidental use of NumPy arrays,
#:   ORM rows, etc., which would defeat the "pure data" guarantee.
_BASE_CONFIG: ConfigDict = ConfigDict(
    frozen=True,
    strict=True,
    extra="forbid",
    populate_by_name=True,
    ser_json_timedelta="iso8601",
    ser_json_bytes="utf8",
    arbitrary_types_allowed=False,
)


def _config_with(**overrides: object) -> ConfigDict:
    """Return a new :class:`ConfigDict` derived from :data:`_BASE_CONFIG`.

    Use this helper when a class needs a one-off configuration tweak (e.g.,
    :class:`Embedding` disables the ``model_*`` protected-namespace warning
    because it intentionally has a ``model_version`` field). The returned
    :class:`ConfigDict` is a brand-new mapping; mutating it does not affect
    :data:`_BASE_CONFIG` or any other class.

    Args:
        **overrides: Configuration keys to set or override. Keys must be
            valid :class:`ConfigDict` keys; invalid keys raise at runtime
            when Pydantic processes the model.

    Returns:
        A new :class:`ConfigDict` with the base settings plus ``overrides``.
    """
    merged: dict[str, object] = dict(_BASE_CONFIG)
    merged.update(overrides)
    # ConfigDict is a TypedDict; constructing it with **merged is the
    # idiomatic way to produce a value with the declared type at runtime.
    # The double type-ignore covers (a) overrides whose typed key is not
    # known to TypedDict and (b) mypy's --strict no-any-return rule, which
    # cannot infer that ConfigDict(**dict[str, object]) preserves the type.
    return ConfigDict(**merged)  # type: ignore[typeddict-item, no-any-return]


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------


class Product(BaseModel):
    """A product as projected into the Recommendation Engine.

    This is a DENORMALIZED view of the authoritative Product Service record
    (AAP R-6 — database per service). Only the attributes needed for
    recommendation scoring, metadata hydration, and category-based filtering
    are kept.

    The ``hydrated`` flag indicates whether the record was successfully
    filled from Product Service (``True``) or is a best-effort placeholder
    (``False`` — used when Product Service is unavailable and the caller
    degrades gracefully per AAP R-20). Downstream serializers can render a
    skeleton ``Product(hydrated=False)`` with ``title="<unknown>"`` so the
    API still returns a useful response shape.

    Attributes:
        id: Product UUID. Matches the Product Service primary key.
        title: Display title (1..500 chars). Empty strings are rejected.
        description: Optional long-form description (<= 5000 chars).
        category_path: Ordered category hierarchy from root to leaf, e.g.
            ``["electronics", "phones", "accessories"]``. An empty list
            denotes "uncategorized".
        price: Monetary amount as :class:`Decimal` (never ``float``). May be
            ``None`` for unpriced/draft products. If provided, it MUST be
            non-negative.
        tags: Free-form string tags (search facets, marketing labels).
        hydrated: ``True`` if filled from Product Service, ``False`` for the
            fallback skeleton (AAP R-20).
        occurred_at: Timestamp of the snapshot/event that produced this
            record (RFC 3339, UTC). MUST be timezone-aware.
    """

    model_config = _BASE_CONFIG

    id: UUID
    title: str = Field(min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=5000)
    category_path: list[str] = Field(default_factory=list)
    price: Decimal | None = None
    tags: list[str] = Field(default_factory=list)
    hydrated: bool = True
    occurred_at: datetime

    @field_validator("occurred_at")
    @classmethod
    def _require_tzaware(cls, value: datetime) -> datetime:
        """Reject naive datetimes; UTC-awareness is mandatory (AAP R-26)."""
        if value.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware (UTC expected)")
        return value

    @field_validator("price")
    @classmethod
    def _price_non_negative(cls, value: Decimal | None) -> Decimal | None:
        """Reject negative prices; ``None`` is permitted for unpriced drafts."""
        if value is not None and value < Decimal("0"):
            raise ValueError("price must be non-negative")
        return value


class User(BaseModel):
    """Minimal user projection used by the Recommendation Engine.

    Only attributes needed for personalization are modeled here; full user
    profile data lives in the User Service (AAP R-6 — database per service).
    Cross-service lookups use ``id`` as the join key.

    Attributes:
        id: User UUID. Matches the User Service primary key.
        preferences: Free-form key/value preferences (e.g.,
            ``{"language": "en-US", "theme": "dark"}``). Both keys and
            values are strings; richer types belong in the User Service.
        region: Optional ISO-3166-style region code (<= 16 chars) used for
            region-aware recommendations and provider routing decisions
            elsewhere in the platform.
    """

    model_config = _BASE_CONFIG

    id: UUID
    preferences: dict[str, str] = Field(default_factory=dict)
    region: str | None = Field(default=None, max_length=16)


class Features(BaseModel):
    """Aggregated interaction features for a ``(user, product)`` pair.

    Persisted in the ``interaction_features`` table (AAP Section 0.4.4).
    This is a snapshot-style record: mutations produce a new row by applying
    a :class:`FeatureDelta` via the feature pipeline's upsert path.

    Attributes:
        user_id: User UUID referenced by the row.
        product_id: Product UUID referenced by the row.
        view_count: Lifetime view count (>= 0).
        purchase_count: Lifetime purchase count (>= 0).
        last_interaction_at: Timestamp of the most recent interaction
            (RFC 3339, UTC). MUST be timezone-aware.
        last_event_version: Positive integer schema-version of the most
            recent event applied to this row (``int >= 1``). Type chosen
            to mirror the unified wire envelope adopted across every
            service (Notification Service, Order Service, Payment
            Service JSON-Schemas, and the Recommendation Engine event
            models). Used to short-circuit replay on consumer restart
            and to detect schema regressions.
    """

    model_config = _BASE_CONFIG

    user_id: UUID
    product_id: UUID
    view_count: int = Field(ge=0)
    purchase_count: int = Field(ge=0)
    last_interaction_at: datetime
    last_event_version: int = Field(ge=1)

    @field_validator("last_interaction_at")
    @classmethod
    def _require_tzaware(cls, value: datetime) -> datetime:
        """Reject naive datetimes; UTC-awareness is mandatory (AAP R-26)."""
        if value.tzinfo is None:
            raise ValueError("last_interaction_at must be timezone-aware")
        return value


class FeatureDelta(BaseModel):
    """Incremental update to :class:`Features` derived from a single event.

    At least one of ``product_id`` / ``user_id`` MUST be provided — events
    without either cannot be applied (the event boundary raises
    ``InvalidEventError`` in that case).

    Semantics of each delta field:
    * ``view_count_delta`` and ``purchase_count_delta`` ACCUMULATE onto the
      corresponding :class:`Features` row.
    * ``rating_value`` REPLACES the row's last rating rather than
      accumulating; rating semantics are point-in-time, not cumulative.

    Attributes:
        product_id: Product UUID; one of ``product_id``/``user_id`` MUST
            be provided.
        user_id: User UUID; one of ``product_id``/``user_id`` MUST be
            provided.
        view_count_delta: Delta to apply to ``Features.view_count``.
            Negative values are permitted (e.g., correction events) and
            applied as ``new = max(0, old + delta)`` by the upsert path.
        purchase_count_delta: Delta to apply to ``Features.purchase_count``;
            same negative-value semantics as ``view_count_delta``.
        rating_value: Optional 0..5 rating that replaces the existing rating.
        occurred_at: Timestamp of the source event (RFC 3339, UTC). MUST be
            timezone-aware.
        event_version: Positive integer schema-version of the source event
            (``int >= 1``). Mirrors the wire-format ``event_version: int``
            adopted across the platform's canonical envelope.
    """

    model_config = _BASE_CONFIG

    product_id: UUID | None = None
    user_id: UUID | None = None
    view_count_delta: int = 0
    purchase_count_delta: int = 0
    rating_value: float | None = Field(default=None, ge=0.0, le=5.0)
    occurred_at: datetime
    event_version: int = Field(ge=1)

    @field_validator("occurred_at")
    @classmethod
    def _require_tzaware(cls, value: datetime) -> datetime:
        """Reject naive datetimes; UTC-awareness is mandatory (AAP R-26)."""
        if value.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _require_at_least_one_entity(self) -> FeatureDelta:
        """Reject deltas that target neither a product nor a user."""
        if self.product_id is None and self.user_id is None:
            raise ValueError(
                "FeatureDelta requires at least one of product_id or user_id"
            )
        return self


class Embedding(BaseModel):
    """Vector embedding for a product or user.

    Persisted in the ``embeddings`` table (AAP Section 0.4.4). Vector length
    is strictly enforced at :data:`EMBEDDING_DIM` (128) AND every component
    must be finite (no ``NaN``/``Inf`` — these poison cosine-similarity math
    and corrupt ranked output).

    The ``protected_namespaces=()`` override silences the Pydantic v2
    ``model_*`` warning for the deliberately-named ``model_version`` field;
    the override is scoped to this class so other models keep the guard.

    Attributes:
        entity_id: UUID of the entity the embedding represents.
        entity_type: Discriminator — ``"product"`` or ``"user"``.
        model_version: ML-model version that produced this embedding
            (1..64 chars). Embeddings from different model versions are NOT
            comparable; the runtime filters by ``model_version`` before
            running KNN.
        vector: List of :data:`EMBEDDING_DIM` finite floats. Pydantic checks
            length via ``Field`` AND a custom validator additionally checks
            for ``NaN``/``Inf`` to provide defense-in-depth.
    """

    model_config = _config_with(protected_namespaces=())

    entity_id: UUID
    entity_type: Literal["product", "user"]
    model_version: str = Field(min_length=1, max_length=64)
    # Use Annotated so Pydantic's compiled length check runs BEFORE the
    # custom validator (faster failure for length errors).
    vector: Annotated[
        list[float],
        Field(min_length=EMBEDDING_DIM, max_length=EMBEDDING_DIM),
    ]

    @field_validator("vector")
    @classmethod
    def _finite_components(cls, value: list[float]) -> list[float]:
        """Reject vectors containing ``NaN`` or ``Inf``.

        Length is enforced by the ``Field(min_length=, max_length=)``
        constraint above; this validator focuses on numeric finiteness.
        We re-check length here to produce a clearer error message in the
        rare case that strict-mode is bypassed (e.g., during development).

        Args:
            value: The candidate vector.

        Returns:
            The original list (validators in Pydantic v2 may return the
            same value when no transformation is needed).

        Raises:
            ValueError: If any component is ``NaN`` or ``Inf``, or if the
                length differs from :data:`EMBEDDING_DIM`.
        """
        if len(value) != EMBEDDING_DIM:
            raise ValueError(
                f"vector must have length {EMBEDDING_DIM}; got {len(value)}"
            )
        for i, component in enumerate(value):
            # NaN comparison: NaN != NaN, so ``component == component`` is
            # False ONLY for NaN. The Inf check uses tuple membership which
            # is the canonical idiom and avoids importing math.isfinite.
            if component != component or component in (
                float("inf"),
                float("-inf"),
            ):
                raise ValueError(
                    f"vector[{i}] must be finite; got {component!r}"
                )
        return value


class Neighbor(BaseModel):
    """Result of a single nearest-neighbor lookup in the vector store.

    **Cosine-only invariant:** ``similarity == 1 - distance``. For L2
    distance, callers MUST convert externally (e.g., a custom mapping) before
    constructing a :class:`Neighbor` — this class assumes cosine to keep the
    invariant single-valued and auditable.

    Storing both ``distance`` and ``similarity`` (rather than computing one
    from the other on demand) is a deliberate trade: the vector store
    returns distance, the HTTP response shows similarity, and persisting
    both makes the conversion explicit at every boundary.

    Attributes:
        entity_id: UUID of the neighboring entity.
        distance: Cosine distance in ``[0.0, 2.0]``; ``Field(ge=0.0)``
            enforces non-negativity. Larger means LESS similar.
        similarity: Cosine similarity in ``[-1.0, 1.0]``. Computed as
            ``1 - distance``; the model validator enforces the invariant
            with a 1e-6 floating-point tolerance.
    """

    model_config = _BASE_CONFIG

    entity_id: UUID
    distance: float = Field(ge=0.0)
    similarity: float

    @model_validator(mode="after")
    def _invariant(self) -> Neighbor:
        """Enforce the cosine invariant ``similarity == 1 - distance``."""
        expected = 1.0 - self.distance
        # pgvector may return distances with numerical noise at the
        # 1e-9 scale; permit a tiny tolerance so legitimate inputs pass.
        if abs(self.similarity - expected) > 1e-6:
            raise ValueError(
                "similarity must equal 1 - distance (cosine); "
                f"got distance={self.distance}, similarity={self.similarity}"
            )
        return self

    @classmethod
    def from_distance(cls, entity_id: UUID, distance: float) -> Neighbor:
        """Construct a :class:`Neighbor` with similarity derived from distance.

        This is the preferred constructor when consuming pgvector / KNN
        results because it computes ``similarity`` exactly, avoiding any
        chance of an off-by-epsilon invariant violation.

        Args:
            entity_id: UUID of the neighbor.
            distance: Cosine distance from the query vector.

        Returns:
            A new :class:`Neighbor` with ``similarity = 1 - distance``.
        """
        return cls(entity_id=entity_id, distance=distance, similarity=1.0 - distance)


class Recommendation(BaseModel):
    """Single scored recommendation produced by the engine or a fallback.

    The ``source`` field discriminates the provenance of the recommendation
    so downstream clients can log and reason about degraded responses
    (AAP R-20). The full set of legitimate sources mirrors the fallback
    chain: ML inference -> Redis cache -> popularity table -> empty default.

    Attributes:
        product_id: UUID of the recommended product.
        score: Source-specific score (cosine similarity for ``"ml"``,
            popularity score for ``"popularity"``, etc.). Higher is better.
            Range is intentionally unconstrained because it is
            source-specific; clients should not compare scores across
            ``source`` values.
        source: Provenance discriminator —
            ``"ml"`` (live model output),
            ``"cache"`` (Redis snapshot of a previous ML response),
            ``"popularity"`` (rolling-window popularity ranking), or
            ``"default"`` (empty/synthetic placeholder when no data exists).
        rank: 1-based position in the recommendation list (``>= 1``).
    """

    model_config = _BASE_CONFIG

    product_id: UUID
    score: float
    source: Literal["ml", "cache", "popularity", "default"]
    rank: int = Field(ge=1)


class RecommendationItem(BaseModel):
    """Hydrated recommendation row for HTTP response payloads.

    A :class:`RecommendationItem` enriches a :class:`Recommendation` with
    Product Service metadata (title, category) when hydration succeeds. If
    Product Service is unavailable (AAP R-20), the client receives a record
    with ``title=None`` and ``category=None`` so the response shape stays
    consistent and the caller can render a "name unavailable" placeholder.

    Attributes:
        product_id: UUID of the recommended product.
        title: Display title (<= 500 chars) when hydrated; ``None`` on
            hydration failure.
        category: Single category label (<= 200 chars) when hydrated;
            ``None`` on hydration failure. This is a flattened
            display-friendly view of :attr:`Product.category_path`.
        score: Same source-specific score as :class:`Recommendation`.
        rank: 1-based position in the recommendation list (``>= 1``).
    """

    model_config = _BASE_CONFIG

    product_id: UUID
    title: str | None = Field(default=None, max_length=500)
    category: str | None = Field(default=None, max_length=200)
    score: float
    rank: int = Field(ge=1)


class RecommendationsResult(BaseModel):
    """Top-level response object from the recommendation pipeline.

    The combination of ``degraded`` and ``source`` lets clients render
    different UI copy based on response provenance: e.g., "Top picks for
    you" when ``degraded=False, source="ml"`` versus "Popular products"
    when ``degraded=True, source="popularity"``.

    Exposing ``degraded`` as a separate flag (rather than inferring from
    ``source``) keeps the contract explicit even if a future fallback tier
    is added that should NOT be considered degraded (e.g., a cached ML
    response that is fresh enough to count as "personalized").

    Attributes:
        recommendations: Ordered list of recommendations. May be empty when
            no recommendation can be produced (paired with
            ``source="default"`` and ``degraded=True``).
        degraded: ``True`` when the response came from a fallback tier
            (cache, popularity, or default); ``False`` only for fresh ML
            output (AAP R-20).
        source: Provenance discriminator — same set as
            :attr:`Recommendation.source`.
        confidence: Overall confidence in the response in ``[0.0, 1.0]``.
            For ML responses this is typically the mean similarity of the
            top-K neighbors; for popularity responses it is a fixed value
            from configuration; for empty/default responses it is ``0.0``.
    """

    model_config = _BASE_CONFIG

    recommendations: list[Recommendation]
    degraded: bool
    source: Literal["ml", "cache", "popularity", "default"]
    confidence: float = Field(ge=0.0, le=1.0)


class InferenceResult(BaseModel):
    """Raw output of the ML inference runtime before fallback bookkeeping.

    The fallback chain (``src.fallback.chain``) inspects ``confidence`` and
    may convert this :class:`InferenceResult` into a
    :class:`RecommendationsResult` with ``degraded=True`` if confidence is
    below ``settings.fallback.min_inference_confidence`` — at that point
    the cache or popularity tier takes over. Keeping the raw inference
    output as a separate type (rather than always using
    :class:`RecommendationsResult`) avoids muddling the inference layer
    with fallback knowledge.

    Attributes:
        recommendations: Ordered list of recommendations from the ML
            runtime.
        confidence: Model self-reported confidence in ``[0.0, 1.0]`` (e.g.,
            mean similarity of top-K neighbors).
    """

    model_config = _BASE_CONFIG

    recommendations: list[Recommendation]
    confidence: float = Field(ge=0.0, le=1.0)


class PopularProduct(BaseModel):
    """Materialized row in the ``rec_cache`` popularity table.

    Populated by an offline job that aggregates interaction events over a
    rolling window (AAP Section 0.4.2 fallback path). The ``category``
    field is optional because both global and per-category popularity
    rankings are tracked in the same table — global rows have
    ``category=None``.

    Attributes:
        product_id: UUID of the popular product.
        popularity_score: Non-negative aggregate popularity score
            (``>= 0.0``); higher is more popular. Score units are
            implementation-defined (typically a weighted sum of view and
            purchase counts within the rolling window).
        category: Optional category label (<= 200 chars). ``None`` denotes
            global (cross-category) popularity.
    """

    model_config = _BASE_CONFIG

    product_id: UUID
    popularity_score: float = Field(ge=0.0)
    category: str | None = Field(default=None, max_length=200)


class ModelMetadata(BaseModel):
    """Metadata describing a loaded ML model artifact.

    Read from ``model_metadata.json`` alongside the serialized model at
    ``settings.model.path``. The model loader compares ``embedding_dim``
    against :data:`EMBEDDING_DIM` at startup and refuses to boot on a
    mismatch (fail-fast per AAP R-19) so the runtime never produces
    silently-wrong recommendations.

    Attributes:
        version: Semantic-version-style identifier for the model artifact
            (1..64 chars), e.g., ``"v3.2.1"``.
        trained_at: Timestamp the model was trained (RFC 3339, UTC). MUST
            be timezone-aware.
        framework: ML framework name (1..64 chars), e.g., ``"scikit-learn"``,
            ``"pytorch"``, ``"tensorflow"``.
        framework_version: Framework version string (1..64 chars), e.g.,
            ``"1.5.0"``. Used to detect ABI mismatches between the runtime
            and the saved artifact.
        embedding_dim: Embedding dimension declared by the model
            (``>= 1``). The loader compares this to :data:`EMBEDDING_DIM`
            and refuses incompatible artifacts.
        top_k_default: Default ``K`` for top-K neighbor queries (``>= 1``).
        metric: Similarity metric the model was trained with —
            ``"cosine"`` or ``"l2"``. Closed :class:`Literal` to refuse
            silently-corrupt metadata.
        training_dataset_hash: Stable hash of the training dataset
            (1..128 chars) for reproducibility audits.
        checksum: Cryptographic checksum of the serialized model artifact
            (1..128 chars) for tamper detection at load time.
        notes: Optional free-form notes (<= 4000 chars) for operator
            context (e.g., release notes, hyperparameter highlights).
    """

    model_config = _BASE_CONFIG

    version: str = Field(min_length=1, max_length=64)
    trained_at: datetime
    framework: str = Field(min_length=1, max_length=64)
    framework_version: str = Field(min_length=1, max_length=64)
    embedding_dim: int = Field(ge=1)
    top_k_default: int = Field(ge=1)
    metric: Literal["cosine", "l2"]
    training_dataset_hash: str = Field(min_length=1, max_length=128)
    checksum: str = Field(min_length=1, max_length=128)
    notes: str | None = Field(default=None, max_length=4000)

    @field_validator("trained_at")
    @classmethod
    def _require_tzaware(cls, value: datetime) -> datetime:
        """Reject naive datetimes; UTC-awareness is mandatory (AAP R-26)."""
        if value.tzinfo is None:
            raise ValueError("trained_at must be timezone-aware")
        return value


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

#: Module-level public API. ``__all__`` controls ``from src.domain.models
#: import *`` and is also used by :mod:`src.domain.__init__` to re-export
#: the canonical surface of the domain package. The constant is listed
#: first for discoverability; data classes follow alphabetically.
__all__ = [
    # Constants
    "EMBEDDING_DIM",
    # Data classes
    "Embedding",
    "FeatureDelta",
    "Features",
    "InferenceResult",
    "ModelMetadata",
    "Neighbor",
    "PopularProduct",
    "Product",
    "Recommendation",
    "RecommendationItem",
    "RecommendationsResult",
    "User",
]
