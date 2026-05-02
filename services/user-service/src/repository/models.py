"""User Service — Pydantic v2 row models mirroring user_db tables.

These models are RAW database row projections — one Pydantic class
per table, with field names and types matching the migration DDL
exactly (AAP R-9). They are constructed by repository methods via
``Model.model_validate(row)`` against psycopg v3 ``dict_row`` rows,
and consumed by the domain layer via ``model_dump()`` projections.

This file deliberately uses **Pydantic v2 BaseModels**, NOT SQLAlchemy
ORM declarative models. The User Service's runtime data-access layer
is pure psycopg v3 (AAP R-6, AAP R-7), as wired in
``src.container.build_container``::

    pg_pool = AsyncConnectionPool(conninfo=settings.database.url, ...)
    user_repository = UserRepository(pool=pg_pool)
    address_repository = AddressRepository(pool=pg_pool)
    outbox_repository = OutboxRepository(pool=pg_pool)

The ``sqlalchemy[asyncio]`` dependency in ``requirements.txt`` is
present only for Alembic migrations (``services/user-service/migrations/``),
not for the runtime data-access layer. This pattern matches the
canonical sibling services (``notification-service``, ``order-service``,
``payment-service``, ``inventory-service``) which all follow the same
"Pydantic row models + psycopg v3 raw SQL" approach.

Schema authority
----------------
- AAP Section 0.4.4 — 4 user-domain tables + ``events_outbox``
- AAP R-6  — Database-per-service (no cross-service FKs; ``user_id`` refs
  in upstream events are opaque UUIDs with no FK constraint to other
  services' databases).
- AAP R-7  — PostgreSQL chosen for relational user data (transactional,
  referential-integrity needs).
- AAP R-9  — Migration scripts under ``../../migrations`` are the DDL
  source of truth; these models mirror them exactly. If a migration
  adds, removes, or retypes a column, this file MUST be updated to
  match — that contract is enforced at runtime by ``extra="forbid"``
  surfacing a ``ValidationError`` on the FIRST DB read.
- AAP R-30 — Events emitted by this service are named ``user.<verb>``
  (``user.registered``, ``user.updated``, ``user.deleted``,
  ``user.profile.updated``, ``user.preferences.updated``,
  ``user.address.added``, etc.) and persisted in ``events_outbox``
  prior to dispatch.
- AAP R-31 — Outbox event payloads are versioned via the ``payload``
  JSONB blob (``schema_version`` field embedded by the event builder).

Field-mapping conventions
-------------------------
* All ``UUID`` columns map to Python ``uuid.UUID``.
* All ``TIMESTAMPTZ`` columns map to ``datetime.datetime`` (TZ-aware
  by repository convention; this layer does NOT enforce TZ-awareness
  to permit naive datetimes in unit tests).
* All ``DATE`` columns map to ``datetime.date``.
* All ``TIME`` columns map to ``datetime.time``.
* All ``JSONB`` columns map to ``dict[str, typing.Any]`` (psycopg v3
  decodes JSONB to dict automatically with the default codec).
* All ``citext`` columns (``users.email``) map to plain ``str``;
  case-insensitive matching is handled by the database column type,
  NOT by the application — the repository never lowercases emails on
  the application side.
* Postgres ``ENUM`` columns (``user_status``, ``profile_gender``,
  ``address_type``) map to ``str`` validated against a tight
  ``typing.Literal[...]`` union — invalid values raise pydantic's
  ``ValidationError`` at construction time.
* Nullable columns map to ``X | None`` (PEP 604 union syntax) with a
  default of ``None`` so callers can construct partial models for tests.
* Timestamp defaulting (``created_at = NOW()``, ``updated_at = NOW()``)
  is handled by SQL — these models reflect them but do NOT compute them
  on the application side.
* Mutable JSON defaults use ``Field(default_factory=dict)`` (NOT
  ``default={}``) to avoid the Python "shared mutable default" footgun.

Cross-references
----------------
* ``src/repository/user_repository.py`` — imports ``UserRow``,
  ``UserProfileRow``, ``UserPreferencesRow`` and constructs them via
  ``Model.model_validate(row)`` from ``psycopg.rows.dict_row`` outputs.
* ``src/repository/address_repository.py`` — imports ``UserAddressRow``
  and uses it as a row projection for ``user_addresses``.
* ``src/repository/outbox_repository.py`` — imports ``OutboxEventRow``
  and uses it as the event envelope shape for the transactional outbox.
* ``src/domain/user.py``, ``src/domain/profile.py``,
  ``src/domain/preferences.py``, ``src/domain/address.py`` — domain
  types that consume row models via ``Model.model_validate(row.model_dump())``
  projections, then enrich with domain validators.
* ``services/user-service/migrations/`` — Alembic migration scripts;
  the DDL in those scripts is the AUTHORITATIVE source.
"""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Final, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# =============================================================================
# Common base class
# =============================================================================


class _RepoBaseModel(BaseModel):
    """Common Pydantic v2 configuration for repository row models.

    All five row models in this module derive from ``_RepoBaseModel`` to
    share a single ``model_config`` and guarantee uniform validation
    semantics across the data-access layer.

    Configuration rationale
    -----------------------
    * ``extra="forbid"`` — Reject unknown columns at validation time.
      This is the safety net for AAP R-9: if a migration adds a column
      but the model layer is not updated, the FIRST DB read raises a
      clear ``pydantic.ValidationError("Extra inputs are not permitted")``
      instead of silently dropping data.
    * ``frozen=False`` — Row models are mutable. We do NOT use them as
      domain values; the domain layer (``src/domain/*``) owns
      immutability via its own ``model_config`` / ``dataclass(frozen=True)``.
    * ``validate_assignment=False`` — Skip per-attribute revalidation
      on mutation. Row models live on the hot read path; the
      construction-time validation is sufficient.
    * ``arbitrary_types_allowed=True`` — Allow ``UUID``, ``datetime``,
      ``date``, ``time`` to flow through without registering custom
      serializers. Pydantic v2 has built-in support for these but the
      flag is set defensively so future stdlib types (e.g. ``timedelta``)
      can be added without re-configuring.
    * ``str_strip_whitespace=False`` — DO NOT silently strip whitespace
      from row data. Doing so could mask DB drift (e.g. a trailing
      newline in an ``external_auth_id`` would be silently normalized
      and never investigated).

    Note: ``from_attributes=True`` is NOT set. We construct row models
    via ``Model.model_validate(dict_row)`` against psycopg v3
    ``dict_row`` results — i.e. plain mappings, not ORM objects.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=False,
        validate_assignment=False,
        arbitrary_types_allowed=True,
        str_strip_whitespace=False,
    )


# =============================================================================
# Type aliases — exported for reuse by domain code without circular imports
# =============================================================================

# Status values for ``users.status`` (Postgres ENUM ``user_status``).
# ``active``       — user is live and usable.
# ``suspended``    — admin-suspended (read-only; cannot log in or modify).
# ``deleted``      — soft-deleted; PII anonymised by the repository layer
#                    on transition. The row stays in place to preserve
#                    referential integrity for ``orders``, ``payments``, etc.
UserStatus = Literal["active", "suspended", "deleted"]


# Gender values for ``user_profiles.gender`` (Postgres ENUM
# ``profile_gender``). The set is intentionally small and inclusive;
# it matches the registration UI's gender picker. NULL is also valid
# (modeled as ``ProfileGender | None``).
ProfileGender = Literal["male", "female", "non_binary", "other", "prefer_not_to_say"]


# Address kind for ``user_addresses.type`` (Postgres ENUM
# ``address_type``). The User Service tracks two kinds; only Order
# Service consumes ``billing`` for invoicing.
AddressType = Literal["shipping", "billing"]


# =============================================================================
# Row models — one per user_db table, mirroring DDL exactly
# =============================================================================


class UserRow(_RepoBaseModel):
    """Mirrors the ``users`` table row.

    Columns (DDL):
        id               UUID PRIMARY KEY
        external_auth_id TEXT NOT NULL UNIQUE
        email            CITEXT NOT NULL UNIQUE
        status           user_status NOT NULL DEFAULT 'active'
                          (ENUM: 'active' | 'suspended' | 'deleted')
        version          BIGINT NOT NULL DEFAULT 1
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        deleted_at       TIMESTAMPTZ NULL  (soft-delete sentinel)

    Indexes (informational; enforced by migrations):
        UNIQUE (external_auth_id)
        UNIQUE (email)                              -- citext, case-insensitive
        partial UNIQUE (id) WHERE deleted_at IS NULL  -- one live user per id

    Concurrency
    -----------
    The ``version`` column drives optimistic concurrency control via the
    repository's ``UPDATE ... WHERE id=:id AND version=:expected``
    pattern; mismatch raises ``OptimisticConcurrencyError``.
    """

    id: UUID
    external_auth_id: str = Field(min_length=1, max_length=255)
    email: str = Field(min_length=3, max_length=320)
    status: UserStatus = "active"
    version: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None


class UserProfileRow(_RepoBaseModel):
    """Mirrors the ``user_profiles`` table row.

    Columns (DDL):
        user_id        UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE
        first_name     TEXT NULL  (anonymized on soft-delete)
        last_name      TEXT NULL  (anonymized on soft-delete)
        display_name   TEXT NULL  (anonymized on soft-delete)
        phone          TEXT NULL  (NULLed on soft-delete)
        avatar_url     TEXT NULL  (NULLed on soft-delete)
        date_of_birth  DATE NULL  (NULLed on soft-delete)
        gender         profile_gender NULL  (ENUM)
        locale         TEXT NULL DEFAULT 'en'  (BCP-47 tag)
        timezone       TEXT NULL DEFAULT 'UTC' (IANA tz)
        updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()

    Privacy
    -------
    On soft-delete (``users.deleted_at`` set), the User Service anonymises
    PII columns (first/last/display name) and NULLs the rest. This row
    model reflects only the storage shape; the anonymization logic lives
    in ``src/services/deletion_service.py``.
    """

    user_id: UUID
    first_name: str | None = None
    last_name: str | None = None
    display_name: str | None = None
    phone: str | None = None
    avatar_url: str | None = None
    date_of_birth: date | None = None
    gender: ProfileGender | None = None
    locale: str | None = None
    timezone: str | None = None
    updated_at: datetime


class UserPreferencesRow(_RepoBaseModel):
    """Mirrors the ``user_preferences`` table row.

    Columns (DDL):
        user_id            UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE
        email_opt_in       BOOLEAN NOT NULL DEFAULT TRUE
        sms_opt_in         BOOLEAN NOT NULL DEFAULT TRUE
        marketing_opt_in   BOOLEAN NOT NULL DEFAULT FALSE
        preferred_language TEXT NOT NULL DEFAULT 'en'  (BCP-47)
        preferred_currency CHAR(3) NOT NULL DEFAULT 'USD' (ISO-4217)
        quiet_hours_start  TIME NULL
        quiet_hours_end    TIME NULL
        channel_overrides  JSONB NOT NULL DEFAULT '{}'::jsonb
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()

    The ``channel_overrides`` JSONB is intentionally typed as a
    free-form ``dict[str, Any]`` at this layer; the domain layer
    (``src/domain/preferences.py``) imposes structure of the form
    ``{channel_name: {<override>}}`` (e.g. ``{"sms": {"enabled": false}}``).

    Defaults
    --------
    Pydantic-side defaults mirror the SQL ``DEFAULT`` clauses so that
    a model constructed without those fields produces the same row
    state the database would produce for an INSERT with omitted
    columns. ``channel_overrides`` uses ``default_factory=dict`` to
    avoid the shared-mutable-default footgun.
    """

    user_id: UUID
    email_opt_in: bool = True
    sms_opt_in: bool = True
    marketing_opt_in: bool = False
    preferred_language: str = Field(default="en", min_length=2, max_length=20)
    preferred_currency: str = Field(default="USD", min_length=3, max_length=3)
    quiet_hours_start: time | None = None
    quiet_hours_end: time | None = None
    channel_overrides: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime


class UserAddressRow(_RepoBaseModel):
    """Mirrors the ``user_addresses`` table row.

    Columns (DDL):
        id           UUID PRIMARY KEY
        user_id      UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE
        type         address_type NOT NULL  (ENUM: 'shipping' | 'billing')
        is_default   BOOLEAN NOT NULL DEFAULT FALSE
        line1        TEXT NOT NULL
        line2        TEXT NULL
        city         TEXT NOT NULL
        state        TEXT NULL
        postal_code  TEXT NOT NULL
        country      CHAR(2) NOT NULL  (ISO-3166-1 alpha-2)
        phone        TEXT NULL
        is_verified  BOOLEAN NOT NULL DEFAULT FALSE
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()

    Indexes (informational):
        INDEX (user_id)
        partial UNIQUE (user_id, type) WHERE is_default = TRUE
            -- "at most one default per type per user"

    Validation guards
    -----------------
    * ``country`` is constrained to exactly 2 characters at the model
      layer, defensively guarding against bad data ever reaching the
      database. The actual ISO-3166-1 membership check lives in
      ``src/domain/validators.py`` (``validate_country``) which uses
      ``pycountry``.
    * ``line1``, ``city``, ``postal_code`` are required NON-EMPTY
      (``min_length=1``) — empty strings would pass the SQL NOT NULL
      check but are nonsensical addresses.
    """

    id: UUID
    user_id: UUID
    type: AddressType
    is_default: bool = False
    line1: str = Field(min_length=1, max_length=255)
    line2: str | None = None
    city: str = Field(min_length=1, max_length=128)
    state: str | None = None
    postal_code: str = Field(min_length=1, max_length=32)
    country: str = Field(min_length=2, max_length=2)
    phone: str | None = None
    is_verified: bool = False
    created_at: datetime
    updated_at: datetime


class OutboxEventRow(_RepoBaseModel):
    """Mirrors the ``events_outbox`` table row.

    Columns (DDL):
        id              UUID PRIMARY KEY
        aggregate_id    UUID NOT NULL
        aggregate_type  TEXT NOT NULL
                          (e.g. 'user', 'user_profile', 'user_address')
        event_type      TEXT NOT NULL
                          (e.g. 'user.registered', 'user.updated',
                           'user.deleted', 'user.profile.updated',
                           'user.preferences.updated',
                           'user.address.added')
        topic           TEXT NOT NULL  (Kafka topic; mirrors event_type
                                        per AAP R-30)
        key             TEXT NOT NULL
                          (Kafka partition key; usually str(user_id))
        payload         JSONB NOT NULL
                          (full event body — versioned per AAP R-31
                           via embedded ``schema_version`` field)
        headers         JSONB NOT NULL DEFAULT '{}'::jsonb
                          (correlation_id, schema_version, source, etc.;
                           AAP R-13)
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        published_at    TIMESTAMPTZ NULL
                          (NULL = pending dispatch; non-NULL = published
                           to Kafka by the outbox dispatcher)
        retry_count     INTEGER NOT NULL DEFAULT 0
        last_error      TEXT NULL  (truncated to 4096 chars by the
                                    OutboxRepository on update)

    Indexes (informational):
        partial INDEX (created_at) WHERE published_at IS NULL
            -- supports OutboxRepository.claim_pending efficiently

    Transactional outbox pattern
    ----------------------------
    Rows are INSERTed in the SAME transaction as the aggregate write
    (e.g. ``UPDATE users SET ...; INSERT INTO events_outbox ...``).
    The outbox dispatcher polls ``WHERE published_at IS NULL ORDER BY
    created_at LIMIT N FOR UPDATE SKIP LOCKED``, publishes to Kafka,
    then UPDATEs ``published_at = NOW()``. On failure it increments
    ``retry_count`` and writes ``last_error``; after a configured
    threshold the row is moved to a DLQ table (handled out of band).
    """

    id: UUID
    aggregate_id: UUID
    aggregate_type: str = Field(min_length=1, max_length=64)
    event_type: str = Field(min_length=1, max_length=128)
    topic: str = Field(min_length=1, max_length=128)
    key: str = Field(min_length=1, max_length=256)
    payload: dict[str, Any]
    headers: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    published_at: datetime | None = None
    retry_count: int = Field(default=0, ge=0)
    last_error: str | None = None


# =============================================================================
# Public surface
# =============================================================================

__all__: Final[list[str]] = [
    # --- Type aliases (alphabetized) ---
    "AddressType",
    "ProfileGender",
    "UserStatus",
    # --- Row models (alphabetized) ---
    "OutboxEventRow",
    "UserAddressRow",
    "UserPreferencesRow",
    "UserProfileRow",
    "UserRow",
]
