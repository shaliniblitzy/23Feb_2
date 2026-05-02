"""create_user_profiles_table

Revision ID: 0002
Revises: 0001
Create Date: 2026-01-01 00:00:02.000000

Bootstrap chain revision 2/5 for user_db.

Creates the user_profiles table -- a 1:1 PII extension of users.

Why split users + user_profiles?

  The users table holds the minimal identity record (auth_id, email,
  status, version, audit) needed for authentication, idempotent
  upsert, and soft-delete operations. The user_profiles table holds
  the higher-density PII (real names, phone, DOB, avatar) which:

    1. Has different access patterns (read-mostly, infrequent writes)
    2. Has different retention semantics (subject to GDPR right-to-
       erasure independent of identity record)
    3. Has different volume profile per user (avatar / display name)

  Splitting them keeps the users table compact for high-frequency
  authentication lookups and isolates PII for targeted anonymization.

Relationship contract::

  user_profiles (1) ---->(1) users
                FK ON DELETE RESTRICT
                UNIQUE (user_id)

  ON DELETE RESTRICT prevents accidental cascading deletes through the
  users table; the application's user-deletion workflow MUST first
  anonymize / delete user_profiles, then delete users.

COPPA / GDPR compliance -- date_of_birth handling:

  date_of_birth DATE NULLABLE

  NULLABLE because:
    * GDPR -- users can decline to provide birth date
    * COPPA -- minimum-age enforcement is application-layer (env var
      USER_DATE_OF_BIRTH_MIN_AGE_YEARS, default 13); ENFORCED before
      INSERT at the service layer, NOT via DB CHECK constraint
      (because the boundary date moves daily and is impossible to
      express as a static check that doesn't drift over time).

References:
  * AAP Section 0.4.4   -- user_db schema specification
  * AAP R-6   -- Database per service
  * AAP R-9   -- Migrations under owning service
  * AAP R-25  -- No secrets in DDL
  * AAP R-26  -- created_at / updated_at mandatory
  * Folder README -- User-Service-Specific Architectural Property #5
    (COPPA/GDPR -- date_of_birth nullable, min-age app-layer)
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, Sequence[str], None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration: create user_profiles (1:1 with users).

    Operation steps:
      1. CREATE TABLE user_profiles with 13 columns (id, user_id,
         first_name, last_name, display_name, phone, avatar_url,
         date_of_birth, gender, locale, timezone, created_at,
         updated_at).
      2. PRIMARY KEY on id named pk_user_profiles -- separate surrogate
         UUID PK rather than reusing user_id as the PK, to keep the
         row identity stable across PII anonymization workflows.
      3. UNIQUE on user_id named uq_user_profiles__user_id -- enforces
         the 1:1 cardinality with users(id).
      4. FOREIGN KEY user_profiles.user_id -> users(id) ON DELETE
         RESTRICT named fk_user_profiles__user_id__users. ON DELETE
         RESTRICT is deliberate (NOT CASCADE) -- the user-deletion
         workflow MUST first anonymize this row before deleting the
         parent users row, ensuring PII redaction is observable and
         auditable rather than silently cascading.

    No CHECK constraints by design:
      * gender is intentionally free-form (no enum CHECK) to support
        inclusive identification.
      * date_of_birth has no min-age CHECK because the boundary date
        moves daily; min-age enforcement is application-layer
        (USER_DATE_OF_BIRTH_MIN_AGE_YEARS env var, default 13).
      * locale, timezone, phone have no format CHECKs because their
        canonical formats (BCP-47, IANA tz, E.164) have edge cases
        and mutate over time; format enforcement is application-layer.

    No additional indexes on phone or display_name -- AAP does not
    specify those access patterns, so adding indexes would be
    premature optimization.
    """
    op.create_table(
        "user_profiles",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
            comment="Profile row primary key (UUID v4 server-generated).",
        ),
        sa.Column(
            "user_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment=(
                "FK to users.id; UNIQUE (1:1 relationship); ON DELETE "
                "RESTRICT (workflow must anonymize profile before user "
                "delete)."
            ),
        ),
        sa.Column(
            "first_name",
            sa.String(length=128),
            nullable=True,
            comment="Given name (NULLABLE; users may provide display_name only).",
        ),
        sa.Column(
            "last_name",
            sa.String(length=128),
            nullable=True,
            comment="Family name (NULLABLE).",
        ),
        sa.Column(
            "display_name",
            sa.String(length=128),
            nullable=True,
            comment=(
                "Public display name (NULLABLE; falls back to first_name "
                "or email-derived handle)."
            ),
        ),
        sa.Column(
            "phone",
            sa.String(length=32),
            nullable=True,
            comment=(
                "Contact phone (E.164 recommended; format enforcement at "
                "app layer; SMS-channel notifications use this)."
            ),
        ),
        sa.Column(
            "avatar_url",
            sa.String(length=512),
            nullable=True,
            comment=(
                "URL to the user's avatar image (NULLABLE). Stored as a "
                "URL -- the User Service does NOT host avatar binaries."
            ),
        ),
        sa.Column(
            "date_of_birth",
            sa.Date(),
            nullable=True,
            comment=(
                "Date of birth (NULLABLE; GDPR users may decline). "
                "COPPA/GDPR min-age enforced at application layer "
                "(USER_DATE_OF_BIRTH_MIN_AGE_YEARS, default 13)."
            ),
        ),
        sa.Column(
            "gender",
            sa.String(length=32),
            nullable=True,
            comment=(
                "Self-described gender (NULLABLE; free-form to support "
                "inclusive identification; no CHECK constraint)."
            ),
        ),
        sa.Column(
            "locale",
            sa.String(length=10),
            nullable=True,
            comment=(
                "BCP 47 locale tag (e.g., 'en', 'en-US', 'pt-BR'); "
                "format enforcement at app layer."
            ),
        ),
        sa.Column(
            "timezone",
            sa.String(length=64),
            nullable=True,
            comment=(
                "IANA timezone name (e.g., 'America/New_York', "
                "'Asia/Kolkata'); used by Notification Service quiet-"
                "hours interpretation."
            ),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="Profile row creation timestamp.",
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="Last-modification timestamp; bumped by application on UPDATE.",
        ),

        # ---- Constraints ----

        sa.PrimaryKeyConstraint("id", name="pk_user_profiles"),
        sa.UniqueConstraint(
            "user_id",
            name="uq_user_profiles__user_id",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_user_profiles__user_id__users",
            ondelete="RESTRICT",
        ),
        comment=(
            "PII extension of users; 1:1 with users.id (ON DELETE "
            "RESTRICT). Subject to GDPR right-to-erasure; deletion "
            "workflow anonymizes this row before deleting the parent "
            "users row."
        ),
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    Drops the user_profiles table; the PK / UNIQUE / FK constraints
    drop with the table.

    Rationale (matches sibling 0001's policy): the parent users table
    and the pgcrypto extension are intentionally left intact -- they
    are the responsibility of revision 0001's downgrade.
    """
    op.drop_table("user_profiles")
