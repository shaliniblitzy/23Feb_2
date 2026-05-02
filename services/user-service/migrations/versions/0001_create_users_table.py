"""create_users_table

Revision ID: 0001
Revises:
Create Date: 2026-01-01 00:00:01.000000

Bootstrap chain HEAD revision (1/5) for user_db.

Performs the foundational setup for the User Service's PostgreSQL
schema:

  1. Enables the `pgcrypto` extension (provides gen_random_uuid()
     used as the server-side default for every UUID primary key in
     this DB).
  2. Creates the `users` table -- the root identity record.
  3. Creates a case-insensitive UNIQUE expression index on LOWER(email).
  4. Creates a partial index for active-user lookups.
  5. Creates a CHECK on the status enum (lowercase domain literals).

Schema rationale:

  * id UUID PK              -- globally unique, server-generated via
                               gen_random_uuid() (pgcrypto).
  * external_auth_id        -- OPAQUE reference to Auth Service
                               (AAP R-6 -- NO FK across DB boundary).
                               UNIQUE so the User Service can perform
                               INSERT ... ON CONFLICT (external_auth_id)
                               DO UPDATE on `user.registered` Kafka
                               events (idempotent upsert).
  * email                   -- Case-insensitive UNIQUE via expression
                               index `LOWER(email)`. NOT NULL.
                               Choosing functional UNIQUE INDEX over
                               the citext extension avoids a second
                               extension dependency.
  * status                  -- Lowercase enum 'active' | 'inactive' |
                               'suspended' | 'deleted'. Coupled with
                               UserStatus StrEnum in domain layer.
  * version                 -- INTEGER NOT NULL DEFAULT 0. Used by
                               application code for optimistic
                               concurrency:
                                 UPDATE users
                                 SET ..., version = version + 1
                                 WHERE id = :id AND version = :expected
                               If 0 rows updated, retry with refreshed
                               version (concurrent-update detected).
  * created_at / updated_at -- TIMESTAMPTZ NOT NULL DEFAULT NOW()
                               (AAP R-26).
  * deleted_at              -- TIMESTAMPTZ NULL. Soft-delete tombstone;
                               application's deletion workflow sets it
                               and concurrently anonymizes PII in
                               user_profiles. Partial index
                               idx_users__active gives O(log N) lookup
                               of "live" users.

References:
  * AAP Section 0.4.4   -- user_db schema specification
  * AAP R-6   -- Database per service (external_auth_id OPAQUE -- NO FK)
  * AAP R-7   -- PostgreSQL polyglot persistence choice
  * AAP R-9   -- Migrations under owning service
  * AAP R-25  -- No secrets in DDL
  * AAP R-26  -- created_at / updated_at mandatory
  * Folder README -- User-Service-Specific Architectural Properties
    (idempotent upsert, optimistic concurrency, soft delete)
  * Sibling: services/inventory-service/migrations/versions/
    0001_create_warehouses.py (canonical pgcrypto-enable HEAD pattern)
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration: enable pgcrypto, create users table,
    add case-insensitive email UNIQUE and active-user partial index."""
    # ----------------------------------------------------------------
    # Step 1: Enable pgcrypto extension.
    # ----------------------------------------------------------------
    # pgcrypto provides gen_random_uuid() which is the server-side
    # default for every UUID primary key in user_db.
    #
    # IF NOT EXISTS makes this idempotent; the extension may already
    # exist if a sibling service in a single-Postgres dev cluster
    # enabled it first.
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # ----------------------------------------------------------------
    # Step 2: Create the users table.
    # ----------------------------------------------------------------
    op.create_table(
        "users",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
            comment=(
                "User primary key (UUID v4 server-generated via "
                "pgcrypto.gen_random_uuid())."
            ),
        ),
        sa.Column(
            "external_auth_id",
            sa.String(length=128),
            nullable=False,
            comment=(
                "OPAQUE reference to Auth Service identity (NO FK across "
                "DB boundary per AAP R-6). UNIQUE -- enables idempotent "
                "upsert on user.registered events: INSERT ... ON "
                "CONFLICT (external_auth_id) DO UPDATE."
            ),
        ),
        sa.Column(
            "email",
            sa.String(length=320),
            nullable=False,
            comment=(
                "User email (RFC 5321 max 320 chars). Case-insensitive "
                "UNIQUE via expression index uq_users__email_lower on "
                "LOWER(email)."
            ),
        ),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'active'"),
            comment=(
                "Account status: 'active' | 'inactive' | 'suspended' | "
                "'deleted' (lowercase). Coupled with UserStatus StrEnum "
                "in domain layer. CHECK ck_users__status_enum enforces "
                "the domain."
            ),
        ),
        sa.Column(
            "version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
            comment=(
                "Optimistic-concurrency version counter; bumped by "
                "application on UPDATE: SET version = version + 1 WHERE "
                "id = :id AND version = :expected. 0 rows updated => "
                "concurrent-update detected; retry."
            ),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="User creation timestamp (immutable post-insert).",
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment=(
                "Last-modification timestamp; bumped by application on "
                "any UPDATE."
            ),
        ),
        sa.Column(
            "deleted_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
            comment=(
                "Soft-delete tombstone. NULL = active; non-NULL = soft-"
                "deleted. Application sets this concurrently with "
                "anonymizing PII in user_profiles. The partial index "
                "idx_users__active accelerates 'live user' lookups."
            ),
        ),

        # ---- Constraints ----

        sa.PrimaryKeyConstraint("id", name="pk_users"),

        # UNIQUE on external_auth_id -- enables idempotent upsert.
        sa.UniqueConstraint(
            "external_auth_id",
            name="uq_users__external_auth_id",
        ),

        # CHECK: status enum (lowercase domain literals).
        sa.CheckConstraint(
            "status IN ('active', 'inactive', 'suspended', 'deleted')",
            name="ck_users__status_enum",
        ),
        comment=(
            "Root identity record for the User Service. One row per "
            "user; 1:1 to user_profiles, 1:1 to user_preferences, 1:N "
            "to user_addresses. Soft-deleted via deleted_at; PII "
            "anonymized concurrently via application logic."
        ),
    )

    # ----------------------------------------------------------------
    # Step 3: Case-insensitive UNIQUE on email via LOWER(email)
    # ----------------------------------------------------------------
    # Implements case-insensitive email uniqueness without requiring
    # the citext extension (folder spec: avoid additional extension
    # dependency). The functional unique index treats 'Alice@Ex.com'
    # and 'alice@ex.com' as the same email.
    op.execute(
        "CREATE UNIQUE INDEX uq_users__email_lower "
        "ON users (LOWER(email))"
    )

    # ----------------------------------------------------------------
    # Step 4: Partial index for active-user lookups.
    # ----------------------------------------------------------------
    # The hot-path query is "find the live user with id = X" or "list
    # live users where ...". Without a partial index, every query must
    # filter `WHERE deleted_at IS NULL` against a full table scan or
    # the PK index. The partial index restricts to live rows so its
    # size scales with the active user population (not all-time users).
    op.execute(
        "CREATE INDEX idx_users__active "
        "ON users (deleted_at) "
        "WHERE deleted_at IS NULL"
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    Drops the partial index, the email UNIQUE expression index, and the
    users table. The PK / UNIQUE / CHECK constraints drop with the table.

    DOES NOT drop the pgcrypto extension -- sibling services in the same
    cluster may depend on it; DROP EXTENSION fails when dependent
    objects exist anyway. Per folder README, pgcrypto is intentionally
    kept on downgrade.
    """
    op.execute("DROP INDEX IF EXISTS idx_users__active")
    op.execute("DROP INDEX IF EXISTS uq_users__email_lower")
    op.drop_table("users")
    # NOTE: pgcrypto extension is intentionally NOT dropped here -- see
    # folder README "Conventions Inside Each Revision File":
    # "the `pgcrypto` extension is intentionally NOT dropped on
    # downgrade because it may be shared with sibling services on a
    # single-Postgres dev cluster."
