"""indexes_and_constraints

Revision ID: 0006
Revises: 0005
Create Date: 2026-01-01 00:00:05.000000

Adds performance-critical indexes and supplementary constraints to tables
created in revisions 0001-0004.

This is the **terminal (head) revision** of the bootstrap Alembic chain.
After this revision applies, the Inventory Service schema is fully
production-ready: every hot-path query has a supporting index, and the
append-only contract on stock_movements is hardened by REVOKEing UPDATE
and DELETE from the application database role (defense-in-depth on top of
the repository-layer enforcement).

Indexes created:

  1. idx_stock_items__product_warehouse  (composite)
     Supports the hottest read path of the Inventory Service: looking up
     stock by (product_id, warehouse_id). Note this is partially subsumed
     by the uq_stock_items__product_warehouse UNIQUE constraint's
     auto-created index, but PostgreSQL only uses the unique-constraint
     index when both columns are queried; we still create the explicit
     index because it makes EXPLAIN plans more predictable across
     PostgreSQL versions and isolates the index from any future relaxation
     of the uniqueness constraint. Created with IF NOT EXISTS to handle
     the implicit-index overlap gracefully.

  2. idx_reservations__expiry  (PARTIAL -- KEY for AAP R-20)
     The single most performance-critical index in the Inventory Service.
     Powers the reservation expiration scheduler's hot-path query:

         SELECT id FROM reservations
         WHERE status = 'ACTIVE' AND expires_at < NOW()
         LIMIT $batch_size

     A naive btree on (status, expires_at) would index every reservation
     ever created, including RELEASED, FULFILLED, and EXPIRED rows that
     the scheduler never queries. The partial index excludes those rows:

         CREATE INDEX idx_reservations__expiry
         ON reservations (expires_at)
         WHERE status = 'ACTIVE';

     This means index size is bounded by the number of currently active
     reservations (typically minutes to hours of pending checkouts) rather
     than by the total reservation history. The scheduler's query
     latency is O(log K) where K is active-reservation count, instead of
     O(log N) where N grows unboundedly with total reservations.

  3. idx_stock_movements__product_time  (composite, occurred_at DESC)
     Supports per-product audit traversal: "show me the last 100 movements
     against product X". DESC ordering on occurred_at means the index can
     serve LIMIT queries without an additional sort. Implemented via raw
     SQL (op.execute) because op.create_index does not support per-column
     ordering directly across all SQLAlchemy versions.

  4. idx_stock_movements__order  (single-column on order_id)
     Supports saga-troubleshooting queries: "show me every movement that
     touched order X". order_id is nullable (replenishments have no
     order_id), so the index is sparse -- but PostgreSQL btree indexes
     always exclude NULLs from index scans on equality queries, which is
     exactly the behavior we want.

  5. idx_stock_movements__time  (single-column on occurred_at)
     Supports time-windowed audit queries: "all movements between T1 and
     T2". This is also the candidate ordering column for future
     occurred_at-range partitioning of stock_movements at production
     scale.

Constraints / privileges:

  6. REVOKE UPDATE, DELETE ON stock_movements FROM <application_role>
     Defense-in-depth append-only enforcement. The repository layer
     already enforces no UPDATE/DELETE at the application level (the
     code path doesn't exist); REVOKE makes accidental DML at the SQL
     level impossible by privileged accident. The REVOKE is guarded:
     if the role does not exist (e.g., in unit-test sqlite or against
     a different role-naming scheme), the REVOKE is silently skipped.

References:
    * AAP R-20 -- reservation expiration scheduler (the partial index is
                  the canonical implementation lever)
    * Parent folder README.md -- Index specification verbatim
    * Revisions 0001-0004 -- All target tables exist before this revision
"""

import os
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa  # noqa: F401  # imported for canonical Alembic template parity

# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: Union[str, Sequence[str], None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# ---------------------------------------------------------------------------
# Optional defense-in-depth REVOKE for stock_movements append-only contract.
#
# The application database role's name is environment-specific; we read it
# from INVENTORY_DB_APP_ROLE. If unset, the REVOKE is skipped -- the
# repository layer's runtime enforcement is sufficient on its own (parent
# README.md classifies this REVOKE as "optional").
# ---------------------------------------------------------------------------
_APP_ROLE_ENV_VAR = "INVENTORY_DB_APP_ROLE"


def upgrade() -> None:
    """Apply forward migration: create performance indexes + optional REVOKE.

    All operations are idempotent (CREATE INDEX IF NOT EXISTS, guarded
    REVOKE) so the revision is safely re-runnable after partial failure.
    """
    # -----------------------------------------------------------------------
    # 1. Composite index on stock_items(product_id, warehouse_id).
    #
    # IF NOT EXISTS is used because the uq_stock_items__product_warehouse
    # UNIQUE constraint from revision 0002 already creates an implicit btree
    # index on (product_id, warehouse_id). On modern PostgreSQL the unique
    # index is auto-named (e.g. "uq_stock_items__product_warehouse") and is
    # distinct from idx_stock_items__product_warehouse, so this index will
    # be created. We use IF NOT EXISTS as a defensive measure in case a
    # future revision changes the naming convention.
    # -----------------------------------------------------------------------
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_items__product_warehouse "
        "ON stock_items (product_id, warehouse_id)"
    )

    # -----------------------------------------------------------------------
    # 2. PARTIAL INDEX on reservations(expires_at) WHERE status = 'ACTIVE'.
    #
    # *** KEY INDEX FOR AAP R-20 ***
    #
    # This is the most performance-critical index in the entire Inventory
    # Service. The reservation expiration scheduler runs every N seconds
    # (default 60s) and issues:
    #
    #     SELECT id FROM reservations
    #     WHERE status = 'ACTIVE' AND expires_at < NOW()
    #     LIMIT $batch_size
    #
    # Without this partial index, the scheduler scans the full reservations
    # table on every tick -- O(N) where N is the total reservation history.
    # With this index, the scan is bounded by K = number of currently active
    # reservations, which is intrinsically small (= count of in-flight
    # checkouts at any moment), giving the scheduler O(log K) latency.
    #
    # Implemented via raw SQL because op.create_index() does not support
    # PARTIAL indexes (the WHERE clause) across all SQLAlchemy versions
    # consistently. The IF NOT EXISTS makes the operation idempotent.
    # -----------------------------------------------------------------------
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_reservations__expiry "
        "ON reservations (expires_at) "
        "WHERE status = 'ACTIVE'"
    )

    # -----------------------------------------------------------------------
    # 3. Composite index on stock_movements(product_id, occurred_at DESC).
    #
    # Per-product audit traversal -- "last 100 movements for product X".
    # DESC ordering allows LIMIT queries to use the index without an
    # additional sort. Raw SQL is required for the DESC clause; op.create_index
    # does not consistently support per-column ordering across all
    # SQLAlchemy versions.
    # -----------------------------------------------------------------------
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_movements__product_time "
        "ON stock_movements (product_id, occurred_at DESC)"
    )

    # -----------------------------------------------------------------------
    # 4. Single-column index on stock_movements(order_id).
    #
    # Saga-troubleshooting query: "every movement related to order X".
    # order_id is nullable, but btree on equality omits NULLs naturally.
    # -----------------------------------------------------------------------
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_movements__order "
        "ON stock_movements (order_id)"
    )

    # -----------------------------------------------------------------------
    # 5. Single-column index on stock_movements(occurred_at).
    #
    # Time-window audit queries; also the candidate partition key for
    # future occurred_at-range partitioning at production scale.
    # -----------------------------------------------------------------------
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_movements__time "
        "ON stock_movements (occurred_at)"
    )

    # -----------------------------------------------------------------------
    # 6. Optional defense-in-depth REVOKE on stock_movements append-only.
    #
    # The repository-layer code path that would issue UPDATE or DELETE
    # against stock_movements does not exist (per AAP R-13 audit-trail
    # integrity). This REVOKE makes accidental DML impossible at the SQL
    # level. Guarded: if INVENTORY_DB_APP_ROLE is unset or the role doesn't
    # exist, the REVOKE is skipped silently.
    # -----------------------------------------------------------------------
    app_role = os.environ.get(_APP_ROLE_ENV_VAR, "").strip()
    if app_role:
        # Use a DO block so a missing role raises a NOTICE instead of an
        # error, keeping this defense-in-depth step optional in environments
        # where the role naming convention differs.
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{app_role}') THEN
                    EXECUTE 'REVOKE UPDATE, DELETE ON stock_movements FROM "{app_role}"';
                    RAISE NOTICE 'Revoked UPDATE, DELETE on stock_movements from %', '{app_role}';
                ELSE
                    RAISE NOTICE 'Role % does not exist; skipping REVOKE on stock_movements.', '{app_role}';
                END IF;
            END
            $$;
            """
        )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    Drops the indexes created in upgrade() and re-grants UPDATE/DELETE on
    stock_movements to the application role if the role and env var are set.

    Idempotency: every DROP uses IF EXISTS; the GRANT is guarded by the
    same role-exists check used in upgrade().
    """
    # Re-grant UPDATE, DELETE on stock_movements (mirror of REVOKE in upgrade)
    app_role = os.environ.get(_APP_ROLE_ENV_VAR, "").strip()
    if app_role:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{app_role}') THEN
                    EXECUTE 'GRANT UPDATE, DELETE ON stock_movements TO "{app_role}"';
                END IF;
            END
            $$;
            """
        )

    # Drop indexes in reverse creation order
    op.execute("DROP INDEX IF EXISTS idx_stock_movements__time")
    op.execute("DROP INDEX IF EXISTS idx_stock_movements__order")
    op.execute("DROP INDEX IF EXISTS idx_stock_movements__product_time")
    op.execute("DROP INDEX IF EXISTS idx_reservations__expiry")
    op.execute("DROP INDEX IF EXISTS idx_stock_items__product_warehouse")
