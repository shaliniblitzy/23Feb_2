"""create_order_status_history

Revision ID: 0003
Revises: 0002
Create Date: 2026-01-01 00:00:03.000000

Create order_status_history table -- append-only audit log of order status
transitions.

Each row records exactly one observed status transition for an order:
  * Initial creation: from_status IS NULL, to_status = 'CREATED'.
  * Subsequent transitions: from_status = previous status,
    to_status = new status.
  * Compensation transitions: to_status IN
    ('COMPENSATING_INVENTORY','COMPENSATING_PAYMENT','CANCELLED').

Append-only invariant: rows are INSERTed by the Order Service status
transition handler and are NEVER updated or deleted. Garbage collection
occurs only via ON DELETE CASCADE when the parent order is removed.

This table powers:
  * Customer support timelines ("when did my order ship?").
  * Saga forensics ("did this order ever reach PAYMENT_TAKEN before
    failing?").
  * Compliance and audit reporting.

Schema highlights:
  * id is BIGSERIAL -- append-volume can be high, so 64-bit auto-incrementing
    integer outperforms UUID generation for inserts and is more compact for
    indexing the time-ordered scan path.
  * from_status is nullable -- the initial CREATED transition has no prior
    state.
  * Composite index (order_id, occurred_at) supports the dominant query
    pattern: 'fetch the full status timeline for one order in chronological
    order'.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: Union[str, Sequence[str], None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration: create order_status_history table and index.

    Operation steps:
      1. CREATE TABLE order_status_history with 7 columns (id, order_id,
         from_status, to_status, reason, correlation_id, occurred_at).
      2. PRIMARY KEY on id (declared inline via primary_key=True; the
         constraint name defaults to order_status_history_pkey -- the
         standard PostgreSQL convention for single-column auto-increment
         PKs). Unlike saga_state's composite PK (which uses the explicit
         sa.PrimaryKeyConstraint(...) form to name it), this is the
         simplest idiom for a single-column auto-increment PK.
      3. FOREIGN KEY order_status_history.order_id -> orders(id)
         ON DELETE CASCADE, named fk_order_status_history__order_id__orders.
         This is the ONLY foreign key permitted on this table per AAP R-6
         because both tables live in order_db. Cascade ensures order
         deletion garbage-collects all of its history rows in one
         statement.
      4. CREATE INDEX idx_order_status_history__order_time on
         (order_id, occurred_at) supporting the dominant query pattern
         'fetch the full status timeline for one order in chronological
         order' used by customer support UIs and saga forensics tooling.

    Why no CHECK constraint on from_status / to_status?
      The audit log is intentionally lenient against future evolution of
      the orders.status enum. If a forward revision ever extends the
      orders.status CHECK (e.g., to add 'PARTIALLY_FULFILLED' or
      'BACKORDERED'), the audit log MUST be able to record those new
      states without requiring a paired migration here. The orders table
      itself (revision 0001) is the canonical enum guard via
      ck_orders__status_enum; the audit log mirrors what was applied
      without re-enforcing.

    Why BIGSERIAL (sa.BigInteger() + autoincrement) and not UUID for id?
      Audit volume can be high (multiple transitions per order x many
      orders). INT4 wraps at ~2.1B rows, so we use INT8. UUID would also
      work but is wider (16 bytes vs 8 bytes) and has no natural insert
      ordering (UUID v4 is random), which hurts B-tree index locality on
      time-ordered scans. UUID v7 (time-ordered) would be a candidate but
      is not standard in PostgreSQL 16.
    """
    # ------------------------------------------------------------------
    # CREATE TABLE order_status_history.
    #
    # 7 columns per the folder spec for services/order-service/
    # migrations/versions/. All constraints declared inline so they
    # become part of the same DDL statement as the table create:
    #   - PRIMARY KEY on id (auto-named order_status_history_pkey via
    #     the inline primary_key=True; PostgreSQL convention).
    #   - FOREIGN KEY (order_id) -> orders(id) ON DELETE CASCADE
    #     named fk_order_status_history__order_id__orders.
    # No CHECK constraints on from_status/to_status -- see docstring
    # rationale above.
    # ------------------------------------------------------------------
    op.create_table(
        "order_status_history",
        # 64-bit auto-incrementing PK. BIGSERIAL because append volume can
        # be high (multiple transitions per order x many orders) and INT4
        # wraps at ~2.1B rows. UUID would also work but is wider and not
        # needed for natural ordering of audit rows.
        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
            nullable=False,
            comment="Auto-incrementing audit log id.",
        ),
        # FK to orders.id. NOT NULL -- every history row belongs to
        # exactly one order. ON DELETE CASCADE so order deletion
        # garbage-collects its history rows.
        sa.Column(
            "order_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment="Order this status transition belongs to.",
        ),
        # Previous status. NULL on the initial CREATED transition (which
        # has no prior state). Stored as Text + enforced by application
        # logic rather than a CHECK constraint to keep the audit log
        # lenient against future status enum evolution (forward
        # compatibility).
        sa.Column(
            "from_status",
            sa.Text(),
            nullable=True,
            comment="Previous status; NULL on initial CREATED transition.",
        ),
        # New status. NOT NULL -- every transition lands somewhere.
        sa.Column(
            "to_status",
            sa.Text(),
            nullable=False,
            comment="New status after this transition.",
        ),
        # Optional human-readable reason (e.g., 'inventory unavailable',
        # 'payment captured', 'customer cancellation'). NULL when no
        # explanatory context is available.
        sa.Column(
            "reason",
            sa.Text(),
            nullable=True,
            comment="Human-readable reason for the transition (NULL if none).",
        ),
        # Distributed tracing correlation id (AAP R-13). Nullable for
        # legacy or system-generated transitions without an originating
        # request.
        sa.Column(
            "correlation_id",
            sa.UUID(as_uuid=True),
            nullable=True,
            comment="Distributed-tracing correlation id (AAP R-13).",
        ),
        # Server-generated timestamp of the transition. NOT NULL with
        # NOW() default -- the application MAY set this explicitly (e.g.,
        # to backfill historical events), but the default ensures the
        # column is never empty.
        sa.Column(
            "occurred_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="Wall-clock timestamp of the status transition.",
        ),
        # ---- Constraints ----
        # FK to parent order with CASCADE for garbage collection.
        # AAP R-6: this is the ONLY foreign key permitted on this table
        # because orders(id) lives in the same order_db. Cross-DB FKs
        # (e.g., to user_db.users for an actor reference) are forbidden
        # -- if such references are ever needed they MUST be opaque
        # UUIDs validated at the application layer.
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            ondelete="CASCADE",
            name="fk_order_status_history__order_id__orders",
        ),
        comment=(
            "Append-only audit log of order status transitions. "
            "Rows are INSERTed only; never updated or deleted."
        ),
    )

    # ------------------------------------------------------------------
    # CREATE INDEX idx_order_status_history__order_time.
    #
    # Composite index supporting the dominant query pattern:
    # "fetch the full status timeline for one order in chronological
    # order" (e.g., customer-support UIs and saga forensics tooling).
    #
    # No DESC qualifier is added -- natural ascending order on
    # occurred_at is correct for chronological timelines (oldest to
    # newest). PostgreSQL B-tree indexes serve both directions
    # efficiently for range scans, so customer-support UIs that display
    # newest-first can use the same index without a second
    # (order_id, occurred_at DESC) covering index.
    #
    # op.create_index(...) is appropriate here because the index is a
    # regular (non-partial, non-DESC) B-tree index. Raw op.execute(...)
    # is reserved for partial indexes (per the sibling notification-
    # service convention) and DESC indexes (per revision 0001's
    # idx_orders__user_created).
    # ------------------------------------------------------------------
    op.create_index(
        "idx_order_status_history__order_time",
        "order_status_history",
        ["order_id", "occurred_at"],
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    Drops the composite index then the order_status_history table. The
    PK and FK constraints drop implicitly with the table -- no explicit
    constraint drops are needed.

    Why drop the index first?
      Symmetry with upgrade() (table created first, then index) and
      defensive: dropping the table also drops the index, but explicitly
      dropping the index first makes the intent clear and matches the
      sibling pattern. op.drop_index(...) requires the table_name=
      keyword per the SQLAlchemy/Alembic API.

    Use ONLY in local dev and CI round-trip tests; never invoke in
    production per the forward-only policy in ../README.md (AAP R-9).
    """
    # Drop the composite index BEFORE the table for symmetry with
    # upgrade() and to make the rollback intent explicit. table_name=
    # keyword is REQUIRED by the Alembic/SQLAlchemy API for op.drop_index.
    op.drop_index(
        "idx_order_status_history__order_time",
        table_name="order_status_history",
    )
    # op.drop_table cascades to the PK and FK constraints; no explicit
    # constraint drops are needed.
    op.drop_table("order_status_history")
