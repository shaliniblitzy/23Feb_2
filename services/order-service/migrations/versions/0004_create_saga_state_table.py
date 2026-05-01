"""create_saga_state_table

Revision ID: 0004
Revises: 0003
Create Date: 2026-01-01 00:00:04.000000

Create saga_state table -- the LINCHPIN for AAP R-18 saga recovery.

This table is the durable backing store that enables the Order Service to
resume in-flight checkout sagas across Pod restarts, crashes, and Kafka
consumer rebalances. Without this table, the Order Service cannot recover
from any mid-saga failure, and AAP R-18 (saga pattern with explicit
compensation steps) is unrealizable.

Saga lifecycle stored here::

  CREATE_ORDER -> AWAIT_INVENTORY -> AWAIT_PAYMENT -> CONFIRM_ORDER -> TERMINATED
                                                  \\
                                                   -> COMPENSATE_PAYMENT
                                                   -> COMPENSATE_INVENTORY -> TERMINATED

Each row is the durable checkpoint of one in-flight saga, keyed 1:1 to
an orders(id). On Pod startup, the saga scheduler scans this table for
incomplete sagas (current_step <> 'TERMINATED' AND deadline_at < now())
and resumes them; on event arrival via Kafka, the consumer correlates
the event back to the saga via awaiting_event matching.

Schema highlights:
  * order_id is BOTH the PRIMARY KEY and a FOREIGN KEY to orders(id)
    with ON DELETE CASCADE -- at most one saga per order, garbage-
    collected when the parent order is deleted.
  * saga_id is a UNIQUE secondary identifier used in event payloads
    so consumers can correlate events back to the saga without
    knowing the order_id.
  * current_step uses an inline CHECK constraint enumerating the 7
    valid saga states (per folder spec; matches the StrEnum in
    services/order-service/src/orderservice/domain/saga.py).
  * Two PARTIAL INDEXES exclude TERMINATED sagas from scheduler scans,
    keeping scans cheap as historical sagas accumulate.

Why partial indexes?
  Production order_db will accumulate millions of historical sagas
  over the platform's lifetime. The vast majority of those rows will
  be in the TERMINATED state, of no interest to either the scheduler
  (which only resumes incomplete sagas) or the Kafka correlation path
  (which only matches sagas currently blocked on an awaiting_event).
  A regular B-tree index would grow linearly with history and bloat
  scheduler scans. Partial indexes with WHERE current_step <>
  'TERMINATED' (for deadline polling) and WHERE awaiting_event IS NOT
  NULL (for event correlation) keep the index footprint bounded by
  the count of in-flight sagas, not by lifetime cumulative count.

AAP rule mapping:
  * AAP R-6: the ONLY foreign key is intra-DB (saga_state.order_id ->
    orders.id, both inside order_db). NO cross-service FKs to
    payment_db.payments or inventory_db.inventory_reservations --
    saga coordination is event-driven via Kafka, not via cross-DB
    referential integrity.
  * AAP R-13: correlation_id column propagates the distributed-tracing
    id from the originating client request through every saga step.
  * AAP R-18: this revision is the durability backbone of the saga
    pattern. Without it, Pod restart loses all in-flight saga state
    and the saga contract is unrealizable.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, Sequence[str], None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration: create saga_state table and partial indexes.

    Operation steps:
      1. CREATE TABLE saga_state with 10 columns (order_id, saga_id,
         current_step, awaiting_event, retry_count, deadline_at,
         compensation_required, last_error, correlation_id, updated_at).
      2. PRIMARY KEY on order_id named pk_saga_state -- deliberate 1:1
         cardinality with orders (no separate surrogate UUID).
      3. UNIQUE on saga_id named uq_saga_state__saga_id -- the secondary
         identifier embedded in event payloads.
      4. FOREIGN KEY saga_state.order_id -> orders(id) ON DELETE CASCADE
         named fk_saga_state__order_id__orders. This is the ONLY FK on
         this table per AAP R-6 (intra-DB only). NO FK to payment_db.payments
         or inventory_db.inventory_reservations -- those are cross-service
         and forbidden.
      5. CHECK ck_saga_state__current_step_enum enforcing the 7-value
         saga step enum (UPPERCASE, matches domain.saga.SagaStep StrEnum).
      6. CHECK ck_saga_state__retry_count_nonneg enforcing retry_count >= 0.
      7. CREATE INDEX idx_saga_state__deadline (deadline_at) WHERE
         current_step <> 'TERMINATED' -- partial index supporting the
         scheduler's "WHERE deadline_at < now() AND current_step <>
         'TERMINATED'" polling query without indexing terminated sagas.
      8. CREATE INDEX idx_saga_state__awaiting (awaiting_event) WHERE
         awaiting_event IS NOT NULL -- partial index supporting the Kafka
         consumer's event-to-saga correlation lookup.

    Why raw SQL for partial indexes (op.execute) instead of
    op.create_index(postgresql_where=...)?
      Project-standard convention established in
      services/notification-service/migrations/versions/
      20260101_000002_add_indexes.py uses raw SQL for partial indexes.
      This idiom (a) renders identical SQL irrespective of SQLAlchemy
      version, (b) avoids the SQLAlchemy/Alembic/PostgreSQL three-way
      compatibility surface around the postgresql_where parameter, and
      (c) keeps the WHERE predicate visible verbatim in the diff.

    Why is order_id BOTH the PK and the FK?
      Deliberate 1:1 cardinality with orders -- each order has at most
      one saga and a saga is meaningless without its order. Using
      order_id as the natural primary key (rather than a separate
      surrogate saga_state.id UUID) is more compact, simplifies joins,
      and makes the ON DELETE CASCADE garbage collection unambiguous.
      The saga_id column is a separate UNIQUE identifier embedded in
      event payloads so consumers can correlate without knowing the
      order_id.
    """
    # ------------------------------------------------------------------
    # Step 1: CREATE TABLE saga_state.
    #
    # 10 columns per the folder spec for services/order-service/
    # migrations/versions/. All constraints declared inline so they
    # become part of the same DDL statement as the table create:
    #   - PRIMARY KEY on order_id named pk_saga_state.
    #   - UNIQUE on saga_id named uq_saga_state__saga_id.
    #   - FOREIGN KEY (order_id) -> orders(id) ON DELETE CASCADE
    #     named fk_saga_state__order_id__orders.
    #   - CHECK current_step IN (...7 saga states...)
    #     named ck_saga_state__current_step_enum.
    #   - CHECK retry_count >= 0
    #     named ck_saga_state__retry_count_nonneg.
    #
    # Each column carries a comment= mirrored to PostgreSQL via
    # COMMENT ON COLUMN per the sibling notification-service convention,
    # so operators inspecting the table via `\d+ saga_state` see the
    # role of each column.
    # ------------------------------------------------------------------
    op.create_table(
        "saga_state",
        # PRIMARY KEY + FK to orders.id with ON DELETE CASCADE.
        # 1:1 cardinality with orders -- at most one saga per order.
        # Application supplies this value (= orders.id) at saga start.
        sa.Column(
            "order_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment="Order this saga coordinates; PK and FK to orders(id).",
        ),
        # UNIQUE secondary identifier embedded in event payloads so
        # consumers can correlate events without knowing order_id.
        # Generated by the service at saga start (uuid4()), not by the
        # database -- keeps event payloads stable across restarts.
        sa.Column(
            "saga_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment="Unique saga identifier propagated in event payloads.",
        ),
        # Current step in the saga state machine. Enforced by inline
        # CHECK constraint (ck_saga_state__current_step_enum) below.
        # The 7 UPPERCASE values match domain.saga.SagaStep StrEnum
        # exactly -- drift between database CHECK and application enum
        # would cause runtime errors on insert.
        sa.Column(
            "current_step",
            sa.Text(),
            nullable=False,
            comment=(
                "Current saga step; one of CREATE_ORDER, AWAIT_INVENTORY, "
                "AWAIT_PAYMENT, CONFIRM_ORDER, COMPENSATE_INVENTORY, "
                "COMPENSATE_PAYMENT, TERMINATED."
            ),
        ),
        # Optional: name of the Kafka event the saga is currently
        # awaiting (e.g., 'inventory.reserved', 'payment.succeeded').
        # NULL when the saga is in a synchronous transition step (no
        # event blocks it). The idx_saga_state__awaiting partial index
        # excludes NULL rows so the Kafka correlator's lookup remains
        # cheap as historical sagas accumulate.
        sa.Column(
            "awaiting_event",
            sa.Text(),
            nullable=True,
            comment=(
                "Kafka event name the saga is awaiting (NULL if not blocked)."
            ),
        ),
        # Number of retries attempted at the current step. Used by the
        # saga scheduler to enforce max-retry policy before transitioning
        # to COMPENSATE_*. Server default 0 covers freshly-inserted rows;
        # application increments via UPDATE on each retry. CHECK
        # constraint (ck_saga_state__retry_count_nonneg) guards against
        # negative values.
        sa.Column(
            "retry_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
            comment="Retries attempted at the current step.",
        ),
        # Wall-clock deadline by which the current step must complete.
        # The saga scheduler scans
        #   WHERE deadline_at < now() AND current_step <> 'TERMINATED'
        # periodically to identify stalled sagas needing retry or
        # compensation. NO server default -- the application MUST
        # compute the deadline based on per-step SLAs (network call
        # SLAs differ from "await Kafka event" SLAs); a generic database
        # default would be misleading.
        sa.Column(
            "deadline_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            comment="Wall-clock deadline for the current step.",
        ),
        # Flag set by the orchestrator when a downstream failure mandates
        # compensation; consumed by the recovery path on resume.
        # Server default FALSE so freshly-inserted (forward-progressing)
        # sagas start without compensation pending; the orchestrator
        # flips this to TRUE before transitioning to COMPENSATE_*.
        sa.Column(
            "compensation_required",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("FALSE"),
            comment="True if downstream failure requires compensation chain.",
        ),
        # Last error message captured at the current step (for debugging
        # and retry decisions). NULL when no error has occurred at this
        # step -- happy-path sagas never set it. Stored as Text rather
        # than a structured JSON column because the message format
        # varies per failure mode (HTTP status, Kafka error, validation
        # failure) and a free-form text column accommodates all of them.
        sa.Column(
            "last_error",
            sa.Text(),
            nullable=True,
            comment="Last error message captured at this step (NULL if none).",
        ),
        # Distributed-tracing correlation id (AAP R-13). Nullable for
        # legacy or pre-instrumentation sagas; new sagas always populate
        # it (the value originates at the API Gateway and propagates
        # through every step's events and HTTP calls).
        sa.Column(
            "correlation_id",
            sa.UUID(as_uuid=True),
            nullable=True,
            comment="Distributed-tracing correlation id (AAP R-13).",
        ),
        # Server-managed update timestamp. Defaults to NOW() at INSERT.
        # Application MUST update this on every state transition (handled
        # at the ORM layer via the onupdate hook on the SQLAlchemy
        # Column) -- the database does not auto-bump it on UPDATE.
        # This column powers operator dashboards ("which sagas have not
        # progressed in 5+ minutes?") and supports debugging.
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="Updated automatically on every state transition.",
        ),
        # ---- Constraints ----
        # PK: order_id is the natural 1:1 key with orders. No separate
        # surrogate UUID -- the order_id IS the saga's identity.
        sa.PrimaryKeyConstraint("order_id", name="pk_saga_state"),
        # UNIQUE saga_id: secondary identifier embedded in event
        # payloads. UUID v4 collisions are astronomically unlikely but
        # the UNIQUE constraint guarantees correctness even under
        # adversarial (or buggy) input.
        sa.UniqueConstraint("saga_id", name="uq_saga_state__saga_id"),
        # FK: order_id -> orders(id) ON DELETE CASCADE. AAP R-6: this
        # is the ONLY foreign key on this table because orders(id)
        # lives in the same order_db. NO FK to payment_db.payments or
        # inventory_db.inventory_reservations -- those are cross-service
        # references handled via opaque UUIDs and event correlation.
        # CASCADE ensures order deletion garbage-collects its saga_state
        # row in one statement (avoiding orphaned saga rows).
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            ondelete="CASCADE",
            name="fk_saga_state__order_id__orders",
        ),
        # CHECK: current_step is one of the 7 enum values. UPPERCASE
        # matches domain.saga.SagaStep StrEnum exactly. Drift between
        # database CHECK and application enum will cause runtime errors
        # on insert -- KEEP THESE IN SYNC. Forward revisions that add a
        # new saga step MUST relax this CHECK in the same revision.
        sa.CheckConstraint(
            "current_step IN ("
            "'CREATE_ORDER',"
            "'AWAIT_INVENTORY',"
            "'AWAIT_PAYMENT',"
            "'CONFIRM_ORDER',"
            "'COMPENSATE_INVENTORY',"
            "'COMPENSATE_PAYMENT',"
            "'TERMINATED'"
            ")",
            name="ck_saga_state__current_step_enum",
        ),
        # CHECK: retry_count is non-negative. Defensive guard against
        # buggy decrement operations or migration-time data import
        # errors; the server default 0 means freshly-inserted rows
        # always satisfy this.
        sa.CheckConstraint(
            "retry_count >= 0",
            name="ck_saga_state__retry_count_nonneg",
        ),
        comment=(
            "Durable saga coordinator state per order -- AAP R-18 linchpin."
        ),
    )

    # ------------------------------------------------------------------
    # Step 2: CREATE INDEX idx_saga_state__deadline (partial).
    #
    # Partial index on deadline_at: the saga scheduler scans
    #   WHERE deadline_at < now() AND current_step <> 'TERMINATED'
    # periodically to identify stalled sagas needing retry or
    # compensation. Excluding TERMINATED sagas from the index keeps
    # scan cost bounded by the count of in-flight sagas (typically
    # thousands at peak), not by the cumulative count of all sagas
    # ever created (potentially millions over the platform lifetime).
    #
    # Why raw op.execute(...)?
    #   - Project-standard idiom established in
    #     services/notification-service/migrations/versions/
    #     20260101_000002_add_indexes.py for partial indexes.
    #   - Renders identical SQL irrespective of SQLAlchemy version.
    #   - Avoids the SQLAlchemy/Alembic/PostgreSQL three-way
    #     compatibility surface around the postgresql_where parameter.
    #   - Keeps the WHERE predicate visible verbatim in the diff,
    #     making the partial-index intent unambiguous.
    # ------------------------------------------------------------------
    op.execute(
        "CREATE INDEX idx_saga_state__deadline "
        "ON saga_state (deadline_at) "
        "WHERE current_step <> 'TERMINATED'"
    )

    # ------------------------------------------------------------------
    # Step 3: CREATE INDEX idx_saga_state__awaiting (partial).
    #
    # Partial index on awaiting_event: Kafka consumers correlate inbound
    # events to the awaiting saga via this column. Specifically, on
    # receipt of e.g. 'inventory.reserved', the consumer queries
    #   SELECT order_id FROM saga_state
    #   WHERE awaiting_event = 'inventory.reserved'
    # to find the saga(s) blocked on that event. Excluding NULL rows
    # (sagas not currently blocked on any event) from the index keeps
    # the lookup bounded by the count of currently-blocked sagas.
    #
    # Same raw-SQL rationale as Step 2.
    # ------------------------------------------------------------------
    op.execute(
        "CREATE INDEX idx_saga_state__awaiting "
        "ON saga_state (awaiting_event) "
        "WHERE awaiting_event IS NOT NULL"
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    Drops the saga_state table and its two partial indexes. Use ONLY in
    local development and CI round-trip tests; never invoke in production
    per the forward-only policy in ../README.md (AAP R-9). Production
    rollback is achieved by a NEW forward revision, not by downgrade.

    Operation steps (REVERSE order of upgrade()):
      1. DROP INDEX IF EXISTS idx_saga_state__awaiting (partial index).
      2. DROP INDEX IF EXISTS idx_saga_state__deadline (partial index).
      3. DROP TABLE saga_state -- the PK, UNIQUE, FK, and two CHECK
         constraints drop implicitly with the table.

    Why drop indexes first?
      Symmetry with upgrade() (table created first, then indexes;
      reversed here). Defensive: dropping the table also drops the
      indexes, but explicitly dropping them first makes the rollback
      intent clear and matches the sibling notification-service
      pattern. IF EXISTS guards against double-downgrade.

    What we do NOT drop here:
      * orders table (revision 0001 owns its drop).
      * pgcrypto extension (revision 0001 enabled it but intentionally
        does NOT drop it -- other tables may depend on it).
      * order_items, order_status_history (revisions 0002, 0003 own
        their drops).
    """
    # Drop partial indexes first (in reverse creation order). Raw SQL
    # for symmetry with the CREATE INDEX raw SQL in upgrade(). IF EXISTS
    # guards against double-downgrade in CI round-trip flows.
    op.execute("DROP INDEX IF EXISTS idx_saga_state__awaiting")
    op.execute("DROP INDEX IF EXISTS idx_saga_state__deadline")
    # Drop the table; PK, UNIQUE, FK, and the two CHECK constraints
    # drop implicitly with the table -- no explicit constraint drops
    # are needed.
    op.drop_table("saga_state")
