"""create_reservations

Revision ID: 0003
Revises: 0002
Create Date: 2026-01-01 00:00:02.000000

Creates the ``reservations`` table and its child ``reservation_items`` table.

These two tables together model the **reservation lifecycle**, the central
state machine of the Inventory Service. The reservation table is keyed on
``order_id`` (UNIQUE) -- that single constraint is the cornerstone of
at-least-once Kafka delivery safety: when ``order.created`` is delivered more
than once (Kafka's default at-least-once semantics + DLQ replay), the second
INSERT is rejected by the unique constraint and the application's
``ON CONFLICT (order_id) DO NOTHING`` clause turns it into a NOOP without
double-allocating stock.

Reservation lifecycle (status enum):
    * ACTIVE     -- newly inserted on order.created; stock is reserved.
    * RELEASED   -- order.cancelled processed OR reservation expired; stock
                    returned to available_qty.
    * FULFILLED  -- order.fulfilled processed; stock has shipped (NOT returned
                    to availability -- terminal state).
    * EXPIRED    -- reservation deadline (expires_at) reached without
                    order.fulfilled or order.cancelled; the AAP R-20 expiration
                    scheduler released the stock back to availability and
                    transitioned the reservation to this terminal state.

The ``expires_at TIMESTAMPTZ NOT NULL`` deadline + the partial index on
``(status, expires_at) WHERE status = 'ACTIVE'`` (created in revision 0006) are
what enable the expiration scheduler to scan for stuck reservations
efficiently. Without the partial index, the scan would degrade to O(N) in the
total reservation count; with it, the scan is O(log K) where K is the number
of currently-active unexpired reservations.

The ``correlation_id`` column propagates the X-Correlation-ID HTTP header
value through Kafka events into reservation rows for AAP R-13 distributed
tracing -- every log line emitted by reservation processing carries the same
correlation_id, allowing operators to follow a single user's journey across
the entire microservice graph.

Cross-service-FK discipline (AAP R-6):
  * ``reservations.order_id`` is OPAQUE -- owned by the Order Service
    (order_db, PostgreSQL). No FK constraint.
  * ``reservation_items.product_id`` is OPAQUE -- owned by the Product Service
    (product_db, MongoDB). No FK constraint.
  * ``reservation_items.reservation_id`` IS an intra-service FK with
    ``ON DELETE CASCADE`` -- child rows must vanish if the parent reservation
    is deleted (a rare operational action; see Rollback Policy in
    ../README.md).
  * ``reservation_items.warehouse_id`` IS an intra-service FK with NO
    CASCADE -- same warehouse-decommissioning discipline as stock_items.

Schema (per AAP Section 0.4.4 + parent folder README.md):

  reservations:
    * id              UUID PK   DEFAULT gen_random_uuid()
    * order_id        UUID      NOT NULL UNIQUE  (idempotency -- R-14, R-17)
    * status          VARCHAR(16) NOT NULL CHECK status IN
                          ('ACTIVE', 'RELEASED', 'FULFILLED', 'EXPIRED')
    * expires_at      TIMESTAMPTZ NOT NULL  (deadline -- R-20 fallback)
    * correlation_id  VARCHAR(64)  NULLABLE  (R-13 distributed tracing)
    * created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    * updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()

  reservation_items:
    * id              UUID PK   DEFAULT gen_random_uuid()
    * reservation_id  UUID      NOT NULL  (FK -> reservations.id, CASCADE)
    * product_id      UUID      NOT NULL  (opaque; no FK -- R-6)
    * warehouse_id    UUID      NOT NULL  (FK -> warehouses.id, NO CASCADE)
    * quantity        INTEGER   NOT NULL  CHECK (quantity > 0)
    * UNIQUE (reservation_id, product_id, warehouse_id)

References:
    * AAP Section 0.4.4 -- inventory_db schema
    * AAP R-6  -- Database per service strict isolation
    * AAP R-13 -- Correlation-ID propagation
    * AAP R-14, R-17 -- Idempotency under at-least-once Kafka delivery
    * AAP R-18 -- Saga pattern (Inventory is participant; Order is coordinator)
    * AAP R-20 -- Fallback paths: reservation expiration scheduler
    * Parent folder README.md -- Schema specification verbatim
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
    """Apply forward migration: create reservations and reservation_items tables.

    Depends on revision 0002 (stock_items table). Order within this revision:
    reservations FIRST (FK target), reservation_items SECOND (FK source).
    """
    # ------------------------------------------------------------------
    # CREATE TABLE reservations.
    #
    # The parent table of the (reservations, reservation_items) pair.
    # MUST be created before reservation_items because
    # reservation_items.reservation_id REFERENCES reservations.id.
    #
    # Key architectural constraints:
    #   * order_id UNIQUE -- the cornerstone of at-least-once Kafka
    #     delivery idempotency. When order.created is redelivered (Kafka
    #     retries, consumer offset rewinds, DLQ replay), the second
    #     INSERT is rejected and the application layer's
    #     `ON CONFLICT (order_id) DO NOTHING` clause turns the duplicate
    #     into a safe NOOP without double-allocating stock.
    #   * status CHECK -- the database enforces the enum
    #     (ACTIVE | RELEASED | FULFILLED | EXPIRED) coupled with the
    #     ReservationStatus StrEnum in ../../src/domain/.
    #   * expires_at NOT NULL with NO server_default -- the application
    #     layer must set this explicitly to NOW() + RESERVATION_EXPIRY_MS;
    #     a database-level default would mask programming errors.
    #   * order_id is OPAQUE per AAP R-6 -- owned by Order Service. No FK.
    #   * correlation_id is NULLABLE -- admin operations bypassing the
    #     API Gateway may legitimately omit it (AAP R-13).
    # ------------------------------------------------------------------
    op.create_table(
        "reservations",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
            comment="Surrogate UUID primary key (v4) generated by pgcrypto.",
        ),
        sa.Column(
            "order_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment=(
                "Opaque order identifier owned by the Order Service "
                "(order_db, PostgreSQL). NO FK constraint per AAP R-6. "
                "UNIQUE constraint below makes duplicate order.created "
                "Kafka events safe NOOPs (at-least-once delivery, AAP R-14/R-17)."
            ),
        ),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            comment=(
                "Reservation lifecycle state. UPPERCASE enum coupled with the "
                "ReservationStatus StrEnum in ../../src/domain/. Values: "
                "ACTIVE | RELEASED | FULFILLED | EXPIRED."
            ),
        ),
        sa.Column(
            "expires_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            comment=(
                "Reservation expiry deadline. The AAP R-20 expiration scheduler "
                "scans rows where status='ACTIVE' AND expires_at < NOW() and "
                "transitions them to EXPIRED, releasing stock back to "
                "available_qty. Set by the application layer to "
                "NOW() + RESERVATION_EXPIRY_MS (default 15 minutes)."
            ),
        ),
        sa.Column(
            "correlation_id",
            sa.String(length=64),
            nullable=True,
            comment=(
                "X-Correlation-ID propagated from the API Gateway through Kafka "
                "headers (AAP R-13). NOT generated server-side; nullable because "
                "ad-hoc admin operations may create reservations without a "
                "correlation context."
            ),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="Row creation timestamp (UTC).",
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment="Last status transition timestamp (UTC).",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_reservations"),
        sa.UniqueConstraint("order_id", name="uq_reservations__order_id"),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'RELEASED', 'FULFILLED', 'EXPIRED')",
            name="ck_reservations__status_enum",
        ),
        comment=(
            "Reservation lifecycle row. Keyed on order_id (UNIQUE) for "
            "idempotency under at-least-once Kafka delivery. expires_at + "
            "the partial index from revision 0006 power the AAP R-20 "
            "expiration scheduler."
        ),
    )

    # ------------------------------------------------------------------
    # CREATE TABLE reservation_items.
    #
    # The child table of reservations. Each row is one (product, warehouse)
    # line item belonging to one reservation. The composite UNIQUE on
    # (reservation_id, product_id, warehouse_id) prevents duplicate line
    # items per reservation -- a defense-in-depth check against application
    # bugs that might otherwise let the same SKU be reserved twice for the
    # same order at the same warehouse.
    #
    # Foreign key policy:
    #   * reservation_id -> reservations.id ON DELETE CASCADE -- when the
    #     parent reservation is deleted, its line items vanish with it.
    #     This makes orphan reservation_items rows structurally impossible.
    #   * warehouse_id -> warehouses.id ON DELETE RESTRICT -- warehouse
    #     decommissioning must drain reservations explicitly; the database
    #     refuses to delete a warehouse that has live reservation_items
    #     pointing at it.
    #   * product_id has NO FK -- AAP R-6 forbids cross-service references
    #     (product_id is owned by Product Service / product_db, MongoDB).
    #
    # The CHECK on quantity > 0 (strictly, NOT >= 0) enforces that a
    # zero-quantity reservation is meaningless and rejected at the database
    # level; this matches the parent README spec verbatim.
    # ------------------------------------------------------------------
    op.create_table(
        "reservation_items",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
            comment="Surrogate UUID primary key (v4) generated by pgcrypto.",
        ),
        sa.Column(
            "reservation_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment=(
                "FK to reservations.id with ON DELETE CASCADE. Child rows "
                "vanish if the parent reservation is deleted; this matters "
                "for the rare operational action of admin reservation cleanup."
            ),
        ),
        sa.Column(
            "product_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment=(
                "Opaque product identifier owned by the Product Service. "
                "NO FK constraint per AAP R-6."
            ),
        ),
        sa.Column(
            "warehouse_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment=(
                "FK to warehouses.id with NO CASCADE. Warehouse decommissioning "
                "must drain reservations explicitly."
            ),
        ),
        sa.Column(
            "quantity",
            sa.Integer(),
            nullable=False,
            comment=(
                "Number of units reserved on this line. Must be strictly "
                "positive -- a zero or negative reservation is meaningless."
            ),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_reservation_items"),
        sa.ForeignKeyConstraint(
            ["reservation_id"],
            ["reservations.id"],
            name="fk_reservation_items__reservation_id__reservations",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["warehouse_id"],
            ["warehouses.id"],
            name="fk_reservation_items__warehouse_id__warehouses",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "reservation_id",
            "product_id",
            "warehouse_id",
            name="uq_reservation_items__reservation_product_warehouse",
        ),
        sa.CheckConstraint(
            "quantity > 0",
            name="ck_reservation_items__quantity_positive",
        ),
        comment=(
            "Per-line reservation detail. UNIQUE (reservation_id, product_id, "
            "warehouse_id) prevents duplicate line items per reservation. "
            "ON DELETE CASCADE from reservations means orphan rows are "
            "structurally impossible."
        ),
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    Drops reservation_items FIRST (FK source) then reservations (FK target).
    The CASCADE on reservation_items.reservation_id makes ordering technically
    optional, but explicit ordering matches forward-create symmetry.
    """
    op.drop_table("reservation_items")
    op.drop_table("reservations")
