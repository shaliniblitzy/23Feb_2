"""create_stock_movements

Revision ID: 0004
Revises: 0003
Create Date: 2026-01-01 00:00:03.000000

Creates the ``stock_movements`` table -- the append-only forensic audit trail
of every state change in the inventory.

Every reservation, release, fulfillment, replenishment, expiration, and
manual adjustment writes one row to this table. The row captures the cause
(``movement_type``), the magnitude (``quantity_delta``), the snapshot of
``stock_items.available_qty`` and ``stock_items.reserved_qty`` immediately
before and after the change, the actor that initiated the change, the
correlation_id of the operation, and a JSONB metadata bag for extensibility.

Append-only contract:
  * UPDATE and DELETE statements against this table are FORBIDDEN by
    application contract. The repository layer at ``../../src/repository/``
    issues only INSERTs.
  * As a defense-in-depth measure, future revisions MAY revoke UPDATE/DELETE
    privileges on this table from the application's database role; that
    enforcement is operational and deferred from this bootstrap revision
    (see revision 0006's optional REVOKE step gated on
    ``INVENTORY_DB_APP_ROLE``).
  * Schema migrations in this revision and any future revision MUST NOT
    issue ``op.alter_column`` or ``op.drop_constraint`` against
    ``stock_movements`` in a way that would mutate or delete existing rows.
    New columns may be added (with NOT NULL DEFAULT or NULL) but existing
    rows are immutable.

BIGSERIAL primary key (NOT UUID):
  * The ``id`` column is ``BIGSERIAL`` (PostgreSQL ``GENERATED ALWAYS AS
    IDENTITY`` is the modern equivalent; we use the legacy BIGSERIAL spelling
    via SQLAlchemy's ``sa.BigInteger() + autoincrement=True``).
  * BIGSERIAL is chosen because the audit table benefits from monotonically
    increasing keys: range scans by id correlate with insertion order, and
    BIGINT (8 bytes) is half the size of UUID (16 bytes) -- meaningful at
    audit-table scale.
  * This is a deliberate departure from the UUID-PK pattern used by
    ``warehouses``, ``stock_items``, ``reservations``, ``reservation_items``.
    Every other surrogate key in this service is UUID; ``stock_movements.id``
    is the lone exception by design.

Cross-service-FK discipline (AAP R-6):
  * ``product_id`` is OPAQUE -- owned by the Product Service. No FK.
  * ``order_id`` is OPAQUE AND NULLABLE -- owned by the Order Service.
    Replenishments and admin adjustments have no order_id.
  * ``reservation_id`` is INTENTIONALLY NOT a FK to reservations.id even
    though the reservations table lives in the same database. This is
    because the audit row must outlive the reservation row: the reservation
    may be deleted (a rare admin action) but the audit trail must persist.
  * ``warehouse_id`` IS a FK to warehouses.id with ON DELETE RESTRICT --
    the same warehouse-decommissioning discipline applied throughout this
    service. RESTRICT here means warehouse decommissioning must drain
    audit-relevant context; in practice warehouses are never deleted, only
    set to status='DECOMMISSIONED'.

Schema (per AAP Section 0.4.4 + parent folder README.md):
    * id                    BIGSERIAL PK   (autoincrement)
    * product_id            UUID    NOT NULL  (opaque)
    * warehouse_id          UUID    NOT NULL  (FK -> warehouses.id, RESTRICT)
    * movement_type         VARCHAR(24) NOT NULL CHECK movement_type IN
                                ('RESERVE', 'RELEASE', 'FULFILL', 'EXPIRE',
                                 'REPLENISH', 'ADJUST')
    * quantity_delta        INTEGER NOT NULL  (can be negative for RESERVE/
                                               RELEASE/FULFILL/EXPIRE)
    * before_available_qty  INTEGER NOT NULL
    * after_available_qty   INTEGER NOT NULL
    * before_reserved_qty   INTEGER NOT NULL
    * after_reserved_qty    INTEGER NOT NULL
    * order_id              UUID    NULLABLE  (opaque; null for replenishments)
    * reservation_id        UUID    NULLABLE  (NOT a FK; null for replenishments)
    * correlation_id        VARCHAR(64) NULLABLE  (R-13)
    * actor                 VARCHAR(64) NOT NULL DEFAULT 'system'
    * metadata              JSONB   NOT NULL  DEFAULT '{}'::jsonb
    * occurred_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()

References:
    * AAP Section 0.4.4 -- inventory_db schema
    * AAP R-6  -- Database per service strict isolation
    * AAP R-13 -- Correlation-ID propagation
    * Parent folder README.md -- Schema specification verbatim
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, Sequence[str], None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration: create the stock_movements append-only audit table.

    Depends on revision 0003. The warehouse_id FK requires the warehouses
    table from revision 0001 to exist.
    """
    op.create_table(
        "stock_movements",
        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
            nullable=False,
            comment=(
                "BIGSERIAL surrogate primary key. Sequential ordering of inserts "
                "correlates with insertion timeline; range scans by id correspond "
                "to time-ordered audit traversal."
            ),
        ),
        sa.Column(
            "product_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment=(
                "Opaque product identifier. NO FK per AAP R-6 (owned by Product Service)."
            ),
        ),
        sa.Column(
            "warehouse_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment=(
                "FK to warehouses.id with ON DELETE RESTRICT. Audit history is "
                "preserved even after warehouse decommissioning (warehouses are "
                "set to status='DECOMMISSIONED', not deleted)."
            ),
        ),
        sa.Column(
            "movement_type",
            sa.String(length=24),
            nullable=False,
            comment=(
                "Cause of the movement. UPPERCASE enum coupled with the "
                "StockMovementType StrEnum in ../../src/domain/. Values: "
                "RESERVE | RELEASE | FULFILL | EXPIRE | REPLENISH | ADJUST."
            ),
        ),
        sa.Column(
            "quantity_delta",
            sa.Integer(),
            nullable=False,
            comment=(
                "Signed change in available_qty for this movement. Negative for "
                "RESERVE / FULFILL (when stock leaves availability); positive for "
                "RELEASE / EXPIRE / REPLENISH (when stock returns to availability). "
                "ADJUST may be either sign. Note that the sign is NOT enforced by "
                "a CHECK because the application layer is the authority on "
                "movement_type semantics; defending against contradiction here "
                "would constrain valid future evolutions."
            ),
        ),
        sa.Column(
            "before_available_qty",
            sa.Integer(),
            nullable=False,
            comment="Snapshot of stock_items.available_qty BEFORE this movement.",
        ),
        sa.Column(
            "after_available_qty",
            sa.Integer(),
            nullable=False,
            comment="Snapshot of stock_items.available_qty AFTER this movement.",
        ),
        sa.Column(
            "before_reserved_qty",
            sa.Integer(),
            nullable=False,
            comment="Snapshot of stock_items.reserved_qty BEFORE this movement.",
        ),
        sa.Column(
            "after_reserved_qty",
            sa.Integer(),
            nullable=False,
            comment="Snapshot of stock_items.reserved_qty AFTER this movement.",
        ),
        sa.Column(
            "order_id",
            sa.UUID(as_uuid=True),
            nullable=True,
            comment=(
                "Opaque order identifier. NO FK per AAP R-6. NULLABLE because "
                "REPLENISH and ADJUST movements have no associated order."
            ),
        ),
        sa.Column(
            "reservation_id",
            sa.UUID(as_uuid=True),
            nullable=True,
            comment=(
                "Reservation identifier. INTENTIONALLY NOT a FK to reservations.id "
                "because the audit row must outlive the reservation row. NULLABLE "
                "for REPLENISH / ADJUST."
            ),
        ),
        sa.Column(
            "correlation_id",
            sa.String(length=64),
            nullable=True,
            comment=(
                "X-Correlation-ID propagated from the API Gateway through Kafka "
                "headers (AAP R-13). Nullable for system-initiated movements "
                "(scheduler-driven expirations, admin replenishments without "
                "request context)."
            ),
        ),
        sa.Column(
            "actor",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'system'"),
            comment=(
                "Identifier of the actor that caused the movement. 'system' for "
                "Kafka-event-triggered changes (default); admin user id (UUID or "
                "email) for manual adjustments via admin endpoints; "
                "'expiration_scheduler' for AAP R-20 deadline-driven releases."
            ),
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
            comment=(
                "Structured extensibility bag (e.g., source IP for admin actions, "
                "batch_id for bulk imports, expired_reason for scheduler-driven "
                "expirations). Empty default '{}'::jsonb is safe."
            ),
        ),
        sa.Column(
            "occurred_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
            comment=(
                "Wall-clock timestamp when the movement was recorded (UTC). "
                "Used by audit time-window queries; indexed in revision 0006."
            ),
        ),
        sa.PrimaryKeyConstraint("id", name="pk_stock_movements"),
        sa.ForeignKeyConstraint(
            ["warehouse_id"],
            ["warehouses.id"],
            name="fk_stock_movements__warehouse_id__warehouses",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "movement_type IN ('RESERVE', 'RELEASE', 'FULFILL', 'EXPIRE', "
            "'REPLENISH', 'ADJUST')",
            name="ck_stock_movements__movement_type_enum",
        ),
        comment=(
            "Append-only forensic audit log of every reservation, release, "
            "fulfillment, replenishment, expiration, and adjustment. UPDATE "
            "and DELETE are forbidden by application contract; future revisions "
            "MUST NOT mutate existing rows. Per AAP R-13, every row carries a "
            "correlation_id linking it to the originating user request."
        ),
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    Drops the stock_movements table along with its sequence (BIGSERIAL implicit
    sequence) and all attached constraints and column comments.
    """
    op.drop_table("stock_movements")
