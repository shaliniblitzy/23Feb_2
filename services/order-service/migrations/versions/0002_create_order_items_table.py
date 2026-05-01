"""create_order_items_table

Revision ID: 0002
Revises: 0001
Create Date: 2026-01-01 00:00:02.000000

Create order_items table -- line items belonging to an order.

Each row is one line item on one order. The composite primary key
(order_id, line_no) enforces stable, gapless-or-gapful but
deterministic line numbering within each order, and provides the
natural index for ordered line retrieval.

AAP R-6 (database per service) is enforced here:
  * order_id is a FOREIGN KEY to orders(id) -- both tables live in
    order_db, so this intra-DB FK is permitted and encouraged.
  * product_id is an OPAQUE UUID -- there is NO foreign key to the
    Product Service's product_db. Product validity is verified at
    order placement via Product Service's REST API; referential
    integrity for product_id is application-layer only.

Pricing fields are stored as BIGINT in MINOR currency units (e.g.,
cents for USD, paise for INR). NEVER store monetary values in
floating-point types. The currency code lives on the parent order
(orders.currency); line items inherit it implicitly because all
items on an order share the order's currency.

ON DELETE CASCADE on the FK ensures order deletion garbage-collects
its line items.
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
    """Apply forward migration: create order_items table with composite PK.

    Operation steps:
      1. CREATE TABLE order_items with 6 columns (order_id, line_no,
         product_id, quantity, unit_price, line_total).
      2. Composite PRIMARY KEY on (order_id, line_no) named
         pk_order_items -- the natural key for an order line item.
      3. FOREIGN KEY order_items.order_id -> orders(id) ON DELETE CASCADE,
         named fk_order_items__order_id__orders. This is the ONLY foreign
         key permitted on this table per AAP R-6 because both tables
         live in order_db. product_id is OPAQUE -- no cross-DB FK to
         product_db allowed.
      4. Three CHECK constraints to enforce domain invariants:
         - ck_order_items__quantity_positive    (quantity > 0)
         - ck_order_items__unit_price_nonneg    (unit_price >= 0)
         - ck_order_items__line_total_nonneg    (line_total >= 0)

    No additional indexes are created -- the PK index automatically
    supports both `WHERE order_id = ?` (leftmost-prefix) and
    `WHERE order_id = ? AND line_no = ?` (full key) queries, which
    are the only access patterns the Order Service uses for this
    table. Cross-order product lookups belong to the Product Service /
    Recommendation Engine via Kafka events, not to order_db.
    """
    # ------------------------------------------------------------------
    # CREATE TABLE order_items.
    #
    # 6 columns per the folder spec for services/order-service/
    # migrations/versions/. All constraints declared inline so they
    # become part of the same DDL statement as the table create:
    #   - PRIMARY KEY (order_id, line_no) named pk_order_items.
    #   - FOREIGN KEY (order_id) -> orders(id) ON DELETE CASCADE
    #     named fk_order_items__order_id__orders.
    #   - CHECK quantity > 0       named ck_order_items__quantity_positive
    #   - CHECK unit_price >= 0    named ck_order_items__unit_price_nonneg
    #   - CHECK line_total >= 0    named ck_order_items__line_total_nonneg
    #
    # Composite PKs MUST use the explicit sa.PrimaryKeyConstraint(...)
    # form (rather than primary_key=True on individual columns) so the
    # PK constraint can be named -- naming makes operator-facing error
    # messages identifiable ("violates pk_order_items") and matches the
    # service-wide naming convention pk_<table>.
    # ------------------------------------------------------------------
    op.create_table(
        "order_items",
        # FK component of composite PK. ON DELETE CASCADE ensures order
        # deletion garbage-collects line items in a single statement,
        # avoiding orphaned children. nullable=False because every line
        # item belongs to exactly one order.
        sa.Column(
            "order_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment="Owning order; PK component and FK to orders(id).",
        ),
        # Sequence number within the order (1, 2, 3, ...). Application-
        # assigned; the database does NOT auto-generate. Combined with
        # order_id forms the composite PK. Using a small Integer (INT4)
        # is sufficient because no order will realistically exceed 2.1B
        # line items.
        sa.Column(
            "line_no",
            sa.Integer(),
            nullable=False,
            comment="Line number within the order (1-based, application-assigned).",
        ),
        # AAP R-6: OPAQUE UUID -- NO FK to products table (which lives
        # in product_db, owned by Product Service). Validity is checked
        # via Product Service REST API at order placement; referential
        # integrity is application-layer only. A hard-deleted product
        # in product_db will leave dangling order_items.product_id
        # values -- that is acceptable because orders are immutable
        # historical records.
        sa.Column(
            "product_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            comment=(
                "Opaque product identifier; NO cross-DB FK per AAP R-6. "
                "Validity is checked via Product Service REST API."
            ),
        ),
        # Number of units of this product on this line. Always positive
        # (zero would be a no-op line; use DELETE instead). The CHECK
        # constraint ck_order_items__quantity_positive enforces this at
        # the database level.
        sa.Column(
            "quantity",
            sa.Integer(),
            nullable=False,
            comment="Units of the product on this line; must be > 0.",
        ),
        # Per-unit price snapshot at order placement, in MINOR currency
        # units (e.g., cents for USD, paise for INR). MUST be the price
        # applied to this order; subsequent product price changes do
        # NOT mutate this field. BigInteger (INT8) avoids overflow on
        # at-scale orders and currencies without minor units (e.g.,
        # JPY where the unit price is the whole amount).
        sa.Column(
            "unit_price",
            sa.BigInteger(),
            nullable=False,
            comment="Per-unit price in MINOR currency units (e.g., cents).",
        ),
        # Materialized line total in MINOR currency units. Application
        # code MUST set line_total = unit_price * quantity at insert
        # time; the database does NOT auto-compute. Materialized for
        # ORDER summary queries that aggregate without re-reading every
        # item. The lenient CHECK >= 0 guards against negative totals
        # but does not enforce the multiplication invariant -- that is
        # an application-layer responsibility.
        sa.Column(
            "line_total",
            sa.BigInteger(),
            nullable=False,
            comment=(
                "line_total = unit_price * quantity, in MINOR currency units. "
                "Application-computed at insert."
            ),
        ),
        # ---- Constraints ----
        # Composite PK: (order_id, line_no) is the natural key.
        # Declared as an explicit sa.PrimaryKeyConstraint so the
        # constraint name pk_order_items follows the service-wide
        # pk_<table> naming convention; primary_key=True on individual
        # columns cannot name a composite PK.
        sa.PrimaryKeyConstraint(
            "order_id", "line_no",
            name="pk_order_items",
        ),
        # FK with ON DELETE CASCADE. The only foreign key permitted on
        # this table per AAP R-6 because orders(id) lives in the same
        # order_db. Cross-DB FKs (e.g., to product_db.products) are
        # forbidden -- product_id is opaque (see above).
        sa.ForeignKeyConstraint(
            ["order_id"],
            ["orders.id"],
            ondelete="CASCADE",
            name="fk_order_items__order_id__orders",
        ),
        # CHECK: quantity must be strictly positive. Zero-quantity rows
        # are a no-op and should be DELETEd, not represented; negative
        # quantities are nonsense for line items.
        sa.CheckConstraint(
            "quantity > 0",
            name="ck_order_items__quantity_positive",
        ),
        # CHECK: unit_price must be non-negative. Free items (price = 0)
        # are valid (e.g., promotional add-ons); negative prices are
        # not -- discounts are represented as separate adjustments at
        # the order level, not as negative line prices.
        sa.CheckConstraint(
            "unit_price >= 0",
            name="ck_order_items__unit_price_nonneg",
        ),
        # CHECK: line_total must be non-negative. A defensive guard
        # against application bugs that might otherwise let a
        # negative materialized total slip through; combined with
        # the unit_price >= 0 and quantity > 0 constraints, the
        # full business invariant (line_total = unit_price * quantity
        # >= 0) is mostly enforceable at the application layer with
        # this DB-level safety net.
        sa.CheckConstraint(
            "line_total >= 0",
            name="ck_order_items__line_total_nonneg",
        ),
        comment=(
            "Order line items. PK is (order_id, line_no); product_id is "
            "an opaque cross-service identifier per AAP R-6."
        ),
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    Drops the order_items table. The composite PK, FK, and CHECK
    constraints all drop implicitly with the table -- no explicit
    constraint drops are needed. No index drops are needed either,
    because only the auto-created PK index exists on this table and
    it drops with the table.

    Use ONLY in local dev and CI round-trip tests; never invoke in
    production per the forward-only policy in ../README.md (AAP R-9).
    """
    op.drop_table("order_items")
