"""seed_initial_data

Revision ID: 20260101_000005
Revises: 20260101_000004
Create Date: 2026-01-01 00:00:05.000000

Bootstrap-chain terminus revision (no-op placeholder).

The order_db schema requires NO static reference data at bootstrap:
  * Order status values are enforced via CHECK constraint in revision
    20260101_000001 (CREATE TABLE orders ... CHECK (status IN
    ('CREATED', ...))).
  * Saga step values are enforced via CHECK constraint in revision
    20260101_000004 (CREATE TABLE saga_state ... CHECK (current_step IN
    ('CREATE_ORDER', ...))).
  * No lookup tables are required because the enum sets are small, stable,
    and validated at the application layer via Pydantic + StrEnum models.

This revision exists to:
  1. Provide a known terminus for the bootstrap revision chain so operators
     and tooling can identify the end-of-bootstrap state with `alembic
     current` returning '20260101_000005' immediately after first apply.
  2. Reserve a slot for future bootstrap seed data (e.g., a default tenant
     row, a default saga timeout reference table) without renumbering the
     existing revisions 20260101_000001-20260101_000004.

Both upgrade() and downgrade() are intentional no-ops. Future seed-data
migrations should be written as NEW forward revisions (e.g.,
20260301_000001_seed_default_warehouses.py) -- never edit this file
in-place after it has been applied to any environment, per the
forward-only policy in ../README.md.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260101_000005"
down_revision: Union[str, Sequence[str], None] = "20260101_000004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration.

    No-op: order_db has no static reference data at bootstrap. Order status
    and saga step enums are enforced by CHECK constraints in revisions
    20260101_000001 and 20260101_000004; no lookup tables are required.

    This revision is intentionally a placeholder so the bootstrap chain has
    a known terminus. Future seed-data migrations should be authored as new
    forward revisions, not by editing this file (per forward-only policy
    documented in ../README.md).
    """
    pass


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production).

    No-op: this revision applies no schema or data changes, so there is
    nothing to revert. Round-trip tests (alembic upgrade head -> alembic
    downgrade base -> alembic upgrade head) traverse this revision
    transparently in both directions.
    """
    pass
