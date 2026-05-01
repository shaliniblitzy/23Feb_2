"""add_indexes

Revision ID: 20260101_000002
Revises: 20260101_000001
Create Date: 2026-01-01 00:00:02.000000

Adds performance indexes supporting the Notification Service's hot-path
queries:
  * idx_notification_log_retry_poll     — partial index for the retry scheduler
    polling query (status = 'PENDING_RETRY' AND next_attempt_at <= NOW()).
  * idx_notification_log_status_created — composite index for status dashboards
    and DLQ sweepers (ORDER BY created_at DESC).
  * idx_delivery_attempts_notification  — composite index for per-notification
    audit joins (notification_id, attempt_number).
  * idx_templates_lookup                — composite index for the runtime
    template-resolution query (event_type, channel, locale, is_active,
    version DESC).

These indexes are additive and fully reversible; see ../README.md "Rollback
Policy" for the forward-only production policy.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260101_000002"
down_revision: Union[str, Sequence[str], None] = "20260101_000001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration."""
    # 1. Partial index for the retry scheduler's polling query.
    #    Raw SQL is used because the WHERE predicate is a partial-index feature
    #    and the raw form is documented in ../README.md for operator clarity.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_notification_log_retry_poll "
        "ON notification_log (status, next_attempt_at) "
        "WHERE status = 'PENDING_RETRY'"
    )

    # 2. Composite index for status dashboards and DLQ sweepers.
    #    (status, created_at DESC) supports "WHERE status = :s ORDER BY created_at DESC LIMIT :n".
    op.create_index(
        "idx_notification_log_status_created",
        "notification_log",
        ["status", sa.text("created_at DESC")],
    )

    # 3. Composite index on delivery_attempts for per-notification audit joins.
    #    (notification_id, attempt_number) supports "WHERE notification_id = :id ORDER BY attempt_number".
    op.create_index(
        "idx_delivery_attempts_notification",
        "delivery_attempts",
        ["notification_id", "attempt_number"],
    )

    # 4. Composite index for the runtime template-resolution query.
    #    (event_type, channel, locale, is_active, version DESC) covers every filter
    #    plus the DESC sort for "latest active version" semantics.
    op.create_index(
        "idx_templates_lookup",
        "templates",
        ["event_type", "channel", "locale", "is_active", sa.text("version DESC")],
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production)."""
    # 4. Drop idx_templates_lookup
    op.drop_index("idx_templates_lookup", table_name="templates")

    # 3. Drop idx_delivery_attempts_notification
    op.drop_index(
        "idx_delivery_attempts_notification",
        table_name="delivery_attempts",
    )

    # 2. Drop idx_notification_log_status_created
    op.drop_index(
        "idx_notification_log_status_created",
        table_name="notification_log",
    )

    # 1. Drop idx_notification_log_retry_poll (partial index).
    #    Dropped via raw SQL for symmetry with its creation path.
    op.execute("DROP INDEX IF EXISTS idx_notification_log_retry_poll")
