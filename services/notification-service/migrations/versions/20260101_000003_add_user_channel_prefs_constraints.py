"""add_user_channel_prefs_constraints

Revision ID: 20260101_000003
Revises: 20260101_000002
Create Date: 2026-01-01 00:00:03.000000

Adds database-level CHECK constraints enforcing enum-value integrity
on the 4 notification_db tables. These constraints are a "belt-and-suspenders"
companion to the Pydantic validation performed in src/domain/channel_types.py
and src/repository/ — if either layer is bypassed (e.g., a raw SQL insert from
an operator), the CHECK constraint prevents invalid enum values from persisting.

IMPORTANT (AAP R-6 reminder):
    The CHECK constraint string literals MUST match the StrEnum values in
    `../../src/domain/channel_types.py` exactly. Any divergence causes
    runtime CHECK-constraint violations on first insert. See ../README.md
    "Cross-Coupling Awareness" section.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa  # noqa: F401  (kept for project convention; mirrors script.py.mako)

# revision identifiers, used by Alembic.
revision: str = "20260101_000003"
down_revision: Union[str, Sequence[str], None] = "20260101_000002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration."""
    # 1. notification_log.channel ∈ {'email', 'sms'}  (ChannelType StrEnum)
    op.create_check_constraint(
        "notification_log_channel_enum",
        "notification_log",
        "channel IN ('email', 'sms')",
    )

    # 2. notification_log.status ∈ NotificationStatus StrEnum values
    op.create_check_constraint(
        "notification_log_status_enum",
        "notification_log",
        "status IN ('PENDING', 'SUCCESS', 'PENDING_RETRY', 'DEAD_LETTER', 'CANCELLED')",
    )

    # 3. notification_log.attempt_count is non-negative
    op.create_check_constraint(
        "notification_log_attempt_count_nonneg",
        "notification_log",
        "attempt_count >= 0",
    )

    # 4. delivery_attempts.outcome ∈ DeliveryOutcome StrEnum values
    op.create_check_constraint(
        "delivery_attempts_outcome_enum",
        "delivery_attempts",
        "outcome IN ('SUCCESS', 'RETRYABLE', 'TERMINAL')",
    )

    # 5. delivery_attempts.attempt_number is positive (1-indexed)
    op.create_check_constraint(
        "delivery_attempts_attempt_number_positive",
        "delivery_attempts",
        "attempt_number >= 1",
    )

    # 6. templates.channel ∈ {'email', 'sms'}  (ChannelType StrEnum)
    op.create_check_constraint(
        "templates_channel_enum",
        "templates",
        "channel IN ('email', 'sms')",
    )

    # 7. templates.criticality ∈ {'critical', 'non_critical'}  (CriticalFlag StrEnum)
    op.create_check_constraint(
        "templates_criticality_enum",
        "templates",
        "criticality IN ('critical', 'non_critical')",
    )

    # 8. user_channel_prefs.quiet_hours_start ∈ [0, 23] or NULL
    op.create_check_constraint(
        "user_channel_prefs_quiet_hours_start_range",
        "user_channel_prefs",
        "quiet_hours_start IS NULL OR (quiet_hours_start >= 0 AND quiet_hours_start <= 23)",
    )

    # 9. user_channel_prefs.quiet_hours_end ∈ [0, 23] or NULL
    op.create_check_constraint(
        "user_channel_prefs_quiet_hours_end_range",
        "user_channel_prefs",
        "quiet_hours_end IS NULL OR (quiet_hours_end >= 0 AND quiet_hours_end <= 23)",
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production)."""
    # 9. Drop user_channel_prefs.quiet_hours_end range constraint
    op.drop_constraint(
        "user_channel_prefs_quiet_hours_end_range",
        "user_channel_prefs",
        type_="check",
    )

    # 8. Drop user_channel_prefs.quiet_hours_start range constraint
    op.drop_constraint(
        "user_channel_prefs_quiet_hours_start_range",
        "user_channel_prefs",
        type_="check",
    )

    # 7. Drop templates.criticality enum constraint
    op.drop_constraint(
        "templates_criticality_enum",
        "templates",
        type_="check",
    )

    # 6. Drop templates.channel enum constraint
    op.drop_constraint(
        "templates_channel_enum",
        "templates",
        type_="check",
    )

    # 5. Drop delivery_attempts.attempt_number positive constraint
    op.drop_constraint(
        "delivery_attempts_attempt_number_positive",
        "delivery_attempts",
        type_="check",
    )

    # 4. Drop delivery_attempts.outcome enum constraint
    op.drop_constraint(
        "delivery_attempts_outcome_enum",
        "delivery_attempts",
        type_="check",
    )

    # 3. Drop notification_log.attempt_count nonneg constraint
    op.drop_constraint(
        "notification_log_attempt_count_nonneg",
        "notification_log",
        type_="check",
    )

    # 2. Drop notification_log.status enum constraint
    op.drop_constraint(
        "notification_log_status_enum",
        "notification_log",
        type_="check",
    )

    # 1. Drop notification_log.channel enum constraint
    op.drop_constraint(
        "notification_log_channel_enum",
        "notification_log",
        type_="check",
    )
