"""initial_schema

Revision ID: 20260101_000001
Revises:
Create Date: 2026-01-01 00:00:01.000000

Creates the 4 base tables for notification_db (per AAP Section 0.4.4):
  * notification_log     — one row per inbound Kafka-event resolution attempt;
                           idempotency key on (event_id, channel).
  * delivery_attempts    — one row per outbound provider API call; append-only
                           audit log; soft-references notification_log.notification_id
                           (NO foreign key per AAP R-6).
  * templates            — versioned message templates per (event_type, channel,
                           locale, version).
  * user_channel_prefs   — per-user channel opt-in/opt-out, locale, quiet-hours.

Per AAP R-6, NO cross-service foreign keys are declared. user_id, template_id,
notification_id, and event_id are stored as OPAQUE UUIDs. The pgcrypto extension
is enabled (idempotently) to provide gen_random_uuid() for default UUID
generation.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20260101_000001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration."""
    # 1. Enable pgcrypto for gen_random_uuid().
    #    Idempotent; not dropped in downgrade() (may be shared with other concerns).
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # 2. notification_log — one row per inbound Kafka-event resolution attempt.
    op.create_table(
        "notification_log",
        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
            nullable=False,
        ),
        sa.Column(
            "notification_id",
            sa.UUID(as_uuid=True),
            nullable=False,
            unique=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "event_id",
            sa.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "event_type",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            sa.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column(
            "channel",
            sa.String(length=16),
            nullable=False,
        ),
        sa.Column(
            "template_id",
            sa.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "template_version",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'PENDING'"),
        ),
        sa.Column(
            "recipient_hash",
            sa.String(length=128),
            nullable=False,
        ),
        sa.Column(
            "correlation_id",
            sa.String(length=40),
            nullable=False,
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "next_attempt_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "last_error",
            sa.Text(),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.UniqueConstraint(
            "event_id",
            "channel",
            name="uq_notification_log_event_channel",
        ),
    )

    # 3. delivery_attempts — one row per outbound provider API call.
    #    notification_id is a SOFT REF to notification_log.notification_id;
    #    NO foreign key per AAP R-6.
    op.create_table(
        "delivery_attempts",
        sa.Column(
            "id",
            sa.BigInteger(),
            primary_key=True,
            autoincrement=True,
            nullable=False,
        ),
        sa.Column(
            "notification_id",
            sa.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "attempt_number",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "attempted_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "duration_ms",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "provider_name",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column(
            "provider_message_id",
            sa.String(length=128),
            nullable=True,
        ),
        sa.Column(
            "status_code",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "outcome",
            sa.String(length=16),
            nullable=False,
        ),
        sa.Column(
            "error_type",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "error_message",
            sa.Text(),
            nullable=True,
        ),
    )

    # 4. templates — versioned message templates per (event_type, channel, locale, version).
    #    NOTE: "metadata" column passed POSITIONALLY because SQLAlchemy's Column
    #    base class has an internal `metadata` attribute that conflicts with kwarg form.
    op.create_table(
        "templates",
        sa.Column(
            "template_id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "event_type",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "channel",
            sa.String(length=16),
            nullable=False,
        ),
        sa.Column(
            "locale",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'en-US'"),
        ),
        sa.Column(
            "version",
            sa.String(length=32),
            nullable=False,
        ),
        sa.Column(
            "subject_template",
            sa.Text(),
            nullable=True,
        ),
        sa.Column(
            "body_template_html",
            sa.Text(),
            nullable=True,
        ),
        sa.Column(
            "body_template_text",
            sa.Text(),
            nullable=False,
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "criticality",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'non_critical'"),
        ),
        sa.Column(
            "is_active",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("TRUE"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )

    # 5. user_channel_prefs — per-user channel opt-in/opt-out, locale, quiet-hours.
    #    user_id is the PRIMARY KEY; per AAP R-6 it is an OPAQUE UUID, NOT an FK.
    op.create_table(
        "user_channel_prefs",
        sa.Column(
            "user_id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column(
            "email_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("TRUE"),
        ),
        sa.Column(
            "sms_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("FALSE"),
        ),
        sa.Column(
            "locale",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'en-US'"),
        ),
        sa.Column(
            "quiet_hours_start",
            sa.SmallInteger(),
            nullable=True,
        ),
        sa.Column(
            "quiet_hours_end",
            sa.SmallInteger(),
            nullable=True,
        ),
        sa.Column(
            "timezone",
            sa.String(length=64),
            nullable=False,
            server_default=sa.text("'UTC'"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production)."""
    # Drop in REVERSE creation order.
    op.drop_table("user_channel_prefs")
    op.drop_table("templates")
    op.drop_table("delivery_attempts")
    op.drop_table("notification_log")
    # NOTE: pgcrypto extension is intentionally NOT dropped (may be shared;
    # upgrade is idempotent so re-application is safe).
