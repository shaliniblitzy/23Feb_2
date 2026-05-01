"""initial_schema

Revision ID: 20260101_000001
Revises:
Create Date: 2026-01-01 00:00:01.000000

Creates the 5 base tables for payment_db (per AAP Section 0.4.4):
  * payments           — one row per payment intent; encrypted-at-rest fields for
                         provider tokens and PCI-allowed display data (last4, brand,
                         holder_name) per AAP R-8.
  * payment_attempts   — one row per outbound provider API call; outbound
                         idempotency-key store (AAP R-8). Intra-DB FK to
                         payments(id) ON DELETE CASCADE.
  * refunds            — one row per refund issued; encrypted provider_refund_id.
                         Intra-DB FK to payments(id).
  * provider_webhooks  — one row per inbound provider webhook (AAP R-12). UNIQUE
                         constraint on (provider, provider_event_id) added in
                         revision 20260101_000002 enforces idempotency.
  * idempotency_keys   — one row per inbound API Idempotency-Key value (AAP R-8).

Per AAP R-6, NO CROSS-SERVICE foreign keys are declared. payments.order_id and
payments.user_id are stored as OPAQUE UUIDs. Intra-DB FKs (payment_attempts.payment_id
→ payments.id; refunds.payment_id → payments.id) ARE permitted because all three
tables live within the same private payment_db database.

The pgcrypto extension is enabled (idempotently) to provide:
  * gen_random_uuid()                — UUID primary key defaults
  * pgp_sym_encrypt / pgp_sym_decrypt — column-level encryption helpers used by
                                       src/repository/encryption.py for the
                                       encrypted column set (provider_charge_id,
                                       last4, brand, holder_name, provider_refund_id)
                                       per AAP R-8.

Initial single-column / two-column indexes are created here for the most common
access patterns; composite UNIQUE constraints (idempotency-related) and CHECK
constraints (enum validation) are added in revision 20260101_000002 to keep this
foundational revision focused on table shape.
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
    # 1. Enable pgcrypto: provides gen_random_uuid() AND pgp_sym_encrypt/decrypt.
    #    Idempotent; not dropped in downgrade() (may be shared with other concerns,
    #    and DROP EXTENSION pgcrypto fails when other DB objects depend on it).
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # 2. payments — one row per payment intent.
    op.create_table(
        "payments",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "order_id",
            sa.UUID(as_uuid=True),
            nullable=False,  # OPAQUE UUID; no FK per AAP R-6
        ),
        sa.Column(
            "user_id",
            sa.UUID(as_uuid=True),
            nullable=False,  # OPAQUE UUID; no FK per AAP R-6
        ),
        sa.Column(
            "amount",
            sa.Numeric(precision=18, scale=4),
            nullable=False,
        ),
        sa.Column(
            "currency",
            sa.String(length=3),
            nullable=False,  # ISO 4217 (USD, INR, EUR, ...); CHECK added in 000002.
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "provider",
            sa.String(length=16),
            nullable=False,  # 'stripe' | 'razorpay'; CHECK added in 000002 (AAP R-10).
        ),
        sa.Column(
            "provider_charge_id",
            sa.Text(),
            nullable=True,  # pgcrypto-encrypted at app layer (AAP R-8).
        ),
        sa.Column(
            "last4",
            sa.Text(),
            nullable=True,  # pgcrypto-encrypted at app layer (AAP R-8).
        ),
        sa.Column(
            "brand",
            sa.Text(),
            nullable=True,  # pgcrypto-encrypted at app layer (AAP R-8).
        ),
        sa.Column(
            "holder_name",
            sa.Text(),
            nullable=True,  # pgcrypto-encrypted at app layer (AAP R-8).
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
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

    # 3. payment_attempts — one row per outbound provider API call.
    #    Intra-DB FK to payments(id) ON DELETE CASCADE is permitted (same DB).
    op.create_table(
        "payment_attempts",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "payment_id",
            sa.UUID(as_uuid=True),
            sa.ForeignKey("payments.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "attempt_no",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "provider",
            sa.String(length=16),
            nullable=False,
        ),
        sa.Column(
            "idempotency_key",
            sa.Text(),
            nullable=False,  # outbound provider idempotency key (AAP R-8).
        ),
        sa.Column(
            "request_id",
            sa.Text(),
            nullable=True,
        ),
        sa.Column(
            "response_code",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "response_status",
            sa.String(length=16),
            nullable=False,
        ),
        sa.Column(
            "error_code",
            sa.String(length=64),
            nullable=True,
        ),
        sa.Column(
            "latency_ms",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "retry_after_ms",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )

    # 4. refunds — one row per refund.
    #    Intra-DB FK to payments(id) is permitted (same DB).
    op.create_table(
        "refunds",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "payment_id",
            sa.UUID(as_uuid=True),
            sa.ForeignKey("payments.id"),
            nullable=False,
        ),
        sa.Column(
            "amount",
            sa.Numeric(precision=18, scale=4),
            nullable=False,
        ),
        sa.Column(
            "currency",
            sa.String(length=3),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "provider",
            sa.String(length=16),
            nullable=False,
        ),
        sa.Column(
            "provider_refund_id",
            sa.Text(),
            nullable=True,  # pgcrypto-encrypted at app layer (AAP R-8).
        ),
        sa.Column(
            "reason",
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
    )

    # 5. provider_webhooks — one row per inbound webhook (AAP R-12).
    #    UNIQUE constraint on (provider, provider_event_id) added in 000002.
    op.create_table(
        "provider_webhooks",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "provider",
            sa.String(length=16),
            nullable=False,
        ),
        sa.Column(
            "provider_event_id",
            sa.Text(),
            nullable=False,
        ),
        sa.Column(
            "event_type",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "signature",
            sa.Text(),
            nullable=False,
        ),
        sa.Column(
            "raw_payload",
            postgresql.JSONB(),
            nullable=False,
        ),
        sa.Column(
            "verified_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "processed_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'verified'"),
        ),
        sa.Column(
            "correlation_id",
            sa.String(length=40),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )

    # 6. idempotency_keys — one row per inbound API Idempotency-Key value (AAP R-8).
    op.create_table(
        "idempotency_keys",
        sa.Column(
            "id",
            sa.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "key",
            sa.Text(),
            nullable=False,
        ),
        sa.Column(
            "endpoint",
            sa.String(length=128),
            nullable=False,
        ),
        sa.Column(
            "request_hash",
            sa.String(length=128),
            nullable=False,
        ),
        sa.Column(
            "response_status",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "response_body",
            postgresql.JSONB(),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "expires_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
        ),
    )

    # 7. Initial indexes for primary access patterns:
    #    - idx_payments_order_id              — saga lookup by order
    #    - idx_payments_user_created          — user payment-history listing (DESC by created_at)
    #    - idx_payments_provider_charge       — webhook reconciliation by provider charge id
    #    - idx_refunds_payment_id             — refund history per payment
    #    - idx_provider_webhooks_processed_at — redrive / reprocessing queries
    #    - idx_idempotency_keys_expires_at    — TTL cleanup sweep
    op.create_index(
        "idx_payments_order_id",
        "payments",
        ["order_id"],
    )
    op.create_index(
        "idx_payments_user_created",
        "payments",
        ["user_id", sa.text("created_at DESC")],
    )
    op.create_index(
        "idx_payments_provider_charge",
        "payments",
        ["provider", "provider_charge_id"],
    )
    op.create_index(
        "idx_refunds_payment_id",
        "refunds",
        ["payment_id"],
    )
    op.create_index(
        "idx_provider_webhooks_processed_at",
        "provider_webhooks",
        ["processed_at"],
    )
    op.create_index(
        "idx_idempotency_keys_expires_at",
        "idempotency_keys",
        ["expires_at"],
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production)."""
    # Drop indexes first (reverse order of creation).
    op.drop_index("idx_idempotency_keys_expires_at", table_name="idempotency_keys")
    op.drop_index("idx_provider_webhooks_processed_at", table_name="provider_webhooks")
    op.drop_index("idx_refunds_payment_id", table_name="refunds")
    op.drop_index("idx_payments_provider_charge", table_name="payments")
    op.drop_index("idx_payments_user_created", table_name="payments")
    op.drop_index("idx_payments_order_id", table_name="payments")

    # Drop tables in REVERSE creation order so intra-DB FKs unwind safely.
    op.drop_table("idempotency_keys")
    op.drop_table("provider_webhooks")
    op.drop_table("refunds")
    op.drop_table("payment_attempts")
    op.drop_table("payments")

    # NOTE: pgcrypto extension is intentionally NOT dropped (may be shared with
    # other DB users; upgrade is idempotent so re-application is safe; DROP
    # EXTENSION can fail when other DB objects depend on it).
