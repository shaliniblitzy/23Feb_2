"""indexes_and_constraints

Revision ID: 20260101_000002
Revises: 20260101_000001
Create Date: 2026-01-01 00:00:02.000000

Adds composite UNIQUE constraints (idempotency-related) and CHECK constraints
(enum validation, range validation, currency length) on the 5 payment_db tables
created in revision 20260101_000001.

UNIQUE constraints (4):
  * payment_attempts (payment_id, attempt_no)        — natural key per payment
  * payment_attempts (provider, idempotency_key)     — outbound provider
                                                       idempotency (AAP R-8)
  * provider_webhooks (provider, provider_event_id)  — webhook idempotency
                                                       (AAP R-12)
  * idempotency_keys (key, endpoint)                 — inbound API idempotency
                                                       (AAP R-8)

CHECK constraints (11): enforce enum-value integrity for status / provider /
response_status fields, currency length = 3 (ISO 4217), and attempt_no
positivity. These are a "belt-and-suspenders" companion to the Pydantic
validation performed in src/repository/ and src/domain/ — if either layer is
bypassed (e.g., a raw SQL insert from an operator), the database prevents
invalid values from persisting.

These additions are non-destructive and fully reversible; see ../README.md
"Rollback Policy" for the forward-only production policy.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa  # noqa: F401  (kept for project convention; mirrors script.py.mako)

# revision identifiers, used by Alembic.
revision: str = "20260101_000002"
down_revision: Union[str, Sequence[str], None] = "20260101_000001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Apply forward migration."""
    # ----------------------------------------------------------------------
    # UNIQUE CONSTRAINTS
    # ----------------------------------------------------------------------
    # 1. payment_attempts.(payment_id, attempt_no) — natural key per payment.
    op.create_unique_constraint(
        "uq_payment_attempts_payment_attempt",
        "payment_attempts",
        ["payment_id", "attempt_no"],
    )

    # 2. payment_attempts.(provider, idempotency_key) — outbound provider
    #    idempotency per AAP R-8. Allows safe retries to provider APIs without
    #    risk of double-charging.
    op.create_unique_constraint(
        "uq_payment_attempts_provider_idem",
        "payment_attempts",
        ["provider", "idempotency_key"],
    )

    # 3. provider_webhooks.(provider, provider_event_id) — webhook idempotency
    #    per AAP R-12. Ensures the same Stripe/Razorpay webhook event is never
    #    processed twice even if the provider re-delivers it.
    op.create_unique_constraint(
        "uq_provider_webhooks_provider_event",
        "provider_webhooks",
        ["provider", "provider_event_id"],
    )

    # 4. idempotency_keys.(key, endpoint) — inbound API idempotency per AAP R-8.
    #    Keys are scoped per endpoint so the same key value can be reused across
    #    different operations without collision.
    op.create_unique_constraint(
        "uq_idempotency_keys_key_endpoint",
        "idempotency_keys",
        ["key", "endpoint"],
    )

    # ----------------------------------------------------------------------
    # CHECK CONSTRAINTS
    # ----------------------------------------------------------------------
    # 5. payments.status ∈ PaymentStatus enum values (lowercase).
    op.create_check_constraint(
        "payments_status_enum",
        "payments",
        "status IN ('pending', 'succeeded', 'failed', 'refunded', 'partially_refunded')",
    )

    # 6. payments.provider ∈ ProviderName enum values (AAP R-10).
    op.create_check_constraint(
        "payments_provider_enum",
        "payments",
        "provider IN ('stripe', 'razorpay')",
    )

    # 7. payments.currency length = 3 (ISO 4217).
    op.create_check_constraint(
        "payments_currency_length",
        "payments",
        "char_length(currency) = 3",
    )

    # 8. payment_attempts.provider ∈ ProviderName enum values.
    op.create_check_constraint(
        "payment_attempts_provider_enum",
        "payment_attempts",
        "provider IN ('stripe', 'razorpay')",
    )

    # 9. payment_attempts.attempt_no is positive (1-indexed).
    op.create_check_constraint(
        "payment_attempts_attempt_no_positive",
        "payment_attempts",
        "attempt_no >= 1",
    )

    # 10. payment_attempts.response_status ∈ AttemptOutcome enum values.
    op.create_check_constraint(
        "payment_attempts_response_status_enum",
        "payment_attempts",
        "response_status IN ('success', 'failure', 'timeout', 'rate_limited', 'network_error')",
    )

    # 11. refunds.status ∈ RefundStatus enum values.
    op.create_check_constraint(
        "refunds_status_enum",
        "refunds",
        "status IN ('pending', 'succeeded', 'failed')",
    )

    # 12. refunds.provider ∈ ProviderName enum values.
    op.create_check_constraint(
        "refunds_provider_enum",
        "refunds",
        "provider IN ('stripe', 'razorpay')",
    )

    # 13. refunds.currency length = 3 (ISO 4217).
    op.create_check_constraint(
        "refunds_currency_length",
        "refunds",
        "char_length(currency) = 3",
    )

    # 14. provider_webhooks.provider ∈ ProviderName enum values.
    op.create_check_constraint(
        "provider_webhooks_provider_enum",
        "provider_webhooks",
        "provider IN ('stripe', 'razorpay')",
    )

    # 15. provider_webhooks.status ∈ WebhookStatus enum values.
    op.create_check_constraint(
        "provider_webhooks_status_enum",
        "provider_webhooks",
        "status IN ('verified', 'processed', 'translated', 'failed', 'rejected')",
    )


def downgrade() -> None:
    """Revert migration (local dev + CI round-trip only; never production)."""
    # Drop CHECK constraints in REVERSE order.
    op.drop_constraint(
        "provider_webhooks_status_enum",
        "provider_webhooks",
        type_="check",
    )
    op.drop_constraint(
        "provider_webhooks_provider_enum",
        "provider_webhooks",
        type_="check",
    )
    op.drop_constraint(
        "refunds_currency_length",
        "refunds",
        type_="check",
    )
    op.drop_constraint(
        "refunds_provider_enum",
        "refunds",
        type_="check",
    )
    op.drop_constraint(
        "refunds_status_enum",
        "refunds",
        type_="check",
    )
    op.drop_constraint(
        "payment_attempts_response_status_enum",
        "payment_attempts",
        type_="check",
    )
    op.drop_constraint(
        "payment_attempts_attempt_no_positive",
        "payment_attempts",
        type_="check",
    )
    op.drop_constraint(
        "payment_attempts_provider_enum",
        "payment_attempts",
        type_="check",
    )
    op.drop_constraint(
        "payments_currency_length",
        "payments",
        type_="check",
    )
    op.drop_constraint(
        "payments_provider_enum",
        "payments",
        type_="check",
    )
    op.drop_constraint(
        "payments_status_enum",
        "payments",
        type_="check",
    )

    # Drop UNIQUE constraints in REVERSE order.
    op.drop_constraint(
        "uq_idempotency_keys_key_endpoint",
        "idempotency_keys",
        type_="unique",
    )
    op.drop_constraint(
        "uq_provider_webhooks_provider_event",
        "provider_webhooks",
        type_="unique",
    )
    op.drop_constraint(
        "uq_payment_attempts_provider_idem",
        "payment_attempts",
        type_="unique",
    )
    op.drop_constraint(
        "uq_payment_attempts_payment_attempt",
        "payment_attempts",
        type_="unique",
    )
