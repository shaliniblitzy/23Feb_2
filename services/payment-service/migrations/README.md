# Payment Service — Database Migrations

Forward-only, versioned Alembic migration scripts that create and evolve the Payment Service's private PostgreSQL schema (`payment_db`, encrypted at rest).

## Purpose

This folder contains versioned **Alembic** (Python) migration scripts for the **private PostgreSQL database** `payment_db` owned by the Payment Service.

- Per **AAP R-6** (database per service), no other service reads from or writes to this database. Cross-service references such as `payments.order_id` and `payments.user_id` are stored as **opaque UUIDs without foreign keys** — referential integrity across service boundaries is maintained through Kafka events and the Order Service's saga semantics, never through SQL joins.
- Per **AAP R-9**, migrations **must** be applied automatically — either on service startup (entrypoint or initContainer) or via a dedicated migration Kubernetes Job / CI step. Pods must not begin serving traffic until all pending migrations have completed successfully.
- Per **AAP R-25**, the database connection URL is supplied at runtime via the `POSTGRES_URL` environment variable. **No credentials live in this folder.**
- Per **AAP R-8** (unique to this service among the relational stores in the monorepo), `payment_db` is **encrypted at rest** via two complementary mechanisms: (a) cluster-level encryption (TDE / EBS / cloud-provider managed disk encryption — an operator concern enforced by Terraform / Helm) and (b) `pgcrypto` **column-level encryption** for the most sensitive columns (`provider_charge_id`, `last4`, `brand`, `holder_name`, `provider_refund_id`). See [Encryption at Rest](#encryption-at-rest-aap-r-8).

## Toolchain

Read this section first — it disambiguates the SQLAlchemy / psycopg3 / Alembic interplay that is identical for both this service and the Notification Service.

- **Migration runner:** Alembic (`alembic>=1.13.0,<2.0.0`, pinned in [`../requirements.txt`](../requirements.txt)).
- **Driver dialect:** SQLAlchemy 2.0 with the **psycopg v3** dialect — URL prefix `postgresql+psycopg://`.
  - **CRITICAL:** Use `postgresql+psycopg://` (psycopg v3) — **never** `postgresql+psycopg2://` (legacy v2). The service's [`../requirements.txt`](../requirements.txt) declares `psycopg[binary,pool]>=3.1.18,<4.0.0`; psycopg2 is not installed and `env.py` rejects `postgresql+psycopg2://` URLs with an explicit `RuntimeError` rather than letting SQLAlchemy raise an opaque `ModuleNotFoundError`.
- **Required PostgreSQL extension:** `pgcrypto` (enabled by the initial migration). Provides `gen_random_uuid()` for primary keys **and** `pgp_sym_encrypt()` / `pgp_sym_decrypt()` helpers used by the application's column-level encryption layer (AAP R-8).
- **Why Alembic + SQLAlchemy when the service uses async SQLAlchemy at runtime?**
  - The Payment Service's runtime data access uses **SQLAlchemy 2.0 async + psycopg v3** with an async connection pool (different from the Notification Service, which uses pure psycopg3).
  - Alembic itself runs **synchronously** through SQLAlchemy Core's sync engine, even when the application uses the async engine — this is the upstream Alembic execution model and is the reason `env.py` constructs a sync `engine_from_config(..., poolclass=pool.NullPool)` rather than reusing the application's async engine.
  - Migrations are written **imperatively** via `op.create_table()`, `op.create_index()`, `op.create_check_constraint()`, and `op.execute()` — never via `--autogenerate`.
  - `target_metadata = None` in [`./env.py`](./env.py) reflects this: there is no SQLAlchemy `MetaData()` to compare against because the schema is defined exclusively by hand-written revision files. In a payments domain every DDL must be reviewed line-by-line for compliance and encryption-at-rest implications; we do not want a missing ORM model attribute to silently drop a column on apply.
  - This is the same pattern the Notification Service follows; see [`../../notification-service/migrations/README.md`](../../notification-service/migrations/README.md) for additional rationale.
- **Service-specific Alembic version table:** This service uses **`alembic_version_payment_service`** (not the default `alembic_version`). The custom name lets operators safely run all services' migrations against a single Postgres cluster in local development without colliding on a shared bookkeeping table. The name is set in [`./env.py`](./env.py) via `context.configure(version_table="alembic_version_payment_service", ...)` on **both** the offline and online paths — see the constant `_VERSION_TABLE_NAME` for the single source of truth.

## File Layout

```text
migrations/
├── alembic.ini                                                   # Alembic CLI config; sqlalchemy.url is empty by design
├── env.py                                                        # Online + offline runtime; reads POSTGRES_URL and rewrites the dialect to postgresql+psycopg
├── script.py.mako                                                # Mako template used to generate new revision files
├── README.md                                                     # This file
└── versions/                                                     # Individual revision files (chronologically ordered)
    ├── 20260101_000001_initial_schema.py                         # CREATE TABLE for all 5 tables; CREATE EXTENSION pgcrypto
    ├── 20260101_000002_indexes_and_constraints.py                # Composite indexes + CHECK constraints + UNIQUE guards
    └── …                                                         # Subsequent revisions added by future feature work
```

- **`alembic.ini`** — Alembic CLI configuration. The `sqlalchemy.url` key is intentionally **empty**; `env.py` populates it at runtime from `POSTGRES_URL` (AAP R-25).
- **`env.py`** — Defines both online and offline migration modes; reads `POSTGRES_URL` from the environment, rewrites a bare `postgresql://` URL to `postgresql+psycopg://`, and propagates `version_table=alembic_version_payment_service` to both `context.configure()` calls.
- **`script.py.mako`** — Mako template used by `alembic revision -m "..."` to scaffold new revision files.
- **`versions/20260101_000001_initial_schema.py`** — Enables the `pgcrypto` extension and creates all five tables (`payments`, `payment_attempts`, `refunds`, `provider_webhooks`, `idempotency_keys`) with primary keys, columns, and the UNIQUE constraints that enforce the inbound idempotency keys (`provider_webhooks (provider, provider_event_id)`, `idempotency_keys (key, endpoint)`, `payment_attempts (provider, idempotency_key)`).
- **`versions/20260101_000002_indexes_and_constraints.py`** — Adds composite indexes (for example `payments (user_id, created_at DESC)` and `payments (provider, provider_charge_id)`) and CHECK constraints encoding the status / provider / outcome / webhook-status enum value sets documented in [Cross-Coupling Awareness](#cross-coupling-awareness).

The `<YYYYMMDD>_<HHMMSS>` timestamp prefix is automatically applied by Alembic via the `file_template` setting in [`./alembic.ini`](./alembic.ini).

## Naming Convention

- Revision filenames: `<YYYYMMDD>_<HHMMSS>_<snake_case_slug>.py`.
- This is configured via the `file_template` setting in [`./alembic.ini`](./alembic.ini); the `timezone = UTC` setting in the same section guarantees deterministic, contributor-independent timestamps.
- Each revision file declares `revision = "<id>"` at the top (for example, `revision = "20260101_000001"`); the file's identifier matches the timestamp prefix exactly.
- New revisions are generated by `alembic revision -m "short description"`. The `--autogenerate` flag is **not used** because `target_metadata = None` (see [Toolchain](#toolchain)); migrations are written by hand.
- **Never reuse** a revision id.
- **Never edit** a revision file that has already been applied in any environment — see [Rollback Policy](#rollback-policy).

## Rollback Policy

Migrations in this folder are **forward-only** in production.

- Do **not** edit a migration that has been applied in any environment.
- Each Alembic revision **does** define a `downgrade()` function (best practice for reversibility), but `downgrade` is reserved for **local development and CI round-trip tests** — never for production rollback.
- To undo a change in production, **create a new forward migration** (for example, `20260201_000001_revert_xyz.py`) that contains the corrective DDL.
- For catastrophic recovery, **restore from a database snapshot** rather than running a reverse migration against live data.
- **Payment-domain note:** Backwards-incompatible schema changes (column drops, type narrowing, NOT-NULL tightening) **must** be deployed as a multi-step expand-contract pattern. The Payment Service's compliance posture (PCI-DSS scope reduction, encryption-at-rest, immutable audit trails on `payment_attempts` / `provider_webhooks`) makes destructive single-shot rollbacks unsafe — once a chargeable row has been written it is part of the financial record.

## Idempotence

Every operation in the migration files is safe to reapply.

- Raw-SQL paths invoked via `op.execute()` use `IF NOT EXISTS` / `OR REPLACE` semantics where applicable. The `pgcrypto` extension is enabled with `CREATE EXTENSION IF NOT EXISTS pgcrypto` in the initial revision and is **intentionally not dropped** in `downgrade()` because the extension may be shared with other services co-located on the same dev database.
- For schema-builder calls such as `op.create_table()`, Alembic's revision tracking via the service-specific `alembic_version_payment_service` bookkeeping table prevents reapplication of the same revision against the same database.
- CI runs the round-trip sequence `alembic upgrade head` → `alembic downgrade base` → `alembic upgrade head` to verify reversibility and idempotence on every pull request that touches this folder.

## Schema Summary

The five tables required by **AAP Section 0.4.4** are created by the initial migration; performance indexes and CHECK constraints are added by a subsequent migration.

| Table | Purpose | Created by |
|-------|---------|------------|
| `payments` | One row per payment intent (id, order_id, user_id, amount, currency, status, provider, encrypted card metadata) | `20260101_000001_initial_schema.py` |
| `payment_attempts` | One row per outbound provider API call; outbound idempotency-key store (AAP R-8); per-attempt audit | `20260101_000001_initial_schema.py` |
| `refunds` | One row per refund issued (id, payment_id, amount, status, encrypted provider_refund_id, reason) | `20260101_000001_initial_schema.py` |
| `provider_webhooks` | One row per inbound webhook; UNIQUE on `(provider, provider_event_id)` enforces AAP R-12 idempotency | `20260101_000001_initial_schema.py` |
| `idempotency_keys` | One row per client-supplied `Idempotency-Key` header value; AAP R-8 inbound API idempotency | `20260101_000001_initial_schema.py` |

## Schema Isolation (AAP R-6)

This database is **private** to the Payment Service.

- **No foreign keys** point at tables in any other service's database (`auth_db`, `user_db`, `product_db`, `order_db`, `inventory_db`, `notification_db`).
- `payments.order_id` and `payments.user_id` are stored as **opaque UUIDs**. Cross-service joins are forbidden — the Payment Service learns about external entities exclusively via Kafka events it consumes (`order.created`, `order.cancelled`) and via REST calls fronted by the API Gateway.
- `refunds.payment_id` and `payment_attempts.payment_id` **are** foreign keys (within `payment_db`) because all three tables live in the same private database; intra-database referential integrity is enforced.
- `idempotency_keys` does **not** carry a foreign key to any payment table — it is a pure write-through cache for inbound API replays and intentionally outlives the lifetime of any individual payment row.

## Encryption at Rest (AAP R-8)

`payment_db` is the only relational database in this monorepo that is required to be encrypted at rest. Two complementary mechanisms achieve this:

- **Cluster-level (volume-level) encryption** is provided by the underlying storage layer — PostgreSQL on AWS RDS / Cloud SQL / Azure Database with the cluster's encryption-at-rest flag enabled, or a self-managed cluster on encrypted EBS / disk volumes. This is an **operator concern** declared in Terraform / Helm and is **not enforced by these migrations**. Operators are responsible for verifying the storage encryption flag in every non-local environment.
- **Column-level encryption** via `pgcrypto` is applied to the following columns at write time by the application's `../src/repository/encryption.py` helper. The migrations only ensure the columns exist (as `BYTEA` / `TEXT` where appropriate) and that the `pgcrypto` extension is enabled; the actual `pgp_sym_encrypt()` / `pgp_sym_decrypt()` calls happen in the application layer:
  - `payments.provider_charge_id` — provider-issued opaque token (never raw PAN).
  - `payments.last4` — last 4 digits of card (PCI-allowed display element, encrypted at rest as defense in depth).
  - `payments.brand` — card brand (Visa, Mastercard) — encrypted as PII even though not strictly card data.
  - `payments.holder_name` — cardholder name — PII.
  - `refunds.provider_refund_id` — provider-issued opaque refund token.
- The encryption key is sourced from a KMS (AWS KMS / GCP KMS / HashiCorp Vault Transit) at runtime via envelope encryption; key references live in env vars, never in the database. See [`../.env.example`](../.env.example) for `ENCRYPTION_PROVIDER` and `ENCRYPTION_KMS_KEY_ARN`.
- **Idempotency keys** (`idempotency_keys.key`, `payment_attempts.idempotency_key`) are **not** considered sensitive and are **not** encrypted: they are opaque, high-entropy values whose secrecy is not load-bearing for security — duplicate-detection only requires equality comparison, not confidentiality.

## Webhook Idempotency (AAP R-12)

The schema enforces at-most-once processing of provider webhooks at the database layer:

- The `provider_webhooks` table carries a `UNIQUE (provider, provider_event_id)` constraint.
- When the application receives a webhook (after HMAC signature verification — AAP R-12), it `INSERT`s a row keyed on `(provider, provider_event_id)`. Duplicate deliveries from Stripe or Razorpay produce a unique-violation error which the application catches and translates into a `200 OK` response (the event is already processed) without re-running side effects.
- This pattern guarantees **at-most-once processing** of provider webhooks regardless of how many times Stripe or Razorpay re-deliver them. The provider's at-least-once delivery semantics combine with the database-enforced uniqueness to give effectively-once observable behavior.

## Application Workflow

Both AAP R-9-compliant workflows are supported. Pick one per environment and stick to it.

- **On service startup** — preferred for local development and single-replica environments. An entrypoint script runs `alembic upgrade head` **before** the FastAPI / uvicorn process starts accepting traffic. A failure is fatal: the container exits non-zero and the readiness probe stays `503` (AAP R-19).
- **Dedicated migration Job / CI step** — **strongly recommended for production**. A Kubernetes `Job` (or a CI stage) runs `alembic upgrade head` exactly once per deployment; service pods only start once the Job has succeeded. This pattern is safer for multi-replica zero-downtime rollouts because it eliminates the race between concurrent replicas all trying to migrate the same database at startup.
- For payment-service specifically, the K8s Job pattern is **strongly recommended** in production because the database is encrypted and migration failures should be isolated to a Job pod with explicit retry semantics rather than entangled with the runtime service's pod lifecycle.

## Local Development Workflow

Concrete commands for day-to-day development:

```bash
# From the service root: services/payment-service/
# Ensure POSTGRES_URL is set (e.g., from a local .env file loaded into the shell).
# Use a development-only password and a local Postgres instance:
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/payment_db"

# Activate venv & install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Switch into the migrations directory
cd migrations

# Apply all pending migrations
alembic upgrade head

# Show current revision
alembic current

# Show full revision history
alembic history --verbose

# Generate a new (manual / blank) revision
alembic revision -m "short description of change"

# Roll back the most recent migration (LOCAL DEV ONLY)
alembic downgrade -1

# Round-trip test (CI parity)
alembic upgrade head && alembic downgrade base && alembic upgrade head
```

The `postgres:postgres` credentials above are placeholder values for a developer's local Docker container only — never use them anywhere else.

## Environment Variables

`env.py` reads exactly the following variables:

- **`POSTGRES_URL`** — **REQUIRED** at runtime. Format: `postgresql://<user>:<pass>@<host>:<port>/payment_db`. A bare `postgresql://` scheme is rewritten to `postgresql+psycopg://` automatically by `env.py`; an explicit `postgresql+psycopg://` URL is also accepted unchanged. An explicit `postgresql+psycopg2://` URL is rejected with an actionable `RuntimeError` because psycopg2 is not pinned in `../requirements.txt`.
- **`ALEMBIC_LOG_LEVEL`** — *Optional.* Default: `WARNING`. Set to `INFO` or `DEBUG` for verbose Alembic logging during debugging; an unknown value falls back to `WARNING`.

**No credentials live in `alembic.ini`** (AAP R-25). The `sqlalchemy.url` key in `alembic.ini` is intentionally empty; `env.py` populates it at runtime from `POSTGRES_URL`. See the operator-facing [`../.env.example`](../.env.example) for the canonical environment-variable template (including the `ENCRYPTION_PROVIDER` / `ENCRYPTION_KMS_KEY_ARN` variables consumed by the application's encryption layer at runtime).

## Troubleshooting

The most common issues encountered while running migrations:

- **`Can't load plugin: sqlalchemy.dialects:postgresql.psycopg`** — install `psycopg[binary,pool]>=3` (already declared in [`../requirements.txt`](../requirements.txt)). Verify with `pip show psycopg` (the package is named `psycopg`, not `psycopg2` or `psycopg3`).
- **`No module named 'psycopg2'`** — **do not** install `psycopg2`. The URL must use `postgresql+psycopg://` (v3), not `postgresql+psycopg2://`. `env.py` performs this rewrite automatically when given a bare `postgresql://` URL; if you provided an explicit `postgresql+psycopg2://` URL, change it to `postgresql+psycopg://` (or strip the `+psycopg2` suffix and let `env.py` rewrite it).
- **`Target database is not up to date`** — run `alembic upgrade head` first. Alembic refuses to generate a new revision until the database is at the latest known head.
- **`Multiple head revisions detected`** — indicates a merge conflict between two parallel migration branches. Reconcile with `alembic merge -m "merge heads" <head1> <head2>` and commit the resulting merge revision.
- **`relation "alembic_version_payment_service" does not exist`** — the database has never had Alembic applied. Just run `alembic upgrade head`; Alembic auto-creates the (service-specific) version table on first apply.
- **`extension "pgcrypto" is not available`** — the running PostgreSQL build does not include contrib modules. Use a contrib-enabled image (for example `postgres:16-bookworm`) or install the `postgresql-contrib` package on the host.
- **`permission denied to create extension "pgcrypto"`** — Alembic is connecting as a non-superuser. Either (a) grant the user the `CREATE` privilege on the database and `SUPERUSER` (dev only), (b) pre-create the extension as a superuser one-time before the migration runs, or (c) execute `CREATE EXTENSION IF NOT EXISTS pgcrypto` in a separate bootstrap step (Terraform / Helm hook) before invoking Alembic.
- **`unique violation on alembic_version_payment_service`** — should not happen with the service-specific version table name; if it does, multiple migration runners are racing. Ensure exactly one runner (initContainer **or** Job, never both) applies migrations per deployment.

## Testing

An integration test under [`../tests/integration/`](../tests) (authored by sibling agents) exercises the migration suite end-to-end:

- Uses **Testcontainers** to spin up a fresh `postgres:16` container per test run.
- Sets `POSTGRES_URL` to the container's connection string.
- Runs `alembic upgrade head` and asserts:
  - All five tables exist with the expected columns and types (verified via `information_schema.columns`).
  - The `pgcrypto` extension is enabled (`SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'`).
  - The `alembic_version_payment_service` table exists with exactly one row pointing at the latest revision id.
  - The UNIQUE constraint on `provider_webhooks (provider, provider_event_id)` is present (the inbound webhook idempotency key per AAP R-12).
  - The UNIQUE constraint on `idempotency_keys (key, endpoint)` is present (inbound API idempotency per AAP R-8).
  - The UNIQUE constraint on `payment_attempts (provider, idempotency_key)` is present (outbound provider idempotency per AAP R-8).
  - The composite indexes (for example `payments (user_id, created_at DESC)` and `payments (provider, provider_charge_id)`) exist.
  - All CHECK constraints are present (status / provider / outcome / webhook-status enum value sets, currency length, `attempt_no >= 1`).
- Re-runs `alembic upgrade head` to verify no-op idempotence on a database already at head.
- Round-trips `alembic downgrade base` then `alembic upgrade head` to verify reversibility.
- Verifies the encryption helpers work end-to-end: writes a row with `pgp_sym_encrypt('test', 'key')` and reads it back via `pgp_sym_decrypt` to confirm the extension is functional, not merely installed.

## Cross-Coupling Awareness

The CHECK-constraint string literals encoded in these migrations are **coupled** with the SQLAlchemy / Pydantic models in `../src/repository/models.py` (or its equivalent) and with the `StrEnum` definitions in `../src/domain/payment_types.py`. Changing a string literal in one place without coordinated changes in the other(s) results in **runtime CHECK-constraint violations** on insert.

The exact coupled value sets:

- `payments.status` uses `'pending' | 'succeeded' | 'failed' | 'refunded' | 'partially_refunded'` (lowercase) — must match the `PaymentStatus` `StrEnum` values.
- `payments.provider`, `refunds.provider`, `payment_attempts.provider`, and `provider_webhooks.provider` all use `'stripe' | 'razorpay'` (lowercase) — must match the `ProviderName` `StrEnum` values per AAP R-10 (dual-provider concurrent integration).
- `refunds.status` uses `'pending' | 'succeeded' | 'failed'` (lowercase) — must match the `RefundStatus` `StrEnum` values.
- `payment_attempts.response_status` uses `'success' | 'failure' | 'timeout' | 'rate_limited' | 'network_error'` (lowercase) — must match the `AttemptOutcome` `StrEnum` values.
- `provider_webhooks.status` uses `'verified' | 'processed' | 'translated' | 'failed' | 'rejected'` (lowercase) — must match the `WebhookStatus` `StrEnum` values.

When you change a domain enum, you **must** add a forward migration (see [Rollback Policy](#rollback-policy)) that updates the corresponding CHECK constraint, and vice versa. The five enum families above (vs. the Notification Service's four) reflect the additional state machines payment processing inherits from PCI flows: `WebhookStatus` and `AttemptOutcome` have no analogues in the notification domain.

## Key Rotation

The migration scripts here do **not** rotate the column-level encryption key — rotation is an **operational task**, not a schema change.

- Operators rotate the KMS-issued key encryption key (KEK) via the runbook at [`../../../docs/runbook/payment-service.md`](../../../docs/runbook/payment-service.md). The high-level procedure: (1) create a new KMS key version, (2) re-encrypt rows in batches with both old and new keys readable, (3) cut over the active key reference in the application config, (4) sweep any rows still encrypted under only the old key.
- The `payment_db` schema supports rotation natively because pgcrypto's `pgp_sym_encrypt` / `pgp_sym_decrypt` accept the key as a parameter at query time — the schema does **not** embed any key reference. Rotation therefore requires zero migrations.
- The `ENCRYPTION_DATA_KEY_CACHE_TTL_SECONDS` env var (see [`../.env.example`](../.env.example)) bounds how long an old data encryption key (DEK) may remain in the in-process cache after a KEK rotation — set it short enough to make rotation propagate within an acceptable window.

## Related Files & References

- [`../README.md`](../README.md) — Service-level README
- [`../config/default.yaml`](../config/default.yaml) — Database pool sizing, statement timeout, encryption provider configuration
- [`../.env.example`](../.env.example) — `POSTGRES_URL`, `ENCRYPTION_PROVIDER`, `ENCRYPTION_KMS_KEY_ARN` template (no real credentials)
- [`../requirements.txt`](../requirements.txt) — Python dependency manifest (Alembic, SQLAlchemy 2.0 async, psycopg v3, pgcrypto-aware crypto helpers)
- [`../Dockerfile`](../Dockerfile) — References this folder via `COPY --chown=${APP_USER}:${APP_USER} migrations/ ./migrations/`
- [`../../notification-service/migrations/README.md`](../../notification-service/migrations/README.md) — Sibling Alembic reference (canonical pattern; this service adapts it for 5-table encryption-at-rest payment_db)
- [`../../../docs/architecture/data-stores.md`](../../../docs/architecture/data-stores.md) — Database-per-service rationale and encryption-at-rest specifics
- [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md) — System architecture diagram
- [`../../../docs/runbook/payment-service.md`](../../../docs/runbook/payment-service.md) — Operational runbook (key rotation, DLQ replay, webhook secret rotation)

AAP citations (non-clickable):

- AAP Section 0.4.4 — Database schema for `payment_db` (5 tables)
- AAP Section 0.5.2.5 — Per-service migrations folder requirement
- AAP R-6 — Database per service strict isolation (no cross-service foreign keys)
- AAP R-7 — Polyglot persistence (PostgreSQL for `payment_db`)
- AAP R-8 — **Encryption at rest + idempotency keys persisted (unique to Payment Service)**
- AAP R-9 — Migrations under owning service, applied automatically on startup or via dedicated Job
- AAP R-10 — Dual-provider concurrent integration (Stripe + Razorpay) behind `PaymentProvider`
- AAP R-12 — Webhook signature verification + idempotency keyed on provider event id
- AAP R-25 — Secrets never in source or configuration files
