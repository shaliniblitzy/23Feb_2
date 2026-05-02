# User Service — Database Migrations

Forward-only, versioned Alembic migration scripts that create and evolve the User Service's private PostgreSQL schema (`user_db`).

## Purpose

This folder contains versioned **Alembic** (Python) migration scripts for the **private PostgreSQL database** `user_db` owned by the User Service.

- Per **AAP R-6** (database per service), no other service reads from or writes to this database. Cross-service references — most notably the User Service's `external_auth_id` field, which is the stable identifier minted by the Auth Service — are stored as **opaque strings without foreign keys**; integrity across service boundaries is maintained through Kafka events (`user.registered`, `user.updated`, `user.deleted`), never through SQL joins.
- Per **AAP R-9**, migrations **must** be applied automatically — either on service startup (entrypoint or initContainer) or via a dedicated migration Kubernetes Job / CI step. Pods must not begin serving traffic until all pending migrations have completed successfully. The parent [`../.env.example`](../.env.example) exposes `RUN_MIGRATIONS_ON_STARTUP=true` to control startup-time application.
- Per **AAP R-25**, the database connection URL is supplied at runtime via the `POSTGRES_URL` environment variable. **No credentials live in this folder.** `user_db` stores PII (email, phone, postal addresses, date of birth), so a leaked URL would be a privacy incident.
- Per **AAP R-26**, every domain table includes `created_at` and `updated_at` `TIMESTAMPTZ` columns to support audit and observability queries; these columns are populated by application code on every write.

## Toolchain

Read this section first — it documents the **Alembic + SQLAlchemy + psycopg3 v3 dialect** stack and the service-specific Alembic version table that operators must understand before running any migration command.

- **Migration runner:** [Alembic](https://alembic.sqlalchemy.org/) (`alembic>=1.13.0,<2.0.0`, pinned in [`../requirements.txt`](../requirements.txt)).
- **Driver dialect:** SQLAlchemy 2.0 with the **psycopg v3** dialect — URL prefix `postgresql+psycopg://`.
  - **CRITICAL:** Use `postgresql+psycopg://` (psycopg v3) — **never** `postgresql+psycopg2://` (legacy v2). The service's [`../requirements.txt`](../requirements.txt) declares `psycopg[binary,pool]>=3.1,<4.0`; psycopg2 is not installed. `env.py` automatically rewrites a bare `postgresql://` URL to use the psycopg v3 dialect, and explicitly rejects `postgresql+psycopg2://` URLs with an actionable `RuntimeError` rather than letting SQLAlchemy raise an opaque `ModuleNotFoundError` deep inside its plugin loader.
- **Required PostgreSQL extension:** `pgcrypto` (enabled by the initial migration). Provides `gen_random_uuid()` for primary keys (UUID v4) — the User Service uses UUIDs for `users.id`, `user_addresses.id`, and the correlation IDs propagated through every event row in `events_outbox`.
- **Why Alembic with `target_metadata = None`?**
  - The schema is defined imperatively in revision files via `op.create_table()`, `op.create_index()`, `op.create_check_constraint()`, and `op.execute()` for raw SQL where needed (for example, partial indexes for soft-delete and outbox-dispatch scans, and the `LOWER(email)` UNIQUE expression index that enforces case-insensitive email uniqueness).
  - Autogenerate is intentionally disabled because every DDL change in a service that handles PII (email, phone, addresses, DOB) must be reviewed line by line for data-protection implications.
  - `target_metadata = None` in [`./env.py`](./env.py) reflects this: there is no SQLAlchemy `MetaData()` to compare against because the schema is defined exclusively by hand-written revision files.
  - This is the same pattern the sibling Postgres-backed services follow; see [`../../notification-service/migrations/README.md`](../../notification-service/migrations/README.md) and [`../../order-service/migrations/README.md`](../../order-service/migrations/README.md) for additional rationale.
- **Service-specific Alembic version table:** This service uses **`alembic_version_user_service`** (not the default `alembic_version`). The custom name lets operators safely run all services' migrations against a single Postgres cluster in local development without colliding on a shared bookkeeping table — defense in depth even though AAP R-6 forbids that arrangement for production. The name is set in [`./env.py`](./env.py) via `context.configure(version_table="alembic_version_user_service", ...)` on **both** the offline and online migration paths. The same name is documented as `MIGRATIONS_TABLE` in [`../.env.example`](../.env.example) so operators have a single authoritative source of truth for the version-table identifier.

## File Layout

```text
migrations/
├── alembic.ini                                  # Alembic CLI config; reads POSTGRES_URL via env.py
├── env.py                                       # Alembic runtime environment (online + offline modes)
├── script.py.mako                               # Mako template for new revision files
├── README.md                                    # This file
└── versions/                                    # Individual revision files (chronologically ordered)
    ├── 0001_create_users_table.py               # CREATE users + pgcrypto extension
    ├── 0002_create_user_profiles_table.py       # CREATE user_profiles
    ├── 0003_create_user_preferences_table.py    # CREATE user_preferences
    ├── 0004_create_user_addresses_table.py      # CREATE user_addresses
    └── 0005_create_events_outbox_table.py       # CREATE events_outbox (outbox pattern)
```

- **`alembic.ini`** — Alembic CLI configuration. The `sqlalchemy.url` key is intentionally **empty**; `env.py` populates it at runtime from `POSTGRES_URL` (AAP R-25).
- **`env.py`** — Defines both online and offline migration modes; reads `POSTGRES_URL` from the environment, rewrites a bare `postgresql://` URL to `postgresql+psycopg://`, rejects `postgresql+psycopg2://`, and propagates `version_table="alembic_version_user_service"` to both `context.configure()` calls.
- **`script.py.mako`** — Mako template used by `alembic revision -m "..."` to scaffold new revision files.
- **`versions/0001_create_users_table.py`** — Enables the `pgcrypto` extension and creates the `users` aggregate-root table with the UNIQUE constraints `users_external_auth_id_uniq` and `users_email_uniq` (case-insensitive), the CHECK constraint on `status`, and the partial index `users_active_idx ON (deleted_at) WHERE deleted_at IS NULL`.
- **`versions/0002_create_user_profiles_table.py`** — Creates the `user_profiles` table with the intra-database FK `user_profiles.user_id REFERENCES users(id) ON DELETE RESTRICT`.
- **`versions/0003_create_user_preferences_table.py`** — Creates the `user_preferences` table with the intra-database FK `user_preferences.user_id REFERENCES users(id) ON DELETE RESTRICT`, plus CHECK constraints on `preferred_currency` (`^[A-Z]{3}$`) and the quiet-hours range.
- **`versions/0004_create_user_addresses_table.py`** — Creates the `user_addresses` table with the intra-database FK `user_addresses.user_id REFERENCES users(id) ON DELETE CASCADE`, the CHECK constraint on `country` (`^[A-Z]{2}$`), and the partial UNIQUE `user_addresses_default_uniq ON (user_id, type) WHERE is_default = TRUE` (one default per address type per user).
- **`versions/0005_create_events_outbox_table.py`** — Creates the `events_outbox` table and the partial index `events_outbox_pending_idx ON (created_at) WHERE published_at IS NULL` for fast dispatcher polling.

This service uses the **ordinal naming convention** (`0001`–`0005`) for its bootstrap revisions to make the linear chain immediately obvious to first-time readers; subsequent feature work will use the timestamped `file_template` (`<YYYYMMDD>_<HHMMSS>_<slug>.py`) configured in [`./alembic.ini`](./alembic.ini).

## Naming Convention

- **Bootstrap revisions** use ordinal four-digit prefixes (`0001`, `0002`, …, `0005`) for clarity in the linear chain.
- **Subsequent revisions** generated via `alembic revision -m "..."` use the `<YYYYMMDD>_<HHMMSS>_<snake_case_slug>.py` template configured in [`./alembic.ini`](./alembic.ini). Both formats coexist in the same `versions/` directory; Alembic resolves the dependency graph from the in-file `revision` / `down_revision` chain, **not** from the filename, so the two conventions interoperate seamlessly.
- Each revision file declares `revision = "<id>"` at the top whose value matches the filename's prefix exactly (`"0001"` for `0001_create_users_table.py`).
- New revisions are generated by `alembic revision -m "short description"`. The `--autogenerate` flag is **not used** because `target_metadata = None` (see [Toolchain](#toolchain)); migrations are written by hand.
- **Never reuse** a revision id.
- **Never edit** a revision file that has already been applied in any environment — see [Rollback Policy](#rollback-policy).

## Rollback Policy

Migrations in this folder are **forward-only** in production.

- Do **not** edit a migration that has been applied in any environment.
- Each Alembic revision **does** define a `downgrade()` function (best practice for reversibility), but `downgrade` is reserved for **local development and CI round-trip tests** — never for production rollback.
- To undo a change in production, **create a new forward migration** (for example, `<timestamp>_revert_xyz.py`) that contains the corrective DDL.
- For catastrophic recovery, **restore from a database snapshot** rather than running a reverse migration against live data.
- **PII-domain note:** Backwards-incompatible schema changes (column drops, type narrowing, NOT-NULL tightening, removing a value from a `users.status` or `user_addresses.type` CHECK constraint) **must** be deployed in a multi-step expand-contract pattern. The User Service's data-protection posture (storing email, phone, addresses, date of birth) makes destructive single-shot rollbacks unsafe — partial rollbacks can desynchronize PII redaction state and violate GDPR right-to-erasure guarantees.

## Idempotence

Every operation in the migration files is safe to reapply.

- Raw-SQL paths invoked via `op.execute()` use `IF NOT EXISTS` semantics where applicable (extensions, partial indexes, expression indexes).
- For schema-builder calls such as `op.create_table()`, Alembic's revision tracking via the service-specific `alembic_version_user_service` bookkeeping table prevents reapplication of the same revision against the same database.
- The `pgcrypto` extension is enabled with `CREATE EXTENSION IF NOT EXISTS pgcrypto` in the initial revision and is **intentionally not dropped** in `downgrade()` because the extension may be shared with other services co-located on the same dev database.
- CI runs the round-trip sequence `alembic upgrade head` → `alembic downgrade base` → `alembic upgrade head` to verify reversibility and idempotence on every pull request that touches this folder.

## Schema Summary

The four user-domain tables required by **AAP Section 0.4.4** plus the user-service-specific `events_outbox` transactional outbox table are created by the bootstrap migrations; all performance indexes and CHECK constraints are added inline within the same revision that creates the parent table.

| Table | Purpose | Created by |
|-------|---------|------------|
| `users` | Root identity record; one row per user (`id` UUID PK, `external_auth_id` UNIQUE [Auth Service id], `email` UNIQUE case-insensitive, `status`, `created_at`, `updated_at`, `deleted_at`, `version` for optimistic concurrency) | `0001_create_users_table.py` |
| `user_profiles` | One-to-one extended profile attributes (`first_name`, `last_name`, `display_name`, `phone`, `avatar_url`, `date_of_birth`, `gender`, `locale`, `timezone`) | `0002_create_user_profiles_table.py` |
| `user_preferences` | One-to-one notification preferences and regional settings (`email_opt_in`, `sms_opt_in`, `marketing_opt_in`, `preferred_language`, `preferred_currency`, `quiet_hours_start`, `quiet_hours_end`, `channel_overrides` JSONB) | `0003_create_user_preferences_table.py` |
| `user_addresses` | One-to-many address book (`id`, `user_id` FK, `type` [billing/shipping/other], `is_default`, `line1`/`line2`/`city`/`state`/`postal_code`, `country` ISO-3166, `phone`, `is_verified`) | `0004_create_user_addresses_table.py` |
| `events_outbox` | Transactional outbox for `user.updated` / `user.deleted` event publication; ensures atomic write-then-publish via a background dispatcher | `0005_create_events_outbox_table.py` |

## Schema Isolation (AAP R-6)

This database is **private** to the User Service.

- **No foreign keys** point at tables in any other service's database (`auth_db`, `order_db`, `payment_db`, `inventory_db`, `notification_db`, `product_db`).
- `users.external_auth_id` is stored as an opaque `VARCHAR(128)` referencing the Auth Service's user identifier; cross-service joins are forbidden — the User Service learns about external entities exclusively via the `user.registered` event it consumes.
- `user_profiles.user_id`, `user_preferences.user_id`, and `user_addresses.user_id` **are** foreign keys (within `user_db`) because all four tables live in the same private database; intra-database referential integrity is enforced at the SQL layer.
- `user_profiles` and `user_preferences` use `ON DELETE RESTRICT` to preserve referential integrity even on user deletion — soft-delete via `users.deleted_at` keeps these rows intact for audit and forensic queries; hard deletion is performed only by the GDPR-erasure tooling described in [`../../../docs/runbook/user-service.md`](../../../docs/runbook/user-service.md).
- `user_addresses` uses `ON DELETE CASCADE` because addresses are owned by their user and have no independent meaning once the user is hard-deleted.
- `events_outbox.aggregate_id` is **not** a foreign key. The outbox is purely a publication staging area; rows persist past the lifetime of the source aggregate for forensics and DLQ replay.

## Idempotent Upsert via `external_auth_id` (User Service-Specific)

The User Service consumes `user.registered` events from Kafka (emitted by the Auth Service). Kafka's at-least-once delivery semantics mean duplicate events **must** be tolerated at the consumer.

- The `users.external_auth_id` column carries a UNIQUE constraint named `users_external_auth_id_uniq`.
- The consumer issues `INSERT ... ON CONFLICT (external_auth_id) DO UPDATE` to make duplicate `user.registered` deliveries safe — the second delivery is observed as a no-op upsert that updates `updated_at` to NOW() but leaves identity-bearing columns untouched.
- The consumer commits Kafka offsets **only** after the durable Postgres write succeeds. Failure routes to `user.registered.dlq` after exhausting the retry policy declared in [`../config/default.yaml`](../config/default.yaml).
- Any future migration that drops or renames `external_auth_id` **must** first migrate consumer code to a different upsert key; doing so in a single migration would silently break duplicate-tolerance and create a Sev-2 ingestion incident.

## Optimistic Concurrency (User Service-Specific)

The User Service uses optimistic locking (rather than row-level pessimistic locks) to coordinate concurrent updates between the user's own client, admin tools, and the `user.registered` consumer.

- The `users.version` column (INTEGER, NOT NULL, DEFAULT 0) is the optimistic-lock token.
- All UPDATE statements on `users` are conditioned on `version` and increment it atomically: `UPDATE users SET ..., version = version + 1, updated_at = NOW() WHERE id = :id AND version = :expected_version`. A row count of zero indicates a stale-version conflict.
- Concurrent client and admin updates collide deterministically; the loser receives a `409 Conflict` HTTP response with the current ETag in the `ETag` header so the client can refresh and retry.
- Application code surfaces the version in API responses via `If-Match` / `ETag` headers — see the controller layer in [`../src/controllers/`](../src/controllers).
- Schema migrations that **add** columns to `users` are safe (existing rows keep their version); migrations that **remove** the `version` column would break clients holding stale ETags and are forbidden.

## Soft Delete with PII Anonymization (User Service-Specific)

The User Service implements soft-delete with PII anonymization to satisfy COPPA and GDPR right-to-erasure requirements without losing the referential anchors needed for downstream consumer compaction.

- The `users.deleted_at` column is `NULL` for active users; setting it to a timestamp marks the user as soft-deleted.
- The partial index `users_active_idx ON users (deleted_at) WHERE deleted_at IS NULL` keeps active-user queries fast on a table that retains soft-deleted rows for audit purposes.
- On `DELETE /users/me`, the application performs three steps in a single transaction:
  1. Sets `users.deleted_at` to NOW().
  2. Anonymizes PII fields by replacing `email`, `phone`, name fields, and address rows with hash-stable placeholders derived from `USER_PII_ANONYMIZATION_HASH_SALT` (see [`../.env.example`](../.env.example)).
  3. Writes a `user.deleted` row to `events_outbox` so downstream consumers (Notification, Recommendation) suppress further sends and purge per-user features.
- The migration schema **supports** this pattern (NULLable PII columns where appropriate, `deleted_at` column, partial index, outbox table) but does **not** perform anonymization itself; anonymization is application code, not DDL.
- COPPA / GDPR compliance: the minimum age `USER_DATE_OF_BIRTH_MIN_AGE_YEARS=13` is enforced at the application layer; the schema permits NULL `date_of_birth` for users who decline to provide it.

## Outbox Pattern via `events_outbox` (User Service-Specific)

The User Service is the canonical write-and-publish service in this monorepo: every user mutation simultaneously durably persists data **and** signals downstream domains via Kafka. The `events_outbox` table eliminates the "double-write" problem (data committed to Postgres but the Kafka publish lost on a process crash).

- The `events_outbox` table (created by `0005_create_events_outbox_table.py`) is the staging area for `user.updated` and `user.deleted` events.
- On any user mutation, the application writes **both** the data change **and** the corresponding `events_outbox` row in the **same** PostgreSQL transaction. Either both writes commit or both roll back; partial publication is structurally impossible.
- A background dispatcher polls `events_outbox WHERE published_at IS NULL ORDER BY created_at LIMIT N`, publishes each row to Kafka via the schema-validated producer, and marks `published_at = NOW()` on success.
- On failure: the dispatcher increments `retry_count`, sets `last_error`, and reschedules. After `OUTBOX_DISPATCH_MAX_RETRIES` attempts (see [`../.env.example`](../.env.example)), the row stays unpublished and is alertable via the `outbox_pending` Prometheus gauge.
- Successful events are purged after `OUTBOX_RETENTION_DAYS=14`; failed rows are retained for `OUTBOX_FAILED_RETENTION_DAYS=90` for forensics.
- The composite partial index `events_outbox_pending_idx ON events_outbox (created_at) WHERE published_at IS NULL` keeps dispatcher polling fast even at high event volume — only unpublished rows are indexed, and successful rows fall out of the working set.

## Application Workflow

Both AAP R-9-compliant workflows are supported. Pick one per environment and stick to it.

- **On service startup** — preferred for local development and single-replica environments. An entrypoint script runs `alembic upgrade head` **before** the FastAPI / uvicorn process starts accepting traffic. A failure is fatal: the container exits non-zero and the readiness probe stays `503` (AAP R-19). Controlled by `RUN_MIGRATIONS_ON_STARTUP=true` (see [`../.env.example`](../.env.example)).
- **Dedicated migration Job / CI step** — recommended for production. A Kubernetes `Job` (or a CI stage) runs `alembic upgrade head` exactly once per deployment; service pods only start once the Job has succeeded. The deploy manifest at `../../../deploy/k8s/user-service-migrate.yaml` (created by sibling agents) embodies this pattern. This is safer for multi-replica zero-downtime rollouts because it eliminates the race between concurrent replicas all trying to migrate the same database at startup.

## Local Development Workflow

```bash
# From the service root: services/user-service/
# Ensure POSTGRES_URL is set (e.g., from .env loaded into the shell)
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/user_db"

# Activate venv & install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# From the migrations directory:
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

The example connection string above is for local development only and uses the conventional Postgres dev defaults; it is **not** a real credential. Production and staging URLs are supplied at runtime by Kubernetes Secrets / Vault per AAP R-25.

## Environment Variables

`env.py` reads exactly two environment variables:

- **`POSTGRES_URL`** — REQUIRED at runtime. Format: `postgresql://user:pass@host:port/user_db` (or `postgresql+psycopg://user:pass@host:port/user_db` — `env.py` rewrites the bare scheme to use the psycopg v3 dialect). A missing or empty value causes `env.py` to exit with code `2` so Kubernetes Job / initContainer operators can distinguish a missing-config failure from a migration execution failure.
- **`ALEMBIC_LOG_LEVEL`** — OPTIONAL. Default: `WARNING`. Allowed values: `DEBUG | INFO | WARNING | ERROR | CRITICAL`. Set to `INFO` or `DEBUG` for verbose Alembic output during incident response or diagnostic runs.

**No credentials live in [`./alembic.ini`](./alembic.ini)** (AAP R-25). The `sqlalchemy.url` key is intentionally empty; `env.py` populates it at runtime from `POSTGRES_URL`.

The parent service's [`../.env.example`](../.env.example) also documents `MIGRATIONS_TABLE=alembic_version_user_service` (informational — must match the constant in `env.py`) and `RUN_MIGRATIONS_ON_STARTUP=true` (consumed by the entrypoint, not by `env.py` directly).

## Troubleshooting

- **`Can't load plugin: sqlalchemy.dialects:postgresql.psycopg`** → install `psycopg[binary,pool]>=3` (already declared in [`../requirements.txt`](../requirements.txt)); verify with `pip show psycopg`.
- **`No module named 'psycopg2'`** → DO NOT install psycopg2; the URL must use `postgresql+psycopg://` (v3), not `postgresql+psycopg2://`. `env.py` performs the rewrite automatically when given a bare `postgresql://` URL and rejects `postgresql+psycopg2://` with an explicit `RuntimeError`.
- **`Target database is not up to date`** → run `alembic upgrade head` first; the service's data layer asserts the database is at `head` on startup and refuses to serve traffic otherwise.
- **`Multiple head revisions detected`** → indicates a merge conflict between two parallel migration branches; run `alembic merge -m "merge heads" <head1> <head2>` to reconcile, then commit the merge revision file.
- **`relation "alembic_version_user_service" does not exist`** → the database has never had Alembic applied; just run `alembic upgrade head` (Alembic auto-creates this table on first apply).
- **`extension "pgcrypto" is not available`** → the running PostgreSQL build does not include contrib modules. Use a contrib-enabled image (for example, `postgres:16-bookworm`) or install the `postgresql-contrib` package on the host.
- **`permission denied to create extension "pgcrypto"`** → Alembic is connecting as a non-superuser. Either (a) grant the user `CREATE` privilege on the database and `SUPERUSER` for the migration window only (dev only), (b) pre-create the extension as a superuser in a one-time bootstrap step, or (c) ensure `CREATE EXTENSION IF NOT EXISTS pgcrypto` runs in a separate bootstrap step before migrations.
- **`unique violation on alembic_version_user_service`** → should not happen with the service-specific version table name; if it does, multiple migration runners are racing — ensure only one runner (initContainer **or** Job, never both) applies migrations per deployment.
- **`duplicate key value violates unique constraint "users_email_uniq"`** during `user.registered` consume → email collision; verify the Auth Service is enforcing email uniqueness on registration **or** upgrade the consumer to handle the conflict by routing the duplicate to `user.registered.dlq` for manual reconciliation.

## Testing

Integration tests live under [`../tests/integration/`](../tests/integration) and exercise the migration toolchain end to end.

- Each test uses Testcontainers to spin up a fresh `postgres:16` container with the contrib bundle.
- The test sets `POSTGRES_URL` to the container's connection string and runs `alembic upgrade head`, then asserts:
  - All five tables (`users`, `user_profiles`, `user_preferences`, `user_addresses`, `events_outbox`) exist with the expected columns and types (verified via `information_schema.columns`).
  - The `pgcrypto` extension is enabled (`SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'`).
  - The `alembic_version_user_service` table exists with exactly one row pointing at the latest revision id.
  - The UNIQUE constraints `users_email_uniq` (case-insensitive via `LOWER(email)` expression index or citext) and `users_external_auth_id_uniq` are present and enforced.
  - The CHECK constraint on `users.status` enforces the enum (`'active' | 'inactive' | 'suspended' | 'deleted'`).
  - The partial index `users_active_idx` on `(deleted_at) WHERE deleted_at IS NULL` exists.
  - The partial UNIQUE `user_addresses_default_uniq ON (user_id, type) WHERE is_default = TRUE` is present (one default per type per user).
  - The CASCADE / RESTRICT FK behaviors are configured correctly per `user_profiles`, `user_preferences`, `user_addresses`.
  - The partial index `events_outbox_pending_idx ON events_outbox (created_at) WHERE published_at IS NULL` is present.
- Re-running `alembic upgrade head` against a head-state database verifies no-op idempotence.
- Round-tripping `alembic downgrade base` then `alembic upgrade head` verifies reversibility.

## Cross-Coupling Awareness

The table column types and string-literal CHECK constraint values in this folder are **coupled** with the SQLAlchemy / Pydantic models in [`../src/repository/models.py`](../src/repository/models.py) and the enum values in `../src/domain/user_types.py` (or the equivalent module created by sibling agents). Specifically:

- `users.status` uses `'active' | 'inactive' | 'suspended' | 'deleted'` (lowercase) — must match the `UserStatus` enum values.
- `user_addresses.type` uses `'billing' | 'shipping' | 'other'` (lowercase) — must match the `AddressType` enum values.
- `users.email` uniqueness is **case-insensitive** (via citext column type or a `LOWER(email)` UNIQUE expression index) — application code must normalize on write or rely on database-level case folding.
- `user_addresses.country` uses ISO-3166-1 alpha-2 codes (uppercase) — the `^[A-Z]{2}$` CHECK constraint enforces this at the SQL layer.
- `user_preferences.preferred_currency` uses ISO-4217 codes (uppercase) — the `^[A-Z]{3}$` CHECK constraint enforces this at the SQL layer.

Changing any string literal here without coordinated updates to the corresponding domain enum (and vice versa) results in runtime CHECK-constraint violations. Treat each migration that touches a string-literal CHECK as a paired change with the enum module and roll them out together.

## Related Files & References

- [`../README.md`](../README.md) — Service-level README
- [`../config/default.yaml`](../config/default.yaml) — Database pool sizing, statement timeout
- [`../.env.example`](../.env.example) — `POSTGRES_URL`, `MIGRATIONS_TABLE`, `RUN_MIGRATIONS_ON_STARTUP` template
- [`../requirements.txt`](../requirements.txt) — Python dependency manifest (Alembic, SQLAlchemy, psycopg3)
- [`../Dockerfile`](../Dockerfile) — References this folder via `COPY --chown=users:users migrations/ /app/migrations/`
- [`../../notification-service/migrations/README.md`](../../notification-service/migrations/README.md) — Sibling Alembic reference
- [`../../order-service/migrations/README.md`](../../order-service/migrations/README.md) — Sibling Alembic reference (saga state pattern)
- [`../../../docs/architecture/data-stores.md`](../../../docs/architecture/data-stores.md) — DB-per-service rationale and polyglot persistence choices
- [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md) — System architecture
- [`../../../docs/runbook/user-service.md`](../../../docs/runbook/user-service.md) — Operational runbook (DLQ replay, PII anonymization audits, manual user re-init from `user.registered`)

AAP citations (non-clickable):

- AAP Section 0.4.4 — Database schema for `user_db`
- AAP Section 0.5.2.5 — Per-service migrations folder requirement
- AAP R-6 — Database per service strict isolation
- AAP R-7 — Polyglot persistence (PostgreSQL)
- AAP R-9 — Migrations under owning service, applied automatically
- AAP R-19 — Liveness / readiness probes; fail-fast
- AAP R-25 — Secrets never in source / config
- AAP R-26 — Audit timestamps on every table
