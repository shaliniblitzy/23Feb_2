# Notification Service — Database Migrations

Forward-only, versioned Alembic migration scripts that create and evolve the Notification Service's private PostgreSQL schema (`notification_db`).

## Purpose

This folder contains versioned **Alembic** (Python) migration scripts for the **private PostgreSQL database** `notification_db` owned by the Notification Service.

- Per **AAP R-6** (database per service), no other service reads from or writes to this database. The only way other services observe data produced here is via Kafka events emitted by the owning service. (Note: the Notification Service is a **terminal Kafka consumer** — it consumes 7 domain topics but produces zero outbound events; see [`../../../docs/architecture/event-catalog.md`](../../../docs/architecture/event-catalog.md) when that catalog is published.)
- Per **AAP R-9**, migrations **must** be applied automatically — either on service startup (entrypoint or initContainer) or via a dedicated migration Kubernetes Job / CI step. Pods must not begin serving traffic until all pending migrations have completed successfully.
- Per **AAP R-25**, the database connection URL is supplied at runtime via the `POSTGRES_URL` environment variable. **No credentials live in this folder.**

## Toolchain

Read this section first — it disambiguates the apparent contradiction between Alembic's SQLAlchemy dependency and the service's pure-psycopg3 runtime.

- **Migration runner:** Alembic (`alembic>=1.13.0,<2.0.0`, pinned in [`../requirements.txt`](../requirements.txt)).
- **Driver dialect:** SQLAlchemy 2.0 with the **psycopg v3** dialect — URL prefix `postgresql+psycopg://`.
  - **CRITICAL:** Use `postgresql+psycopg://` (psycopg v3) — **never** `postgresql+psycopg2://` (legacy v2). The service's [`../requirements.txt`](../requirements.txt) declares `psycopg[binary,pool]>=3.1.18,<4.0.0`; psycopg2 is not installed.
- **Why Alembic + SQLAlchemy when the service uses pure psycopg3 at runtime?**
  - The Notification Service's runtime data-access code under [`../src/repository/`](../src/repository) uses **pure psycopg3** with `AsyncConnectionPool`. There is no SQLAlchemy ORM, session factory, or declarative model in the hot path.
  - SQLAlchemy is a **transitive dependency of Alembic only** — Alembic is built on SQLAlchemy Core. SQLAlchemy is not imported anywhere in the request path or the Kafka consumer loop.
  - Migrations therefore use **imperative `op.create_table()` / `op.create_index()` / `op.execute()`** calls — never ORM model autogeneration.
  - `target_metadata = None` in `env.py` reflects this design: there is no SQLAlchemy `MetaData()` to compare against because the schema is defined exclusively by the migration scripts.

## File Layout

```text
migrations/
├── alembic.ini                                                   # Alembic CLI config; sqlalchemy.url is empty by design
├── env.py                                                        # Online + offline runtime; reads POSTGRES_URL and rewrites the dialect to postgresql+psycopg
├── script.py.mako                                                # Mako template used to generate new revision files
├── README.md                                                     # This file
└── versions/                                                     # Individual revision files (chronologically ordered)
    ├── 20260101_000001_initial_schema.py                         # CREATE TABLE for all 4 tables
    ├── 20260101_000002_add_indexes.py                            # Performance indexes (partial + composite)
    └── 20260101_000003_add_user_channel_prefs_constraints.py     # CHECK constraints + unique guards
```

- **`alembic.ini`** — Alembic CLI configuration. The `sqlalchemy.url` key is intentionally **empty**; `env.py` populates it at runtime from `POSTGRES_URL` (AAP R-25).
- **`env.py`** — Defines both online and offline migration modes; reads `POSTGRES_URL` from the environment and rewrites a bare `postgresql://` URL to `postgresql+psycopg://` so the psycopg v3 dialect is selected.
- **`script.py.mako`** — Mako template used by `alembic revision -m "..."` to scaffold new revision files.
- **`versions/20260101_000001_initial_schema.py`** — Creates all four tables (`notification_log`, `delivery_attempts`, `templates`, `user_channel_prefs`) with columns, primary keys, and the unique constraint on `notification_log (event_id, channel)` that enforces the inbound-event idempotency key.
- **`versions/20260101_000002_add_indexes.py`** — Adds the partial index `idx_notification_log_retry_poll` (predicate `WHERE status = 'PENDING_RETRY'`) and the composite index `idx_templates_lookup` on `(event_type, channel, locale, is_active, version DESC)`.
- **`versions/20260101_000003_add_user_channel_prefs_constraints.py`** — Adds CHECK constraints (channel/status/outcome/criticality enum sets, quiet-hours range `0..23`, `attempt_count >= 0`) and any additional unique guards (for example, `(user_id, channel)` on `user_channel_prefs`).

## Naming Convention

- Revision filenames: `<YYYYMMDD>_<HHMMSS>_<snake_case_slug>.py`.
- This is configured via the `file_template` setting in `alembic.ini`.
- Each revision file declares `revision = "<id>"` at the top (for example, `revision = "20260101_000001"`); the file's identifier matches the timestamp prefix.
- New revisions are generated by `alembic revision -m "short description"`. The `--autogenerate` flag is **not used** because `target_metadata = None` (see [Toolchain](#toolchain)); migrations are written by hand.
- **Never reuse** a revision id.
- **Never edit** a revision file that has already been applied in any environment — see [Rollback Policy](#rollback-policy).

## Rollback Policy

Migrations in this folder are **forward-only** in production.

- Do **not** edit a migration that has been applied in any environment.
- Each Alembic revision **does** define a `downgrade()` function (best-practice for reversibility), but `downgrade` is reserved for **local development and CI round-trip tests** — never for production rollback.
- To undo a change in production, **create a new forward migration** (for example, `20260201_000001_revert_xyz.py`) that contains the corrective DDL.
- For catastrophic recovery, **restore from a database snapshot** rather than running a reverse migration against live data.

The rationale: edit-in-place migrations cause diverged database states across environments and are a leading cause of production incidents. A forward-only ledger keeps every environment reproducible from the very first revision onward.

## Idempotence

Every operation in the migration files is safe to reapply.

- Raw-SQL paths invoked via `op.execute()` use `IF NOT EXISTS` / `OR REPLACE` semantics where applicable.
- For schema-builder calls such as `op.create_table()`, Alembic's revision tracking via the auto-managed `alembic_version` table prevents reapplication of the same revision against the same database.
- CI runs the round-trip sequence `alembic upgrade head` → `alembic downgrade base` → `alembic upgrade head` to verify reversibility and idempotence on every pull request that touches this folder.

## Schema Summary

The four tables required by **AAP Section 0.4.4** are created by the initial migration; performance indexes and constraints are added by subsequent migrations.

| Table | Purpose | Created by |
|-------|---------|------------|
| `notification_log` | One row per inbound Kafka-event resolution attempt; idempotency key on `(event_id, channel)` | `20260101_000001_initial_schema.py` |
| `delivery_attempts` | One row per outbound provider API call; append-only delivery audit | `20260101_000001_initial_schema.py` |
| `templates` | Versioned message templates (email subject/body, SMS body) per locale | `20260101_000001_initial_schema.py` |
| `user_channel_prefs` | Per-user channel opt-in/opt-out, locale, quiet-hours window | `20260101_000001_initial_schema.py` |

## Schema Isolation (AAP R-6)

This database is **private** to the Notification Service.

- **No foreign keys** point at tables in any other service's database (`user_db`, `product_db`, `order_db`, `auth_db`, `payment_db`, `inventory_db`).
- `user_id`, `notification_id`, `event_id`, and `template_id` are stored as **opaque UUIDs**. Cross-service joins are forbidden — the Notification Service learns about external entities exclusively via Kafka events it consumes.
- `delivery_attempts.notification_id` is a **soft reference** to `notification_log.notification_id` (no `FOREIGN KEY` declared) for write-throughput reasons; integrity is maintained by application invariants in the repository layer.

## Application Workflow

Both AAP R-9-compliant workflows are supported. Pick one per environment and stick to it.

- **On service startup** — preferred for local development and single-replica environments. An entrypoint script runs `alembic upgrade head` **before** the FastAPI / uvicorn process starts accepting traffic. A failure is fatal: the container exits non-zero and the readiness probe stays `503` (AAP R-19).
- **Dedicated migration Job / CI step** — recommended for production. A Kubernetes `Job` (or a CI stage) runs `alembic upgrade head` exactly once per deployment; service pods only start once the Job has succeeded. This pattern is safer for multi-replica zero-downtime rollouts because it eliminates the race between concurrent replicas all trying to migrate the same database at startup.

## Local Development Workflow

Concrete commands for day-to-day development:

```bash
# From the service root: services/notification-service/
# Ensure POSTGRES_URL is set (e.g., from a local .env file loaded into the shell).
# Use a development-only password and a local Postgres instance:
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/notification_db"

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

- **`POSTGRES_URL`** — **REQUIRED** at runtime. Format: `postgresql://<user>:<pass>@<host>:<port>/notification_db`. A bare `postgresql://` scheme is rewritten to `postgresql+psycopg://` automatically by `env.py`; an explicit `postgresql+psycopg://` URL is also accepted unchanged.
- **`ALEMBIC_LOG_LEVEL`** — *Optional.* Default: `WARNING`. Set to `INFO` for verbose Alembic logging during debugging.

**No credentials live in `alembic.ini`** (AAP R-25). The `sqlalchemy.url` key in `alembic.ini` is intentionally empty; `env.py` populates it at runtime from `POSTGRES_URL`. See the operator-facing [`../.env.example`](../.env.example) for the canonical environment-variable template.

## Troubleshooting

The most common issues encountered while running migrations:

- **`Can't load plugin: sqlalchemy.dialects:postgresql.psycopg`** — install `psycopg[binary,pool]>=3` (already declared in [`../requirements.txt`](../requirements.txt)). Verify with `pip show psycopg` (the package is named `psycopg`, not `psycopg2` or `psycopg3`).
- **`No module named 'psycopg2'`** — **do not** install `psycopg2`. The URL must use `postgresql+psycopg://` (v3), not `postgresql+psycopg2://`. `env.py` performs this rewrite automatically when given a bare `postgresql://` URL; if you provided an explicit `postgresql+psycopg2://` URL, change it to `postgresql+psycopg://`.
- **`Target database is not up to date`** — run `alembic upgrade head` first. Alembic refuses to generate a new revision until the database is at the latest known head.
- **`Multiple head revisions detected`** — indicates a merge conflict between two parallel migration branches. Reconcile with `alembic merge -m "merge heads" <head1> <head2>` and commit the resulting merge revision.
- **`relation "alembic_version" does not exist`** — the database has never had Alembic applied. Just run `alembic upgrade head`; Alembic auto-creates the version table on first apply.

## Testing

An integration test under `../tests/integration/` (authored by sibling agents) exercises the migration suite end-to-end:

- Uses **Testcontainers** to spin up a fresh `postgres:16` container per test run.
- Sets `POSTGRES_URL` to the container's connection string.
- Runs `alembic upgrade head` and asserts:
  - All four tables exist with the expected columns and types (verified via `information_schema.columns`).
  - The unique constraint on `notification_log (event_id, channel)` is present (the inbound idempotency key).
  - The partial index `idx_notification_log_retry_poll` exists with the predicate `WHERE status = 'PENDING_RETRY'`.
  - The composite index `idx_templates_lookup` on `(event_type, channel, locale, is_active, version DESC)` exists.
  - All CHECK constraints are present (channel/status/outcome/criticality enum value sets, quiet-hours range, `attempt_count >= 0`).
- Re-runs `alembic upgrade head` to verify no-op idempotence on a database already at head.
- Round-trips `alembic downgrade base` then `alembic upgrade head` to verify reversibility.

## Cross-Coupling Awareness

The CHECK-constraint string literals encoded in these migrations are **coupled** with the Pydantic models under [`../src/repository/`](../src/repository) and the `StrEnum` definitions in [`../src/domain/channel_types.py`](../src/domain/channel_types.py). Changing a string literal in one place without coordinated changes in the other(s) results in **runtime CHECK-constraint violations** on insert.

The exact coupled value sets:

- `notification_log.channel` and `templates.channel` use `'email' | 'sms'` (lowercase) — must match the `ChannelType` `StrEnum` values.
- `notification_log.status` uses `'PENDING' | 'SUCCESS' | 'PENDING_RETRY' | 'DEAD_LETTER' | 'CANCELLED'` (uppercase) — must match the `NotificationStatus` `StrEnum` values.
- `delivery_attempts.outcome` uses `'SUCCESS' | 'RETRYABLE' | 'TERMINAL'` (uppercase) — must match the `DeliveryOutcome` `StrEnum` values.
- `templates.criticality` uses `'critical' | 'non_critical'` (lowercase) — must match the `CriticalFlag` `StrEnum` values.

When you change a domain enum, you **must** add a forward migration (see [Rollback Policy](#rollback-policy)) that updates the corresponding CHECK constraint, and vice versa.

## Related Files & References

- [`../README.md`](../README.md) — Service-level README
- [`../config/default.yaml`](../config/default.yaml) — Database pool sizing, statement timeout
- [`../.env.example`](../.env.example) — `POSTGRES_URL` template (no real credentials)
- [`../src/repository/`](../src/repository) — Pure-psycopg3 runtime data access (no SQLAlchemy)
- [`../src/domain/channel_types.py`](../src/domain/channel_types.py) — `StrEnum` definitions coupled with the CHECK-constraint string literals above
- [`../../../docs/architecture/data-stores.md`](../../../docs/architecture/data-stores.md) — Database-per-service rationale and polyglot persistence choices
- [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md) — System architecture diagram

AAP citations (non-clickable):

- AAP Section 0.4.4 — Database schema for `notification_db`
- AAP Section 0.5.2.5 — Per-service migrations folder requirement
- AAP R-6 — Database per service strict isolation
- AAP R-7 — Polyglot persistence (PostgreSQL for `notification_db`)
- AAP R-9 — Migrations under owning service, applied automatically
- AAP R-25 — Secrets never in source/config
