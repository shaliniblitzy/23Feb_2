# Order Service — Database Migrations

Forward-only, versioned Alembic migration scripts that create and evolve the Order Service's private PostgreSQL schema (`order_db`), including the `saga_state` table that enables saga recovery across service restarts.

## Purpose

This folder contains versioned **Alembic** (Python) migration scripts for the **private PostgreSQL database** `order_db` owned by the Order Service.

- Per **AAP R-6** (database per service), no other service reads from or writes to this database. Cross-service references such as `orders.user_id` and `order_items.product_id` are stored as **opaque UUIDs without foreign keys** — referential integrity across service boundaries is maintained through Kafka events (`order.*`, `inventory.*`, `payment.*`) and saga semantics, never through SQL joins.
- Per **AAP R-9**, migrations **must** be applied automatically — either on service startup (entrypoint or initContainer) or via a dedicated migration Kubernetes Job / CI step. Pods must not begin serving traffic until all pending migrations have completed successfully.
- Per **AAP R-25**, the database connection URL is supplied at runtime via the `POSTGRES_URL` environment variable. **No credentials live in this folder.**
- Per **AAP R-18** (the architectural linchpin of this service among the four tables here), the `saga_state` table provides **durable saga progress persistence**. A coordinator restart recovers all in-flight sagas by reading `saga_state` rows whose `current_step` is not `'TERMINATED'`. Without this table a crash mid-saga would lose orchestration state and could cause stuck orders or duplicate compensations. See [Saga State Persistence (AAP R-18)](#saga-state-persistence-aap-r-18) for the complete recovery story.

## Toolchain

Read this section first — it documents the **Alembic + SQLAlchemy + psycopg3 v3 dialect** stack and the service-specific Alembic version table that operators must understand before running any migration command.

- **Migration runner:** [Alembic](https://alembic.sqlalchemy.org/) (`alembic>=1.13.0,<2.0.0`, pinned in [`../requirements.txt`](../requirements.txt)).
- **Driver dialect:** SQLAlchemy 2.0 with the **psycopg v3** dialect — URL prefix `postgresql+psycopg://`.
  - **CRITICAL:** Use `postgresql+psycopg://` (psycopg v3) — **never** `postgresql+psycopg2://` (legacy v2). The service's [`../requirements.txt`](../requirements.txt) declares `psycopg[binary,pool]>=3.1.18,<4.0.0`; psycopg2 is not installed and `env.py` rejects `postgresql+psycopg2://` URLs with an explicit `RuntimeError` rather than letting SQLAlchemy raise an opaque `ModuleNotFoundError`.
- **Required PostgreSQL extension:** `pgcrypto` (enabled by the initial migration). Provides `gen_random_uuid()` for primary keys (UUID v4) — the Order Service uses UUIDs for `orders.id`, `saga_state.saga_id`, and the correlation IDs propagated through every order, status-history row, and saga record.
- **Why Alembic with `target_metadata = None`?**
  - The schema is defined imperatively in revision files via `op.create_table()`, `op.create_index()`, `op.create_check_constraint()`, and `op.execute()` for raw SQL where needed (for example, partial indexes for the saga scheduler scan and table comments).
  - Autogenerate is intentionally disabled because every DDL must be reviewed line-by-line; we do not want a missing ORM model attribute to silently drop a column on apply — and especially not a column referenced by the saga state machine.
  - `target_metadata = None` in [`./env.py`](./env.py) reflects this: there is no SQLAlchemy `MetaData()` to compare against because the schema is defined exclusively by hand-written revision files.
  - This is the same pattern Payment Service follows; see [`../../payment-service/migrations/README.md`](../../payment-service/migrations/README.md) for the canonical structural rationale.
- **Service-specific Alembic version table:** This service uses **`alembic_version_order_service`** (not the default `alembic_version`). The custom name lets operators safely run all services' migrations against a single Postgres cluster in local development without colliding on a shared bookkeeping table. The name is set in [`./env.py`](./env.py) via `context.configure(version_table="alembic_version_order_service", ...)` on **both** the offline and online migration paths.

## File Layout

```text
migrations/
├── alembic.ini                                      # Alembic CLI config; sqlalchemy.url is empty by design
├── env.py                                           # Online + offline runtime; reads POSTGRES_URL and rewrites the dialect to postgresql+psycopg
├── script.py.mako                                   # Mako template used to generate new revision files
├── README.md                                        # This file
└── versions/                                        # Individual revision files (chronologically ordered)
    ├── 0001_create_orders_table.py                  # CREATE TABLE orders + indexes; CREATE EXTENSION pgcrypto
    ├── 0002_create_order_items_table.py             # CREATE TABLE order_items + intra-DB FK to orders
    ├── 0003_create_order_status_history.py          # CREATE TABLE order_status_history + composite index
    ├── 0004_create_saga_state_table.py              # CREATE TABLE saga_state + partial indexes (AAP R-18)
    └── 0005_seed_initial_data.py                    # Optional supplementary CHECK constraints; no runtime data seeding
```

- **`alembic.ini`** — Alembic CLI configuration. The `sqlalchemy.url` key is intentionally **empty**; `env.py` populates it at runtime from `POSTGRES_URL` (AAP R-25).
- **`env.py`** — Defines both online and offline migration modes; reads `POSTGRES_URL` from the environment, rewrites a bare `postgresql://` URL to `postgresql+psycopg://`, and propagates `version_table=alembic_version_order_service` to both `context.configure()` calls.
- **`script.py.mako`** — Mako template used by `alembic revision -m "..."` to scaffold new revision files.
- **`versions/0001_create_orders_table.py`** — Enables the `pgcrypto` extension and creates the `orders` aggregate-root table with the `UNIQUE` constraint on `idempotency_key` plus the composite index `idx_orders__user_created` on `(user_id, created_at DESC)`.
- **`versions/0002_create_order_items_table.py`** — Creates the `order_items` line-item table with the intra-database foreign key `order_items.order_id REFERENCES orders(id) ON DELETE CASCADE`.
- **`versions/0003_create_order_status_history.py`** — Creates the append-only `order_status_history` audit log with the composite index `idx_order_status_history__order_time` on `(order_id, occurred_at)`.
- **`versions/0004_create_saga_state_table.py`** — Creates the `saga_state` table with the partial indexes `idx_saga_state__deadline` (for the timeout-driven compensation scheduler) and `idx_saga_state__awaiting` (for Kafka-event correlation lookups). This is the AAP R-18 durability guarantee.
- **`versions/0005_seed_initial_data.py`** — Reserved for optional supplementary CHECK constraints or non-essential housekeeping. Does **not** seed runtime data; the canonical domain enums are CHECK-constraint-enforced inline within the `0001`–`0004` migrations.

The bootstrap migrations use **ordinal prefixes** (`0001_`, `0002_`, ...) to convey the intended apply order at first sight. Subsequent migrations created via `alembic revision -m "..."` will use the `<YYYYMMDD>_<HHMMSS>_<slug>.py` template specified by `file_template` in [`./alembic.ini`](./alembic.ini); both naming patterns coexist safely because Alembic resolves the dependency graph from the `down_revision` chain, not from filename order.

## Naming Convention

- **Bootstrap revision filenames:** `<NNNN>_<snake_case_slug>.py` (for example, `0001_create_orders_table.py`). The four-digit zero-padded ordinal expresses the intended apply order of the initial schema.
- **Subsequent (post-bootstrap) revisions:** `<YYYYMMDD>_<HHMMSS>_<snake_case_slug>.py` per the `file_template` setting in [`./alembic.ini`](./alembic.ini); the `timezone = UTC` setting in the same file guarantees deterministic, contributor-independent timestamps.
- Each revision file declares `revision = "<id>"` at the top (for example, `revision = "0001"` or `revision = "20260101_000001"`); the file's identifier matches the prefix exactly.
- New revisions are generated by `alembic revision -m "short description"`. The `--autogenerate` flag is **not used** because `target_metadata = None` (see [Toolchain](#toolchain)); migrations are written by hand.
- **Never reuse** a revision id.
- **Never edit** a revision file that has already been applied in any environment — see [Rollback Policy](#rollback-policy).

## Rollback Policy

Migrations in this folder are **forward-only** in production.

- Do **not** edit a migration that has been applied in any environment.
- Each Alembic revision **does** define a `downgrade()` function (best practice for reversibility), but `downgrade` is reserved for **local development and CI round-trip tests** — never for production rollback.
- To undo a change in production, **create a new forward migration** (for example, `20260201_000001_revert_xyz.py`) that contains the corrective DDL.
- For catastrophic recovery, **restore from a database snapshot** rather than running a reverse migration against live data.
- **Saga-state note:** The `saga_state` table is operationally critical for in-flight saga recovery. Backwards-incompatible changes (column drops, type narrowing, NOT-NULL tightening, removing a value from a `current_step` CHECK constraint) **must** be deployed in a multi-step expand-contract pattern. Never break the schema while sagas may be in flight; doing so can leave orchestrator-managed orders permanently stuck or trigger spurious compensations.

## Idempotence

Every operation in the migration files is safe to reapply.

- Raw-SQL paths invoked via `op.execute()` use `IF NOT EXISTS` / `OR REPLACE` semantics where applicable. The `pgcrypto` extension is enabled with `CREATE EXTENSION IF NOT EXISTS pgcrypto` in the initial revision and is **intentionally not dropped** in `downgrade()` because the extension may be shared with other services co-located on the same dev database.
- For schema-builder calls such as `op.create_table()`, Alembic's revision tracking via the service-specific `alembic_version_order_service` bookkeeping table prevents reapplication of the same revision against the same database.
- CI runs the round-trip sequence `alembic upgrade head` → `alembic downgrade base` → `alembic upgrade head` to verify reversibility and idempotence on every pull request that touches this folder.

## Schema Summary

The four tables required by **AAP Section 0.4.4** are created by the bootstrap migrations; all performance indexes and CHECK constraints are added inline within the same revision that creates the parent table.

| Table | Purpose | Created by |
|-------|---------|------------|
| `orders` | Aggregate root — one row per customer order (`id`, `user_id`, `status`, `currency`, `total_amount`, `idempotency_key` UNIQUE, `correlation_id`, `created_at`, `updated_at`, `version`) | `0001_create_orders_table.py` |
| `order_items` | Line items per order (`order_id` intra-DB FK, `line_no`, `product_id`, `quantity`, `unit_price`, `line_total`) | `0002_create_order_items_table.py` |
| `order_status_history` | Append-only audit log of state transitions (`id`, `order_id` intra-DB FK, `from_status`, `to_status`, `reason`, `correlation_id`, `occurred_at`) | `0003_create_order_status_history.py` |
| `saga_state` | **Durable saga coordinator persistence (AAP R-18)** — `order_id` intra-DB FK + PK, `saga_id`, `current_step`, `awaiting_event`, `retry_count`, `deadline_at`, `compensation_required`, `last_error`, `correlation_id`, `updated_at` | `0004_create_saga_state_table.py` |

The `0005_seed_initial_data.py` revision is reserved for optional supplementary CHECK constraints or non-essential housekeeping — it does **not** seed runtime data. Domain enums are CHECK-constraint-enforced inline within the `0001`–`0004` migrations.

## Schema Isolation (AAP R-6)

This database is **private** to the Order Service.

- **No foreign keys** point at tables in any other service's database (`auth_db`, `user_db`, `product_db`, `payment_db`, `inventory_db`, `notification_db`).
- `orders.user_id` and `order_items.product_id` are stored as **opaque UUIDs**. Cross-service joins are forbidden — the Order Service learns about external entities exclusively via Kafka events it consumes (`inventory.reserved`, `inventory.released`, `payment.succeeded`, `payment.failed`) and via REST calls fronted by the API Gateway.
- **Intra-database** foreign keys are used (and are encouraged) because all four tables live in the same private database:
  - `order_items.order_id REFERENCES orders(id) ON DELETE CASCADE`
  - `order_status_history.order_id REFERENCES orders(id) ON DELETE CASCADE`
  - `saga_state.order_id REFERENCES orders(id) ON DELETE CASCADE` — the `saga_state` table has a single row per order and uses `order_id` as its primary key.
- Cross-service consistency is achieved exclusively via Kafka events (`order.*`, `inventory.*`, `payment.*`) and saga semantics (AAP R-18), never via SQL.

## Saga State Persistence (AAP R-18)

This is the **single most distinguishing concern** of the Order Service among the eleven services in the monorepo: the `saga_state` table is the **durable backing store** for in-flight saga orchestration. Each row represents exactly one saga (one-to-one with an order). Without this table, a coordinator restart would lose all in-flight saga progress.

- **Recovery on restart.** On service start, the saga scheduler reads all rows `WHERE current_step <> 'TERMINATED'` and resumes orchestration from the persisted step. Combined with the saga state machine's idempotence — each state transition is a no-op if already applied — this enables clean recovery after coordinator crashes, rolling deployments, and pod evictions. The application-level recovery flow is documented in [Saga Recovery Semantics](#saga-recovery-semantics).
- **Deadline-driven compensation.** The `deadline_at` column drives the timeout-based compensation path. A background scheduler periodically scans `WHERE deadline_at < now() AND current_step <> 'TERMINATED'`, supported by the partial index `idx_saga_state__deadline` (`WHERE current_step <> 'TERMINATED'`), and triggers compensation per AAP R-20 (graceful degradation). The partial index keeps the scheduler scan O(stale-rows) rather than O(all-rows), which matters as the table grows.
- **Event correlation.** When a Kafka event arrives (for example, `inventory.reserved`), the consumer queries `WHERE awaiting_event = 'inventory.reserved'`, supported by the partial index `idx_saga_state__awaiting` (`WHERE awaiting_event IS NOT NULL`), to find the saga awaiting that signal. This is the indexed event-correlation lookup that bridges Kafka event arrivals to saga progression without scanning the full table.
- **Compensation tracking.** The `compensation_required` boolean flips to `TRUE` when the saga enters a compensation path (inventory release, payment refund). The scheduler treats compensating sagas separately from forward-progressing sagas because their retry budget, deadline semantics, and alerting thresholds differ.
- **Retry counting.** The `retry_count` column increments on each compensation attempt; once it exceeds `SAGA_MAX_COMPENSATION_ATTEMPTS` (env-driven; see [`../.env.example`](../.env.example)), the saga is marked `FAILED` requiring manual intervention. An operator alert fires via the `saga_manual_intervention_total` Prometheus counter and is documented in [`../../../docs/runbook/order-service.md`](../../../docs/runbook/order-service.md).
- **Last error capture.** The `last_error` column stores the most recent exception or event-failure message (free-form `TEXT`) for forensic analysis without requiring full log dives. Operators inspecting a stuck saga can read `last_error` directly from the table.

## Idempotency Key Store

The `orders` schema enforces order-placement idempotency at the database layer:

- The `orders` table carries a `UNIQUE` constraint on `idempotency_key`.
- When the API Gateway forwards a `POST /orders` request, the service stores `(idempotency_key, request_hash, response)` and replays the cached response on duplicate keys with matching hash; rejects (409) on hash mismatch.
- The unique constraint guarantees **at-most-once** order creation per idempotency key, even under concurrent retries from the gateway or client. A duplicate `INSERT` raises a unique-violation error which the application catches and translates into a replay of the original response.
- Entries are TTL-bounded by application logic (env: `IDEMPOTENCY_KEY_TTL_HOURS`); old keys are pruned by a periodic housekeeping task. The schema does **not** enforce TTL via partitioning at this stage — pruning is a runtime concern, not a migration concern.

## Application Workflow

Both AAP R-9-compliant workflows are supported. Pick one per environment and stick to it.

- **On service startup** — preferred for local development and single-replica environments. An entrypoint script runs `alembic upgrade head` **before** the FastAPI / uvicorn process starts accepting traffic. A failure is fatal: the container exits non-zero and the readiness probe stays `503` (AAP R-19).
- **Dedicated migration Job / CI step** — **strongly recommended for production**. A Kubernetes `Job` (or a CI stage) runs `alembic upgrade head` exactly once per deployment; service pods only start once the Job has succeeded. This pattern is safer for multi-replica zero-downtime rollouts because it eliminates the race between concurrent replicas all trying to migrate the same database at startup.
- **Saga-coordinator note.** During a rolling deployment, in-flight sagas may span the cutover. The expand-contract pattern (see [Rollback Policy](#rollback-policy)) ensures both old and new pod versions can read and write `saga_state` without loss. Avoid running migrations that drop columns referenced by the saga state machine until all old replicas have terminated.

## Local Development Workflow

Concrete commands for day-to-day development:

```bash
# From the service root: services/order-service/
# Ensure POSTGRES_URL is set (e.g., from a local .env file loaded into the shell).
# Use a development-only password and a local Postgres instance:
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/order_db"

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

- **`POSTGRES_URL`** — **REQUIRED** at runtime. Format: `postgresql://<user>:<pass>@<host>:<port>/order_db`. A bare `postgresql://` scheme is rewritten to `postgresql+psycopg://` automatically by `env.py`; an explicit `postgresql+psycopg://` URL is also accepted unchanged. An explicit `postgresql+psycopg2://` URL is rejected with an actionable `RuntimeError` because psycopg2 is not pinned in [`../requirements.txt`](../requirements.txt).
- **`ALEMBIC_LOG_LEVEL`** — *Optional.* Default: `WARNING`. Set to `INFO` or `DEBUG` for verbose Alembic logging during debugging; an unknown value falls back to `WARNING`.

**No credentials live in `alembic.ini`** (AAP R-25). The `sqlalchemy.url` key in [`./alembic.ini`](./alembic.ini) is intentionally empty; `env.py` populates it at runtime from `POSTGRES_URL`. See the operator-facing [`../.env.example`](../.env.example) for the canonical environment-variable template (including the `SAGA_*` variables consumed by the saga coordinator at runtime).

## Troubleshooting

The most common issues encountered while running migrations:

- **`Can't load plugin: sqlalchemy.dialects:postgresql.psycopg`** — install `psycopg[binary,pool]>=3` (already declared in [`../requirements.txt`](../requirements.txt)). Verify with `pip show psycopg` (the package is named `psycopg`, not `psycopg2` or `psycopg3`).
- **`No module named 'psycopg2'`** — **do not** install `psycopg2`. The URL must use `postgresql+psycopg://` (v3), not `postgresql+psycopg2://`. `env.py` performs this rewrite automatically when given a bare `postgresql://` URL; if you provided an explicit `postgresql+psycopg2://` URL, change it to `postgresql+psycopg://` (or strip the `+psycopg2` suffix and let `env.py` rewrite it).
- **`Target database is not up to date`** — run `alembic upgrade head` first. Alembic refuses to generate a new revision until the database is at the latest known head.
- **`Multiple head revisions detected`** — indicates a merge conflict between two parallel migration branches. Reconcile with `alembic merge -m "merge heads" <head1> <head2>` and commit the resulting merge revision.
- **`relation "alembic_version_order_service" does not exist`** — the database has never had Alembic applied. Just run `alembic upgrade head`; Alembic auto-creates the (service-specific) version table on first apply.
- **`extension "pgcrypto" is not available`** — the running PostgreSQL build does not include contrib modules. Use a contrib-enabled image (for example `postgres:16-bookworm`) or install the `postgresql-contrib` package on the host.
- **`permission denied to create extension "pgcrypto"`** — Alembic is connecting as a non-superuser. Either (a) grant the user the `CREATE` privilege on the database and `SUPERUSER` (dev only), (b) pre-create the extension as a superuser one-time before the migration runs, or (c) execute `CREATE EXTENSION IF NOT EXISTS pgcrypto` in a separate bootstrap step (Terraform / Helm hook) before invoking Alembic.
- **`unique violation on alembic_version_order_service`** — should not happen with the service-specific version table name; if it does, multiple migration runners are racing. Ensure exactly one runner (initContainer **or** Job, never both) applies migrations per deployment.
- **`saga_state` "stuck" rows after a schema change** — if a saga is stuck after a deployment, check that no in-flight saga's `current_step` value falls outside the new schema's CHECK constraint. The expand-contract migration pattern prevents this; if it has already happened, manually update the affected rows to a valid step value or `'TERMINATED'`, then file a follow-up incident — the next forward migration must reintroduce the missing CHECK value before the next deployment.

## Testing

An integration test under [`../tests/integration/`](../tests) (authored by sibling agents) exercises the migration suite end-to-end:

- Uses **Testcontainers** to spin up a fresh `postgres:16` container per test run.
- Sets `POSTGRES_URL` to the container's connection string.
- Runs `alembic upgrade head` and asserts:
  - All four tables (`orders`, `order_items`, `order_status_history`, `saga_state`) exist with the expected columns and types (verified via `information_schema.columns`).
  - The `pgcrypto` extension is enabled (`SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'`).
  - The `alembic_version_order_service` table exists with exactly one row pointing at the latest revision id.
  - The UNIQUE constraint on `orders.idempotency_key` is present (idempotency safety per [Idempotency Key Store](#idempotency-key-store)).
  - The intra-DB FKs from `order_items.order_id`, `order_status_history.order_id`, and `saga_state.order_id` to `orders.id` are present with `ON DELETE CASCADE`.
  - The composite indexes `idx_orders__user_created`, `idx_order_status_history__order_time`, `idx_saga_state__deadline` (partial), and `idx_saga_state__awaiting` (partial) exist.
  - All CHECK constraints are present (status enum, saga step enum, currency length, `total_amount > 0`, `retry_count >= 0`, etc.).
- Re-runs `alembic upgrade head` to verify no-op idempotence on a database already at head.
- Round-trips `alembic downgrade base` then `alembic upgrade head` to verify reversibility.
- Verifies **saga scheduler scan semantics**: inserts a `saga_state` row with `current_step = 'AWAIT_INVENTORY'` and `deadline_at < now()`, then asserts the partial index `idx_saga_state__deadline` returns it via the scheduler's `WHERE deadline_at < now() AND current_step <> 'TERMINATED'` clause and that an `EXPLAIN (ANALYZE, BUFFERS)` plan uses the partial index rather than a sequential scan.

## Cross-Coupling Awareness

The CHECK-constraint string literals encoded in these migrations are **coupled** with the SQLAlchemy / Pydantic models in `../src/repository/models.py` and with the enum definitions in `../src/domain/order_types.py` and `../src/saga/saga_types.py` (or equivalent modules). Changing a string literal in one place without coordinated changes in the other(s) results in **runtime CHECK-constraint violations** that immediately fail saga progression.

The exact coupled value sets:

- `orders.status` uses `'CREATED' | 'INVENTORY_RESERVED' | 'PAYMENT_TAKEN' | 'FULFILLED' | 'COMPENSATING_INVENTORY' | 'COMPENSATING_PAYMENT' | 'CANCELLED' | 'FAILED'` (UPPERCASE) — must match the `OrderStatus` enum values.
- `saga_state.current_step` uses `'CREATE_ORDER' | 'AWAIT_INVENTORY' | 'AWAIT_PAYMENT' | 'CONFIRM_ORDER' | 'COMPENSATE_INVENTORY' | 'COMPENSATE_PAYMENT' | 'TERMINATED'` (UPPERCASE) — must match the `SagaStep` enum values.
- `orders.currency` is `CHAR(3)` and is constrained to ISO 4217 codes by **application validation**, not by a CHECK constraint, because the ISO 4217 code set is large and may evolve. Adding new currency support therefore does **not** require a migration; only the application validator must be updated.
- `order_status_history.from_status` and `order_status_history.to_status` use the same enum values as `orders.status` (the audit log mirrors the canonical state). Whenever `OrderStatus` gains a new member, both the `orders.status` CHECK and the two `order_status_history` CHECK constraints must be updated together in a single forward migration.

When you change a domain enum, you **must** add a forward migration (see [Rollback Policy](#rollback-policy)) that updates the corresponding CHECK constraint, and vice versa. The two enum families above (vs. the Payment Service's five) reflect the narrower state-machine surface of the order domain — but `SagaStep` carries the additional weight of being the saga coordinator's persisted progress marker (see [Saga State Persistence (AAP R-18)](#saga-state-persistence-aap-r-18)), so changes there must follow the expand-contract pattern.

## Saga Recovery Semantics

The migration scripts here do **not** implement saga recovery — recovery is application-level logic in `../src/saga/`. The migrations **do** ensure the schema can support recovery:

- `saga_state` has a PRIMARY KEY on `order_id` (one row per saga, lookup by order id is `O(1)`).
- The partial index `idx_saga_state__deadline` (`WHERE current_step <> 'TERMINATED'`) supports the periodic timeout scan.
- The partial index `idx_saga_state__awaiting` (`WHERE awaiting_event IS NOT NULL`) supports the indexed event-correlation lookup.
- Sufficient column types — `last_error TEXT` for free-form messages, `correlation_id UUID` for cross-service request correlation, `updated_at TIMESTAMPTZ` for ordering — give the application all the introspection surface it needs without further joins.

The application's recovery procedure on startup:

1. Scan `saga_state` `WHERE current_step <> 'TERMINATED'` `ORDER BY updated_at`.
2. For each row, re-attach the saga to the in-memory orchestrator state.
3. If `compensation_required = TRUE`, the saga is in a compensation path; the orchestrator retries the next compensation step.
4. If `deadline_at < now()`, the saga has timed out; trigger compensation immediately.
5. Otherwise, the saga is awaiting a Kafka event (`awaiting_event` column); the consumer will progress it on next event arrival.

Operators may inspect saga state via `GET /orders/{id}/saga-state` (admin-scoped). The migration ensures the table is readable; the API surface is implemented in `../src/api/`.

## Related Files & References

- [`../README.md`](../README.md) — Service-level README
- [`../config/default.yaml`](../config/default.yaml) — Database pool sizing, statement timeout, saga config
- [`../.env.example`](../.env.example) — `POSTGRES_URL` and `SAGA_*` env var template (no real credentials)
- [`../requirements.txt`](../requirements.txt) — Python dependency manifest (Alembic, SQLAlchemy 2.0, psycopg v3)
- [`../Dockerfile`](../Dockerfile) — References this folder via `COPY --chown=${APP_USER}:${APP_USER} migrations/ ./migrations/`
- [`../../payment-service/migrations/README.md`](../../payment-service/migrations/README.md) — Sibling Alembic reference (canonical structural pattern)
- [`../../../docs/architecture/data-stores.md`](../../../docs/architecture/data-stores.md) — Database-per-service rationale and polyglot persistence
- [`../../../docs/architecture/resilience-patterns.md`](../../../docs/architecture/resilience-patterns.md) — Saga compensation matrix and retry policies
- [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md) — System architecture diagram
- [`../../../docs/runbook/order-service.md`](../../../docs/runbook/order-service.md) — Operational runbook (saga manual intervention, DLQ replay, schema migration procedure)

AAP citations (non-clickable):

- AAP Section 0.4.4 — Database schema for `order_db` (4 tables)
- AAP Section 0.5.2.5 — Per-service migrations folder requirement
- AAP R-6 — Database per service strict isolation (no cross-service foreign keys)
- AAP R-7 — Polyglot persistence (PostgreSQL for `order_db`)
- AAP R-9 — Migrations under owning service, applied automatically on startup or via dedicated Job
- **AAP R-18 — Saga pattern with explicit compensation; `saga_state` durability is the unique architectural focus of the Order Service**
- AAP R-19 — Fail fast on missing critical dependencies
- AAP R-25 — Secrets never in source or configuration files
