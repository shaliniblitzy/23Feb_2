# Inventory Service — Database Migrations

Forward-only, versioned Alembic migration scripts that create and evolve the Inventory Service's private PostgreSQL schema (`inventory_db`), with optimistic-lock concurrency control, idempotent reservation insertion, and a deadline-driven release scheduler that backstops stuck sagas.

## Purpose

This folder contains versioned **Alembic** (Python) migration scripts for the **private PostgreSQL database** `inventory_db` owned by the Inventory Service.

- Per **AAP R-6** (database per service), no other service reads from or writes to this database. Cross-service references such as `stock_items.product_id`, `reservations.order_id`, and `stock_movements.order_id` are stored as **opaque UUIDs without foreign keys** — referential integrity across service boundaries is maintained through Kafka events (`order.*` consumed; `inventory.*` produced) and saga semantics, never through SQL joins.
- Per **AAP R-7** (polyglot persistence — UNIQUE among the eleven services), PostgreSQL is the chosen store specifically because reservations require **ACID transactional integrity**. Overselling is a critical business defect that document stores cannot defensibly prevent without complex two-phase coordination; the relational engine's row-level locking, MVCC, and unique constraints together give us the correctness primitives we need.
- Per **AAP R-9**, migrations **must** be applied automatically — either on service startup (entrypoint or initContainer) or via a dedicated migration Kubernetes Job / CI step. The `RUN_MIGRATIONS_ON_STARTUP` env var (declared in [`../.env.example`](../.env.example) Group K) toggles between these modes; pods must not begin serving traffic until all pending migrations have completed successfully.
- Per **AAP R-25**, the database connection URL is supplied at runtime via the `POSTGRES_URL` environment variable. **No credentials live in this folder.**
- Per **AAP R-20** (UNIQUE focus of the Inventory Service among the eleven services), the schema supports a deadline-driven reservation expiration scheduler that releases stuck reservations automatically. Without this safety net, a Payment Service outage would freeze stock indefinitely because a stuck saga never sends `order.cancelled` or `order.fulfilled`. See [Reservation Expiration Scheduler (AAP R-20)](#reservation-expiration-scheduler-aap-r-20) for the complete fallback story.

## Toolchain

Read this section first — it documents the **Alembic + SQLAlchemy + psycopg3 v3 dialect** stack and the service-specific Alembic version table that operators must understand before running any migration command.

- **Migration runner:** [Alembic](https://alembic.sqlalchemy.org/) (`alembic~=1.14.0`, pinned in [`../requirements.txt`](../requirements.txt)).
- **Driver dialect:** SQLAlchemy `~=2.0.36` with the **psycopg v3** dialect — URL prefix `postgresql+psycopg://`.
  - **CRITICAL:** Use `postgresql+psycopg://` (psycopg v3) — **never** `postgresql+psycopg2://` (legacy v2). The service's [`../requirements.txt`](../requirements.txt) declares `psycopg[binary,pool]~=3.2.0`; psycopg2 is not installed and `env.py` rejects `postgresql+psycopg2://` URLs with an explicit `RuntimeError` rather than letting SQLAlchemy raise an opaque `ModuleNotFoundError`.
- **Required PostgreSQL extension:** `pgcrypto` (enabled by the initial migration). Provides `gen_random_uuid()` for primary keys (UUID v4) — the Inventory Service uses UUIDs for `warehouses.id`, `stock_items.id`, `reservations.id`, `reservation_items.id`, and the correlation IDs propagated through every reservation, release, fulfillment, and stock movement. PostgreSQL 13+ also exposes a built-in `gen_random_uuid()` without `pgcrypto`, but the extension is enabled explicitly for compatibility with older managed PostgreSQL services that still ship the function only via contrib.
- **Why Alembic with `target_metadata = None`?**
  - The schema is defined imperatively in revision files via `op.create_table()`, `op.create_index()`, `op.create_check_constraint()`, and `op.execute()` for raw SQL where needed (for example, the partial index on `(status, expires_at)` for the expiration scheduler scan, table comments, and `CREATE EXTENSION IF NOT EXISTS pgcrypto`).
  - Autogenerate is intentionally disabled because every DDL must be reviewed line-by-line; we do not want a missing ORM model attribute to silently drop a column on apply — and especially not a column referenced by the reservation engine or the optimistic-lock predicate.
  - This is the same pattern Order Service and Payment Service follow; see [`../../order-service/migrations/README.md`](../../order-service/migrations/README.md) for additional rationale.
- **Service-specific Alembic version table:** This service uses **`alembic_version_inventory_service`** (not the default `alembic_version`). The custom name lets operators safely run all services' migrations against a single Postgres cluster in local development without colliding on a shared bookkeeping table. The name is set in [`./env.py`](./env.py) via `context.configure(version_table="alembic_version_inventory_service", ...)` on **both** the offline and online migration paths.

## File Layout

```text
migrations/
├── alembic.ini                          # Alembic CLI config; sqlalchemy.url is empty by design
├── env.py                               # Online + offline runtime; reads POSTGRES_URL and rewrites the dialect
├── script.py.mako                       # Mako template used to generate new revision files
├── README.md                            # This file
└── versions/                            # Individual revision files (chronologically ordered)
    ├── 0001_create_warehouses.py            # CREATE TABLE warehouses + CREATE EXTENSION pgcrypto
    ├── 0002_create_stock_items.py           # CREATE TABLE stock_items (FK to warehouses) + version column
    ├── 0003_create_reservations.py          # CREATE TABLE reservations + reservation_items (FK CASCADE)
    ├── 0004_create_stock_movements.py       # CREATE TABLE stock_movements (append-only audit trail)
    ├── 0005_seed_default_warehouse.py       # Seed: default warehouse row for local dev (ON CONFLICT DO NOTHING)
    └── 0006_indexes_and_constraints.py      # Composite indexes + partial indexes (incl. (status, expires_at))
```

- **`alembic.ini`** — CLI configuration. The `sqlalchemy.url` key is intentionally **empty**; `env.py` populates it at runtime from `POSTGRES_URL` (AAP R-25).
- **`env.py`** — Defines both online and offline migration modes; reads `POSTGRES_URL`, rewrites a bare `postgresql://` URL to `postgresql+psycopg://`, and propagates `version_table=alembic_version_inventory_service` to both `context.configure()` calls.
- **`script.py.mako`** — Mako template used by `alembic revision -m "..."` to scaffold new revision files.
- **`versions/0001_create_warehouses.py`** — Enables `pgcrypto` and creates the `warehouses` registry with the `name` UNIQUE constraint, the `status` CHECK, and the `adapter_type` strategy column.
- **`versions/0002_create_stock_items.py`** — Creates `stock_items` with the intra-DB FK `warehouse_id REFERENCES warehouses(id)`, the UNIQUE constraint on `(product_id, warehouse_id)`, the optimistic-lock `version` column, and the `available_qty >= 0` / `reserved_qty >= 0` CHECK constraints.
- **`versions/0003_create_reservations.py`** — Creates `reservations` (with the `order_id` UNIQUE idempotency constraint, the `expires_at` deadline column, and the `correlation_id` AAP R-13 column) and the child `reservation_items` table (with `ON DELETE CASCADE` from `reservation_id` and the UNIQUE constraint on `(reservation_id, product_id, warehouse_id)`).
- **`versions/0004_create_stock_movements.py`** — Creates the append-only `stock_movements` audit trail with `BIGSERIAL id`, `movement_type` CHECK, before/after quantity columns, `correlation_id`, and the JSONB `metadata` column.
- **`versions/0005_seed_default_warehouse.py`** — Inserts a default warehouse row for local development using `INSERT ... ON CONFLICT DO NOTHING` for safe re-runnability.
- **`versions/0006_indexes_and_constraints.py`** — Adds composite and partial indexes that depend on multiple tables — notably the partial index `idx_reservations__expiry WHERE status = 'ACTIVE'` for the expiration scheduler hot path, and the `(product_id, occurred_at DESC)` composite index on `stock_movements`.

The bootstrap revision filenames use simple ordinal prefixes (`0001_`–`0006_`) to convey the intended apply order at a glance. Future revisions created via `alembic revision -m "..."` will use the **`<YYYYMMDD>_<HHMMSS>_<slug>.py`** template per the `file_template` setting in [`./alembic.ini`](./alembic.ini); both styles coexist because Alembic resolves the dependency graph from the `down_revision` chain, not from filename order.

## Naming Convention

- **Bootstrap revision filenames:** `<NNNN>_<snake_case_slug>.py` (for example, `0001_create_warehouses.py`).
- **Subsequent (post-bootstrap) revisions:** `<YYYYMMDD>_<HHMMSS>_<snake_case_slug>.py` per the `file_template` setting in [`./alembic.ini`](./alembic.ini); the `timezone = UTC` setting guarantees deterministic, contributor-independent timestamps.
- Each revision file declares `revision = "<id>"` at the top (for example, `revision = "0001"` or `revision = "20260201_000001"`); the file's identifier matches the filename prefix exactly.
- New revisions are generated by `alembic revision -m "short description"`. The `--autogenerate` flag is **not used** because `target_metadata = None` (see [Toolchain](#toolchain)); migrations are written by hand.
- **Never reuse** a revision id.
- **Never edit** a revision file that has already been applied in any environment — see [Rollback Policy](#rollback-policy).

## Rollback Policy

Migrations in this folder are **forward-only** in production.

- Do **not** edit a migration that has been applied in any environment.
- Each Alembic revision **does** define a `downgrade()` function (best practice for reversibility), but `downgrade` is reserved for **local development and CI round-trip tests** — never for production rollback.
- To undo a change in production, **create a new forward migration** (for example, `20260201_000001_revert_xyz.py`) that contains the corrective DDL.
- For catastrophic recovery, **restore from a database snapshot** rather than running a reverse migration against live data.
- **Append-only `stock_movements` note.** The `stock_movements` table is operationally critical as the audit trail of every reservation, release, fulfillment, replenishment, expiration, and adjustment. UPDATE and DELETE statements **must never** touch it; the repository layer enforces this, and revoking UPDATE/DELETE privileges at the Postgres role level is recommended for defense in depth. Migration scripts must not alter rows in `stock_movements` after insert.
- **Reservation expand-contract.** Schema changes to `reservations` or `reservation_items` **must** be deployed in a multi-step expand-contract pattern; never break the schema while reservations may be in flight. Doing so can leave in-flight orders permanently stuck or trigger spurious double-reservations.

## Idempotence

Every operation in the migration files is safe to reapply.

- Raw-SQL paths invoked via `op.execute()` use `IF NOT EXISTS` / `OR REPLACE` semantics where applicable. The `pgcrypto` extension is enabled with `CREATE EXTENSION IF NOT EXISTS pgcrypto` in the initial revision and is **intentionally not dropped** in `downgrade()` because the extension may be shared with other services co-located on the same dev database.
- For schema-builder calls such as `op.create_table()`, Alembic's revision tracking via the service-specific `alembic_version_inventory_service` bookkeeping table prevents reapplication of the same revision against the same database.
- The `0005_seed_default_warehouse.py` revision uses `INSERT ... ON CONFLICT DO NOTHING` so re-running it (for example, after a manual `DELETE` of the seed row) is a no-op when the row is already present.
- CI runs the round-trip sequence `alembic upgrade head` → `alembic downgrade base` → `alembic upgrade head` to verify reversibility and idempotence on every pull request that touches this folder.

## Schema Summary

The five tables required by **AAP Section 0.4.4** (the four named tables plus the `reservation_items` child of `reservations`) are created by the bootstrap migrations; all performance indexes and CHECK constraints are added inline within the revision that creates the parent table or in the dedicated `0006_indexes_and_constraints.py` revision.

| Table | Purpose | Created by |
|-------|---------|------------|
| `warehouses` | Warehouse registry — `id`, `name` UNIQUE, `region`, `status`, **`adapter_type`** (drives strategy pattern), `config` JSONB, `created_at`, `updated_at` | `0001_create_warehouses.py` |
| `stock_items` | Per `(product_id, warehouse_id)` tuple — `id`, `product_id`, `warehouse_id` (FK), `available_qty`, `reserved_qty`, `low_stock_threshold`, **`version`** (optimistic locking), `updated_at`; UNIQUE `(product_id, warehouse_id)` | `0002_create_stock_items.py` |
| `reservations` | One row per active reservation — `id`, **`order_id` UNIQUE** (idempotency), `status`, **`expires_at`** (deadline), `correlation_id`, `created_at` | `0003_create_reservations.py` |
| `reservation_items` | Child of reservations — `id`, `reservation_id` (FK CASCADE), `product_id`, `warehouse_id` (FK), `quantity`; UNIQUE `(reservation_id, product_id, warehouse_id)` | `0003_create_reservations.py` |
| `stock_movements` | **Append-only audit trail** — `BIGSERIAL id`, `product_id`, `warehouse_id` (FK), `movement_type`, `quantity_delta`, before/after qtys, `order_id`, `reservation_id`, **`correlation_id` (AAP R-13)**, `actor`, `metadata` JSONB, `occurred_at` | `0004_create_stock_movements.py` |

The `0005_seed_default_warehouse.py` revision seeds a single default warehouse row for local-development convenience (uses `ON CONFLICT DO NOTHING`). The `0006_indexes_and_constraints.py` revision adds composite and partial indexes that depend on multiple tables existing first — notably the `(status, expires_at)` partial index on `reservations` for the expiration scheduler hot-path query.

## Schema Isolation (AAP R-6)

This database is **private** to the Inventory Service.

- **No foreign keys** point at tables in any other service's database (`auth_db`, `user_db`, `product_db`, `order_db`, `payment_db`, `notification_db`).
- `stock_items.product_id`, `reservations.order_id`, `reservation_items.product_id`, `stock_movements.product_id`, and `stock_movements.order_id` are stored as **opaque UUIDs**. Cross-service joins are forbidden — the Inventory Service learns about external entities exclusively via Kafka events it consumes (`order.created`, `order.cancelled`, `order.fulfilled`) and via REST calls fronted by the API Gateway.
- **Intra-database** foreign keys are used (and are encouraged) because all five tables live in the same private database:
  - `stock_items.warehouse_id REFERENCES warehouses(id)` — **no CASCADE**; warehouse decommissioning requires an explicit migration path so we never silently lose stock rows.
  - `reservation_items.reservation_id REFERENCES reservations(id) ON DELETE CASCADE` — when a reservation is purged, its line items follow.
  - `reservation_items.warehouse_id REFERENCES warehouses(id)` — **no CASCADE**.
  - `stock_movements.warehouse_id REFERENCES warehouses(id)` — **no CASCADE**; preserves the audit trail even if a warehouse is decommissioned.
- Cross-service consistency is achieved exclusively via Kafka events (`order.*` consumed; `inventory.*` produced) and saga semantics (AAP R-18 — orchestrated by the Order Service), never via SQL.

## Reservation Idempotency

The schema enforces reservation idempotency at the database layer to make Kafka's at-least-once delivery semantics safe.

- The `reservations` table carries a `UNIQUE` constraint on `order_id`.
- When the Inventory Service consumes an `order.created` Kafka event, the reservation insert path uses `INSERT ... ON CONFLICT (order_id) DO NOTHING` (or the equivalent SQLAlchemy `on_conflict_do_nothing()` clause). Subsequent duplicate deliveries — caused by Kafka retries, consumer offset rewinds, or Order Service re-emission after a transient failure — become NOOPs after the first successful insert.
- The unique constraint guarantees **at-most-once** stock reservation per order, even under concurrent retries from the Order Service or Kafka redelivery from offset rewinds.
- This is the primary defense against double-allocation of stock when an `order.created` event is delivered more than once.

## Optimistic Locking (AAP R-15)

The `stock_items.version` column is the foundation of the service's concurrent-reservation safety story.

- The `version` column (BIGINT, default `0`) is incremented on every UPDATE.
- Stock reservation transactions follow this pattern:
  1. `SELECT id, available_qty, reserved_qty, version FROM stock_items WHERE product_id = ? AND warehouse_id = ? FOR UPDATE`
  2. Validate `available_qty >= requested_qty`.
  3. `UPDATE stock_items SET available_qty = available_qty - ?, reserved_qty = reserved_qty + ?, version = version + 1, updated_at = NOW() WHERE id = ? AND version = ?`
  4. If the UPDATE returns 0 rows (version mismatch — another concurrent transaction won), abort and retry per the application's `STOCK_OPTIMISTIC_LOCK_*` env policy (max retries, exponential backoff with jitter — see [`../.env.example`](../.env.example)).
- The combination of `SELECT ... FOR UPDATE` (pessimistic during the read-validate-update sequence) and optimistic `version`-checking on the UPDATE provides defense-in-depth against contention scenarios that exceed a single transaction's isolation level. AAP R-15's "retries with exponential backoff" requirement is satisfied by the application-level retry loop driven by the `STOCK_OPTIMISTIC_LOCK_BACKOFF_*` env knobs.
- Sustained nonzero `stock_optimistic_lock_retries_total` metric (exposed at `/metrics`) indicates contention hotspots — typically a flash-sale SKU. Alert thresholds should trigger investigation but not auto-page unless retries exhaust.

## Reservation Expiration Scheduler (AAP R-20)

This is the **single most distinguishing concern** of the Inventory Service among the eleven services in the monorepo: the `(status, expires_at)` partial index on `reservations` is the architectural linchpin of the service's R-20 fallback story.

- Every `reservations` row has an `expires_at TIMESTAMPTZ NOT NULL` deadline (typically `NOW() + RESERVATION_EXPIRY_MS` from env, default 15 minutes — see [`../.env.example`](../.env.example)).
- A background scheduler (Inventory Service runtime, **not** migrations) periodically scans for expired reservations using the partial index `idx_reservations__expiry`:

  ```sql
  SELECT id FROM reservations
  WHERE status = 'ACTIVE' AND expires_at < NOW()
  LIMIT $batch_size;
  ```

- The partial index `WHERE status = 'ACTIVE'` keeps the scan tiny (only active reservations are indexed), which is critical because `reservations` grows by every order ever placed; a non-partial index would degrade the scheduler scan to `O(all-reservations)` instead of `O(stale-rows)`.
- For each expired row, the scheduler:
  1. Updates the reservation's `status` to `'EXPIRED'`.
  2. Increments `stock_items.available_qty` and decrements `stock_items.reserved_qty` (with the optimistic-lock predicate).
  3. Inserts a `stock_movements` row with `movement_type = 'EXPIRE'`.
  4. Emits an `inventory.released` Kafka event with `expired = true`.
- **Without this scheduler, a Payment Service outage would freeze stock indefinitely** because a stuck saga never sends `order.cancelled` or `order.fulfilled`. The `(status, expires_at)` partial index is the critical schema enabler — it is what makes the scheduler's hot path index-only at production scale.

## Append-Only Stock Movements Audit (AAP R-13)

The `stock_movements` table is the inventory's **forensic timeline** and the canonical place where AAP R-13's correlation-ID requirement is persisted.

- The table is **append-only**: every reservation, release, fulfillment, replenishment, expiration, and adjustment writes one row. UPDATE and DELETE are forbidden by repository convention; revoking those privileges at the Postgres role level is recommended for defense in depth.
- Each row captures `before_available_qty`, `after_available_qty`, `before_reserved_qty`, `after_reserved_qty`, `quantity_delta`, **`correlation_id` (AAP R-13)**, `actor` (`'system'` for automated changes, `'admin'` or an admin user id for manual replenishment), and a `metadata JSONB` column for extensibility (for example, source IP for admin actions, batch id for bulk imports, idempotency key from the originating Kafka event).
- This timeline answers any "where did 5 units of SKU X go between Tuesday and Wednesday" question with a single query: `SELECT * FROM stock_movements WHERE product_id = ? AND occurred_at BETWEEN ... ORDER BY occurred_at`.
- The table is a **partition candidate**. For production-scale workloads, partition by `occurred_at` monthly using declarative range partitioning. The bootstrap migrations create a non-partitioned table; partitioning DDL may be added in a future migration once write throughput crosses the threshold described in [Troubleshooting](#troubleshooting).
- The `(occurred_at)` index enables time-range scans; the `(product_id, occurred_at DESC)` composite index enables per-SKU history queries; the `(order_id)` index enables order-traceability queries that reconstruct the inventory side of an order's saga.

## Warehouse Adapter Pattern

The `warehouses.adapter_type` column drives the runtime strategy-pattern selection per AAP Section 0.4.3, keeping adapter choice **data-driven** rather than hard-coded in source.

- The column is declared as `adapter_type VARCHAR(32) NOT NULL DEFAULT 'database'`.
- Supported adapter types — extensible without further migration when new values are added by future code:
  - `'database'` — Default `DatabaseWarehouseAdapter` manages stock entirely inside `inventory_db`.
  - `'external_wms'` — Future `ExternalWMSWarehouseAdapter` bridges to a third-party warehouse-management system over REST/AMQP.
- The companion `warehouses.config JSONB NOT NULL DEFAULT '{}'` column carries adapter-specific configuration (for example, WMS endpoint URL, an opaque reference to API credentials stored in a secret manager, sync interval, region-specific overrides).
- The migration creates the column and default; runtime code in `../src/` reads the column and instantiates the appropriate adapter strategy. **Hard-coded adapter selection in source is a code smell** — always drive adapter choice from this column.

## Application Workflow

Both AAP R-9-compliant workflows are supported. Pick one per environment and stick to it.

- **On service startup** — preferred for local development and single-replica environments. The `RUN_MIGRATIONS_ON_STARTUP=true` env flag (see [`../.env.example`](../.env.example) Group K) causes the entrypoint script to run `alembic upgrade head` **before** the FastAPI / uvicorn process starts accepting traffic. A failure is fatal: the container exits non-zero and the readiness probe stays `503` (AAP R-19).
- **Dedicated migration Job / CI step** — **strongly recommended for production**. Set `RUN_MIGRATIONS_ON_STARTUP=false` and run `alembic upgrade head` once per deployment via a Kubernetes `Job` (see `deploy/k8s/inventory-service-migrate-job.yaml`, owned by the infrastructure agent); service pods only start after the Job has succeeded. This pattern is safer for multi-replica zero-downtime rollouts because it eliminates the race between concurrent replicas all trying to migrate the same database at startup.
- **Concurrent reservation note.** During a rolling deployment, in-flight reservations may span the cutover. The expand-contract pattern (see [Rollback Policy](#rollback-policy)) ensures both old and new pod versions can read and write `reservations` and `stock_items` without loss. Avoid running migrations that drop columns referenced by the reservation engine until all old replicas have terminated.

## Local Development Workflow

Concrete commands for day-to-day development:

```bash
# From the service root: services/inventory-service/
# Ensure POSTGRES_URL is set (e.g., from a local .env file loaded into the shell).
# Use a development-only password and a local Postgres instance:
export POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/inventory_db"

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

# Inspect the alembic state table
psql "$POSTGRES_URL" -c "SELECT * FROM alembic_version_inventory_service;"
```

The `postgres:postgres` credentials above are placeholder values for a developer's local Docker container only — never use them anywhere else.

## Environment Variables

`env.py` reads exactly the following variables:

- **`POSTGRES_URL`** — **REQUIRED** at runtime. Format: `postgresql://<user>:<pass>@<host>:<port>/inventory_db`. A bare `postgresql://` scheme is rewritten to `postgresql+psycopg://` automatically by `env.py`; an explicit `postgresql+psycopg://` URL is also accepted unchanged. An explicit `postgresql+psycopg2://` URL is rejected with an actionable `RuntimeError` because psycopg2 is not pinned in [`../requirements.txt`](../requirements.txt). Per AAP R-19, missing `POSTGRES_URL` causes `env.py` to exit with a non-zero status code so the failure is loud and immediate.
- **`ALEMBIC_LOG_LEVEL`** — *Optional.* Default: `WARNING`. Set to `INFO` or `DEBUG` for verbose Alembic logging during debugging or scheduled migration windows; an unknown value falls back to `WARNING`.

**No credentials live in `alembic.ini`** (AAP R-25). The `sqlalchemy.url` key in [`./alembic.ini`](./alembic.ini) is intentionally empty; `env.py` populates it at runtime from `POSTGRES_URL`.

The service-level [`../.env.example`](../.env.example) also documents `RUN_MIGRATIONS_ON_STARTUP` (Group K) and `MIGRATIONS_DIR=migrations`; these are read by the application's entrypoint script, **not** by `env.py` directly.

## Troubleshooting

The most common issues encountered while running migrations:

- **`Can't load plugin: sqlalchemy.dialects:postgresql.psycopg`** — install `psycopg[binary,pool]>=3` (already declared in [`../requirements.txt`](../requirements.txt)). Verify with `pip show psycopg` (the package is named `psycopg`, not `psycopg2` or `psycopg3`).
- **`No module named 'psycopg2'`** — **do not** install `psycopg2`. The URL must use `postgresql+psycopg://` (v3), not `postgresql+psycopg2://`. `env.py` performs this rewrite automatically when given a bare `postgresql://` URL; if you provided an explicit `postgresql+psycopg2://` URL, change it to `postgresql+psycopg://` (or strip the `+psycopg2` suffix and let `env.py` rewrite it).
- **`Target database is not up to date`** — run `alembic upgrade head` first. Alembic refuses to generate a new revision until the database is at the latest known head.
- **`Multiple head revisions detected`** — indicates a merge conflict between two parallel migration branches. Reconcile with `alembic merge -m "merge heads" <head1> <head2>` and commit the resulting merge revision.
- **`relation "alembic_version_inventory_service" does not exist`** — the database has never had Alembic applied. Just run `alembic upgrade head`; Alembic auto-creates the (service-specific) version table on first apply.
- **`extension "pgcrypto" is not available`** — the running PostgreSQL build does not include contrib modules. Use a contrib-enabled image (for example `postgres:16-bookworm`) or install the `postgresql-contrib` package on the host. As a fallback, drop the `CREATE EXTENSION` line and rely on PostgreSQL 13+'s built-in `gen_random_uuid()` (the `0001` migration's docstring documents this fallback path).
- **`permission denied to create extension "pgcrypto"`** — Alembic is connecting as a non-superuser. Either (a) grant the user the `CREATE` privilege on the database and `SUPERUSER` (dev only), (b) pre-create the extension as a superuser one-time before the migration runs, or (c) execute `CREATE EXTENSION IF NOT EXISTS pgcrypto` in a separate bootstrap step (Terraform / Helm hook) before invoking Alembic.
- **`unique violation on alembic_version_inventory_service`** — should not happen with the service-specific version table name; if it does, multiple migration runners are racing. Ensure exactly one runner (initContainer **or** Job, never both) applies migrations per deployment.
- **`unique violation on reservations.order_id` during runtime** — expected and handled by the application layer's `ON CONFLICT DO NOTHING` clause; if the violation surfaces as an error, the application is missing the conflict handler — fix the repository code, **not** the schema.
- **`stock_items.version` deadlock or repeated optimistic-lock failures** — indicates two concurrent reservation transactions attempted to update the same row; one is aborted and retried per the `STOCK_OPTIMISTIC_LOCK_MAX_RETRIES` env policy. Sustained deadlocks indicate a hot SKU; consider warehouse-aware sharding for that product or rate-limiting the offending campaign at the gateway.
- **`stock_movements` write rate sustained > 1k/s** — time to partition by `occurred_at` monthly. Add a future migration with `CREATE TABLE stock_movements_2026_01 PARTITION OF stock_movements FOR VALUES FROM (...) TO (...);` etc.

## Testing

An integration test under [`../tests/integration/`](../tests) (authored by sibling agents) exercises the migration suite end-to-end:

- Uses **Testcontainers** to spin up a fresh `postgres:16` container per test run.
- Sets `POSTGRES_URL` to the container's connection string.
- Runs `alembic upgrade head` and asserts:
  - All five tables (`warehouses`, `stock_items`, `reservations`, `reservation_items`, `stock_movements`) exist with the expected columns and types (verified via `information_schema.columns`).
  - The `pgcrypto` extension is enabled (`SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto'`).
  - The `alembic_version_inventory_service` table exists with exactly one row pointing at the latest revision id.
  - The UNIQUE constraint on `reservations.order_id` is present (idempotency safety per [Reservation Idempotency](#reservation-idempotency)).
  - The UNIQUE constraint on `stock_items (product_id, warehouse_id)` is present.
  - The UNIQUE constraint on `reservation_items (reservation_id, product_id, warehouse_id)` is present.
  - The intra-DB FKs exist with the documented CASCADE / NO-CASCADE policies (see [Schema Isolation (AAP R-6)](#schema-isolation-aap-r-6)).
  - The composite index `idx_stock_items__product_warehouse` and the partial index `idx_reservations__expiry WHERE status = 'ACTIVE'` exist.
  - The composite indexes `idx_stock_movements__product_time` (on `product_id, occurred_at DESC`), `idx_stock_movements__order` (on `order_id`), and `idx_stock_movements__time` (on `occurred_at`) exist.
  - All CHECK constraints are present (status enums, movement_type enum, `quantity > 0`, `available_qty >= 0`, `reserved_qty >= 0`, `low_stock_threshold >= 0`).
- Re-runs `alembic upgrade head` to verify no-op idempotence on a database already at head.
- Round-trips `alembic downgrade base` then `alembic upgrade head` to verify reversibility.
- Verifies **optimistic-lock semantics**: inserts a `stock_items` row with `version = 0`, then issues two concurrent UPDATEs with `WHERE version = 0` and asserts exactly one returns `1 row updated` and the other returns `0 rows updated`.
- Verifies **idempotency-by-order**: inserts a `reservations` row, then attempts a second insert with the same `order_id` and asserts the second insert is rejected (or NOOP'd via `ON CONFLICT DO NOTHING`).
- Verifies **expiration scheduler partial-index efficacy**: inserts an active reservation with `expires_at < NOW()`, runs the scheduler's `WHERE` clause, and asserts the row is returned via an `Index Scan` (not `Seq Scan`) on `idx_reservations__expiry` per `EXPLAIN (ANALYZE, BUFFERS)`.

## Cross-Coupling Awareness

The CHECK-constraint string literals encoded in these migrations are **coupled** with the SQLAlchemy / Pydantic models in `../src/repository/models.py` and with the enum definitions in `../src/domain/inventory_types.py` (or equivalent modules). Changing a string literal in one place without coordinated changes in the other(s) results in **runtime CHECK-constraint violations** that immediately fail reservation processing.

The exact coupled value sets:

- `warehouses.status` uses `'ACTIVE' | 'MAINTENANCE' | 'DECOMMISSIONED'` (UPPERCASE) — must match the `WarehouseStatus` `StrEnum` values.
- `reservations.status` uses `'ACTIVE' | 'RELEASED' | 'FULFILLED' | 'EXPIRED'` (UPPERCASE) — must match the `ReservationStatus` `StrEnum` values.
- `stock_movements.movement_type` uses `'RESERVE' | 'RELEASE' | 'FULFILL' | 'EXPIRE' | 'REPLENISH' | 'ADJUST'` (UPPERCASE) — must match the `StockMovementType` `StrEnum` values.
- `warehouses.adapter_type` accepts `'database' | 'external_wms'` (lowercase) — must match the adapter-registry keys in `../src/adapters/`.

When you change a domain enum, you **must** add a forward migration (see [Rollback Policy](#rollback-policy)) that updates the corresponding CHECK constraint, and vice versa. Schema and code drift here surfaces as immediate reservation failures, not silent corruption — but that visibility relies on the CHECK constraints existing in the first place.

## Concurrent Reservation Semantics

The migration scripts here do **not** implement reservation logic — that is application-level behavior in `../src/`. The migrations **do** ensure the schema can support atomic reservations via these primitives: `stock_items.version` for optimistic locking (see [Optimistic Locking (AAP R-15)](#optimistic-locking-aap-r-15)); `stock_items (product_id, warehouse_id)` UNIQUE for one canonical row per (SKU, warehouse); `reservations.order_id` UNIQUE for at-most-once reservation per order (see [Reservation Idempotency](#reservation-idempotency)); `reservation_items (reservation_id, product_id, warehouse_id)` UNIQUE to prevent duplicate line items; and `CHECK (available_qty >= 0)` / `CHECK (reserved_qty >= 0)` on `stock_items` as defense-in-depth against negative-stock corruption (a malformed admin replenishment query fails loudly here instead of silently corrupting stock).

The application's reservation procedure:

1. Begin transaction.
2. `SELECT ... FOR UPDATE` on each affected `stock_items` row, ordered by `id` to prevent deadlocks.
3. Validate sufficient stock per item.
4. UPDATE each `stock_items` row using the optimistic `version` predicate.
5. INSERT into `reservations` with `ON CONFLICT (order_id) DO NOTHING`; if the insert is a NOOP, abort the entire transaction (duplicate event).
6. INSERT into `reservation_items` (one row per line).
7. INSERT into `stock_movements` (one row per affected SKU).
8. Commit.
9. Emit `inventory.reserved` Kafka event with the full reservation details.

**The single transaction in steps 1–8 is the heart of the service's correctness story.** Schema changes that break atomicity (for example, splitting `stock_items` into two tables without coordinated locking) **must** be carefully reviewed before merging.

## Related Files & References

- [`../README.md`](../README.md) — Service-level README
- [`../config/default.yaml`](../config/default.yaml) — Database pool sizing, statement timeout, reservation expiry config
- [`../.env.example`](../.env.example) — `POSTGRES_URL` and `RESERVATION_*` / `STOCK_OPTIMISTIC_LOCK_*` env var template (no real credentials)
- [`../requirements.txt`](../requirements.txt) — Python dependency manifest (Alembic, SQLAlchemy 2.0, psycopg v3)
- [`../Dockerfile`](../Dockerfile) — References this folder via `COPY --chown=inventory:inventory migrations/ /app/migrations/`
- [`../../order-service/migrations/README.md`](../../order-service/migrations/README.md) — Sibling Alembic reference (saga coordinator companion)
- [`../../payment-service/migrations/README.md`](../../payment-service/migrations/README.md) — Sibling Alembic reference (saga peer)
- [`../../../docs/architecture/data-stores.md`](../../../docs/architecture/data-stores.md) — Database-per-service rationale and polyglot persistence
- [`../../../docs/architecture/resilience-patterns.md`](../../../docs/architecture/resilience-patterns.md) — Retry / circuit breaker / DLQ policies
- [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md) — System architecture diagram
- [`../../../docs/runbook/inventory-service.md`](../../../docs/runbook/inventory-service.md) — Operational runbook (DLQ replay, stuck reservation cleanup, threshold tuning)

AAP citations (non-clickable):

- AAP Section 0.4.4 — Database schema for `inventory_db` (5 tables: `warehouses`, `stock_items`, `reservations`, `reservation_items`, `stock_movements`)
- AAP Section 0.5.2.5 — Per-service migrations folder requirement
- AAP R-6 — Database per service strict isolation (no cross-service foreign keys)
- AAP R-7 — Polyglot persistence (PostgreSQL chosen for ACID transactional integrity)
- AAP R-9 — Migrations under owning service, applied automatically on startup or via dedicated Job
- AAP R-13 — Correlation-ID propagation in `stock_movements` and `reservations`
- AAP R-15 — Retry policies (optimistic-lock retries on `stock_items` contention)
- AAP R-19 — Fail fast on missing critical dependencies
- **AAP R-20 — Fallback paths: reservation expiration scheduler is the unique architectural focus of the Inventory Service**
- AAP R-25 — Secrets never in source or configuration files
