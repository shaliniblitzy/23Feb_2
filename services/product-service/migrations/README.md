# Product Service — Database Migrations

Forward-only, idempotent, version-tracked MongoDB migration scripts that bootstrap and evolve the Product Service's private database (`product_db`) — index definitions, `$jsonSchema` validators, and root-category seed data — applied automatically by a hand-rolled pymongo-based runner that is the MongoDB equivalent of the sibling services' Alembic toolchain.

## Why MongoDB Migrations?

This folder intentionally diverges from the **Alembic + SQLAlchemy + psycopg3 + PostgreSQL** pattern used by the sibling [`../../inventory-service/migrations/README.md`](../../inventory-service/migrations/README.md), [`../../order-service/migrations/README.md`](../../order-service/migrations/README.md), Payment Service, and Notification Service migration folders. The Product Service is the **one service** in the monorepo backed by **MongoDB** (per AAP R-7 — polyglot persistence) because catalog documents need flexible, per-product attribute schemas: apparel sizing, electronics specs, grocery dimensions. There is no `CREATE TABLE` equivalent to migrate.

- **MongoDB is schemaless at the document level.** Documents written via `INSERT` are accepted as-is, regardless of shape; there is no Alembic-style DDL to author.
- **However, schema management still applies to four dimensions:**
  1. **Index definitions** — UNIQUE, multikey, compound, and text indexes that the application code REQUIRES to be present at startup. Without them, queries are slow or admin writes succeed when they should fail at the DB layer.
  2. **`$jsonSchema` validators** — Optional but recommended document-shape validation rules registered with `db.create_collection(..., validator=...)` or `db.command("collMod", ..., validator=...)` to catch malformed documents on write.
  3. **Top-level taxonomy seed data** — Root categories (`Apparel`, `Electronics`, `Home`, `Groceries`) needed for the storefront skeleton to render at all on a fresh database. Product documents are NEVER seeded.
  4. **Versioned forward migrations** — When the application later changes a document shape (for example, adds a new attribute that older documents lack), idempotent scripts back-fill or reshape existing documents. Currently no shape-change migrations exist; the bootstrap migrations cover indexes + validators + seed.
- **Why not Alembic?** Alembic targets relational schemas (tables, columns, constraints). MongoDB's data model (collections, documents, indexes, validators) does not map cleanly to Alembic's `op.create_table()` operations. Hand-rolled pymongo scripts are idiomatic, lighter-weight, and more direct.
- **Why not Mongock or migrate-mongo?** Both are valid third-party choices. We chose a hand-rolled runner to avoid an extra dependency and to keep the migration mechanism transparent and grep-able. The runner is roughly 150 lines of Python and reuses the same idempotent-by-checksum pattern those libraries implement.
- **Sibling services use Alembic + PostgreSQL** — see [`../../inventory-service/migrations/README.md`](../../inventory-service/migrations/README.md) and [`../../order-service/migrations/README.md`](../../order-service/migrations/README.md) for the relational counterpart. The intent and discipline are identical (idempotent, versioned, owned by the service); only the implementation idiom differs. There is NO `alembic.ini`, NO SQL DDL, and NO `script.py.mako` in this folder. The [`./runner.py`](./runner.py) and [`./env.py`](./env.py) files are the MongoDB equivalents of Alembic's CLI runner and `env.py` configuration loader.

## Toolchain

Read this section first — it documents the **pymongo (sync) + hand-rolled runner** stack and the service-local `_migrations` bookkeeping collection that operators must understand before running any migration command.

- **Language:** Python 3.11+ (matching [`../requirements.txt`](../requirements.txt)).
- **Driver:** [pymongo](https://pymongo.readthedocs.io/) `>=4.6.0,<5.0.0` (sync API) — pinned in [`../requirements.txt`](../requirements.txt) BOTH because `motor` (the async MongoDB driver used by the application's runtime hot path) declares `pymongo` as a hard dependency AND because migration scripts use the sync API directly.
- **Why sync, not async?** Migrations run synchronously before uvicorn loads — either in the container entrypoint or in a Kubernetes Job. There is no event loop running yet. The sync API is simpler, has no `asyncio.run()` ceremony, and matches the linear "apply migration N before migration N+1" semantics. The application's runtime read/write paths use `motor` (async); migrations use `pymongo` (sync). Both drivers share the same wire protocol, the same connection-string format, and the same authentication surface.
- **Index, validator, and seed expression:** Pure Python dicts and lists; no DSL, no ORM, no SQL.
- **Version tracking:** A `_migrations` collection in `product_db` records `{_id: "<migration_id>", applied_at: <UTC datetime>, checksum: <sha256 of script>}` per applied migration. [`./runner.py`](./runner.py) queries this collection on startup to determine which migrations are pending, and writes one new row per successful apply.
- **Dependencies / pinning:** Both `pymongo` and `motor` are pinned in [`../requirements.txt`](../requirements.txt); no separate manifest is needed in this folder.
- **No Alembic. No SQLAlchemy. No psycopg. No Mongock or migrate-mongo dependency.**

## File Layout

```text
migrations/
├── README.md                                       # This file
├── runner.py                                       # Idempotent migration runner (pymongo sync)
├── env.py                                          # MongoDB connection bootstrap from env vars
├── versions/                                       # Sequenced migration scripts
│   ├── 0001_create_collections_and_indexes.py      # Initial schema bootstrap (indexes + validators)
│   ├── 0002_seed_root_categories.py                # Top-level taxonomy seed
│   └── 0003_text_index_for_search.py               # Text index on products.name + products.description
├── seed/                                           # Declarative seed data (consumed by versions/)
│   └── root_categories.json                        # Root categories for storefront skeleton
└── jsonschema/                                     # $jsonSchema validators (registered via collMod)
    ├── product_validator.json
    ├── category_validator.json
    └── product_media_validator.json
```

- **`README.md`** — This file. Operator-facing navigation hub for the folder.
- **`runner.py`** — The idempotent migration runner. Discovers files in `versions/` lexically, queries the `_migrations` collection for already-applied ids, computes a sha256 checksum per pending file, acquires an advisory lock, and invokes each script's `apply(database)` function in order.
- **`env.py`** — MongoDB connection bootstrap. Reads `MONGODB_URL`, `MONGODB_DATABASE`, and the optional `MONGODB_TLS_*` / timeout / retry env vars; constructs a `pymongo.MongoClient`; performs a fail-fast `admin.command("ping")` (per AAP R-19); and returns a `(client, database)` pair.
- **`versions/0001_create_collections_and_indexes.py`** — Creates the `products`, `categories`, and `product_media` collections with their `$jsonSchema` validators registered at create time, and creates all required non-text indexes on each collection.
- **`versions/0002_seed_root_categories.py`** — Reads [`./seed/root_categories.json`](./seed/root_categories.json) and upserts each root category via `find_one_and_update({"slug": <slug>}, {"$set": ..., "$setOnInsert": ...}, upsert=True)`.
- **`versions/0003_text_index_for_search.py`** — Creates the `idx_products__text_search` text index on `products.name + products.description`. Isolated from `0001` because text-index creation can be slow on populated collections, so its failure surface is reviewable independently and the language setting is configurable via `CATALOG_TEXT_SEARCH_LANGUAGE`.
- **`seed/root_categories.json`** — Declarative JSON file listing each root category and its slug, name, depth, display order, and metadata. Editing this file is the supported way to add a new top-level category; `0002_seed_root_categories.py` reads it on every run and upserts new entries idempotently.
- **`jsonschema/product_validator.json`** — `$jsonSchema` validator for the `products` collection.
- **`jsonschema/category_validator.json`** — `$jsonSchema` validator for the `categories` collection.
- **`jsonschema/product_media_validator.json`** — `$jsonSchema` validator for the `product_media` collection.

## Naming Convention

- Migration files in `versions/` are named `<NNNN>_<snake_case_slug>.py` (zero-padded to 4 digits).
- Examples: `0001_create_collections_and_indexes.py`, `0002_seed_root_categories.py`, `0003_text_index_for_search.py`.
- The 4-digit prefix establishes lexical ordering, which [`./runner.py`](./runner.py) uses to determine apply order.
- The `_id` recorded in the `_migrations` collection is the **filename without the `.py` extension** (for example, `_id = "0001_create_collections_and_indexes"`).
- New migrations are added with the next sequential number (for example, `0004_<slug>.py`).
- **Never reuse** a migration id.
- **Never edit** a migration file that has been applied in any environment — create a new forward migration instead.
- The runner additionally validates a sha256 checksum on re-runs: if a migration with id `0001_...` was previously applied with checksum `abc...` and now has checksum `def...`, the runner refuses to proceed (configurable via the `MIGRATIONS_CHECKSUM_MISMATCH_POLICY` env var: `error` (default) | `warn` | `ignore`). This prevents silent edit-after-apply mistakes.

## Rollback Policy

Migrations in this folder are **forward-only** in production.

- **Forward-only in production.** Do NOT edit a migration that has been applied in any environment.
- **No `downgrade()` function in MongoDB migrations.** Unlike Alembic-based migrations (which require a paired `upgrade()`/`downgrade()`), MongoDB migrations in this folder export only a single `apply(database)` function. Reasons:
  1. Index drops and validator removals are easy to express on the forward path, and most schema-evolution scenarios in MongoDB are additive (new fields are simply absent in old documents).
  2. Rolling back a seed insert is rarely useful (root categories should never be deleted).
  3. The forward-only discipline aligns with the production policy; there is no "local dev round-trip" use case strong enough to justify the boilerplate of always-paired downgrades.
- **To undo a change in production**, create a new forward migration (for example, `0005_drop_legacy_field_index.py`) that contains the corrective operation.
- **For catastrophic recovery**, restore from a database snapshot — NOT by running ad-hoc reverse scripts against live data.
- **Index drops** are the one common operation that is easy to express as a forward migration: `db.products.drop_index("idx_legacy")`.
- **Validator removal** is similarly simple: `db.command("collMod", "products", validator={})`.

## Idempotence

Every operation in the migration files is safe to reapply.

- **Index creation:** `pymongo.collection.create_index()` is idempotent **by index name**. Re-creating an index with the same name and identical options is a no-op. Always pass `name=` explicitly so the runner is in control of identity. Re-creating an index with the same name but DIFFERENT options raises `OperationFailure` — this is the desired behavior because it surfaces accidental schema drift.
- **Collection creation:** Use `db.create_collection(name, ...)` only when registering a `$jsonSchema` validator at create time; otherwise let the first `insert_one` create the collection implicitly. For collections that already exist, use `db.command("collMod", name, validator={...})` to update the validator without re-creating.
- **Seed inserts:** Use `find_one_and_update(filter, {"$set": doc, "$setOnInsert": {...}}, upsert=True)` with a natural-key filter (for example, `{"slug": "apparel"}`). This is idempotent: first apply inserts, subsequent applies are no-ops.
- **`_migrations` tracking:** The runner skips already-applied migrations by `_id`, so the `apply(database)` function is invoked at most once per migration per database (in the absence of checksum mismatches).
- **CI verification:** CI runs `python -m migrations.runner` twice in succession against a Testcontainers MongoDB instance and asserts the second run is a no-op (logs report `0 migrations to apply`).

## Schema Summary

The three target collections in `product_db` (per AAP Section 0.4.4 and the parent [`../README.md`](../README.md)) are created by the bootstrap migration; all required indexes and `$jsonSchema` validators are registered inline within the same migration that creates the parent collection (with the exception of the text index, which is isolated to `0003`).

| Collection | Purpose | Created By |
|------------|---------|------------|
| `products` | Product catalog: id, sku, name, slug, description, category_ids[], brand, price, currency, attributes (free-form sub-document), variants[] (embedded), media_ids[], status, version, created_at, updated_at | `0001_create_collections_and_indexes.py` |
| `categories` | Hierarchical category tree: id, name, slug, parent_id (nullable for root categories), depth, path[] (materialized ancestor chain), display_order, metadata, status | `0001_create_collections_and_indexes.py` (created); `0002_seed_root_categories.py` (root rows) |
| `product_media` | Per-product media metadata: id, product_id, cdn_url, kind (`image` \| `video` \| `360-spin`), alt_text, sort_order, created_at | `0001_create_collections_and_indexes.py` |

A bookkeeping collection (NOT a domain collection) also exists:

- `_migrations` — Migration state: `{_id: "<migration_id>", applied_at: <UTC datetime>, checksum: <sha256>}`. Created automatically by [`./runner.py`](./runner.py) on first apply.

## Required Indexes

This is the operationally most important section of the document. Every index below is referenced by an actual query in `../src/repository/`. Future agents adding indexes MUST update both the migration AND the corresponding repository.

**`products` collection:**

| Name | Specification | Type | Justification | Created By |
|------|---------------|------|---------------|------------|
| `idx_products__sku` | `{ sku: 1 }` | UNIQUE | SKU is the immutable canonical identifier — duplicates must fail at the DB layer | `0001` |
| `idx_products__slug` | `{ slug: 1 }` | UNIQUE | URL slugs must be globally unique; storefront URLs depend on this | `0001` |
| `idx_products__categories` | `{ category_ids: 1 }` | Multikey | Fast `GET /categories/{id}/products` listing | `0001` |
| `idx_products__status_created` | `{ status: 1, created_at: -1 }` | Compound | Fast admin-listing queries with status filter sorted by recency | `0001` |
| `idx_products__updated` | `{ updated_at: -1 }` | Single | Used by event-replay and outbox reconciliation queries | `0001` |
| `idx_products__text_search` | `{ name: "text", description: "text" }` | Text | `GET /products?q=...` full-text search | `0003` (separate revision because text indexes carry a language setting and are larger; isolating the operation makes its failure mode reviewable) |

**`categories` collection:**

| Name | Specification | Type | Justification | Created By |
|------|---------------|------|---------------|------------|
| `idx_categories__slug` | `{ slug: 1 }` | UNIQUE | URL routing; duplicates must fail | `0001` |
| `idx_categories__parent` | `{ parent_id: 1 }` | Single | Tree traversal — fetch children of a node | `0001` |
| `idx_categories__path` | `{ path: 1 }` | Multikey | Ancestor queries via materialized path | `0001` |
| `idx_categories__depth_order` | `{ depth: 1, display_order: 1 }` | Compound | Sorted listing per tree level | `0001` |

**`product_media` collection:**

| Name | Specification | Type | Justification | Created By |
|------|---------------|------|---------------|------------|
| `idx_product_media__product_sort` | `{ product_id: 1, sort_order: 1 }` | Compound | Fetch media for a product in display order | `0001` |

**Every index above is referenced by an actual query in `../src/repository/`.** Future agents adding indexes MUST update both the migration AND the corresponding repository.

## $jsonSchema Validators

The [`./jsonschema/`](./jsonschema/) subfolder contains three JSON files: [`./jsonschema/product_validator.json`](./jsonschema/product_validator.json), [`./jsonschema/category_validator.json`](./jsonschema/category_validator.json), [`./jsonschema/product_media_validator.json`](./jsonschema/product_media_validator.json). Each file contains a complete `$jsonSchema` document expressing required fields, types, enums, and constraints for one collection.

The initial migration `0001_create_collections_and_indexes.py` reads these files and registers them via `db.create_collection(..., validator={"$jsonSchema": {...}})` for newly-created collections, or `db.command("collMod", ..., validator={"$jsonSchema": {...}}, validationLevel="moderate", validationAction="warn")` for collections that already exist.

- **Validation level / action policy:**
  - **Development / test:** `validationLevel="strict"`, `validationAction="error"` — malformed writes fail loudly so developers catch bugs immediately.
  - **Production:** `validationLevel="moderate"`, `validationAction="warn"` — old documents that pre-date a validator change continue to be readable; new and updated documents are warned-on (logged) but not rejected, preventing a runtime outage from a too-strict validator.
  - The `MIGRATIONS_VALIDATION_ACTION` env var (`error` | `warn`; default `error` for non-prod, `warn` for prod) drives this and is forwarded into the migration call.
- **Why validators in addition to indexes?** Indexes enforce uniqueness; validators enforce shape. Both are useful at different layers. UNIQUE on `sku` prevents duplicate keys but does NOT prevent a malformed document with `sku` set to `null` or to a non-string. Validators close that gap.
- **Why not enforce strictly in production?** Because in a flexible-schema system, the cost of rejecting a write at the DB layer is high — it kills an in-flight admin import. Logging-and-alerting (warn mode) gives the team the same observability without the outage risk.

## Seed Data Policy

- **Seed ONLY top-level / root categories** needed for the storefront skeleton (for example, `Apparel`, `Electronics`, `Home`, `Groceries`).
- **NEVER seed product documents.** Products come from real merchants, CMS imports, or admin API calls — they are runtime data, not bootstrap data.
- **Idempotent insertion:** Each seeded category uses `find_one_and_update({"slug": <slug>}, {"$set": <doc>, "$setOnInsert": {"created_at": <utc>}}, upsert=True)`. Re-running the migration on an existing database is a no-op.
- **Stable ids:** Each seeded category gets a deterministic `_id` derived from its slug (a UUIDv5 in a fixed namespace, or the slug itself). This ensures references from later migrations and from the storefront's hard-coded landing pages remain stable across environments.
- **The seed data lives in [`./seed/root_categories.json`](./seed/root_categories.json)** as a declarative JSON document; `0002_seed_root_categories.py` reads and applies it. This separation lets non-engineers add a new top-level category by editing a single JSON file rather than authoring Python.

## Application Workflow

Per AAP R-9, migrations MUST be applied automatically. Two workflows are supported:

- **On service startup (preferred for local dev / single-replica):** the container entrypoint runs `python -m migrations.runner` BEFORE starting uvicorn. Failure is fatal — the container exits non-zero and the readiness probe stays `503` (per AAP R-19, fail fast on missing critical dependencies).
- **Dedicated migration Job / CI step (recommended for production):** a Kubernetes `Job` (or CI stage) runs `python -m migrations.runner` once per deployment; service pods only start after the Job completes successfully. Safer for multi-replica zero-downtime rollouts because all pods see a consistent post-migration state.
- **Concurrency:** The runner acquires a lightweight advisory lock by attempting to insert a sentinel `{_id: "_lock", acquired_at: <utc>}` into the `_migrations` collection. The `_id` field is the natural key — a duplicate insert raises `DuplicateKeyError`, signaling that another runner is in flight. The lock TTL is configurable via `MIGRATIONS_LOCK_TTL_SECONDS` (default `600`). On normal completion, the runner deletes the sentinel; on crash, the next runner re-acquires after the TTL via a CAS update keyed on `acquired_at < now() - ttl`. (MongoDB has no native advisory locks like PostgreSQL's `pg_advisory_lock`; the runner has to roll its own.)
- **Note on Dockerfile:** [`../Dockerfile`](../Dockerfile) already `COPY`s this folder into the runtime image at `/app/migrations/`. The entrypoint script invokes `python -m migrations.runner`. Set `RUN_MIGRATIONS_ON_STARTUP=true` (default) to enable startup migrations, or `false` if running them via a Job.

## Local Development Workflow

```bash
# From the service root: services/product-service/
# Ensure MONGODB_URL is set in your shell:
export MONGODB_URL="mongodb://product:product@localhost:27017/product_db?authSource=admin"
export MONGODB_DATABASE="product_db"

# Activate venv & install dependencies (once per machine)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Apply all pending migrations (idempotent — safe to re-run)
python -m migrations.runner

# Show currently-applied migrations (queries the _migrations collection)
python -m migrations.runner --status

# Apply a single migration by id (LOCAL DEV ONLY, useful for debugging)
python -m migrations.runner --target 0001_create_collections_and_indexes

# Verbose / debug logging
MIGRATIONS_LOG_LEVEL=DEBUG python -m migrations.runner

# Inspect _migrations collection in Mongo shell
mongosh "$MONGODB_URL" --eval 'db._migrations.find().sort({applied_at:1}).pretty()'
```

## Environment Variables

The scripts in this folder consume the following environment variables (all read by [`./env.py`](./env.py) and [`./runner.py`](./runner.py)). No defaults appear in source for credentials or hostnames; per AAP R-25, those values are supplied at runtime via shell, K8s Secret, or platform secret manager.

**REQUIRED:**

- `MONGODB_URL` — MongoDB connection URI (for example, `mongodb://user:pass@host:27017/db?authSource=admin` or `mongodb+srv://user:pass@cluster0.mongodb.net/db`). Per AAP R-25, supplied at runtime; never committed.
- `MONGODB_DATABASE` — Database name. Documented default in [`../.env.example`](../.env.example) is `product_db`.

**OPTIONAL (with sensible defaults):**

- `MONGODB_TLS_ENABLED` — `true` | `false`. Default `true` (TLS enabled).
- `MONGODB_TLS_CA_FILE` — Path to CA bundle (for example, `/etc/ssl/certs/ca-bundle.crt`). Default empty (uses the system CA store).
- `MONGODB_APP_NAME` — Identifies the connection in MongoDB server logs. Default `product-service-migrations` for the migration runner (distinct from the application's own `product-service` to make migration sessions easy to grep in server logs).
- `MONGODB_SERVER_SELECTION_TIMEOUT_MS` — Default `5000`.
- `MONGODB_CONNECT_TIMEOUT_MS` — Default `5000`.
- `MONGODB_SOCKET_TIMEOUT_MS` — Default `10000`.
- `MONGODB_RETRY_WRITES` — Default `true`.
- `MONGODB_RETRY_READS` — Default `true`.
- `MIGRATIONS_LOG_LEVEL` — Default `INFO`. Set to `DEBUG` for verbose runner output.
- `MIGRATIONS_CHECKSUM_MISMATCH_POLICY` — `error` | `warn` | `ignore`. Default `error`.
- `MIGRATIONS_VALIDATION_ACTION` — `error` | `warn`. Default `error` (non-prod), `warn` (prod). Drives the `$jsonSchema` `validationAction` setting registered with `collMod`.
- `MIGRATIONS_LOCK_TTL_SECONDS` — Default `600`. Controls advisory-lock auto-release for stale runners.
- `RUN_MIGRATIONS_ON_STARTUP` — `true` | `false`. Default `true`. Toggles startup-mode versus Job-mode application (see [Application Workflow](#application-workflow)).
- `CATALOG_TEXT_SEARCH_LANGUAGE` — Default `english`. Drives the language setting on the text index created by migration `0003`.

**NO credentials, hostnames, or environment-specific values appear anywhere in this folder** (per AAP R-25). All such values are read from environment variables at runtime by [`./env.py`](./env.py).

## Schema Isolation (AAP R-6)

This database (`product_db`) is **private** to the Product Service.

- **No other service** reads from or writes to `product_db` directly.
- Cross-service references in product documents (for example, `category_ids[]`) are stored as opaque ObjectId or UUID values **within this database only**; cross-service joins are forbidden.
- Cross-service consistency is achieved via Kafka events (`product.created`, `product.updated`) consumed by Recommendation Engine, Inventory Service, and Order Service — see [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md) for the full event topology.
- The `_migrations` collection is a **service-local bookkeeping concern**. It is never read by other services and never appears in any cross-service contract.

## Troubleshooting

- **`pymongo.errors.ServerSelectionTimeoutError`** → MongoDB is unreachable. Check `MONGODB_URL`, network policy, and TLS certificate validity. Confirm the server is alive with `mongosh "$MONGODB_URL" --eval 'db.runCommand({ping:1})'`.
- **`pymongo.errors.OperationFailure: Authentication failed`** → User / password mismatch or wrong `authSource`. Confirm via `mongosh "$MONGODB_URL" --eval 'db.runCommand({connectionStatus:1})'`. Most managed MongoDB providers require `authSource=admin` even when connecting to a non-admin database.
- **`pymongo.errors.OperationFailure: Index ... already exists with different options`** → Two migrations attempted to create the same-named index with different specifications. Either rename the new index OR drop the old one in a forward migration, then create with the new options.
- **`Migration <id> already applied with checksum <X>; current checksum <Y>`** → The migration file was edited after being applied. Either revert the edit (forward-only policy) OR set `MIGRATIONS_CHECKSUM_MISMATCH_POLICY=warn` for local dev only (NEVER in production).
- **`Migration runner refuses to start: lock held by <pid> at <timestamp>`** → Another runner is in flight, or a previous run crashed. If the `acquired_at` timestamp is older than `MIGRATIONS_LOCK_TTL_SECONDS`, the lock auto-releases on the next run. To force-release manually: `mongosh "$MONGODB_URL" --eval 'db._migrations.deleteOne({_id:"_lock"})'`.
- **`pymongo.errors.WriteError: Document failed validation`** → A migration tried to insert a document that violates the `$jsonSchema` validator. Either fix the migration's payload OR loosen the validator (in a separate forward migration that uses `validationAction="warn"` first, then tightens later once back-fill is complete).
- **`Multiple authentication mechanisms` warning** → MongoDB's connection string supports both `authSource` and SCRAM mechanism selectors; ensure `authSource=admin` (or your auth db) is set explicitly in `MONGODB_URL`.
- **Text-index creation slow** → Text indexes on large collections take time. The `0003` migration is intentionally separate so this cost is paid in isolation; it is safe to run on an empty `products` collection in seconds.

## Testing

The integration test (created by sibling agents under [`../tests/integration/`](../tests/integration/)) exercises the full migration runner against a fresh MongoDB instance.

- Uses **Testcontainers** to spin up a fresh `mongo:7` container per test run.
- Sets `MONGODB_URL` to the container's connection string and `MONGODB_DATABASE` to a unique random name.
- Runs `python -m migrations.runner` and asserts:
  - The three domain collections exist (`db.list_collection_names()` returns `products`, `categories`, `product_media`, plus the bookkeeping `_migrations`).
  - All required indexes exist with their expected names and options (verified via `db.<col>.list_indexes()`):
    - `idx_products__sku` UNIQUE
    - `idx_products__slug` UNIQUE
    - `idx_products__categories` multikey
    - `idx_products__status_created` compound `(status: 1, created_at: -1)`
    - `idx_products__updated` `(updated_at: -1)`
    - `idx_products__text_search` text on `name + description` with the configured language
    - `idx_categories__slug` UNIQUE
    - `idx_categories__parent`
    - `idx_categories__path` multikey
    - `idx_categories__depth_order` compound `(depth: 1, display_order: 1)`
    - `idx_product_media__product_sort` compound `(product_id: 1, sort_order: 1)`
  - The `$jsonSchema` validators are registered (verified via `db.runCommand({listCollections: 1, filter: {name: "products"}})` returning a non-empty `validator` field on each domain collection).
  - The root-categories seed inserted exactly the documents declared in [`./seed/root_categories.json`](./seed/root_categories.json).
  - The `_migrations` collection contains one entry per applied migration (`0001`, `0002`, `0003`) with non-null `applied_at` and a 64-character `checksum`.
- Re-runs the runner to verify no-op idempotence (runner reports `0 migrations to apply`).
- Verifies UNIQUE-constraint enforcement: attempts to insert two products with the same `sku`; asserts the second insert raises `DuplicateKeyError`.
- Verifies validator enforcement: attempts to insert a product with `sku=null`; asserts the insert raises `WriteError` (in `error` mode) or logs a warning (in `warn` mode).
- Verifies seed idempotence: re-runs `0002_seed_root_categories.py` directly via `python -m migrations.runner --target 0002_seed_root_categories`; asserts no duplicate category rows.

## Cross-Coupling Awareness

The contents of this folder are tightly coupled with the application source under `../src/`. Coordinated changes are required when any of the following dimensions evolve.

- **Index names** are referenced by application code — for example, when forcing an index hint in a query plan or when reporting unused-index warnings to ops. Agents adding indexes MUST update both the migration AND the corresponding code or documentation under `../src/repository/`.
- **`$jsonSchema` field requirements** are coupled with the Pydantic models in `../src/domain/models.py` (or equivalent). Adding a required field in a Pydantic model without a corresponding migration to add the field to the validator (in `warn` mode initially, then `error` once back-fill is complete) breaks both forward AND backward compatibility.
- **Seed slugs** in [`./seed/root_categories.json`](./seed/root_categories.json) are referenced by storefront URL routes (for example, `/category/apparel`); changing a slug requires a coordinated update in the storefront. NEVER rename a seeded slug — instead, add a new category and let old URLs redirect at the gateway layer.
- **Status enum values** in product and category documents are stored as strings in MongoDB. The validators constrain them via `enum` keywords; the application's `StrEnum` classes must match. Coordinated changes only — adding a new status value requires both a validator update (forward migration) and a code update.

## Related Files & References

- [../README.md](../README.md) — Service-level Product Service README
- [../requirements.txt](../requirements.txt) — Python dependencies (pins `pymongo` and `motor`)
- [../.env.example](../.env.example) — Documented MongoDB env vars
- [../Dockerfile](../Dockerfile) — Mounts this folder into the runtime image
- [../config/default.yaml](../config/default.yaml) — Application configuration including `text_search_language`
- [../../inventory-service/migrations/README.md](../../inventory-service/migrations/README.md) — Sibling migrations README (PostgreSQL / Alembic for contrast)
- [../../order-service/migrations/README.md](../../order-service/migrations/README.md) — Sibling migrations README (PostgreSQL / Alembic for contrast)
- [../../../docs/architecture/data-stores.md](../../../docs/architecture/data-stores.md) — Polyglot persistence rationale
- [../../../docs/architecture/system-diagram.md](../../../docs/architecture/system-diagram.md) — Canonical system architecture diagram
- [../../../docs/runbook/product-service.md](../../../docs/runbook/product-service.md) — Operational runbook (if present)

AAP citations (non-clickable):

- AAP Section 0.4.4 — `product_db` (MongoDB) collections `products`, `categories`, `product_media`
- AAP Section 0.5.2.5 — Per-service `migrations/` folder requirement
- AAP R-6 — Database per service strict isolation
- AAP R-7 — Polyglot persistence (MongoDB chosen for catalog flexibility)
- AAP R-9 — Migrations under owning service, applied automatically
- AAP R-19 — Fail fast on missing critical dependencies
- AAP R-25 — Secrets never in source / config
