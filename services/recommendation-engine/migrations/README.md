# Recommendation Engine — Database Migrations

Forward-only, versioned SQL DDL scripts that create and evolve the Recommendation Engine's private PostgreSQL (with pgvector) schema.

## Purpose

This folder contains versioned SQL DDL migration scripts for the **private PostgreSQL database** (with the `pgvector` extension) owned by the Recommendation Engine service.

- Per **AAP R-6** (database per service), no other service reads from or writes to this database. The only way other services observe data produced here is via Kafka events emitted by the owning service (see [`../../../docs/architecture/event-catalog.md`](../../../docs/architecture/event-catalog.md) for the topic catalog).
- Per **AAP R-9**, migrations **must** be applied automatically — either on service startup or via a dedicated migration Kubernetes Job / CI step. Pods must not begin serving traffic until all pending migrations have completed successfully.

## File Layout

```text
migrations/
├── README.md                                   # This file
├── V001__enable_pgvector_extension.sql         # CREATE EXTENSION vector
├── V002__create_embeddings_table.sql           # embeddings (vector(128))
├── V003__create_interaction_features_table.sql # interaction_features
├── V004__create_rec_cache_table.sql            # rec_cache
├── V005__create_hnsw_index_on_embeddings.sql   # HNSW index (cosine)
└── V006__create_popularity_refresh_helpers.sql # popularity helpers
```

- **V001** — Enables the `vector` extension; must run first because subsequent migrations declare `vector(N)` columns and vector indexes.
- **V002** — Creates the `embeddings` table with an `embedding vector(128) NOT NULL` column keyed by a `product_id` UUID.
- **V003** — Creates the `interaction_features` table that stores per-`(user_id, product_id)` engagement signals consumed by the inference pipeline.
- **V004** — Creates the `rec_cache` table that backs the durable fallback cache used when the Redis cache is cold or unavailable.
- **V005** — Builds the HNSW index `USING hnsw (embedding vector_cosine_ops)` on `embeddings` to enable approximate nearest-neighbor lookups.
- **V006** — Creates SQL functions / views that support periodic refresh of the popularity-based fallback recommendations (AAP R-20 fallback path).

## Naming Convention

- Format: `V<3-digit number>__<snake_case_description>.sql` (note the **double underscore** between version and description).
- The convention is **Flyway-style** and sorts lexicographically; it is also fully compatible with Liquibase (in `sql` format) and with simple custom runners that invoke `psql -f` in sorted order.
- **Never reuse** a version number across two migrations.
- **Never edit** a migration that has already been applied in any environment — create a new forward-only migration instead (see [Rollback Policy](#rollback-policy)).

## Tool Selection

The AAP (R-9) explicitly defers the migration-tool choice to build time. Acceptable choices include:

1. **Flyway** — Open-source, JVM-based; ships as a standalone CLI and an official Docker image. Tracks state in a `flyway_schema_history` table. The `V###__name.sql` convention used in this folder is native to Flyway.
2. **Liquibase** — Open-source; supports XML, YAML, JSON, and SQL changesets. Configured for the `sql` format, Liquibase reads the same `V###__name.sql` files.
3. **Custom runner / `psql`** — A shell script that runs `ls migrations/V*.sql | sort | xargs -I {} psql -f {}` in sorted order, executed by an entrypoint, an initContainer, or a one-off Kubernetes Job. Simple, portable, and language-agnostic; tracks its own state in a `schema_migrations` table.

A minimal custom tracking table for option 3:

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     VARCHAR(32) PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum    VARCHAR(128)
);
```

## Application Workflow

Both AAP R-9-compliant workflows are supported. Pick one per environment and stick to it.

- **On service startup.** An entrypoint script runs all pending migrations **before** the FastAPI / uvicorn process accepts traffic. A failure is fatal: the container exits non-zero and the readiness probe reports `503` (AAP R-19). Suitable for local development and small single-replica environments.
- **Dedicated migration Job / CI step.** A separate Kubernetes `Job` (or a CI stage) runs the migrations exactly once per deployment; service pods only start once the Job has succeeded. This is the **recommended production pattern** for zero-downtime rollouts because it eliminates the race between concurrent replicas all attempting to migrate the same database at startup.

## Rollback Policy

Migrations in this folder are **forward-only**.

- Do **not** edit a migration that has been applied in any environment.
- To undo a change, **create a new migration** (for example, `V007__undo_hnsw_index.sql`) that contains the corrective DDL.
- For catastrophic recovery, **restore from a database snapshot** rather than running a reverse migration against live data.

The rationale is simple: edit-in-place migrations cause diverged database states across environments and are a leading cause of production incidents. A forward-only ledger keeps every environment reproducible from `V001` onward.

## Idempotence

Every DDL statement in this folder uses `IF NOT EXISTS` / `OR REPLACE` semantics so reruns are safe.

- `CREATE EXTENSION IF NOT EXISTS vector` (V001)
- `CREATE TABLE IF NOT EXISTS <name> (...)` (V002, V003, V004)
- `CREATE INDEX IF NOT EXISTS <name> ON <table> (...)` (V002, V003, V004, V005)
- `CREATE OR REPLACE FUNCTION` / `CREATE OR REPLACE VIEW` where applicable (V006)

Idempotence allows the same migration script to be re-applied without error, which is essential for both Flyway-style replay during recovery and for hand-runs against environments where the migration history table has been lost or rebuilt.

## Baseline for Existing Databases

If applying these migrations to a non-empty database that predates this ledger:

- **Flyway:** run `flyway baseline -baselineVersion=0` so that `V001` is treated as the next pending migration.
- **Custom runner:** insert a sentinel row before running the loop:

  ```sql
  INSERT INTO schema_migrations (version) VALUES ('000') ON CONFLICT DO NOTHING;
  ```

- **Greenfield deployments need no baseline** — just apply `V001` onward.

## Schema Isolation (AAP R-6)

This database is **private** to the Recommendation Engine.

- **No foreign keys** from these tables point at tables in any other service's database (`product_db`, `user_db`, `order_db`, `auth_db`, `inventory_db`, `payment_db`, `notification_db`).
- **No JOIN-ability** is assumed across services — cross-service fields (a product name, a user preference, an order outcome) arrive via Kafka events or are fetched on demand via REST from the owning service.
- `user_id` and `product_id` are stored as **UUIDs** with no constraint back to `user_db.users.id` or `product_db.products.id`. This is intentional and non-negotiable.

## Dimension Coupling (CRITICAL)

The embedding vector dimension **128** is a coupling constant that appears in five places. **All five must agree** at all times — a mismatch causes silent inference failures or runtime errors when the model emits a vector that the database column rejects:

1. [`V002__create_embeddings_table.sql`](./V002__create_embeddings_table.sql) → `embedding vector(128) NOT NULL`
2. [`../config/default.yaml`](../config/default.yaml) → `database.vector.embedding_dim: 128`
3. [`../config/default.yaml`](../config/default.yaml) → `model.embedding_dim: 128`
4. [`../.env.example`](../.env.example) → `MODEL_EMBEDDING_DIM=128`
5. [`../models/README.md`](../models/README.md) → metadata example `"embedding_dim": 128`

Changing the dimension requires a coordinated update to **all five** sites, plus a model retraining pass and a full HNSW index rebuild against re-encoded embeddings. Plan it as a dedicated migration — never as a hot-fix.

## pgvector Version Compatibility

- `V005` uses `USING hnsw (embedding vector_cosine_ops)`, which requires **pgvector ≥ 0.5.0** (released in 2023).
- If operating against an older pgvector build, replace V005's HNSW DDL with the IVFFLAT fallback: `USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)`. The query path does not change; only recall and build-time characteristics differ.
- **Recommended Docker image:** `pgvector/pgvector:pg16`, which ships a current pgvector build pre-installed with PostgreSQL 16.

## Encryption at Rest

Per **AAP R-8**, only `payment_db` mandates **column-level** encryption. The Recommendation Engine database holds no financial data and no PII beyond `user_id` (a UUID), so:

- Column-level encryption is **not required** for this database.
- **Disk-level / tablespace-level** encryption at the PostgreSQL cluster level **is** expected; this is an operator concern managed by the platform team (cloud-provider KMS, LUKS, or equivalent) and is out of scope for these migration scripts.

## Testing

The integration test [`../tests/integration/test_migrations.py`](../tests/integration/test_migrations.py) exercises this ledger end-to-end:

- Spins up a fresh `pgvector/pgvector:pg16` container via Testcontainers.
- Applies every migration in lexical order (equivalent to `ls V*.sql | sort | xargs psql -f`).
- Asserts each table exists with the expected columns and types.
- Asserts the `embedding` column is of type `vector` and has the dimension declared in [`../config/default.yaml`](../config/default.yaml).
- Asserts the HNSW index exists on `embeddings` and uses `vector_cosine_ops`.
- Re-applies the migrations and asserts the second pass is a no-op (idempotence guarantee).

A local one-liner developers can run for a quick smoke test (uses an angle-bracket placeholder — substitute any local-only password):

```bash
docker run --rm -d --name pgv -e POSTGRES_PASSWORD=<local-pwd> -p 5433:5432 pgvector/pgvector:pg16
sleep 3
for f in V*.sql; do PGPASSWORD=<local-pwd> psql -h localhost -p 5433 -U postgres -d postgres -f "$f"; done
```

## Redis Note

There are **no migration files for Redis**. Redis is schemaless; its "schema" is the set of documented key patterns used by the service code (see [`../config/README.md`](../config/README.md) under the `cache.namespaces` section, and the runtime adapter at `../src/repository/redis_adapter.py`). Adding a new Redis key namespace is a code change, not a database migration.

## Related Files and References

- [`../README.md`](../README.md) — Service-level README
- [`../config/default.yaml`](../config/default.yaml) — `database.vector.embedding_dim: 128`, `model.embedding_dim: 128`
- [`../.env.example`](../.env.example) — `MODEL_EMBEDDING_DIM=128`
- [`../models/README.md`](../models/README.md) — Model artifact metadata `embedding_dim`
- [`../../../docs/architecture/data-stores.md`](../../../docs/architecture/data-stores.md) — Database-per-service rationale and polyglot persistence
- [`../../../docs/architecture/system-diagram.md`](../../../docs/architecture/system-diagram.md) — Canonical system architecture diagram

AAP rule citations:

- **AAP R-6** — Database per service, strict isolation (no cross-service FKs).
- **AAP R-7** — Polyglot persistence; vector store + Redis for the Recommendation Engine.
- **AAP R-8** — Only `payment_db` mandates column-level encryption; this database is out of scope for column-level encryption.
- **AAP R-9** — Migrations live under the owning service and are applied automatically on startup or via a dedicated job.
