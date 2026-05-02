# User Service

User profile, preference, and address book authority — PostgreSQL-backed CRUD service with Apache Kafka event consumption (`user.registered`) and emission (`user.updated`, `user.deleted`).

> **📐 Canonical architecture diagram:** [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md)
>
> This service appears in the `CommerceCore > UserBox` subgraph as `UserSvc`, wired to `UserDB` (PostgreSQL — includes `users`, `user_profiles`, `user_preferences`, `user_addresses` tables). It consumes `user.registered` events from Kafka (emitted by Auth Service to initialize a profile on registration) and produces `user.updated` / `user.deleted` events for downstream consumers (Notification Service, Recommendation Engine).

---

## Overview

The User Service is the platform's authority for **user profile, preference, and address-book data**. It bridges the identity events emitted by the Auth Service (which owns credentials, OAuth flows, and JWT issuance) into the rich, mutable user records that downstream services rely on. Its core responsibilities are:

- **User CRUD.** Create (via `user.registered` consumption), read, update, and soft-delete user records. Every mutation is durable in PostgreSQL and emits a corresponding `user.*` event for downstream consumers.
- **Profile management.** Stores name, email, phone, avatar URL, and demographic attributes (`gender`, `date_of_birth`, `locale`, `timezone`).
- **Preference management.** Stores notification channel preferences (email / SMS opt-ins per AAP R-11), preferred language, currency, regional settings, marketing consent, and per-channel quiet-hours overrides. The Notification Service reads these preferences (over Kafka, never via direct DB access) to make per-event channel-routing decisions.
- **Address book.** One-to-many user addresses (billing, shipping, other) with default flags and country-aware validation. Order Service references address ids snapshotted into orders at checkout time, so historical orders survive subsequent address mutations.
- **Event consumer.** Consumes `user.registered` from Kafka (emitted by Auth Service) to **initialize** a profile + preferences row on first registration. This is the canonical bridge between identity and user-data domains.
- **Event producer.** Emits `user.updated` (any profile / preference / address change) and `user.deleted` (soft-delete) events for downstream consumers (AAP R-30, R-32). Producers remain agnostic of consumers.
- **Read API.** Fronted by API Gateway at `/users/*` for profile lookup, preference retrieval, and address book queries (see [HTTP API](#http-api)).

This service is **both an event consumer AND an event producer**, which distinguishes it architecturally from purely-producing services (e.g., Product Service) and purely-consuming services (e.g., Notification Service).

---

## Architecture Context

| Aspect | Value |
|--------|-------|
| Domain | Commerce Core |
| Role | Profile / preference / address authority — bridges Auth Service registration events into rich user-data emissions for downstream consumers |
| Fronted by | API Gateway at `/users/*` |
| Inbound sync | `GET /users/me`, `GET /users/{id}`, `PUT /users/me`, `PATCH /users/me/preferences`, `GET/POST/PUT/DELETE /users/me/addresses`, plus admin endpoints for support tooling |
| Inbound async | `user.registered` (from Auth Service) — initializes the user's profile/preferences/address records on first sign-up |
| Outbound sync | NONE in the steady state (this service does not call other services synchronously) |
| Outbound async | `user.updated` (any profile / preference / address change), `user.deleted` (soft-delete) |
| Primary store | PostgreSQL `user_db` — includes `users`, `user_profiles`, `user_preferences`, `user_addresses` (AAP R-6, R-7) |
| DLQs | `user.updated.dlq`, `user.deleted.dlq` (per-event DLQs); `user.registered.dlq` (poison-message destination on inbound consume failure) |
| Design pattern | Aggregate-per-user — `User` root aggregate owns Profile + Preferences + Addresses, persisted in normalized relational tables for transactional consistency |

---

## Data Stores

This service owns a single private database — `user_db` — per the database-per-service principle (AAP R-6). All four tables are owned exclusively by this service; **no other service reads from or writes to them**. Cross-service access flows over this service's HTTP API or via the `user.*` events on Kafka.

The schema comprises four tables (per AAP Section 0.4.4):

- **`users`** — root identity record; one row per user.
  Columns: `id` (UUID, PK), `external_auth_id` (stable id from Auth Service, UNIQUE — the upsert key for `user.registered`), `email` (UNIQUE), `status`, `created_at`, `updated_at`, `deleted_at` (`NULL` = active), `version` (for optimistic concurrency).
- **`user_profiles`** — extended profile attributes; one row per user.
  Columns: `user_id` (FK + UNIQUE), `first_name`, `last_name`, `display_name`, `phone`, `avatar_url`, `date_of_birth`, `gender`, `locale`, `timezone`.
- **`user_preferences`** — notification preferences and regional settings; one row per user.
  Columns: `user_id` (FK + UNIQUE), `email_opt_in`, `sms_opt_in`, `marketing_opt_in`, `preferred_language`, `preferred_currency`, `quiet_hours_start`, `quiet_hours_end`, `channel_overrides` (JSONB).
- **`user_addresses`** — one-to-many address book.
  Columns: `id` (PK), `user_id` (FK), `type` (`billing` / `shipping` / `other`), `is_default`, `line1`, `line2`, `city`, `state`, `postal_code`, `country` (ISO-3166), `phone`, `is_verified`, `created_at`, `updated_at`.

### Index hints (defined in `migrations/`)

- `users.email` — UNIQUE.
- `users.external_auth_id` — UNIQUE (idempotent upsert key for the `user.registered` consumer; see [Concurrency & Consistency](#concurrency--consistency)).
- `users.deleted_at` — partial index `WHERE deleted_at IS NULL` for active-user queries.
- `user_profiles.user_id` — UNIQUE (1:1 with `users`).
- `user_preferences.user_id` — UNIQUE (1:1 with `users`).
- `user_addresses.user_id` — non-unique (multi-row per user).
- `(user_addresses.user_id, user_addresses.is_default)` — partial UNIQUE per `type` so a user has at most one default per address type.

### Why PostgreSQL?

User data has a **well-defined relational structure** (one user → one profile → one preferences row → many addresses). Referential integrity matters (no orphan profiles, no addresses without owners), and **ACID semantics simplify the upsert flow** on `user.registered` events: the four inserts (users + profile + preferences + optional initial address) succeed or fail as a single transaction, making the consumer trivially idempotent on retry. This is the canonical fit for AAP R-7 (choose the most appropriate persistence per service).

### Ownership boundary (AAP R-6)

All four tables are owned exclusively by this service. **Other services receive user data exclusively via REST API calls or via `user.*` Kafka events** — never by direct database access. This is non-negotiable per AAP R-6 and is enforced at the network layer in production deployments.

DDL for all four tables and the migration runner contract live under `migrations/`. Migrations are applied automatically at startup or via a dedicated migration job per AAP R-9.

---

## Events Consumed

The User Service subscribes to a single inbound topic. All consumed events are validated against the schema in the Schema Registry (AAP R-14); validation failures route to `user.registered.dlq`.

| Event | Behavior |
|-------|----------|
| `user.registered` (from Auth Service) | **Idempotent upsert.** INSERT a new `users` row keyed by `external_auth_id` from the event; INSERT default `user_profiles` (display name from event metadata, locale = `en-US` if absent), `user_preferences` (sensible defaults: `email_opt_in=true`, `sms_opt_in=false`, `marketing_opt_in=false`), and (if present in the event) initial `user_addresses` rows. After the durable Postgres write commits, emit `user.updated` so downstream consumers (Notification, Recommendation) see the freshly minted user. |

### Idempotency rule

Duplicate `user.registered` events (at-least-once Kafka delivery) are **safe**. The upsert keys on the UNIQUE `external_auth_id` constraint and is a NOOP on duplicate. **The consumer commits Kafka offsets ONLY after the durable Postgres write succeeds**, so a crash between message receipt and DB commit results in safe replay rather than data loss.

### Schema validation

The consumer validates inbound payloads against the registered schema for `user.registered` in `infrastructure/kafka/schemas/` (AAP R-14). Payloads that fail schema validation route to `user.registered.dlq` for ops triage; they are never silently dropped.

---

## Events Produced

This service produces exactly two outbound events. The producer remains **agnostic of consumers** per AAP R-32 — adding a new consumer requires zero changes to this service.

| Event | Schema (Schema Registry) | Downstream Consumers (informational) |
|-------|--------------------------|--------------------------------------|
| `user.updated` | `user.updated.v1` (JSON Schema or Avro) | Notification Service (re-evaluate channel preferences for in-flight events), Recommendation Engine (refresh per-user features such as locale + preferences) |
| `user.deleted` | `user.deleted.v1` (JSON Schema or Avro) | Notification Service (suppress further sends), Recommendation Engine (purge per-user features per data-retention policy) |

### Event payload shape (informational)

Every event carries: `event_id`, `event_version`, `correlation_id` (per AAP R-13), `occurred_at` (RFC 3339), `producer` (always `user-service`), and a `user` envelope with `id`, `external_auth_id`, `email`, `status`. The `user.updated` payload additionally carries a `changes` summary that denormalizes the post-mutation state of the affected sections (profile / preferences / addresses) so consumers do **not** need to call back to this service to interpret the event (AAP R-33 — events are self-contained). Soft-delete events include the deletion timestamp and the operator/cause when known.

### Schema evolution

Schemas are registered in `infrastructure/kafka/schemas/` and evolve via Schema Registry compatibility rules (AAP R-14). Backward-compatible additions are always permitted; field renames or removals require a major version bump and a coordinated rollout.

### Partition key

The Kafka partition key for **all** `user.*` events is `user_id`. This guarantees per-user strict ordering for consumers — every event for a given user lands on the same partition, so a consumer that processes partitions in order will see `user.registered` before any subsequent `user.updated`, and `user.updated` events in the order this service emitted them.

---

## HTTP API

### Public endpoints (fronted by API Gateway at `/users/*`)

Most endpoints require a valid JWT issued by the Auth Service (AAP R-21). Admin endpoints additionally require the `users:admin` scope.

- `GET /users/me` — fetch the authenticated user's full profile, preferences, and addresses.
- `GET /users/{id}` — admin-only single-user lookup (requires `users:admin` scope).
- `PUT /users/me` — update profile fields (`first_name`, `last_name`, `display_name`, `phone`, `avatar_url`, `date_of_birth`, `gender`, `locale`, `timezone`); emits `user.updated`.
- `PATCH /users/me/preferences` — partial update of channel preferences and regional settings; emits `user.updated`.
- `GET /users/me/addresses` — list the user's addresses.
- `POST /users/me/addresses` — add a new address (validated against country-specific format rules); emits `user.updated`.
- `PUT /users/me/addresses/{addressId}` — update an existing address; emits `user.updated`.
- `DELETE /users/me/addresses/{addressId}` — remove an address; emits `user.updated`.
- `DELETE /users/me` — soft-delete the user (sets `deleted_at`, anonymizes PII fields, emits `user.deleted`). See [Security](#security) for the anonymization contract.

### Admin endpoints (require `users:admin` scope)

- `GET /users` — paginated user search by email / external_auth_id / status (support tooling).
- `POST /users/{id}/force-delete` — operator-driven soft-delete; emits `user.deleted` with operator metadata.
- `POST /users/{id}/reissue-welcome` — re-emit a synthetic `user.registered`-like event to drive Notification Service welcome paths during recovery scenarios.

### Liveness / readiness probes (AAP R-19)

- `GET /health/live` — process-alive check.
- `GET /health/ready` — PostgreSQL connection pool ready and Kafka producer + consumer reachable.

The full OpenAPI specification lives at [../../docs/api/user-service.md](../../docs/api/user-service.md).

---

## Domain Model

The aggregate shape:

```
User (root aggregate)
  ├── Profile        (1:1; persisted as separate row in user_profiles for normalization)
  ├── Preferences    (1:1; persisted as separate row in user_preferences)
  └── Addresses[]    (1:N; rows in user_addresses)
```

All User mutations go through a domain command — `InitializeUser` (driven by `user.registered`), `UpdateProfile`, `UpdatePreferences`, `AddAddress`, `UpdateAddress`, `RemoveAddress`, `SoftDeleteUser`. Each command produces a **write-then-publish sequence**: durable PostgreSQL transaction first → emit corresponding `user.updated` or `user.deleted` event with the resulting aggregate snapshot. The two halves are bridged by an outbox table (see [Concurrency & Consistency](#concurrency--consistency)) so the publish step is atomic with the data change.

The `users.version` column enables optimistic concurrency for in-flight admin overrides; clients (and admin tooling) supply an `If-Match` header carrying the expected `version`. UPDATE statements are conditioned on the version, and a mismatch yields `409 Conflict` rather than silently overwriting concurrent edits.

---

## Concurrency & Consistency

- **Optimistic concurrency.** Every `users` row carries a `version` field; UPDATE statements are conditioned on `version` and increment it atomically (`UPDATE users SET ..., version = version + 1 WHERE id = $1 AND version = $2`). Concurrent admin and user-driven updates collide deterministically, surfacing as `HTTP 409 Conflict` to the slower writer.
- **Idempotent ingestion.** The `user.registered` consumer commits Kafka offsets ONLY after the durable PostgreSQL write succeeds. Duplicate inbound events are safe via the `external_auth_id` UNIQUE constraint — duplicates become no-ops at the SQL layer.
- **Outbox pattern (recommended).** `user.updated` / `user.deleted` events are written to a small `events_outbox` table inside the **same transaction** as the data change; a dispatcher process publishes outbox rows to Kafka and marks them `published_at`. Without this, a successful PostgreSQL write followed by a Kafka producer failure would create event drift between the durable user record and the event stream — a class of bug that is notoriously hard to detect and reconcile after the fact. The outbox pattern is the canonical solution to this "double-write" problem and is the recommended implementation choice.
- **Per-user ordering.** The Kafka partition key for `user.*` events is `user_id`, so all events for a given user land on the same partition and consumers observe strict ordering.

---

## Retry & Fallback Policy

Per AAP R-15, R-16, R-17, and R-20.

- **Outbound HTTP retries (AAP R-15).** This service makes **minimal** outbound HTTP — the only synchronous remote call in the steady state is the JWKS fetch from the Auth Service for JWT validation. JWKS fetches use exponential backoff with jitter, max 3 attempts; a JWKS cache with bounded TTL absorbs transient outages (AAP R-22), and key rotation does not require a service restart.
- **PostgreSQL write retries.** The DB driver retries on transient connection errors (network blips, failover to standby) with exponential backoff, max 3 attempts; the retry loop aborts immediately on duplicate-key errors (which indicate logical retry safety, not a transient failure) per AAP R-15.
- **Kafka producer retries.** Configured with `acks=all` and `enable.idempotence=true`. Internal producer retries are bounded by a 30-second total budget; on permanent failure the corresponding outbox row remains `published_at IS NULL` and a background reconciler re-attempts publication, so the durable user record is never out of sync with the event stream for more than the reconciler interval.
- **Kafka consumer retry topics (AAP R-17).** Processing failures on `user.registered` route to `user.registered.retry` (with exponential backoff between retry attempts); after exhausting retries, messages route to `user.registered.dlq` for ops triage.
- **DLQ topics.**
  - `user.registered.dlq` — inbound events that could not be processed (schema validation failure, persistent DB error after retry exhaustion).
  - `user.updated.dlq` — outbound events that could not be produced after exhausting producer retries (extremely rare given the outbox pattern; the outbox dispatcher writes here only as a last resort after the reconciler also fails).
  - `user.deleted.dlq` — same contract as `user.updated.dlq`, scoped to delete events.
- **Circuit breaker (AAP R-16).** Wraps the JWKS HTTP client. Opens at ≥50% failure rate over a 20-call window; open-state duration 30 s; half-open probe is 1 call before re-closing. **On open-circuit, JWT validation requests fail-closed (HTTP 503) per AAP R-20** — fail-closed behavior is mandatory for security-critical paths, even at the cost of brief read-side unavailability during JWKS provider outages.
- **Graceful degradation (AAP R-20).**
  - **Read endpoints** (`GET /users/me`, `GET /users/me/addresses`) remain available even when Kafka is unreachable — Kafka outages do not affect read traffic.
  - **Write endpoints** depend on the outbox pattern: writes succeed and are durable, with event publication eventually reconciled. Clients see successful HTTP responses even when the Kafka producer is degraded.

Retry and timeout thresholds — along with per-call-site overrides — are documented in [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md).

---

## Running Locally

```bash
# From the service directory (services/user-service/)
cp .env.example .env
# Fill in or accept defaults for: POSTGRES_URL (user_db), KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL,
#   AUTH_SERVICE_URL, JWT_PUBLIC_KEY_URL, JWT_ISSUER

# Install dependencies (Python 3.11+)
python -m venv .venv
source .venv/bin/activate   # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Apply database migrations (creates users, user_profiles, user_preferences, user_addresses)
# NOTE: `migrations/` contains the DDL; apply with alembic, psql, or the container's entrypoint.

# Run the service
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

The repository-root `docker-compose.yml` spins up this service alongside PostgreSQL `user-db`, Kafka, the Schema Registry, sibling services (Auth, Notification, Recommendation), and every other dependency in a single command for full-stack local development.

---

## Testing

Per AAP Section 0.5.2.6:

- **Unit tests** (`tests/unit/`) — domain validators (email format, country code, BCP-47 locale, postal-code-per-country, phone format), preference logic (default channel selection per AAP R-11), version-conflict handling, and the idempotent-upsert behavior of the `user.registered` consumer.
- **Integration tests** (`tests/integration/`) — full pipeline with Testcontainers (PostgreSQL + Kafka): consume `user.registered` → INSERT users / profile / preferences → emit `user.updated`; CRUD round-trip via HTTP and verification of outbound events on the topic.

Repository-level cross-service tests live under [../../tests/contract/](../../tests/contract/) (consumer-driven contract tests for `user.*` schema compatibility).

```bash
pytest tests/unit -v
pytest tests/integration -v --maxfail=1
```

---

## Configuration

The authoritative configuration files for this service are `config/default.yaml` (non-secret defaults) and `.env.example` (the full environment-variable template, names only).

Required environment variable **names** (values supplied by Kubernetes Secrets / Vault / cloud secret manager — never by source or by `.env.example`, per AAP R-25):

- `POSTGRES_URL` — connection string for `user_db`.
- `KAFKA_BOOTSTRAP` — Kafka bootstrap servers.
- `SCHEMA_REGISTRY_URL` — Confluent / Apicurio schema registry URL.
- `AUTH_SERVICE_URL` — base URL for the Auth Service (used for non-JWKS administrative round-trips during recovery scenarios).
- `JWT_PUBLIC_KEY_URL` — JWKS endpoint exposed by the Auth Service (AAP R-22).
- `JWT_ISSUER` — expected `iss` claim on inbound JWTs.
- `LOG_LEVEL` — structured-logging verbosity (`DEBUG` | `INFO` | `WARN` | `ERROR`).

---

## Security

- **JWT validation (AAP R-21, R-22).** All customer endpoints require a JWT with the appropriate scope; admin endpoints additionally require the `users:admin` scope. **JWTs are issued exclusively by the Auth Service (AAP R-21)** — this service never mints tokens. JWKS is fetched from `JWT_PUBLIC_KEY_URL` and cached with a bounded TTL; key rotation does NOT require a service restart (AAP R-22).
- **PII handling.** This service stores high-sensitivity PII (email, phone, addresses, `date_of_birth`). On `DELETE /users/me`, soft-delete **anonymizes PII fields** by replacing them with hash-stable placeholders (e.g., `email = "deleted+<hash(id)>@anonymized.local"`, `phone = NULL`, `first_name = "[deleted]"`) while preserving the row id and `external_auth_id` so historical references from Order Service / Payment Service remain valid. The `deleted_at` column is set, the row is excluded from active-user queries by the partial index on `users.deleted_at`, and a `user.deleted` event is emitted so downstream consumers (Notification, Recommendation) can also purge their per-user features per data-retention policy.
- **TLS everywhere (AAP R-24).** All inter-service traffic is TLS-encrypted; the PostgreSQL connection uses `sslmode=verify-full` in production. Plaintext HTTP is permitted only on the loopback interface for sidecar communication inside a single pod.
- **Secrets never in source (AAP R-25).** PostgreSQL credentials, Kafka credentials, and any third-party tokens come exclusively from Kubernetes Secrets / HashiCorp Vault / cloud secret managers — never from source files or from `.env.example`. `.env.example` carries variable names only, never values.
- **Input validation.** All admin and customer write payloads are validated by Pydantic models with strict types; unknown fields are rejected to prevent attribute injection. Country / postal-code / phone validation prevents malformed addresses from corrupting downstream order flows.
- **Rate limiting.** Per-route and per-user rate limits are enforced upstream at the API Gateway (per AAP R-1's edge tier responsibility) to prevent profile-update abuse and credential-stuffing-adjacent enumeration via `GET /users/{id}`.

---

## Observability

Logs are structured JSON emitted to stdout, then shipped by Filebeat to Logstash, indexed in Elasticsearch, and visualized in Kibana (AAP R-26, R-27).

Required log fields on every line (AAP R-26): `timestamp` (RFC 3339), `level`, `service` (always `user-service`), `correlation_id` (per AAP R-13), `user_id` (when known), `route`, `method`, `status`, `latency_ms`, `event_type` (when consuming or producing), `message`.

Key metrics exposed at `/metrics` in Prometheus exposition format (scraped by Metricbeat per AAP R-27):

- `http_requests_total{route,method,status}` — counter.
- `http_request_latency_ms{route,method}` — histogram.
- `users_registered_total` — counter; increments on every successful `user.registered` upsert.
- `users_updated_total{change_kind}` — counter; `change_kind` ∈ `profile` | `preferences` | `address_added` | `address_updated` | `address_removed`.
- `users_deleted_total` — counter.
- `kafka_consumer_lag{topic}` — gauge.
- `outbox_pending{event_type}` — gauge; **alertable** — sustained nonzero indicates the Kafka producer or its reconciler is stuck.
- `dlq_depth{topic}` — gauge; **alertable**.
- `jwks_fetch_total{status}` — counter (success / cache_hit / failure).
- `optimistic_lock_conflicts_total` — counter; persistent rises indicate write-contention hot spots that may merit batching or queueing.

The pre-built Kibana dashboard for this service lives in [../../infrastructure/elk/kibana/dashboards/](../../infrastructure/elk/kibana/dashboards/).

---

## Related Documentation

- [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md) — Complete system topology (canonical Mermaid diagram).
- [../../docs/architecture/service-catalog.md](../../docs/architecture/service-catalog.md) — All services at a glance.
- [../../docs/architecture/event-catalog.md](../../docs/architecture/event-catalog.md) — Full Kafka topic catalog and event schemas.
- [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md) — Retry / circuit-breaker / DLQ policies.
- [../../docs/architecture/data-stores.md](../../docs/architecture/data-stores.md) — Database-per-service rationale.
- [../../docs/api/user-service.md](../../docs/api/user-service.md) — OpenAPI specification.
- [../../docs/runbook/user-service.md](../../docs/runbook/user-service.md) — Operational runbook (DLQ replay, PII-anonymization audits, manual user re-init from `user.registered`).

