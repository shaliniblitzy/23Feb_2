# Product Service

Product catalog and category hierarchy authority — flexible-schema MongoDB-backed catalog with REST query/admin API and Apache Kafka event emission for downstream consumers.

> **📐 Canonical architecture diagram:** [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md)
>
> This service appears in the `CommerceCore > ProductBox` subgraph as `ProductSvc`, wired to `ProductDB` (MongoDB). Solid edges from the API Gateway carry `/products` REST traffic; dashed outgoing edges to Kafka carry `product.created` and `product.updated` events consumed by the Recommendation Engine and the Inventory Service.

---

## Overview

The Product Service is the **catalog source-of-truth** of the e-commerce platform. It owns the
product, category, and product-media collections in its private MongoDB database and is the only
service permitted to mutate that data (per the database-per-service principle, AAP R-6). Every
catalog state change is published as a domain event on the Apache Kafka backbone for downstream
consumption. Its core responsibilities are:

- **Product catalog queries** — list, search, and filter by category, price range, and free-form
  attribute predicates; pagination and sorting are first-class.
- **Admin writes** — create, update, and deprecate products and variants through admin-scoped
  endpoints guarded by JWT scope `products:admin` (per AAP R-21, R-22).
- **Category tree management** — hierarchical taxonomy with parent/child relationships, stable
  URL slugs, materialized ancestor paths, and breadcrumb support.
- **Product media metadata** — references to CDN-hosted images and videos. The media bytes
  themselves live on the CDN; this service stores only the metadata records that point at them.
- **Event emission** — publishes `product.created` and `product.updated` events on every catalog
  state change (per AAP R-30 naming and R-32 producer-agnostic contract).
- **Read-side metadata service** — the Recommendation Engine calls this service via REST for
  product metadata hydration; consumer fan-out is decoupled from the producer per AAP R-32.

## Architecture Context

| Aspect | Value |
|--------|-------|
| Domain | Commerce Core |
| Role | Catalog source-of-truth — produces `product.*` events; serves REST reads for catalog metadata |
| Fronted by | API Gateway at `/products/*` and `/categories/*` |
| Inbound sync | `GET /products`, `GET /products/{id}`, `GET /categories`, `GET /categories/{id}/products`, plus admin `POST/PUT/DELETE` (admin scope) |
| Inbound async | NONE (this service is a pure event producer) |
| Outbound sync | NONE (no upstream dependencies in the steady state) |
| Outbound async | `product.created`, `product.updated` |
| Primary store | MongoDB `product_db` (collections: `products`, `categories`, `product_media`) |
| DLQs | `product.created.dlq`, `product.updated.dlq` |
| Design pattern | **Document-oriented aggregate** — `Product`, `Category`, `Variant` aggregates persisted as MongoDB documents with embedded variants and media references |

## Data Stores

This service owns a single private database — `product_db` — backed by **MongoDB** per AAP
Section 0.4.4 and AAP R-7. No other service reads from or writes to `product_db` directly; all
cross-service access flows over this service's HTTP API or via the `product.*` events on Kafka
(per AAP R-6).

The schema comprises three collections (per AAP Section 0.4.4):

- **`products`** — one document per product:
  `_id`, `sku`, `name`, `slug`, `description`, `category_ids[]`, `brand`, `price`, `currency`,
  `attributes` (free-form sub-document), `variants[]` (embedded), `media_ids[]`, `status`,
  `version`, `created_at`, `updated_at`.
- **`categories`** — one document per category:
  `_id`, `name`, `slug`, `parent_id` (nullable for root categories), `depth`, `path[]` (materialized
  ancestor chain), `display_order`, `metadata`, `status`.
- **`product_media`** — one document per media asset:
  `_id`, `product_id`, `cdn_url`, `kind` (`image` / `video` / `360-spin`), `alt_text`,
  `sort_order`, `created_at`.

**Index hints (defined in `migrations/`):**

- `products.sku` — UNIQUE (one product per stock-keeping unit).
- `products.slug` — UNIQUE (URL-stable slug).
- `products.category_ids` — multikey index for category-scoped listing.
- `products.status, products.created_at` — compound index for admin lists by recency.
- Text index on `products.name + products.description` for full-text search.
- `categories.slug` — UNIQUE.
- `categories.parent_id` — for tree traversal.
- `product_media.product_id` — for media listing per product.

**Why MongoDB? (AAP R-7)** — Product attributes vary widely by category: apparel needs
`size`/`color`, electronics needs `voltage`/`capacity`, groceries needs `weight`/`expiry`. A
document model avoids JOIN overhead and schema migrations for new attribute types. Variants are
embedded for read-locality (one fetch returns the product and all its buyable SKUs); media is a
separate collection because its lifecycle (CDN replication, soft-delete, re-encoding) differs.

**No cross-service foreign keys (AAP R-6).** `category_ids[]` references categories within the
same database; `media_ids[]` references the local `product_media` collection. We never reference
the Inventory Service's stock rows or the Order Service's line items — those are foreign domains
and only reachable via their public APIs or via Kafka events.

## Events Produced

Every successful catalog mutation publishes a domain event to Kafka. Schemas are registered in
the central [`infrastructure/kafka/schemas/`](../../infrastructure/kafka/schemas/) directory and
evolve under Schema Registry compatibility rules (per AAP R-14).

| Event | Schema (Schema Registry) | Downstream Consumers (informational) |
|-------|--------------------------|--------------------------------------|
| `product.created` | `product.created.v1` (JSON Schema or Avro) | Recommendation Engine (rebuild embeddings), Inventory Service (sync SKU) |
| `product.updated` | `product.updated.v1` (JSON Schema or Avro) | Recommendation Engine (refresh embeddings), Inventory Service (status sync) |

**Event payload shape (informational).** Every event includes a stable header envelope plus a
domain-specific body. Header fields are: `event_id`, `event_version`, `correlation_id`,
`occurred_at`, `producer` (= `product-service`). The body carries a `product` envelope with
`id`, `sku`, `slug`, `name`, `category_ids`, `price`, `currency`, `status`, `attributes`,
`variants[]`, and `media_refs[]`. Per AAP R-33, **events are self-contained** — consumers do not
need to call back to this service to interpret an event.

**Producer-agnostic contract (AAP R-32).** This service does **not** know which services consume
its events. Adding a new consumer must require zero code changes here. The downstream consumer
list above is informational only and is maintained centrally in
[../../docs/architecture/event-catalog.md](../../docs/architecture/event-catalog.md).

## Events Consumed

**This service consumes NO Kafka events** in the steady state. The Product Service is a **pure
event producer** — admin writes flow exclusively over the synchronous HTTP API, and there is no
upstream domain event that drives a catalog mutation. Catalog truth originates here and
propagates outward; the symmetric extreme is the Notification Service, which is a pure terminal
consumer.

## HTTP API

The service is fronted by the API Gateway at `/products/*` and `/categories/*`. Read endpoints
accept anonymous traffic by default; admin write endpoints require a JWT issued by the Auth
Service with scope `products:admin` (per AAP R-21, R-22). The full OpenAPI specification lives
at [../../docs/api/product-service.md](../../docs/api/product-service.md).

**Public read endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/products?category={id}&q={text}&min_price={n}&max_price={n}&page={n}&size={n}&sort={field}` | List/search products with filters, pagination, and sort. |
| `GET`  | `/products/{id}` | Fetch a single product (full document including embedded variants and media references). |
| `GET`  | `/products/by-slug/{slug}` | Fetch by stable URL slug. |
| `GET`  | `/products/{id}/media` | List media references for a product. |
| `GET`  | `/categories` | List categories — top-level by default; pass `?parent={id}` to list children. |
| `GET`  | `/categories/{id}` | Fetch a single category. |
| `GET`  | `/categories/{id}/products` | List products in a category (paginated). |

**Admin write endpoints (require JWT scope `products:admin`):**

| Method | Path | Description |
|--------|------|-------------|
| `POST`   | `/products` | Create a product. Emits `product.created`. |
| `PUT`    | `/products/{id}` | Update a product. Emits `product.updated`. |
| `DELETE` | `/products/{id}` | Soft-delete (sets `status=deprecated`). Emits `product.updated`. |
| `POST`   | `/categories` | Create a category. |
| `PUT`    | `/categories/{id}` | Update a category. |
| `POST`   | `/products/{id}/media` | Register a media reference (CDN URL is provided externally). |

**Standard liveness / readiness probes (AAP R-19):**

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/health/live`  | Process-alive only. |
| `GET`  | `/health/ready` | Verifies MongoDB and the Kafka producer are reachable. |

## Domain Model

The service centers on three root aggregates:

```text
Product       — root aggregate; owns embedded Variant[] and references Category and ProductMedia
Category      — root aggregate; tree node with parent_id and materialized path[] for fast ancestor queries
ProductMedia  — root aggregate; references Product by id; describes CDN-hosted asset metadata
```

`Product` documents are **immutable in the application layer** between writes — every mutation
flows through a domain command (`CreateProduct`, `UpdateProduct`, `DeprecateProduct`) that
produces a new document version and emits the corresponding domain event **after** the MongoDB
write commits. This **write-then-publish** ordering, combined with the stable `event_id`,
guarantees idempotent retries: a duplicate publish carries the same `event_id` and is deduped by
downstream consumers.

## Concurrency & Versioning

- **Optimistic concurrency.** Every product document carries a `version` field. Updates are
  conditioned on the current version using `findOneAndUpdate` with a `version` filter and an
  atomic `$inc: { version: 1 }`. On version mismatch the request returns **HTTP 409 Conflict**
  and the caller must re-read and retry.
- **Event ordering.** Events for the same `product_id` are produced to the same Kafka partition
  using `product_id` as the partition key, so consumers observe strict per-product ordering (an
  implication of AAP R-30).
- **Idempotent writes.** `POST /products` and `PUT /products/{id}` accept an optional
  `Idempotency-Key` header. The service deduplicates on the tuple `(idempotency_key,
  request_hash)` so a safe client retry does not produce duplicate documents or duplicate
  Kafka events.

## Retry & Fallback Policy

This section is the canonical declaration of retry, circuit-breaker, and fallback behaviors for
this service per AAP R-15, R-16, R-17, and R-20. Platform-wide patterns are documented in
[../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md).

- **MongoDB write retries.** Exponential backoff with jitter, max 3 attempts on transient driver
  errors (`AutoReconnect`, network timeouts, primary stepdown). Aborts immediately on
  duplicate-key errors — those are caller bugs, not transient infrastructure failures.
- **Kafka producer retries.** Configured with `acks=all` and `enable.idempotence=true`; retries
  are unbounded up to a 30-second total budget per produce call. If the budget is exhausted the
  document write is left in place (it already committed) and a background **outbox-style
  reconciler** republishes the missed event from a durable outbox collection — see
  [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md).
- **Circuit breaker.** Wraps the Kafka producer. Opens at ≥50% failure rate over a 20-call
  rolling window; remains open for 30 seconds; transitions to half-open on a single probe call
  (per AAP R-16). When open, writes still succeed (the document commits) and the outbox
  reconciler picks up the slack once Kafka recovers.
- **DLQ topics (AAP R-17):**
  - `product.created.dlq` — events that fail schema validation or cannot be produced after
    exhausting retries.
  - `product.updated.dlq` — same for update events.
- **Fallback for read-side consumers (AAP R-20).** The Recommendation Engine consumes
  `product.*` events for embedding refresh; if events are temporarily delayed it falls back to
  its Redis cache and to eventual REST hydration from this service. This service has **no
  upstream dependencies** in the steady state and therefore declares no inbound fallback path.

## Running Locally

```bash
# From the service directory: services/product-service/
cp .env.example .env
# Fill in or accept defaults for: MONGODB_URL, KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL

# Install dependencies (Python 3.11+)
python -m venv .venv
source .venv/bin/activate          # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Apply MongoDB index definitions and seed categories
# NOTE: `migrations/` contains index creation scripts and category seed data;
#       apply via the bundled migration runner or the container entrypoint.

# Run the service
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

The repository-root [`docker-compose.yml`](../../docker-compose.yml) spins up this service plus
MongoDB, Kafka, the Schema Registry, and all peer services for full-stack local development.
Prefer the root compose stack for any flow that exercises end-to-end behavior; prefer the
in-directory workflow above for fast inner-loop iteration on this service alone.

## Testing

Per the platform test layout in AAP Section 0.5.2.6, tests are split into unit and integration
suites under this service:

- **Unit tests** — `tests/unit/` — domain validation, slug generation, query builder, event
  payload assembly, and version-conflict handling.
- **Integration tests** — `tests/integration/` — full pipeline against ephemeral MongoDB and
  Kafka instances spun up via Testcontainers, end-to-end CRUD round-trip and event emission
  verification.

```bash
pytest tests/unit -v
pytest tests/integration -v --maxfail=1
```

End-to-end checkout flows that span the Product Service and other services live under
`tests/e2e/` at the repository root.

## Configuration

Configuration is loaded from `config/default.yaml`, layered with environment-specific overrides,
and finalized by environment variables read at startup. See `config/default.yaml` and
`.env.example` in this directory for the complete configuration surface.

The following environment variables are required at startup. Per AAP R-25, **no secret values are
ever stored in this repository** — `.env.example` lists names only and concrete values are
supplied at deploy time via Kubernetes Secrets, Vault, or a cloud KMS-backed secret manager.

- `MONGODB_URL` — connection string for the `product_db` MongoDB instance.
- `MONGODB_DATABASE` — logical database name (defaults to `product_db`).
- `KAFKA_BOOTSTRAP` — comma-separated list of Kafka bootstrap brokers.
- `SCHEMA_REGISTRY_URL` — URL of the Confluent Schema Registry for event schema validation.
- `JWT_PUBLIC_KEY_URL` — JWKS endpoint exposed by the Auth Service for JWT signature
  verification (per AAP R-22).
- `LOG_LEVEL` — log verbosity (`DEBUG`, `INFO`, `WARN`, `ERROR`).

The service fails fast at startup if any required variable is missing or any critical dependency
is unreachable.

## Security

- **JWT validation (AAP R-21, R-22).** Admin endpoints require a JWT with scope
  `products:admin`. JWTs are issued exclusively by the Auth Service — no other service mints
  tokens. JWKS is fetched from `JWT_PUBLIC_KEY_URL` and cached with a bounded TTL so key
  rotation does not require a service restart. Read endpoints accept anonymous traffic from the
  API Gateway by default; rate limiting is enforced upstream at the gateway.
- **TLS everywhere (AAP R-24).** All inter-service traffic, MongoDB connections, and outbound
  Kafka traffic use TLS. Plaintext HTTP is only acceptable on loopback within a pod for sidecar
  communication.
- **Secrets never in source (AAP R-25).** MongoDB credentials, Kafka credentials, and any
  third-party tokens come exclusively from Kubernetes Secrets, Vault, or a cloud KMS — never
  from source files or `.env.example`. The `.env.example` file lists variable **names** only.
- **Input sanitization.** All admin write payloads are validated by Pydantic models with strict
  types. Unknown fields are rejected to prevent attribute injection into the free-form
  `attributes` sub-document.

## Observability

Logs are emitted as structured JSON to stdout and shipped by **Filebeat** to **Logstash**, which
forwards them to **Elasticsearch** for indexing and **Kibana** for visualization (per AAP R-27).
A pre-built dashboard for this service surfaces request volume, p95 latency, error rate, Kafka
producer success rate, circuit-breaker state, and DLQ depth.

Every log line includes the following structured fields (per AAP R-26):

| Field | Description |
|-------|-------------|
| `timestamp` | RFC 3339 timestamp of the log event. |
| `level` | Log level (`DEBUG`, `INFO`, `WARN`, `ERROR`). |
| `service` | Always `product-service`. |
| `correlation_id` | Propagated from the API Gateway via the `X-Correlation-ID` header and echoed on Kafka message headers. |
| `user_id` | Authenticated user ID (when known). |
| `route` | HTTP route or Kafka topic being processed. |
| `method` | HTTP method (when relevant). |
| `status` | HTTP status code or Kafka processing outcome. |
| `latency_ms` | Wall-clock latency of the operation in milliseconds. |
| `product_id` | Product identifier (when relevant). |
| `category_id` | Category identifier (when relevant). |
| `message` | Human-readable log message. |

Key metrics exposed at `/metrics`:

- `http_requests_total{route,method,status}` — counter.
- `http_request_latency_ms{route,method}` — histogram.
- `mongo_operations_total{operation,collection,status}` — counter.
- `mongo_operation_latency_ms{operation,collection}` — histogram.
- `kafka_producer_send_total{topic,status}` — counter.
- `kafka_producer_circuit_breaker_state` — gauge (`0=closed`, `1=half-open`, `2=open`).
- `dlq_depth{topic}` — gauge.
- `catalog_size{collection}` — gauge (number of documents per collection).

## Related Documentation

- [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md) — Complete system topology
- [../../docs/architecture/service-catalog.md](../../docs/architecture/service-catalog.md) — All services at a glance
- [../../docs/architecture/event-catalog.md](../../docs/architecture/event-catalog.md) — Full Kafka topic catalog
- [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md) — Retry/CB/DLQ policies
- [../../docs/architecture/data-stores.md](../../docs/architecture/data-stores.md) — DB-per-service rationale, MongoDB choice for Product Service
- [../../docs/api/product-service.md](../../docs/api/product-service.md) — OpenAPI spec
- [../../docs/runbook/product-service.md](../../docs/runbook/product-service.md) — Operational runbook (index management, DLQ replay, etc.)
