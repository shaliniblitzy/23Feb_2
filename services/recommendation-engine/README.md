# Recommendation Engine Service

ML-based personalized product recommendation service with vector similarity inference and popularity-based fallback.

> **📐 Canonical architecture diagram:** [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md)
>
> This service appears in the `IntelligenceDomain` subgraph as `RecoSvc`, wired to `VecStore` (pgvector) and `RecoCache` (Redis).

---

## Overview

The Recommendation Engine is the ML inference component of the e-commerce platform. It serves
personalized product recommendations to authenticated users via the API Gateway and continuously
learns from domain events flowing across the Apache Kafka backbone. Its core responsibilities are:

- **Feature pipeline** — aggregates user interaction signals (views, purchases, ratings) and
  product attributes into feature vectors.
- **Embedding store writer** — computes and persists product/user embeddings in pgvector.
- **Inference runtime** — serves personalized recommendations via vector similarity + reranking.
- **Popularity-based fallback** — returns globally popular products when ML inference has no
  signal (cold-start) or upstream cache misses occur.
- **Product metadata hydration** — reads from Product Service over REST for product details;
  falls back to Redis cache on Product Service unavailability.

## Architecture Context

| Aspect | Value |
|--------|-------|
| Domain | Intelligence Domain |
| Role | Terminal Kafka consumer + synchronous HTTP query endpoint |
| Fronted by | API Gateway at `/recommendations/*` |
| Inbound sync | GET recommendation queries (from API Gateway) |
| Inbound async | `product.created`, `product.updated`, `order.created`, `order.fulfilled`, `user.registered`, `user.updated` |
| Outbound sync | Product Service (REST) — metadata hydration |
| Outbound async | NONE (terminal consumer) |
| Primary store | Vector store — **pgvector** (PostgreSQL extension) |
| Cache | **Redis** — recommendation cache, metadata cache |
| Model store | Local filesystem `./models/` (mounted from image or volume) |
| Fallback chain | ML inference → Redis cache → popularity-based (`rec_cache` table) |

## Data Stores

This service owns a single private database (per the database-per-service principle) backed by
**PostgreSQL with the pgvector extension**, plus a **Redis** cache. No other service reads from or
writes to these stores directly; all cross-service access flows over Kafka events or the public
HTTP API.

The pgvector database contains three tables:

- `embeddings` — product and user embedding vectors stored in a pgvector `vector` column; queried
  via approximate nearest-neighbour search for similarity inference.
- `interaction_features` — aggregated interaction signals per `(user, product)` tuple, fed by
  domain events to power feature engineering.
- `rec_cache` — materialized list of globally popular products used for the popularity-based
  fallback path; refreshed on every `order.fulfilled` event.

Redis is used as a short-lived cache for recommendation responses (per-user, short TTL) and for
Product Service metadata. The metadata cache is also the failure-mode fallback when the Product
Service is unavailable: the service degrades gracefully by reading the most recently cached
metadata rather than failing the request.

The on-disk `./models/` directory holds the serialized inference model artefact and is mounted
from the container image or a persistent volume in production deployments.

## Events Consumed

The Recommendation Engine subscribes to six domain topics across the `product.*`, `order.*`, and
`user.*` namespaces. Each event triggers a deterministic update to the feature/embedding stores.

| Event | Behavior |
|-------|----------|
| `product.created` | Compute product embedding → upsert into `embeddings` |
| `product.updated` | Recompute product embedding → upsert into `embeddings`; invalidate cached product metadata |
| `order.created` | Increment interaction features (weak signal); update user profile draft |
| `order.fulfilled` | Increment interaction features (strong signal); increment popularity counter for each line item in `rec_cache` |
| `user.registered` | Create empty user embedding row |
| `user.updated` | Recompute user embedding based on preference changes |

**This service produces NO Kafka events.** It is a terminal consumer — every other service in the
monorepo emits events for downstream consumption, but the Recommendation Engine only reads. This
asymmetry is intentional: recommendations are a derived, read-side concern and never mutate the
authoritative state owned by other domains.

## HTTP API

The service exposes a single primary query endpoint plus the standard health probes. The full
OpenAPI specification lives at [../../docs/api/recommendation-engine.md](../../docs/api/recommendation-engine.md).

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/recommendations?userId={uuid}&limit={n}&category={optional}` | Returns a ranked list of recommended products for the given user. Supports optional category scoping and a result limit. |
| `GET`  | `/health/live` | Liveness probe — process-alive only. |
| `GET`  | `/health/ready` | Readiness probe — verifies all critical dependencies (Kafka, pgvector, Redis, model file) are reachable. |

All authenticated routes require a JWT issued by the Auth Service; the API Gateway terminates
authentication and forwards a verified token in the `Authorization` header. Health probes are
unauthenticated to allow Kubernetes and load-balancer health checks.

## Fallback Chain

The service implements an explicit three-tier fallback chain so that recommendation queries never
return a hard error to the client when an upstream component is degraded. AAP R-20 mandates that
"fallback behavior must be declared for every inter-service and external dependency" — this
section is the canonical declaration for this service.

1. **Primary — ML inference.** Vector similarity search in pgvector against the user's embedding,
   followed by a reranker that incorporates recency and category preference. This is the path
   exercised when the model is loaded, pgvector is healthy, and the user has at least one
   interaction signal.
2. **Secondary — Redis cache.** Previously computed recommendations are cached per user with a
   short TTL. When the inference runtime is unavailable or slow, the service serves the cached
   list from Redis.
3. **Tertiary — Popularity-based.** When both inference and cache are unavailable (cold-start,
   model load failure, or full Redis outage), the service returns the top-N entries from the
   `rec_cache` table. This list is materialized continuously from `order.fulfilled` events so it
   always reflects current global purchase popularity.

When the response is served from a non-primary tier, the inference runtime returns HTTP 200 with a
`degraded: true` flag in the response payload. Clients can use this flag to render a UI hint or
to log that the response came from a fallback path.

## Resilience

The service follows the platform-wide retry, circuit-breaker, and dead-letter conventions defined
in [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md).

- **Retry policy (Product Service client).** Exponential backoff with jitter, max 3 attempts, and
  a 5-second total budget per outbound call (per AAP R-15). Idempotent GET requests for product
  metadata are safe to retry; non-idempotent calls do not exist on this code path.
- **Circuit breaker (Product Service client).** Opens at ≥50% failure rate over a 20-call rolling
  window; remains open for 30 seconds; transitions to half-open on a single probe call (per AAP
  R-16). When open, the service short-circuits to the Redis metadata cache and the popularity
  fallback.
- **Kafka consumer retries.** On processing failure, the message is routed to `<topic>.retry`;
  after the configured retry budget is exhausted, it is routed to `<topic>.dlq` (per AAP R-17).
  DLQ depth is exposed as a Prometheus-compatible metric and surfaced on the Kibana dashboard.

This service is a **read-side service** — there is no saga coordination here. End-to-end
transactional orchestration of the checkout flow is the Order Service's responsibility per AAP
R-18.

## Running Locally

```bash
# From the service directory: services/recommendation-engine/
cp .env.example .env
# Fill in or accept defaults for: POSTGRES_URL, REDIS_URL, KAFKA_BOOTSTRAP, PRODUCT_SERVICE_URL, MODEL_PATH

# Install dependencies (Python 3.11+)
python -m venv .venv
source .venv/bin/activate          # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Apply database migrations (pgvector schema)
# NOTE: `migrations/` contains the DDL; apply with your preferred tool (psql, Flyway, Liquibase)
#       or the container's entrypoint.

# Run the service
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

The repository-root [`docker-compose.yml`](../../docker-compose.yml) spins up this service plus
pgvector, Redis, Kafka, the Schema Registry, and all peer services for full-stack local
development. Prefer the root compose stack for any flow that exercises end-to-end behavior;
prefer the in-directory workflow above for fast inner-loop iteration on this service alone.

## Testing

Per the platform test layout in AAP Section 0.5.2.6, tests are split into unit and integration
suites under this service:

- **Unit tests** — `tests/unit/` — covers feature-extraction math, fallback-selection logic,
  vector-similarity helpers, and request validation.
- **Integration tests** — `tests/integration/` — exercises the full pipeline against ephemeral
  Kafka, pgvector, and Redis instances spun up via Testcontainers.

```bash
pytest tests/unit -v
pytest tests/integration -v --maxfail=1
```

End-to-end checkout flows that span the Recommendation Engine and other services live under
`tests/e2e/` at the repository root.

## Configuration

Configuration is loaded from `config/default.yaml`, layered with environment-specific overrides,
and finalized by environment variables read at startup. See `config/default.yaml` and
`.env.example` in this directory for the complete configuration surface.

The following environment variables are required at startup. Per AAP R-25, **no secret values are
ever stored in this repository** — `.env.example` lists names only and concrete values are
supplied at deploy time via Kubernetes Secrets or a secret manager.

- `POSTGRES_URL` — connection string for the pgvector-enabled PostgreSQL database.
- `REDIS_URL` — connection string for the Redis cache.
- `KAFKA_BOOTSTRAP` — comma-separated list of Kafka bootstrap brokers.
- `SCHEMA_REGISTRY_URL` — URL of the Confluent Schema Registry for event schema validation.
- `PRODUCT_SERVICE_URL` — base URL of the Product Service for metadata hydration.
- `MODEL_PATH` — filesystem path to the serialized inference model artefact.
- `JWT_PUBLIC_KEY_URL` — JWKS endpoint exposed by the Auth Service for JWT signature verification
  (per AAP R-22).
- `LOG_LEVEL` — log verbosity (`DEBUG`, `INFO`, `WARN`, `ERROR`).

The service fails fast at startup if any required variable is missing or any critical dependency
is unreachable.

## Observability

Logs are emitted as structured JSON to stdout and shipped by **Filebeat** to **Logstash**, which
forwards them to **Elasticsearch** for indexing and **Kibana** for visualization (per AAP R-27).
A pre-built dashboard for this service surfaces request volume, p95 latency, error rate, Kafka
consumer lag per topic, and DLQ depth.

Every log line includes the following structured fields (per AAP R-26):

| Field | Description |
|-------|-------------|
| `timestamp` | RFC 3339 timestamp of the log event. |
| `level` | Log level (`DEBUG`, `INFO`, `WARN`, `ERROR`). |
| `service` | Always `recommendation-engine`. |
| `correlation_id` | Propagated from the API Gateway via the `X-Correlation-ID` header and Kafka message headers. |
| `user_id` | Authenticated user ID (when known). |
| `route` | HTTP route or Kafka topic being processed. |
| `status` | HTTP status code or Kafka processing outcome. |
| `latency_ms` | Wall-clock latency of the operation in milliseconds. |
| `message` | Human-readable log message. |

## Related Documentation

- [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md) — Complete system topology
- [../../docs/architecture/service-catalog.md](../../docs/architecture/service-catalog.md) — All services at a glance
- [../../docs/architecture/event-catalog.md](../../docs/architecture/event-catalog.md) — Full Kafka topic catalog
- [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md) — Retry/CB/DLQ policies
- [../../docs/architecture/data-stores.md](../../docs/architecture/data-stores.md) — DB-per-service rationale
