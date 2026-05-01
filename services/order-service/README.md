# Order Service

Order lifecycle orchestrator and saga coordinator for the e-commerce checkout flow with PostgreSQL-backed state, Kafka event emission, and explicit compensation paths.

> **📐 Canonical architecture diagram:** [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md)
>
> This service appears in the `CommerceCore > OrderBox` subgraph as `OrderSvc` (the **Saga Coordinator**), wired to `OrderDB` (PostgreSQL — includes `orders`, `order_items`, `order_status_history`, `saga_state` tables). It produces `order.created`/`order.cancelled`/`order.fulfilled` events and consumes `inventory.*` + `payment.*` events to drive saga state transitions.

---

## Overview

The Order Service is the **canonical reference implementation of AAP R-18** (the saga pattern with explicit compensation) for this monorepo. Every other service that participates in a distributed transaction should consult this service's saga coordinator as the template. Its core responsibilities are:

- **Order lifecycle state machine.** Drives orders through `CREATED` → `INVENTORY_RESERVED` → `PAYMENT_TAKEN` → `FULFILLED`, with explicit compensating transitions (`COMPENSATING_INVENTORY`, `COMPENSATING_PAYMENT`, `CANCELLED`, `FAILED`) on saga failure.
- **Saga orchestration (AAP R-18).** Coordinates the distributed checkout transaction across the Inventory Service and the Payment Service using the saga pattern with explicit compensation:
  1. Create the order (local DB transaction; emit `order.created`).
  2. Reserve inventory (await `inventory.reserved` or `inventory.reservation_failed`).
  3. Take payment (await `payment.succeeded` or `payment.failed`).
  4. Confirm (emit `order.fulfilled`) **OR** compensate (release inventory, refund payment, emit `order.cancelled`).
- **Order + order-item persistence** with a full status history (`order_status_history` audit trail) for every state transition.
- **Saga state persistence** in `saga_state` for **recovery across service restarts** — a crash mid-saga must not lose progress; the durable saga record is the safety net that makes the coordinator restart-safe.
- **Order placement and lookup HTTP API** fronted by the API Gateway at `/orders/*` (place, lookup, history, manual cancel, status history, saga state).
- **DLQ handling.** Poison messages route to `order.dlq` per AAP R-17, and saga compensation triggers when consumer retry attempts exhaust.

---

## Architecture Context

| Aspect | Value |
|--------|-------|
| Domain | Commerce Core |
| Role | **Saga Coordinator** — orchestrates the distributed checkout transaction (AAP R-18) |
| Fronted by | API Gateway at `/orders/*` |
| Inbound sync | `POST /orders` (place order), `GET /orders/{id}` (lookup), `GET /orders?userId={uuid}` (history), `POST /orders/{id}/cancel` (manual cancel) |
| Inbound async | `inventory.reserved`, `inventory.reservation_failed`, `payment.succeeded`, `payment.failed` (saga drivers) |
| Outbound sync | NONE (saga steps are coordinated via Kafka, not direct HTTP — pure event-driven orchestration) |
| Outbound async | `order.created`, `order.cancelled`, `order.fulfilled` |
| Primary store | PostgreSQL `order_db` — includes `saga_state` for recovery (AAP R-6, R-7) |
| DLQs | `order.dlq` (poison messages); per-consumed-topic `<topic>.dlq` for unprocessable inputs |
| Design pattern | **Saga (orchestration variant)** — this service is the canonical reference implementation of AAP R-18 |

---

## Data Stores

This service owns a single private database — `order_db` — per the database-per-service principle (AAP R-6). All four tables are owned exclusively by this service; no other service reads from or writes to them. Cross-service access flows over this service's HTTP API or via the `order.*` events on Kafka.

The schema comprises four tables (per AAP Section 0.4.4):

- **`orders`** — one row per customer order
  (`id`, `user_id`, `status`, `currency`, `total_amount`, `created_at`, `updated_at`, `idempotency_key`).
  The `idempotency_key` column is UNIQUE and enables safe retry of `POST /orders` (see [Idempotency](#idempotency)).
- **`order_items`** — line items per order
  (`order_id` FK, `product_id`, `quantity`, `unit_price`, `line_total`).
- **`order_status_history`** — append-only audit trail of state transitions
  (`order_id`, `from_status`, `to_status`, `reason`, `occurred_at`, `correlation_id`).
  Every transition driven by the saga state machine writes a row here, providing a complete forensic record per order.
- **`saga_state`** — durable saga progress per order
  (`order_id`, `saga_id`, `current_step`, `awaiting_event`, `retry_count`, `deadline_at`, `compensation_required`, `last_error`, `updated_at`).
  This table is **architecturally critical**: it is the mechanism that makes the coordinator restart-safe. Without `saga_state`, a crashed coordinator would lose in-flight saga progress and could cause duplicate compensations or stuck orders. The `deadline_at` column also drives the timeout-based compensation safety net (see [Retry & Fallback Policy](#retry--fallback-policy)).

PostgreSQL is encrypted at rest in production via standard managed-DB encryption-at-rest. Note that the AAP requires encryption-at-rest specifically for `payment_db` per AAP R-8; for `order_db` it is recommended but not strictly required by the AAP.

DDL for all four tables and the migration runner contract live under `migrations/`. Migrations are applied automatically at startup or by a dedicated migration job per AAP R-9.

---

## Saga Coordination Flow

The saga is the centerpiece of this service. The diagram below sketches the canonical happy path and the compensation hand-offs in plain ASCII; the canonical visual representation lives in [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md).

```
┌─────────────┐                    ┌──────────────────┐                  ┌─────────────────┐
│  Client     │  POST /orders      │   Order Service  │  order.created   │     Kafka       │
│ via Gateway ├───────────────────►│ (saga coordinator│─────────────────►│                 │
└─────────────┘                    │  state=CREATED)  │                  └────────┬────────┘
                                   └────────┬─────────┘                           │
                                            │                                     ▼
                                            │                          ┌──────────────────┐
                                            │ inventory.reserved       │ Inventory Service│
                                            │◄─────────────────────────┤ (reserves stock) │
                                            │  OR inventory.reservation_failed             │
                                            ▼                          └──────────────────┘
                            ┌─────────────────────────────┐
                            │  state = INVENTORY_RESERVED │
                            │  (or COMPENSATING on fail)  │
                            └─────────────┬───────────────┘
                                          │
                                          ▼
                                  (Payment Service consumes order.created
                                   directly per AAP Section 0.5.1 diagram;
                                   it captures payment after detecting the order)
                                          │
                                          ▼
                                          │ payment.succeeded
                                          │◄────────────  Payment Service
                                          │ OR payment.failed
                                          ▼
                            ┌─────────────────────────────┐
                            │  state = PAYMENT_TAKEN      │
                            │  (or COMPENSATING on fail)  │
                            └─────────────┬───────────────┘
                                          │
                                          ▼
                                          │ order.fulfilled (success path)
                                          │ OR order.cancelled + compensation events
                                          ▼
                                   ┌──────────────────┐
                                   │     Kafka        │
                                   │ Notification Svc │
                                   └──────────────────┘
```

### Compensation Matrix

Per AAP R-18 — *"The Order Service must implement the saga pattern with explicit compensation steps for inventory release and payment reversal on failure."* The compensation matrix below enumerates every failure mode and its compensating action, so operators can reason about saga behavior at a glance.

| Failed Step | Compensating Action | Final State |
|-------------|---------------------|-------------|
| Inventory reservation failed | Emit `order.cancelled` (no inventory to release; never reserved) | `CANCELLED` |
| Payment failed (after inventory reserved) | Inventory Service consumes `order.cancelled` and releases reserved stock; emit `order.cancelled` | `CANCELLED` |
| Saga timeout (`deadline_at` reached without expected event) | Run full compensation: trigger inventory release if reserved; trigger refund if payment captured; emit `order.cancelled` | `CANCELLED` |
| DLQ exhaustion on consumed event | Saga marks order as requiring manual intervention; emit `order.cancelled`; alert via metric `saga_manual_intervention_total` | `FAILED` |

---

## Events Consumed

Per AAP R-30 and R-33, every consumed event is self-contained and triggers a single, well-defined state transition in the saga.

| Event | Saga Behavior |
|-------|---------------|
| `inventory.reserved` | Advance saga state from `CREATED` → `INVENTORY_RESERVED`; await payment outcome |
| `inventory.reservation_failed` | Advance saga state to `CANCELLED`; emit `order.cancelled`; trigger notification |
| `payment.succeeded` | Advance saga state from `INVENTORY_RESERVED` → `PAYMENT_TAKEN` → `FULFILLED`; emit `order.fulfilled` |
| `payment.failed` | Advance saga state to compensation; trigger inventory release (via `order.cancelled` consumed by Inventory Service); emit `order.cancelled` |

---

## Events Produced

Per AAP R-30 and R-32, this service does NOT know which services consume the events it produces. New consumers may be added without any change to this service. All event schemas are registered in `infrastructure/kafka/schemas/` and contracts evolve via Schema Registry backward-compatibility rules (AAP R-14).

| Event | Downstream Consumers (Producer-agnostic per AAP R-32) |
|-------|--------------------------------------------------------|
| `order.created` | Inventory Service (trigger reservation), Payment Service (capture payment), Notification Service (placement confirmation), Recommendation Engine (interaction signal) |
| `order.cancelled` | Inventory Service (release reservation if held), Payment Service (issue refund if captured), Notification Service (cancellation notice) |
| `order.fulfilled` | Notification Service (shipping/fulfillment notice), Recommendation Engine (strong purchase signal) |

---

## HTTP API

### Public endpoints (fronted by API Gateway at `/orders/*`)

- `POST /orders` — place a new order. Request body includes `userId`, `items[]` (`productId`, `quantity`), `currency`. Clients MAY supply an `Idempotency-Key` header for safe retries (see [Idempotency](#idempotency)). The response is `202 Accepted` with the order id and `status=CREATED`; the saga drives subsequent state transitions asynchronously.
- `GET /orders/{id}` — fetch a single order with line items and current status.
- `GET /orders?userId={uuid}&status={enum}&limit={n}&cursor={opaque}` — paginated order history for a user.
- `POST /orders/{id}/cancel` — request manual cancellation. Valid only in pre-fulfillment states; triggers saga compensation if payment was already captured.
- `GET /orders/{id}/status-history` — audit trail of state transitions (admin scope or owning user).
- `GET /orders/{id}/saga-state` — current saga progress (admin scope; useful for ops debugging).

### Liveness / readiness probes (AAP R-19)

- `GET /health/live` — process-alive check.
- `GET /health/ready` — all critical dependencies (PostgreSQL, Kafka, Schema Registry) reachable.

The full OpenAPI specification lives at [../../docs/api/order-service.md](../../docs/api/order-service.md).

---

## Idempotency

Per AAP R-18 (saga safety) and the at-least-once delivery semantics of Kafka, the service implements **three distinct idempotency mechanisms** so that retries — by clients, by the framework, or by Kafka — never create duplicate orders, duplicate state transitions, or duplicate compensations.

- **Inbound API idempotency.** Clients (typically the API Gateway forwarding from a checkout client) MAY supply an `Idempotency-Key` header on `POST /orders`. The service stores `(idempotency_key → order_id, response)` and replays the cached response on duplicate keys with a matching request hash; duplicate keys with a *different* request hash are rejected with HTTP `409 Conflict`. The `idempotency_key` column on the `orders` table is UNIQUE.
- **Saga state idempotency.** Every saga state transition is durable in `saga_state`. If a consumer reprocesses an event after restart (at-least-once delivery), the saga state machine recognizes the duplicate and is a no-op (e.g., `INVENTORY_RESERVED` + `inventory.reserved` → no state change, log debug).
- **Event production idempotency.** The Kafka producer is configured with `enable.idempotence=true` and `acks=all` to prevent duplicate event publication on producer retry.

---

## Retry & Fallback Policy

Per AAP R-15, R-16, R-17, and R-20.

- **Outbound HTTP retries (AAP R-15).** This service has no synchronous outbound HTTP to other services — saga steps are pure event-driven coordination. The PostgreSQL connection pool retries on transient errors; the Kafka producer retries internally per its configuration (idempotent producer, `acks=all`).
- **Kafka consumer retry topics (AAP R-17).** On processing failure, messages route to `<topic>.retry` with exponential backoff; after exhausting retries, they route to `<topic>.dlq`. Specifically:
  - `inventory.reserved.retry`, `inventory.reserved.dlq`
  - `inventory.reservation_failed.retry`, `inventory.reservation_failed.dlq`
  - `payment.succeeded.retry`, `payment.succeeded.dlq`
  - `payment.failed.retry`, `payment.failed.dlq`
- **Service DLQ.** `order.dlq` carries internal poison-message events (for example, events that fail schema validation entirely or cannot be routed to a saga state machine).
- **Saga timeout fallback (AAP R-20).** Every saga step has a `deadline_at` recorded in `saga_state`. A background scheduler scans for overdue sagas and triggers compensation per the matrix in [Saga Coordination Flow](#saga-coordination-flow). Without this safety net, sagas could hang indefinitely when downstream services never respond.
- **Graceful degradation.** The placement endpoint stays available even if downstream services are slow — the order is recorded, `order.created` is published, and the saga drives forward asynchronously. The HTTP response returns `202 Accepted` with the order id and current status (`CREATED`).

Retry and timeout thresholds — along with per-call-site overrides — are documented in [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md).

---

## Running Locally

```bash
# From the service directory
cp .env.example .env
# Fill in or accept defaults for: POSTGRES_URL (order_db), KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL,
#   JWT_PUBLIC_KEY_URL, SAGA_STEP_TIMEOUT_MS, SAGA_COMPENSATION_TIMEOUT_MS

# Install dependencies (Python 3.11+)
python -m venv .venv
source .venv/bin/activate   # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Apply database migrations (creates orders, order_items, order_status_history, saga_state)
# NOTE: `migrations/` contains the DDL; apply with alembic, psql, or the container's entrypoint.

# Run the service
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

The repository-root `docker-compose.yml` spins up this service alongside PostgreSQL `order-db`, Kafka, the Schema Registry, sibling services (Inventory, Payment, Notification), and every other dependency in a single command for full-stack local development.

---

## Testing

Per AAP Section 0.5.2.6:

- **Unit tests** (`tests/unit/`) — saga state machine transitions, compensation handler logic, deadline computation, DLQ routing, idempotency key replay handling.
- **Integration tests** (`tests/integration/`) — full checkout saga with mocked Inventory and Payment services using Testcontainers (PostgreSQL + Kafka). Cover the happy path AND every compensation path in the matrix above.

Repository-level cross-service tests live under [../../tests/e2e/checkout-flow.spec.*](../../tests/e2e/) and [../../tests/e2e/resilience.spec.*](../../tests/e2e/) per AAP Section 0.5.2.6 — they verify end-to-end checkout including saga compensation paths.

```bash
pytest tests/unit -v
pytest tests/integration -v --maxfail=1
```

---

## Configuration

The authoritative configuration files for this service are `config/default.yaml` (non-secret defaults) and `.env.example` (the full environment-variable template, names only).

Required environment variable **names** (values supplied by Kubernetes Secrets / Vault / cloud secret manager — never by source or by `.env.example`, per AAP R-25):

- `POSTGRES_URL` — connection string for `order_db`.
- `KAFKA_BOOTSTRAP` — Kafka bootstrap servers.
- `SCHEMA_REGISTRY_URL` — Confluent / Apicurio schema registry URL.
- `SAGA_STEP_TIMEOUT_MS` — per-step deadline (drives `deadline_at` in `saga_state`).
- `SAGA_COMPENSATION_TIMEOUT_MS` — maximum time budget for a full compensation chain.
- `JWT_PUBLIC_KEY_URL` — JWKS endpoint exposed by the Auth Service (AAP R-22).
- `LOG_LEVEL` — structured-logging verbosity (`DEBUG` | `INFO` | `WARN` | `ERROR`).

---

## Security

- **JWT validation (AAP R-21, R-22).** Customer-facing endpoints require a JWT with the appropriate scope; admin endpoints (`/orders/{id}/saga-state`, status overrides) require the `orders:admin` scope. JWTs are issued exclusively by the Auth Service (AAP R-21). JWKS is fetched from `JWT_PUBLIC_KEY_URL` and cached with a bounded TTL; key rotation does NOT require a service restart (AAP R-22).
- **TLS everywhere (AAP R-24).** All inter-service traffic is TLS-encrypted. Plaintext HTTP is permitted only on the loopback interface for sidecar communication inside a single pod.
- **Secrets never in source (AAP R-25).** Database credentials and JWT signing-key references come exclusively from Kubernetes Secrets / HashiCorp Vault / cloud secret managers. The `.env.example` file in this service contains variable names only — never values.
- **No payment card data.** This service does NOT handle payment card data (that is the Payment Service's concern). It stores order totals and currency, which are not sensitive but are still subject to standard data-protection logging hygiene (no PII in log messages beyond `user_id` and `order_id`).

---

## Observability

Logs are structured JSON emitted to stdout, then shipped by Filebeat to Logstash, indexed in Elasticsearch, and visualized in Kibana (AAP R-26, R-27).

Required log fields on every line (AAP R-26): `timestamp`, `level`, `service` (always `order-service`), `correlation_id` (per AAP R-13), `user_id` (when known), `order_id` (when known), `saga_id` (when in saga context), `route`, `method`, `status`, `latency_ms`, `message`.

Key metrics exposed at `/metrics` in Prometheus exposition format (scraped by Metricbeat per AAP R-27):

- `orders_placed_total{status,currency}` — counter of placement outcomes.
- `order_placement_latency_ms` — histogram of `POST /orders` end-to-end latency.
- `saga_state_transitions_total{from_state,to_state}` — counter of saga state machine transitions.
- `saga_compensation_total{reason}` — counter of compensation events; **alertable** — sustained nonzero indicates downstream failure.
- `saga_manual_intervention_total` — counter of sagas escalated to manual handling; **alertable** — nonzero indicates DLQ exhaustion requiring ops attention.
- `saga_in_flight{state}` — gauge of sagas currently in each state.
- `saga_step_latency_ms{step}` — histogram of per-step saga latency (`inventory_reserve`, `payment_capture`, etc.).
- `kafka_consumer_lag{topic}` — gauge of consumer lag per consumed topic.
- `dlq_depth{topic}` — gauge of DLQ depth per topic.

The pre-built Kibana dashboard for this service lives in [../../infrastructure/elk/kibana/dashboards/](../../infrastructure/elk/kibana/dashboards/).

---

## Related Documentation

- [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md) — Complete system topology (canonical Mermaid diagram).
- [../../docs/architecture/service-catalog.md](../../docs/architecture/service-catalog.md) — All services at a glance.
- [../../docs/architecture/event-catalog.md](../../docs/architecture/event-catalog.md) — Full Kafka topic catalog and event schemas.
- [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md) — Retry / circuit-breaker / DLQ / saga compensation policies.
- [../../docs/architecture/data-stores.md](../../docs/architecture/data-stores.md) — Database-per-service rationale.
- [../../docs/api/order-service.md](../../docs/api/order-service.md) — OpenAPI specification.
- [../../docs/runbook/order-service.md](../../docs/runbook/order-service.md) — Operational runbook (saga manual intervention, DLQ replay procedures, schema migrations).

