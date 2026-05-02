# Inventory Service

Stock reservation engine and warehouse-state owner with transactional PostgreSQL persistence, pluggable warehouse adapters, low-stock detection, and Apache Kafka event emission.

> **📐 Canonical architecture diagram:** [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md)
>
> This service appears in the `CommerceCore > InventoryBox` subgraph as `InventorySvc`, wired to `InventoryDB` (PostgreSQL — includes `stock_items`, `reservations`, `warehouses`, `stock_movements` tables). It produces `inventory.reserved` / `inventory.released` / `inventory.low-stock` events and consumes `order.*` events to drive reservation lifecycle.

---

## Overview

The Inventory Service is the **stock reservation engine and warehouse-state owner** of the e-commerce platform. It is a **saga participant** (not coordinator) — it consumes `order.*` events emitted by the Order Service and emits `inventory.*` outcome events that drive the saga forward to the next step (typically payment) or trigger compensation. Its core responsibilities are:

- **Stock reservation engine** — atomically reserves stock on `order.created`, releases it on `order.cancelled`, and finalizes it on `order.fulfilled`. Reservations are written under PostgreSQL transactional semantics so that concurrent orders cannot oversell the same SKU.
- **Warehouse adapter abstraction** — a pluggable per-warehouse `WarehouseAdapter` interface for multi-warehouse fulfillment. The default `DatabaseWarehouseAdapter` keeps stock entirely inside `inventory_db`; future adapters (e.g. an external WMS) can be added without touching the reservation code paths. This mirrors the adapter pattern used by Payment Service's `PaymentProvider` and Notification Service's `NotificationChannel`.
- **Low-stock detection** — emits `inventory.low-stock` alerts when the available quantity for a (product, warehouse) tuple drops below a configurable threshold. Threshold is per-SKU and per-warehouse and is admin-configurable through the HTTP API.
- **Stock movement audit trail** — every reserve / release / fulfill / replenish action writes an append-only row to `stock_movements`, enabling reconciliation and forensic analysis without back-filling state from event streams.
- **Stock query API** — fronted by the API Gateway at `/inventory/*` for product detail page (PDP) reads and cart preflight stock checks, with batch lookup support to avoid N+1 calls from the cart.

---

## Architecture Context

| Aspect | Value |
|--------|-------|
| Domain | Commerce Core |
| Role | Saga participant — consumes `order.*` events; emits `inventory.*` outcome events to drive Order Service saga forward (AAP R-18) |
| Fronted by | API Gateway at `/inventory/*` |
| Inbound sync | `GET /inventory/{productId}` (stock lookup), `GET /inventory?productIds={uuid,...}` (batch lookup), admin endpoints for warehouse + threshold management |
| Inbound async | `order.created` (reserve trigger), `order.cancelled` (release trigger), `order.fulfilled` (finalize trigger) |
| Outbound sync | NONE (warehouse adapters MAY call external WMS APIs in the future; default in-DB adapter is the reference implementation) |
| Outbound async | `inventory.reserved`, `inventory.released`, `inventory.low-stock` |
| Primary store | PostgreSQL `inventory_db` (transactional integrity required for reservations — AAP R-7) |
| DLQs | `inventory.dlq` (poison messages); per-consumed-topic `<topic>.dlq` for unprocessable inputs |
| Design pattern | **Adapter / Strategy** — `WarehouseAdapter` interface for multi-warehouse fulfillment |

---

## Data Stores

This service owns a single private database — `inventory_db` — per the database-per-service principle (AAP R-6). All four tables below are owned exclusively by this service; no other service reads from or writes to them. Cross-service access flows over this service's HTTP API or via the `inventory.*` events on Kafka.

The schema comprises four tables (per AAP Section 0.4.4):

- **`stock_items`** — current available + reserved counts per `(product_id, warehouse_id)` tuple
  (`id`, `product_id`, `warehouse_id`, `available_qty`, `reserved_qty`, `low_stock_threshold`, `version`, `updated_at`).
  The `version` column is the **optimistic-locking discriminator** — every UPDATE bumps it and asserts the prior value, so two concurrent reservation transactions cannot silently lose an update.
- **`reservations`** — one row per active reservation
  (`id`, `order_id` UNIQUE, `status`, `expires_at`, `created_at`) with child **`reservation_items`**
  (FK `reservation_id`, `product_id`, `warehouse_id`, `quantity`).
  The UNIQUE constraint on `order_id` is the **idempotency primitive** — duplicate `order.created` events (at-least-once Kafka delivery) become NOOPs after the first successful insert.
- **`warehouses`** — warehouse registry
  (`id`, `name`, `region`, `status`, `adapter_type`, `config`).
  The `adapter_type` column is the data-driven dispatch key for the `WarehouseAdapter` resolver — it is **never hard-coded** in source.
- **`stock_movements`** — append-only audit trail of every reservation, release, fulfillment, replenishment
  (`id`, `product_id`, `warehouse_id`, `movement_type`, `quantity_delta`, `before_qty`, `after_qty`, `order_id`, `correlation_id`, `occurred_at`).
  This is the canonical source of truth for reconciliation queries; it is never UPDATEd or DELETEd, only INSERTed.

**PostgreSQL was chosen specifically because reservations require ACID transactional integrity (AAP R-7).** Overselling — fulfilling more orders than there is stock to ship — is a critical business defect. Document stores cannot defensibly prevent it without complex two-phase coordination, and read-after-write inconsistencies in eventually-consistent stores would manifest as oversells under sustained concurrency. PostgreSQL gives us serial-equivalent reservation under transaction isolation plus the `SELECT ... FOR UPDATE` row-level lock that tightens the window further (see [Concurrency & Atomicity](#concurrency--atomicity) below).

DDL for all four tables and the migration runner contract live under `migrations/`. Migrations are applied automatically at startup or by a dedicated migration job per AAP R-9.

---

## Reservation Lifecycle Flow

The reservation lifecycle is the centerpiece of this service. The diagram below sketches the canonical sequence in plain ASCII; the canonical visual representation lives in [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md).

```
┌────────────────┐    order.created    ┌─────────────────────┐    inventory.reserved    ┌────────────────┐
│ Order Service  │────────────────────▶│  Inventory Service  │─────────────────────────▶│ Order Service  │
│ (saga init)    │   (qty per item)    │ (atomic reservation │   (per-item reservation  │ (saga step #2  │
└────────────────┘                     │  via PostgreSQL TX) │    + reservation_id)     │  advances)     │
                                        └──────────┬──────────┘                          └────────────────┘
                                                   │
                                        ┌──────────▼──────────┐
                                        │ stock_items: dec    │
                                        │   available_qty     │
                                        │ reservations: insert│
                                        │ stock_movements: log│
                                        └──────────┬──────────┘
                                                   │
                                                   ▼ if available_qty < threshold
                                        ┌─────────────────────┐
                                        │ inventory.low-stock │ (alert event)
                                        └─────────────────────┘

Compensation paths:
  order.cancelled  → release reservation (return qty to available); emit inventory.released
  order.fulfilled  → finalize reservation (decrement reserved_qty permanently); emit inventory.released
                     (terminal "released" state means stock has shipped; not returned to availability)
```

The reservation state machine progresses `CREATED → ACTIVE → RELEASED | FULFILLED | EXPIRED`. The full state matrix is:

| Inbound Event | Reservation Action | Stock Effect | Emitted Event |
|---------------|-------------------|--------------|---------------|
| `order.created` (sufficient stock) | INSERT reservation; status=ACTIVE | available_qty -= qty; reserved_qty += qty | `inventory.reserved` |
| `order.created` (insufficient stock) | NO insert; abort | No change | `inventory.reservation_failed` (consumed by Order Service for saga compensation) |
| `order.cancelled` (active reservation exists) | UPDATE reservation; status=RELEASED | available_qty += qty; reserved_qty -= qty | `inventory.released` |
| `order.fulfilled` (active reservation exists) | UPDATE reservation; status=FULFILLED | reserved_qty -= qty (terminal; stock has shipped) | `inventory.released` (with `final=true` flag) |
| Reservation expired (`expires_at` reached without `order.fulfilled`/`order.cancelled`) | UPDATE reservation; status=EXPIRED | available_qty += qty; reserved_qty -= qty | `inventory.released` (with `expired=true` flag) |

The `EXPIRED` row is **the safety net for stuck sagas** — without it, a coordinator crash mid-saga or a long-running provider outage upstream would freeze stock indefinitely. See [Concurrency & Atomicity](#concurrency--atomicity) for the scheduler that enforces this.

---

## Events Consumed

The Inventory Service subscribes to three `order.*` topics. Each event triggers a deterministic reservation transition.

| Event | Behavior |
|-------|----------|
| `order.created` | Atomically attempt reservation per line item; emit `inventory.reserved` on success or `inventory.reservation_failed` on insufficient stock |
| `order.cancelled` | Look up active reservation by `order_id`; release reserved stock back to available; emit `inventory.released` |
| `order.fulfilled` | Look up active reservation by `order_id`; finalize reservation (stock has shipped, NOT returned to availability); emit `inventory.released` (with `final=true`) |

Per AAP R-33, every consumed event is **self-contained** — the service never calls back to the Order Service to "resolve the meaning" of an inbound event during normal processing.

---

## Events Produced

| Event | Downstream Consumers (Producer-agnostic per AAP R-32) |
|-------|--------------------------------------------------------|
| `inventory.reserved` | Order Service (saga progress; advance to PAYMENT step), Recommendation Engine (interaction signal) |
| `inventory.released` | Order Service (saga compensation acknowledgement), Notification Service (cancellation context) |
| `inventory.low-stock` | Notification Service (operations alert), Recommendation Engine (suppress recommendations for low-stock items) |

Per AAP R-32, **this service does NOT know which services consume these events**. New consumers can be added with zero changes here. Schemas are registered in [`infrastructure/kafka/schemas/`](../../infrastructure/kafka/schemas/) and contracts evolve via Schema Registry compatibility rules (AAP R-14). Topic names follow the `<domain>.<verb>` convention (AAP R-30).

---

## HTTP API

Public stock-lookup endpoints are fronted by the API Gateway at `/inventory/*` and are publicly readable but rate-limited at the gateway. Admin endpoints require a JWT with the `inventory:admin` scope (see [Security](#security) below). The full OpenAPI specification lives at [../../docs/api/inventory-service.md](../../docs/api/inventory-service.md).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/inventory/{productId}` | Single SKU stock lookup (aggregated across warehouses). |
| `GET` | `/inventory?productIds={uuid,uuid,...}` | Batch stock lookup (e.g. cart preflight). |
| `GET` | `/inventory/{productId}/warehouses` | Per-warehouse breakdown for a SKU. **Admin scope.** |
| `POST` | `/inventory/replenishments` | Record stock receipt from procurement; appends a `stock_movements` row. **Admin scope.** |
| `GET` | `/inventory/reservations/{orderId}` | Look up reservation state by order ID. **Admin scope.** |
| `PUT` | `/inventory/{productId}/threshold` | Set per-SKU low-stock threshold. **Admin scope.** |

Standard liveness and readiness probes (per AAP R-19) are also exposed:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health/live` | Liveness probe — process-alive only. |
| `GET` | `/health/ready` | Readiness probe — verifies all critical dependencies (PostgreSQL, Kafka, Schema Registry) are reachable. |

---

## Adapter Pattern — `WarehouseAdapter`

The service implements the **adapter / strategy pattern** for pluggable per-warehouse fulfillment. The default adapter keeps stock entirely inside `inventory_db`; a future adapter can bridge to a third-party WMS without touching the reservation code paths.

```
WarehouseAdapter (interface)
  ├── DatabaseWarehouseAdapter   (default — manages stock entirely in inventory_db)
  └── ExternalWMSWarehouseAdapter (future — bridges to a third-party WMS via REST/AMQP)
```

Each adapter implements the same simplified contract:

- `async def reserve(items: list[ReservationItem], correlation_id: str) -> ReservationOutcome`
- `async def release(reservation_id: UUID, correlation_id: str) -> None`
- `async def finalize(reservation_id: UUID, correlation_id: str) -> None`
- `async def get_stock(product_id: UUID) -> StockSnapshot`
- `async def health_check() -> AdapterHealth`
- `name: str` — property returning the warehouse identifier used in logs and metrics.

Adapter selection per warehouse is driven by the `warehouses.adapter_type` column — **never hard-coded**. The resolver loads the column on warehouse registration and instantiates the matching adapter class through a registry.

**This service's adapter layer mirrors Payment Service's `PaymentProvider` and Notification Service's `NotificationChannel` patterns** — pluggable, data-driven, extensible without code changes for new warehouses. Together, the three services form the canonical adapter-pattern reference set in this monorepo.

---

## Concurrency & Atomicity

Inventory is the most concurrency-sensitive surface in the platform: multiple buyers racing to reserve the last unit of a popular SKU is the canonical contention case, and getting the locking story wrong manifests as oversells (a critical business defect). The service uses **four reinforcing primitives** to prevent oversells while keeping the happy path fast:

- **Optimistic locking on `stock_items`.** The `stock_items.version` column is incremented on every UPDATE and the prior value is asserted in the `WHERE` clause. Conflicting concurrent reservations see a zero-row update and **retry with bounded exponential backoff**. This handles the optimistic case where lock contention is rare.
- **`SELECT ... FOR UPDATE` within the reservation transaction.** Under sustained contention, the optimistic path would degrade into livelock; the pessimistic row lock taken at the start of the reservation transaction serializes concurrent `order.created` events for the same `(product_id, warehouse_id)` and prevents two concurrent reservations from over-allocating the same SKU.
- **Single-shot transaction.** The reservation INSERT, the `stock_items` UPDATE (decrement available, increment reserved), and the `stock_movements` INSERT all execute in **one** PostgreSQL transaction. There is no intermediate state in which the reservation exists but the stock has not been decremented (or vice versa) — readers always see a consistent snapshot.
- **Idempotency by `order_id`.** The `reservations.order_id` column is UNIQUE. Duplicate `order.created` events (Kafka's at-least-once delivery semantics make duplicates inevitable on consumer rebalances) become NOOPs after the first successful reservation: the second insert violates the UNIQUE constraint, the handler catches the conflict, and the prior reservation outcome is re-emitted to keep the saga driving forward.

In addition, every reservation carries an **`expires_at` deadline**. A background scheduler (interval governed by `RESERVATION_EXPIRY_SCHEDULER_POLL_INTERVAL_MS`) scans for expired-but-unresolved reservations and releases them, emitting `inventory.released` with `expired=true`. **Without this safety net, a stuck saga upstream would freeze stock indefinitely** — for example, if the Payment Service goes down between `inventory.reserved` and `payment.succeeded`, the reservation deadline ensures stock returns to availability instead of being held by a saga that will never complete. The expiration scheduler is the deadline-driven complement to the saga's explicit compensation events.

---

## Retry & Fallback Policy

This service follows the platform-wide retry, circuit-breaker, and dead-letter conventions defined in [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md).

- **Outbound HTTP retries (AAP R-15).** This service has **no synchronous outbound HTTP** to other services by default — the `DatabaseWarehouseAdapter` keeps everything in `inventory_db`. The PostgreSQL connection pool retries on transient connection errors with bounded backoff, and the Kafka producer retries on broker errors per its internal config. The future `ExternalWMSWarehouseAdapter` will be wrapped in the standard exponential-backoff + circuit-breaker stack (AAP R-15, R-16) when it is added.
- **Kafka consumer retry topics (AAP R-17).** On processing failure, messages route to `<topic>.retry` (with exponential backoff between attempts); after exhausting the configured retry budget, they route to `<topic>.dlq`. Specifically:
  - `order.created.retry`, `order.created.dlq`
  - `order.cancelled.retry`, `order.cancelled.dlq`
  - `order.fulfilled.retry`, `order.fulfilled.dlq`
- **Service DLQ.** `inventory.dlq` carries internal poison-message events — for example, events that fail Schema Registry validation entirely or whose payload cannot be parsed under the registered schema. This DLQ is alerted on and is replayed by the operational runbook (see [../../docs/runbook/inventory-service.md](../../docs/runbook/inventory-service.md)).
- **Reservation failure (insufficient stock) is NOT a DLQ event.** Insufficient stock is a normal business outcome that emits `inventory.reservation_failed` for the Order Service saga to handle (typically by cancelling the order and notifying the customer). Treating insufficient stock as an exception would conflate infrastructure failures with business logic, both of which require very different operational responses.
- **Graceful degradation (AAP R-20).** Stock-lookup endpoints (`GET /inventory/*`) remain available even when Kafka is unreachable — the read-side service is independent of the event backbone. Reservations require Kafka availability to emit outcome events, so a sustained Kafka outage will eventually fail the readiness probe and pull this service out of the load balancer for the write path while leaving reads online for the PDP.

---

## Running Locally

```bash
# From the service directory: services/inventory-service/
cp .env.example .env
# Fill in or accept defaults for: POSTGRES_URL (inventory_db), KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL,
#   JWT_PUBLIC_KEY_URL, LOW_STOCK_THRESHOLD_DEFAULT, RESERVATION_EXPIRY_MS

# Install dependencies (Python 3.11+)
python -m venv .venv
source .venv/bin/activate          # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Apply database migrations (creates stock_items, reservations, warehouses, stock_movements)
# NOTE: `migrations/` contains the DDL; apply with alembic, psql, or the container's entrypoint.

# Run the service
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

The repository-root [`docker-compose.yml`](../../docker-compose.yml) spins up this service plus PostgreSQL `inventory-db`, Kafka, Schema Registry, sibling services (Order, Notification), and all dependencies for full-stack local development. Prefer the root compose stack for any flow that exercises the end-to-end checkout saga (browse → reserve → pay → fulfill); prefer the in-directory workflow above for fast inner-loop iteration on this service alone.

---

## Testing

Per the platform test layout in AAP Section 0.5.2.6, tests are split into unit and integration suites under this service:

- **Unit tests** — `tests/unit/` — reservation logic, low-stock threshold trigger, optimistic-lock retry, expiration scheduler tick, and warehouse adapter contract conformance for `DatabaseWarehouseAdapter`.
- **Integration tests** — `tests/integration/` — full saga step using Testcontainers (PostgreSQL + Kafka): consume `order.created` → reserve in PostgreSQL → emit `inventory.reserved`; plus the compensation paths (`order.cancelled`, `order.fulfilled`) and the expiration sweep.

```bash
pytest tests/unit -v
pytest tests/integration -v --maxfail=1
```

Repository-level cross-service tests live under [../../tests/e2e/checkout-flow.spec.*](../../tests/) and [../../tests/e2e/resilience.spec.*](../../tests/) (per AAP Section 0.5.2.6) — they verify end-to-end checkout including reservation, release, and fulfillment paths across the Order, Inventory, Payment, and Notification services.

---

## Configuration

Configuration is loaded from `config/default.yaml`, layered with environment-specific overrides, and finalized by environment variables read at startup. See `config/default.yaml` and `.env.example` in this directory for the complete configuration surface.

The following environment variables are required at startup. Per AAP R-25, **no secret values are ever stored in this repository** — `.env.example` lists names only and concrete values are supplied at deploy time via Kubernetes Secrets or a secret manager.

- `POSTGRES_URL` — connection string for the `inventory_db` PostgreSQL database.
- `KAFKA_BOOTSTRAP` — comma-separated list of Kafka bootstrap brokers.
- `SCHEMA_REGISTRY_URL` — URL of the Confluent Schema Registry for event schema validation (per AAP R-14).
- `LOW_STOCK_THRESHOLD_DEFAULT` — fallback per-SKU threshold for `inventory.low-stock` event emission when no per-SKU override is set.
- `RESERVATION_EXPIRY_MS` — deadline (in milliseconds) for releasing stuck reservations whose saga has not completed.
- `RESERVATION_EXPIRY_SCHEDULER_POLL_INTERVAL_MS` — how often the background expiration scheduler scans for expired reservations.
- `JWT_PUBLIC_KEY_URL` — JWKS endpoint exposed by the Auth Service for JWT signature verification (per AAP R-22).
- `LOG_LEVEL` — log verbosity (`DEBUG`, `INFO`, `WARN`, `ERROR`).

The service fails fast at startup if any required variable is missing or any critical dependency is unreachable (per AAP R-19).

---

## Security

- **JWT validation only (AAP R-21).** Customer-facing endpoints (stock lookups) are publicly readable but rate-limited at the API Gateway. Admin endpoints (warehouse + threshold management, replenishment recording, reservation lookup) require a JWT with the `inventory:admin` scope. JWTs are issued exclusively by the Auth Service; this service is **never** an issuer and only validates signatures.
- **JWKS caching (AAP R-22).** Public keys are fetched from `JWT_PUBLIC_KEY_URL` and cached with a bounded TTL so key rotation propagates without service restart.
- **TLS in transit (AAP R-24).** All inter-service traffic — including Kafka, PostgreSQL, and JWKS fetches — is TLS-encrypted. Plaintext HTTP is permitted only on loopback within a pod for sidecar communication.
- **Secrets handling (AAP R-25).** Database credentials and any value pointing at a production endpoint come exclusively from Kubernetes Secrets, HashiCorp Vault, or cloud-provider secret managers. They are **never** committed to source or to `.env.example`; `.env.example` lists variable names only.
- **Data sensitivity.** This service does NOT handle PII or payment data. Stock-level numbers are not sensitive on their own, but `stock_movements` rows carry `order_id` (which can be linked to `user_id` by the Order Service); standard log hygiene applies and `stock_movements` is access-controlled to the `inventory:admin` scope through the HTTP API.

---

## Observability

Logs are emitted as structured JSON to stdout and shipped by **Filebeat** to **Logstash**, which forwards them to **Elasticsearch** for indexing and **Kibana** for visualization (per AAP R-27). A pre-built dashboard for this service surfaces request volume, p95 latency, error rate, Kafka consumer lag per topic, DLQ depth, and reservation-outcome counters.

Every log line includes the following structured fields (per AAP R-26 plus service-specific fields for the reservation pipeline):

| Field | Description |
|-------|-------------|
| `timestamp` | RFC 3339 timestamp of the log event. |
| `level` | Log level (`DEBUG`, `INFO`, `WARN`, `ERROR`). |
| `service` | Always `inventory-service`. |
| `correlation_id` | Propagated from the API Gateway via the `X-Correlation-ID` header and Kafka message headers (per AAP R-13). |
| `user_id` | Recipient user ID when known via order_id resolution. |
| `order_id` | Saga-driving order ID when handling `order.*` events. |
| `product_id` | SKU under reservation (when relevant). |
| `warehouse_id` | Warehouse handling the reservation (when relevant). |
| `route` | HTTP route (when handling a sync request). |
| `method` | HTTP method (when handling a sync request). |
| `status` | HTTP status code or reservation outcome (`success`, `insufficient_stock`, `expired`, etc.). |
| `latency_ms` | Wall-clock latency in milliseconds. |
| `message` | Human-readable log message. |

Key metrics exposed at `/metrics` (Prometheus-format, scraped by Metricbeat per AAP R-27):

- `reservations_created_total{warehouse,outcome}` — counter; `outcome` is `success` or `insufficient_stock`.
- `reservations_released_total{warehouse,reason}` — counter; `reason` is `order_cancelled`, `order_fulfilled`, or `expired`.
- `reservation_latency_ms{operation}` — histogram; `operation` is `reserve`, `release`, or `finalize`.
- `low_stock_events_total{warehouse}` — counter of `inventory.low-stock` emissions per warehouse.
- `stock_optimistic_lock_retries_total{operation}` — counter; **alertable** — sustained nonzero indicates contention requiring lock-strategy review.
- `expired_reservations_total` — counter; **alertable** — sustained nonzero indicates upstream saga failures (e.g. Payment Service unavailable) that the deadline-driven release is masking.
- `kafka_consumer_lag{topic}` — gauge of consumer lag per consumed topic.
- `dlq_depth{topic}` — gauge of pending messages in each DLQ.

---

## Related Documentation

- [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md) — Complete system topology
- [../../docs/architecture/service-catalog.md](../../docs/architecture/service-catalog.md) — All services at a glance
- [../../docs/architecture/event-catalog.md](../../docs/architecture/event-catalog.md) — Full Kafka topic catalog
- [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md) — Retry/CB/DLQ policies
- [../../docs/architecture/data-stores.md](../../docs/architecture/data-stores.md) — DB-per-service rationale
- [../../docs/api/inventory-service.md](../../docs/api/inventory-service.md) — OpenAPI specification
- [../../docs/runbook/inventory-service.md](../../docs/runbook/inventory-service.md) — Operational runbook (DLQ replay, stuck reservation cleanup, threshold tuning)

