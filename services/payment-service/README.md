# Payment Service

Dual-provider payment processing hub with pluggable Stripe and Razorpay adapters, encrypted-at-rest persistence, idempotent webhook handling, and Apache Kafka event emission.

> **📐 Canonical architecture diagram:** [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md)
>
> This service appears in the `CommerceCore > PaymentBox` subgraph as `PaymentSvc`, wired to `PaymentDB` (PostgreSQL, encrypted at rest), and to the `External` subgraph endpoints `Stripe` (global markets) and `Razorpay` (India-focused). Inbound webhook arrows from Stripe and Razorpay are dotted (asynchronous notifications).

---

## Overview

The Payment Service is the platform's payment processing hub. It is one of the two reference implementations of the adapter pattern in this monorepo (the other being the Notification Service's `NotificationChannel`); future multi-provider services should consult this service's adapter layer as the canonical template.

Core responsibilities:

- **`PaymentProvider` interface** with two pluggable implementations:
  - **`StripeProvider`** — Stripe charges, refunds, and webhooks for global markets (135+ currencies, international card brands).
  - **`RazorpayProvider`** — Razorpay charges, refunds, and webhooks for India (UPI, net banking, and domestic wallets).
- **Provider routing strategy** (data-driven per AAP R-10, never hard-coded) — selects a provider by currency, merchant region, and per-user preference.
- **Webhook handlers** with **HMAC signature verification** (AAP R-12) and **idempotent processing** keyed on the provider's event ID.
- **Idempotency key store** that guarantees safe retry semantics — no double-charge on retry (AAP R-8).
- **Charge / refund / webhook-event persistence** with encryption at rest (AAP R-8).
- **Saga participation** — consumes `order.created` (capture trigger) and `order.cancelled` (refund trigger), and emits `payment.succeeded`, `payment.failed`, and `payment.refunded` for downstream sagas.

---

## Architecture Context

| Aspect | Value |
|--------|-------|
| Domain | Commerce Core |
| Role | Saga participant — consumes order events and produces payment outcome events |
| Fronted by | API Gateway at `/payments/*` |
| Inbound sync | `POST /payments` (charge), `POST /payments/{id}/refund` (refund), webhook receivers `POST /webhooks/stripe`, `POST /webhooks/razorpay` |
| Inbound async | `order.created` (capture trigger), `order.cancelled` (refund trigger) |
| Outbound sync | Stripe API (`api.stripe.com`), Razorpay API (`api.razorpay.com`) — both HTTPS |
| Outbound async | `payment.succeeded`, `payment.failed`, `payment.refunded` |
| Primary store | PostgreSQL `payment_db` — **encrypted at rest** (AAP R-8) |
| DLQs | `payment.dlq` (poison messages); `<consumed-topic>.dlq` |
| Design pattern | **Adapter / Strategy** — `PaymentProvider` interface is a reference implementation for pluggability (mirrors Notification Service's `NotificationChannel`) |

---

## Data Stores

The service owns a single private PostgreSQL database, `payment_db`, with five tables (per AAP Section 0.4.4). No other service is permitted to read from or write to this database — cross-service access is exclusively via this service's HTTP API or via the `payment.*` events on Kafka (AAP R-6).

- **`payments`** — one row per payment intent
  (`id`, `order_id`, `user_id`, `amount`, `currency`, `status`, `provider`, `provider_charge_id`, `created_at`, `updated_at`).
- **`payment_attempts`** — one row per outbound provider API call
  (`payment_id`, `attempt_no`, `provider`, `request_id`, `response_code`, `latency_ms`, `error_code`, `retry_after_ms`).
- **`refunds`** — one row per refund
  (`id`, `payment_id`, `amount`, `currency`, `status`, `provider_refund_id`, `reason`, `created_at`).
- **`provider_webhooks`** — one row per inbound webhook
  (`provider`, `provider_event_id` UNIQUE, `event_type`, `signature`, `raw_payload`, `verified_at`, `processed_at`, `status`).
  The UNIQUE constraint on `(provider, provider_event_id)` enforces webhook idempotency per AAP R-12.
- **`idempotency_keys`** — one row per client-supplied idempotency key
  (`key`, `request_hash`, `response_status`, `response_body`, `created_at`, `expires_at`).
  Enforces AAP R-8 safe-retry semantics for both inbound API calls and outbound provider calls.

PostgreSQL is encrypted at rest using either **pgcrypto column-level encryption** (for sensitive columns such as `provider_charge_id`, `last4`, `brand`, and `holder_name`) or **transparent disk-level encryption** (TDE / cloud-provider EBS volume encryption). Operators select and document the chosen strategy in [../../docs/runbook/payment-service.md](../../docs/runbook/payment-service.md).

DDL for all tables and the migration runner contract live under `migrations/`. Migrations are applied automatically at startup or by a dedicated migration job per AAP R-9.

---

## Events Consumed

Per AAP R-30 and R-33, every consumed event is self-contained and triggers a single, well-defined behavior in this service.

| Event | Behavior |
|-------|----------|
| `order.created` | Resolve provider via routing strategy → create payment intent → emit `payment.succeeded` or `payment.failed` |
| `order.cancelled` | Look up active payment → issue refund via originating provider → emit `payment.refunded` |

**Routing rule note:** Provider selection for capture happens inside this service via `ProviderRouter` (currency, region, merchant preference) per AAP R-10. The Order Service does NOT specify which provider to use; it merely emits `order.created` with currency and region context.

---

## Events Produced

Per AAP R-30 and R-32, this service does NOT know which services consume the events it produces. New consumers may be added without any change to this service. All event schemas are registered in [../../infrastructure/kafka/schemas/](../../infrastructure/kafka/schemas/) and contracts evolve via Schema Registry backward-compatibility rules (AAP R-14).

| Event | Downstream Consumers |
|-------|----------------------|
| `payment.succeeded` | Order Service (saga commit), Notification Service (receipt email) |
| `payment.failed` | Order Service (saga compensation), Notification Service (failure alert) |
| `payment.refunded` | Order Service (state transition), Notification Service (refund notification) |

---

## HTTP API

### Public endpoints (fronted by API Gateway at `/payments/*`)

- `POST /payments` — charge a payment. Request body includes `order_id`, `amount`, `currency`, `customer_id`. Clients MAY supply an `Idempotency-Key` header (per AAP R-8).
- `GET /payments/{id}` — read payment details.
- `POST /payments/{id}/refund` — issue a refund against an existing payment.
- `GET /payments/{id}/attempts` — list provider call attempts for a payment (admin scope).

### Webhook receivers

These endpoints are typically NOT fronted by the API Gateway; they are reachable on a dedicated path or sub-domain so provider callbacks bypass the rate-limited public surface.

- `POST /webhooks/stripe` — Stripe webhook receiver. Verifies `Stripe-Signature` HMAC against `STRIPE_WEBHOOK_SECRET`.
- `POST /webhooks/razorpay` — Razorpay webhook receiver. Verifies `X-Razorpay-Signature` HMAC against `RAZORPAY_WEBHOOK_SECRET`.

Both webhook endpoints write to `provider_webhooks` (UNIQUE on `(provider, provider_event_id)`) for AAP R-12 idempotency, then translate the verified provider event into the appropriate `payment.*` Kafka event.

### Liveness / readiness probes (AAP R-19)

- `GET /health/live` — process-alive check.
- `GET /health/ready` — all critical dependencies (PostgreSQL, Kafka, Stripe API, Razorpay API) reachable.

The full OpenAPI specification lives at [../../docs/api/payment-service.md](../../docs/api/payment-service.md).

---

## Adapter Pattern — `PaymentProvider`

This is the reference implementation of the multi-provider adapter pattern (AAP R-10). The Notification Service's `NotificationChannel` mirrors this shape for Email and SMS; future multi-provider services should follow the same template.

```
PaymentProvider (interface)
  ├── StripeProvider   (api.stripe.com, global, 135+ currencies)
  └── RazorpayProvider (api.razorpay.com, India: UPI, net banking, wallets)
```

Each concrete provider implements a common contract (simplified):

- `async def charge(payment_intent: PaymentIntent, idempotency_key: str) -> ChargeResult`
- `async def refund(refund_request: RefundRequest, idempotency_key: str) -> RefundResult`
- `def verify_webhook_signature(headers: Mapping[str, str], raw_body: bytes) -> WebhookVerification` (AAP R-12)
- `def parse_webhook(raw_body: bytes) -> ProviderEvent`
- `async def health_check() -> ProviderHealth`
- `name: str` — provider identifier used in logs, metrics, and event payloads.

### Provider routing

Provider selection is performed by `ProviderRouter` using the tuple `(currency, region, user_preference, merchant_config)` — **never hard-coded** (AAP R-10). Default routing rules (operator-overridable in `config/default.yaml`):

- **Currency `INR`** → prefer `RazorpayProvider`; fall back to `StripeProvider` only if the Razorpay circuit breaker is open AND the merchant has explicitly enabled cross-region fallback.
- **Currency `USD` / `EUR` / `GBP` / all other ISO 4217 currencies** → prefer `StripeProvider`; fall back to `RazorpayProvider` only when explicitly enabled in merchant config.
- **Merchant override** → if a merchant has pinned a provider for a specific currency, that pin overrides the currency-based default.

This service's adapter layer is the canonical adapter-pattern reference for future multi-provider services in this monorepo.

---

## Idempotency

Per AAP R-8 and R-12, the service implements **three distinct idempotency mechanisms** so that retries — by clients, by the framework, or by providers — never double-charge, double-refund, or double-process.

- **Inbound API idempotency.** Clients (typically the Order Service) MAY supply an `Idempotency-Key` header on `POST /payments` and `POST /payments/{id}/refund`. The service stores `(key → request_hash, response)` in the `idempotency_keys` table and replays the cached response on duplicate keys with a matching request hash. A duplicate key with a *different* request hash is rejected with HTTP `409 Conflict`.
- **Outbound provider idempotency.** Every call to Stripe and Razorpay carries a per-attempt idempotency key derived deterministically from `(payment_id, attempt_no)`. Safe HTTP retries reuse the same key, so the provider's idempotency contract guarantees at-most-once execution at the provider's side (AAP R-8).
- **Webhook idempotency.** The `provider_webhooks` table has a UNIQUE constraint on `(provider, provider_event_id)`. The same provider event is never processed twice, even if the provider re-delivers the same event multiple times (AAP R-12).

---

## Retry & Fallback Policy

- **Outbound HTTP retries (AAP R-15).** Exponential backoff with jitter; maximum 5 attempts per chain; total budget 30 seconds. All retries within a chain reuse the same provider idempotency key (AAP R-8) to avoid duplicate charges.
- **Circuit breaker (AAP R-16).** Per-provider breaker. Trips when failure rate ≥ 50% over a rolling 20-call window. Open-state duration: 30 s. Half-open probe: 1 call. Breaker state is exported via the `provider_circuit_breaker_state` gauge.
- **Provider fallback (AAP R-20).** If the primary provider's breaker is open AND the payment is eligible for the alternate provider (currency support, merchant config), `ProviderRouter` routes the in-flight charge to the fallback provider. The response payload sets `fallback_used: true` and a structured log entry records the failover.
- **Kafka consumer retry topics (AAP R-17).** On consumer-side processing failure the message is routed to `<topic>.retry`; after exhausting retries it is routed to `<topic>.dlq`. Specifically: `order.created.retry`, `order.created.dlq`, `order.cancelled.retry`, `order.cancelled.dlq`.
- **Service DLQ.** `payment.dlq` carries internal poison-message events (for example, webhooks whose signature verifies but whose body cannot be parsed for the standard flow).
- **Saga compensation.** On `payment.failed`, the Order Service runs compensation per AAP R-18 (release reserved inventory, fail the order). This service does NOT manage saga state; it only emits accurate payment outcome events.

Retry and circuit-breaker thresholds — along with per-call-site overrides — are documented in [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md).

---

## Running Locally

```bash
# From the service directory
cp .env.example .env
# Fill in or accept defaults for: POSTGRES_URL, KAFKA_BOOTSTRAP, STRIPE_API_KEY,
# RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET.
# Local dev MUST use Stripe test-mode and Razorpay test-mode keys, NEVER live keys.

# Install dependencies (Python 3.11+)
python -m venv .venv
source .venv/bin/activate   # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Apply database migrations
# NOTE: `migrations/` contains the DDL; apply it with psql or via the
# container's entrypoint (which runs the migration runner on startup).

# Run the service
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

The repository-root `docker-compose.yml` spins up this service alongside PostgreSQL, Kafka, the Schema Registry, mock provider servers (for Stripe and Razorpay), and every other service dependency in a single command for full-stack local development.

---

## Testing

Per AAP Section 0.5.2.6:

- **Unit tests** (`tests/unit/`) — cover provider routing logic, webhook signature verification, the idempotency key store, retry/backoff math, and adapter contract conformance.
- **Integration tests** (`tests/integration/`) — exercise the full charge flow against mocked Stripe and Razorpay HTTP endpoints (via `respx`), Testcontainers-backed PostgreSQL and Kafka, and end-to-end `order.created` consumption with `payment.succeeded` emission.

Cross-service tests at the repository level live under [../../tests/e2e/provider-failover.spec.*](../../tests/e2e/) — they verify the Stripe → Razorpay fallback path by simulating a Stripe outage.

```bash
pytest tests/unit -v
pytest tests/integration -v --maxfail=1
```

---

## Configuration

The authoritative configuration files for this service are `config/default.yaml` (non-secret defaults) and `.env.example` (the full environment-variable template, names only).

Required environment variable **names** (values supplied by Kubernetes Secrets / Vault / cloud KMS — never by source or by `.env.example`, per AAP R-25):

- `POSTGRES_URL` — connection string for `payment_db`.
- `POSTGRES_ENCRYPTION_KEY` — column-level encryption key reference. In production this is a KMS key alias, not a literal key.
- `KAFKA_BOOTSTRAP` — Kafka bootstrap servers.
- `SCHEMA_REGISTRY_URL` — Confluent / Apicurio schema registry URL.
- `STRIPE_API_KEY`, `STRIPE_WEBHOOK_SECRET` — Stripe provider credentials and webhook HMAC secret (AAP R-12).
- `RAZORPAY_KEY_ID`, `RAZORPAY_KEY_SECRET`, `RAZORPAY_WEBHOOK_SECRET` — Razorpay provider credentials and webhook HMAC secret (AAP R-12).
- `JWT_PUBLIC_KEY_URL` — JWKS endpoint exposed by the Auth Service (AAP R-22).
- `LOG_LEVEL` — structured-logging verbosity (`DEBUG` | `INFO` | `WARN` | `ERROR`).

---

## Security

- **Encryption at rest (AAP R-8).** `payment_db` is encrypted; sensitive columns additionally use pgcrypto column-level encryption with keys sourced from KMS. This service is the only relational-DB-backed service in the platform that mandates encryption at rest.
- **Webhook signature verification (AAP R-12).** Every inbound webhook is HMAC-verified against the provider's signing secret BEFORE any state mutation. Webhooks that fail verification are logged and dropped — they are NOT routed to the DLQ, to prevent replay attacks from poisoning the retry pipeline.
- **JWT validation (AAP R-21, R-22).** Admin endpoints require a JWT with the `payments:admin` scope. JWTs are issued exclusively by the Auth Service. JWKS is fetched from `JWT_PUBLIC_KEY_URL` and cached with a bounded TTL; key rotation does NOT require a service restart.
- **TLS everywhere (AAP R-24).** All inter-service traffic and all outbound provider calls use TLS. Plaintext HTTP is permitted only on the loopback interface for sidecar communication inside a single pod.
- **Secrets never in source (AAP R-25).** Provider API keys, webhook secrets, database credentials, and KMS key references come exclusively from Kubernetes Secrets / HashiCorp Vault / cloud secret managers. The `.env.example` file in this service contains variable names only.
- **PCI-DSS scope reduction.** This service NEVER receives raw card numbers. Clients use Stripe Elements or Razorpay Checkout (hosted or embedded), which return tokens to the client; the service stores only tokenized references (`provider_charge_id`, `last4`, `brand`) — never raw PANs.

---

## Observability

Logs are structured JSON emitted to stdout, then shipped by Filebeat to Logstash, indexed in Elasticsearch, and visualized in Kibana (AAP R-26, R-27).

Required log fields on every line: `timestamp`, `level`, `service` (always `payment-service`), `correlation_id`, `user_id` (when known), `order_id` (when known), `payment_id` (when known), `provider`, `attempt_no`, `route`, `status`, `latency_ms`, `message`.

> **CRITICAL — never log full PANs, full cardholder names, full CVV values, or full webhook bodies.** Card-related fields MUST be logged as `last4` and `brand` only. Webhook bodies MUST be logged with the `raw_payload` field truncated or hashed. Violation of this rule is a PCI-DSS incident.

Key metrics exposed at `GET /metrics` (Prometheus exposition format):

- `payments_total{provider,status,currency}` — counter of payment outcomes.
- `payment_latency_ms{provider,operation}` — histogram (`operation` = `charge` | `refund`).
- `provider_circuit_breaker_state{provider}` — gauge (`0` = closed, `1` = half-open, `2` = open).
- `webhook_signature_failures_total{provider}` — counter; alertable. Sustained nonzero values indicate either a misconfigured webhook secret or an active replay attack.
- `kafka_consumer_lag{topic}` — gauge.
- `idempotency_replay_total{endpoint}` — counter of successful replays from the `idempotency_keys` table.
- `dlq_depth{topic}` — gauge for `payment.dlq` and the consumed-topic DLQs.

---

## Related Documentation

- [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md) — Complete system topology (canonical Mermaid diagram).
- [../../docs/architecture/service-catalog.md](../../docs/architecture/service-catalog.md) — All services at a glance.
- [../../docs/architecture/event-catalog.md](../../docs/architecture/event-catalog.md) — Full Kafka topic catalog.
- [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md) — Retry, circuit-breaker, DLQ, and saga compensation policies.
- [../../docs/architecture/data-stores.md](../../docs/architecture/data-stores.md) — Database-per-service rationale and encryption-at-rest specifics for the Payment Service.
- [../../docs/api/payment-service.md](../../docs/api/payment-service.md) — OpenAPI specification.
- [../../docs/runbook/payment-service.md](../../docs/runbook/payment-service.md) — Operational runbook (encryption-key rotation, webhook-secret rotation, DLQ replay, provider failover drill).
