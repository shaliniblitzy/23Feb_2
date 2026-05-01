# Notification Service

Multi-channel transactional notification dispatcher with pluggable Email and SMS adapters, backed by Apache Kafka event consumption and a retry/DLQ pipeline.

> **📐 Canonical architecture diagram:** [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md)
>
> This service appears in the `NotificationDomain` subgraph as `NotifSvc`, wired to `NotifDB` (PostgreSQL/Cassandra) and to the `External` subgraph endpoints `EmailProv` (SendGrid / AWS SES) and `SMSProv` (Twilio / AWS SNS).

---

## Overview

The Notification Service is the multi-channel transactional dispatcher of the e-commerce platform.
It consumes domain events from the Apache Kafka backbone, resolves each event into one or more
rendered messages, and fans out those messages to recipients via Email and SMS providers behind a
unified adapter interface. Its core responsibilities are:

- **`NotificationChannel` interface** with two pluggable implementations:
  - **`EmailChannel`** — dispatches transactional email via SendGrid (or AWS SES).
  - **`SmsChannel`** — dispatches transactional SMS via Twilio (or AWS SNS).
- **Channel routing** — selects channel(s) for each event based on event type, template
  metadata, and per-user preferences. Routing is data-driven, never hard-coded (per AAP R-11).
- **Template renderer** — resolves event payloads into localized email/SMS bodies via versioned
  templates stored in the service's private database. Templates are channel-specific and
  locale-aware.
- **Delivery log** — persists a per-attempt audit trail (success, bounce, throttled, failed) in
  the `delivery_attempts` table so every outbound provider call is observable end-to-end.
- **Retry scheduler** — bounded exponential backoff with jitter for failed dispatches; messages
  exhausting the retry budget are routed to a per-channel dead-letter topic (per AAP R-17).

## Architecture Context

| Aspect | Value |
|--------|-------|
| Domain | Notification Domain |
| Role | **Terminal Kafka consumer** — no domain events are produced |
| Fronted by | API Gateway at `/notifications/*` (admin endpoints only) |
| Inbound sync | Admin GET/POST for templates + user preferences |
| Inbound async | `user.registered`, `order.created`, `order.cancelled`, `order.fulfilled`, `payment.succeeded`, `payment.failed`, `payment.refunded` |
| Outbound sync | Email provider API, SMS provider API (external) |
| Outbound async | NONE (terminal consumer) |
| Primary store | PostgreSQL `notification_db` (write-optimized log schema) |
| DLQs | `notifications.email.dlq`, `notifications.sms.dlq` |
| Design pattern | **Adapter / Strategy** — `NotificationChannel` interface is the reference implementation for pluggability (mirrors Payment Service's `PaymentProvider`) |

## Data Stores

This service owns a single private database — `notification_db` — per the database-per-service
principle (AAP R-6). No other service reads from or writes to `notification_db` directly; all
cross-service access flows over the public HTTP API (admin endpoints) or via Kafka events. The
schema comprises four tables (per AAP Section 0.4.4):

- **`notification_log`** — one row per inbound event resolution attempt: `event_id`, `user_id`,
  `channel`, `template`, `correlation_id`, `status`, and creation/completion timestamps. This
  table is the canonical record of which events produced which notifications and is the join
  parent for the per-attempt rows in `delivery_attempts`.
- **`delivery_attempts`** — one row per outbound provider API call: `attempt_no`, `provider`,
  `provider_message_id`, `response_code`, and `latency_ms`. This is the table the retry scheduler
  polls when deciding whether to re-dispatch a message; it is also the source of the delivery
  log query exposed at `GET /notifications/log`.
- **`templates`** — versioned message templates: `id`, `channel`, `locale`, `subject`, `body`,
  `variables`, `version`. Updates create a new version row rather than mutating the prior version
  in place, so historical sends remain reproducible against the exact template they used.
- **`user_channel_prefs`** — per-user channel opt-in/opt-out and locale: `user_id`, `channel`,
  `opted_in`, `locale`, `quiet_hours`. Consulted by the channel router on every dispatch unless
  the template is flagged as critical (e.g. `payment.failed`).

**PostgreSQL is the default store** for simpler local development and full transactional
guarantees on the four tables. **Cassandra is operator-selectable** as an alternative for
very-high-volume deployments where the write-only access pattern of `notification_log` and
`delivery_attempts` benefits from horizontal write scaling and tunable consistency.

## Events Consumed

The Notification Service subscribes to seven domain topics across the `user.*`, `order.*`, and
`payment.*` namespaces. Each event triggers a deterministic resolution into one or more channel
dispatches.

| Event | Default Channel(s) | Template Family |
|-------|--------------------|-----------------|
| `user.registered` | Email (welcome) + SMS (verification code) | `welcome.email.v1`, `verification.sms.v1` |
| `order.created` | Email (order confirmation) | `order-created.email.v1` |
| `order.cancelled` | Email + SMS | `order-cancelled.email.v1`, `order-cancelled.sms.v1` |
| `order.fulfilled` | Email (shipping updates) | `order-fulfilled.email.v1` |
| `payment.succeeded` | Email (receipt) | `payment-receipt.email.v1` |
| `payment.failed` | Email + SMS (failure alert) | `payment-failed.email.v1`, `payment-failed.sms.v1` |
| `payment.refunded` | Email | `refund.email.v1` |

**Channel routing rule (AAP R-11):** the default channel(s) above are overridden by
`user_channel_prefs` — a user who has opted out of SMS marketing will not receive SMS for
optional notifications, but may still receive critical-path SMS (e.g. `payment.failed`)
depending on template metadata. Selection is performed by `ChannelRouter` from the tuple
`(event_type, user_prefs, template_metadata)` and is never hard-coded per event.

**This service produces NO Kafka events.** It is a terminal consumer per the service folder
spec — every other service in the monorepo emits domain events for downstream consumption, but
the Notification Service only reads. This asymmetry is intentional: a notification is the
end-of-pipeline action triggered by a domain change, never a domain change in its own right.

## HTTP API

Admin endpoints are fronted by the API Gateway at `/notifications/*` and require a JWT with the
`notifications:admin` scope (see [Security](#security) below). The full OpenAPI specification
lives at [../../docs/api/notification-service.md](../../docs/api/notification-service.md).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/notifications/templates` | List templates (paginated, filterable by `channel`, `locale`, `version`). |
| `GET` | `/notifications/templates/{id}` | Retrieve a single template by ID and (optionally) version. |
| `POST` | `/notifications/templates` | Create a new template. Requires the `notifications:admin` scope. |
| `PUT` | `/notifications/templates/{id}` | Update a template; creates a new version row. Prior versions remain queryable for reproducibility. |
| `GET` | `/notifications/users/{userId}/prefs` | Read a user's channel preferences. |
| `PUT` | `/notifications/users/{userId}/prefs` | Update a user's channel preferences. |
| `GET` | `/notifications/log?userId={uuid}&channel={email\|sms}&limit={n}` | Query the per-user delivery log with optional channel filter and limit. |

Standard liveness and readiness probes (per AAP R-19) are also exposed:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health/live` | Liveness probe — process-alive only. |
| `GET` | `/health/ready` | Readiness probe — verifies all critical dependencies (Kafka, PostgreSQL, Email provider, SMS provider) are reachable. |

## Adapter Pattern — `NotificationChannel`

The service implements the **adapter / strategy pattern** for pluggable notification delivery
(per AAP R-11). Both Email and SMS are integrated concurrently behind a single
`NotificationChannel` interface so that a new channel (or a new provider for an existing
channel) can be added without touching the routing or scheduling code paths.

```
NotificationChannel (interface)
  ├── EmailChannel (SendGrid / AWS SES)
  └── SmsChannel   (Twilio / AWS SNS)
```

Each channel implements the same simplified contract:

- `async def send(message: RenderedMessage, correlation_id: str) -> DeliveryResult`
- `async def health_check() -> ChannelHealth`
- `name: str` — property returning the channel identifier used in logs and metrics.

Channel selection is performed by `ChannelRouter` from the tuple
`(event_type, user_prefs, template_metadata)` — **never hard-coded** per AAP R-11. The router
consults `user_channel_prefs` on every dispatch and respects opt-outs unless the template is
flagged critical (in which case the override behavior is governed by template metadata, not by
the router itself). Provider selection within a channel (SendGrid vs AWS SES; Twilio vs AWS SNS)
is configuration-driven — see [Configuration](#configuration).

**This service's adapter layer mirrors Payment Service's `PaymentProvider` abstraction and
should be consulted as the canonical adapter-pattern reference for future multi-provider
services.** The two services together (Notification + Payment) are the templates of pluggability
in this codebase.

## Retry & Fallback Policy

This service follows the platform-wide retry, circuit-breaker, and dead-letter conventions
defined in [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md).

- **Outbound HTTP retries (AAP R-15).** Every provider call is wrapped in an exponential-backoff
  retry policy with jitter, a maximum of 5 attempts, and a 30-second total budget per attempt
  chain. Idempotent operations are safe to retry; non-idempotent operations are guarded by
  provider-side idempotency keys where the provider supports them.
- **Circuit breaker (AAP R-16).** Each provider has its own breaker that opens at ≥50% failure
  rate over a 20-call rolling window, remains open for 30 seconds, and transitions to half-open
  on a single probe call before deciding closed-vs-open. The breaker state per provider is
  exposed as the `provider_circuit_breaker_state` metric.
- **Kafka consumer retries (AAP R-17).** On processing failure, messages route to
  `<topic>.retry`; after exhausting the configured retry budget, they route to `<topic>.dlq`.
  The retry/DLQ suffix mapping is governed by `KAFKA_RETRY_TOPIC_SUFFIX` and
  `KAFKA_DLQ_TOPIC_SUFFIX` (see `.env.example`).
- **Dead-letter topics.** Three classes of DLQ topics are produced to:
  - `notifications.email.dlq` — emails that could not be delivered after all provider retries.
  - `notifications.sms.dlq` — SMS that could not be delivered after all provider retries.
  - `<consumed-topic>.dlq` — domain events that could not be processed at all (poison messages).
- **Degraded mode (AAP R-20).** If the Email provider's circuit breaker is open, SMS-eligible
  notifications still flow; users are not blocked on a single provider failure. Conversely, an
  open SMS breaker does not block email-eligible notifications. The router treats the two
  channels as independently available subsystems.

## Running Locally

```bash
# From the service directory: services/notification-service/
cp .env.example .env
# Fill in or accept defaults for: POSTGRES_URL, KAFKA_BOOTSTRAP, SENDGRID_API_KEY,
# TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN
# (Local dev can run against provider sandboxes or the in-repo mock providers.)

# Install dependencies (Python 3.11+)
python -m venv .venv
source .venv/bin/activate          # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Apply database migrations
# NOTE: `migrations/` contains the DDL; apply with psql or the container's entrypoint.

# Run the service
uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload
```

The repository-root [`docker-compose.yml`](../../docker-compose.yml) spins up this service plus
PostgreSQL, Kafka, mock Email/SMS providers, and all dependencies for full-stack local
development. Prefer the root compose stack for any flow that exercises end-to-end behavior;
prefer the in-directory workflow above for fast inner-loop iteration on this service alone.

## Testing

Per the platform test layout in AAP Section 0.5.2.6, tests are split into unit and integration
suites under this service:

- **Unit tests** — `tests/unit/` — channel routing logic, template rendering, retry scheduler
  math, and adapter contract conformance for both `EmailChannel` and `SmsChannel`.
- **Integration tests** — `tests/integration/` — full pipeline with Testcontainers
  (Kafka + PostgreSQL) and mocked provider HTTP endpoints (`respx`) covering happy-path
  dispatch, retry exhaustion into DLQ, and circuit-breaker open/half-open/closed transitions.

```bash
pytest tests/unit -v
pytest tests/integration -v --maxfail=1
```

End-to-end checkout flows that span the Notification Service and other services live under
`tests/e2e/` at the repository root.

## Configuration

Configuration is loaded from `config/default.yaml`, layered with environment-specific overrides,
and finalized by environment variables read at startup. See `config/default.yaml` and
`.env.example` in this directory for the complete configuration surface.

The following environment variables are required at startup. Per AAP R-25, **no secret values
are ever stored in this repository** — `.env.example` lists names only and concrete values are
supplied at deploy time via Kubernetes Secrets or a secret manager.

- `POSTGRES_URL` — connection string for the `notification_db` PostgreSQL database.
- `KAFKA_BOOTSTRAP` — comma-separated list of Kafka bootstrap brokers.
- `SCHEMA_REGISTRY_URL` — URL of the Confluent Schema Registry for event schema validation
  (per AAP R-14).
- `SENDGRID_API_KEY` (or `AWS_SES_*`) — credentials for the active email provider, selected by
  `EMAIL_PROVIDER`. Populate only the credentials block for the active provider.
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN` (or `AWS_SNS_*`) — credentials for the active SMS
  provider, selected by `SMS_PROVIDER`. Populate only the credentials block for the active
  provider.
- `JWT_PUBLIC_KEY_URL` — JWKS endpoint exposed by the Auth Service for JWT signature
  verification (per AAP R-22).
- `LOG_LEVEL` — log verbosity (`DEBUG`, `INFO`, `WARN`, `ERROR`).

The service fails fast at startup if any required variable is missing or any critical
dependency is unreachable.

## Observability

Logs are emitted as structured JSON to stdout and shipped by **Filebeat** to **Logstash**, which
forwards them to **Elasticsearch** for indexing and **Kibana** for visualization (per AAP R-27).
A pre-built dashboard for this service surfaces request volume, p95 latency, error rate, Kafka
consumer lag per topic, DLQ depth per channel, and per-provider circuit-breaker state.

Every log line includes the following structured fields (per AAP R-26 plus service-specific
fields for the dispatch pipeline):

| Field | Description |
|-------|-------------|
| `timestamp` | RFC 3339 timestamp of the log event. |
| `level` | Log level (`DEBUG`, `INFO`, `WARN`, `ERROR`). |
| `service` | Always `notification-service`. |
| `correlation_id` | Propagated from the API Gateway via the `X-Correlation-ID` header and Kafka message headers. |
| `user_id` | Recipient user ID (when known). |
| `event_type` | Source domain event (e.g. `order.created`). |
| `channel` | Resolved channel (`email` or `sms`). |
| `template_id` | Template ID + version used to render the message. |
| `provider` | Active provider that handled the dispatch (e.g. `sendgrid`, `twilio`). |
| `attempt_no` | Attempt number within the per-message retry chain. |
| `status` | Dispatch outcome (`success`, `bounce`, `throttled`, `failed`). |
| `latency_ms` | Wall-clock latency of the provider call in milliseconds. |
| `message` | Human-readable log message. |

Key metrics exposed at `/metrics`:

- `notifications_sent_total{channel,provider,status}` — counter of dispatch outcomes.
- `notifications_delivery_latency_ms{channel,provider}` — histogram of provider call latency.
- `notifications_dlq_depth{channel}` — gauge of pending messages in each per-channel DLQ.
- `kafka_consumer_lag{topic}` — gauge of consumer lag per consumed topic.
- `provider_circuit_breaker_state{provider}` — gauge of per-provider breaker state
  (`0`=closed, `1`=half-open, `2`=open).

## Security

- **JWT validation only (AAP R-21).** Admin endpoints require a JWT with the
  `notifications:admin` scope. JWTs are issued exclusively by the Auth Service; this service
  is **never** an issuer and only validates signatures.
- **JWKS caching (AAP R-22).** Public keys are fetched from `JWT_PUBLIC_KEY_URL` and cached
  with a bounded TTL (`JWT_JWKS_CACHE_TTL_SECONDS`, default 1 hour) so key rotation propagates
  without service restart.
- **TLS in transit (AAP R-24).** All inter-service traffic — including Kafka, PostgreSQL, JWKS
  fetches, and outbound provider calls — is TLS-encrypted. Plaintext HTTP is permitted only on
  loopback within a pod for sidecar communication.
- **Secrets handling (AAP R-25).** Provider API keys, database credentials, and any value
  pointing at a production endpoint come exclusively from Kubernetes Secrets, HashiCorp Vault,
  or cloud-provider secret managers. They are **never** committed to source or to
  `.env.example`; `.env.example` lists variable names only.

## Related Documentation

- [../../docs/architecture/system-diagram.md](../../docs/architecture/system-diagram.md) — Complete system topology
- [../../docs/architecture/service-catalog.md](../../docs/architecture/service-catalog.md) — All services at a glance
- [../../docs/architecture/event-catalog.md](../../docs/architecture/event-catalog.md) — Full Kafka topic catalog
- [../../docs/architecture/resilience-patterns.md](../../docs/architecture/resilience-patterns.md) — Retry/CB/DLQ policies
- [../../docs/architecture/data-stores.md](../../docs/architecture/data-stores.md) — DB-per-service rationale
- [../../docs/api/notification-service.md](../../docs/api/notification-service.md) — OpenAPI specification
