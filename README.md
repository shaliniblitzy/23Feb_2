# E-Commerce Microservices Platform

A production-grade, event-driven microservices architecture for e-commerce, organized around an
**API Gateway** edge, **OAuth 2.0 + JWT** identity, an **Apache Kafka** asynchronous messaging
backbone, **polyglot persistence** (PostgreSQL, MongoDB, vector store, Redis), the **ELK stack**
for observability, and **dual payment** (Stripe + Razorpay) and **dual notification** (Email + SMS)
providers. The repository is a monorepo of independently deployable services; the canonical source
of truth for the topology is the complex Mermaid diagram referenced below.

> **📐 Canonical architecture diagram:** [docs/architecture/system-diagram.md](docs/architecture/system-diagram.md) — the complex Mermaid diagram is the primary deliverable of this project and the source of truth for every integration.

---

## Architecture

The platform is partitioned into eight logical domains that map one-to-one onto the subgraphs in
the canonical Mermaid diagram:

- **Client Tier** — Web (browser SPA) and Mobile (iOS/Android) consumers of the public API.
- **Edge & API Gateway** — TLS termination, routing, rate limiting, and authentication delegation.
- **Identity Domain** — Auth Service issuing and validating OAuth 2.0 / JWT tokens.
- **Commerce Core** — User, Product, Inventory, Order, and Payment services owning the
  transactional surface area of the storefront.
- **Intelligence Domain** — Recommendation Engine performing ML inference with a graceful
  popularity-based fallback.
- **Notification Domain** — Multi-channel dispatcher fanning events out to email and SMS.
- **Messaging Backbone** — Apache Kafka with Schema Registry, retry topics, and dead-letter topics.
- **Observability** — ELK stack (Elasticsearch + Logstash + Kibana) fed by Filebeat and Metricbeat.
- **External Integrations** — Stripe, Razorpay, an email provider, and an SMS provider.

The eleven first-class components rendered in the canonical diagram are:

1. **API Gateway** — single edge entry point handling routing, authentication delegation, rate
   limiting, and protocol translation for all client traffic.
2. **Auth Service** — identity and access management built on OAuth 2.0 authorization flows and
   JWT-based session tokens.
3. **User Service** — user profile, account, address, and preference management.
4. **Product Service** — product catalog, pricing, and category management.
5. **Inventory Service** — stock tracking, reservation, and warehouse state management.
6. **Order Service** — order lifecycle orchestration, state transitions, and the saga coordinator
   for the checkout flow.
7. **Payment Service** — dual-provider payment processing with **Stripe** (global markets) and
   **Razorpay** (India-focused) adapters behind a unified provider interface.
8. **Notification Service** — multi-channel dispatcher delivering both **Email** and **SMS**
   notifications behind a unified channel interface.
9. **Recommendation Engine** — **ML-based** personalized product recommendations with a
   popularity-based fallback when the inference path is unavailable.
10. **Logging & Monitoring** — **ELK stack** (Elasticsearch + Logstash + Kibana) plus Filebeat and
    Metricbeat shippers for centralized observability.
11. **Message Queue** — **Apache Kafka** as the event streaming backbone for asynchronous
    inter-service communication, paired with a Schema Registry for contract validation.

---

## Documentation

Architecture documentation lives under [docs/architecture/](docs/architecture/) and is the
authoritative reference for every cross-cutting concern in the platform.

| Document | Purpose |
|----------|---------|
| [docs/architecture/system-diagram.md](docs/architecture/system-diagram.md) | Complex Mermaid system topology (primary deliverable) |
| [docs/architecture/service-catalog.md](docs/architecture/service-catalog.md) | Per-service responsibilities, public APIs, and owned topics |
| [docs/architecture/event-catalog.md](docs/architecture/event-catalog.md) | Kafka topics, event schemas, and producer/consumer matrix |
| [docs/architecture/resilience-patterns.md](docs/architecture/resilience-patterns.md) | Retry, circuit-breaker, DLQ, and saga compensation policies |
| [docs/architecture/data-stores.md](docs/architecture/data-stores.md) | Database-per-service rationale and polyglot persistence choices |

Additional references:

- [docs/onboarding.md](docs/onboarding.md) — Developer onboarding walkthrough.
- [docs/runbook/](docs/runbook/) — Per-service operational runbooks.
- [docs/api/](docs/api/) — Per-service OpenAPI specifications and consumer guides.

---

## Repository Layout

```
.
├── README.md                      # This file
├── docker-compose.yml             # One-command local orchestration
├── .env.example                   # Non-secret env variable template
├── services/                      # 9 microservices (one folder per service)
│   ├── api-gateway/
│   ├── auth-service/
│   ├── user-service/
│   ├── product-service/
│   ├── inventory-service/
│   ├── order-service/
│   ├── payment-service/
│   ├── notification-service/
│   └── recommendation-engine/
├── infrastructure/                # Shared platform infra
│   ├── kafka/                     # Brokers, Schema Registry, topics.yaml
│   └── elk/                       # Elasticsearch, Logstash, Kibana, Beats
├── docs/                          # Architecture + runbooks + API docs
│   ├── architecture/
│   ├── runbook/
│   ├── api/
│   └── onboarding.md
├── deploy/                        # Kubernetes manifests
│   └── k8s/
├── tests/                         # E2E + contract tests (cross-service)
│   ├── e2e/
│   └── contract/
└── .github/workflows/             # CI/CD pipelines
    ├── ci.yml
    ├── cd.yml
    └── diagram-validate.yml
```

---

## Quickstart

The repository is designed for one-command local orchestration via Docker Compose. The root
[docker-compose.yml](docker-compose.yml) wires up every service, the Kafka cluster (brokers and
Schema Registry), the ELK stack, and per-service databases.

```bash
# 1) Copy env template and fill in provider keys as needed
cp .env.example .env

# 2) Launch all services, Kafka (+ Schema Registry), ELK, and per-service databases
docker-compose up -d

# 3) Verify the stack is healthy
docker-compose ps

# 4) Open Kibana dashboards
# → http://localhost:5601
```

Detailed steps — language-specific builds, running individual services in isolation, executing the
full test suites, and provisioning Kubernetes — live in [docs/onboarding.md](docs/onboarding.md)
and in each service's own `README.md` under [services/](services/).

---

## Services

Every service owns a private database, exposes a documented HTTP surface for synchronous calls,
and produces or consumes events on the Kafka backbone. Database-per-service isolation is strictly
enforced — see [docs/architecture/data-stores.md](docs/architecture/data-stores.md).

| Service | Purpose | Database |
|---------|---------|----------|
| [api-gateway](services/api-gateway/) | Single edge entry point; routing, auth delegation, rate limiting, TLS | — |
| [auth-service](services/auth-service/) | OAuth 2.0 authorization flows + JWT issuance and validation | PostgreSQL `auth_db` |
| [user-service](services/user-service/) | User profiles, preferences, and addresses | PostgreSQL `user_db` |
| [product-service](services/product-service/) | Product catalog, categories, and media metadata | MongoDB `product_db` |
| [inventory-service](services/inventory-service/) | Stock reservations and warehouse state management | PostgreSQL `inventory_db` |
| [order-service](services/order-service/) | Order lifecycle orchestration and saga coordinator | PostgreSQL `order_db` |
| [payment-service](services/payment-service/) | Stripe + Razorpay adapters and webhook handlers | PostgreSQL `payment_db` (encrypted) |
| [notification-service](services/notification-service/) | Multi-channel dispatcher for Email + SMS | PostgreSQL/Cassandra `notification_db` |
| [recommendation-engine](services/recommendation-engine/) | ML-based recommendations with popularity fallback | Vector store + Redis cache |

---

## Messaging & Observability

**Messaging backbone — Apache Kafka.** All asynchronous inter-service communication flows through
Kafka. Every domain event is governed by a versioned schema registered with the Schema Registry,
which enforces backward compatibility on producers and consumers. Each event topic ships with a
companion `<topic>.retry` topic for bounded retry attempts and a `<topic>.dlq` dead-letter topic
for poison-message isolation. Topic definitions, partition counts, and retention settings are
declarative; see [infrastructure/kafka/](infrastructure/kafka/) for the broker, Schema Registry,
and topic manifests, and [docs/architecture/event-catalog.md](docs/architecture/event-catalog.md)
for the event catalog and producer/consumer matrix.

**Observability — ELK stack.** Filebeat and Metricbeat ship structured JSON logs and host/container
metrics to Logstash, which transforms and forwards them to Elasticsearch. Kibana hosts the
dashboards that render request volume, p95 latency, error rates, Kafka consumer lag, payment
success rate, and DLQ depth across the platform. Index Lifecycle Management policies bound storage
cost across hot, warm, cold, and delete phases. Configuration manifests, pipelines, and dashboards
live under [infrastructure/elk/](infrastructure/elk/); resilience-aware logging and metric
conventions are documented in
[docs/architecture/resilience-patterns.md](docs/architecture/resilience-patterns.md).

---

## Resilience & Security

Every external call and inter-service hop is protected by **retries with exponential backoff and
jitter**, a **circuit breaker** with configurable failure-rate and open-state thresholds, and a
declared **fallback path** (cached response, degraded-mode response, or fail-closed behavior for
security-critical paths). Kafka consumers route repeatedly failing messages to **retry topics** and
ultimately to **dead-letter queues**. The Order Service implements the **saga pattern** with
explicit compensation steps for inventory release and payment reversal. Identity is anchored in
**OAuth 2.0** (authorization code with PKCE for public clients, client credentials for
machine-to-machine) and **JWT** tokens issued exclusively by the Auth Service and validated via
JWKS. **TLS** is mandatory for all inter-service traffic, and **secrets** are sourced exclusively
from Kubernetes Secrets, HashiCorp Vault, or cloud-provider secret managers — never from source or
configuration files. The complete policy matrix lives in
[docs/architecture/resilience-patterns.md](docs/architecture/resilience-patterns.md).

---

## Testing

The platform follows a layered test pyramid. Each service owns its inner layers; cross-service
suites live at the repository root.

- **Unit tests** — `services/*/tests/unit/` — pure domain logic, validators, and adapter
  registries.
- **Integration tests** — `services/*/tests/integration/` — service plus its database plus Kafka,
  driven through Testcontainers or equivalent ephemeral infrastructure.
- **Contract tests** — [tests/contract/](tests/contract/) — consumer-driven contract suites
  validating Kafka event schemas across producers and consumers.
- **End-to-end tests** — [tests/e2e/](tests/e2e/) — cross-service flows including
  `checkout-flow`, `provider-failover` (Stripe and Razorpay), and `resilience` (retry exhaustion,
  DLQ routing, saga compensation).

CI executes the full pyramid on every pull request via the workflows under
[.github/workflows/](.github/workflows/).

---

## Contributing

Set up a local development environment by following [docs/onboarding.md](docs/onboarding.md). The
CI/CD pipelines under [.github/workflows/](.github/workflows/) document the lint, test, build, and
deploy stages every change must pass. Contributions to the canonical architecture diagram at
[docs/architecture/system-diagram.md](docs/architecture/system-diagram.md) are validated for
Mermaid syntax by the `diagram-validate.yml` workflow on every pull request that touches it.
