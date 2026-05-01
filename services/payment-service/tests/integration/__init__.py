"""Integration tests for the Payment Service.

This is the integration test package for the Payment Service. Modules in this package exercise
the service's production code paths against REAL infrastructure dependencies brought up via
``testcontainers``: a real PostgreSQL 16 instance for the ``payment_db`` schema and a real
Apache Kafka broker for the ``payment.*`` event topics, their ``*.retry`` siblings, and their
``*.dlq`` terminals. Postgres and Kafka are NEVER mocked at this tier — they are live processes
exposed on ephemeral ports by the Testcontainers Python SDK.

Outbound HTTP calls to the Stripe and Razorpay payment provider APIs are stubbed at the HTTP
transport layer using ``respx``. This keeps the suite hermetic against the public internet while
still exercising the full ``httpx`` request lifecycle (timeouts, retries, circuit-breaker state
transitions, signature parsing on inbound webhooks). When envelope encryption tests need a KMS
backend, AWS KMS calls are stubbed via ``moto[kms]`` so that no real AWS account is contacted
and no real key material leaves the process.

Cross-service end-to-end tests (e.g., the full checkout flow that spans the Order Service, the
Payment Service, the Notification Service, and the Inventory Service) live at the repository
root under ``tests/e2e/`` — NOT in this package. This package stops at the Payment Service's
own boundary; tests that require multiple services to be running concurrently belong in the
repository-level e2e suite per AAP Section 0.5.2.6.

Tests requiring no real Postgres or Kafka belong in ``services/payment-service/tests/unit/``.
The unit tier is faster, fully hermetic, and runs without Docker — use it for any logic that
can be exercised without container orchestration. Move tests from integration to unit whenever
a real container dependency can be removed without losing coverage.

Webhook signature-verification UNIT tests (the HMAC math itself, exercised without HTTP routing
or persistence) belong in ``tests/unit/test_webhook_signature_verification.py``. Webhook
INBOUND HTTP tests (full FastAPI routing, request body parsing, persistence to the
``provider_webhooks`` table, and downstream Kafka emission of ``payment.*`` events) belong here
in ``test_webhook_inbound_stripe.py`` and ``test_webhook_inbound_razorpay.py``. Keeping the
two layers separate guards against bloated tests that conflate signature math with HTTP
plumbing.

Module roster:

- ``conftest.py`` — Testcontainer + respx fixtures (sole owner of testcontainers imports).
- ``test_charge_flow.py`` — order.created -> charge -> payment.succeeded.
- ``test_refund_flow.py`` — order.cancelled -> refund -> payment.refunded.
- ``test_provider_failover.py`` — Stripe outage -> fallback to Razorpay -> payment.succeeded
  with ``fallback_used=true``.
- ``test_webhook_inbound_stripe.py`` — POST /webhooks/stripe with valid + invalid HMAC;
  idempotent re-delivery.
- ``test_webhook_inbound_razorpay.py`` — POST /webhooks/razorpay with valid + invalid HMAC;
  idempotent re-delivery.
- ``test_idempotency_inbound.py`` — Idempotency-Key header replay (matching + mismatching
  request hash).
- ``test_dlq_routing.py`` — Poison-message routing to payment.dlq + per-topic .dlq.
- ``test_health_endpoints.py`` — /health/live + /health/ready including provider reachability.

This package realizes the in-scope path declared in AAP Sections 0.5.2.6 and 0.6.1, validating
production code paths from AAP Section 0.4.2 (the Payment Service producer/consumer matrix and
its Stripe + Razorpay external integrations). The suite collectively asserts the following AAP
rules: R-8 (encryption at rest + idempotency keys for safe retries), R-10 (concurrent dual-
provider integration behind a unified ``PaymentProvider`` interface), R-12 (provider webhook
signature verification + idempotent re-delivery), R-13 (correlation-ID propagation across HTTP
calls and Kafka headers), R-14 (Schema Registry validation of every produced event), R-17
(retry topics + dead-letter topics on consumer failure), R-19 (liveness + readiness probes
with fail-fast on missing critical dependencies), R-20 (declared fallback behavior on every
external dependency), R-30 (events owned by the producing domain and named ``<domain>.<verb>``),
and R-31 (event payloads carry a version field to enable backward-compatible evolution).

This package mirrors the structural minimalism of ``services/notification-service/tests/
integration/`` and ``services/recommendation-engine/tests/integration/`` for monorepo
consistency: a docstring-only ``__init__.py`` that documents scope, boundary, and module
roster without importing anything. Sibling parity is intentional — every service's integration
test package looks the same so contributors can navigate the monorepo without surprises.

Run command::

    pytest services/payment-service/tests/integration/ -v
"""
