"""Test suites for the Payment Service.

This package contains the unit and integration test trees for the
Payment Service (a dual-provider payment processing microservice
integrating both Stripe and Razorpay behind a unified
``PaymentProvider`` adapter abstraction, per AAP R-10).

Layout
------

::

    services/payment-service/tests/
    ├── __init__.py        (this file: package marker only)
    ├── conftest.py        (top-level fixtures shared by both tiers)
    ├── unit/              (hermetic, mocked tests)
    │   ├── __init__.py
    │   ├── conftest.py
    │   ├── test_provider_routing.py
    │   ├── test_stripe_provider.py
    │   ├── test_razorpay_provider.py
    │   ├── test_webhook_signature_verification.py
    │   ├── test_idempotency_store.py
    │   ├── test_retry_backoff.py
    │   ├── test_circuit_breaker.py
    │   ├── test_provider_fallback_routing.py
    │   ├── test_envelope_encryption.py
    │   └── test_event_serialization.py
    └── integration/       (Testcontainer-backed tests)
        ├── __init__.py
        ├── conftest.py
        ├── test_charge_flow.py
        ├── test_refund_flow.py
        ├── test_provider_failover.py
        ├── test_webhook_inbound_stripe.py
        ├── test_webhook_inbound_razorpay.py
        ├── test_idempotency_inbound.py
        ├── test_dlq_routing.py
        └── test_health_endpoints.py

Test tier responsibilities
--------------------------

* ``tests.unit`` — fast, hermetic, no network or container
  dependencies. Covers provider-routing logic (currency, region,
  merchant override), webhook signature verification (AAP R-12),
  the idempotency key store (AAP R-8), retry / backoff math,
  circuit-breaker state transitions, envelope encryption helpers,
  and ``payment.*`` event serialization.

* ``tests.integration`` — exercises the full service against a real
  Postgres + Kafka via Testcontainers, with provider HTTP endpoints
  (Stripe, Razorpay) stubbed via ``respx``. Validates the
  end-to-end flows declared in AAP Section 0.4.2: ``order.created``
  → charge via routed provider → ``payment.succeeded`` emitted;
  ``order.cancelled`` → refund via originating provider →
  ``payment.refunded`` emitted; webhook receivers translate
  HMAC-verified events to ``payment.*`` events; cross-provider
  failover when the primary's circuit breaker is open (AAP R-20);
  DLQ routing on persistent failures (AAP R-17); idempotent
  request handling (AAP R-8).

Shared fixtures
---------------

Fixtures consumed by BOTH tiers (Faker, correlation IDs, structlog
log capture, ``settings_factory``) live in this folder's
``conftest.py``. That module is intentionally **service-init-free**:
no Testcontainer wiring, no DI container construction, no respx
routing. Per-tier fixtures (Testcontainers, ``respx`` routes,
moto KMS context, FastAPI ``TestClient``) live in the respective
subpackage's ``conftest.py``.

Repository-level end-to-end tests that span MULTIPLE services
(e.g., ``tests/e2e/provider-failover.spec.*`` per AAP Section
0.5.2.6) live at the repository root, not here. This package
stops at the Payment Service's boundary.

AAP cross-references
--------------------

* AAP Section 0.5.2.6 — declares ``services/*/tests/unit/**/*``
  and ``services/*/tests/integration/**/*`` in scope.
* AAP Section 0.6.1 — lists this folder's subtrees as in-scope.
* AAP Rules R-8 (encryption + idempotency), R-10 (dual-provider
  concurrent integration), R-12 (webhook HMAC verification),
  R-13 (correlation ID propagation), R-15 (retry with backoff),
  R-16 (circuit breaker), R-17 (retry topics + DLQ), R-19
  (fail-fast on missing deps), R-20 (provider fallback on
  circuit-open), R-22 (JWT JWKS validation with TTL caching),
  R-25 (no secrets in source/config), R-26 (structured JSON logs).
"""
