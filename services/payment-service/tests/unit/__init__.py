"""Hermetic unit tests for the Payment Service (AAP Section 0.5.2.6).

This package contains unit-test modules that are fast, hermetic, and
have NO network or container dependencies. Coverage is organized by
the system under test:

* ``test_provider_routing.py`` — ``ProviderRouter`` data-driven
  selection (currency, region, merchant override; AAP R-10).
* ``test_stripe_provider.py`` — ``StripeProvider`` adapter contract
  (charge, refund, webhook parsing; 9-element event mapping;
  100+ supported currencies).
* ``test_razorpay_provider.py`` — ``RazorpayProvider`` adapter
  contract (charge, refund, webhook parsing; 5-element event
  mapping; INR + USD supported currencies).
* ``test_webhook_signature_verification.py`` — HMAC-SHA256
  verification for Stripe (``Stripe-Signature: t=...,v1=...``) and
  Razorpay (``X-Razorpay-Signature``) with 300s tolerance and
  constant-time comparison (AAP R-12, R-25).
* ``test_idempotency_store.py`` — ``IdempotencyStore`` cache /
  conflict / new-key semantics with TTL enforcement (AAP R-8).
* ``test_retry_backoff.py`` — ``tenacity`` exponential backoff
  math, max-attempts cap, retryable status codes (AAP R-15).
* ``test_circuit_breaker.py`` — ``pybreaker`` state transitions
  (CLOSED ⇄ OPEN ⇄ HALF_OPEN) with structured-log emission on
  state change (AAP R-16, R-26).
* ``test_provider_fallback_routing.py`` — cross-provider failover
  when the primary's circuit breaker is OPEN (AAP R-20).
* ``test_envelope_encryption.py`` — column-level envelope
  encryption (AES-256-GCM); LocalBackend, AwsKmsBackend (moto),
  GcpKmsBackend (mocked), VaultTransitBackend (mocked); AAD
  binding for the five PCI-relevant columns ``last4``, ``brand``,
  ``holder_name``, ``provider_charge_id``, ``provider_refund_id``
  (AAP R-8).
* ``test_event_serialization.py`` — ``payment.succeeded``,
  ``payment.failed``, ``payment.refunded`` topic routing, partition
  key (``str(payment.order_id)``), required headers
  (``x-correlation-id``, ``x-event-type``, ``x-event-version``,
  ``x-producer``), Decimal/UUID/datetime encoding (AAP R-14, R-30,
  R-31, R-33).

Hermeticity contract
--------------------

Tests in this package MUST:

* NEVER import ``testcontainers`` (those imports are forbidden
  outside ``tests/integration/conftest.py``).
* NEVER open real network connections (use ``respx`` for outbound
  HTTPX or assert no HTTP call was made).
* NEVER require Docker / Postgres / Kafka to be running.
* Run in under 5 seconds total for the full unit tier locally.
* Be deterministic: ``freezegun`` (or fake clocks) for time;
  ``Faker.seed()`` (or fixed seeds) for RNG; no race conditions.

Shared fixtures
---------------

The unit tier's ``conftest.py`` provides shared fixtures including
``mock_kms`` (moto-backed KMS), ``frozen_time`` (freezegun helper),
``stripe_webhook_signed`` / ``razorpay_webhook_signed`` (signed-body
factories), ``payment_intent_factory``, ``charge_result_factory``,
``provider_event_factory``, and ``payment_succeeded_payload``.

Fixtures from the root ``tests/conftest.py`` (``faker_instance``,
``correlation_id``, ``structlog_test_capture``, ``captured_logs``,
``settings_factory``, ``assert_required_log_fields``) are also
available through pytest's conftest hierarchy.

References
----------

* AAP Section 0.5.2.6 — declares this subfolder in scope.
* AAP Section 0.6.1 — listed as in-scope path.
* AAP Rules R-8 (idempotency + encryption), R-10 (dual provider),
  R-12 (webhook HMAC), R-13 (correlation ID), R-14 (Schema Registry
  validation), R-15 (retry), R-16 (circuit breaker), R-20 (fallback),
  R-25 (no secrets), R-26 (structured logs), R-30 (event naming).
"""
