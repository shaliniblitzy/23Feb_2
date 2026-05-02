"""Hermetic unit tests for resilience patterns used by the Product Service.

This sub-package contains fast, hermetic unit tests for the cross-cutting
resilience helpers that protect every outbound interaction the Product
Service performs (Auth Service introspection over HTTP, Kafka producer
publishes, cache writes, and any other external boundary):

- ``test_retry_policy.py`` — AAP R-15: exponential backoff with bounded
  jitter, configurable initial interval and multiplier, max-attempt cap,
  retryable-vs-non-retryable exception classification, and idempotent-method
  gating (GET/HEAD/OPTIONS/PUT/DELETE auto-retry; POST/PATCH only with an
  ``Idempotency-Key`` header).
- ``test_circuit_breaker.py`` — AAP R-16: per-dependency ``pybreaker``
  instances with failure-rate threshold, open-state duration, single-probe
  HALF-OPEN admission, and CLOSED/OPEN/HALF-OPEN state-transition semantics.

Every test in this package MUST be hermetic — no real databases, no real
Kafka brokers, no real network calls. All external boundaries are mocked
via ``unittest.mock`` (or the in-memory stubs in
:mod:`tests.unit.conftest`). Time-related assertions either use
``freezegun.freeze_time`` (for tenacity-based retry tests where retry uses
the standard ``time`` clock) OR a tiny ``open_duration_ms`` plus a real
``await asyncio.sleep`` (for ``pybreaker``-based circuit-breaker tests
where freezegun cannot patch ``time.monotonic``).

See :mod:`tests.unit.conftest` for the shared resilience fixtures
(``passthrough_retry_policy``, ``closed_breaker``, ``open_breaker``,
``half_open_breaker``) available to every test module in this package.

Integration tests that exercise the resilience helpers against real
broker/HTTP boundaries live under
``services/product-service/tests/integration/resilience/`` and are out of
scope for this sub-package.
"""
