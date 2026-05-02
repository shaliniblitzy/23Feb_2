"""Unit tests for the :mod:`src.warehouse` package.

This subfolder is the canonical home for **hermetic unit tests of the
WarehouseAdapter strategy pattern** — the architectural keystone that
abstracts over in-process database-backed reservation logic and any
future external WMS integration. The Inventory Service's
``WarehouseAdapter`` is the inventory-side analogue of the Payment
Service's ``PaymentProvider`` (Stripe/Razorpay) and the Notification
Service's ``NotificationChannel`` (Email/SMS) abstractions.

Test Modules
------------

- ``test_warehouse_adapter_contract.py``
    Cross-cutting contract tests every concrete ``WarehouseAdapter``
    must satisfy: Protocol structural conformance
    (``isinstance(adapter, WarehouseAdapter)``), 5-method async surface,
    co-located frozen dataclass invariants, validation in
    ``ReservationItemRequest.__post_init__`` and
    ``ReservationResult.__post_init__``, ``AdapterHealthStatus`` StrEnum
    semantics, and ``ExternalWMSWarehouseAdapter`` stub semantics
    (``reserve``/``release``/``finalize`` raise ``NotImplementedError``;
    ``health_check`` performs a real HTTP probe mocked via ``respx``).

- ``test_database_warehouse_adapter.py``
    Orchestration logic for ``DatabaseWarehouseAdapter`` — THE
    ARCHITECTURAL KEYSTONE class. Covers all 5 async methods
    (``reserve``, ``release``, ``finalize``, ``get_stock``,
    ``health_check``) across happy-path, idempotent replay,
    insufficient-stock, warehouse pre-flight, validation, exception
    passthrough, NOOP terminal-state, and structured-logging branches.

- ``test_warehouse_adapter_registry.py``
    ``WarehouseAdapterRegistry`` resolution logic: registration,
    replacement (with WARNING log), get / get_for_warehouse /
    get_default semantics, ``AdapterNotRegisteredError`` on miss, and
    parallel ``health_snapshot`` via ``asyncio.gather`` with graceful
    UNHEALTHY-wrapping for raising adapters.

- ``conftest.py``
    Optional warehouse-tier fixtures: mock repositories, adapter
    factory, httpx client factory, mock circuit breaker, aware-datetime
    factory, and the hermeticity guard. Inherits all parent fixtures
    from ``services/inventory-service/tests/conftest.py``.

Hermetic Boundary
-----------------

Every test in this subfolder is **strictly hermetic**:

- ZERO real Postgres connections (mocked via ``unittest.mock.AsyncMock``
  + ``MagicMock`` async context managers).
- ZERO real Kafka producers/consumers.
- ZERO real HTTP traffic — all ``httpx`` calls intercepted by
  ``respx.mock``.
- ZERO real circuit-breaker timers — unique breaker names per test (via
  ``uuid4().hex[:8]``) avoid Prometheus collector-registry collisions;
  state transitions exercised via ``asyncio.sleep`` rather than
  freezegun (``pybreaker`` uses ``time.monotonic()`` which freezegun
  does not patch).
- ZERO real secrets — synthetic ``SecretStr`` test values only.

AAP Cross-References
--------------------

- Section 0.5.2.6 Group 6 — Tests: ``services/*/tests/unit/**/*`` in
  scope.
- Section 0.6.1 In-scope: ``services/*/tests/unit/**/*`` explicitly
  listed.
- R-6 — Database-per-service (no cross-service DB access in tests).
- R-7 — Polyglot persistence: PostgreSQL ``SELECT … FOR UPDATE``,
  ``ON CONFLICT (order_id) DO NOTHING`` semantics tested.
- R-15 — Retry behavior: optimistic-lock retry passthrough; HTTP retry
  on ``ExternalWMSWarehouseAdapter.health_check``.
- R-16 — Circuit breaker state transitions in
  ``ExternalWMSWarehouseAdapter``.
- R-19 — Liveness/readiness via ``health_check`` (must NEVER raise).
- R-25 — No real secrets; ``api_key`` wrapped in ``SecretStr``.
- R-26 — Structured JSON logs (``adapter.{op}.begin``/``.done``/
  ``.idempotent_replay``/``.noop_terminal_state``/
  ``.unexpected_failure``) with required fields ``correlation_id``,
  ``order_id``, ``reservation_id``, ``elapsed_ms``.
"""
