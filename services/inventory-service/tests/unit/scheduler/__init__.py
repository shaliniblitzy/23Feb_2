"""Unit-test package for ``services.inventory-service.src.scheduler``.

Scope
-----
This subpackage contains hermetic unit tests for the
``ReservationExpiryScheduler`` -- the AAP R-20 keystone fallback path that
detects reservations whose ``expires_at`` is in the past and which are still
in ``ACTIVE`` status, transitions them to ``EXPIRED``, reverses the stock
deltas, and emits ``inventory.released`` events with
``final=False, expired=True, cancellation_reason=None`` so that downstream
sagas can compensate.

The scheduler is **unique to the Inventory Service** -- no peer microservice
in the e-commerce platform (Auth, User, Product, Order, Payment,
Notification, Recommendation) implements an equivalent. Its tests therefore
live in this dedicated subfolder rather than under ``domain/``,
``repository/``, ``warehouse/``, or ``events/``.

Test Modules
------------
- ``test_expiry_scheduler_polling.py``
    Constructor validation gates (AAP R-19 fail-fast), the ``async run()``
    polling-loop lifecycle, ``_tick()`` batch sizing and pagination, and
    threshold computation (``before=now-utc``, NOT ``now - expiry_ms``).

- ``test_expiry_scheduler_emits_released_event.py``
    The ``_process_one()`` exception-isolation contract (NEVER raises except
    ``CancelledError``), the ``_expire_transaction()`` 3-step transactional
    flow (lock -> reverse stock per item -> transition status -> return
    event), and the ``_route_to_dlq()`` DLQ-routing contract (``kind_label``,
    ``attempt=0``, ``extra_metadata`` schema).

- ``test_expiry_scheduler_disabled_when_feature_off.py``
    The feature-flag-disabled contract:
    ``Settings.features.reservation_expiry_scheduler_enabled`` exists, is
    ``bool``-typed, defaults to ``False``, and is honored at the **container
    layer** (``src/main.py`` lifespan) -- NOT at the scheduler layer. The
    scheduler class itself has no feature-flag check.

Hermetic Boundary
-----------------
ZERO real Postgres, ZERO real Kafka, ZERO real timers.

- Database: ``mock_async_connection_pool`` provides a 3-tuple of
  ``MagicMock`` pool / conn / transaction async context managers.
- Repositories: ``AsyncMock`` instances for ``ReservationRepository``,
  ``StockRepository``, ``StockMovementRepository``.
- Warehouse adapter / registry: ``MagicMock(spec=DatabaseWarehouseAdapter)``
  satisfies the ``isinstance`` check in the scheduler's constructor.
- Kafka producer / DLQ writer: ``AsyncMock`` instances for ``EventProducer``
  and ``DlqWriter``.
- Time: ``freezegun.freeze_time(...)`` controls
  ``datetime.now(tz=timezone.utc)``; ``asyncio.sleep`` is patched to a
  no-op via the ``_patch_asyncio_sleep`` autouse fixture (TENACITY pattern,
  mirrored from ``tests/unit/repository/conftest.py``).

Logging
-------
The scheduler module uses STDLIB
``logging.getLogger("inventory_service.scheduler.expiry_scheduler")`` --
NOT ``structlog``. Tests therefore use pytest's built-in ``caplog`` fixture,
NOT the parent ``conftest``'s ``captured_logs`` /
``structlog_test_capture`` fixtures. Log-record assertions use
``record.message`` (NOT ``record["message"]``) and access ``extra=...``
fields as record attributes (e.g., ``record.order_id``,
``record.correlation_id``).

Fixtures
--------
The local ``conftest.py`` exposes scheduler-specific fixtures:

- Repository / adapter / registry / producer / DLQ-writer ``AsyncMock``
  instances with ``spec=`` set against the real classes.
- ``mock_async_connection_pool`` for the pool / conn / transaction
  async-context-manager stack.
- Domain-object factories: ``make_reservation``, ``make_reservation_item``,
  ``make_stock_item``.
- ``scheduler_factory`` -- produces a fully-configured
  ``ReservationExpiryScheduler`` with sensible defaults
  (``poll_interval_ms=60000``, ``batch_size=200``, default
  ``max_concurrency=8``).
- Autouse ``_patch_asyncio_sleep`` (TENACITY pattern) so polling-loop
  tests complete in microseconds rather than waiting on real wall-clock
  intervals.

The parent ``tests/conftest.py`` provides 14 canonical fixtures
(``settings_factory``, ``correlation_id``, ``faker_instance``,
``assert_required_log_fields``, etc.) which remain available here without
re-declaration.

AAP References
--------------
- AAP Section 0.5.2.6 Group 6 -- Tests.
- AAP Section 0.6.1           -- In-scope: ``services/*/tests/unit/**/*``.
- R-13 -- Correlation-ID propagation onto every emitted event and log
  record.
- R-15 -- Retry policy (delegated to
  ``update_with_optimistic_lock_retrying`` in the repository layer; the
  scheduler itself uses the non-retrying variant inside its single
  transaction).
- R-17 -- DLQ routing on retry exhaustion / non-retryable failures.
- R-19 -- Fail-fast at startup (constructor validation gates for
  ``poll_interval_ms``, ``batch_size``, ``max_concurrency``).
- R-20 -- Fallback paths (this scheduler IS the canonical fallback path
  for orphaned ``ACTIVE`` reservations whose owning saga never completed).
- R-26 -- Structured JSON logging discipline (``correlation_id``,
  ``reservation_id``, ``order_id``, ``warehouse_id``, ``elapsed_ms``).
"""
