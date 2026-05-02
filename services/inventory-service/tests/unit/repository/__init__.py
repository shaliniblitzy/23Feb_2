"""Hermetic unit tests for the inventory-service repository layer.

This subpackage covers PostgreSQL repository adapter tests for the
four owned tables (``warehouses``, ``stock_items``, ``reservations``,
``stock_movements``). Tests are HERMETIC -- no real Postgres, no
network, no Kafka, no HTTP. All ``psycopg`` interactions are mocked
via ``mocker.AsyncMock`` (pool / connection / cursor).

Modules
-------
- ``test_stock_item_repository``   -- StockRepository:
    pessimistic + optimistic locking, ``tenacity`` retry, Postgres
    CheckViolation translation to InsufficientStockError.
- ``test_reservation_repository``  -- ReservationRepository:
    ON CONFLICT idempotency, state-as-version transitions, partial-index
    contract for ``find_expired_active``.
- ``test_stock_movement_repository`` -- StockMovementRepository:
    append-only audit log, BIGSERIAL id round-trip via
    ``dataclasses.replace``, JSONB metadata adapter.
- ``test_warehouse_repository``    -- WarehouseRepository:
    master-data CRUD, ``Jsonb(...)`` adapter for config column,
    UPSERT keyed on ``name``.

Shared infrastructure
---------------------
- ``conftest.py`` provides repository-test-specific fixtures
  (``mock_async_cursor``, ``mock_async_connection``, ``mock_async_pool``,
  ``mock_pool_with_cursor_factory``) plus the autouse
  ``assert_no_real_psycopg_connections`` hermetic-boundary guard.
- The parent ``services/inventory-service/tests/conftest.py`` provides
  cross-cutting fixtures (``correlation_id``, ``captured_logs``,
  ``settings_factory``, ``assert_required_log_fields``).

AAP cross-references
--------------------
- Section 0.5.2.6 Group 6 -- in scope.
- Section 0.6.1           -- explicitly listed.
- R-6                     -- database-per-service.
- R-15                    -- bounded retries with exponential backoff.
- R-25                    -- placeholder-only SQL (no string interpolation).
- R-26                    -- structured JSON logs.
"""
