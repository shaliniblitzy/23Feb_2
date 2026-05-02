"""Hermetic unit tests for the inventory-service domain layer.

This package contains pure-Python, zero-I/O, zero-mock business-logic tests
that cover every aggregate, value object, enum, and pure function in
``services.inventory-service.src.domain``. Domain tests are the foundational
regression bedrock of the service: if any test in this package fails, every
higher-tier test (repository, warehouse, events, scheduler) is suspect.

Test modules
------------
- ``test_warehouse.py`` -- ``Warehouse`` aggregate, ``WarehouseStatus`` and
  ``WarehouseAdapterType`` enums, ``is_operational()``,
  ``can_accept_reservation()`` (raises ``WarehouseUnavailableError`` when
  not operational).
- ``test_stock_movement.py`` -- ``StockMovement`` aggregate (15 fields,
  ``id: int`` from BIGSERIAL), 6-member ``StockMovementType`` enum, and the
  ``create()`` keyword-only factory's validation rules.
- ``test_low_stock_threshold.py`` -- ``StockItem.is_low_stock()`` boundary
  semantics (``<=`` inclusive) and threshold-crossing scenarios.
- ``test_optimistic_lock_retry.py`` -- ``StockItem.version`` increment
  semantics: ``reserve``, ``release``, and ``finalize`` each return a new
  instance with ``version + 1``. Detection of version mismatches and the
  retry loop itself live in the repository layer (AAP R-15).
- ``test_stock_item_aggregate.py`` -- ``StockItem`` construction,
  ``reserve()`` (raises ``InsufficientStockError`` on shortfall),
  ``release()`` and ``finalize()`` (raise ``ValidationError`` on negative
  reserved), and arithmetic conservation invariants.
- ``test_reservation_creation.py`` -- ``Reservation`` and ``ReservationItem``
  direct construction (no factory method), 8-field shape, ``frozen=True``
  and ``slots=True`` semantics.
- ``test_reservation_state_transitions.py`` -- the ``Reservation.transition_to()``
  state machine: ACTIVE -> {RELEASED, FULFILLED, EXPIRED} are legal; every
  other transition raises ``ReservationStateError``.
- ``test_reservation_idempotency.py`` -- ``order_id`` as a passive UUID
  carrier; uniqueness for at-least-once Kafka delivery is enforced by the
  DB ``UNIQUE (order_id)`` constraint and the repository's
  ``ON CONFLICT (order_id) DO NOTHING`` clause, NOT by the domain layer.

Hermetic boundaries
-------------------
Domain tests run in microseconds because they touch nothing outside the
process:

- No DB, no Kafka, no HTTP, no filesystem, no environment variables.
- No mocks or stubs (the code under test is pure-Python with no
  collaborators).
- Synchronous ``pytest`` only -- ``pytest-asyncio`` is not required.
- Reproducible inputs come from the ``faker_instance`` fixture (seeded
  ``20240101``) inherited from ``services/inventory-service/tests/conftest.py``.

No per-folder ``conftest.py`` is required for this package; all needed
fixtures (``faker_instance``, ``correlation_id``, ``settings_factory``,
``captured_logs``, etc.) inherit from the parent test package.

AAP cross-references
--------------------
- Section 0.5.2.6 Group 6 -- ``services/*/tests/unit/**/*`` in scope.
- Section 0.6.1           -- this folder is explicitly enumerated in scope.
- R-6                     -- database-per-service (domain layer is
                             persistence-agnostic; tests touch no DB).
- R-7                     -- polyglot persistence (irrelevant to the domain
                             layer).
- R-15                    -- optimistic-lock retry (the version-increment
                             portion is verified here; the retry loop is in
                             ``tests/unit/repository/``).
"""
