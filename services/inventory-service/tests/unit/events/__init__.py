"""Unit tests for the Inventory Service event layer.

This package contains hermetic, fully-async unit tests for the event
producer/consumer surface of the Inventory Service:

    * test_order_created_consumer.py     — OrderCreatedHandler dispatch
    * test_order_cancelled_consumer.py   — OrderCancelledHandler dispatch
    * test_order_fulfilled_consumer.py   — OrderFulfilledHandler dispatch
    * test_inventory_reserved_payload.py — InventoryReservedEvent producer
                                           payload schema + routing
    * test_inventory_released_payload.py — InventoryReleasedEvent (3-flag)
                                           and InventoryReservationFailed
                                           producer contracts
    * test_inventory_low_stock_payload.py — InventoryLowStockEvent
                                            producer contract (HYPHEN
                                            topic per AAP R-30)

Per-folder fixtures live in ``conftest.py`` and provide:

    * Mock collaborators with proper ``spec=`` (AsyncMock /
      MagicMock for sync vs async surfaces).
    * Result-dataclass factories (ReservationResult / ReleaseResult /
      FinalizeResult).
    * Event factories for all consumed and emitted events.
    * Domain-object fixtures (Warehouse, Reservation).

AAP rule coverage spanning the package:

    * R-13 / R-26 — Correlation ID and structured ``extra`` log fields.
    * R-14        — Schema-Registry validation pre-produce; consumers
                    tolerate unknown fields (forward compatibility).
    * R-15 / R-17 — Retryable failures bubble up to the consumer for
                    retry-topic routing; poison messages route to DLQ.
    * R-18        — Saga compensation: failure events emitted on
                    non-retryable errors enable Order Service
                    compensation.
    * R-30        — ``inventory.low-stock`` topic is HYPHENATED;
                    ``inventory.reserved`` and ``inventory.released``
                    are single-dot canonical names.
    * R-31        — ``event_version`` field present and ≥1 on every
                    event.
    * R-33        — Self-contained event payloads (consumers never
                    call back to producers).

Hermetic boundaries (NEVER violate):
    * No real ``confluent_kafka`` clients — Producer / Consumer mocked
      via ``unittest.mock.AsyncMock`` and ``MagicMock``.
    * No real Schema Registry HTTP — validation runs locally against
      INLINE JSON Schema fixtures bundled in each payload-test module
      (canonical schemas live in ``infrastructure/kafka/schemas/`` once
      that directory is populated; the tests carry an inline copy for
      hermetic self-containment).
    * No real Postgres — repositories and adapters mocked with
      ``AsyncMock(spec=...)``.

This package marker is intentionally side-effect-free per AAP R-19:
no module-level imports of ``src.*``, no executable logic, no
``__all__`` re-exports beyond the explicit empty list below.
"""

from __future__ import annotations

__all__: list[str] = []
