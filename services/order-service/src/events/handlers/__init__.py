"""Saga-driver Kafka event handlers for the Order Service.

Submodules:
    inventory_reserved_handler              Topic: ``inventory.reserved``.
                                            Advances saga to INVENTORY_RESERVED.
    inventory_reservation_failed_handler    Topic: ``inventory.reservation_failed``.
                                            Triggers compensation -> CANCELLED.
    payment_succeeded_handler               Topic: ``payment.succeeded``.
                                            Advances saga to FULFILLED.
    payment_failed_handler                  Topic: ``payment.failed``.
                                            Triggers inventory compensation -> CANCELLED.

Each handler implements the ``EventHandler`` Protocol declared in
``src.events.consumer`` and delegates state transitions to the
``SagaCoordinator`` from ``src.saga.coordinator``. The dependency-injection
container (``src.container``) is the canonical wiring point; it imports
each handler from its specific module path and registers it in the
``handlers_by_topic`` mapping passed to ``KafkaConsumerRunner``.

This package init is intentionally side-effect-free. No submodule is
imported at package load; importing ``src.events.handlers`` does NOT
spin up any Kafka clients, DB connections, or saga coordinators.

AAP rules satisfied: R-18, R-19, R-32.
"""

from __future__ import annotations

__all__: list[str] = []
