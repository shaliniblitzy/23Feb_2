"""Order Service saga sub-package — AAP R-18 reference implementation.

This sub-package contains the canonical implementation of the saga pattern
for the e-commerce platform. It coordinates the multi-service checkout
flow with explicit compensation paths and durable state in the
``saga_state`` Postgres table (created by Alembic revision 0004).

Modules:
    state_machine
        Pure functions and the allowed-transition matrix mapping
        ``(current_status, event_type) -> next_status``. Imports only
        from ``src.domain``; foundational with zero side effects.

    compensation
        Per-failure-type compensator classes (inventory reservation
        failed, payment failed, saga timeout, DLQ exhausted). Each
        compensator implements ``async def compensate(saga_state) -> None``
        and emits the appropriate ``order.cancelled`` event.

    coordinator
        ``SagaCoordinator`` — THE central class. Exposes
        ``handle_event(topic, event)``, ``start_saga(...)``, and
        ``trigger_compensation(saga_id, reason)``. Idempotent on every
        transition; durable via ``saga_state`` table optimistic locking.

    scheduler
        ``SagaScheduler`` background task. Polls ``saga_state`` for
        timed-out sagas via ``FOR UPDATE SKIP LOCKED`` and invokes the
        coordinator's compensation path. Started by ``src.main``
        lifespan as ``asyncio.create_task``.

    steps
        One module per forward-path saga step: create-order, await-
        inventory, await-payment, confirm-order. Each step encapsulates
        the per-step logic the coordinator dispatches to.

Architectural rules enforced by this sub-package (AAP):
    R-18 — every state transition is durable (commits before any
        external visibility); compensation is explicit and idempotent.
    R-30 — events emitted are named ``<domain>.<verb>`` —
        ``order.created``, ``order.cancelled``, ``order.fulfilled``.
    R-32 — the saga (producer) does NOT know consumers. Adding new
        consumers requires zero changes here.
    R-33 — events are self-contained; consumers should not need to
        call back to the saga to interpret them.

Public API discovery: import submodules explicitly. This package init
intentionally does NOT re-export ``SagaCoordinator`` or ``SagaScheduler``
because eager re-exports would force the state-machine matrix and
compensator classes to load even when only a peripheral utility is
needed (e.g., a unit test patching one compensator).
"""

from __future__ import annotations

__all__: list[str] = []
