"""Inventory-service Kafka event handlers package.

This sub-package hosts the per-topic event handler classes that
implement the inventory service's reactive logic in response to
order-domain events:

* :mod:`src.events.handlers.order_created` —
  :class:`~src.events.handlers.order_created.OrderCreatedHandler`
  reserves stock for the inbound ``order.created`` event and emits
  ``inventory.reserved`` (success), ``inventory.reservation_failed``
  (non-retryable error), or ``inventory.low-stock`` (post-reserve
  fan-out) accordingly.
* :mod:`src.events.handlers.order_cancelled` —
  :class:`~src.events.handlers.order_cancelled.OrderCancelledHandler`
  releases the reservation for the inbound ``order.cancelled`` event
  (restores ``available_qty``, decrements ``reserved_qty``) and emits
  ``inventory.released`` with ``final=False, expired=False``.
* :mod:`src.events.handlers.order_fulfilled` —
  :class:`~src.events.handlers.order_fulfilled.OrderFulfilledHandler`
  finalizes the reservation for the inbound ``order.fulfilled`` event
  (decrements ``reserved_qty`` without restoring ``available_qty`` —
  stock has shipped) and emits ``inventory.released`` with
  ``final=True, expired=False``.

All handler classes conform to the ``EventHandler`` Protocol defined
in :mod:`src.events.consumer` and are registered into
``handlers_by_topic`` in :mod:`src.container` for dispatch by the
:class:`KafkaConsumerRunner`.

Import contract
---------------
Per AAP R-19, this ``__init__.py`` is side-effect-free: it does NOT
import any handler modules at package import time. Importers MUST
use fully-qualified module paths:

.. code-block:: python

    from src.events.handlers.order_created import OrderCreatedHandler
    from src.events.handlers.order_cancelled import OrderCancelledHandler
    from src.events.handlers.order_fulfilled import OrderFulfilledHandler

This is exactly how :mod:`src.container` wires the handlers. Eager
imports here would defeat dependency injection (handlers would be
instantiated transitively at startup, before settings are loaded) and
risk circular-import failures with :mod:`src.events.consumer`,
:mod:`src.events.producer`, and :mod:`src.warehouse.adapter`.

AAP Cross-References
--------------------
* AAP Section 0.4.2 — Inventory consumes ``order.*`` events; this
  package hosts the per-topic handler classes.
* AAP Section 0.5.2.2 bullet 5 — Inventory Service implementation.
* AAP Section 0.6.1 — In-scope path for the inventory-service event-
  handlers sub-package.
* AAP R-19 — Side-effect-free ``__init__.py``.
"""

from __future__ import annotations

__all__: list[str] = []
