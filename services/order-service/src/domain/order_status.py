"""Order lifecycle status enum and saga transition table for the Order Service.

This module is the most foundational leaf of the Order Service's domain
package. It defines:

1. :class:`OrderStatus` — a :class:`enum.StrEnum` enumerating the 8
   canonical lifecycle states of the ``Order`` aggregate. The string
   values are an EXTERNAL CONTRACT (see "External contract" below).
2. :data:`_ALLOWED_TRANSITIONS` — an immutable mapping from each
   :class:`OrderStatus` to the set of valid successor statuses.
   Consumed by ``src/saga/state_machine.py`` to enforce the saga
   pattern (AAP R-18) on every status mutation.
3. :func:`is_terminal` — a module-level helper that returns ``True``
   for the three terminal states (``FULFILLED``, ``CANCELLED``,
   ``FAILED``). The helper is a derivation from the transition table
   (single source of truth) so that adding a new terminal state in
   the future requires editing exactly one place.

Design invariants
-----------------
* **Foundational module** — this file MUST NOT import from any other
  ``src/*`` module (not even peer domain modules such as ``order.py``,
  ``order_item.py``, ``saga_state.py``, or ``errors.py``). Cross-cutting
  concerns (validation, error raising, logging) live in their owning
  layers and consume :class:`OrderStatus` / :data:`_ALLOWED_TRANSITIONS`
  rather than the other way around. This keeps the import graph
  acyclic and the domain leaf cheap to import in tests.
* **No I/O at import time** — only enum construction, dict / frozenset
  literal evaluation, and ``MappingProxyType`` wrapping happen when
  this module is loaded. No logging, no HTTP framework, no DB driver,
  no Kafka client.
* **No** :func:`enum.auto` — every member assigns its string value
  EXPLICITLY. The strings are externally visible (DB ``orders.status``
  column, Kafka event payloads, audit logs); :func:`enum.auto` would
  derive values from member names and silently break the contract on
  rename. Explicit values are stable across refactors and match the
  convention established by the sibling Payment Service in
  ``services/payment-service/src/domain/enums.py``.
* **Pure data** — no methods on the enum. Predicates such as
  "is this status terminal?" or "is this transition allowed?" live
  on this module's free functions (:func:`is_terminal`) or on the
  consumers (``saga/state_machine.py`` for transition validation),
  NOT on the enum. This preserves the "thin domain leaf" invariant
  and matches the established pattern.
* **Runtime immutability** for :data:`_ALLOWED_TRANSITIONS` —
  ``MappingProxyType`` wraps an underlying ``dict``; direct mutation
  attempts (``_ALLOWED_TRANSITIONS[OrderStatus.X] = ...``,
  ``_ALLOWED_TRANSITIONS.pop(...)``, etc.) raise ``TypeError``. The
  successor sets are :class:`frozenset` instances, so attempting to
  ``.add(...)`` to a successor set also raises. Defense-in-depth for
  the saga state-machine's source of truth.
* **Why** ``MappingProxyType`` **instead of** ``frozendict`` —
  ``frozendict`` is a third-party PyPI package; the domain layer
  intentionally restricts itself to the standard library plus
  Pydantic. ``MappingProxyType`` (in :mod:`types`) provides identical
  read-only semantics and ships with CPython. The folder spec mentions
  "frozendict" colloquially; the canonical Python implementation is
  ``MappingProxyType``.

External contract
-----------------
The 8 string values declared by :class:`OrderStatus` are pinned and
must remain stable across releases:

* **Database** — :class:`OrderStatus` mirrors the ``orders.status``
  column in the Order Service's private ``order_db`` (AAP Section
  0.4.4). The DDL declares a CHECK constraint of the form::

      status TEXT NOT NULL CHECK (status IN (
          'CREATED', 'INVENTORY_RESERVED', 'PAYMENT_TAKEN',
          'FULFILLED', 'COMPENSATING_INVENTORY',
          'COMPENSATING_PAYMENT', 'CANCELLED', 'FAILED'
      ))

  Renaming any value REQUIRES a coordinated DDL migration plus a
  backfill of existing rows.
* **Kafka events** — emitted as the ``status`` field of
  ``order.created`` / ``order.cancelled`` / ``order.fulfilled`` event
  payloads (AAP R-30 / R-31). Renaming requires a Schema Registry
  version bump plus a coordinated consumer rollout.
* **Structured logs** — emitted as the ``order_status`` log field
  (AAP R-26) and surfaced in Kibana dashboards (AAP R-28). Renaming
  invalidates saved queries and dashboard filters.

Authoritative references
------------------------
* AAP Section 0.1.1 Component #6 — Order Service overview.
* AAP Section 0.4.4 — ``order_db`` schema (``orders``, ``order_items``,
  ``order_status_history``, ``saga_state`` tables); the
  ``orders.status`` column accepts exactly the 8 values declared here.
* AAP Section 0.5.2.2 bullet 6 — Order Service implementation plan
  with saga coordinator (create -> reserve-inventory -> take-payment
  -> confirm | compensate).
* AAP R-18 — saga pattern with explicit compensation transitions.
  The :data:`_ALLOWED_TRANSITIONS` table is the source of truth.
* AAP R-30 / R-31 — events named ``<domain>.<verb>`` with status
  fields that mirror :class:`OrderStatus` values.
"""

from __future__ import annotations

from enum import StrEnum
from types import MappingProxyType
from typing import Final, Mapping


class OrderStatus(StrEnum):
    """Canonical lifecycle status for the ``Order`` aggregate.

    Mirrors the ``orders.status`` column (AAP Section 0.4.4) in the
    Order Service's private ``order_db``. The string values are part
    of an EXTERNAL CONTRACT:

      * Persisted directly as PostgreSQL ``TEXT`` values constrained
        by a ``CHECK`` clause (DDL excerpt:
        ``CHECK (status IN ('CREATED', 'INVENTORY_RESERVED', ...))``).
      * Emitted as the ``status`` field in Kafka event payloads
        (``order.created``, ``order.cancelled``, ``order.fulfilled``)
        per AAP R-30 / R-31.
      * Logged as the ``order_status`` field in structured JSON logs
        (AAP R-26) and used in Kibana dashboards (AAP R-28).

    Renaming any value REQUIRES a coordinated DDL migration plus an
    event schema bump plus a consumer rollout. DO NOT change values
    casually.

    Members (UPPERCASE — exactly as specified in the domain folder
    spec; member NAMES match VALUES for symmetry):

      * ``CREATED`` — Initial state. The order has been recorded
        durably in the ``orders`` table and ``order.created`` has
        been emitted to Kafka. The saga coordinator now drives next
        steps (inventory reservation).
      * ``INVENTORY_RESERVED`` — The Inventory Service confirmed via
        the ``inventory.reserved`` event that all line items are
        reserved. The saga proceeds to take payment.
      * ``PAYMENT_TAKEN`` — The Payment Service confirmed via the
        ``payment.succeeded`` event that the customer was charged.
        The saga proceeds to fulfillment.
      * ``FULFILLED`` — Terminal SUCCESS state. The saga emitted
        ``order.fulfilled``; downstream services (notifications,
        recommendations) have been informed.
      * ``COMPENSATING_INVENTORY`` — Failure path. The saga is
        releasing previously-reserved inventory (e.g., because
        ``payment.failed`` arrived or a step timed out).
      * ``COMPENSATING_PAYMENT`` — Failure path. The saga is reversing
        a captured payment (e.g., because the customer cancelled
        post-charge or downstream fulfillment failed).
      * ``CANCELLED`` — Terminal FAILURE state due to a deliberate
        cancellation (customer-initiated, fraud check, compensation
        completed cleanly, etc.).
      * ``FAILED`` — Terminal FAILURE state due to an UNRECOVERABLE
        system error (e.g., compensation attempts exhausted; the
        order is left in an inconsistent state and must be reconciled
        manually via the runbook).

    See AAP R-18 (saga pattern) and the transition table
    :data:`_ALLOWED_TRANSITIONS` below for valid moves.

    Example:
        >>> OrderStatus.CREATED == "CREATED"
        True
        >>> OrderStatus("INVENTORY_RESERVED") is OrderStatus.INVENTORY_RESERVED
        True
        >>> isinstance(OrderStatus.FULFILLED, str)
        True
        >>> str(OrderStatus.CANCELLED)
        'CANCELLED'
    """

    CREATED = "CREATED"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    PAYMENT_TAKEN = "PAYMENT_TAKEN"
    FULFILLED = "FULFILLED"
    COMPENSATING_INVENTORY = "COMPENSATING_INVENTORY"
    COMPENSATING_PAYMENT = "COMPENSATING_PAYMENT"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


#: Allowed status transitions for the ``Order`` aggregate, consumed by
#: ``src/saga/state_machine.py``.
#:
#: Encoded as a :class:`types.MappingProxyType` over a ``dict`` of
#: :class:`frozenset` values to guarantee runtime IMMUTABILITY:
#:
#:   * ``_ALLOWED_TRANSITIONS[OrderStatus.X] = frozenset()`` raises
#:     ``TypeError`` (read-only mapping view).
#:   * ``_ALLOWED_TRANSITIONS[OrderStatus.X].add(...)`` raises
#:     ``AttributeError`` (frozenset has no ``.add``).
#:   * ``_ALLOWED_TRANSITIONS.pop(...)`` raises ``TypeError``.
#:
#: Defense-in-depth ensures the saga state-machine's source of truth
#: cannot be silently mutated by tests or buggy call sites.
#:
#: The leading underscore signals that callers OUTSIDE the Order
#: Service domain / saga packages should access this table via the
#: forthcoming high-level helper ``saga.state_machine.is_allowed_transition``
#: rather than directly. Module-level export is permitted for tests
#: and for the saga state machine itself, both of which are in-tree.
#:
#: Transition rationale (AAP R-18 saga pattern; the saga choreographs
#: events from inventory and payment services to walk the state
#: machine forward, and walks compensations backward on failure):
#:
#:   * ``CREATED -> INVENTORY_RESERVED``
#:       Triggered by ``inventory.reserved`` event. Forward path.
#:   * ``CREATED -> COMPENSATING_INVENTORY``
#:       Triggered by ``inventory.reservation_failed`` event OR a
#:       reservation step timeout. Begins compensation.
#:   * ``CREATED -> CANCELLED``
#:       Triggered by a customer cancellation BEFORE inventory was
#:       reserved (nothing to compensate; transition straight to
#:       terminal failure).
#:   * ``INVENTORY_RESERVED -> PAYMENT_TAKEN``
#:       Triggered by ``payment.succeeded`` event. Forward path.
#:   * ``INVENTORY_RESERVED -> COMPENSATING_INVENTORY``
#:       Triggered by ``payment.failed`` event OR a payment step
#:       timeout. Begins compensation by releasing inventory; payment
#:       was never captured, so no payment compensation is needed.
#:   * ``PAYMENT_TAKEN -> FULFILLED``
#:       Downstream fulfillment confirmed. The saga emits
#:       ``order.fulfilled``. Terminal success.
#:   * ``PAYMENT_TAKEN -> COMPENSATING_PAYMENT``
#:       Late-stage failure (rare). Examples: customer cancellation
#:       post-charge, downstream fulfillment failure that requires
#:       refund. Begins payment compensation.
#:   * ``COMPENSATING_INVENTORY -> CANCELLED``
#:       ``inventory.released`` confirmed. Saga ends in CANCELLED.
#:   * ``COMPENSATING_INVENTORY -> FAILED``
#:       Inventory release attempts exhausted. Saga ends in FAILED;
#:       requires manual reconciliation per the runbook.
#:   * ``COMPENSATING_PAYMENT -> COMPENSATING_INVENTORY``
#:       Payment reversal confirmed; the saga must now release the
#:       previously-reserved inventory. From here it eventually
#:       reaches CANCELLED (clean) or FAILED (exhausted).
#:   * ``COMPENSATING_PAYMENT -> FAILED``
#:       Payment reversal attempts exhausted. Saga ends in FAILED.
#:   * ``FULFILLED``, ``CANCELLED``, ``FAILED`` — terminal; NO
#:     outgoing transitions (encoded as empty :class:`frozenset` so
#:     :func:`is_terminal` can detect them uniformly).
_ALLOWED_TRANSITIONS: Final[Mapping[OrderStatus, frozenset[OrderStatus]]] = (
    MappingProxyType(
        {
            OrderStatus.CREATED: frozenset(
                {
                    OrderStatus.INVENTORY_RESERVED,
                    OrderStatus.COMPENSATING_INVENTORY,
                    OrderStatus.CANCELLED,
                }
            ),
            OrderStatus.INVENTORY_RESERVED: frozenset(
                {
                    OrderStatus.PAYMENT_TAKEN,
                    OrderStatus.COMPENSATING_INVENTORY,
                }
            ),
            OrderStatus.PAYMENT_TAKEN: frozenset(
                {
                    OrderStatus.FULFILLED,
                    OrderStatus.COMPENSATING_PAYMENT,
                }
            ),
            OrderStatus.COMPENSATING_INVENTORY: frozenset(
                {
                    OrderStatus.CANCELLED,
                    OrderStatus.FAILED,
                }
            ),
            OrderStatus.COMPENSATING_PAYMENT: frozenset(
                {
                    OrderStatus.COMPENSATING_INVENTORY,
                    OrderStatus.FAILED,
                }
            ),
            # Terminal states: no outgoing transitions.
            OrderStatus.FULFILLED: frozenset(),
            OrderStatus.CANCELLED: frozenset(),
            OrderStatus.FAILED: frozenset(),
        }
    )
)


def is_terminal(status: OrderStatus) -> bool:
    """Return ``True`` if ``status`` is a terminal state.

    Terminal states are :attr:`OrderStatus.FULFILLED`,
    :attr:`OrderStatus.CANCELLED`, and :attr:`OrderStatus.FAILED`. By
    definition, no outgoing transitions are permitted from these
    states; the saga coordinator stops scheduling further steps once
    the order reaches one of them.

    The implementation is a derivation from
    :data:`_ALLOWED_TRANSITIONS` so the helper and the table stay
    consistent automatically: if a future evolution adds a new
    terminal state with an empty successor set, :func:`is_terminal`
    immediately recognizes it as terminal without code change.
    Hardcoding the set ``{FULFILLED, CANCELLED, FAILED}`` would
    create a drift risk and a second source of truth.

    Args:
        status: An :class:`OrderStatus` value to classify.

    Returns:
        ``True`` when the status has no outgoing transitions in
        :data:`_ALLOWED_TRANSITIONS`; ``False`` otherwise.

    Example:
        >>> is_terminal(OrderStatus.FULFILLED)
        True
        >>> is_terminal(OrderStatus.CANCELLED)
        True
        >>> is_terminal(OrderStatus.FAILED)
        True
        >>> is_terminal(OrderStatus.CREATED)
        False
        >>> is_terminal(OrderStatus.INVENTORY_RESERVED)
        False
    """
    return len(_ALLOWED_TRANSITIONS[status]) == 0


__all__ = [
    "OrderStatus",
    "_ALLOWED_TRANSITIONS",
    "is_terminal",
]
