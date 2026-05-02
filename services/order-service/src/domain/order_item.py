"""Order Service — :class:`OrderItem` value object.

This module defines a single Pydantic v2 frozen value object,
:class:`OrderItem`, representing one line item belonging to an
``Order`` aggregate. It is the in-memory representation of a row in
the ``order_items`` table of the Order Service's private ``order_db``
(per AAP Section 0.4.4) and is owned exclusively by the Order Service
(AAP R-6 — database per service).

Design principles
-----------------
1. **Foundational module** — this file lives in the most foundational
   layer of the ``services/order-service/src/domain`` package. It
   MUST NOT import from any other ``src/*`` module (not even peer
   domain modules such as ``order.py``, ``order_status.py``, or
   ``errors.py``). The deliberate self-containment keeps the import
   graph acyclic and the domain leaf cheap to import in tests. This
   matches the pattern established by the sibling foundational
   modules :mod:`src.domain.order_status` and :mod:`src.domain.errors`.

2. **Frozen / immutable** — Domain-Driven Design value objects are
   inherently immutable: they have no identity beyond their values.
   Two ``OrderItem`` instances with identical field values ARE the
   same value, even if they're different Python objects. Freezing
   makes this explicit (see ``model_config.frozen=True``) and
   prevents accidental mutation in repositories or handlers. To
   "modify" a line, callers MUST construct a new ``OrderItem`` with
   the updated fields and replace it in the parent ``Order``'s
   ``items`` tuple (which itself requires constructing a new
   ``Order`` via :meth:`pydantic.BaseModel.model_copy`).

3. **Strict typing** — :class:`uuid.UUID` for ``product_id`` (an
   opaque cross-service reference per AAP R-6, NOT a foreign key to
   the Product Service's database); :class:`decimal.Decimal` for
   monetary fields (AAP R-26 — Decimal not float for monetary
   amounts); :class:`int` for ``quantity`` and ``line_no``.

4. **Pure data** — no behavior beyond what Pydantic provides plus a
   single :func:`pydantic.model_validator` that enforces the
   ``line_total == quantity * unit_price`` cross-field invariant.

5. **No I/O, no logging, no side effects at module import time** —
   keeps unit-test startup cheap and prevents the module from
   accidentally pulling in framework-specific code paths.

Field summary (per the Order Service domain folder spec)
--------------------------------------------------------
* ``line_no: int``      — 1-indexed line number, ``>= 1``.
* ``product_id: UUID``  — opaque cross-service reference to the
                          Product Service catalog (AAP R-6).
* ``quantity: int``     — bounded ``[1, MAX_QUANTITY_PER_ITEM]``
                          (i.e., ``[1, 999]``).
* ``unit_price: Decimal`` — monetary, ``>= 0``, max 18 digits, 2
                            decimal places (free items allowed for
                            promotional bundling).
* ``line_total: Decimal`` — monetary, ``>= 0``, max 18 digits, 2
                            decimal places, MUST equal
                            ``quantity * unit_price``.

Why store ``line_total`` redundantly with ``quantity * unit_price``?
1. The DB column is materialized for query performance (filter / sort
   by line value without recomputation).
2. Round-tripping via the repository preserves the value the system
   originally wrote (defensive against future formula evolution).
3. The cross-field validator guards against drift between fields,
   surfacing bugs at construction time rather than at query time.

Why no ``name``, ``sku``, or ``currency`` fields?
* The Order Service does NOT cache product display data. Display name
  and SKU live in the Product Service catalog (AAP R-6 — the Order
  Service has no read-through to product-service's database) and are
  fetched on-demand by the storefront. Storing them here would create
  a denormalization-staleness problem (rename a product -> orders
  show old name).
* All lines in an order share the parent ``Order.currency`` (a
  single order is always one currency). Storing currency per line
  would invite multi-currency orders, which the system explicitly
  does not support.

Authoritative references
------------------------
* AAP Section 0.1.1 Component #6 — Order Service overview.
* AAP Section 0.4.4 — ``order_db.order_items`` table schema; this
  value object mirrors the row shape.
* AAP Section 0.5.2.2 bullet 6 — Order Service implementation plan.
* AAP R-6 — DB per service; ``product_id`` is opaque.
* AAP R-26 — Decimal not float for monetary amounts.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Final
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


# =============================================================================
# Module-level constants
# =============================================================================

#: Maximum quantity allowed per line item.
#:
#: Mirrors ``order.max_quantity_per_item`` in
#: ``services/order-service/config/default.yaml`` (999). This value is
#: the ABSOLUTE STRUCTURAL CEILING enforced at the domain layer; the
#: per-environment runtime ``Settings`` may apply a tighter limit
#: (e.g., 100 in dev) but cannot exceed this constant. Tests can
#: import this symbol directly to verify boundary semantics without
#: spinning up the configuration layer.
#:
#: Real-world e-commerce orders rarely exceed 10 of any single SKU;
#: 999 is generous yet bounds DoS-via-large-orders attacks. Typed
#: :class:`typing.Final` so static analyzers (mypy) prevent accidental
#: reassignment elsewhere in the codebase.
MAX_QUANTITY_PER_ITEM: Final[int] = 999


# =============================================================================
# Value object
# =============================================================================


class OrderItem(BaseModel):
    """A single line item in an :class:`Order` aggregate.

    Mirrors a row in the ``order_items`` table of the Order Service's
    private ``order_db`` (per AAP Section 0.4.4). Owned exclusively
    by the Order Service; ``product_id`` is an OPAQUE cross-service
    reference (NOT a foreign key to product-service's database) per
    AAP R-6 (database per service).

    The class is FROZEN: value objects are inherently immutable in
    Domain-Driven Design. To "modify" a line, callers MUST construct
    a new :class:`OrderItem` with the updated fields and replace it
    in the parent ``Order``'s ``items`` tuple (which itself requires
    constructing a new ``Order`` via
    :meth:`pydantic.BaseModel.model_copy`). Direct field assignment
    raises :class:`pydantic.ValidationError` at runtime.

    Validators applied:
        * ``line_no >= 1`` (1-indexed for human readability in
          invoices, audit logs, and notifications).
        * ``quantity`` is bounded ``[1, MAX_QUANTITY_PER_ITEM]``
          (i.e., ``[1, 999]``).
        * ``unit_price >= 0`` (zero allowed; free items are legitimate
          for promotional bundling such as "buy one, get one free").
        * ``line_total >= 0``.
        * ``line_total == quantity * unit_price`` (enforced by the
          ``_line_total_consistency`` ``model_validator(mode="after")``
          hook below).

    Attributes:
        line_no: 1-indexed line number; UNIQUE within the parent
            :class:`Order` (uniqueness enforced by ``Order``'s own
            ``_line_no_uniqueness`` validator and by a DB UNIQUE
            constraint on ``(order_id, line_no)``). This class only
            sees one line at a time, so it just enforces the lower
            bound ``>= 1``.
        product_id: Opaque cross-service reference to the Product
            Service catalog. NOT a foreign key (AAP R-6). The Order
            Service does NOT validate that the product exists at the
            Product Service; it trusts the Product Service to surface
            invalid product IDs via its catalog API. A defense-in-
            depth existence check at order placement time is a
            controller-layer concern, not a domain-layer concern.
        quantity: Integer count of units ordered for this line.
            Bounded ``[1, MAX_QUANTITY_PER_ITEM]``. Zero quantity is
            disallowed: the correct way to remove a line is to omit
            it from the parent order, not to construct a zero-line.
        unit_price: Monetary unit price for this line, in the parent
            order's currency. Stored as :class:`decimal.Decimal` for
            exact arithmetic (AAP R-26 — Decimal not float). Capped
            at 18 digits and 2 decimal places to match the DB column
            precision. Zero is permitted to support promotional
            bundles (e.g., "buy one, get one free" sets the bonus
            line's ``unit_price`` to 0).
        line_total: Monetary total for this line, in the parent
            order's currency. Stored explicitly (rather than computed
            on-the-fly) for three reasons: (1) the DB column is
            materialized for query performance; (2) round-tripping
            via the repository preserves the originally written
            value; (3) the cross-field validator can detect drift
            between ``line_total`` and ``quantity * unit_price`` at
            construction time. Capped at 18 digits and 2 decimal
            places.

    Example:
        >>> from decimal import Decimal
        >>> from uuid import UUID
        >>> item = OrderItem(
        ...     line_no=1,
        ...     product_id=UUID("12345678-1234-5678-1234-567812345678"),
        ...     quantity=2,
        ...     unit_price=Decimal("9.99"),
        ...     line_total=Decimal("19.98"),
        ... )
        >>> item.quantity
        2
        >>> item.line_total
        Decimal('19.98')
    """

    # ------------------------------------------------------------------
    # Model configuration (frozen / strict)
    # ------------------------------------------------------------------
    #
    # ``frozen=True``                — immutability (DDD value object).
    # ``str_strip_whitespace=True``  — strip surrounding whitespace
    #                                  from any string-typed field
    #                                  (defensive even though no
    #                                  string fields exist today; if a
    #                                  string field is added later
    #                                  this default applies).
    # ``extra="forbid"``             — reject unknown fields; protects
    #                                  against typos in API request
    #                                  bodies and Kafka event payloads
    #                                  decoded into this model.
    # ``populate_by_name=True``      — allow construction by either
    #                                  field name or alias; future-
    #                                  proofs schema evolution.
    # ``validate_assignment=True``   — even though ``frozen=True``
    #                                  blocks direct assignment, this
    #                                  flag ensures any framework path
    #                                  that bypasses ``__setattr__``
    #                                  (e.g., custom deserialization)
    #                                  is still validated.
    # ``arbitrary_types_allowed=False`` — refuse non-Pydantic-aware
    #                                     types; forces explicit
    #                                     handling of every field.
    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
        arbitrary_types_allowed=False,
    )

    # ------------------------------------------------------------------
    # Fields
    # ------------------------------------------------------------------

    line_no: Annotated[int, Field(ge=1)] = Field(
        ...,
        description=(
            "1-indexed line number; UNIQUE within the parent Order "
            "(enforced by Order's _line_no_uniqueness validator and "
            "by a DB UNIQUE constraint on (order_id, line_no))."
        ),
    )

    product_id: UUID = Field(
        ...,
        description=(
            "Opaque cross-service reference to the Product Service "
            "catalog. NOT a foreign key (AAP R-6 — DB per service)."
        ),
    )

    quantity: Annotated[int, Field(ge=1, le=MAX_QUANTITY_PER_ITEM)] = Field(
        ...,
        description=(
            f"Quantity ordered; bounded [1, {MAX_QUANTITY_PER_ITEM}]. "
            f"Zero is disallowed — omit the line instead of "
            f"constructing a zero-quantity line."
        ),
    )

    unit_price: Annotated[
        Decimal,
        Field(
            max_digits=18,
            decimal_places=2,
            ge=Decimal("0"),
            description=(
                "Unit price in the parent order's currency. Decimal "
                "for exact arithmetic (AAP R-26). Zero allowed for "
                "promotional bundling (e.g., BOGO bonus lines)."
            ),
        ),
    ]

    line_total: Annotated[
        Decimal,
        Field(
            max_digits=18,
            decimal_places=2,
            ge=Decimal("0"),
            description=(
                "Line total in the parent order's currency. MUST "
                "equal quantity * unit_price (enforced by the "
                "_line_total_consistency model_validator)."
            ),
        ),
    ]

    # ------------------------------------------------------------------
    # Cross-field validators
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def _line_total_consistency(self) -> "OrderItem":
        """Enforce ``line_total == quantity * unit_price``.

        Decimal arithmetic is exact when both operands quantize to
        the same scale. Both ``unit_price`` and ``line_total`` are
        constrained to 2 decimal places by the
        :class:`pydantic.Field` declarations above, and ``quantity``
        is an :class:`int`, so the product
        ``unit_price * Decimal(quantity)`` is exact and direct
        equality is correct (no need for ``quantize`` or epsilon
        comparison). This is fundamentally why monetary fields use
        :class:`Decimal` rather than :class:`float` — see AAP R-26.

        The validator runs in ``mode="after"`` so the per-field
        constraints (``ge``, ``le``, ``max_digits``,
        ``decimal_places``) have already been satisfied by the time
        the cross-field check runs. If any per-field constraint
        fails, Pydantic short-circuits and this method is never
        invoked.

        Returns:
            ``self`` — the validated :class:`OrderItem` instance.
            Pydantic ``model_validator(mode="after")`` requires the
            method to return the (possibly mutated) model instance;
            because :class:`OrderItem` is frozen, no mutation occurs
            and the same instance is returned.

        Raises:
            ValueError: when
                ``self.line_total != self.unit_price * Decimal(self.quantity)``.
                Pydantic wraps this in a
                :class:`pydantic.ValidationError`. FastAPI's exception
                middleware translates the ``ValidationError`` to an
                HTTP ``422 Unprocessable Entity`` response at the
                controller boundary.
        """
        # Casting ``quantity`` (int) to ``Decimal`` keeps the
        # multiplication in the exact-arithmetic domain. Multiplying
        # a ``Decimal`` by a Python ``int`` is also exact, but the
        # explicit cast documents the intent and matches the type
        # of both other operands.
        expected = self.unit_price * Decimal(self.quantity)
        if self.line_total != expected:
            raise ValueError(
                f"line_total {self.line_total} does not equal "
                f"quantity * unit_price ({self.quantity} * "
                f"{self.unit_price} = {expected})"
            )
        return self


# =============================================================================
# Public surface
# =============================================================================

__all__ = [
    "MAX_QUANTITY_PER_ITEM",
    "OrderItem",
]
