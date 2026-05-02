"""Embedded ``Variant`` value object for the Product Service domain layer.

A :class:`Variant` represents a specific buyable variation of a product
(e.g., ``{"color": "red", "size": "M"}`` distinguishes one variant from
another). Variants are EMBEDDED inside the parent :class:`Product`
document — they have no independent ``id``, no ``version``, and no
independent lifecycle. They are constructed inline by
:meth:`src.domain.product.Product.new` and are immutable post-creation.

The :class:`Variant` class is a frozen Pydantic v2 ``BaseModel`` with no
peer-domain imports — it is purely a data class with declarative
validation. This deliberate self-containment keeps the import graph
acyclic and the domain leaf cheap to import in tests.

Variant SKU uniqueness within a single product's ``variants`` list is
enforced at the :class:`src.domain.product.Product` level (cross-cutting
invariant); this module enforces only field-level validation.

Pure domain layer — no MongoDB / Kafka / HTTP imports.

Design principles
-----------------
1. **Value object, not aggregate.** A :class:`Variant` has no identity
   beyond the values of its fields; equality is structural and its
   lifecycle is bound to the parent :class:`Product` document. This
   mirrors the Order Service's :class:`OrderItem` value-object pattern
   (see ``services/order-service/src/domain/order_item.py``) and the
   Product Service domain folder spec, which states explicitly:
   "**Variant is a value object, not an aggregate** — it has NO ``id``,
   NO ``version``, and is embedded inside Product's document."

2. **No factory method.** Per the folder spec: "No factory — Variant is
   constructed inline by ``Product.new`` and is immutable
   post-creation." Callers construct ``Variant(sku=..., attributes=...,
   price_adjustment=..., available=...)`` directly with keyword
   arguments. This keeps the value object minimal and emphasizes its
   immutable, "data-only" nature.

3. **Frozen / immutable.** ``model_config.frozen=True`` prevents
   accidental mutation in repositories or handlers. To "modify" a
   variant, callers MUST construct a new :class:`Variant` with the
   updated fields and replace it in the parent product's ``variants``
   list, then invoke ``Product.apply_changes`` (which itself creates a
   new ``Product`` instance via :meth:`pydantic.BaseModel.model_copy`).

4. **Strict typing.** :class:`decimal.Decimal` for the monetary
   ``price_adjustment`` field (AAP R-26 — Decimal not float for monetary
   amounts); :class:`str` for ``sku``; ``dict[str, str]`` for
   ``attributes`` (deliberately constrained to string values for
   predictable variant matching, NOT ``dict[str, Any]``).

5. **No I/O, no logging, no module-level side effects.** Only the
   module-level constants, precompiled regex patterns, and class
   definition are evaluated at import time. Keeps unit-test startup
   cheap and prevents the module from accidentally pulling in
   framework-specific code paths.

Field summary (per the Product Service domain folder spec)
----------------------------------------------------------
* ``sku: str``               — variant-level SKU; must be unique within
                                a single product's variants list
                                (uniqueness enforced by Product).
* ``attributes: dict[str, str]`` — free-form variant-distinguishing
                                attributes. Keys are lowercase
                                identifiers; values are non-empty
                                strings.
* ``price_adjustment: Decimal`` — signed adjustment relative to the
                                parent product's price; positive =
                                upcharge; negative = discount; zero =
                                no change.
* ``available: bool``        — whether this variant is currently
                                orderable.

Why ``attributes`` is ``dict[str, str]`` (not ``dict[str, Any]``)?
* Variants distinguish on simple key-value pairs (color, size, etc.).
  Constraining the value type to ``str`` keeps variant matching
  predictable and serializes cleanly into MongoDB and Kafka event
  payloads. The product-level ``attributes`` field uses ``dict[str,
  Any]`` for catalog-wide flexibility, but variant-level attributes
  are deliberately constrained.

Why the attribute key pattern ``^[a-z][a-z0-9_]{0,63}$``?
* Enforces a uniform naming convention (lowercase, snake_case-ish) for
  variant-distinguishing attributes. Clients submitting ``Color`` or
  ``Size`` (mixed case) will get a clear error pointing to the
  canonical lowercase form, preventing accidental case-divergence
  between products.

Why ``price_adjustment`` is signed?
* Supports both upcharges (e.g., XL is +$2) and discounts (e.g.,
  last-of-stock is -$5). The lower / upper bounds are deliberately
  generous yet finite to defend against accidental misconfiguration
  (a runaway ``-1e12`` would otherwise zero-out the price).

Authoritative references
------------------------
* AAP Section 0.1.1 Component #4 — Product Service overview.
* AAP Section 0.4.4 — ``product_db`` MongoDB ``products`` collection
  ownership; variants are an embedded array inside ``products``
  documents.
* AAP Section 0.5.2.2 bullet 4 — Product Service implementation plan.
* AAP R-7 — MongoDB flexible schema enables embedded variants without
  a separate collection.
* AAP R-26 — Decimal not float for monetary amounts.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Annotated, Final

from pydantic import BaseModel, ConfigDict, Field, field_validator


# =============================================================================
# Module-level constants
# =============================================================================
#
# All length / count ceilings exposed on this module are typed
# :class:`typing.Final` so static analyzers (mypy) prevent accidental
# reassignment elsewhere in the codebase. Tests can import these symbols
# directly to verify boundary semantics without spinning up the
# configuration layer.

#: Maximum length of a variant SKU string (in characters).
#:
#: Mirrors the conventional column width for SKU columns in OLTP
#: schemas and matches the upper bound of the SKU regex pattern (a
#: leading alphanumeric plus up to 63 trailing characters, totaling
#: 64). 64 chars is generous yet bounds DoS-via-large-key attacks
#: against the unique-SKU index Product enforces over its variants.
MAX_VARIANT_SKU_LENGTH: Final[int] = 64

#: Maximum length of an attribute key (in characters).
#:
#: Variant attribute keys are short, snake_case identifiers (e.g.,
#: ``color``, ``size``, ``material``) — 64 is far more than typical
#: usage requires but matches :data:`MAX_VARIANT_SKU_LENGTH` for a
#: predictable upper bound.
MAX_ATTRIBUTE_KEY_LENGTH: Final[int] = 64

#: Maximum length of an attribute value (in characters).
#:
#: Attribute values are short human-readable strings (e.g., ``red``,
#: ``M``, ``stainless steel``). 256 chars caps any single value at a
#: reasonable upper bound — long enough to accommodate descriptive
#: values, short enough to keep the embedded variant array
#: lightweight inside the parent product document.
MAX_ATTRIBUTE_VALUE_LENGTH: Final[int] = 256

#: Maximum number of attribute key/value pairs per variant.
#:
#: Real-world variants rarely have more than 3-5 distinguishing
#: attributes (color, size, material, finish, scent). 32 is generous
#: yet bounds the size of the embedded variant array's individual
#: entries inside the parent product document.
MAX_ATTRIBUTES_PER_VARIANT: Final[int] = 32

#: Lower bound for :attr:`Variant.price_adjustment`.
#:
#: ``Decimal("-1000000.00")`` permits a deep discount adjustment but
#: rejects nonsense values like ``-1e12`` that would silently zero-out
#: any conceivable parent product price. Bounded by accident-input
#: defense rather than by domain semantics.
MIN_PRICE_ADJUSTMENT: Final[Decimal] = Decimal("-1000000.00")

#: Upper bound for :attr:`Variant.price_adjustment`.
#:
#: Symmetric counterpart to :data:`MIN_PRICE_ADJUSTMENT`. ``Decimal(
#: "1000000.00")`` permits a substantial upcharge adjustment but
#: rejects nonsense values that would silently inflate any conceivable
#: parent product price.
MAX_PRICE_ADJUSTMENT: Final[Decimal] = Decimal("1000000.00")


# =============================================================================
# Precompiled regex patterns (private)
# =============================================================================
#
# Compiled once at module load so that field-level validation does not
# pay the regex compilation cost on every model construction. Both
# patterns are referenced by Pydantic ``Field(pattern=...)`` constraints
# (which need the source string) and by the
# :meth:`Variant._validate_attributes` field validator (which needs the
# compiled :class:`re.Pattern` object).

#: Variant SKU pattern: alphanumeric + hyphen + underscore, must start
#: with an alphanumeric character; total length capped at 64 chars
#: (1 leading + up to 63 trailing).
#:
#: Examples accepted:  ``SKU-1``, ``SKU-1-RED-M``, ``ABC_123``,
#:                     ``a-b_c-1``.
#: Examples rejected:  ``-SKU`` (leading hyphen), ``SKU 1`` (space),
#:                     ``SKU!`` (punctuation), empty string,
#:                     65+ chars.
#:
#: Anchored at both ends (``^...$``) so partial matches do not pass.
#: The same source string is shared with the Pydantic
#: ``Field(pattern=...)`` constraint on :attr:`Variant.sku` so the
#: rule is declared exactly once.
_VARIANT_SKU_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,63}$"
)

#: Attribute-key pattern: lowercase letters / digits / underscores;
#: must start with a lowercase letter; total length capped at 64
#: chars (1 leading + up to 63 trailing).
#:
#: Examples accepted:  ``color``, ``size``, ``shoe_size``,
#:                     ``material_2``.
#: Examples rejected:  ``Color`` (uppercase), ``2_size`` (leading
#:                     digit), ``shoe-size`` (hyphen), empty key.
#:
#: Lowercase is enforced to keep variant matching deterministic — a
#: client passing ``Color`` would otherwise create a variant
#: indistinguishable to humans from one with key ``color`` but
#: NOT-equal under string comparison.
_ATTRIBUTE_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-z][a-z0-9_]{0,63}$"
)


# =============================================================================
# Value object
# =============================================================================


class Variant(BaseModel):
    """Embedded variant value object for a parent :class:`Product`.

    Mirrors a single entry in the embedded ``variants`` array inside a
    document of the ``products`` collection in the Product Service's
    private ``product_db`` (MongoDB, per AAP Section 0.4.4). Owned
    exclusively by the Product Service (AAP R-6 — database per
    service).

    Variants are FROZEN: value objects are inherently immutable in
    Domain-Driven Design. To "modify" a variant, callers MUST construct
    a new :class:`Variant` with the updated fields and replace it in
    the parent product's ``variants`` list, then call
    :meth:`src.domain.product.Product.apply_changes` (which itself
    creates a new ``Product`` instance via
    :meth:`pydantic.BaseModel.model_copy`). Direct field assignment
    raises :class:`pydantic.ValidationError` at runtime.

    Validators applied:
        * ``sku`` matches :data:`_VARIANT_SKU_PATTERN`
          (``^[A-Za-z0-9][A-Za-z0-9_\\-]{0,63}$``); length bounded
          ``[1, MAX_VARIANT_SKU_LENGTH]``.
        * ``attributes`` is a ``dict[str, str]`` with at most
          :data:`MAX_ATTRIBUTES_PER_VARIANT` entries; each key matches
          :data:`_ATTRIBUTE_KEY_PATTERN`
          (``^[a-z][a-z0-9_]{0,63}$``); each value is a non-empty
          (after :meth:`str.strip`) string of at most
          :data:`MAX_ATTRIBUTE_VALUE_LENGTH` characters. Enforced by
          the :meth:`_validate_attributes` ``field_validator``.
        * ``price_adjustment`` is a :class:`decimal.Decimal` bounded
          ``[MIN_PRICE_ADJUSTMENT, MAX_PRICE_ADJUSTMENT]`` with at
          most 18 total digits and exactly 2 decimal places. Signed
          (positive = upcharge; negative = discount; zero = no
          change). Defaults to ``Decimal("0")``.
        * ``available`` is a ``bool``; defaults to ``True``.

    SKU uniqueness within a single product's variants list is a
    cross-cutting invariant enforced at the :class:`Product` level
    (in ``Product._validate_variant_skus_unique``). Enforcing it
    here would require seeing the entire variant list, which a
    value object should not assume.

    Attributes:
        sku: Variant-level SKU. Must be unique within a single
            product's ``variants`` list (uniqueness enforced by
            :class:`Product`, NOT by this class).
        attributes: Free-form variant-distinguishing attributes
            (e.g., ``{"color": "red", "size": "M"}``). Keys are
            lowercase identifiers; values are non-empty strings.
            Defaults to an empty dict.
        price_adjustment: Decimal adjustment relative to the parent
            product's ``price``. Positive = upcharge; negative =
            discount; ``Decimal("0")`` = no change. Defaults to
            ``Decimal("0")``.
        available: Whether this variant is currently orderable.
            ``False`` indicates the variant is hidden from the
            storefront / not purchasable. Defaults to ``True``.

    Example:
        >>> from decimal import Decimal
        >>> v = Variant(
        ...     sku="SKU-1-RED-M",
        ...     attributes={"color": "red", "size": "M"},
        ...     price_adjustment=Decimal("1.50"),
        ... )
        >>> v.available
        True
        >>> v.price_adjustment
        Decimal('1.50')
        >>> v.attributes["color"]
        'red'
    """

    # ------------------------------------------------------------------
    # Model configuration (frozen / strict)
    # ------------------------------------------------------------------
    #
    # ``frozen=True``                — immutability (DDD value object).
    #                                  Prevents accidental mutation in
    #                                  repositories or handlers. Direct
    #                                  field assignment after
    #                                  construction raises
    #                                  ``pydantic.ValidationError``.
    # ``str_strip_whitespace=True``  — strip surrounding whitespace
    #                                  from any string-typed field
    #                                  (``sku`` and the values inside
    #                                  ``attributes``). Defends against
    #                                  copy-paste artifacts from API
    #                                  request bodies and Kafka event
    #                                  payloads.
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

    sku: Annotated[
        str,
        Field(
            min_length=1,
            max_length=MAX_VARIANT_SKU_LENGTH,
            pattern=_VARIANT_SKU_PATTERN.pattern,
            description=(
                "Variant-level SKU. Must be unique within a single "
                "product's variants list (uniqueness enforced by "
                "Product, NOT by this class). Pattern: "
                "alphanumeric leading character followed by up to 63 "
                "alphanumerics / hyphens / underscores."
            ),
        ),
    ]

    attributes: Annotated[
        dict[str, str],
        Field(
            default_factory=dict,
            max_length=MAX_ATTRIBUTES_PER_VARIANT,
            description=(
                "Free-form variant-distinguishing attributes (e.g., "
                "{'color': 'red', 'size': 'M'}). Keys are lowercase "
                "identifiers (snake_case); values are non-empty "
                "strings. Bounded to "
                f"{MAX_ATTRIBUTES_PER_VARIANT} entries per variant. "
                "Defaults to an empty dict."
            ),
        ),
    ]

    price_adjustment: Annotated[
        Decimal,
        Field(
            default=Decimal("0"),
            ge=MIN_PRICE_ADJUSTMENT,
            le=MAX_PRICE_ADJUSTMENT,
            max_digits=18,
            decimal_places=2,
            description=(
                "Signed Decimal adjustment relative to the parent "
                "product's price. Positive = upcharge; negative = "
                "discount; Decimal('0') = no change. Bounded "
                f"[{MIN_PRICE_ADJUSTMENT}, {MAX_PRICE_ADJUSTMENT}] "
                "to defend against accidental misconfiguration. "
                "Decimal (not float) for exact monetary arithmetic "
                "(AAP R-26)."
            ),
        ),
    ] = Decimal("0")

    available: bool = Field(
        default=True,
        description=(
            "Whether this variant is currently orderable. False "
            "indicates the variant is hidden from the storefront "
            "and not purchasable. Defaults to True."
        ),
    )

    # ------------------------------------------------------------------
    # Field validators
    # ------------------------------------------------------------------

    @field_validator("attributes")
    @classmethod
    def _validate_attributes(cls, v: dict[str, str]) -> dict[str, str]:
        """Enforce per-key / per-value constraints on ``attributes``.

        Pydantic's per-field declarative constraints validate the dict
        as a whole (length / type), but the inner key and value
        constraints — lowercase snake_case keys, bounded key/value
        length, non-empty (after strip) string values — must be
        validated explicitly. This validator runs in mode ``"after"``
        (the implicit default for ``@field_validator``) so the dict
        has already been coerced into ``dict[str, str]`` by the time
        this method runs; the explicit ``isinstance`` checks below
        are defense-in-depth against future schema evolution rather
        than the primary type guard.

        Each key is required to:

          * Be a Python :class:`str` (defensive — Pydantic should have
            already enforced this).
          * Match :data:`_ATTRIBUTE_KEY_PATTERN`
            (``^[a-z][a-z0-9_]{0,63}$``) — lowercase letter leading,
            followed by zero or more lowercase letters / digits /
            underscores.
          * Have length at most :data:`MAX_ATTRIBUTE_KEY_LENGTH`. The
            regex already caps total length at 64, so this check is
            belt-and-braces against a future relaxation of the regex.

        Each value is required to:

          * Be a Python :class:`str` (defensive — Pydantic should
            have already enforced this).
          * Have length at most :data:`MAX_ATTRIBUTE_VALUE_LENGTH`.
          * Be non-empty after :meth:`str.strip` (rejects values that
            consist of only whitespace or the empty string). Note
            ``model_config.str_strip_whitespace=True`` strips at the
            top-level ``str`` field type but does NOT recurse into
            container values, so an explicit ``strip()`` check here
            is required.

        Args:
            v: The candidate ``attributes`` dict to validate.

        Returns:
            The same dict ``v`` (Pydantic ``field_validator`` returns
            the validated value; no copy is made).

        Raises:
            ValueError: When any key fails the pattern / length check
                or any value is the wrong type / too long / empty
                after stripping. Pydantic wraps this in a
                :class:`pydantic.ValidationError`. The FastAPI
                exception middleware translates the
                :class:`pydantic.ValidationError` to an HTTP
                ``422 Unprocessable Entity`` response at the
                controller boundary.
        """
        for key, value in v.items():
            # --- Key validation ------------------------------------
            if not isinstance(key, str) or not _ATTRIBUTE_KEY_PATTERN.match(key):
                raise ValueError(
                    f"attribute key must match "
                    f"{_ATTRIBUTE_KEY_PATTERN.pattern!r}; got {key!r}"
                )
            if len(key) > MAX_ATTRIBUTE_KEY_LENGTH:
                # Belt-and-braces: the regex already caps length at 64.
                raise ValueError(
                    f"attribute key must be at most "
                    f"{MAX_ATTRIBUTE_KEY_LENGTH} chars; got {key!r}"
                )

            # --- Value validation ----------------------------------
            if not isinstance(value, str):
                raise ValueError(
                    f"attribute value for key {key!r} must be a "
                    f"string; got {type(value).__name__}"
                )
            if len(value) > MAX_ATTRIBUTE_VALUE_LENGTH:
                raise ValueError(
                    f"attribute value for key {key!r} exceeds "
                    f"{MAX_ATTRIBUTE_VALUE_LENGTH} chars"
                )
            if not value.strip():
                # Reject values that are empty after stripping
                # whitespace. ``str_strip_whitespace=True`` operates
                # only on the top-level ``str`` field type; it does
                # NOT recurse into the values of a ``dict[str, str]``
                # field, so this explicit check is required.
                raise ValueError(
                    f"attribute value for key {key!r} must not be empty"
                )

        return v


# =============================================================================
# Public surface
# =============================================================================
#
# Sorted alphabetically (the standard convention for ``__all__``) so
# diff hygiene is preserved on future additions.

__all__ = [
    "MAX_ATTRIBUTE_KEY_LENGTH",
    "MAX_ATTRIBUTE_VALUE_LENGTH",
    "MAX_ATTRIBUTES_PER_VARIANT",
    "MAX_PRICE_ADJUSTMENT",
    "MAX_VARIANT_SKU_LENGTH",
    "MIN_PRICE_ADJUSTMENT",
    "Variant",
]
