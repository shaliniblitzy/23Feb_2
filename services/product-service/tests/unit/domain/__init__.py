"""Unit tests for the Product Service domain layer.

This package contains synchronous, hermetic, Pydantic-only unit tests
for the three aggregate models and one value object that compose the
:mod:`src.domain` layer of the Product Service:

* :class:`src.domain.product.Product` (aggregate root) and
  :class:`src.domain.product.ProductStatus` (StrEnum) are exercised by
  :mod:`.test_product`.
* :class:`src.domain.variant.Variant` (frozen value object embedded in
  Product) is exercised inside :mod:`.test_product` (under the
  ``TestVariantValueObject`` class).
* :class:`src.domain.category.Category` (aggregate root) is exercised by
  :mod:`.test_category`.
* :class:`src.domain.product_media.ProductMedia` (aggregate root) and
  :class:`src.domain.product_media.MediaKind` (StrEnum) are exercised by
  :mod:`.test_product_media`.

These tests are the **innermost layer** of the Product Service test
pyramid:

* They have ZERO I/O dependencies — no MongoDB (real or via
  ``mongomock-motor``), no Kafka, no HTTPX, no FastAPI, no DI container.
* They have ZERO framework dependencies beyond Pydantic v2,
  ``python-slugify``, and the Python standard library.
* They are entirely **synchronous** — no ``@pytest.mark.asyncio``
  markers, no ``async def test_...`` functions. The domain layer is
  pure code with no awaitables.
* They run in microseconds; CI runs hundreds of them per second.

Authority and scope:

* Mandated by AAP Section 0.5.2.6 Group 6 (Tests) and explicitly in
  scope per AAP Section 0.6.1 (``services/*/tests/unit/**/*``).
* Aligned with AAP Section 0.4.4 (the ``product_db`` schema: collections
  ``products``, ``categories``, ``product_media``).
* Aligned with AAP R-7 (MongoDB / flexible schema) — the aggregates use
  ``dict[str, Any]`` for ``Product.attributes`` to support evolving
  catalog data without schema migrations.

What these tests verify:

* Pydantic field validation (required fields, type coercion,
  pattern/length/numeric bounds, ``extra='forbid'``).
* SKU and slug pattern enforcement (regex, reserved-prefix rejection).
* Slug auto-generation via ``python-slugify`` (lowercase
  normalization, transliteration, length capping, fallback to
  ``"product"``).
* Currency allow-list (USD/EUR/GBP/INR/AUD/CAD) and case upcasting.
* Price bounds and decimal-place constraints (Decimal, max 2 dp,
  ``[0, 1_000_000.00]``).
* Category tree invariants (root vs child, materialized path,
  cycle detection, max depth).
* Frozen-aggregate immutability (assignment fails; ``apply_changes``
  rejects identity fields ``id``, ``sku``, ``parent_id``,
  ``product_id``, ``version``, ``created_at``).
* ``apply_changes`` semantics — returns a NEW instance, bumps
  ``updated_at``, does NOT increment ``version`` (version increment is
  the repository's atomic responsibility via conditional ``$inc`` in
  ``findOneAndUpdate``).
* Slug auto-regeneration on name change unless an explicit slug is
  supplied to ``apply_changes``.
* Embedded ``Variant`` value objects with SKU uniqueness within a
  single product, attribute key/value patterns, and signed
  ``price_adjustment`` bounds.
* Datetime invariants (timezone-aware required;
  ``updated_at >= created_at``).
* Version semantics (``version`` starts at 1; must be ``>= 1``).
* Event envelope serialization (per AAP R-33: self-contained
  payloads). Decimals are serialized as strings to preserve precision
  through JSON; datetimes as ISO 8601; enums as their raw string
  values; nested aggregates flattened to dicts.

Files in this package:

* :mod:`.test_product` — Product aggregate + ProductStatus enum +
  embedded Variant value object.
* :mod:`.test_category` — Category aggregate + materialized path
  invariants + cycle detection.
* :mod:`.test_product_media` — ProductMedia aggregate + MediaKind enum.

Sibling test packages (NOT covered here):

* ``tests.unit.repository`` — Repository tests using
  ``mongomock-motor`` (async I/O against an in-process MongoDB double).
* ``tests.unit.events`` — Kafka event payload builders + topic naming.
* ``tests.unit.controllers`` — FastAPI route tests via ``httpx.AsyncClient``.
* ``tests.unit.resilience`` — Retry policy + circuit breaker tests.

Cross-references:

* Production code under test: :mod:`src.domain` (the
  ``services/product-service/src/domain/`` package, exporting the
  aggregates, value objects, enums, commands, and exceptions).
* Repository contract that depends on these aggregates' invariants:
  :mod:`src.repository`.
* Event producers that emit ``to_event_envelope()`` output:
  :mod:`src.events`.

This package marker is intentionally empty (docstring only) — it
contains no imports, no ``__all__``, no test discovery hooks, and no
runtime code. Pytest collects test files in this directory by
walking the package tree.
"""
