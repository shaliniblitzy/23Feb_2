"""Unit tests for the Product Service MongoDB repository layer.

This subpackage houses fast, hermetic unit tests for the repository
classes in :mod:`src.repository`:

- :class:`src.repository.product_repository.ProductRepository`
  — exercised by :mod:`tests.unit.repository.test_product_repo`.
- :class:`src.repository.category_repository.CategoryRepository`
  — exercised by :mod:`tests.unit.repository.test_category_repo`.

These tests run the production repository code path *unchanged* against
``mongomock_motor.AsyncMongoMockClient`` — an async-compatible MongoDB
mock that fully implements the ``motor.motor_asyncio`` interface — so
no real MongoDB cluster is required at the unit-test tier.

Coverage focus
--------------

- **Query builder composition** — repositories correctly construct
  MongoDB filter dicts from input arguments (``$text``, ``$in``,
  ``$ne``, multikey lookups on ``category_ids`` / ``path``).
- **Optimistic concurrency** (AAP R-9) — ``find_one_and_update`` with
  a ``version`` filter and ``$inc: {version: 1}``; conflicts raise
  :class:`src.domain.exceptions.VersionConflict` with both the
  ``expected_version`` and (where available) the ``actual_version``.
- **Soft-delete semantics** — ``ProductRepository.delete_product``
  flips ``status`` to ``"deprecated"`` and excludes deprecated docs
  from default reads (Product Service-specific).
- **Slug-uniqueness scope** — products use a global unique slug;
  categories use a sibling-scoped compound unique index on
  ``(parent_id, slug)``; ``DuplicateSlug`` is raised with the
  appropriate ``scope`` ("product" vs "category-sibling").
- **Materialized-path tree** — :class:`CategoryRepository` uses
  ``path: list[str]`` (ancestor ids) with a multikey index for
  subtree queries; ``reparent_category`` rewrites descendant paths
  and bumps every descendant's version.
- **Index awareness** — ``ensure_indexes`` is idempotent and creates
  the unique constraints that drive the ``DuplicateKeyError``
  classification logic.

Hermetic contract
-----------------

- **NO real MongoDB**: tests use the ``mongo_client``,
  ``mongo_database``, and ``mongo_collections`` fixtures from
  :mod:`tests.unit.conftest`, which provide
  ``mongomock_motor.AsyncMongoMockClient`` instances mimicking
  ``motor.motor_asyncio.AsyncIOMotorClient`` exactly.
- **NO Testcontainers** — no Docker, no networking.
- **NO real Kafka, HTTPX, or wall-clock dependencies** — the
  repository layer doesn't touch these.
- **Strict ``@pytest.mark.asyncio`` discipline** — the parent unit
  conftest's autouse fixture warns when an async test is missing
  the marker.

See also
--------

- AAP Section 0.5.2.6 Group 6 — Tests.
- AAP Section 0.4.4 — ``product_db`` schema (collections ``products``,
  ``categories``, ``product_media``).
- AAP R-6 — database per service strictly enforced.
- AAP R-7 — MongoDB chosen for the Product Service for flexible
  schema across varied catalog attributes.
- AAP R-9 — optimistic concurrency on ``products.version`` /
  ``categories.version`` enforced via ``findOneAndUpdate`` with the
  ``version`` filter and ``$inc: {version: 1}``; conflicts raise
  :class:`VersionConflict`.
"""
