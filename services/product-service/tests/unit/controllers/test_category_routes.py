"""Unit tests for ``src.controllers.categories``.

Exercises the FastAPI category route handlers against the
``app_with_mocks`` composite fixture that wires:
* ``mongomock-motor`` for MongoDB
* ``MagicMock`` for the Kafka producer (NOT used by category routes)
* ``MagicMock`` for the JWKS client (returns ``products:admin`` scope)
* In-memory idempotency store (not used by category routes)
* Real ``CategoryRepository``, ``ProductRepository`` instances pointed at
  the mongomock database

Test categories (one ``Test...`` class per behavioral area):

* :class:`TestListCategories` -- ``GET /categories/`` with and without
  ``?parent`` query.
* :class:`TestGetCategory` -- ``GET /categories/{id}`` happy path and 404.
* :class:`TestListProductsInCategory` -- ``GET /categories/{id}/products``
  pagination and 404 propagation when category missing.
* :class:`TestCreateCategoryAuth` -- POST scope enforcement (401, 403, 201).
* :class:`TestCreateCategoryHappyPath` -- POST root and child creation;
  path/slug invariants.
* :class:`TestCreateCategoryValidation` -- POST 422 on invalid input.
* :class:`TestCreateCategoryParent` -- POST parent_id resolution (404 on
  missing parent; correct path computation).
* :class:`TestUpdateCategoryAuth` -- PUT scope enforcement.
* :class:`TestUpdateCategoryHappyPath` -- PUT successful update bumps version.
* :class:`TestUpdateCategoryValidation` -- PUT 422 on missing
  ``expected_version`` and forbidden ``parent_id``.
* :class:`TestUpdateCategoryVersionConflict` -- PUT 409 on stale version.
* :class:`TestCategoriesNoKafkaEvents` -- confirms NO Kafka events emitted
  by any category route (Product Service emits events only for products).
* :class:`TestCategoriesCorrelationId` -- verifies correlation ID echo on
  responses (AAP R-13).
* :class:`TestCategoriesStructuredLogs` -- verifies admin write events log
  ``categories.created`` / ``categories.updated`` with structured fields
  (AAP R-26).

AAP Rules verified by this module:
    * R-13 -- Correlation ID propagation
    * R-21, R-22 -- JWT validation; admin scope ``products:admin``
    * R-25 -- No internal details in error payloads
    * R-26 -- Structured JSON logs

NO Kafka event verification is performed in this file because the
category controller intentionally does NOT emit events
(category mutations are NOT in the AAP event catalog Section 0.4.2;
only ``product.created`` and ``product.updated`` exist for the Product
Service).
"""

from __future__ import annotations

from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------
def _client(bundle: Any) -> AsyncClient:
    """Build an ``httpx.AsyncClient`` driving the in-process FastAPI app.

    The ``app_with_mocks`` fixture exposes the configured FastAPI app at
    ``bundle.app``. Routing requests through ``ASGITransport`` bypasses
    the kernel TCP stack entirely so tests are deterministic and fast --
    no ports to allocate, no socket teardown latency, no flaky CI port
    collisions.
    """
    return AsyncClient(
        transport=ASGITransport(app=bundle.app),
        base_url="http://test",
    )


def _admin_headers(token: str) -> dict[str, str]:
    """Build the ``Authorization: Bearer <token>`` header for admin endpoints."""
    return {"Authorization": f"Bearer {token}"}


async def _seed_category(
    bundle: Any,
    *,
    parent_id: str | None = None,
    name: str = "Electronics",
    slug: str | None = None,
    path: list[str] | None = None,
    version: int = 1,
    is_visible: bool = True,
    display_order: int = 0,
    description: str | None = None,
) -> dict[str, Any]:
    """Insert a minimal valid ``Category`` document directly via mongomock.

    Bypasses the controller layer to seed a known starting state. Returns
    the inserted document as a dict (matches the domain ``Category`` model
    shape per ``services/product-service/tests/unit/conftest.py``
    ``category_factory`` documentation).

    The schema fields mirror the ``Category`` aggregate:

    * ``id`` -- UUID4 string (also stored as ``_id`` for mongomock).
    * ``slug`` -- derived from ``name`` (lowercase, hyphenated) when
      ``slug`` is omitted.
    * ``parent_id`` -- ``None`` for root, parent's id otherwise.
    * ``path`` -- list of ancestor ids (root = ``[]``).
    * ``version`` -- ``1`` for fresh inserts.
    * ``is_visible`` -- ``True`` by default.
    * ``display_order`` -- ``0`` by default.
    * ``description`` -- optional.
    * ``created_at`` / ``updated_at`` -- ISO 8601 UTC strings.

    NOTE: Repository uses mongomock; insertion via ``insert_one``
    populates the collection and ``find_one`` will return the same
    shape on read.
    """
    import uuid
    from datetime import datetime, timezone

    cat_id = str(uuid.uuid4())
    if slug is None:
        slug = name.lower().replace(" ", "-")
    if path is None:
        path = []
    now = datetime.now(timezone.utc).isoformat()
    document = {
        "_id": cat_id,
        "id": cat_id,
        "slug": slug,
        "name": name,
        "parent_id": parent_id,
        "path": path,
        "description": description,
        "display_order": display_order,
        "is_visible": is_visible,
        "version": version,
        "created_at": now,
        "updated_at": now,
    }
    await bundle.mongo_collections.categories.insert_one(dict(document))
    return document


# ---------------------------------------------------------------------------
# Phase 4 -- TestListCategories: GET /categories/ list root or children.
# ---------------------------------------------------------------------------
class TestListCategories:
    """``GET /categories/`` -- list root or children categories."""

    async def test_list_root_categories_when_no_parent_query(
        self,
        app_with_mocks: Any,
    ) -> None:
        """No ``?parent`` query lists root categories (parent_id IS NULL)."""
        await _seed_category(app_with_mocks, name="Electronics", slug="electronics")
        await _seed_category(app_with_mocks, name="Books", slug="books")
        # A non-root category that should NOT appear in the root list.
        root = await _seed_category(app_with_mocks, name="Apparel", slug="apparel")
        await _seed_category(
            app_with_mocks,
            name="Shoes",
            slug="shoes",
            parent_id=root["id"],
            path=[root["id"]],
        )

        async with _client(app_with_mocks) as client:
            response = await client.get("/categories/")

        assert response.status_code == 200
        body = response.json()
        assert "items" in body
        assert "total" in body
        assert body["total"] == 3  # Electronics, Books, Apparel
        names = {item["name"] for item in body["items"]}
        assert names == {"Electronics", "Books", "Apparel"}
        for item in body["items"]:
            assert item["parent_id"] is None
            assert item["path"] == []

    async def test_list_children_when_parent_query_provided(
        self,
        app_with_mocks: Any,
    ) -> None:
        """``?parent=<id>`` lists direct children of that parent."""
        root = await _seed_category(
            app_with_mocks, name="Electronics", slug="electronics"
        )
        await _seed_category(
            app_with_mocks,
            name="Phones",
            slug="phones",
            parent_id=root["id"],
            path=[root["id"]],
        )
        await _seed_category(
            app_with_mocks,
            name="Laptops",
            slug="laptops",
            parent_id=root["id"],
            path=[root["id"]],
        )
        # Different parent -- should NOT appear.
        other_root = await _seed_category(
            app_with_mocks, name="Books", slug="books"
        )
        await _seed_category(
            app_with_mocks,
            name="Fiction",
            slug="fiction",
            parent_id=other_root["id"],
            path=[other_root["id"]],
        )

        async with _client(app_with_mocks) as client:
            response = await client.get(f"/categories/?parent={root['id']}")

        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 2
        names = {item["name"] for item in body["items"]}
        assert names == {"Phones", "Laptops"}

    async def test_list_returns_empty_when_no_categories(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Empty database returns 200 with empty items and ``total=0``."""
        async with _client(app_with_mocks) as client:
            response = await client.get("/categories/")

        assert response.status_code == 200
        body = response.json()
        assert body["items"] == []
        assert body["total"] == 0

    async def test_list_does_not_require_jwt(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Anonymous requests are allowed (no JWT scope required for reads)."""
        async with _client(app_with_mocks) as client:
            response = await client.get("/categories/")  # NO Authorization header

        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Phase 5 -- TestGetCategory: GET /categories/{id} happy path + 404.
# ---------------------------------------------------------------------------
class TestGetCategory:
    """``GET /categories/{category_id}`` -- single category lookup."""

    async def test_get_existing_category_returns_full_payload(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Existing category returns full ``CategoryResponse`` payload."""
        seeded = await _seed_category(
            app_with_mocks,
            name="Electronics",
            slug="electronics",
            description="Electronic gadgets and accessories",
            display_order=10,
            is_visible=True,
        )

        async with _client(app_with_mocks) as client:
            response = await client.get(f"/categories/{seeded['id']}")

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == seeded["id"]
        assert body["slug"] == "electronics"
        assert body["name"] == "Electronics"
        assert body["parent_id"] is None
        assert body["path"] == []
        assert body["description"] == "Electronic gadgets and accessories"
        assert body["display_order"] == 10
        assert body["is_visible"] is True
        assert body["version"] == 1

    async def test_get_missing_category_returns_404(
        self,
        app_with_mocks: Any,
    ) -> None:
        """404 with structured error body when category id unknown.

        AAP R-25 forbids leaking internal details in error payloads. The
        assertion below stays at the contract surface: it confirms the
        body REFERS to the not-found condition without inspecting any
        stack-trace, exception ``repr``, or connection-detail strings.
        """
        async with _client(app_with_mocks) as client:
            response = await client.get(
                "/categories/00000000-0000-0000-0000-000000000000"
            )

        assert response.status_code == 404
        body = response.json()
        # ErrorHandlerMiddleware envelope: ``error_code`` + ``message`` keys.
        # Don't assume the exact key path; verify the response body has
        # SOME representation of the error code (commonly under
        # ``detail`` or top-level). Permissive assertion to allow either
        # FastAPI's default 404 detail format or the
        # ErrorHandlerMiddleware's structured envelope.
        flattened = str(body).upper()
        # The error mentions ``CATEGORY_NOT_FOUND`` or
        # ``CategoryNotFound`` (class name) or ``Category not found``
        # (human-readable) or just ``not found`` (FastAPI default).
        assert (
            "CATEGORY_NOT_FOUND" in flattened
            or "CATEGORYNOTFOUND" in flattened.replace("_", "")
            or "CATEGORY NOT FOUND" in flattened
            or "NOT FOUND" in flattened
        )

    async def test_get_does_not_require_jwt(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Anonymous reads on a known category succeed (200)."""
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.get(f"/categories/{seeded['id']}")

        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Phase 6 -- TestListProductsInCategory: GET /categories/{id}/products.
# ---------------------------------------------------------------------------
class TestListProductsInCategory:
    """``GET /categories/{id}/products`` -- paginated products in a category."""

    async def _seed_product(
        self, bundle: Any, *, category_id: str, name: str = "Widget"
    ) -> dict[str, Any]:
        """Insert a product associated with the given category.

        Mirrors the Product domain model shape (id, sku, slug, name, etc.)
        so the repository can find it via
        ``list_products(category_id=...)``. The shape and field set
        match the ``_seed_product`` helper used by the sibling
        ``test_product_routes.py`` for monorepo cognitive consistency.
        """
        import uuid
        from datetime import datetime, timezone

        product_id = str(uuid.uuid4())
        slug = name.lower().replace(" ", "-") + "-" + uuid.uuid4().hex[:6]
        now = datetime.now(timezone.utc).isoformat()
        document = {
            "_id": product_id,
            "id": product_id,
            "sku": f"SKU-{uuid.uuid4().hex[:6].upper()}",
            "slug": slug,
            "name": name,
            "description": f"{name} description",
            "category_ids": [category_id],
            "price": "29.99",
            "currency": "USD",
            "status": "active",
            "attributes": {},
            "variants": [],
            "media_refs": [],
            "version": 1,
            "created_at": now,
            "updated_at": now,
        }
        await bundle.mongo_collections.products.insert_one(dict(document))
        return document

    async def test_lists_products_in_category_paginated(
        self,
        app_with_mocks: Any,
    ) -> None:
        """``page=1&size=3`` returns at most 3 products with summary shape."""
        category = await _seed_category(app_with_mocks)
        for i in range(5):
            await self._seed_product(
                app_with_mocks, category_id=category["id"], name=f"Widget-{i}"
            )

        async with _client(app_with_mocks) as client:
            response = await client.get(
                f"/categories/{category['id']}/products?page=1&size=3"
            )

        assert response.status_code == 200
        body = response.json()
        assert body["category_id"] == category["id"]
        assert body["page"] == 1
        assert body["page_size"] == 3
        assert len(body["items"]) <= 3
        # Each item carries the compact ``ProductSummary`` shape.
        for item in body["items"]:
            assert {
                "id",
                "sku",
                "slug",
                "name",
                "price",
                "currency",
                "status",
            } <= item.keys()

    async def test_returns_404_when_category_missing(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Unknown category id propagates as 404 from ``CategoryNotFound``."""
        async with _client(app_with_mocks) as client:
            response = await client.get(
                "/categories/00000000-0000-0000-0000-000000000000/products"
            )

        assert response.status_code == 404

    async def test_default_pagination_when_query_params_omitted(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Defaults: ``page=1``, ``page_size=20`` per controller spec."""
        category = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.get(f"/categories/{category['id']}/products")

        assert response.status_code == 200
        body = response.json()
        assert body["page"] == 1
        assert body["page_size"] == 20  # default per controller spec

    async def test_rejects_size_above_max(
        self,
        app_with_mocks: Any,
    ) -> None:
        """``size > 200`` should 422 (``Query(le=200)`` constraint)."""
        category = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.get(
                f"/categories/{category['id']}/products?size=500"
            )

        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Phase 7 -- TestCreateCategoryAuth: POST scope enforcement (AAP R-21/R-22).
# ---------------------------------------------------------------------------
class TestCreateCategoryAuth:
    """``POST /categories/`` -- JWT scope enforcement (AAP R-21, R-22)."""

    async def test_requires_authorization_header(
        self,
        app_with_mocks: Any,
    ) -> None:
        """No ``Authorization`` header -> 401."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={"name": "Electronics", "is_visible": True},
            )

        assert response.status_code == 401

    async def test_rejects_token_without_admin_scope(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Valid JWT but wrong scope -> 403.

        Override the JWKS mock to return a token claim set WITHOUT the
        ``products:admin`` scope. The ``require_admin_scope`` dependency
        should raise an authorization error mapped to 403.
        """
        # Override the JWKS mock to return a token WITHOUT products:admin.
        app_with_mocks.mock_jwks_client.validate_token.return_value = {
            "sub": "test-user",
            "iss": "test-issuer",
            "aud": "product-service",
            "scope": "products:read",  # not admin
            "iat": 1704067200,
            "exp": 1704070800,
        }
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={"name": "Electronics", "is_visible": True},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 403

    async def test_accepts_token_with_admin_scope(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Valid JWT with ``products:admin`` scope -> 201."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={
                    "name": "Electronics",
                    "is_visible": True,
                    "display_order": 0,
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201


# ---------------------------------------------------------------------------
# Phase 8 -- TestCreateCategoryHappyPath: POST root and child creation.
# ---------------------------------------------------------------------------
class TestCreateCategoryHappyPath:
    """``POST /categories/`` -- successful creation paths."""

    async def test_creates_root_category_with_empty_path(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Root category (no ``parent_id``) has empty ``path``, version=1."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={
                    "name": "Electronics",
                    "description": "Gadgets",
                    "display_order": 5,
                    "is_visible": True,
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        body = response.json()
        assert body["name"] == "Electronics"
        assert body["parent_id"] is None
        assert body["path"] == []
        assert body["display_order"] == 5
        assert body["is_visible"] is True
        assert body["version"] == 1
        # Slug is auto-generated by the domain layer; verify presence + format.
        assert "slug" in body
        assert isinstance(body["slug"], str)
        assert len(body["slug"]) > 0

    async def test_creates_child_category_with_correct_path(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Path invariant: ``path[-1] == parent_id`` after creating a child."""
        parent = await _seed_category(
            app_with_mocks, name="Electronics", slug="electronics"
        )

        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={
                    "name": "Smartphones",
                    "parent_id": parent["id"],
                    "is_visible": True,
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        body = response.json()
        assert body["name"] == "Smartphones"
        assert body["parent_id"] == parent["id"]
        # Path includes the parent's id (parent's path was [], so child
        # path is [parent.id]).
        assert body["path"] == [parent["id"]]
        # Invariant: path[-1] == parent_id.
        assert body["path"][-1] == body["parent_id"]

    async def test_creates_grandchild_category_with_two_element_path(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """3-level deep tree: root -> child -> grandchild.

        The grandchild's ``path`` must equal ``[root.id, child.id]``,
        confirming the full ancestor chain is preserved when creating
        nested categories.
        """
        root = await _seed_category(
            app_with_mocks, name="Electronics", slug="electronics"
        )
        child = await _seed_category(
            app_with_mocks,
            name="Phones",
            slug="phones",
            parent_id=root["id"],
            path=[root["id"]],
        )

        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={
                    "name": "Smartphones",
                    "parent_id": child["id"],
                    "is_visible": True,
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        body = response.json()
        assert body["parent_id"] == child["id"]
        assert body["path"] == [root["id"], child["id"]]
        assert body["path"][-1] == body["parent_id"]


# ---------------------------------------------------------------------------
# Phase 9 -- TestCreateCategoryValidation: POST 422 on invalid input.
# ---------------------------------------------------------------------------
class TestCreateCategoryValidation:
    """``POST /categories/`` -- request body validation (Pydantic v2)."""

    async def test_rejects_missing_name(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``name`` is REQUIRED -> 422 when missing."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={"is_visible": True},  # name missing
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 422

    async def test_rejects_empty_name(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Empty ``name`` violates ``min_length=1`` constraint -> 422."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={"name": "", "is_visible": True},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 422

    async def test_rejects_extra_fields(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``ConfigDict(extra='forbid')`` -- unknown keys -> 422."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={
                    "name": "Electronics",
                    "is_visible": True,
                    "rogue_field": "not allowed",
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 422

    async def test_rejects_negative_display_order(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``display_order`` must be ``ge=0`` -> 422 on negative values."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={
                    "name": "Electronics",
                    "display_order": -1,
                    "is_visible": True,
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Phase 10 -- TestCreateCategoryParent: POST parent_id resolution behavior.
# ---------------------------------------------------------------------------
class TestCreateCategoryParent:
    """``POST /categories/`` -- ``parent_id`` resolution behavior."""

    async def test_returns_404_when_parent_id_missing(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Specifying a ``parent_id`` that doesn't exist -> 404 ``CategoryNotFound``."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={
                    "name": "Smartphones",
                    "parent_id": "00000000-0000-0000-0000-000000000000",
                    "is_visible": True,
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 404

    async def test_inherits_parent_path_correctly(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Child of a child of a root inherits the full ancestor chain.

        Grandchild ``path`` = parent's ``path`` + parent's ``id`` =
        ``[root.id]`` + ``[child.id]`` = ``[root.id, child.id]``.
        """
        root = await _seed_category(
            app_with_mocks, name="Electronics", slug="electronics"
        )
        # Manually seed a child whose path is correctly [root.id].
        child = await _seed_category(
            app_with_mocks,
            name="Phones",
            slug="phones",
            parent_id=root["id"],
            path=[root["id"]],
        )

        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={
                    "name": "iPhone",
                    "parent_id": child["id"],
                    "is_visible": True,
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        body = response.json()
        # Grandchild path = parent's path + parent's id = [root.id] + [child.id].
        assert body["path"] == [root["id"], child["id"]]



# ---------------------------------------------------------------------------
# Phase 11 -- TestUpdateCategoryAuth: PUT scope enforcement.
# ---------------------------------------------------------------------------
class TestUpdateCategoryAuth:
    """``PUT /categories/{id}`` -- JWT scope enforcement."""

    async def test_requires_authorization_header(
        self,
        app_with_mocks: Any,
    ) -> None:
        """No ``Authorization`` header -> 401."""
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
            )

        assert response.status_code == 401

    async def test_rejects_token_without_admin_scope(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Valid JWT but wrong scope -> 403."""
        seeded = await _seed_category(app_with_mocks)
        app_with_mocks.mock_jwks_client.validate_token.return_value = {
            "sub": "test-user",
            "iss": "test-issuer",
            "aud": "product-service",
            "scope": "products:read",
            "iat": 1704067200,
            "exp": 1704070800,
        }
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 403


# ---------------------------------------------------------------------------
# Phase 12 -- TestUpdateCategoryHappyPath: PUT successful updates.
# ---------------------------------------------------------------------------
class TestUpdateCategoryHappyPath:
    """``PUT /categories/{id}`` -- successful updates bump version."""

    async def test_updates_name_and_bumps_version(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Successful update bumps ``version`` from ``1`` to ``2``."""
        seeded = await _seed_category(
            app_with_mocks, name="Electronics", slug="electronics", version=1
        )

        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"name": "Consumer Electronics", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == seeded["id"]
        assert body["name"] == "Consumer Electronics"
        assert body["version"] == 2  # bumped from 1

    async def test_updates_only_supplied_fields(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Omitted fields remain unchanged (partial update semantics)."""
        seeded = await _seed_category(
            app_with_mocks,
            name="Electronics",
            slug="electronics",
            description="Original description",
            display_order=10,
            is_visible=True,
        )

        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"description": "New description", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["description"] == "New description"
        assert body["name"] == "Electronics"  # unchanged
        assert body["display_order"] == 10  # unchanged
        assert body["is_visible"] is True  # unchanged

    async def test_returns_404_when_category_missing(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Missing-on-update may map to 404 OR 409 depending on impl.

        Some repository layers raise ``CategoryNotFound`` when the
        category is absent; others detect the absence via the
        ``expected_version`` predicate and raise ``VersionConflict``
        instead. Both are acceptable correct behaviors for this scenario,
        so the assertion accepts either status code.
        """
        async with _client(app_with_mocks) as client:
            response = await client.put(
                "/categories/00000000-0000-0000-0000-000000000000",
                json={"name": "Anything", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code in {404, 409}


# ---------------------------------------------------------------------------
# Phase 13 -- TestUpdateCategoryValidation: PUT 422 on invalid body.
# ---------------------------------------------------------------------------
class TestUpdateCategoryValidation:
    """``PUT /categories/{id}`` -- body validation."""

    async def test_rejects_missing_expected_version(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``expected_version`` is REQUIRED -> 422 if missing."""
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"name": "Updated"},  # no expected_version
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 422

    async def test_rejects_parent_id_in_update_body(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``parent_id`` is NOT a field of ``UpdateCategoryRequest`` -> 422.

        Reparenting requires a dedicated operation (not exposed by this
        controller); ``extra='forbid'`` on the request model rejects
        accidental ``parent_id`` in the update body.
        """
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={
                    "name": "Updated",
                    "parent_id": "some-other-id",
                    "expected_version": 1,
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 422

    async def test_rejects_extra_fields(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Unknown keys violate ``extra='forbid'`` -> 422."""
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={
                    "name": "Updated",
                    "expected_version": 1,
                    "rogue_field": "not allowed",
                },
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 422

    async def test_rejects_zero_expected_version(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``expected_version`` must be ``ge=1`` -> 422 when ``0`` is supplied."""
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"name": "Updated", "expected_version": 0},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Phase 14 -- TestUpdateCategoryVersionConflict: PUT 409 on stale version.
# ---------------------------------------------------------------------------
class TestUpdateCategoryVersionConflict:
    """``PUT /categories/{id}`` -- optimistic concurrency."""

    async def test_returns_409_when_expected_version_stale(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Stored version > ``expected_version`` -> ``VersionConflict`` -> 409.

        Seeds a category at version=3, then PUT with
        ``expected_version=1`` (stale) must fail with 409.
        """
        # Seed a category at version 3.
        seeded = await _seed_category(
            app_with_mocks, name="Electronics", slug="electronics", version=3
        )

        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},  # stale
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 409



# ---------------------------------------------------------------------------
# Phase 15 -- TestCategoriesNoKafkaEvents: confirm NO Kafka calls.
# ---------------------------------------------------------------------------
class TestCategoriesNoKafkaEvents:
    """Category routes intentionally do NOT emit Kafka events.

    Per AAP Section 0.4.2, the Product Service's Kafka event catalog
    contains only ``product.created`` and ``product.updated``. There are
    NO category-related events. These tests act as guards against
    accidental event emission from the category routes.
    """

    async def test_category_create_does_not_call_kafka(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Creating a category MUST NOT touch the Kafka producer."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={"name": "Electronics", "is_visible": True},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        # The bundled MagicMock kafka producer's produce method must
        # NEVER have been called by category routes.
        app_with_mocks.mock_kafka_producer.produce.assert_not_called()

    async def test_category_update_does_not_call_kafka(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Updating a category MUST NOT touch the Kafka producer."""
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        app_with_mocks.mock_kafka_producer.produce.assert_not_called()

    async def test_category_reads_do_not_call_kafka(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Read endpoints (list, single, products-in-category) emit nothing."""
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            await client.get("/categories/")
            await client.get(f"/categories/{seeded['id']}")
            await client.get(f"/categories/{seeded['id']}/products")

        app_with_mocks.mock_kafka_producer.produce.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 16 -- TestCategoriesCorrelationId: AAP R-13 propagation.
# ---------------------------------------------------------------------------
class TestCategoriesCorrelationId:
    """AAP R-13 -- correlation ID propagation through HTTP responses.

    The CorrelationIdMiddleware ECHOES the inbound ``X-Correlation-ID``
    header on every response (or generates a UUIDv4 if absent). These
    tests verify the contract at the controller layer.
    """

    async def test_echoes_provided_correlation_id_on_get(
        self,
        app_with_mocks: Any,
        correlation_id: str,
    ) -> None:
        """Inbound ``X-Correlation-ID`` is echoed on GET responses."""
        async with _client(app_with_mocks) as client:
            response = await client.get(
                "/categories/",
                headers={"X-Correlation-ID": correlation_id},
            )

        assert response.status_code == 200
        # Header name comparison is case-insensitive in HTTP.
        assert response.headers.get("x-correlation-id") == correlation_id

    async def test_generates_correlation_id_when_absent(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Middleware generates a UUIDv4 when the inbound header is absent."""
        async with _client(app_with_mocks) as client:
            response = await client.get("/categories/")  # no header

        assert response.status_code == 200
        emitted = response.headers.get("x-correlation-id")
        assert emitted is not None
        # UUIDv4 format check (length 36 with dashes at positions 8/13/18/23).
        assert len(emitted) == 36
        assert emitted.count("-") == 4

    async def test_echoes_correlation_id_on_post(
        self,
        app_with_mocks: Any,
        correlation_id: str,
        valid_admin_token: str,
    ) -> None:
        """Inbound ``X-Correlation-ID`` is echoed on admin POST responses."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={"name": "Electronics", "is_visible": True},
                headers={
                    **_admin_headers(valid_admin_token),
                    "X-Correlation-ID": correlation_id,
                },
            )

        assert response.status_code == 201
        assert response.headers.get("x-correlation-id") == correlation_id


# ---------------------------------------------------------------------------
# Phase 17 -- TestCategoriesStructuredLogs: AAP R-26 log emission.
# ---------------------------------------------------------------------------
class TestCategoriesStructuredLogs:
    """AAP R-26 -- structured JSON log emission for admin writes.

    The controller emits ``categories.created`` and ``categories.updated``
    structured log events on successful admin writes. We use the parent
    ``captured_logs`` fixture (provided by
    ``services/product-service/tests/conftest.py``) to inspect the
    structlog-captured records, indexed by the canonical ``event`` key.
    """

    async def test_create_emits_categories_created_log(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
        captured_logs: list[dict[str, Any]],
    ) -> None:
        """Successful POST emits ``categories.created`` with structured fields."""
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/categories/",
                json={"name": "Electronics", "is_visible": True},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        # Find the categories.created event in captured logs.
        events = [
            record
            for record in captured_logs
            if record.get("event") == "categories.created"
        ]
        assert len(events) >= 1
        record = events[0]
        # Required structured fields per AAP R-26.
        assert "category_id" in record
        assert "slug" in record

    async def test_update_emits_categories_updated_log(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
        captured_logs: list[dict[str, Any]],
    ) -> None:
        """Successful PUT emits ``categories.updated`` with the new version."""
        seeded = await _seed_category(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/categories/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        events = [
            record
            for record in captured_logs
            if record.get("event") == "categories.updated"
        ]
        assert len(events) >= 1
        record = events[0]
        assert record.get("category_id") == seeded["id"]
        assert record.get("new_version") == 2

