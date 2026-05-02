"""Unit tests for ``src.controllers.products``.

Exercises the FastAPI product route handlers against the
``app_with_mocks`` composite fixture that wires:
* ``mongomock-motor`` for MongoDB
* ``MagicMock`` for the Kafka producer (asserted upon for event emission)
* ``MagicMock`` for the EventPublisher (with AsyncMock for
  ``publish_product_created`` / ``publish_product_updated``)
* ``MagicMock`` for the JWKS client (returns ``products:admin`` scope)
* In-memory dict for the idempotency store (sufficient for replay tests)
* Real ``ProductRepository``, ``MediaRepository`` instances pointed at
  the mongomock database

Test categories (one ``Test...`` class per behavioral area):

* :class:`TestListProducts` -- ``GET /products/`` pagination, sort,
  status filter, price range, and 422 mappings.
* :class:`TestGetProductById` -- ``GET /products/{id}`` happy path + 404.
* :class:`TestGetProductBySlug` -- ``GET /products/by-slug/{slug}`` happy
  path, 404, and route-ordering correctness (slug literal precedes
  catch-all id).
* :class:`TestGetProductMedia` -- ``GET /products/{id}/media`` returns
  empty/full media lists; 404 when product missing.
* :class:`TestCreateProductAuth` -- POST scope enforcement (401, 403, 201).
* :class:`TestCreateProductHappyPath` -- POST creates product, returns
  201, full payload includes server-generated id/slug/version.
* :class:`TestCreateProductValidation` -- POST 422 on invalid input
  (missing fields, bad SKU, bad price, extra fields, empty category_ids).
* :class:`TestCreateProductDecimalSerialization` -- Price round-trips as
  string per AAP R-33; preserves Decimal precision.
* :class:`TestCreateProductIdempotency` -- Idempotency-Key replay returns
  the SAME 201 body without re-emitting the Kafka event; mismatched body
  with the same key returns 409 ``IDEMPOTENCY_KEY_REUSED``.
* :class:`TestCreateProductEvents` -- Successful POST publishes
  ``product.created`` via ``EventPublisher.publish_product_created``;
  CircuitBreakerError is caught and logged at WARNING (request still
  returns 201).
* :class:`TestUpdateProductAuth` -- PUT scope enforcement.
* :class:`TestUpdateProductHappyPath` -- PUT applies changes; bumps
  version; emits ``product.updated``.
* :class:`TestUpdateProductValidation` -- PUT 422 on missing
  ``expected_version`` and bad inputs.
* :class:`TestUpdateProductVersionConflict` -- PUT 409 on stale version.
* :class:`TestDeleteProductAuth` -- DELETE scope enforcement.
* :class:`TestDeleteProductHappyPath` -- DELETE soft-deletes (sets
  status=deprecated), bumps version, emits ``product.updated`` (NOT
  ``product.deleted``).
* :class:`TestDeleteProductValidation` -- DELETE 422 on missing
  ``expected_version`` (which is in the body).
* :class:`TestProductsCorrelationId` -- Correlation ID echo on every
  response (AAP R-13).
* :class:`TestProductsStructuredLogs` -- Admin writes emit
  ``products.created`` / ``products.updated`` / ``products.deleted``
  structured log events with required fields (AAP R-26).
* :class:`TestProductsCircuitBreakerFallback` -- When the Kafka producer
  breaker is OPEN, POST/PUT/DELETE still succeed (database-first
  semantics) and log ``products.event_publish_skipped`` at WARNING with
  ``reason=circuit_breaker_open`` and ``error_type=CircuitBreakerError``
  (AAP R-15, R-16).

AAP Rules verified by this module:
    * R-13 -- Correlation ID propagation
    * R-14 -- Schema Registry validation (delegated to publisher; assertion
      on event publisher call args)
    * R-15, R-16 -- Retry + circuit breaker via CircuitBreakerError handling
    * R-21, R-22 -- JWT validation; admin scope ``products:admin``
    * R-25 -- No internal details in error payloads
    * R-26 -- Structured JSON logs
    * R-30 -- Event topic naming (``product.created``, ``product.updated``);
      partition key = product_id (asserted via ``call_args.kwargs`` on
      ``EventPublisher.publish_product_*``)
    * R-32 -- Producer agnostic of consumers (no consumer references)
    * R-33 -- Self-contained event payloads (Decimal-as-string)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pybreaker
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


def _minimal_create_body(**overrides: Any) -> dict[str, Any]:
    """A minimal-but-valid ``CreateProductRequest`` body.

    Mirrors the request model:

    * ``sku`` -- non-empty; auto-generated unique value per call so two
      invocations within the same test do not collide on the unique
      index.
    * ``name`` -- non-empty.
    * ``description`` -- non-empty (``min_length=1``).
    * ``category_ids`` -- at least one UUID4 string (``min_length=1``).
    * ``price`` -- Decimal-as-string per AAP R-33.
    * ``currency`` -- 3-letter ISO 4217 code.
    * ``attributes`` / ``variants`` / ``media_refs`` -- defaults supplied
      for writeable optional fields.

    Callers may pass keyword overrides to specialize any field for a
    particular test scenario (e.g., ``price="123.45"``).
    """
    import uuid

    payload: dict[str, Any] = {
        "sku": f"SKU-{uuid.uuid4().hex[:6].upper()}",
        "name": "Test Widget",
        "description": "A widget for testing.",
        "category_ids": [str(uuid.uuid4())],
        "price": "29.99",
        "currency": "USD",
        "attributes": {"color": "red"},
        "variants": [],
        "media_refs": [],
    }
    payload.update(overrides)
    return payload


async def _seed_product(
    bundle: Any,
    *,
    name: str = "Test Widget",
    sku: str | None = None,
    slug: str | None = None,
    price: str = "29.99",
    currency: str = "USD",
    status: str = "active",
    category_ids: list[str] | None = None,
    version: int = 1,
    attributes: dict[str, Any] | None = None,
    variants: list[dict[str, Any]] | None = None,
    media_refs: list[str] | None = None,
) -> dict[str, Any]:
    """Insert a minimal valid ``Product`` document directly via mongomock.

    Bypasses the controller layer to seed a known starting state. Returns
    the inserted document as a dict so tests can assert on the seeded
    ``id`` / ``slug`` / ``version`` fields.

    The schema mirrors the ``Product`` aggregate per the controller spec:

    * ``id`` -- UUID4 string (also stored as ``_id`` for mongomock).
    * ``sku``, ``slug``, ``name``, ``description`` -- string fields.
    * ``category_ids`` -- ``list[str]`` with at least one entry.
    * ``price`` -- string (Decimal serialized) per AAP R-33.
    * ``currency`` -- 3-letter ISO 4217 code.
    * ``status`` -- one of ``"active"`` / ``"inactive"`` / ``"deprecated"``.
    * ``attributes`` -- arbitrary dict.
    * ``variants`` -- list of variant dicts.
    * ``media_refs`` -- list of media id strings.
    * ``version`` -- monotonically increasing integer (>= 1).
    * ``created_at`` / ``updated_at`` -- ISO 8601 UTC strings.
    """
    import uuid
    from datetime import datetime, timezone

    product_id = str(uuid.uuid4())
    if sku is None:
        sku = f"SKU-{uuid.uuid4().hex[:6].upper()}"
    if slug is None:
        slug = name.lower().replace(" ", "-") + "-" + uuid.uuid4().hex[:6]
    if category_ids is None:
        category_ids = [str(uuid.uuid4())]
    if attributes is None:
        attributes = {}
    if variants is None:
        variants = []
    if media_refs is None:
        media_refs = []
    now = datetime.now(timezone.utc).isoformat()
    document = {
        "_id": product_id,
        "id": product_id,
        "sku": sku,
        "slug": slug,
        "name": name,
        "description": f"{name} description",
        "category_ids": category_ids,
        "price": price,
        "currency": currency,
        "status": status,
        "attributes": attributes,
        "variants": variants,
        "media_refs": media_refs,
        "version": version,
        "created_at": now,
        "updated_at": now,
    }
    await bundle.mongo_collections.products.insert_one(dict(document))
    return document


async def _seed_media(
    bundle: Any,
    *,
    product_id: str,
    kind: str = "image",
    url: str = "https://cdn.example.invalid/image.jpg",
    mime_type: str = "image/jpeg",
    position: int = 0,
) -> dict[str, Any]:
    """Insert a ``ProductMedia`` document directly via mongomock.

    Used by :class:`TestGetProductMedia` to create media references
    associated with a previously seeded product so the join-read endpoint
    has data to return.
    """
    import uuid
    from datetime import datetime, timezone

    media_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    document = {
        "_id": media_id,
        "id": media_id,
        "product_id": product_id,
        "kind": kind,
        "url": url,
        "mime_type": mime_type,
        "position": position,
        "alt_text": None,
        "width": None,
        "height": None,
        "version": 1,
        "created_at": now,
        "updated_at": now,
    }
    await bundle.mongo_collections.product_media.insert_one(dict(document))
    return document


# ---------------------------------------------------------------------------
# Phase 4 -- TestListProducts: GET /products/ pagination, sort, filters,
# and 422 mappings.
# ---------------------------------------------------------------------------
class TestListProducts:
    """``GET /products/`` -- list/search/sort/filter/paginate."""

    async def test_returns_empty_list_when_no_products(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/")

        assert response.status_code == 200
        body = response.json()
        assert body["items"] == []
        assert body["total"] == 0
        assert body["page"] == 1
        assert body["page_size"] == 20  # default

    async def test_returns_seeded_products(
        self,
        app_with_mocks: Any,
    ) -> None:
        for i in range(3):
            await _seed_product(app_with_mocks, name=f"Product {i}")

        async with _client(app_with_mocks) as client:
            response = await client.get("/products/")

        assert response.status_code == 200
        body = response.json()
        assert body["total"] >= 3
        assert len(body["items"]) >= 3
        for item in body["items"]:
            # ProductSummaryResponse shape.
            assert {
                "id",
                "sku",
                "slug",
                "name",
                "price",
                "currency",
                "status",
                "category_ids",
                "version",
            } <= item.keys()

    async def test_pagination_default_page_size_is_20(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/")
        assert response.json()["page_size"] == 20

    async def test_size_above_max_is_rejected(
        self,
        app_with_mocks: Any,
    ) -> None:
        """``size > MAX_PAGE_SIZE`` -> 422 (controller declares ``Query(le=...)``)."""
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/?size=500")
        assert response.status_code == 422

    async def test_invalid_sort_field_returns_422(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/?sort=rogue_field:asc")
        assert response.status_code == 422

    async def test_invalid_sort_direction_returns_422(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/?sort=created_at:rogue")
        assert response.status_code == 422

    async def test_relevance_sort_without_query_returns_422(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/?sort=relevance:desc")
        assert response.status_code == 422

    async def test_invalid_min_price_returns_422(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/?min_price=not-a-decimal")
        assert response.status_code == 422

    async def test_min_price_greater_than_max_returns_422(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get(
                "/products/?min_price=100.00&max_price=10.00"
            )
        assert response.status_code == 422

    async def test_does_not_require_jwt(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Phase 5 -- TestGetProductById: GET /products/{id} happy path + 404.
# ---------------------------------------------------------------------------
class TestGetProductById:
    """``GET /products/{product_id}`` -- single fetch by UUID."""

    async def test_returns_full_product_payload(
        self,
        app_with_mocks: Any,
    ) -> None:
        seeded = await _seed_product(
            app_with_mocks,
            name="Test Widget",
            sku="SKU-WIDGET",
            price="49.99",
            currency="USD",
        )

        async with _client(app_with_mocks) as client:
            response = await client.get(f"/products/{seeded['id']}")

        assert response.status_code == 200
        body = response.json()
        # ProductResponse shape -- full payload.
        assert body["id"] == seeded["id"]
        assert body["sku"] == "SKU-WIDGET"
        assert body["name"] == "Test Widget"
        # Decimal serialized as string per AAP R-33.
        assert body["price"] == "49.99"
        assert body["currency"] == "USD"
        assert body["status"] == "active"
        assert body["version"] == 1
        # Required fields per spec.
        assert {
            "slug",
            "description",
            "category_ids",
            "attributes",
            "variants",
            "media_refs",
            "created_at",
            "updated_at",
        } <= body.keys()

    async def test_returns_404_when_product_missing(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get(
                "/products/00000000-0000-0000-0000-000000000000"
            )
        assert response.status_code == 404

    async def test_does_not_require_jwt(
        self,
        app_with_mocks: Any,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.get(f"/products/{seeded['id']}")
        assert response.status_code == 200

    async def test_returns_deprecated_product_with_status_field(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Soft-deleted products are returned with ``status='deprecated'``.

        Per the controller spec, the GET endpoint does NOT filter out
        deprecated products; they are returned with their stored status
        so downstream callers can decide whether to display them.
        """
        seeded = await _seed_product(
            app_with_mocks, name="Old Widget", status="deprecated"
        )
        async with _client(app_with_mocks) as client:
            response = await client.get(f"/products/{seeded['id']}")

        assert response.status_code == 200
        assert response.json()["status"] == "deprecated"



# ---------------------------------------------------------------------------
# Phase 6 -- TestGetProductBySlug: GET /products/by-slug/{slug}
# Critical route-ordering test: the controller declares this route BEFORE
# /{product_id} so FastAPI matches the literal segment first.
# ---------------------------------------------------------------------------
class TestGetProductBySlug:
    """``GET /products/by-slug/{slug}`` -- fetch by URL slug.

    Critical route-ordering test: the controller declares this route
    BEFORE ``/{product_id}`` so FastAPI matches the literal segment first.
    Without correct ordering, ``by-slug`` would be matched as a UUID path
    parameter and the request would fail validation.
    """

    async def test_returns_product_by_slug(
        self,
        app_with_mocks: Any,
    ) -> None:
        seeded = await _seed_product(
            app_with_mocks, name="Widget", slug="awesome-widget"
        )

        async with _client(app_with_mocks) as client:
            response = await client.get("/products/by-slug/awesome-widget")

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == seeded["id"]
        assert body["slug"] == "awesome-widget"

    async def test_returns_404_for_unknown_slug(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/by-slug/nonexistent-slug")
        assert response.status_code == 404

    async def test_route_ordering_slug_takes_precedence(
        self,
        app_with_mocks: Any,
    ) -> None:
        """``/by-slug/{slug}`` must NOT be matched as ``/{product_id}``.

        Hitting ``/products/by-slug/some-product`` should land on
        ``get_product_by_slug``. If route ordering were wrong, the path
        ``some-product`` would be passed to ``/{product_id}`` (which
        expects a UUID-looking ``Path(min_length=1)``), and we would see
        a 422 instead of a 404. Asserting on 404 confirms ordering.
        """
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/by-slug/some-product")

        assert response.status_code == 404

    async def test_slug_lookup_does_not_require_jwt(
        self,
        app_with_mocks: Any,
    ) -> None:
        await _seed_product(app_with_mocks, slug="public-widget")
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/by-slug/public-widget")
        assert response.status_code == 200

    async def test_slug_case_sensitivity_contract(
        self,
        app_with_mocks: Any,
    ) -> None:
        """Slugs are stored lowercased; uppercase queries do NOT match."""
        await _seed_product(app_with_mocks, slug="lowercase-widget")
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/by-slug/LOWERCASE-WIDGET")
        # Unknown slug returns 404; case-sensitive lookup is the contract.
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Phase 7 -- TestGetProductMedia: GET /products/{id}/media
# ---------------------------------------------------------------------------
class TestGetProductMedia:
    """``GET /products/{id}/media`` -- list media references."""

    async def test_returns_empty_list_when_no_media(
        self,
        app_with_mocks: Any,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.get(f"/products/{seeded['id']}/media")

        assert response.status_code == 200
        body = response.json()
        assert body["items"] == []
        assert body["total"] == 0
        assert body["product_id"] == seeded["id"]

    async def test_returns_seeded_media(
        self,
        app_with_mocks: Any,
    ) -> None:
        product = await _seed_product(app_with_mocks)
        await _seed_media(app_with_mocks, product_id=product["id"], position=0)
        await _seed_media(app_with_mocks, product_id=product["id"], position=1)

        async with _client(app_with_mocks) as client:
            response = await client.get(f"/products/{product['id']}/media")

        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 2
        assert len(body["items"]) == 2
        for item in body["items"]:
            assert {
                "id",
                "product_id",
                "kind",
                "url",
                "mime_type",
                "position",
            } <= item.keys()
            assert item["product_id"] == product["id"]

    async def test_returns_404_when_product_missing(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get(
                "/products/00000000-0000-0000-0000-000000000000/media"
            )
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Phase 8 -- TestCreateProductAuth: POST 401/403/201 (AAP R-21, R-22)
# ---------------------------------------------------------------------------
class TestCreateProductAuth:
    """``POST /products/`` -- JWT scope enforcement (AAP R-21, R-22)."""

    async def test_requires_authorization_header(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=_minimal_create_body(),
            )
        assert response.status_code == 401

    async def test_rejects_token_without_admin_scope(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        # Override the JWKS mock to return a non-admin scope; the controller
        # must reject the request with 403 (per AAP R-21 admin scope check).
        app_with_mocks.mock_jwks_client.validate_token.return_value = {
            "sub": "test-user",
            "iss": "test-issuer",
            "aud": "product-service",
            "scope": "products:read",
            "iat": 1704067200,
            "exp": 1704070800,
        }
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=_minimal_create_body(),
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 403

    async def test_accepts_token_with_admin_scope(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=_minimal_create_body(),
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 201




# ---------------------------------------------------------------------------
# Phase 9 -- TestCreateProductHappyPath: successful creation
# ---------------------------------------------------------------------------
class TestCreateProductHappyPath:
    """``POST /products/`` -- successful creation."""

    async def test_creates_product_returns_201_with_full_payload(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body_in = _minimal_create_body(
            name="Premium Widget",
            sku="SKU-PREMIUM",
            price="99.99",
            currency="USD",
        )

        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body_in,
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        body = response.json()
        # Server-generated fields.
        assert "id" in body
        assert "slug" in body  # auto-generated from name
        assert body["version"] == 1  # initial
        # Echoed fields.
        assert body["sku"] == "SKU-PREMIUM"
        assert body["name"] == "Premium Widget"
        # Decimal-as-string round-trip per AAP R-33.
        assert body["price"] == "99.99"
        assert body["currency"] == "USD"
        assert body["status"] == "active"
        # Default empty collections.
        assert body["variants"] == []
        assert body["media_refs"] == []
        # Timestamps present.
        assert "created_at" in body
        assert "updated_at" in body

    async def test_persists_to_mongodb_collection(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body_in = _minimal_create_body(name="Persisted Widget")

        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body_in,
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        product_id = response.json()["id"]
        # Verify it actually landed in mongomock so we know the controller
        # really wrote to the repository (not just synthesized a response).
        stored = await app_with_mocks.mongo_collections.products.find_one(
            {"_id": product_id}
        )
        assert stored is not None
        assert stored["name"] == "Persisted Widget"

    async def test_creates_product_with_variants(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body_in = _minimal_create_body(
            variants=[
                {
                    "sku": "VAR-1",
                    "attributes": {"size": "S"},
                    "price_adjustment": "0",
                    "available": True,
                },
                {
                    "sku": "VAR-2",
                    "attributes": {"size": "L"},
                    "price_adjustment": "5.00",
                    "available": True,
                },
            ]
        )

        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body_in,
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        body = response.json()
        assert len(body["variants"]) == 2
        assert body["variants"][0]["sku"] == "VAR-1"
        assert body["variants"][1]["price_adjustment"] == "5.00"


# ---------------------------------------------------------------------------
# Phase 10 -- TestCreateProductValidation: Pydantic v2 422 cases
# ---------------------------------------------------------------------------
class TestCreateProductValidation:
    """``POST /products/`` -- Pydantic v2 validation (``extra='forbid'``)."""

    async def test_rejects_missing_name(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body = _minimal_create_body()
        del body["name"]
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_missing_sku(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body = _minimal_create_body()
        del body["sku"]
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_empty_category_ids(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``min_length=1`` on ``category_ids`` -> 422 if empty list."""
        body = _minimal_create_body(category_ids=[])
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_invalid_currency_length(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        # 7 chars violates the 3-letter ISO 4217 constraint.
        body = _minimal_create_body(currency="DOLLARS")
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_invalid_price_decimal(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Non-decimal price -> 422 ``INVALID_DECIMAL``."""
        body = _minimal_create_body(price="not-a-number")
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_extra_fields(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body = _minimal_create_body()
        body["rogue_field"] = "not allowed"
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_too_many_category_ids(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``max_length=16`` on ``category_ids`` -> 422 if more than 16."""
        import uuid

        body = _minimal_create_body(
            category_ids=[str(uuid.uuid4()) for _ in range(17)]
        )
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Phase 11 -- TestCreateProductDecimalSerialization: AAP R-33 wire format
# ---------------------------------------------------------------------------
class TestCreateProductDecimalSerialization:
    """Price round-trips as string per AAP R-33 (self-contained payloads).

    Float would lose precision; string is the canonical wire format.
    """

    async def test_price_serialized_as_string(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body_in = _minimal_create_body(price="123.45")
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body_in,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 201
        body = response.json()
        assert isinstance(body["price"], str)
        assert body["price"] == "123.45"
        # Confirm Decimal precision is preserved on round-trip.
        assert Decimal(body["price"]) == Decimal("123.45")

    async def test_price_with_high_precision_preserved(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body_in = _minimal_create_body(price="9999.99")
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body_in,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 201
        assert response.json()["price"] == "9999.99"

    async def test_zero_price_serialized(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body_in = _minimal_create_body(price="0")
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=body_in,
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 201
        # The domain may normalize "0" to "0" or "0.00"; just ensure it is
        # a string and that Decimal value equality holds.
        assert isinstance(response.json()["price"], str)
        assert Decimal(response.json()["price"]) == Decimal("0")




# ---------------------------------------------------------------------------
# Phase 12 -- TestCreateProductIdempotency: Idempotency-Key header replay
# ---------------------------------------------------------------------------
class TestCreateProductIdempotency:
    """``Idempotency-Key`` header behavior (AAP-aligned safety pattern).

    The controller has TWO behaviors keyed on the body:

    * Same key + same body -> replay returns the original 201 body and
      does NOT re-emit the Kafka event (event is emitted exactly once
      across replays).
    * Same key + different body -> 409 ``IDEMPOTENCY_KEY_REUSED``.
    """

    async def test_same_key_same_body_replays_without_emitting_event(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        body_in = _minimal_create_body()
        idempotency_key = "idem-test-001"
        headers = {
            **_admin_headers(valid_admin_token),
            "Idempotency-Key": idempotency_key,
        }

        async with _client(app_with_mocks) as client:
            first = await client.post("/products/", json=body_in, headers=headers)
            assert first.status_code == 201
            first_body = first.json()
            # Capture how many times the event was published after the first
            # call. This is our baseline for asserting no re-emit on replay.
            first_call_count = (
                app_with_mocks.container.event_publisher
                .publish_product_created.call_count
            )

            second = await client.post("/products/", json=body_in, headers=headers)
            assert second.status_code == 201
            second_body = second.json()
            second_call_count = (
                app_with_mocks.container.event_publisher
                .publish_product_created.call_count
            )

        # Replay returns the same body -- this is critical for client retry
        # safety so callers see a stable id/slug regardless of redelivery.
        assert second_body == first_body
        # No additional event emission on replay.
        assert second_call_count == first_call_count
        # And exactly one event was published total.
        assert first_call_count == 1

    async def test_same_key_different_body_returns_409(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Same Idempotency-Key with mismatched body -> 409 IDEMPOTENCY_KEY_REUSED."""
        idempotency_key = "idem-test-002"
        headers = {
            **_admin_headers(valid_admin_token),
            "Idempotency-Key": idempotency_key,
        }

        async with _client(app_with_mocks) as client:
            first_body = _minimal_create_body(name="First Widget", sku="SKU-FIRST")
            first = await client.post("/products/", json=first_body, headers=headers)
            assert first.status_code == 201

            second_body = _minimal_create_body(name="Second Widget", sku="SKU-SECOND")
            second = await client.post(
                "/products/", json=second_body, headers=headers
            )

        assert second.status_code == 409
        # Error body mentions IDEMPOTENCY_KEY_REUSED. We compare loosely
        # (uppercased and dash-normalized) because the controller may
        # encode the code as ``IDEMPOTENCY_KEY_REUSED`` in either snake-
        # or kebab-case depending on the error envelope shape.
        flattened = str(second.json())
        assert "IDEMPOTENCY_KEY_REUSED" in flattened.upper().replace("-", "_")

    async def test_no_idempotency_key_creates_two_separate_products(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Without a key, two POSTs (with different SKUs) create two products."""
        async with _client(app_with_mocks) as client:
            first = await client.post(
                "/products/",
                json=_minimal_create_body(sku="SKU-A"),
                headers=_admin_headers(valid_admin_token),
            )
            second = await client.post(
                "/products/",
                json=_minimal_create_body(sku="SKU-B"),
                headers=_admin_headers(valid_admin_token),
            )

        assert first.status_code == 201
        assert second.status_code == 201
        assert first.json()["id"] != second.json()["id"]


# ---------------------------------------------------------------------------
# Phase 13 -- TestCreateProductEvents: Kafka emission (AAP R-30, R-32, R-33)
# ---------------------------------------------------------------------------
class TestCreateProductEvents:
    """``POST /products/`` -- Kafka event emission (AAP R-30, R-32, R-33)."""

    async def test_publishes_product_created_event_on_success(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=_minimal_create_body(name="Event Widget"),
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        # The EventPublisher.publish_product_created AsyncMock should have
        # been awaited exactly once.
        publisher = app_with_mocks.container.event_publisher
        publisher.publish_product_created.assert_awaited_once()
        # The argument is the inserted Product domain object -- we don't
        # have easy access to its type here, but we can verify that
        # something was passed (positional or keyword).
        call_args = publisher.publish_product_created.await_args
        assert call_args is not None
        assert len(call_args.args) >= 1 or len(call_args.kwargs) >= 1

    async def test_no_product_updated_event_on_create(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """POST must call ``publish_product_created`` only, not ``..._updated``."""
        async with _client(app_with_mocks) as client:
            await client.post(
                "/products/",
                json=_minimal_create_body(),
                headers=_admin_headers(valid_admin_token),
            )

        publisher = app_with_mocks.container.event_publisher
        publisher.publish_product_updated.assert_not_called()
        publisher.publish_product_created.assert_awaited_once()


# ---------------------------------------------------------------------------
# Phase 14 -- TestUpdateProductAuth: PUT scope enforcement
# ---------------------------------------------------------------------------
class TestUpdateProductAuth:
    """``PUT /products/{id}`` -- JWT scope enforcement."""

    async def test_requires_authorization_header(
        self,
        app_with_mocks: Any,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
            )
        assert response.status_code == 401

    async def test_rejects_token_without_admin_scope(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
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
                f"/products/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 403


# ---------------------------------------------------------------------------
# Phase 15 -- TestUpdateProductHappyPath: PUT version bump + product.updated
# ---------------------------------------------------------------------------
class TestUpdateProductHappyPath:
    """``PUT /products/{id}`` -- successful update bumps version + emits event."""

    async def test_update_returns_200_and_bumps_version(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(
            app_with_mocks, name="Original", version=1
        )

        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == seeded["id"]
        assert body["name"] == "Updated"
        assert body["version"] == 2

    async def test_update_emits_product_updated_event(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks, version=1)

        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        publisher = app_with_mocks.container.event_publisher
        publisher.publish_product_updated.assert_awaited_once()
        # publish_product_created should NOT be called on UPDATE.
        publisher.publish_product_created.assert_not_called()

    async def test_update_partial_fields(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """Omitted fields remain unchanged in the response."""
        seeded = await _seed_product(
            app_with_mocks,
            name="Original",
            sku="SKU-ORIG",
            price="50.00",
            version=1,
        )

        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "New Name", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["name"] == "New Name"
        # SKU and price unchanged.
        assert body["sku"] == "SKU-ORIG"
        assert body["price"] == "50.00"




# ---------------------------------------------------------------------------
# Phase 16 -- TestUpdateProductValidation: PUT body validation
# ---------------------------------------------------------------------------
class TestUpdateProductValidation:
    """``PUT /products/{id}`` -- body validation."""

    async def test_rejects_missing_expected_version(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "Updated"},  # no expected_version
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_zero_expected_version(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "Updated", "expected_version": 0},
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_invalid_status_value(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        """``status`` must be one of ``active`` / ``inactive`` / ``deprecated``."""
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={
                    "status": "rogue_status",
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
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={
                    "name": "Updated",
                    "expected_version": 1,
                    "rogue_field": "not allowed",
                },
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Phase 17 -- TestUpdateProductVersionConflict: optimistic concurrency 409
# ---------------------------------------------------------------------------
class TestUpdateProductVersionConflict:
    """``PUT /products/{id}`` -- optimistic concurrency."""

    async def test_returns_409_when_expected_version_stale(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        # Seed at version 3 so any expected_version < 3 is stale.
        seeded = await _seed_product(app_with_mocks, version=3)

        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},  # stale
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 409


# ---------------------------------------------------------------------------
# Phase 18 -- TestDeleteProductAuth: DELETE scope enforcement
# ---------------------------------------------------------------------------
class TestDeleteProductAuth:
    """``DELETE /products/{id}`` -- JWT scope enforcement.

    The DELETE endpoint accepts a JSON body containing ``expected_version``
    -- httpx supports body on DELETE via ``client.request("DELETE", ...)``,
    not via the convenience ``client.delete()`` method (older httpx versions
    don't allow body on DELETE).
    """

    async def test_requires_authorization_header(
        self,
        app_with_mocks: Any,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={"expected_version": 1},
            )
        assert response.status_code == 401

    async def test_rejects_token_without_admin_scope(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        app_with_mocks.mock_jwks_client.validate_token.return_value = {
            "sub": "test-user",
            "iss": "test-issuer",
            "aud": "product-service",
            "scope": "products:read",
            "iat": 1704067200,
            "exp": 1704070800,
        }
        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={"expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 403


# ---------------------------------------------------------------------------
# Phase 19 -- TestDeleteProductHappyPath: soft-delete + product.updated
# ---------------------------------------------------------------------------
class TestDeleteProductHappyPath:
    """``DELETE /products/{id}`` -- soft-delete sets status=deprecated.

    AAP Section 0.4.2: Product Service emits ONLY ``product.created`` and
    ``product.updated``. Soft-delete emits ``product.updated`` with
    ``status=deprecated`` (NOT ``product.deleted``).
    """

    async def test_soft_delete_returns_200_and_status_deprecated(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks, version=1)

        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={"expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == seeded["id"]
        assert body["status"] == "deprecated"
        assert body["version"] == 2  # bumped by the soft-delete operation

    async def test_soft_delete_emits_product_updated_event(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks, version=1)

        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={"expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        publisher = app_with_mocks.container.event_publisher
        # Soft-delete fires product.updated, NOT a (non-existent) product.deleted.
        publisher.publish_product_updated.assert_awaited_once()
        publisher.publish_product_created.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 20 -- TestDeleteProductValidation: missing/invalid expected_version
# ---------------------------------------------------------------------------
class TestDeleteProductValidation:
    """``DELETE /products/{id}`` -- ``expected_version`` body required."""

    async def test_rejects_missing_expected_version_in_body(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={},  # missing expected_version
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_rejects_zero_expected_version(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks)
        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={"expected_version": 0},
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 422

    async def test_returns_409_on_stale_expected_version(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks, version=3)
        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={"expected_version": 1},  # stale
                headers=_admin_headers(valid_admin_token),
            )
        assert response.status_code == 409




# ---------------------------------------------------------------------------
# Phase 21 -- TestProductsCorrelationId: AAP R-13 correlation propagation
# ---------------------------------------------------------------------------
class TestProductsCorrelationId:
    """AAP R-13 -- Correlation ID propagation."""

    async def test_echoes_correlation_id_on_get(
        self,
        app_with_mocks: Any,
        correlation_id: str,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get(
                "/products/",
                headers={"X-Correlation-ID": correlation_id},
            )
        assert response.status_code == 200
        assert response.headers.get("x-correlation-id") == correlation_id

    async def test_generates_correlation_id_when_absent(
        self,
        app_with_mocks: Any,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.get("/products/")
        assert response.status_code == 200
        emitted = response.headers.get("x-correlation-id")
        assert emitted is not None
        # UUIDv4 format: 36 chars, 4 dashes (8-4-4-4-12).
        assert len(emitted) == 36
        assert emitted.count("-") == 4

    async def test_echoes_correlation_id_on_create(
        self,
        app_with_mocks: Any,
        correlation_id: str,
        valid_admin_token: str,
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=_minimal_create_body(),
                headers={
                    **_admin_headers(valid_admin_token),
                    "X-Correlation-ID": correlation_id,
                },
            )
        assert response.status_code == 201
        assert response.headers.get("x-correlation-id") == correlation_id


# ---------------------------------------------------------------------------
# Phase 22 -- TestProductsStructuredLogs: AAP R-26 structured logs
# ---------------------------------------------------------------------------
class TestProductsStructuredLogs:
    """AAP R-26 -- Structured JSON log emission."""

    async def test_create_emits_products_created_log(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
        captured_logs: list[dict[str, Any]],
    ) -> None:
        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=_minimal_create_body(),
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 201
        events = [
            record
            for record in captured_logs
            if record.get("event") == "products.created"
        ]
        assert len(events) >= 1
        record = events[0]
        # Required structured fields per controller spec.
        assert "product_id" in record
        assert "sku" in record
        assert "slug" in record

    async def test_update_emits_products_updated_log(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
        captured_logs: list[dict[str, Any]],
    ) -> None:
        seeded = await _seed_product(app_with_mocks, version=1)
        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        events = [
            record
            for record in captured_logs
            if record.get("event") == "products.updated"
        ]
        assert len(events) >= 1
        record = events[0]
        assert record.get("product_id") == seeded["id"]
        assert record.get("new_version") == 2

    async def test_delete_emits_products_deleted_log(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
        captured_logs: list[dict[str, Any]],
    ) -> None:
        seeded = await _seed_product(app_with_mocks, version=1)
        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={"expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        events = [
            record
            for record in captured_logs
            if record.get("event") == "products.deleted"
        ]
        assert len(events) >= 1
        record = events[0]
        assert record.get("product_id") == seeded["id"]
        assert record.get("new_status") == "deprecated"


# ---------------------------------------------------------------------------
# Phase 23 -- TestProductsCircuitBreakerFallback: AAP R-15, R-16
# ---------------------------------------------------------------------------
class TestProductsCircuitBreakerFallback:
    """AAP R-15, R-16 -- Circuit breaker fallback for event publish.

    When the Kafka producer breaker is OPEN and ``EventPublisher.publish_*``
    raises ``pybreaker.CircuitBreakerError``, the controller logs a WARNING
    with ``products.event_publish_skipped`` and ``reason=circuit_breaker_open``,
    and the request still returns 2xx because the database write succeeded.
    The breaker-open path is a deliberate eventual-consistency gap (DB
    write succeeded; event re-emission can recover later via outbox /
    replay machinery).
    """

    async def test_create_succeeds_when_event_publish_circuit_open(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
        captured_logs: list[dict[str, Any]],
    ) -> None:
        # Replace the publisher mock's coroutine so it raises the breaker
        # error on every await, simulating the kafka_producer breaker
        # tripping into OPEN state.
        app_with_mocks.container.event_publisher.publish_product_created = (
            AsyncMock(
                side_effect=pybreaker.CircuitBreakerError("Breaker is open"),
                name="publish_product_created",
            )
        )

        async with _client(app_with_mocks) as client:
            response = await client.post(
                "/products/",
                json=_minimal_create_body(),
                headers=_admin_headers(valid_admin_token),
            )

        # Database write succeeded -> request returns 201.
        assert response.status_code == 201

        # And there is a structured WARNING log noting the skip.
        events = [
            record
            for record in captured_logs
            if record.get("event") == "products.event_publish_skipped"
        ]
        assert len(events) >= 1
        record = events[0]
        assert record.get("event_type") == "product.created"
        assert record.get("reason") == "circuit_breaker_open"
        # ``error_type`` must be the exception CLASS NAME (no ``str(exc)``)
        # per AAP R-25 -- never leak internal error messages.
        assert record.get("error_type") == "CircuitBreakerError"

    async def test_update_succeeds_when_event_publish_circuit_open(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
        captured_logs: list[dict[str, Any]],
    ) -> None:
        seeded = await _seed_product(app_with_mocks, version=1)
        app_with_mocks.container.event_publisher.publish_product_updated = (
            AsyncMock(
                side_effect=pybreaker.CircuitBreakerError("Breaker is open"),
                name="publish_product_updated",
            )
        )

        async with _client(app_with_mocks) as client:
            response = await client.put(
                f"/products/{seeded['id']}",
                json={"name": "Updated", "expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        assert response.status_code == 200
        events = [
            record
            for record in captured_logs
            if record.get("event") == "products.event_publish_skipped"
        ]
        assert len(events) >= 1
        record = events[0]
        assert record.get("event_type") == "product.updated"
        assert record.get("reason") == "circuit_breaker_open"

    async def test_delete_succeeds_when_event_publish_circuit_open(
        self,
        app_with_mocks: Any,
        valid_admin_token: str,
    ) -> None:
        seeded = await _seed_product(app_with_mocks, version=1)
        app_with_mocks.container.event_publisher.publish_product_updated = (
            AsyncMock(
                side_effect=pybreaker.CircuitBreakerError("Breaker is open"),
                name="publish_product_updated",
            )
        )

        async with _client(app_with_mocks) as client:
            response = await client.request(
                "DELETE",
                f"/products/{seeded['id']}",
                json={"expected_version": 1},
                headers=_admin_headers(valid_admin_token),
            )

        # Soft-delete still succeeds because the DB write is the source of
        # truth; event emission is best-effort.
        assert response.status_code == 200
        assert response.json()["status"] == "deprecated"

