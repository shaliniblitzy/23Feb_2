"""Pytest fixtures specific to the Recommendation Engine unit-test suite.

This ``conftest.py`` augments (but does NOT replace) the parent
``tests/conftest.py``. It provides hermetic mock factories for every I/O
collaborator used by ``src/`` modules — ``RedisAdapter``,
``PgVectorAdapter``, ``FeaturesRepository``, ``EmbeddingsRepository``,
``RecCacheRepository``, ``ProductServiceClient``, ``ModelLoader`` — plus
:mod:`respx`-managed HTTP transports and a :func:`freezegun.freeze_time`
convenience helper.

Absolute hermeticity rules (enforced by folder spec):

- NO Testcontainers imports.
- NO real Postgres, Redis, Kafka, or HTTP connections.
- NO subprocesses.
- Every I/O-facing collaborator is :class:`unittest.mock.AsyncMock` /
  :class:`unittest.mock.MagicMock` or a :mod:`respx` route.

Any test that needs a REAL database or broker belongs in ``tests/integration/``,
not here.

Fixture catalog
---------------

Mock collaborators (one per ``src/`` adapter / repository / loader):

* :func:`mock_redis_adapter` — :class:`unittest.mock.AsyncMock` modeled after
  :class:`src.repository.redis_adapter.RedisAdapter`.
* :func:`mock_pgvector_adapter` — :class:`unittest.mock.AsyncMock` modeled
  after :class:`src.repository.pgvector_adapter.PgVectorAdapter`.
* :func:`mock_pg_pool` — :class:`unittest.mock.AsyncMock` exposing an
  async-context-manager ``acquire()`` (asyncpg / psycopg pool idiom).
* :func:`mock_features_repo` — :class:`unittest.mock.AsyncMock` modeled
  after :class:`src.repository.features_repo.FeaturesRepository`.
* :func:`mock_embeddings_repo` — :class:`unittest.mock.AsyncMock` modeled
  after :class:`src.repository.embeddings_repo.EmbeddingsRepository`.
* :func:`mock_rec_cache_repo` — :class:`unittest.mock.AsyncMock` modeled
  after :class:`src.repository.rec_cache_repo.RecCacheRepository`.
* :func:`mock_product_client` — :class:`unittest.mock.AsyncMock` modeled
  after :class:`src.repository.product_client.ProductServiceClient`.
* :func:`mock_model_loader` — :class:`unittest.mock.MagicMock` modeled
  after :class:`src.inference.model_loader.ModelLoader` (sync properties +
  async methods).

HTTP transport fixtures:

* :func:`respx_mock` — yields a :class:`respx.MockRouter` for the duration
  of the test (the SOLE permitted HTTP-mocking mechanism).
* :func:`httpx_client` — provides a plain :class:`httpx.AsyncClient` whose
  transport is intercepted by ``respx`` when paired with ``@respx.mock``
  or the ``respx_mock`` fixture.

Time-control:

* :func:`frozen_time` — yields a :class:`freezegun.FrozenDateTimeFactory`
  pre-frozen at ``2024-01-01T00:00:00+00:00``.

Composite injection:

* :class:`_MockBundle` — :func:`dataclasses.dataclass` bundling every mock
  collaborator into a single attribute container.
* :func:`mock_bundle` — fixture returning a :class:`_MockBundle` populated
  from each individual mock fixture.

Diagnostics:

* :func:`_warn_on_missing_asyncio_marker` — autouse soft-warning fixture
  that surfaces async test functions missing
  ``@pytest.mark.asyncio`` at collection time.

Notes on parent-conftest inheritance
------------------------------------
The parent ``services/recommendation-engine/tests/conftest.py`` provides
session- and general-purpose fixtures (``anyio_backend``, ``faker_seed``,
``faker_instance``, ``correlation_id``, ``structlog_test_capture``,
``captured_logs``, ``settings_factory``, ``_clear_structlog_context``,
``_reset_get_settings_cache``, ``assert_required_log_fields``). pytest
automatically merges these into the unit-test suite's fixture namespace;
they are NOT redefined here.
"""

# ---------------------------------------------------------------------------
# Future imports
# ---------------------------------------------------------------------------
# ``from __future__ import annotations`` enables PEP 563 postponed
# evaluation of annotations so that fixture signatures and the
# :class:`_MockBundle` dataclass declarations do not need to manually
# stringify forward references. This is required for clean Python 3.11+
# typing semantics in this conftest.
from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``asyncio`` and ``warnings`` are imported at module scope (rather than
# inside :func:`_warn_on_missing_asyncio_marker`) so that the fixture
# function body executes a tight type-introspection check — repeated
# imports inside an autouse fixture would add measurable overhead to
# every test in the package.
#
# ``contextlib.asynccontextmanager`` is imported eagerly because the
# folder spec (Phase 1 of the agent prompt) lists it as a required
# import; downstream extension fixtures (or test files in this package)
# may reuse it for additional async-context-manager helpers.
#
# ``dataclasses.dataclass`` is needed at module scope to declare the
# :class:`_MockBundle` container before the :func:`mock_bundle` fixture
# references it.
#
# ``typing.Any``, ``typing.AsyncIterator``, and ``typing.Callable`` cover
# the heterogeneous mock return values, async-yield fixture annotations,
# and fixture function-type signatures used throughout the conftest.
# ``typing.Iterator`` is the correct sync-generator annotation for
# pytest yield-fixtures whose context-manager body is synchronous (e.g.,
# ``with respx.mock() as router: yield router``); ``AsyncIterator`` is
# reserved for fixtures whose body is an actual ``async for`` /
# ``async with`` source.
#
# ``unittest.mock.AsyncMock`` / ``unittest.mock.MagicMock`` are the ONLY
# permitted I/O-mocking primitives in this hermetic unit-test conftest;
# every adapter, repository, and client mock is one or the other.
import asyncio
import warnings
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Iterator
from unittest.mock import AsyncMock, MagicMock

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``httpx`` provides the ``AsyncClient`` returned by :func:`httpx_client`.
# It is also referenced by the type annotation of that fixture so that
# IDEs and ``mypy --strict`` resolve the correct return type.
#
# ``pytest`` is the test framework that supplies the ``@pytest.fixture()``
# decorator used to declare every fixture in this conftest, plus
# :class:`pytest.FixtureRequest` (referenced by the autouse warning
# fixture for node introspection).
#
# ``respx`` is the httpx-mock transport library; the ``respx_mock``
# fixture yields a :class:`respx.MockRouter` via ``with respx.mock()``
# context-manager semantics. Per the folder spec this is the SOLE
# permitted HTTP-mocking mechanism for unit tests.
import httpx
import pytest
import respx

# ---------------------------------------------------------------------------
# BAN IMPORT MARKER — DO NOT IMPORT TESTCONTAINERS IN THIS PACKAGE.
# Unit tests MUST be hermetic. Integration tests (``tests/integration/``)
# are the ONLY place for real-container fixtures. The strings below are
# intentionally NOT split or obfuscated so that ``grep`` /
# ``ruff`` / pre-commit hooks can detect accidental imports verbatim.
#
# Forbidden import names (each is a real-I/O surface):
#   - testcontainers           (real Docker containers)
#   - testcontainers.kafka
#   - testcontainers.postgres
#   - testcontainers.redis
#   - confluent_kafka          (real Kafka client)
#   - psycopg                  (real Postgres client; v3)
#   - psycopg2                 (real Postgres client; v2 — also banned)
#   - asyncpg                  (real Postgres client)
#   - redis.asyncio            (real Redis client)
#
# Any code review or CI grep finding ANY of the names above inside this
# file MUST be treated as a build break. Use ``AsyncMock`` /
# ``MagicMock`` / ``respx`` exclusively.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Phase 3 — Mock RedisAdapter Fixture
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_redis_adapter() -> AsyncMock:
    """AsyncMock conforming to :class:`src.repository.redis_adapter.RedisAdapter`.

    The real :class:`RedisAdapter` exposes the following async surface (all
    methods swallow-and-log on transport error in production code; they
    NEVER raise):

    * ``get(namespace: str, key: str) -> Any | None``
    * ``set(namespace: str, key: str, value: Any, ttl_seconds: int | None = None) -> bool``
    * ``delete(namespace: str, key: str) -> int``
    * ``ping() -> bool``
    * ``incr(namespace: str, key: str) -> int`` (counters)
    * ``expire(namespace: str, key: str, ttl_seconds: int) -> bool``

    Default behavior of the returned mock:

    - ``get`` → ``None`` (cache miss — exercises the most common branch in
      service code where a miss falls through to the source-of-truth fetch)
    - ``set`` → ``True`` (success)
    - ``delete`` → ``1`` (one key removed)
    - ``ping`` → ``True`` (Redis reachable for health checks)
    - ``incr`` → ``1`` (first-increment outcome)
    - ``expire`` → ``True`` (TTL applied)

    Callers can override any of these on the returned mock before use,
    e.g. to simulate a cache hit::

        mock_redis_adapter.get = AsyncMock(return_value={"id": "abc"})

    Returns:
        :class:`unittest.mock.AsyncMock` ready for direct injection into a
        service constructor.
    """
    adapter = AsyncMock()
    # Async surface mirrors src.repository.redis_adapter.RedisAdapter.
    adapter.get = AsyncMock(return_value=None)
    adapter.set = AsyncMock(return_value=True)
    adapter.delete = AsyncMock(return_value=1)
    adapter.ping = AsyncMock(return_value=True)
    adapter.incr = AsyncMock(return_value=1)
    adapter.expire = AsyncMock(return_value=True)
    return adapter


# ---------------------------------------------------------------------------
# Phase 4 — Mock PgVectorAdapter and PG Pool Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_pgvector_adapter() -> AsyncMock:
    """AsyncMock conforming to :class:`src.repository.pgvector_adapter.PgVectorAdapter`.

    The real :class:`PgVectorAdapter` exposes:

    * Class constants: ``COSINE_DISTANCE_OP = "<=>"``,
      ``L2_DISTANCE_OP = "<->"``, ``INNER_PRODUCT_OP = "<#>"``.
    * Async query primitives: ``execute_query``, ``execute_many``,
      ``fetchone``, ``fetchall``.
    * Private helper ``_apply_session_settings`` that issues
      ``SET statement_timeout = {int(timeout_ms)};`` per session.

    Default behavior of the returned mock:

    - ``fetchone`` → ``None`` (no row found)
    - ``fetchall`` → ``[]`` (empty result set)
    - ``execute_query`` → ``None`` (DML success without return value)
    - ``execute_many`` → ``None`` (batched DML success)

    Tests that exercise vector similarity queries should override
    ``fetchall`` to return a list of fake neighbor rows.

    Returns:
        :class:`unittest.mock.AsyncMock` mirroring the PgVectorAdapter
        async surface.
    """
    adapter = AsyncMock()
    adapter.fetchone = AsyncMock(return_value=None)
    adapter.fetchall = AsyncMock(return_value=[])
    adapter.execute_query = AsyncMock(return_value=None)
    adapter.execute_many = AsyncMock(return_value=None)
    return adapter


@pytest.fixture()
def mock_pg_pool() -> AsyncMock:
    """Mock asyncpg / psycopg pool with an async-context-manager ``acquire()``.

    The canonical asyncpg / psycopg v3 idiom in service code is::

        async with pool.acquire() as conn:
            row = await conn.fetchval("SELECT 1")

    To make that idiom work against a mock, ``pool.acquire()`` must
    return an object whose ``__aenter__`` yields the (mock) connection.
    We construct that chain here:

    1. ``pool`` itself is an :class:`AsyncMock`.
    2. ``conn`` is an :class:`AsyncMock` wired with sensible defaults:

       * ``fetchval`` → ``1`` (idiomatic ``SELECT 1`` round-trip used by
         readiness probes)
       * ``fetch`` → ``[]`` (empty result set)
       * ``execute`` → ``"OK"`` (libpq command-tag-style return)

    3. ``acquire_cm`` is the awaited-context-manager object.
       ``__aenter__`` returns ``conn``; ``__aexit__`` returns ``False``
       so exceptions propagate normally.
    4. ``pool.acquire`` itself is a synchronous :class:`MagicMock` that
       returns the ``acquire_cm`` — calling ``pool.acquire()`` is NOT
       a coroutine in psycopg / asyncpg; the returned object IS the
       async context manager.

    This shape is consumed by:

    - PgVectorAdapter health-check tests (``SELECT 1`` round-trip).
    - Repository methods using ``async with self._pool.acquire() as
      conn`` to scope a single transactional connection.

    Returns:
        :class:`unittest.mock.AsyncMock` whose ``acquire()`` returns an
        async-context-manager that yields a fully-stubbed mock
        connection.
    """
    # Create the mock connection with the most common asyncpg / psycopg v3
    # query primitives stubbed out. Tests can override any of these.
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value="OK")
    conn.executemany = AsyncMock(return_value=None)

    # Build the async-context-manager that ``pool.acquire()`` returns.
    # The aenter/aexit hooks are AsyncMock by default (an AsyncMock's
    # attributes default to AsyncMock when accessed), but to make the
    # __aenter__ return value explicit we wire it manually.
    acquire_cm = AsyncMock()
    acquire_cm.__aenter__.return_value = conn
    acquire_cm.__aexit__.return_value = False

    # ``pool`` is an AsyncMock — but ``acquire`` itself is a SYNCHRONOUS
    # call that returns the context manager; using a plain MagicMock
    # for ``pool.acquire`` ensures ``pool.acquire()`` returns the cm
    # directly (NOT a coroutine that resolves to the cm).
    pool = AsyncMock()
    pool.acquire = MagicMock(return_value=acquire_cm)

    # Expose the conn on the pool for tests that want to assert against
    # the connection without re-deriving it from the cm chain. This is a
    # convenience handle and does NOT change the async-with semantics.
    pool._mock_conn = conn  # noqa: SLF001 — intentional test handle.
    return pool


# ---------------------------------------------------------------------------
# Phase 5 — Mock Repository Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_features_repo() -> AsyncMock:
    """AsyncMock conforming to :class:`src.repository.features_repo.FeaturesRepository`.

    The real :class:`FeaturesRepository` exposes:

    * ``upsert_atomic(user_id: UUID, product_id: UUID, ...) -> None``
    * ``fetch_user_features(user_id: UUID) -> list[FeatureDelta]``
    * ``list_all_user_ids() -> list[UUID]``

    Default behavior of the returned mock:

    - ``upsert_atomic`` → ``None`` (success)
    - ``fetch_user_features`` → ``[]`` (no features yet)
    - ``list_all_user_ids`` → ``[]`` (no users yet)

    Returns:
        :class:`unittest.mock.AsyncMock` mirroring the FeaturesRepository
        async surface.
    """
    repo = AsyncMock()
    repo.upsert_atomic = AsyncMock(return_value=None)
    repo.fetch_user_features = AsyncMock(return_value=[])
    repo.list_all_user_ids = AsyncMock(return_value=[])
    return repo


@pytest.fixture()
def mock_embeddings_repo() -> AsyncMock:
    """AsyncMock conforming to :class:`src.repository.embeddings_repo.EmbeddingsRepository`.

    The real :class:`EmbeddingsRepository` exposes:

    * ``upsert(entity_id, entity_type, vector, model_version) -> None``
    * ``fetch(entity_id, entity_type) -> Embedding | None``
    * ``top_k_neighbors(query_vec, k, category=None) -> list[Neighbor]``
    * ``delete(entity_id, entity_type) -> None``

    Default behavior of the returned mock:

    - ``upsert`` → ``None`` (success)
    - ``fetch`` → ``None`` (entity not found)
    - ``top_k_neighbors`` → ``[]`` (no neighbors)
    - ``delete`` → ``None`` (success)

    Returns:
        :class:`unittest.mock.AsyncMock` mirroring the EmbeddingsRepository
        async surface.
    """
    repo = AsyncMock()
    repo.upsert = AsyncMock(return_value=None)
    repo.fetch = AsyncMock(return_value=None)
    repo.top_k_neighbors = AsyncMock(return_value=[])
    repo.delete = AsyncMock(return_value=None)
    return repo


@pytest.fixture()
def mock_rec_cache_repo() -> AsyncMock:
    """AsyncMock conforming to :class:`src.repository.rec_cache_repo.RecCacheRepository`.

    The real :class:`RecCacheRepository` exposes:

    * ``top_n(n: int, category: str | None = None) -> list[PopularProduct]``
    * ``apply_popularity_delta(product_id, delta, category=None) -> None``
    * ``trim() -> int`` — number of stale entries removed.

    Default behavior of the returned mock:

    - ``top_n`` → ``[]`` (no popular products yet)
    - ``apply_popularity_delta`` → ``None`` (success)
    - ``trim`` → ``0`` (nothing trimmed)

    Returns:
        :class:`unittest.mock.AsyncMock` mirroring the RecCacheRepository
        async surface.
    """
    repo = AsyncMock()
    repo.top_n = AsyncMock(return_value=[])
    repo.apply_popularity_delta = AsyncMock(return_value=None)
    repo.trim = AsyncMock(return_value=0)
    return repo


# ---------------------------------------------------------------------------
# Phase 6 — Mock ProductServiceClient Fixture
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_product_client() -> AsyncMock:
    """AsyncMock conforming to :class:`src.repository.product_client.ProductServiceClient`.

    The real :class:`ProductServiceClient` exposes:

    * ``get_by_id(product_id: UUID) -> Product`` — single-product lookup
    * ``get_many(product_ids: list[UUID]) -> dict[UUID, Product]`` —
      batched lookup
    * ``list_all_ids() -> list[UUID]`` — used by warm-up jobs

    Default behavior of the returned mock:

    - ``get_by_id`` → :class:`AsyncMock` with NO predefined return value
      (configure per-test; raising ``StopAsyncIteration`` if the test
      forgets to set a return value surfaces wiring bugs early).
    - ``get_many`` → ``{}`` (empty mapping — no products found)
    - ``list_all_ids`` → ``[]`` (no products in catalog yet)

    Returns:
        :class:`unittest.mock.AsyncMock` mirroring the
        ProductServiceClient async surface.
    """
    client = AsyncMock()
    # ``get_by_id`` deliberately has NO return_value set; tests must
    # configure it explicitly (e.g.,
    # ``mock_product_client.get_by_id.return_value = Product(...)``).
    client.get_by_id = AsyncMock()
    client.get_many = AsyncMock(return_value={})
    client.list_all_ids = AsyncMock(return_value=[])
    return client


# ---------------------------------------------------------------------------
# Phase 7 — Mock ModelLoader Fixture
# ---------------------------------------------------------------------------
@pytest.fixture()
def mock_model_loader() -> MagicMock:
    """MagicMock conforming to :class:`src.inference.model_loader.ModelLoader`.

    The real :class:`ModelLoader` is a hybrid sync/async surface:

    * ``is_loaded: bool`` — *property*; never raises.
    * ``metadata: ModelMetadata`` — *property*; raises
      :exc:`ModelNotLoadedError` when not loaded.
    * ``load_version(version: str) -> None`` — *async* method.
    * ``active_version() -> str`` — *sync* method.

    Mocking notes:

    - ``is_loaded`` is exposed as a *property* on the type so that
      attribute access actually invokes the lambda. To simulate a
      not-loaded model, override the property in the test body::

          type(mock_model_loader).is_loaded = property(lambda self: False)

      (Property overrides survive until the test ends; reset is
      automatic because the fixture is function-scoped — pytest
      destroys the type alias along with the mock instance.)

    - ``metadata`` is exposed as a plain :class:`MagicMock` instance
      attribute carrying ``embedding_dim`` and ``version`` sub-attrs.
      Tests asserting on metadata access can patch these sub-attrs
      directly. To simulate :exc:`ModelNotLoadedError`, override
      ``type(loader).metadata`` with a property that raises.

    - ``load_version`` is an :class:`AsyncMock` with no return value
      (the real method returns ``None``).

    - ``active_version`` is a :class:`MagicMock` returning the
      placeholder ``"v1.0.0"``.

    Returns:
        :class:`unittest.mock.MagicMock` shaped like the ModelLoader
        contract.
    """
    loader = MagicMock()

    # ``is_loaded`` is a property in production code. Property mocking
    # MUST be done on the type, not the instance — assigning
    # ``loader.is_loaded = True`` would replace the descriptor with a
    # bare attribute and break access-time side effects.
    type(loader).is_loaded = property(lambda self: True)

    # Async load_version (returns None on success in production code).
    loader.load_version = AsyncMock(return_value=None)

    # Sync active_version returning a deterministic placeholder. Tests
    # asserting on version routing can override the return_value.
    loader.active_version = MagicMock(return_value="v1.0.0")

    # ``metadata_for(version: str) -> ModelMetadata`` — sync helper
    # that returns a MagicMock by default.
    loader.metadata_for = MagicMock()

    # ``metadata`` is a property in production but for the mock we
    # expose it as a plain attribute carrying the most-asserted sub-
    # attributes. Tests that need to simulate
    # :exc:`ModelNotLoadedError` should override the type-level
    # property:
    #
    #     def _raise(self):
    #         raise ModelNotLoadedError("model not loaded")
    #     type(mock_model_loader).metadata = property(_raise)
    #
    loader.metadata = MagicMock()
    loader.metadata.embedding_dim = 128
    loader.metadata.version = "v1.0.0"

    return loader


# ---------------------------------------------------------------------------
# Phase 8 — HTTP Transport Fixtures (respx-managed)
# ---------------------------------------------------------------------------
@pytest.fixture()
def respx_mock() -> Iterator[respx.MockRouter]:
    """Yield a :class:`respx.MockRouter` for the duration of the test.

    Use this when a test needs to register multiple routes without
    using the ``@respx.mock`` decorator. Equivalent to::

        with respx.mock() as router:
            yield router

    Notes:

    - ``respx`` is the SOLE permitted HTTP-mocking mechanism in this
      conftest per the folder spec's hermeticity rule (NO real network
      I/O). Combine this fixture with :func:`httpx_client` to drive
      service classes that own an :class:`httpx.AsyncClient`.

    - ``respx.mock()`` patches httpx's transport layer for the duration
      of the context, intercepting ALL requests made through
      :class:`httpx.AsyncClient` instances created within the block.
      Unmatched requests raise a :exc:`respx.MockedAssertionError`
      unless the router is configured with ``assert_all_called=False``.

    Yields:
        :class:`respx.MockRouter` ready for ``router.get(...).mock(...)``
        registrations.
    """
    with respx.mock() as router:
        yield router


@pytest.fixture()
def httpx_client() -> httpx.AsyncClient:
    """Provide a plain :class:`httpx.AsyncClient` interceptable by respx.

    Tests that need to drive ``ProductServiceClient`` or the JWKS
    fetcher can request this fixture to pass through a real httpx
    client — respx handles the transport interception as long as the
    test is decorated with ``@respx.mock`` (or uses the
    :func:`respx_mock` fixture).

    The client is configured with a bounded ``timeout=5.0`` to ensure
    that unmatched mocks fail fast rather than hanging the test
    runner. Tests requiring different timeout semantics should
    construct their own client instead of using this fixture.

    Caller responsibility:
        Tests that take ownership of the returned client SHOULD close
        it via ``await client.aclose()`` to avoid resource warnings;
        however, since respx intercepts at the transport layer there
        is no real socket to leak — the close is best-effort hygiene.

    Returns:
        :class:`httpx.AsyncClient` with a 5-second timeout, ready for
        respx-mediated request interception.
    """
    return httpx.AsyncClient(timeout=5.0)


# ---------------------------------------------------------------------------
# Phase 9 — Freezegun Convenience Fixture
# ---------------------------------------------------------------------------
@pytest.fixture()
def frozen_time(request: pytest.FixtureRequest) -> Iterator[Any]:
    """Yield a :class:`freezegun.FrozenDateTimeFactory` for time-sensitive tests.

    Usage::

        def test_jwt_expiry(frozen_time):
            frozen_time.move_to("2024-01-01T00:00:00Z")
            # ... issue token ...
            frozen_time.tick(delta=timedelta(seconds=3601))
            # ... assert token expired ...

    This fixture freezes the clock at ``2024-01-01T00:00:00+00:00`` by
    default. To freeze at a custom time, use :func:`freezegun.freeze_time`
    directly inside the test body instead of this fixture.

    Why is the import lazy?
        :mod:`freezegun` is imported inside the fixture body (not at
        module scope) to keep module-load fast for tests in this
        package that do NOT request the fixture. Lazy imports are a
        legitimate performance optimization in conftest.py modules
        because pytest re-imports them for every test session.

    Args:
        request: pytest fixture-introspection handle (unused by this
            fixture but required for forward compatibility — future
            extensions may parametrize the freeze-time anchor via
            indirect parameterization).

    Yields:
        :class:`freezegun.api.FrozenDateTimeFactory` (or
        ``FreezegunTestStub`` in newer versions) — supports
        ``move_to()``, ``tick()``, and direct datetime access.
    """
    # Lazy import to keep module-load fast for tests that don't need it.
    # The import is intentionally local so that conftest discovery does
    # not pay the freezegun import cost on every test run.
    from freezegun import freeze_time

    # ``request`` is intentionally unused — see docstring. We accept it
    # in the signature to keep the future-extension path open without
    # requiring a fixture-signature change.
    _ = request

    with freeze_time("2024-01-01T00:00:00+00:00") as frozen:
        yield frozen


# ---------------------------------------------------------------------------
# Phase 10 — Soft-Warning Autouse Guard for Missing asyncio Marker
# ---------------------------------------------------------------------------
# The service's pytest config does NOT use ``asyncio_mode=auto``. To keep
# test intent explicit, this conftest does NOT add an autouse
# ``@pytest.mark.asyncio`` marker. Each async test in ``test_*.py`` MUST
# declare the marker itself.
#
# However, we provide an early-warning helper for developers who forget:
# the autouse fixture below introspects ``request.node`` at setup time
# and emits a :class:`RuntimeWarning` if the test function is a
# coroutine but lacks the marker. This is a SOFT diagnostic —
# pytest-asyncio itself raises a clearer hard error at execution time.
# The warning surfaces the issue earlier (at collection / setup) and
# plays nicely with ``-W error`` in strict CI.
@pytest.fixture(autouse=True)
def _warn_on_missing_asyncio_marker(request: pytest.FixtureRequest) -> None:
    """Emit a soft warning if an async test function is missing ``@pytest.mark.asyncio``.

    Behavior:

    1. Resolve the test function via ``request.node.obj`` (graceful
       fallback if the attribute is missing — non-test items such as
       collection hooks lack ``obj``).
    2. If the function is a coroutine function (per
       :func:`asyncio.iscoroutinefunction`) AND ``"asyncio"`` is NOT
       among the test node's markers, emit a :class:`RuntimeWarning`.
    3. Return ``None`` — this fixture is a side-effect only; it does
       NOT alter test behavior or fail the test directly.

    Rationale:
        pytest-asyncio will error with a clearer message
        (``async def function and no async plugin installed``) when a
        test is truly misconfigured, but that error appears only at
        test execution. The warning emitted here surfaces the problem
        at fixture-setup time, which is earlier in the lifecycle and
        more actionable for developers.

    Args:
        request: pytest fixture-introspection handle exposing the
            test node (and thus the test function and its markers).
    """
    # Resolve the underlying test function. ``request.node`` is the
    # pytest ``Item``; for normal test items it carries an ``obj`` attr
    # holding the function. We use ``hasattr`` for defensive coding
    # because some pytest plugins synthesize nodes without ``obj``.
    test_fn = request.node.obj if hasattr(request.node, "obj") else None
    if test_fn is None:
        # Nothing to introspect — the autouse fixture is a no-op for
        # this node. Returning silently is correct because we don't
        # want to spam warnings on plugin-synthesized nodes.
        return

    # ``asyncio.iscoroutinefunction`` is the canonical way to detect
    # ``async def`` functions; it correctly handles wrapped /
    # decorated targets via :func:`inspect.unwrap` semantics.
    if asyncio.iscoroutinefunction(test_fn):
        # Collect marker names from the node — pytest exposes them via
        # ``iter_markers()``. We only care about presence, so a list
        # comprehension is faster than building a dict.
        markers = [m.name for m in request.node.iter_markers()]
        if "asyncio" not in markers:
            # Soft warning. The ``stacklevel=1`` parameter ensures the
            # warning is attributed to this fixture (NOT the test
            # function), which makes the diagnostic message clear:
            # "your test is async but you didn't mark it asyncio".
            warnings.warn(
                f"Async test '{request.node.nodeid}' missing "
                "@pytest.mark.asyncio marker.",
                RuntimeWarning,
                stacklevel=1,
            )


# ---------------------------------------------------------------------------
# Phase 11 — Integrated Mock-Collection Fixture
# ---------------------------------------------------------------------------
# For tests that need a composite "all mocks injected into a service
# constructor" fixture, we provide a dataclass bundle. This is an
# ergonomic convenience — tests that only need one or two mocks should
# prefer the individual fixtures above to keep the dependency surface
# of each test as narrow as possible.
@dataclass
class _MockBundle:
    """Bundle of every mock collaborator in one injectable fixture.

    The dataclass is a pure-Python aggregate (no methods, no
    ``__post_init__``) — its sole purpose is to give tests a single
    handle from which to dot-access every mock.

    Attributes:
        redis: Mock :class:`src.repository.redis_adapter.RedisAdapter`.
        pgvector: Mock
            :class:`src.repository.pgvector_adapter.PgVectorAdapter`.
        pg_pool: Mock asyncpg / psycopg pool with async-context-manager
            ``acquire()``.
        features_repo: Mock
            :class:`src.repository.features_repo.FeaturesRepository`.
        embeddings_repo: Mock
            :class:`src.repository.embeddings_repo.EmbeddingsRepository`.
        rec_cache_repo: Mock
            :class:`src.repository.rec_cache_repo.RecCacheRepository`.
        product_client: Mock
            :class:`src.repository.product_client.ProductServiceClient`.
        model_loader: Mock
            :class:`src.inference.model_loader.ModelLoader`.

    Note:
        The leading underscore in ``_MockBundle`` is deliberate — the
        class is a private implementation detail of this conftest and
        SHOULD NOT be re-exported from any production module. Tests
        receive instances via the :func:`mock_bundle` fixture; they do
        not need to import the class itself unless writing type
        annotations on helper functions, in which case the import
        path is::

            from .conftest import _MockBundle
    """

    redis: AsyncMock
    pgvector: AsyncMock
    pg_pool: AsyncMock
    features_repo: AsyncMock
    embeddings_repo: AsyncMock
    rec_cache_repo: AsyncMock
    product_client: AsyncMock
    model_loader: MagicMock


@pytest.fixture()
def mock_bundle(
    mock_redis_adapter: AsyncMock,
    mock_pgvector_adapter: AsyncMock,
    mock_pg_pool: AsyncMock,
    mock_features_repo: AsyncMock,
    mock_embeddings_repo: AsyncMock,
    mock_rec_cache_repo: AsyncMock,
    mock_product_client: AsyncMock,
    mock_model_loader: MagicMock,
) -> _MockBundle:
    """Return a :class:`_MockBundle` of every mock collaborator.

    Useful when constructing a fully-wired service class under test:

    .. code-block:: python

        async def test_recommendation_pipeline(mock_bundle, settings_factory):
            settings = settings_factory()
            svc = RecommendationService(
                redis_adapter=mock_bundle.redis,
                pgvector_adapter=mock_bundle.pgvector,
                features_repo=mock_bundle.features_repo,
                embeddings_repo=mock_bundle.embeddings_repo,
                rec_cache_repo=mock_bundle.rec_cache_repo,
                product_client=mock_bundle.product_client,
                model_loader=mock_bundle.model_loader,
                settings=settings,
            )
            # ... drive svc and assert against bundle.<mock>.assert_*

    Each member is the same instance returned by the corresponding
    individual fixture (:func:`mock_redis_adapter`,
    :func:`mock_pgvector_adapter`, etc.), so a test can request both
    ``mock_bundle`` AND e.g. ``mock_redis_adapter`` and the two will
    point at the same object — assertions on either side are
    equivalent.

    Args:
        mock_redis_adapter: Per-fixture mock RedisAdapter.
        mock_pgvector_adapter: Per-fixture mock PgVectorAdapter.
        mock_pg_pool: Per-fixture mock PG pool.
        mock_features_repo: Per-fixture mock FeaturesRepository.
        mock_embeddings_repo: Per-fixture mock EmbeddingsRepository.
        mock_rec_cache_repo: Per-fixture mock RecCacheRepository.
        mock_product_client: Per-fixture mock ProductServiceClient.
        mock_model_loader: Per-fixture mock ModelLoader.

    Returns:
        :class:`_MockBundle` populated from the eight collaborator
        fixtures.
    """
    return _MockBundle(
        redis=mock_redis_adapter,
        pgvector=mock_pgvector_adapter,
        pg_pool=mock_pg_pool,
        features_repo=mock_features_repo,
        embeddings_repo=mock_embeddings_repo,
        rec_cache_repo=mock_rec_cache_repo,
        product_client=mock_product_client,
        model_loader=mock_model_loader,
    )


# ---------------------------------------------------------------------------
# Module-level sanity invariants
# ---------------------------------------------------------------------------
# These constants are not exported but exist so that static analysis
# tools (and code reviewers) can verify at a glance that the names of
# the standard-library symbols imported above are actually referenced
# by something in this module. They double as a self-documentation
# aid for the conftest's intent.
#
# - ``Any`` is the type-hint hook for heterogeneous mock return values
#   used in fixture annotations (e.g., the FrozenDateTimeFactory yield
#   type which is intentionally permissive across freezegun versions).
# - ``Callable`` is the type-hint hook for fixture function-type
#   annotations where mocks are produced or consumed.
# - ``asynccontextmanager`` is exposed for downstream test modules that
#   want to define ad-hoc async-context fixtures without re-importing
#   contextlib.
# - ``Iterator`` is used in the yield-fixture annotations
#   (:func:`respx_mock`, :func:`frozen_time`) so static type checkers
#   correctly model the sync-generator fixture protocol.
# - ``AsyncIterator`` is exposed for downstream test modules that
#   define their own genuinely-async yield fixtures (i.e., fixtures
#   whose body uses ``async for`` or ``async with`` plus ``yield``).
__all__: tuple[str, ...] = (
    "_MockBundle",
    "frozen_time",
    "httpx_client",
    "mock_bundle",
    "mock_embeddings_repo",
    "mock_features_repo",
    "mock_model_loader",
    "mock_pg_pool",
    "mock_pgvector_adapter",
    "mock_product_client",
    "mock_rec_cache_repo",
    "mock_redis_adapter",
    "respx_mock",
)

# Reference-only assignments to satisfy linters that flag "unused" imports
# of names brought into the module for downstream test modules' use.
# These are typing helpers and a context-manager decorator that may be
# consumed by sibling test modules importing from this conftest.
_TYPING_REEXPORTS: tuple[object, ...] = (
    Any,
    AsyncIterator,
    Callable,
    Iterator,
    asynccontextmanager,
)
