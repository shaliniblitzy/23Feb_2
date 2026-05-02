"""Shared fixtures for inventory-service repository unit tests.

This conftest augments the parent service conftest
(``services/inventory-service/tests/conftest.py``) with
repository-test-specific mock factories. The fixtures here are an
OPTIONAL convenience layer -- the 4 sibling test files in this folder
each define module-level helpers (``_make_pool_with_cursor``,
``_make_caller_conn``, etc.) for in-line use. Tests MAY use either
pattern.

Why a dedicated repository conftest?
-------------------------------------
- DRY: ``AsyncMock``-shaped pool/connection/cursor fixtures are needed
  by every test in this folder. Centralizing them reduces boilerplate.
- Hermetic guarantees: a single autouse fixture
  (``assert_no_real_psycopg_connections``) defensively asserts that no
  test in this folder accidentally imports a real ``AsyncConnectionPool``
  bound to a live database.
- Discoverability: new repository tests can grab a ready-to-use mock
  pool by injecting ``mock_async_pool``.

Fixture inventory (this conftest)
---------------------------------
- ``mock_async_cursor``        -- fresh ``AsyncMock`` cursor with
  execute/fetchone/fetchall stubs.
- ``mock_async_connection``    -- ``AsyncMock`` connection whose
  ``cursor()`` async-CM yields the ``mock_async_cursor``.
- ``mock_async_pool``          -- ``MagicMock`` pool whose
  ``connection()`` async-CM yields the ``mock_async_connection``.
- ``mock_pool_with_cursor_factory`` -- callable that builds a
  (pool, cursor) pair on demand for tests needing multiple cursors.
- ``assert_no_real_psycopg_connections`` (autouse) -- defensive guard.

Coexistence with sibling test-module helpers
--------------------------------------------
The 4 sibling test files in this folder
(``test_stock_item_repository.py``, ``test_reservation_repository.py``,
``test_stock_movement_repository.py``, ``test_warehouse_repository.py``)
each define module-level helpers (``_make_pool_with_cursor``,
``_make_caller_conn``, ``_normalize_sql``) for in-line use within those
modules. The fixtures in this conftest intentionally use a different
naming convention (snake_case fixture names without leading underscore)
so they coexist cleanly. Tests MAY use either pattern; this conftest
neither overrides nor shadows the in-test helpers.

Parent conftest awareness
-------------------------
The parent ``services/inventory-service/tests/conftest.py`` provides
cross-cutting fixtures (``correlation_id``, ``captured_logs``,
``settings_factory``, ``faker_instance``, ``anyio_backend``, the
autouse ``_clear_structlog_context`` and ``_reset_get_settings_cache``,
and the callable ``assert_required_log_fields``). This conftest does
NOT redefine, override, or shadow ANY parent fixture -- it ONLY adds
new repository-test-specific fixtures.

The ``mocker`` fixture (from ``pytest-mock``) is also provided
externally (it is not defined here). ``pytest-mock`` is pinned in
``services/inventory-service/tests/requirements-dev.txt``.

AAP cross-references
--------------------
- Section 0.5.2.6 Group 6 -- in scope (services/*/tests/unit/**/*).
- Section 0.6.1           -- explicitly listed.
- R-6                     -- service autonomy / hermetic boundary.
- R-26                    -- structured-log fields covered by parent
                              conftest (this conftest does not handle
                              logging fixtures).
- Hermetic boundary       -- NO real psycopg, NO real Postgres,
                              NO live HTTP, NO real Kafka.
"""

from __future__ import annotations

from typing import Any

import pytest


# ============================================================================
# Mock cursor / connection / pool fixtures
# ----------------------------------------------------------------------------
# The fixture trio (mock_async_cursor, mock_async_connection,
# mock_async_pool) composes into the canonical psycopg async-pool shape:
#
#     async with pool.connection() as conn:        # mock_async_pool
#         async with conn.cursor() as cur:         # mock_async_connection
#             await cur.execute(...)               # mock_async_cursor
#             row = await cur.fetchone()
#
# Each fixture sits one rung lower in the stack and is independently
# usable: caller-owned-TX repository methods (which accept a
# pre-acquired connection argument) can use mock_async_connection
# without needing the pool wrapper; own-pool methods (which acquire
# from self._pool.connection()) use mock_async_pool.
# ============================================================================


@pytest.fixture()
def mock_async_cursor(mocker: Any) -> Any:
    """Fresh ``AsyncMock`` cursor with the standard psycopg surface
    stubbed.

    Stubs:
        - ``execute``     : ``AsyncMock`` (no return value).
        - ``executemany`` : ``AsyncMock`` (no return value).
        - ``fetchone``    : ``AsyncMock`` returning ``None`` by default.
        - ``fetchall``    : ``AsyncMock`` returning ``[]`` by default.
        - ``rowcount``    : ``int`` attribute (``0`` by default).

    Tests should override the return values they care about::

        async def test_something(mock_async_cursor):
            mock_async_cursor.fetchone.return_value = {"id": 1}
            mock_async_cursor.rowcount = 1
            ...

    Why ``AsyncMock`` (not ``MagicMock``)?
        ``cursor.execute(...)`` is awaited in the production repository
        layer (``await cur.execute(...)``), so the underlying mock must
        return a coroutine. ``AsyncMock`` does this automatically;
        ``MagicMock`` would return a non-awaitable ``MagicMock``
        instance and trigger ``TypeError: object MagicMock can't be
        used in 'await' expression`` at the first awaited call.

    Args:
        mocker: pytest-mock's ``MockerFixture``, injected by the
            ``mocker`` fixture provided by ``pytest-mock``.

    Returns:
        An ``AsyncMock`` cursor pre-stubbed with the standard psycopg
        ``execute`` / ``executemany`` / ``fetchone`` / ``fetchall`` /
        ``rowcount`` surface.
    """
    cursor = mocker.AsyncMock()
    cursor.execute = mocker.AsyncMock()
    cursor.executemany = mocker.AsyncMock()
    cursor.fetchone = mocker.AsyncMock(return_value=None)
    cursor.fetchall = mocker.AsyncMock(return_value=[])
    cursor.rowcount = 0
    return cursor


@pytest.fixture()
def mock_async_connection(mocker: Any, mock_async_cursor: Any) -> Any:
    """``AsyncMock`` connection whose ``cursor()`` async-CM yields the
    ``mock_async_cursor``.

    Use case: caller-owned-TX repository methods (e.g.,
    ``stock.get_for_update``, ``reservation.lock_by_order``,
    ``stock_movement.append``) accept a connection argument; this
    fixture provides a ready-made stand-in.

    Example::

        async def test_caller_owned_tx(mock_async_connection,
                                       mock_async_cursor):
            mock_async_cursor.fetchone.return_value = {...}
            repo = StockRepository(mocker.MagicMock())  # pool unused
            result = await repo.get_for_update(
                mock_async_connection, product_id=..., warehouse_id=...
            )

    Why the ``__aenter__`` / ``__aexit__`` async-CM trick?
        psycopg's ``async with conn.cursor() as cur`` requires that
        ``conn.cursor()`` returns an *object* (not a coroutine) whose
        ``__aenter__`` is awaited. We model this with:
            - ``conn.cursor`` -- a synchronous ``MagicMock`` method
              that returns a context-manager-like return value when
              CALLED (not awaited).
            - ``conn.cursor.return_value.__aenter__`` -- an
              ``AsyncMock`` whose return value is the cursor.
            - ``conn.cursor.return_value.__aexit__`` -- an
              ``AsyncMock`` returning ``None`` (suppresses no
              exceptions).

        Why is ``conn.cursor`` explicitly set to ``MagicMock`` and not
        left as the AsyncMock-auto-specced default? Because
        ``AsyncMock`` specs every attribute access as another
        ``AsyncMock``, which means ``conn.cursor()`` would return a
        *coroutine* rather than the chained ``return_value`` -- and
        ``async with <coroutine>`` raises
        ``TypeError: 'coroutine' object does not support the
        asynchronous context manager protocol``. By assigning
        ``conn.cursor = mocker.MagicMock()``, we restore the
        synchronous-call semantics that psycopg's real
        ``AsyncConnection.cursor()`` exhibits.

    Args:
        mocker: pytest-mock's ``MockerFixture``.
        mock_async_cursor: The cursor that ``conn.cursor()`` yields.

    Returns:
        An ``AsyncMock`` connection whose ``cursor()`` async-CM yields
        the injected cursor.
    """
    conn = mocker.AsyncMock()
    # Override the AsyncMock-auto-specced async method with a
    # synchronous MagicMock so conn.cursor() returns the chained
    # context manager directly (not a coroutine). This matches
    # psycopg's real AsyncConnection.cursor() semantics.
    conn.cursor = mocker.MagicMock()
    conn.cursor.return_value.__aenter__ = mocker.AsyncMock(
        return_value=mock_async_cursor
    )
    conn.cursor.return_value.__aexit__ = mocker.AsyncMock(return_value=None)
    return conn


@pytest.fixture()
def mock_async_pool(mocker: Any, mock_async_connection: Any) -> Any:
    """``MagicMock`` pool whose ``connection()`` async-CM yields the
    ``mock_async_connection``.

    Use case: own-pool repository methods (e.g., ``warehouse.get_by_id``,
    ``reservation.get_by_order``, ``stock.get_by_product``,
    ``stock_movement.find_by_order``) acquire from
    ``self._pool.connection()``; this fixture provides a pool that
    fulfills that contract.

    Example::

        async def test_own_pool(mock_async_pool, mock_async_cursor):
            mock_async_cursor.fetchone.return_value = {...}
            repo = WarehouseRepository(mock_async_pool)
            result = await repo.get_by_id(warehouse_id=...)
            mock_async_pool.connection.assert_called_once()

    Why ``MagicMock`` for the pool (not ``AsyncMock``)?
        ``pool.connection()`` is *not* awaited -- it returns an
        async-CM that is then entered via ``async with``. A plain
        ``MagicMock`` returns the chained ``return_value`` directly,
        which is exactly what the production code does
        (``async with self._pool.connection() as conn``). Using
        ``AsyncMock`` would make ``pool.connection()`` itself a
        coroutine, breaking the ``async with`` contract.

    Args:
        mocker: pytest-mock's ``MockerFixture``.
        mock_async_connection: The connection that
            ``pool.connection()`` yields.

    Returns:
        A ``MagicMock`` pool whose ``connection()`` async-CM yields
        the injected connection.
    """
    pool = mocker.MagicMock()
    pool.connection.return_value.__aenter__ = mocker.AsyncMock(
        return_value=mock_async_connection
    )
    pool.connection.return_value.__aexit__ = mocker.AsyncMock(return_value=None)
    return pool


# ============================================================================
# Factory fixture for tests needing multiple distinct (pool, cursor) pairs
# ----------------------------------------------------------------------------
# The simple mock_async_pool fixture binds to a SINGLE cursor instance
# for the test's lifetime. Tests that issue multiple operations on
# multiple cursors -- notably the AAP R-15 tenacity retry tests in
# test_stock_item_repository.py, where each retry attempt opens a fresh
# cursor and the test wants to inspect each cursor's
# `execute.call_args_list` SEPARATELY -- need a way to mint fresh
# pairs on demand.
#
# The factory pattern is the canonical pytest idiom for this case:
# inject the fixture, call the returned callable as many times as
# needed, and each call returns an independent (pool, cursor) pair
# that does not share state with any other pair.
# ============================================================================


@pytest.fixture()
def mock_pool_with_cursor_factory(mocker: Any) -> Any:
    """Callable factory for tests needing distinct (pool, cursor) pairs
    -- e.g., tenacity retry tests that issue multiple operations and
    need to inspect each cursor's ``execute.call_args_list`` separately.

    Returns:
        A callable that, when invoked, returns ``(pool, cursor)``.
        Each invocation produces a **fresh, independent** pair: no
        attribute mutations on one pair affect another. This is
        critical for tests that assert per-attempt SQL call shapes
        (e.g., AAP R-15 retry tests).

    Example::

        async def test_multi_call(mock_pool_with_cursor_factory):
            pool_1, cursor_1 = mock_pool_with_cursor_factory()
            pool_2, cursor_2 = mock_pool_with_cursor_factory()

            # Configure each independently:
            cursor_1.fetchone.return_value = {"attempt": 1}
            cursor_2.fetchone.return_value = {"attempt": 2}

            # Inspect each independently:
            assert cursor_1.execute.call_count != cursor_2.execute.call_count

    Args:
        mocker: pytest-mock's ``MockerFixture``.

    Returns:
        A zero-argument callable. Each call returns ``(pool, cursor)``
        where ``pool`` is a ``MagicMock`` whose ``connection()`` async-CM
        yields a fresh ``AsyncMock`` connection, and ``cursor`` is a
        fresh ``AsyncMock`` cursor with the standard psycopg surface
        stubbed.
    """

    def _factory() -> tuple[Any, Any]:
        # Build a fresh cursor with the standard psycopg stub surface.
        cursor = mocker.AsyncMock()
        cursor.execute = mocker.AsyncMock()
        cursor.executemany = mocker.AsyncMock()
        cursor.fetchone = mocker.AsyncMock(return_value=None)
        cursor.fetchall = mocker.AsyncMock(return_value=[])
        cursor.rowcount = 0

        # Build a fresh connection whose cursor() async-CM yields the
        # cursor above. conn.cursor is explicitly MagicMock (not
        # AsyncMock) so calling conn.cursor() returns the chained
        # context manager synchronously rather than as a coroutine.
        # See mock_async_connection's docstring for the full rationale.
        conn = mocker.AsyncMock()
        conn.cursor = mocker.MagicMock()
        conn.cursor.return_value.__aenter__ = mocker.AsyncMock(
            return_value=cursor
        )
        conn.cursor.return_value.__aexit__ = mocker.AsyncMock(return_value=None)

        # Build a fresh pool whose connection() async-CM yields the
        # connection above.
        pool = mocker.MagicMock()
        pool.connection.return_value.__aenter__ = mocker.AsyncMock(
            return_value=conn
        )
        pool.connection.return_value.__aexit__ = mocker.AsyncMock(
            return_value=None
        )
        return pool, cursor

    return _factory


# ============================================================================
# Defensive autouse hermetic-boundary guard
# ----------------------------------------------------------------------------
# This is the most critical fixture in the file. Every other fixture
# is an OPTIONAL convenience -- this one is a defense-in-depth check
# that fires for EVERY test in this folder regardless of whether the
# test opts into other fixtures.
#
# The 4 sibling test files use mocker.MagicMock() for the pool, so
# they NEVER touch the real psycopg_pool.AsyncConnectionPool class.
# But if a future contributor accidentally imports the real class and
# instantiates it (perhaps by copy-pasting boilerplate from an
# integration test), THIS guard ensures the test fails LOUDLY at
# construction time with a clear remediation pointer, rather than
# either:
#   (a) silently succeeding and reaching a real Postgres instance
#       (defeating the hermetic boundary), or
#   (b) failing later at first SQL call with an opaque connection
#       error that doesn't point the developer at the root cause.
#
# Implementation notes:
# - Uses pytest's built-in monkeypatch fixture (NOT mocker.patch)
#   because the patching is environmental (applies to every test in
#   this folder) rather than test-specific (opt-in via mocker).
# - Imports psycopg_pool DEFERRED inside the fixture body and wrapped
#   in try/except so the conftest remains COLLECTIBLE even if
#   psycopg_pool is not installed (e.g., during partial scaffolding
#   when the runtime requirements have not yet been pip-installed).
# - The replacement constructor accepts variadic *args/**kwargs to
#   faithfully match AsyncConnectionPool's signature.
# ============================================================================


@pytest.fixture(autouse=True)
def assert_no_real_psycopg_connections(monkeypatch: pytest.MonkeyPatch) -> None:
    """Defensive guard: ensures NO test in this folder accidentally
    instantiates a real ``AsyncConnectionPool`` bound to a live
    database.

    If a test imports ``psycopg_pool.AsyncConnectionPool`` and tries
    to instantiate it, the resulting object will raise on use. This
    fixture monkeypatches the class so any accidental instantiation
    fails LOUDLY at construction time rather than at first SQL call.

    This is a defense-in-depth check -- the 4 sibling test files in
    this folder use ``mocker.MagicMock`` for the pool, so they never
    touch the real class. But if a future test imports the real class,
    this guard ensures it can't silently succeed and reach a real
    Postgres instance.

    Notes:
        - Only patches the symbol at ``psycopg_pool.AsyncConnectionPool``.
          Tests that use ``mocker.MagicMock()`` (the established
          pattern) are unaffected.
        - The error message points the developer at the canonical
          ``mocker.MagicMock()`` pattern.
        - If ``psycopg_pool`` is not installed (partial scaffolding
          state), the fixture is a no-op so the conftest remains
          collectible.
        - ``monkeypatch`` (pytest's built-in fixture) is used instead
          of ``mocker.patch`` because the patching is environmental
          (applies to every test in this folder, regardless of
          whether the test opts into ``mocker``).

    Args:
        monkeypatch: pytest's built-in ``MonkeyPatch`` fixture, used
            to apply the patch with automatic teardown at the end of
            each test.

    Returns:
        ``None``. The fixture has side-effects only (patching) and
        returns no value.
    """
    try:
        import psycopg_pool
    except ImportError:
        # If psycopg_pool isn't installed, there's nothing to guard
        # against -- mocker fixtures will still work fine.
        return

    def _forbidden_constructor(*args: Any, **kwargs: Any) -> None:
        """Replacement for ``AsyncConnectionPool.__init__`` that raises
        on any instantiation attempt.

        The variadic signature matches the real
        ``AsyncConnectionPool`` constructor's flexibility (it accepts
        keyword-only configuration plus an optional positional
        connection-string argument).
        """
        raise RuntimeError(
            "Hermetic boundary violated: tests in "
            "services/inventory-service/tests/unit/repository/ MUST NOT "
            "instantiate a real AsyncConnectionPool. Use "
            "mocker.MagicMock() (with .connection().__aenter__ async-CM) "
            "instead. See sibling tests for the canonical pattern."
        )

    monkeypatch.setattr(
        psycopg_pool, "AsyncConnectionPool", _forbidden_constructor
    )
