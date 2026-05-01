"""Repository for the ``idempotency_keys`` table (inbound HTTP idempotency).

Implements :class:`IdempotencyRepository` --- async access to the
``idempotency_keys`` table in ``payment_db``. This is the only module
permitted to read from or write to that table (per AAP R-6).

Inbound vs outbound idempotency
--------------------------------
Payment Service tracks **two distinct flavors** of idempotency:

1. **Inbound HTTP idempotency** (this table). Clients send a unique
   ``Idempotency-Key`` header on POST requests; the middleware in
   :mod:`src.idempotency.store` looks up ``(key, endpoint)`` on entry
   and stores the response on exit. This guarantees that a client retry
   (e.g., due to a network blip) returns the *same* response without
   re-executing side effects.

2. **Outbound provider idempotency**, persisted as
   ``payment_attempts.idempotency_key``. The application generates a
   unique key for each outbound Stripe / Razorpay call and includes it
   in the provider request --- both providers honor this header and
   guarantee exactly-once charge / refund semantics. This is in
   :class:`PaymentAttemptsRepository` (``src/repository/payments_repo.py``),
   not here.

Both flavors are mandated by AAP R-8.

Schema
------
* PK: ``id`` (UUID, ``gen_random_uuid()`` server-side default)
* UNIQUE: ``(key, endpoint)`` --- the same idempotency key may be reused
  on different endpoints (rare, but allowed) without conflict. This
  matches Stripe's documented behavior for cross-endpoint key reuse.
* ``request_hash``: SHA-256 (or similar) digest of the canonicalized
  request body. Used to detect "same key, different request" --- that
  is a client error and triggers HTTP 409 Conflict.
* ``response_status``: integer HTTP status code (e.g., ``200``,
  ``201``, ``409``).
* ``response_body``: JSONB containing the canonical response payload.
  ``Json(...)`` adapter required for binding.
* ``expires_at``: TTL boundary (default 72h per :mod:`config.settings`
  ``settings.idempotency.ttl_hours``). Rows past expiry are pruned by
  the sweeper.

Methods
-------
* :meth:`IdempotencyRepository.lookup` --- fetch the row for
  ``(key, endpoint)`` if not yet expired. Returns the cached response
  so the middleware can short-circuit the handler chain.
* :meth:`IdempotencyRepository.insert` --- race-safe INSERT after handler
  completion. Returns whether the row was newly inserted or whether a
  conflict was caught.
* :meth:`IdempotencyRepository.delete_expired` --- sweeper for
  past-TTL rows; called by the scheduled job in
  :mod:`src.idempotency.sweeper`.
* :meth:`IdempotencyRepository.lookup_including_expired` --- diagnostic
  fetch that ignores TTL (forensic / support tooling only).

Conflict semantics
------------------
:meth:`IdempotencyRepository.lookup` does NOT compare request hashes ---
it returns whatever is cached for ``(key, endpoint)``. The middleware
compares ``stored.request_hash`` against the inbound request's hash and:

* **Equal hashes** -> return cached response (HTTP idempotent replay).
* **Different hashes** -> raise :class:`IdempotencyMismatchError`
  (HTTP 409, "this key was used with a different request"); see
  :mod:`src.domain.exceptions`.

Logging contract (AAP R-26)
---------------------------
Every public method emits a structured log line on completion. The log
record includes ``endpoint``, ``idempotency_id`` (UUID --- not sensitive),
``cached_status``, and ``latency_ms``. The following fields are
**deliberately NEVER logged**:

* ``key`` --- may correlate with client identifiers in some deployments.
* ``request_hash`` --- not secret, but high-noise; ``endpoint`` plus
  ``idempotency_id`` already provide sufficient correlation context.
* ``response_body`` --- may contain customer PII (email, name, address)
  routed through a payments response.

Operations slower than :data:`_SLOW_QUERY_THRESHOLD_MS` are logged at
WARNING level so ops dashboards surface degraded DB latency immediately.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from time import perf_counter
from typing import Any, Final
from uuid import UUID

import structlog
from psycopg.rows import dict_row
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

# =============================================================================
# Module-level constants
# =============================================================================

# Module-scoped structured logger. ``structlog.get_logger`` returns a
# proxy whose processor chain is configured globally during the
# application's lifespan startup; the proxy is bound lazily on first
# use, which keeps import time cheap and avoids re-binding on every
# function call. Per AAP R-26 this is a structured JSON emitter.
_LOGGER: Final[structlog.stdlib.BoundLogger] = structlog.get_logger(__name__)

# Operations exceeding this latency budget log at WARNING instead of
# INFO so ops dashboards can alert on slow DB calls. The 500 ms
# threshold is roughly 5x the p95 of a healthy single-row PostgreSQL
# query at this workload; sustained breaches indicate connection-pool
# saturation, lock contention, or unhealthy network paths.
_SLOW_QUERY_THRESHOLD_MS: Final[int] = 500

# -----------------------------------------------------------------------------
# SQL constants
# -----------------------------------------------------------------------------
# All SQL is parameterized via psycopg's ``%s`` placeholder. NEVER inline
# values via f-strings or ``%``-formatting --- doing so would make this
# layer SQL-injection-vulnerable and is forbidden by AAP R-25.
#
# All SQL strings are kept at module scope as ``Final[str]`` so:
#   1. mypy --strict can validate immutability,
#   2. psycopg's prepared-statement cache hits efficiently (the same
#      query text is reused across every call), and
#   3. SQL is reviewable in one place rather than scattered through
#      method bodies.
# -----------------------------------------------------------------------------

# SELECT a non-expired row for ``(key, endpoint)``. The TTL comparison
# uses the database's ``NOW()`` so the answer is independent of any
# application-side clock skew (an application clock that runs slightly
# fast must NOT cause a row the database considers expired to leak back
# to the client).
_SQL_LOOKUP_BY_KEY_ENDPOINT: Final[str] = """
    SELECT
        id,
        key,
        endpoint,
        request_hash,
        response_status,
        response_body,
        created_at,
        expires_at
    FROM idempotency_keys
    WHERE key = %s
      AND endpoint = %s
      AND expires_at > NOW()
    LIMIT 1
"""

# Race-safe INSERT. ``ON CONFLICT (key, endpoint) DO NOTHING`` collapses
# concurrent duplicate inserts into a single survivor without raising
# ``IntegrityError`` --- the loser sees ``RETURNING id`` produce no row
# (``cur.fetchone()`` returns ``None``). The application's
# ``inserted: bool`` return value is computed from that absence.
#
# ``gen_random_uuid()`` and ``NOW()`` execute server-side so the values
# are guaranteed to be consistent with the row's other server-side
# defaults (and avoid round-tripping a Python ``uuid4()`` for what the
# database can produce locally).
_SQL_INSERT_KEY: Final[str] = """
    INSERT INTO idempotency_keys (
        id,
        key,
        endpoint,
        request_hash,
        response_status,
        response_body,
        created_at,
        expires_at
    )
    VALUES (
        gen_random_uuid(),
        %s,
        %s,
        %s,
        %s,
        %s,
        NOW(),
        %s
    )
    ON CONFLICT (key, endpoint) DO NOTHING
    RETURNING id
"""

# Sweeper DELETE for past-TTL rows. The migration adds an index on
# ``expires_at`` precisely so this scan is index-driven; without it the
# sweep would degrade into a full-table seq scan as the table grows.
# Caller examines ``cur.rowcount`` for the number of deleted rows ---
# DELETE has no rows to RETURN unless explicitly requested, and counting
# deleted rows via ``rowcount`` is the canonical psycopg pattern.
_SQL_DELETE_EXPIRED: Final[str] = """
    DELETE FROM idempotency_keys
    WHERE expires_at < NOW()
"""

# Diagnostic SELECT that does NOT filter by ``expires_at``. Used by
# :meth:`IdempotencyRepository.lookup_including_expired` so support
# tooling can investigate "why did my client get a different response
# on retry?" --- the answer might be "your retry came in after the TTL
# window expired and the row was swept / is about to be swept". Never
# called on the production hot path.
_SQL_LOOKUP_INCLUDING_EXPIRED: Final[str] = """
    SELECT
        id,
        key,
        endpoint,
        request_hash,
        response_status,
        response_body,
        created_at,
        expires_at
    FROM idempotency_keys
    WHERE key = %s
      AND endpoint = %s
    LIMIT 1
"""


# =============================================================================
# IdempotencyRow --- pure DB-row transport object
# =============================================================================


@dataclass(frozen=True, slots=True)
class IdempotencyRow:
    """A single row from ``idempotency_keys``.

    All fields mirror the DB columns 1:1. ``response_body`` is the JSONB
    payload deserialized to a Python value (``dict``, ``list``, ``str``,
    ``int``, ``bool``, or ``None``) by psycopg3's default JSON loader.

    A frozen dataclass with ``slots=True`` is the right shape for this
    object because:

    * It is a pure DB-row transport with no validation requirements ---
      the database is the single source of truth for the column shape.
    * ``frozen=True`` prevents accidental mutation by the middleware.
    * ``slots=True`` avoids per-instance ``__dict__`` overhead in the hot
      path (every inbound POST request hits one of these on lookup).
    * Avoiding Pydantic dodges a circular dependency with the domain
      layer that owns the typed response-model classes --- and dodges
      Pydantic's per-instance validation cost which buys nothing here.

    Attributes:
        id: Server-generated UUID primary key.
        key: The ``Idempotency-Key`` header value.
        endpoint: Canonical endpoint identifier (e.g.,
            ``"POST /payments"``).
        request_hash: Hex-encoded digest (typically SHA-256) of the
            canonicalized request body.
        response_status: HTTP status code returned by the original
            handler invocation.
        response_body: JSON-deserialized response body. Typed as
            :class:`typing.Any` because the repository deliberately does
            not (and should not) know the response shape of every
            endpoint --- routing endpoint shape concerns into a generic
            DB cache layer is a violation of the repository pattern.
        created_at: Server-side timestamp when the row was inserted.
        expires_at: Absolute TTL boundary; rows past this time are
            invisible to :meth:`IdempotencyRepository.lookup` and
            eligible for :meth:`IdempotencyRepository.delete_expired`.
    """

    id: UUID
    key: str
    endpoint: str
    request_hash: str
    response_status: int
    response_body: Any
    created_at: datetime
    expires_at: datetime


# =============================================================================
# IdempotencyRepository --- async data-access class
# =============================================================================


class IdempotencyRepository:
    """Async repository over the ``idempotency_keys`` table.

    Backs the inbound HTTP idempotency middleware
    (:mod:`src.idempotency.store`). Methods are designed so the
    middleware can implement the full lifecycle:

    1. On request entry: ``lookup(key, endpoint)`` --- if hit, the
       middleware compares the stored ``request_hash`` against the
       inbound request's hash and either replays the cached response
       (200 / 201 / ...) or raises
       :class:`src.domain.exceptions.IdempotencyMismatchError` (409).
    2. On response completion: ``insert(...)`` --- race-safe.
    3. Periodically: ``delete_expired()`` --- sweep past-TTL rows.
    4. On demand (forensic): ``lookup_including_expired(...)``.

    Each method opens (and closes) one pooled connection. There is no
    cross-method transaction --- the middleware's "lookup -> handler ->
    insert" sequence intentionally is NOT a single DB transaction
    because the handler's work (charging Stripe / Razorpay) takes
    seconds, and holding a transaction open across that span would
    exhaust the pool.

    Attributes:
        _pool: psycopg3 :class:`AsyncConnectionPool`. Constructed once
            in :mod:`src.repository.base` (``make_pg_pool``), registered
            on the DI container, and shared across all repositories per
            AAP Section 0.4.3.
    """

    __slots__ = ("_pool",)

    def __init__(self, pool: AsyncConnectionPool) -> None:
        """Construct the repository with an injected connection pool.

        Args:
            pool: A configured :class:`AsyncConnectionPool`. The pool is
                shared with all sibling repositories in the Payment
                Service so connection-count budgets are bounded
                centrally. The constructor signature is intentionally
                positional-only (no encryption backend, no query
                timeout, no per-instance configuration) so the DI
                container can call ``IdempotencyRepository(pg_pool)``
                with no keyword arguments --- this matches the contract
                in :mod:`src.container`.
        """
        self._pool = pool

    # ------------------------------------------------------------------
    # lookup --- production hot path
    # ------------------------------------------------------------------

    async def lookup(self, key: str, endpoint: str) -> IdempotencyRow | None:
        """Return the cached response for ``(key, endpoint)`` if not expired.

        The TTL filter (``expires_at > NOW()``) runs in the database so
        the answer is invariant under clock skew between the application
        instance and the database --- the database's clock is the
        canonical clock for this comparison.

        Args:
            key: The ``Idempotency-Key`` header value from the inbound
                request. Length-validated by the middleware
                (:mod:`src.idempotency.key_validator`) before this
                method is reached; typical bounds are 1-255 characters.
            endpoint: Canonical endpoint identifier --- typically the
                HTTP method joined with the route (e.g.,
                ``"POST /payments"``,
                ``"POST /payments/{id}/refunds"``). Stored verbatim;
                the caller is responsible for canonicalization.

        Returns:
            An :class:`IdempotencyRow` instance if the key exists and
            has not expired, or ``None`` when the key has never been
            seen or has expired. ``None`` instructs the middleware to
            execute the handler and call :meth:`insert` afterward.
        """
        started = perf_counter()
        async with (
            self._pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cur,
        ):
            await cur.execute(
                _SQL_LOOKUP_BY_KEY_ENDPOINT,
                (key, endpoint),
            )
            row = await cur.fetchone()
        latency_ms = int((perf_counter() - started) * 1000)
        log_level = "warning" if latency_ms > _SLOW_QUERY_THRESHOLD_MS else "info"

        if row is None:
            # Cache miss. Do NOT log ``key`` (may correlate with client
            # identifiers); ``endpoint`` is enough for correlation.
            getattr(_LOGGER, log_level)(
                "idempotency_lookup_miss",
                endpoint=endpoint,
                latency_ms=latency_ms,
            )
            return None

        # Cache hit. Log the UUID (not sensitive) and the cached HTTP
        # status (operationally useful for ops dashboards) but NOT the
        # ``key``, ``request_hash``, or ``response_body``.
        getattr(_LOGGER, log_level)(
            "idempotency_lookup_hit",
            idempotency_id=str(row["id"]),
            endpoint=endpoint,
            cached_status=row["response_status"],
            latency_ms=latency_ms,
        )
        return IdempotencyRow(
            id=row["id"],
            key=row["key"],
            endpoint=row["endpoint"],
            request_hash=row["request_hash"],
            response_status=row["response_status"],
            response_body=row["response_body"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
        )

    # ------------------------------------------------------------------
    # insert --- post-handler caching
    # ------------------------------------------------------------------

    async def insert(
        self,
        *,
        key: str,
        endpoint: str,
        request_hash: str,
        response_status: int,
        response_body: Any,
        expires_at: datetime,
    ) -> bool:
        """Race-safely insert a new idempotency row.

        Uses ``ON CONFLICT (key, endpoint) DO NOTHING`` so two
        concurrent requests with the same key (rare under normal
        traffic, common during burst retries) do not both succeed ---
        exactly one wins. The loser's caller (the middleware) MAY
        choose to re-call :meth:`lookup` to obtain the winner's cached
        response, but that is an explicit middleware-level decision;
        this layer only signals win / lose via the boolean return.

        Args:
            key: ``Idempotency-Key`` header value.
            endpoint: Canonical endpoint identifier.
            request_hash: Hex-encoded digest (typically SHA-256) of the
                canonicalized request body. Used by the middleware on
                subsequent lookups to detect "same key, different
                request" conflicts. Storing the digest (one-way) is
                safe; storing the body itself would not be.
            response_status: HTTP status code returned by the handler.
            response_body: JSON-serializable response body. Bound as
                JSONB via :class:`psycopg.types.json.Json`.
            expires_at: Absolute UTC time after which the cached
                response is invalid. Caller computes this as
                ``now + settings.idempotency.ttl_hours`` (default 72h
                per AAP, matching Stripe's documented retention).

        Returns:
            ``True`` if the INSERT created a new row. ``False`` if a
            row already existed for ``(key, endpoint)`` --- the caller's
            response is dropped; the existing cached response is the
            canonical one.

        Note:
            A ``False`` return is NOT an error. It means a concurrent
            request won the race --- the contract is "I tell you
            whether you won; you decide what to do with that
            information." Specifically, this layer does NOT raise an
            exception for the conflict case (concurrent-insert race is
            an EXPECTED case under load, per AAP R-15 / R-17 retry
            semantics), and it does NOT compare ``request_hash``
            against the existing row (that is the middleware's job).
        """
        # Bind ``response_body`` through ``Json(...)`` so psycopg sends
        # it to the JSONB column with the correct adapter. Passing a
        # bare Python ``dict`` would either fail or be coerced to a
        # text representation that breaks the JSONB roundtrip.
        params: tuple[str, str, str, int, Json, datetime] = (
            key,
            endpoint,
            request_hash,
            response_status,
            Json(response_body),
            expires_at,
        )

        started = perf_counter()
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_SQL_INSERT_KEY, params)
                row = await cur.fetchone()
            # psycopg3 async does NOT auto-commit; without an explicit
            # ``commit`` the row is rolled back when the connection is
            # returned to the pool.
            await conn.commit()
        latency_ms = int((perf_counter() - started) * 1000)

        log_level = "warning" if latency_ms > _SLOW_QUERY_THRESHOLD_MS else "info"

        # Branch on ``row is not None`` directly (rather than going
        # through an intermediate boolean) so mypy --strict can narrow
        # the row type from ``tuple | None`` to ``tuple`` inside the
        # success branch. ``row`` is a 1-tuple of just ``id`` because
        # the cursor was opened without a row factory.
        if row is not None:
            getattr(_LOGGER, log_level)(
                "idempotency_inserted",
                idempotency_id=str(row[0]),
                endpoint=endpoint,
                response_status=response_status,
                latency_ms=latency_ms,
            )
            return True

        # Concurrent insert beat us --- operationally interesting
        # because frequent conflicts on the same (key, endpoint) may
        # signal client retry storms or load-balancer hash imbalance.
        getattr(_LOGGER, log_level)(
            "idempotency_insert_conflict",
            endpoint=endpoint,
            response_status=response_status,
            latency_ms=latency_ms,
        )
        return False

    # ------------------------------------------------------------------
    # delete_expired --- scheduled sweeper
    # ------------------------------------------------------------------

    async def delete_expired(self) -> int:
        """Delete all rows where ``expires_at < NOW()`` and return the count.

        Called periodically by the scheduled sweeper job in
        :mod:`src.idempotency.sweeper`. Acceptable to run frequently
        (every 15 minutes is the documented default) because the
        ``WHERE`` clause is index-supported via the
        ``idx_idempotency_keys_expires_at`` index added by migration
        ``20260101_000001_initial_schema``. On a healthy system, the
        deleted count per sweep is bounded by
        ``sweep_interval * peak_inbound_rps``.

        Returns:
            The number of rows deleted in this sweep. ``0`` is normal
            on a low-traffic instance or immediately after a previous
            sweep.
        """
        started = perf_counter()
        async with self._pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_SQL_DELETE_EXPIRED)
                # ``cursor.rowcount`` is the canonical psycopg way to
                # get the affected-row count for DML statements.
                deleted = cur.rowcount
            await conn.commit()
        latency_ms = int((perf_counter() - started) * 1000)
        log_level = "warning" if latency_ms > _SLOW_QUERY_THRESHOLD_MS else "info"
        getattr(_LOGGER, log_level)(
            "idempotency_sweep_completed",
            deleted=deleted,
            latency_ms=latency_ms,
        )
        return deleted

    # ------------------------------------------------------------------
    # lookup_including_expired --- diagnostic / forensic
    # ------------------------------------------------------------------

    async def lookup_including_expired(
        self,
        key: str,
        endpoint: str,
    ) -> IdempotencyRow | None:
        """Diagnostic lookup that ignores TTL.

        Used by support tooling to investigate "why did my client get
        a different response on retry?" --- the answer might be "your
        retry happened after the TTL window expired and the row was
        swept" or "your retry happened after the TTL window expired
        but before the next sweep, so the row is technically still
        in the table but invisible to production callers."

        Returns ``None`` if no row at all (sweeper has run); returns
        the row even if past expiry, otherwise.

        Args:
            key: The ``Idempotency-Key`` value to look up.
            endpoint: Canonical endpoint identifier.

        Returns:
            An :class:`IdempotencyRow` (possibly past expiry) or
            ``None``.
        """
        started = perf_counter()
        async with (
            self._pool.connection() as conn,
            conn.cursor(row_factory=dict_row) as cur,
        ):
            await cur.execute(
                _SQL_LOOKUP_INCLUDING_EXPIRED,
                (key, endpoint),
            )
            row = await cur.fetchone()
        latency_ms = int((perf_counter() - started) * 1000)
        log_level = "warning" if latency_ms > _SLOW_QUERY_THRESHOLD_MS else "info"

        if row is None:
            getattr(_LOGGER, log_level)(
                "idempotency_diagnostic_miss",
                endpoint=endpoint,
                latency_ms=latency_ms,
            )
            return None

        # ``expires_at`` is a tz-aware ``datetime`` (the column is
        # ``TIMESTAMP WITH TIME ZONE``); using its tzinfo on the
        # right-hand-side of ``datetime.now(...)`` is the cleanest
        # tz-aware comparison without importing ``timezone``.
        is_expired = row["expires_at"] <= datetime.now(row["expires_at"].tzinfo)
        getattr(_LOGGER, log_level)(
            "idempotency_diagnostic_hit",
            idempotency_id=str(row["id"]),
            endpoint=endpoint,
            is_expired=is_expired,
            latency_ms=latency_ms,
        )
        return IdempotencyRow(
            id=row["id"],
            key=row["key"],
            endpoint=row["endpoint"],
            request_hash=row["request_hash"],
            response_status=row["response_status"],
            response_body=row["response_body"],
            created_at=row["created_at"],
            expires_at=row["expires_at"],
        )


# =============================================================================
# Public exports
# =============================================================================
# Listed alphabetically. Both names are part of the documented public
# surface of this module: callers (the inbound idempotency middleware)
# need both the repository class AND the row dataclass to construct
# typed values from ``lookup`` results.

__all__ = [
    "IdempotencyRepository",
    "IdempotencyRow",
]
