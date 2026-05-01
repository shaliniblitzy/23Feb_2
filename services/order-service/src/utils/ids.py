"""Identifier generation helpers for the Order Service.

The bottom of the dependency graph alongside :mod:`src.utils.time`. It
provides a single, monkeypatch-friendly source for every UUID and
correlation-id the service mints, so tests can deterministically assert
on generated ids and so all id generation routes through one audited
code path.

Architectural rules (folder spec + AAP R-13):
    * NO imports from ``src.*`` — pure stdlib only.
    * Side-effect-free at import time — no logging, no env reads, no I/O.
    * Deterministic in tests — patch ``src.utils.ids.new_*`` to fix ids.
    * Every function has a precise return type annotation.

Public surface:
    * :func:`new_order_id`       -> :class:`uuid.UUID`  (random v4;
      matches ``orders.id`` Postgres column type per AAP Section 0.4.4)
    * :func:`new_saga_id`        -> :class:`uuid.UUID`  (random v4;
      matches ``saga_state.saga_id`` per AAP R-18 saga pattern)
    * :func:`new_correlation_id` -> :class:`str`        (string form
      for ``X-Correlation-ID`` HTTP header / log field per AAP R-13/R-26)

ULID support is intentionally NOT exposed — the folder spec mandates
"default is uuid4 for simplicity" and this module honors that. A future
PR can add ``new_ulid`` if monotonic ordering becomes a hard requirement;
until then, ``uuid4`` (collision-safe, schema-stable, natively supported
by Postgres + ``psycopg`` v3) is sufficient.

Distinct functions, not aliases — ``new_order_id`` and ``new_saga_id``
deliberately are not the same callable so tests can monkeypatch each
generator independently without one fix bleeding into the other.
"""

from __future__ import annotations

from uuid import UUID, uuid4

# Public surface — explicit, narrow, and stable. ``UUID`` and ``uuid4``
# are imported for internal use (typing / generation) and are NOT
# re-exported; callers needing the raw stdlib symbols should import
# them directly from :mod:`uuid` to keep this module's contract minimal.
__all__ = [
    "new_order_id",
    "new_saga_id",
    "new_correlation_id",
]


def new_order_id() -> UUID:
    """Return a freshly generated random UUID v4 for use as an order id.

    The Order Service can either let Postgres mint the id via the
    ``gen_random_uuid()`` ``DEFAULT`` on ``orders.id`` OR mint it app-side
    via this helper. Pattern (b) is REQUIRED when the order placement
    endpoint must know the id BEFORE the row is committed — to emit
    ``order.created`` events (AAP R-30) carrying the canonical id, to
    return the id in the API response before the saga starts, and to
    write ``idempotency_key -> order_id`` mappings before the row insert
    so duplicate ``POST /orders`` within the same TTL short-circuit and
    replay the cached response (AAP idempotency requirement).

    Both patterns produce random v4 UUIDs and are wire-compatible —
    ``psycopg`` v3 round-trips Python :class:`uuid.UUID` <-> Postgres
    ``uuid`` natively. Stringification happens at JSON / Kafka boundaries.

    Tests can fix the id by monkeypatching ``src.utils.ids.new_order_id``::

        fixed = UUID("00000000-0000-4000-8000-000000000001")
        monkeypatch.setattr("src.utils.ids.new_order_id", lambda: fixed)

    Returns:
        UUID: a freshly generated, statistically-independent random UUID v4.
    """
    return uuid4()


def new_saga_id() -> UUID:
    """Return a freshly generated random UUID v4 for use as a saga id.

    Every order has exactly one saga that coordinates its lifecycle
    (CREATE -> RESERVING -> PAYING -> CONFIRMED | COMPENSATING ->
    CANCELLED) per AAP R-18. The ``saga_state.saga_id`` column is
    INTENTIONALLY independent of ``orders.id`` so a future "retry the
    failed checkout as a new saga" feature can mint a fresh saga without
    re-issuing the order id (today the relationship is 1:1 but the
    schema does not couple them).

    The :class:`uuid.UUID` return type matches the ``saga_state.saga_id``
    Postgres ``uuid`` column type (AAP Section 0.4.4).

    Tests can fix the id by monkeypatching ``src.utils.ids.new_saga_id``::

        fixed = UUID("00000000-0000-4000-8000-000000000002")
        monkeypatch.setattr("src.utils.ids.new_saga_id", lambda: fixed)

    Returns:
        UUID: a freshly generated, statistically-independent random UUID v4
        — distinct from any value :func:`new_order_id` produces even when
        called in the same scope.
    """
    return uuid4()


def new_correlation_id() -> str:
    """Return a freshly generated correlation-id STRING for log/header use.

    Used by ``src/middleware/correlation_id.py`` when an inbound HTTP
    request does NOT carry an ``X-Correlation-ID`` header (e.g., direct
    service-to-service traffic that bypassed the API Gateway, or test
    fixtures that didn't supply one).

    Per AAP R-13, the correlation id MUST appear on every structured log
    line (AAP R-26), every outbound HTTP request made during the request
    scope (the ``httpx`` ``event_hook`` reads it from contextvars and
    sets the header automatically), and every Kafka event message header
    produced during the request scope (so downstream services —
    payment-service, inventory-service, notification-service,
    recommendation-engine — can stitch the end-to-end trace).

    The string form is the canonical wire format. UUIDs are emitted in
    their lower-case hyphenated 36-character form
    (``xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx``) which fits comfortably in
    HTTP headers and structured-log indexes alike.

    Tests can fix the value by monkeypatching
    ``src.utils.ids.new_correlation_id``::

        monkeypatch.setattr(
            "src.utils.ids.new_correlation_id",
            lambda: "00000000-0000-4000-8000-000000000003",
        )

    Returns:
        str: a freshly generated UUID v4 in lower-case hyphenated string
        form (length 36); a valid input to :class:`uuid.UUID`.
    """
    return str(uuid4())
