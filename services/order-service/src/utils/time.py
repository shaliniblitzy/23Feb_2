"""Deterministic time helpers for the Order Service.

The bottom of the dependency graph alongside :mod:`src.utils.ids`. Keeps
the rest of the service free of inline ``datetime.now()`` /
``datetime.utcnow()`` calls — neither is acceptable for the saga state
machine (AAP R-18) or for Kafka event payloads / structured-log
timestamps (AAP R-26).

Architectural rules (folder spec):
    * NO imports from ``src.*`` — pure stdlib only.
    * Side-effect-free at import time — no logging, no env reads, no I/O.
    * Deterministic in tests — patch ``src.utils.time.utcnow`` to freeze
      the clock; saga deadline assertions rely on this single seam.

Public surface:
    * :func:`utcnow`        -> :class:`~datetime.datetime`  (canonical clock).
    * :func:`monotonic_ms`  -> :class:`int`                 (latency timer).
    * :func:`to_rfc3339`    -> :class:`str`                 (wire format).
    * :func:`parse_rfc3339` -> :class:`~datetime.datetime`  (strict inverse).
    * :func:`add_ms`        -> :class:`~datetime.datetime`  (deadline math).

The filename ``time.py`` shadows the stdlib ``time`` module within the
package namespace; the alias ``import time as _time`` makes the stdlib
origin obvious at every call site.
"""

from __future__ import annotations

import time as _time
from datetime import datetime, timedelta, timezone

# Public surface — explicit, narrow, and stable.
__all__ = [
    "utcnow",
    "monotonic_ms",
    "to_rfc3339",
    "parse_rfc3339",
    "add_ms",
]


def utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime.

    THIS IS THE CANONICAL CLOCK FOR THE ORDER SERVICE. Every module
    that needs "now" MUST call ``utcnow`` instead of
    :func:`datetime.datetime.utcnow` (naive, deprecated as of Python
    3.12) or :func:`datetime.datetime.now` without ``tz`` (local-zone,
    wrong in distributed systems). Tests fix the clock by monkeypatching
    ``src.utils.time.utcnow``.

    Returns:
        datetime: current UTC time, always timezone-aware
        (``tzinfo`` is ``datetime.timezone.utc``).
    """
    return datetime.now(timezone.utc)


def monotonic_ms() -> int:
    """Return a monotonic millisecond timestamp suitable for latency math.

    Wraps :func:`time.monotonic_ns` (immune to wall-clock jumps from
    NTP / DST / manual edits). Single-process duration timer only —
    absolute value is meaningless across processes. Integer return,
    not float, keeps Prometheus histogram bucketing reproducible and
    matches the structured-log ``latency_ms`` field type per AAP R-26.

    Returns:
        int: monotonic millisecond timestamp, non-decreasing within
        the running process.
    """
    return _time.monotonic_ns() // 1_000_000


def to_rfc3339(dt: datetime) -> str:
    """Format a datetime as RFC 3339 with millisecond precision.

    Output: ``YYYY-MM-DDTHH:MM:SS.mmmZ`` (e.g.,
    ``"2025-01-01T12:34:56.789Z"``). Canonical timestamp format for
    Kafka event payloads, structured logs (AAP R-26),
    ``order_status_history.occurred_at``, and saga-state API responses.
    Naive inputs are assumed UTC (defensive); aware inputs are
    normalized. Sub-millisecond precision is **truncated, not rounded**
    for reproducibility. Millisecond precision matches Elasticsearch's
    default ``strict_date_optional_time`` mapping.

    Args:
        dt: a :class:`datetime.datetime` to format.

    Returns:
        str: RFC 3339 string matching
        ``^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$``.
    """
    if dt.tzinfo is None:
        # Naive -> UTC; raising would cripple the logging pipeline.
        dt = dt.replace(tzinfo=timezone.utc)
    aware_utc = dt.astimezone(timezone.utc)
    # ``%f`` is microseconds (6 digits); ``[:-3]`` truncates to ms.
    return aware_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_rfc3339(s: str) -> datetime:
    """Parse an RFC 3339 timestamp string into a UTC-aware datetime.

    Accepts the canonical form produced by :func:`to_rfc3339` plus
    common variants (numeric offsets like ``+00:00`` / ``+05:30``,
    microsecond precision). STRICTLY rejects empty / whitespace-only /
    non-string inputs and timestamps missing timezone info — silently
    assuming UTC for a missing zone would mask producer bugs.

    Args:
        s: an RFC 3339 / ISO 8601 timestamp string.

    Returns:
        datetime: parsed datetime normalized to UTC. Round-trip identity
        ``parse_rfc3339(to_rfc3339(dt)) == dt.astimezone(timezone.utc)``
        holds for any UTC-aware ``dt`` modulo sub-millisecond truncation.

    Raises:
        ValueError: if ``s`` is not a string, is empty / whitespace-only,
        is malformed, or is missing timezone info.
    """
    if not isinstance(s, str) or not s.strip():
        raise ValueError("parse_rfc3339: input must be a non-empty string")

    # Python 3.11+ ``fromisoformat`` accepts "Z" natively, but
    # normalizing to "+00:00" is idempotent and forward-safe.
    text = s.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            f"parse_rfc3339: invalid RFC 3339 timestamp: {s!r}"
        ) from exc

    if parsed.tzinfo is None:
        raise ValueError(
            f"parse_rfc3339: missing timezone info in {s!r}; "
            "expected 'Z' or numeric offset (e.g., '+00:00')"
        )

    return parsed.astimezone(timezone.utc)


def add_ms(dt: datetime, ms: int) -> datetime:
    """Return ``dt`` shifted by ``ms`` milliseconds (preserves tzinfo).

    Used primarily by the saga subsystem (``src/saga/coordinator.py``)
    for ``deadline_at = add_ms(utcnow(), step_timeout_ms)``. Reads more
    cleanly than ``now + timedelta(milliseconds=...)`` and reinforces
    the millisecond-unit semantics. ``tzinfo`` is preserved by
    ``datetime + timedelta`` (no silent UTC-ification, unlike
    :func:`to_rfc3339`); ``ms`` may be negative for backdated tests.

    Args:
        dt: a :class:`datetime.datetime` (aware or naive).
        ms: integer milliseconds to add (positive, zero, or negative).

    Returns:
        datetime: ``dt`` shifted by ``ms`` milliseconds, same ``tzinfo``.
    """
    return dt + timedelta(milliseconds=ms)
