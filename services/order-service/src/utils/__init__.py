"""Foundational utility helpers for the Order Service.

This is the bottom of the dependency graph: pure standard-library code
with no imports from any other ``src.*`` package. Every other layer of
the service (controllers, saga, repositories, middleware, observability,
events) may import from here; nothing in here imports from them.

Submodules
----------
``src.utils.time``
    Deterministic time helpers (``utcnow``, ``monotonic_ms``,
    ``to_rfc3339``, ``parse_rfc3339``, ``add_ms``). The canonical clock
    for the saga state machine and the structured-log timestamps. Tests
    monkeypatch ``src.utils.time.utcnow`` to fix the clock for deadline
    assertions (AAP R-18 — saga deadlines).

``src.utils.ids``
    UUID and correlation-id generators (``new_order_id``, ``new_saga_id``,
    ``new_correlation_id``). The single audited code path for every
    identifier the service mints. Tests monkeypatch the per-function
    entry points to produce deterministic ids in assertions (AAP R-13 —
    correlation-id propagation).

Architectural rules (folder spec)
---------------------------------
* NO imports from ``src.*`` — pure stdlib only.
* NO side effects at import time (no logging, no env reads, no I/O).
* Submodules are NOT eagerly re-exported here; callers MUST import
  them explicitly::

      from src.utils.time import utcnow, monotonic_ms
      from src.utils.ids  import new_order_id, new_correlation_id
"""

from __future__ import annotations
