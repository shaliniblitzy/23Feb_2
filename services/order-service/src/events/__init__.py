"""Order Service event-driven backbone (Kafka producers, consumers, schemas).

Submodules:
    topics      Canonical topic-name constants and retry/DLQ helpers (AAP R-17, R-30).
    schemas     Pydantic v2 event payloads with the unified wire envelope
                ``event_type: Literal["<domain>.<verb>"]`` (AAP R-30) and
                ``event_version: int`` (AAP R-31).
    producer    Schema-Registry-validated Kafka producer (AAP R-14).
    consumer    Long-running Kafka consumer with retry+DLQ topology (AAP R-17).
    handlers    Per-consumed-topic handlers that drive the saga state machine.

This package init is intentionally side-effect-free. Submodules are imported
explicitly by the dependency-injection container (``src/container.py``) and
by the saga coordinator (``src/saga/coordinator.py``); importing the package
itself does NOT spin up any Kafka clients.
"""

from __future__ import annotations

__all__: list[str] = []
