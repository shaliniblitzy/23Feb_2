"""ASGI middleware for the Recommendation Engine.

This package contains the cross-cutting middleware mounted on the
FastAPI application by ``src.main.create_app()``.

Modules
-------
* ``src.middleware.correlation_id``
    Pure-ASGI middleware that reads or generates the
    ``X-Correlation-ID`` request header, binds it to a contextvar so
    it appears on every log line emitted while serving the request,
    and propagates it to outbound HTTP calls and Kafka message
    headers (AAP R-13).

* ``src.middleware.structured_logging``
    Pure-ASGI middleware that emits a single structured-JSON access
    log line per HTTP request (RFC 3339 timestamp, level, service,
    correlation_id, user_id, route, method, status, latency_ms,
    message) — AAP R-26. Uses a ``finally:`` block so the access log
    is emitted EXACTLY ONCE even when the downstream handler raises.

Design rules
------------
1. **Pure ASGI** — both middlewares implement the ASGI 3 protocol
   directly rather than subclassing
   :class:`starlette.middleware.base.BaseHTTPMiddleware`. This avoids
   the BaseHTTPMiddleware double-buffering performance hit on the
   recommendation hot path.
2. **No side effects at import time** — middleware classes are
   defined but not instantiated; instantiation happens in
   ``src.main.create_app()`` so unit tests can import the modules
   without spinning up a FastAPI app.

Public API
----------
This package marker re-exports nothing. Callers MUST import the
specific middleware class they need:

    from src.middleware.correlation_id import CorrelationIdMiddleware
    from src.middleware.structured_logging import StructuredLoggingMiddleware

Keeping ``__all__`` empty preserves the explicit-import discipline
documented at the package root (see ``src/__init__.py``).

Compliance notes
----------------
- AAP R-13 — Correlation ID propagation through HTTP and Kafka.
- AAP R-26 — Structured JSON logs with the canonical AAP-mandated
  field set; the structured logging middleware is the sole emitter
  of HTTP access logs in this service.
- AAP R-27 — Logs are written to stdout in JSON form for Filebeat
  capture (the shipping side lives under ``infrastructure/elk/``,
  deferred to a later checkpoint).
"""

from __future__ import annotations

# ``__all__`` is intentionally empty. This package marker exists solely so that
# ``services/recommendation-engine/src/middleware/`` is recognised as a regular
# Python package and so that the explicit-import discipline above is preserved
# across the codebase.
__all__: list[str] = []
