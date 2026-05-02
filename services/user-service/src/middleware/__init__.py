"""HTTP middleware package for the User Service.

Cross-cutting middleware layer per AAP Section 0.4.5 and the User
Service folder spec for ``services/user-service/src/middleware/``.

Submodules
----------
- :mod:`src.middleware.correlation_id` — provides
  :class:`~src.middleware.correlation_id.CorrelationIdMiddleware`
  (pure-ASGI) and :func:`~src.middleware.correlation_id.get_correlation_id`
  for AAP R-13 correlation-ID propagation across HTTP and structlog
  contextvars.
- :mod:`src.middleware.request_logging` — provides
  :class:`~src.middleware.request_logging.RequestLoggingMiddleware`
  (pure-ASGI) for AAP R-26 structured per-request log emission and
  Prometheus HTTP request/latency metric updates.
- :mod:`src.middleware.error_handler` — provides
  :class:`~src.middleware.error_handler.ErrorHandlerMiddleware`
  (pure-ASGI INNERMOST safety net) and
  :func:`~src.middleware.error_handler.register_exception_handlers`
  for RFC 7807 ``application/problem+json`` error translation.

Runtime middleware order (outer → inner), per the User Service
folder spec, after Starlette wraps in registration-reverse order in
:func:`src.main.create_app`:

    CorrelationIdMiddleware (outermost)
        → RequestLoggingMiddleware
            → ErrorHandlerMiddleware (innermost)
                → route handler

Note that JWT validation is **NOT** a middleware in the User Service —
it is implemented as a router-level dependency
:func:`src.auth.jwt_validator.get_current_user` so that allow-listed
endpoints (``/health/live``, ``/health/ready``, ``/metrics``,
``/docs``, ``/redoc``, ``/openapi.json``) bypass it cleanly and
per-route OAuth scopes can be expressed via additional dependencies.

Import policy
-------------
This package init is intentionally empty (no submodule imports, no
side effects) to keep import cost predictable and to avoid forcing
all of FastAPI / Pydantic / structlog / Prometheus into memory for
import paths that don't exercise middleware. Consumers (e.g.,
:mod:`src.main`) MUST import directly from the specific submodule::

    from src.middleware.correlation_id import (
        CorrelationIdMiddleware,
        get_correlation_id,
    )
    from src.middleware.request_logging import RequestLoggingMiddleware
    from src.middleware.error_handler import (
        ErrorHandlerMiddleware,
        register_exception_handlers,
    )

References
----------
- AAP Section 0.4.5 — Cross-cutting interceptors
- AAP R-13 — Correlation ID propagation across HTTP & Kafka
- AAP R-19 — Liveness/readiness probes; fail-fast on missing deps
- AAP R-26 — Structured JSON logs with required fields
- AAP R-27 — Filebeat ships logs to Logstash (stdout JSON format)
- User Service folder spec — ``services/user-service/src/middleware/``
- Sibling: ``services/order-service/src/middleware/`` (BaseHTTPMiddleware
  reference pattern)
- Sibling: ``services/notification-service/src/middleware/`` (pure-ASGI
  canonical reference pattern adopted here)
"""
from __future__ import annotations

# Intentionally empty: this package provides no top-level re-exports.
# See the module docstring for the canonical import paths into the
# three submodules.
__all__: list[str] = []
