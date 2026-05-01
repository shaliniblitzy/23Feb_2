"""Notification Service HTTP controller package.

This package groups the FastAPI controllers that expose the Notification
Service's small administrative HTTP surface. The bulk of the service's
work is asynchronous Kafka consumption (see `src.events`), so HTTP is
limited to four narrowly-scoped controllers:

  * `health`       — Liveness and readiness probes (AAP R-19).
                     Allow-listed from JWT in `JWTAuthMiddleware`.
                     Mounted UNPREFIXED — paths are `/health/live`
                     and `/health/ready`.

  * `templates`    — Versioned notification template CRUD
                     (AAP Section 0.4.4 -> `templates` table).
                     Mounted at `/api/v1/templates`. Requires JWT
                     scopes `notification:templates:read` /
                     `notification:templates:write`.

  * `preferences`  — Per-user channel preferences (AAP R-11 — drives
                     the channel router's data-driven selection).
                     Mounted at `/api/v1/preferences`. Self-or-admin
                     authorization with scopes
                     `notification:preferences:read` /
                     `notification:preferences:write` /
                     `notification:preferences:admin`.

  * `log`          — Read-only delivery-log query
                     (AAP Section 0.4.4 -> `notification_log` +
                     `delivery_attempts`). Mounted at `/api/v1/log`.
                     Admin-only via scope `notification:log:read`.

Design rules that every controller in this package follows:

  1. Each module exposes `router: APIRouter` at module scope; consumers
     in `src.main` import the router directly:

         from src.controllers.health import router as health_router

     This package's `__init__` deliberately does NOT re-export
     submodule names so importing `src.controllers` stays cheap and
     side-effect-free. There is no `from .health import router` here.

  2. Controllers are thin (5-15 lines per handler); business logic
     lives in the repositories and channel router collaborators
     resolved through `request.app.state.container`.

  3. Controllers do NOT make raw Postgres / Kafka / Redis calls; they
     only call repositories obtained through the dependency-injection
     container.

  4. The `/metrics` endpoint is mounted at the FastAPI application
     level by `src.main` (so it can fall outside the `/api/v1`
     prefix), NOT inside this package.

  5. JWT validation and the allow-list (`/health/live`, `/health/ready`,
     `/metrics`) are handled by `src.middleware.jwt_auth`, NOT in
     these controller modules. Each controller, however, enforces
     its own per-route scope requirements against `request.state.user`
     populated by the middleware.

  6. Domain exceptions raised by repositories propagate up to
     `src.middleware.error_handler.ErrorHandlerMiddleware`, which
     maps them to the appropriate 4xx HTTP responses. Controllers
     do NOT wrap collaborator calls in try/except for domain errors;
     they raise `HTTPException` only for HTTP-layer concerns
     (cursor parsing, scope enforcement, range validation).

This module intentionally exports nothing.
"""

from __future__ import annotations

__all__: list[str] = []
