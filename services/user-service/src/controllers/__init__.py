"""User Service — controllers (HTTP API) package.

Implements the HTTP API surface for the User Service. The API Gateway
routes ``/users/*`` and ``/admin/users/*`` traffic to this service;
each submodule defines a FastAPI ``APIRouter`` mounted by
``src.main:create_app`` with the appropriate prefix.

Submodule layout
----------------
* ``users_router``       — customer-facing user CRUD
                           (``/users/me``, ``/users/{id}``)
* ``preferences_router`` — user preferences
                           (``/users/me/preferences``)
* ``addresses_router``   — user address book
                           (``/users/me/addresses``,
                            ``/users/me/addresses/{addressId}``)
* ``admin_router``       — operator-only endpoints
                           (``/admin/users/search``,
                            ``/admin/users/{id}``,
                            ``/admin/users/{id}/reissue-welcome-event``)
                           gated by ``feature_flags.admin_api_enabled``
* ``health_router``      — liveness and readiness probes
                           (``/health/live``, ``/health/ready``)
                           per AAP R-19; JWT-exempt by design

Authentication
--------------
Every customer-facing endpoint requires a valid JWT introspected via
``Depends(get_current_user)`` from ``src.auth.jwt_validator``
(AAP R-21). Admin endpoints additionally require the ``users:admin``
scope via ``Depends(require_scope(SCOPE_USERS_ADMIN))`` from
``src.auth.scopes``. Health endpoints are JWT-exempt and allow-listed
by the API Gateway.

Cross-cutting concerns
----------------------
The following ASGI middleware (declared in ``src.main``) wraps every
request handled by these routers:

* ``CorrelationIdMiddleware`` (outermost) — ensures every request has
  ``X-Correlation-ID`` and propagates it into structured logs and
  outbound calls (AAP R-13).
* ``RequestLoggingMiddleware`` (middle) — records request volume,
  latency, status code, route, and updates Prometheus metrics
  (AAP R-26).
* ``ErrorHandlerMiddleware`` (innermost) — translates ``DomainError``
  subclasses (``UserNotFound``, ``OptimisticConcurrencyError``,
  ``EmailAlreadyExists``, ``MaxAddressesExceeded``, ``AuthError``,
  etc.) to RFC 7807 ``application/problem+json`` responses. Routers
  do NOT translate domain errors manually — they let exceptions
  bubble.

Submodule discovery convention
------------------------------
This package is intentionally an EMPTY package marker per the
user-service folder spec. Consumers (notably ``src.main``) import
submodules using ABSOLUTE paths::

    from src.controllers.users_router import router as users_router
    from src.controllers.preferences_router import router as preferences_router
    from src.controllers.addresses_router import router as addresses_router
    from src.controllers.admin_router import router as admin_router
    from src.controllers.health_router import router as health_router

This convention matches the rest of the User Service's package layout
(``src.auth``, ``src.services``, ``src.repository``, ``src.domain``,
``src.middleware``, ``src.observability``, ``src.config`` are all
empty package markers) and avoids any side effects at package import
time.

Authority
---------
* AAP Section 0.4.5 — Cross-cutting middleware (auth, correlation-ID,
  logging) and JWT-as-router-dependency convention.
* AAP Section 0.5.2.2 bullet 3 — User Service HTTP API.
* AAP R-13 — Correlation ID propagation.
* AAP R-19 — Liveness/readiness probes.
* AAP R-21 — JWT validation only via ``get_current_user``.
* AAP R-26 — Structured JSON logs.
* Folder spec — explicitly enumerates the five router files and
  states this ``__init__.py`` is an empty package marker.
"""
