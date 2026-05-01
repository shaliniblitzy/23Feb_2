"""FastAPI controllers for the Recommendation Engine service.

This package groups HTTP route handlers that expose the service's
public API. Each module in the package defines a ``router`` symbol
(``fastapi.APIRouter``) that is included into the app by
``src.main.create_app()``.

Modules
-------
- ``src.controllers.recommendations``
    Exposes ``GET /recommendations?userId=&limit=&category=`` — the
    sole query endpoint for personalized product recommendations.
    Business logic is delegated to the DI-injected ``FallbackChain``.

- ``src.controllers.health``
    Exposes the Kubernetes probes required by AAP R-19:
    - ``GET /health/live``  — process-alive probe.
    - ``GET /health/ready`` — aggregate readiness probe over Postgres,
      Redis, Kafka, and the ML model loader.

Design rules (folder-spec-enforced)
-----------------------------------
1. **Controllers are thin** — ideally 5–15 lines per handler.
   Validation is performed by Pydantic v2 models; business logic
   lives in the DI-injected dependencies (``FallbackChain``,
   ``ModelLoader``, repository adapters).
2. **No raw DB / Kafka / Redis calls here** — controllers only
   touch typed facades borrowed from ``request.app.state.container``.
3. **``/metrics`` is NOT mounted here** — it is mounted at the app
   level in ``src/main.py`` via ``prometheus_client.make_asgi_app()``.
4. **JWT allow-list is NOT enforced here** — ``JWTAuthMiddleware``
   (see ``src.middleware.jwt_auth``) exempts ``/health/live``,
   ``/health/ready``, and ``/metrics`` from JWT validation. This
   package does NOT know about that allow-list.

Import convention
-----------------
Consumers MUST import the ``router`` symbols DIRECTLY from the
concrete submodule, not from this package root::

    # Correct
    from src.controllers.health import router as health_router
    from src.controllers.recommendations import router as recommendations_router

    # Incorrect (this package does not re-export)
    from src.controllers import health_router  # AttributeError

This avoids circular-import risk and preserves the ability to
lazy-load each controller only when its route tree is registered.
"""

from __future__ import annotations

# ``__all__`` is intentionally empty. This package marker exists solely so that
# ``services/recommendation-engine/src/controllers/`` is recognized as a Python
# package (enabling ``from src.controllers.health import router`` and
# ``from src.controllers.recommendations import router`` from ``src.main``).
# Re-exporting submodule symbols here would create unnecessary import-time
# coupling and risk circular imports — see the module docstring for the
# enforced import convention.
__all__: list[str] = []
