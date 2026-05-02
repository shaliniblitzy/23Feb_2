"""User Service application package.

This package contains the Python source for the User Service — the
profile, preference, and address authority for the e-commerce platform
described in AAP Section 0.1.1 (Component #3) and AAP Section 0.5.2.2
bullet 3.

Sub-packages:
    config/         Pydantic Settings (env-driven configuration)
    container.py    Dependency-injection wiring root
    main.py         FastAPI ASGI entry point
    controllers/    HTTP route handlers (users, preferences, addresses,
                    admin, health)
    auth/           JWT validation + JWKS fetching (AAP R-21, R-22)
    domain/         User aggregate root + value objects (Profile,
                    Preferences, Address) + domain validators
    repository/     Postgres data access (users, profiles, preferences,
                    addresses, outbox)
    services/       Application services and command handlers
    events/         Kafka producer/consumer + outbox dispatcher +
                    correlation helpers
    middleware/     Cross-cutting middleware (correlation-id, request
                    logging, error handler)
    observability/  Logging + metrics + tracing setup
    health/         Liveness and readiness probes (AAP R-19)

Public API
----------
This module intentionally exposes ONLY the package version constant.
Consumers import specific submodules directly, for example::

    from src.main import app
    from src.container import build_container
    from src.controllers.health_router import router as health_router

Do NOT add submodule imports or side-effect code here; see the rationale
in the package-level architecture notes.
"""

from __future__ import annotations

from typing import Final

__version__: Final[str] = "1.0.0"

__all__: list[str] = ["__version__"]
