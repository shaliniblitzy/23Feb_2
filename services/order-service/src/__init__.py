"""Order Service application package.

This package contains the Python source for the Order Service — the saga
coordinator (AAP R-18) for the e-commerce platform's checkout flow.

Sub-packages:
    config/         Pydantic Settings (env-driven configuration)
    container.py    Dependency-injection wiring root
    main.py         FastAPI ASGI entry point
    controllers/    HTTP route handlers (orders, health)
    domain/         Order/SagaState aggregates + enums
    saga/           Saga coordinator + scheduler + state machine (AAP R-18)
    repository/     Postgres data access (orders, saga_state, idempotency)
    events/         Kafka producers/consumers + Schema Registry serializers
    middleware/     Cross-cutting middleware (correlation-id, JWT, retry, breaker)
    observability/  Logging + metrics + tracing setup
    utils/          Time + ID helpers (deterministic in tests)
"""

from __future__ import annotations

from typing import Final

__version__: Final[str] = "1.0.0"
__all__: list[str] = ["__version__"]
