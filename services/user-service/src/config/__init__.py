"""User Service configuration package.

This package centralizes the loading and validation of all runtime
configuration for the User Service via Pydantic v2 ``BaseSettings``.
It is the bridge between the declarative ``services/user-service/config/``
YAML/JSON files (and the ``services/user-service/.env.example`` env-var
catalog) and the Python code that consumes those values throughout the
service (controllers, middleware, container wiring, repositories,
event producers/consumers, observability bootstrap).

Submodules
----------
``settings``
    Defines the typed ``Settings`` pydantic-settings model (organized
    into 13 nested section classes including ``DatabaseSettings``,
    ``KafkaSettings``, ``KafkaTopicsSettings``, ``SchemaRegistrySettings``,
    ``AuthSettings``, ``UserDomainSettings``, ``OutboxSettings``,
    ``HttpClientSettings``, ``HealthSettings``, ``ObservabilitySettings``,
    ``FeatureFlagsSettings``, ``MigrationsSettings``) and exposes
    ``get_settings()`` (cached via ``functools.lru_cache(maxsize=1)``).

Consumption rules
-----------------
This package's ``__init__.py`` is intentionally **side-effect-free** and
**does not re-export submodule symbols** (per the folder specification).
Downstream code MUST import directly from the submodule::

    # CORRECT — used by src.container, src.main, and every other consumer:
    from src.config.settings import Settings, get_settings

    # INCORRECT — relative imports break tooling (pytest --import-mode=importlib,
    # mypy plugins, ruff). Always use absolute imports across the monorepo:
    from .settings import get_settings  # NO

Note: the User Service's logging entry point lives in
``src.observability.logging.configure_logging``, NOT here. Do NOT add a
``logging_config`` submodule or ``configure_logging`` re-export to this
package — the lifespan in ``src.main`` already imports ``configure_logging``
from its observability home.

AAP traceability
----------------
- AAP Section 0.1.1 Component #3 — User Service scope (profile, preference,
  and address authority).
- AAP Section 0.4.3 — Per-service config loaders construct typed Settings
  at startup and fail fast if required dependencies are missing (R-19).
- AAP Section 0.5.2.2 bullet 3 — User Service implementation directive.
- AAP R-19 — Fail-fast at startup is enforced inside ``settings.py``.
- AAP R-25 — Secrets supplied exclusively via environment variables;
  never present in source files. ``settings.py`` uses ``pydantic.SecretStr``
  for every credential-bearing field.
- AAP R-26 — Structured JSON logging configuration is consumed from
  ``Settings`` (via ``settings.observability.*``) inside
  ``src.observability.logging``.
"""

from __future__ import annotations

__all__: list[str] = []
