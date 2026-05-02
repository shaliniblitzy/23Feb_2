"""Configuration loading for the Recommendation Engine.

This package contains the strongly-typed settings classes and the
fail-fast environment-variable loader used by the service:

* ``src.config.settings``
    Pydantic-settings v2 ``BaseSettings`` subclass that validates every
    required environment variable at import time and raises if a
    required value is missing or malformed (AAP R-19 — fail fast on
    missing critical dependencies). Exposes a memoised
    ``get_settings()`` accessor (``functools.lru_cache(maxsize=1)``)
    so callers obtain a single shared, immutable instance per process.

Public API
----------
This package marker re-exports nothing. Callers MUST import the
specific symbol they need:

    from src.config.settings import get_settings, Settings

Keeping ``__all__`` empty enforces explicit imports, prevents
import-time side effects (settings construction), and avoids
circular imports — the ``Settings`` object is referenced by every
controller, repository, and middleware in the service.

Compliance notes
----------------
- AAP R-19 — Fail-fast on missing required environment variables.
- AAP R-25 — No secrets are committed to the repository; the
  ``Settings`` class declares secret values as required environment
  variables and rejects empty strings at validation time.
- AAP R-26 — The logging configuration (which uses values from
  :class:`Settings`) is initialised in :mod:`src.main.lifespan`,
  not at package import time, so importing this package never
  triggers logging side effects.
"""

from __future__ import annotations

# ``__all__`` is intentionally empty. This package marker exists solely so that
# ``services/recommendation-engine/src/config/`` is recognised as a regular
# Python package. Re-exporting symbols here would force every importer to
# evaluate the (potentially heavy) Pydantic-settings model at package import
# time, defeating the explicit ``get_settings()`` memoisation pattern.
__all__: list[str] = []
