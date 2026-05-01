"""Notification Service source package.

This package houses the FastAPI/ASGI application for the Notification
Service: the multi-channel (Email + SMS) dispatcher described in AAP
Section 0.1.1 (Component #8) and AAP Section 0.5.2.2 bullet 8.

Public API
----------
This module intentionally exposes ONLY the package version constant.
Consumers import specific submodules directly, for example::

    from src.main import app
    from src.container import build_container
    from src.controllers.health import router as health_router

Do NOT add submodule imports or side-effect code here; see the rationale
in the package-level architecture notes.
"""

from __future__ import annotations

__version__: str = "1.0.0"

__all__: list[str] = ["__version__"]
