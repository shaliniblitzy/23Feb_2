"""User Service domain package.

Pure domain model for the User Service: User aggregate root, value objects
(Profile, Preferences), Address entity, shared validators, and the typed
exception hierarchy.

Domain types are persistence-agnostic (no SQLAlchemy imports) and
transport-agnostic (no FastAPI imports). They are framework-free Python with
``dataclasses``, ``pydantic`` v2, ``phonenumbers``, ``pycountry``, and
``email-validator`` as their only third-party dependencies.

This package is intentionally a thin marker — it does NOT re-export submodule
members. Consumers MUST import from the relevant submodule directly:

* ``from src.domain.user import User, UserStatus``
* ``from src.domain.profile import Profile, ProfilePatch``
* ``from src.domain.preferences import Preferences, PreferencesPatch, ChannelPreference``
* ``from src.domain.address import Address, AddressType, AddressPatch``
* ``from src.domain.validators import validate_email, validate_phone, validate_locale``
* ``from src.domain.errors import DomainError, UserNotFound, OptimisticConcurrencyError``

This deliberate non-re-export pattern keeps the public surface explicit at every
import site, makes circular-import bugs impossible to introduce at the package
level, and matches AAP R-30 (events emitted from this domain are
``user.registered`` / ``user.updated`` / ``user.deleted``; the package
boundary is informational, not API-shaped).
"""
from __future__ import annotations

__all__: list[str] = []
