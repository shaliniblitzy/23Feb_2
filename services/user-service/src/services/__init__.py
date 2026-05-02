"""Application service layer for the User Service.

This subpackage contains command handlers that orchestrate domain operations
across repositories and event production via the transactional outbox pattern.

Modules:
    user_service: Command handlers (RegisterUserCommandHandler,
                  UpdateProfileCommandHandler, UpdatePreferencesCommandHandler,
                  AddAddressCommandHandler, UpdateAddressCommandHandler,
                  RemoveAddressCommandHandler, SoftDeleteUserCommandHandler)
                  plus the inline UserRegisteredEvent Pydantic model.
    pii_anonymizer: PIIAnonymizer -- hash-stable PII placeholder generator
                    used during user soft-delete to preserve referential
                    integrity while removing reverse-lookupable PII.

This module is intentionally a SIDE-EFFECT-FREE package marker. Importing
``src.services`` does NOT trigger any submodule imports -- consumers must
import submodules explicitly (e.g., ``from src.services.user_service import
RegisterUserCommandHandler``). This avoids import-cycles and reduces the
cold-start cost of the service.
"""

from __future__ import annotations

__all__: list[str] = []
