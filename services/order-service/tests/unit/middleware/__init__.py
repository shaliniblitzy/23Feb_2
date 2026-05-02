"""Cross-cutting middleware unit tests — JWT validation and correlation-ID propagation.

Covers ``src/middleware/{jwt_auth, correlation_id}.py``. JWT tests use an
in-memory RSA fake issuer (NEVER real keys, per AAP R-25) and respx
to mount the JWKS endpoint. Correlation-ID tests verify header
read/generate semantics and contextvar binding.

Modules
-------
test_jwt_middleware
    JWT signature validation, JWKS cache TTL semantics, key rotation,
    expiry/audience/issuer enforcement, scope claim plumbing, and
    structured log assertions for ``src/middleware/jwt_auth.py``.

test_correlation_id_middleware
    X-Correlation-ID header propagation (read or generate), contextvar
    binding, structlog log enrichment, downstream call seam via
    ``get_correlation_id()``, and saga context helpers for
    ``src/middleware/correlation_id.py``.

Hygiene
-------
- HERMETIC: no real network, no Testcontainers, no real secrets.
- Every test class is decorated with ``@pytest.mark.asyncio``.
- Fixtures are declared in the parent ``tests/unit/conftest.py``.
- All tests target the leaf source modules; no cross-module integration.

AAP Rules Honored
-----------------
- R-13: X-Correlation-ID propagation across HTTP and Kafka.
- R-21: Auth Service is sole JWT issuer; Order Service only validates.
- R-22: JWKS distribution; cache with bounded TTL; key rotation.
- R-25: No real secrets; in-memory RSA only.
- R-26: Structured log assertions verify correlation_id in every record.
"""

from __future__ import annotations
