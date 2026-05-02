"""Repository layer unit tests — postgres helpers, OrderRepository,
SagaRepository, IdempotencyRepository.

All tests mock ``psycopg.AsyncConnection`` and ``AsyncConnectionPool``.
Real database tests live in ``tests/integration/repository/``.
"""

from __future__ import annotations
