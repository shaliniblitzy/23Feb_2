"""Domain layer unit tests — pure-Python types for orders, saga state, and errors.

Covers ``src/domain/{order, order_item, order_status, saga_state, errors,
idempotency_key}.py``. All tests are pure-function / pure-data: NO I/O,
NO DB, NO Kafka.
"""

from __future__ import annotations
