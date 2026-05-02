"""Unit-test package for the Product Service.

Marks ``services/product-service/tests/unit/`` as a Python package so that
sibling subpackages (``tests.unit.domain``, ``tests.unit.repository``,
``tests.unit.events``, ``tests.unit.controllers``, ``tests.unit.resilience``)
are importable via absolute paths from pytest and IDE tooling.

This module is intentionally empty beyond this docstring (matching the
canonical sibling pattern at ``services/recommendation-engine/tests/unit/__init__.py``):

* No imports — preserves the strict hermeticity contract that
  ``conftest.py`` follows. Importing ``tests.unit`` must NEVER trigger
  service bootstrap, Docker calls, MongoDB/Kafka/HTTP connections, or
  Pydantic validation against environment-supplied secrets.
* No ``__all__`` — pytest discovers fixtures via ``conftest.py`` and
  tests via filename convention (``test_*.py``); no re-exports needed.
* No executable code at module scope.

Test categorization (subpackages):

* :mod:`tests.unit.domain` — Pure domain-layer tests (Product / Category
  / ProductMedia aggregate validation, slug generation via
  ``python-slugify``, version semantics, immutable-field protection).
* :mod:`tests.unit.repository` — Repository tests using
  ``mongomock-motor`` for the async MongoDB client. Verifies query-
  builder composition, filter expressions, sort/pagination semantics,
  and that repository code calls mongomock with the expected MongoDB
  filter dict.
* :mod:`tests.unit.events` — Event payload builders + topic naming.
  Verifies that ``product.created`` and ``product.updated`` event
  payloads include all required envelope fields per AAP R-33 and that
  topic names conform to AAP R-30.
* :mod:`tests.unit.controllers` — FastAPI route tests via httpx
  ``AsyncClient`` against the ``app_with_mocks`` composite fixture.
  Covers route input/output validation, scope enforcement (admin
  requires ``products:admin``), idempotency-key replay semantics, and
  version-conflict 409 mapping.
* :mod:`tests.unit.resilience` — Retry policy tests (AAP R-15:
  exponential backoff with jitter, bounded attempts) and circuit-
  breaker tests (AAP R-16: failure-rate threshold, open-state duration,
  half-open probe).

Hermeticity contract enforced for every test in this tree:

* No real MongoDB — use ``mongomock-motor`` (drop-in for
  ``motor.motor_asyncio.AsyncIOMotorClient``).
* No real Kafka — use :class:`unittest.mock.MagicMock` for
  ``confluent_kafka.Producer``.
* No real HTTPX servers — use ``respx`` to intercept HTTPX calls (e.g.,
  JWKS endpoint, Schema Registry).
* No real time — use ``freezegun`` for retry/TTL tests (NOT for
  ``pybreaker`` state transitions, which use ``time.monotonic``).
* No Docker — Testcontainer-backed tests live in
  :mod:`tests.integration` (the sibling subpackage), NOT here.

Top-level test fixtures live in :mod:`tests.conftest` (the parent root
conftest, providing 14 canonical fixtures: ``anyio_backend``,
``faker_seed``, ``faker_instance``, ``correlation_id``,
``structlog_test_capture``, ``captured_logs``, ``settings_factory``,
``_clear_structlog_context``, ``_reset_get_settings_cache``,
``assert_required_log_fields``, plus private helpers ``_deep_merge``
and ``_CapturingProcessor``); unit-tier-specific fixtures live in
:mod:`tests.unit.conftest` (mongomock-motor, respx, factory builders,
mock_jwks_client, mock_kafka_producer, app_with_mocks). Both layers are
inherited automatically by every test in this tree.
"""
