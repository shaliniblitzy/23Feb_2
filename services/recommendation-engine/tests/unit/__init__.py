"""Hermetic unit-test suite for the Recommendation Engine service.

Every test in this package MUST be hermetic — no real databases, no real Kafka
brokers, no real network calls. All external boundaries are mocked via
:mod:`unittest.mock`, :mod:`respx`, and in-memory stubs.

See :mod:`tests.unit.conftest` for the mock factories and transport fixtures
available to every test module in this package.

Integration tests that DO spin up real containers live under
``services/recommendation-engine/tests/integration/``.
"""
