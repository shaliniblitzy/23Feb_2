"""Unit tests for the Notification Service.

Hermetic, fast unit tests for every module of the notification-service.
Per AAP Section 0.5.2.6 Group 6 ("Tests") and AAP Section 0.6.1 (in-scope:
``services/*/tests/unit/**/*``), this tier validates each component in
isolation using ``unittest.mock``, ``respx`` (HTTPX mocking for SendGrid /
JWKS endpoints), and ``moto[ses,sns]`` (AWS service mocking).

This tier does NOT use Testcontainers, real Kafka, or real Postgres. All
external I/O is replaced by in-process mocks.

Test files in this package:

- ``test_channel_router.py`` — AAP R-11 channel routing logic
- ``test_email_channel.py`` — Email adapter (SendGrid + SES)
- ``test_sms_channel.py`` — SMS adapter (Twilio + SNS)
- ``test_channel_registry.py`` — DI registry validation (AAP R-19)
- ``test_template_renderer.py`` — Jinja2 strict-undefined rendering
- ``test_retry_scheduler.py`` — AAP R-15 exponential backoff
- ``test_circuit_breaker.py`` — AAP R-16 per-provider breakers
- ``test_event_handlers.py`` — AAP R-30 7-event consumer handlers
- ``test_repositories.py`` — Mocked psycopg3 repositories
- ``test_jwt_middleware.py`` — AAP R-21 / R-22 JWKS validation
- ``test_correlation_id.py`` — AAP R-13 correlation propagation
- ``test_structured_logging.py`` — AAP R-26 JSON log shape

Shared fixtures live in ``conftest.py`` (this directory) and inherit from
``services/notification-service/tests/conftest.py``.
"""
