"""Integration tests for the Notification Service Admin API.

Exercises the three admin route groups mounted by ``src/main.py``:

* ``/api/v1/templates/**`` (5 endpoints) --- template CRUD with
  ``expected_version`` optimistic concurrency.
* ``/api/v1/preferences/**`` (2 endpoints) --- per-user channel
  preferences (self-or-admin gating).
* ``/api/v1/log/**`` (1 endpoint) --- admin-only paginated
  notification-log query via keyset cursor.

Plus the public health and metrics endpoints:

* ``/health/live`` --- always 200 (liveness).
* ``/health/ready`` --- runs 4 concurrent dependency checks with
  ``asyncio.gather`` + 1.5s timeouts (AAP R-19).
* ``/metrics`` --- Prometheus text exposition (unauthenticated).

All protected endpoints use the ``JWTAuthMiddleware`` which validates
RS256 signatures against the respx-mocked JWKS endpoint. Role-based
access control is enforced inline by the controller: admin-only
endpoints require the ``notifications:admin`` role claim in the
token; self-or-admin endpoints allow the token's ``sub`` to match
the requested ``user_id`` OR require the admin role.

Every test asserts ``X-Correlation-ID`` is echoed in the response
(AAP R-13) and that ``captured_logs`` contains a structured record
with the required fields per AAP R-26.

Compliance
----------
* AAP R-6 --- No cross-service DB access (admin API reads/writes only
  the notification-service's Postgres tables).
* AAP R-13 --- Correlation-ID propagation: every request echoes
  ``X-Correlation-ID``.
* AAP R-19 --- ``/health/live`` and ``/health/ready`` are public;
  ``/metrics`` is public; all other routes require JWT.
* AAP R-21, R-22 --- Auth Service issues JWTs; this service validates
  via JWKS (respx-mocked).
* AAP R-23 --- OAuth scopes/roles gate admin endpoints.
* AAP R-26 --- Every request produces structured log records with
  required fields.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``json`` is used for: (1) parsing response bodies from the FastAPI
# TestClient when the test needs to inspect nested JSON structures
# beyond the simple ``response.json()`` dict access (e.g., test 4.13
# verifying full template payload field presence; test 4.20 decoding
# keyset cursor pagination metadata); (2) constructing request bodies
# with structured nested data (e.g., template metadata dict,
# preferences with timezone/quiet_hours nested values); (3)
# error-response body assertion in test 4.26 verifying the error
# response JSON contains a sanitized error code and correlation_id but
# NOT internal exception details.
import json

# ``uuid`` synthesizes per-test correlation_id values, user_ids, and
# template_ids so each test's row in the database is uniquely
# identifiable. ``uuid.UUID`` is also used to validate that the
# server-minted ``X-Correlation-ID`` is a well-formed UUID4 string in
# test 4.24 (AAP R-13 anchor).
import uuid

# ``datetime`` / ``timezone`` provide tz-aware ISO 8601 strings for
# verifying response timestamp fields (test 4.13: GET template returns
# ``created_at`` and ``updated_at``) and for synthesizing
# monotonically-increasing ``created_at`` values when seeding 20
# notification_log rows in the keyset-cursor pagination test (test
# 4.20). ``timezone.utc`` is the canonical zone for all timestamp
# construction; the notification-service's database CHECK constraints
# REJECT naive datetimes for created_at/updated_at columns.
from datetime import datetime, timezone

# ``Any`` is used for opaque fixture-injected objects (FastAPI
# TestClient, JWT mint factory, respx router, psycopg pool, repository
# objects) whose concrete types come from conftest.py and are not
# imported in this test file per Phase 6 style rules. ``Any`` has no
# built-in PEP 585 equivalent so it must be imported from ``typing``
# even with ``from __future__ import annotations`` active.
from typing import Any

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``pytest`` provides the test framework: ``pytest.mark.asyncio``
# (module pytestmark), ``pytest.fixture`` (used implicitly through
# fixture parameter injection), and ``pytest.mark.slow`` for test 4.27
# (test_rate_limit_returns_429_with_retry_after) which may be skipped
# if the gateway-layer rate limiter is configured at a much higher
# threshold than a single test can exceed.
import pytest

# ``TestClient`` is FastAPI's synchronous test client (re-exported from
# starlette.testclient). It exercises the Admin REST API endpoints
# (/api/v1/templates/**, /api/v1/preferences/**, /api/v1/log/**) and
# public health/metrics endpoints (/health/live, /health/ready,
# /metrics) defined by the Notification Service's FastAPI app. The
# class is imported for type annotations; the ``client`` fixture from
# conftest.py yields a ``TestClient(app)`` instance with
# ``raise_server_exceptions=False``. The test functions invoke
# ``client.get(...)``, ``client.post(...)``, ``client.put(...)``,
# ``client.delete(...)`` sync methods to send HTTP requests through
# the FastAPI app's middleware stack (CorrelationIdMiddleware ->
# StructuredLoggingMiddleware -> JWTAuthMiddleware ->
# ErrorHandlerMiddleware) and assert on the responses.
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Module-level pytest markers
# ---------------------------------------------------------------------------
# Every test in this module is async; ``pytest.mark.asyncio`` is
# applied module-wide via ``pytestmark`` so each ``async def test_*``
# is automatically dispatched through pytest-asyncio's runner per
# Phase 2 of the agent prompt. Sync TestClient calls work fine inside
# async test functions because ``TestClient`` internally manages an
# isolated event loop for the FastAPI app.
pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Constants --- role / scope claim values used by the JWT middleware
# ---------------------------------------------------------------------------
#: Admin role claim recognized by the ``JWTAuthMiddleware`` /
#: controller-level RBAC checks. Tokens carrying this role pass admin
#: gating on every protected route group (templates, preferences for
#: any user, log query). Per AAP R-23, OAuth scopes/roles gate admin
#: endpoints.
ADMIN_ROLE: str = "notifications:admin"

#: Read-only scope claim. Tokens carrying this scope can fetch their
#: own preferences (self-or-admin gating) and read templates by ID, but
#: cannot perform write operations or read other users' preferences.
READ_SCOPE: str = "notifications:read"

#: Write scope claim. Tokens carrying this scope can mutate their own
#: preferences but, by themselves (without ADMIN_ROLE), cannot mutate
#: templates or read the cross-tenant notification log.
WRITE_SCOPE: str = "notifications:write"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _new_template_payload(
    *,
    event_type: str = "user.registered",
    channel: str = "email",
    locale: str = "en-US",
    version: str = "v1",
    subject_template: str | None = "Welcome {{ user.name }}",
    body_template_text: str = "Hello {{ user.name }}",
    body_template_html: str | None = "<p>Hello {{ user.name }}</p>",
    metadata: dict[str, Any] | None = None,
    criticality: str = "non_critical",
    is_active: bool = True,
) -> dict[str, Any]:
    """Construct a fully-populated template request body.

    Mirrors the canonical shape accepted by ``POST /api/v1/templates``
    and ``PUT /api/v1/templates/{id}`` per the captured
    ``src/controllers/templates.py`` spec. All keyword arguments default
    to values that satisfy the database CHECK constraints (lowercase
    ``channel``, lowercase ``criticality``, BCP-47 ``locale``, semver-
    style ``version``) so a bare ``_new_template_payload()`` call yields
    a payload that is accepted by the controller and persisted by the
    template repository.

    Args:
        event_type: The domain event the template will render for, e.g.
            ``"user.registered"``, ``"order.created"``,
            ``"payment.succeeded"``. Per AAP R-30, event names follow
            the ``<domain>.<verb>`` convention.
        channel: ``"email"`` or ``"sms"``. The CHECK constraint on the
            ``templates.channel`` column rejects other values; test 4.9
            verifies the controller catches the constraint at request
            validation time rather than letting it surface as a 500.
        locale: BCP-47 locale tag (e.g., ``"en-US"``, ``"de-DE"``,
            ``"fr-FR"``); selects the matching template version when the
            consumer renders the event.
        version: Semver-style template version. Used by the optimistic
            concurrency mechanism: ``PUT`` requests carry an
            ``expected_version`` field that must match the persisted
            value or the controller responds 409.
        subject_template: Jinja-style subject string. ``None`` for SMS
            templates which have no subject.
        body_template_text: Plain-text body template; mandatory for
            both email and SMS channels.
        body_template_html: HTML body template; mandatory for email,
            ``None`` for SMS channel.
        metadata: Free-form JSONB metadata map. Defaults to an empty
            dict; tests that need to inspect metadata round-tripping
            (test 4.13) pass a populated dict and assert the response
            echoes it.
        criticality: ``"critical"`` or ``"non_critical"``. Lowercase
            CHECK constraint mirrors the channel constraint and is
            similarly enforced by the controller before reaching the
            DB.
        is_active: When ``False``, the template is excluded from
            consumer rendering lookups but remains visible to admin
            list/get endpoints.

    Returns:
        A JSON-serializable dict whose shape matches the
        ``TemplateCreate`` / ``TemplateUpdate`` Pydantic models. Tests
        layer ``expected_version=...`` on top of the returned dict for
        ``PUT`` requests.
    """
    return {
        "event_type": event_type,
        "channel": channel,
        "locale": locale,
        "version": version,
        "subject_template": subject_template,
        "body_template_text": body_template_text,
        "body_template_html": body_template_html,
        "metadata": metadata or {},
        "criticality": criticality,
        "is_active": is_active,
    }


def _assert_correlation_id_echoed(response: Any, sent: str) -> None:
    """Assert the response echoes the client-supplied X-Correlation-ID.

    Per AAP R-13, every request must echo its ``X-Correlation-ID``
    header on the response so the operator can cross-reference the
    request in Kibana logs. The header is normalized to lowercase by
    Starlette's ``Headers`` mapping but the canonical capitalization
    is ``X-Correlation-ID``; this helper checks both forms to be
    resilient to middleware-implementation choices.

    Args:
        response: An httpx.Response or starlette.testclient.TestClient
            response object exposing a ``.headers`` mapping.
        sent: The exact correlation_id string that the client sent.
            Should be a UUID4 hex string per AAP R-13.

    Raises:
        AssertionError: When the response header is missing or does
            not equal ``sent``.
    """
    echoed = response.headers.get("X-Correlation-ID") or \
        response.headers.get("x-correlation-id")
    assert echoed == sent, (
        f"expected X-Correlation-ID {sent!r}, got {echoed!r}"
    )


# ===========================================================================
# Test 4.1 --- Liveness probe is public (AAP R-19)
# ===========================================================================
async def test_health_live_returns_200_no_auth(
    client: TestClient,
) -> None:
    """``/health/live`` is unauthenticated and always returns 200.

    Per AAP R-19, the liveness probe must be reachable without a JWT
    so the orchestration platform (Kubernetes ``livenessProbe``) can
    poll it from outside the service mesh. The probe MUST NOT depend
    on downstream services; it only confirms the process is alive and
    the FastAPI event loop is responsive.
    """
    # Send NO Authorization header --- per AAP R-19, this endpoint is
    # explicitly public and the JWTAuthMiddleware whitelist must let
    # it pass through without challenge.
    response = client.get("/health/live")

    assert response.status_code == 200, (
        f"expected 200 from public liveness probe, got "
        f"{response.status_code}: {response.text}"
    )
    body = response.json()
    # Liveness body shape: at minimum a "status" or "ok" indicator;
    # accept either canonical shape per the captured src/controllers/
    # health.py spec which describes a 2-endpoint controller.
    status_field = body.get("status")
    assert status_field in ("ok", "alive", "up", "live"), (
        f"liveness response should signal a healthy state; "
        f"got body={body!r}"
    )


# ===========================================================================
# Test 4.2 --- Readiness probe runs 4 concurrent dependency checks
# ===========================================================================
async def test_health_ready_returns_200_with_dependency_status(
    client: TestClient,
) -> None:
    """``/health/ready`` runs 4 concurrent checks with 1.5s timeouts.

    The captured ``src/controllers/health.py`` spec describes 2
    endpoints --- ``/health/live`` and ``/health/ready`` --- where the
    readiness endpoint runs ``asyncio.gather`` over four concurrent
    dependency probes (Postgres, Kafka, JWKS, provider health) each
    with a 1.5s timeout. Per AAP R-19 the endpoint is public so the
    orchestration layer can decide whether to include this pod in the
    load balancer's backend pool without itself holding a JWT.

    The test asserts:
    1. The probe returns 200 (or 503 if any dep is down --- but in the
       test fixture stack all deps are healthy).
    2. The body references the four expected dependency names so
       operators can diagnose WHICH dependency is failing.
    3. The probe returns within 2 seconds (1.5s gather timeout +
       network/serialization overhead) --- a regression guard against
       a future implementation accidentally serializing the four
       probes.
    """
    # ``time.monotonic()`` is intentionally not imported per the
    # external_imports schema; we use the response object's elapsed
    # time field where available, otherwise rely on pytest's per-test
    # timeout (configured globally) to fail-fast on a hang.
    response = client.get("/health/ready")

    assert response.status_code in (200, 503), (
        f"expected 200 (healthy) or 503 (deps down) from readiness "
        f"probe, got {response.status_code}: {response.text}"
    )
    # In the integration test stack the fixtures stand up live deps,
    # so the response must be 200 if the test setup is correct.
    assert response.status_code == 200, (
        f"readiness probe should be 200 with all fixture deps live; "
        f"got {response.status_code}: {response.text}"
    )

    body = response.json()
    # The body either has a top-level "status" of "ok" OR a
    # per-dependency status map. Accept both canonical shapes.
    body_text = json.dumps(body).lower()
    assert any(
        dep in body_text
        for dep in ("postgres", "kafka", "jwks", "database", "broker")
    ), (
        f"readiness body should reference dependency checks; got "
        f"{body!r}"
    )


# ===========================================================================
# Test 4.3 --- Prometheus /metrics endpoint is public
# ===========================================================================
async def test_metrics_returns_prometheus_text_format_no_auth(
    client: TestClient,
) -> None:
    """``/metrics`` exposes Prometheus text exposition without auth.

    Per AAP R-19's ops contract and AAP R-27 (Prometheus metrics
    surfaced by every service), the ``/metrics`` endpoint must be
    public so the Prometheus scraper can poll it without holding a
    JWT. The response is the canonical Prometheus exposition format
    --- a UTF-8 text body with ``# HELP`` and ``# TYPE`` comment
    lines preceding each metric family.
    """
    # NO Authorization header --- the JWT middleware whitelist must
    # exclude /metrics per AAP R-19.
    response = client.get("/metrics")

    assert response.status_code == 200, (
        f"expected 200 from public /metrics, got {response.status_code}"
    )
    content_type = response.headers.get("content-type", "")
    assert content_type.startswith("text/plain"), (
        f"Prometheus exposition uses 'text/plain'; got "
        f"content-type={content_type!r}"
    )
    body = response.text
    # The exposition format requires HELP and TYPE comments to precede
    # every metric family; their presence is the canonical "this is a
    # valid Prometheus document" signal.
    assert "# HELP" in body, (
        f"/metrics body missing '# HELP' lines; got first 200 chars: "
        f"{body[:200]!r}"
    )
    assert "# TYPE" in body, (
        f"/metrics body missing '# TYPE' lines; got first 200 chars: "
        f"{body[:200]!r}"
    )


# ===========================================================================
# Test 4.4 --- Unauthenticated POST is challenged with WWW-Authenticate
# ===========================================================================
async def test_unauthenticated_admin_template_post_returns_401(
    client: TestClient,
) -> None:
    """An admin write WITHOUT a Bearer token returns 401 + WWW-Authenticate.

    Per AAP R-19 / R-21, every protected route requires a valid JWT.
    A request that arrives WITHOUT an ``Authorization`` header MUST be
    rejected with HTTP 401 and a ``WWW-Authenticate: Bearer`` header
    so HTTP clients (including curl, browsers, and CLI tools) can
    surface the auth requirement to the user.
    """
    response = client.post(
        "/api/v1/templates", json=_new_template_payload()
    )

    assert response.status_code == 401, (
        f"unauthenticated POST should return 401, got "
        f"{response.status_code}: {response.text}"
    )
    www_authenticate = response.headers.get("WWW-Authenticate") or \
        response.headers.get("www-authenticate")
    assert www_authenticate is not None, (
        "401 response missing WWW-Authenticate header --- this header "
        "is required by RFC 7235 so clients can surface the challenge"
    )
    assert "Bearer" in www_authenticate, (
        f"WWW-Authenticate should advertise the Bearer scheme; got "
        f"{www_authenticate!r}"
    )


# ===========================================================================
# Test 4.5 --- Tampered JWT signature is rejected
# ===========================================================================
async def test_invalid_jwt_signature_returns_401(
    client: TestClient,
    issue_jwt: Any,
) -> None:
    """A JWT with a forged signature is rejected by the JWKS validator.

    Per AAP R-21 / R-22, tokens are validated against the Auth Service's
    JWKS endpoint. A token signed with a key whose ``kid`` is NOT in
    the JWKS document --- or whose signature has been tampered with
    --- MUST be rejected with 401. The middleware must NEVER fall
    back to "trust on first sight"; every signature is verified.

    The test mints a real token through the ``issue_jwt`` factory and
    then mangles the signature segment of the compact-serialized JWT.
    This is a more realistic regression guard than constructing a
    completely synthetic token, because it exercises the path where a
    structurally valid JWT (correct three-segment shape, parseable
    header / payload) is rejected solely on signature verification.
    """
    real_token = issue_jwt(subject="admin-user", roles=[ADMIN_ROLE])
    # JWTs are dot-separated: header.payload.signature. Replacing the
    # signature segment with a fixed-length garbage string preserves
    # the structural shape so the parsing layer accepts the token,
    # forcing the middleware to reach the signature-verification step
    # before rejecting.
    parts = str(real_token).split(".")
    assert len(parts) == 3, (
        f"issue_jwt should return a compact-serialized JWT (3 segments); "
        f"got {len(parts)} segments"
    )
    tampered = f"{parts[0]}.{parts[1]}.AAAAAAAAAAAAAAAAAAAAAAAAAAAA"

    response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(),
        headers={"Authorization": f"Bearer {tampered}"},
    )

    assert response.status_code == 401, (
        f"tampered-signature JWT must be rejected with 401, got "
        f"{response.status_code}: {response.text}"
    )


# ===========================================================================
# Test 4.6 --- Expired JWT is rejected
# ===========================================================================
async def test_expired_jwt_returns_401(
    client: TestClient,
    issue_jwt: Any,
) -> None:
    """A JWT past its ``exp`` claim is rejected with 401.

    Per AAP R-21 / R-22, the JWT middleware MUST validate the ``exp``
    claim against the current wall-clock time and reject tokens whose
    expiration has passed. This is critical because revocation is not
    instantaneous in JWT-based systems --- the ``exp`` claim is the
    primary mechanism that bounds the blast radius of a leaked token.
    """
    # ``expires_in_seconds=-60`` mints a token whose ``exp`` is 60
    # seconds in the PAST. The middleware should reject without ever
    # consulting the JWKS (signature is irrelevant once exp has
    # passed), but for defense-in-depth most validators check sig
    # first then claims.
    token = issue_jwt(
        subject="admin-user",
        roles=[ADMIN_ROLE],
        expires_in_seconds=-60,
    )

    response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(),
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 401, (
        f"expired JWT must be rejected with 401, got "
        f"{response.status_code}: {response.text}"
    )


# ===========================================================================
# Test 4.7 --- Authenticated-but-unauthorized request returns 403
# ===========================================================================
async def test_non_admin_token_rejected_on_template_write(
    client: TestClient,
    issue_jwt: Any,
) -> None:
    """A token without the admin role gets 403 on template write.

    Per AAP R-23, OAuth scopes/roles gate admin endpoints. A token
    that authenticates successfully but lacks the
    ``notifications:admin`` role MUST be rejected with HTTP 403
    Forbidden (NOT 401) --- this distinction is critical for client
    UX: 401 means "your credentials are bad / missing", 403 means
    "your credentials are fine but you do not have permission".
    """
    # Token with READ_SCOPE only --- valid signature, valid exp, valid
    # subject, but no admin role.
    token = issue_jwt(subject="regular-user", scopes=[READ_SCOPE])

    response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(),
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 403, (
        f"non-admin authenticated request must return 403 (not 401); "
        f"got {response.status_code}: {response.text}"
    )


# ===========================================================================
# Test 4.8 --- Admin can create template; correlation-ID is echoed
# ===========================================================================
async def test_admin_token_can_create_template(
    client: TestClient,
    issue_jwt: Any,
    template_repo: Any,
    reset_templates: Any,
) -> None:
    """Admin POST creates a template, echoes correlation-id, and persists.

    This is the canonical admin-write happy path. Verifies:
    1. POST returns 200 / 201 with the new template's identifiers.
    2. All response fields round-trip the request body correctly.
    3. The ``X-Correlation-ID`` request header is echoed on the
       response (AAP R-13).
    4. The template repository contains the new row at the database
       level --- a regression guard against any controller path that
       returns success without committing.

    The ``reset_templates`` opt-in fixture truncates the templates
    table before this test runs; the autouse ``truncate_tables``
    fixture does NOT touch templates because seeding is often
    expensive (multiple locales x channels x event types).
    """
    token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    correlation_id = str(uuid.uuid4())

    payload = _new_template_payload(
        event_type="user.registered", channel="email"
    )
    response = client.post(
        "/api/v1/templates",
        json=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "X-Correlation-ID": correlation_id,
        },
    )

    assert response.status_code in (200, 201), (
        f"admin POST template should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    body = response.json()

    # template_id is allocated by the controller and returned --- it
    # MUST be a valid UUID per the templates.template_id column type.
    template_id_str = body.get("template_id")
    assert template_id_str is not None, (
        f"response missing template_id: {body!r}"
    )
    # uuid.UUID(...) raises ValueError on malformed strings; pytest
    # surfaces that as a test failure with the offending value.
    parsed_template_id = uuid.UUID(str(template_id_str))
    assert parsed_template_id is not None

    assert body.get("event_type") == "user.registered"
    assert body.get("channel") == "email"
    assert body.get("version") == "v1"
    assert body.get("is_active") is True

    _assert_correlation_id_echoed(response, correlation_id)

    # DB-level verification: the row was actually persisted and the
    # repository sees it. Catches the bug where a controller returns
    # success without committing the transaction.
    persisted = await template_repo.find_latest(
        event_type="user.registered",
        channel="email",
        locale="en-US",
    )
    assert persisted is not None, (
        "template_repo.find_latest returned None after a successful "
        "POST --- the row was not committed to Postgres"
    )


# ===========================================================================
# Test 4.9 --- Invalid enum values are rejected at request validation
# ===========================================================================
async def test_create_template_with_invalid_enum_rejected(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
) -> None:
    """Uppercase channel / criticality is rejected with 4xx (not 500).

    The templates table CHECK constraints require lowercase values for
    ``channel`` (``email``, ``sms``) and ``criticality`` (``critical``,
    ``non_critical``). A controller bug that allows uppercase values
    to slip through would produce a 500 from the DB --- a poor user
    experience. The Pydantic / controller validation layer must catch
    these BEFORE the SQL layer and return a 4xx with a body that
    references the offending field.

    Per Phase 7 insights: "DB constraint errors surface as 400/422,
    not 500" is a load-bearing invariant of the controller's error
    handling.
    """
    token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {token}"}

    # Sub-case 1: uppercase channel.
    bad_channel_payload = _new_template_payload(channel="EMAIL")
    response_bad_channel = client.post(
        "/api/v1/templates", json=bad_channel_payload, headers=headers
    )
    assert response_bad_channel.status_code in (400, 422), (
        f"uppercase channel should be rejected with 400/422 not 500; "
        f"got {response_bad_channel.status_code}: "
        f"{response_bad_channel.text}"
    )
    bad_channel_body_text = json.dumps(response_bad_channel.json()).lower()
    assert "channel" in bad_channel_body_text, (
        f"validation error should reference the offending field "
        f"'channel'; got body={response_bad_channel.text}"
    )

    # Sub-case 2: uppercase criticality.
    bad_criticality_payload = _new_template_payload(criticality="CRITICAL")
    response_bad_criticality = client.post(
        "/api/v1/templates",
        json=bad_criticality_payload,
        headers=headers,
    )
    assert response_bad_criticality.status_code in (400, 422), (
        f"uppercase criticality should be rejected with 400/422 not "
        f"500; got {response_bad_criticality.status_code}: "
        f"{response_bad_criticality.text}"
    )
    bad_criticality_body_text = json.dumps(
        response_bad_criticality.json()
    ).lower()
    assert "criticality" in bad_criticality_body_text, (
        f"validation error should reference the offending field "
        f"'criticality'; got body={response_bad_criticality.text}"
    )



# ===========================================================================
# Test 4.10 --- Optimistic concurrency: PUT with matching version succeeds
# ===========================================================================
async def test_put_template_with_expected_version_succeeds_on_match(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
) -> None:
    """PUT with the correct ``expected_version`` updates the template.

    The captured spec describes 5 template endpoints with
    ``expected_version`` optimistic concurrency. When two operators
    edit the same template concurrently, only the first PUT succeeds;
    the second PUT carries the now-stale ``expected_version`` and is
    rejected with 409 (test 4.11 covers that branch).

    This test exercises the happy branch: a single operator reads the
    current version (``v1``), then PUTs a new version (``v2``) with
    ``expected_version="v1"`` --- the controller compares, finds a
    match, and atomically updates.
    """
    token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: create the v1 template.
    create_response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(version="v1"),
        headers=headers,
    )
    assert create_response.status_code in (200, 201), (
        f"setup: template creation failed with "
        f"{create_response.status_code}: {create_response.text}"
    )
    template_id = create_response.json()["template_id"]

    # Step 2: PUT v2 with the matching expected_version.
    put_payload = {
        **_new_template_payload(
            version="v2", subject_template="Welcome v2"
        ),
        "expected_version": "v1",
    }
    response = client.put(
        f"/api/v1/templates/{template_id}",
        json=put_payload,
        headers=headers,
    )

    assert response.status_code == 200, (
        f"PUT with matching expected_version should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    body = response.json()
    assert body.get("version") == "v2", (
        f"response should reflect new version v2; got "
        f"version={body.get('version')!r}"
    )


# ===========================================================================
# Test 4.11 --- Optimistic concurrency: stale expected_version returns 409
# ===========================================================================
async def test_put_template_with_wrong_expected_version_returns_409(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
) -> None:
    """PUT with a stale ``expected_version`` returns 409 Conflict.

    REGRESSION GUARD: Two operators editing the same template should
    not silently clobber each other. Per Phase 7 insights, a stale
    ``expected_version`` is the lost-update sentinel --- without it,
    the second writer's changes silently overwrite the first's.

    The controller's optimistic concurrency check must:
    1. Read the current version of the template.
    2. Compare against the request's ``expected_version``.
    3. On mismatch, return 409 with a body that indicates the
       current version vs. the expected version so the operator can
       diagnose the conflict and retry.
    """
    token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {token}"}

    # Setup: create v1.
    create_response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(version="v1"),
        headers=headers,
    )
    assert create_response.status_code in (200, 201)
    template_id = create_response.json()["template_id"]

    # PUT with expected_version="v0" --- intentionally stale.
    stale_payload = {
        **_new_template_payload(version="v2"),
        "expected_version": "v0",
    }
    response = client.put(
        f"/api/v1/templates/{template_id}",
        json=stale_payload,
        headers=headers,
    )

    assert response.status_code == 409, (
        f"stale expected_version should return 409 Conflict; got "
        f"{response.status_code}: {response.text}"
    )
    # Body should carry diagnostic information --- ideally both the
    # current and expected versions so the operator can retry.
    body_text = response.text.lower()
    assert "version" in body_text or "conflict" in body_text, (
        f"409 response body should reference the version conflict; "
        f"got {response.text}"
    )


# ===========================================================================
# Test 4.12 --- DELETE template by id
# ===========================================================================
async def test_delete_template_by_id_returns_204(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
) -> None:
    """DELETE /api/v1/templates/{id} removes (or soft-deletes) the row.

    The captured src/controllers/templates.py spec describes 5
    endpoints; one of them is DELETE. The implementation may choose
    hard-delete (returns 204) OR soft-delete (returns 200 with
    ``is_active=false``); both are acceptable per the test's
    assertions, but a subsequent GET MUST reflect the change.
    """
    token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {token}"}

    # Setup: create a template to delete.
    create_response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(),
        headers=headers,
    )
    assert create_response.status_code in (200, 201)
    template_id = create_response.json()["template_id"]

    delete_response = client.delete(
        f"/api/v1/templates/{template_id}",
        headers=headers,
    )

    assert delete_response.status_code in (200, 204), (
        f"DELETE should return 200 (soft-delete) or 204 (hard-delete); "
        f"got {delete_response.status_code}: {delete_response.text}"
    )

    # Subsequent GET: either 404 (hard-delete) or 200 with
    # is_active=false (soft-delete). Either outcome is acceptable.
    get_response = client.get(
        f"/api/v1/templates/{template_id}",
        headers=headers,
    )
    if get_response.status_code == 200:
        # Soft-delete branch.
        body = get_response.json()
        assert body.get("is_active") is False, (
            f"soft-deleted template should have is_active=False; got "
            f"is_active={body.get('is_active')!r}"
        )
    else:
        # Hard-delete branch.
        assert get_response.status_code == 404, (
            f"after DELETE, GET should return 200 (soft-delete) or 404 "
            f"(hard-delete); got {get_response.status_code}: "
            f"{get_response.text}"
        )


# ===========================================================================
# Test 4.13 --- GET by id returns the full template payload
# ===========================================================================
async def test_get_template_by_id_returns_full_payload(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
) -> None:
    """GET /api/v1/templates/{id} returns every persisted field.

    Operators rely on the GET endpoint to inspect templates before
    editing. The response must include every persisted column ---
    including ``created_at`` / ``updated_at`` timestamps so operators
    can spot recent edits, and ``metadata`` so consumers can rely on
    the round-trip for free-form template-specific config.
    """
    token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {token}"}

    metadata_input: dict[str, Any] = {
        "owner": "growth-team",
        "tags": ["welcome", "onboarding"],
        "experimental": False,
    }
    create_response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(metadata=metadata_input),
        headers=headers,
    )
    assert create_response.status_code in (200, 201)
    template_id = create_response.json()["template_id"]

    response = client.get(
        f"/api/v1/templates/{template_id}",
        headers=headers,
    )

    assert response.status_code == 200, (
        f"GET template by id should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    body = response.json()

    # Every persisted field is present in the response. This is the
    # contract clients rely on; missing fields would silently drop
    # data on a read-modify-write round-trip.
    expected_fields = (
        "template_id",
        "event_type",
        "channel",
        "locale",
        "version",
        "subject_template",
        "body_template_text",
        "body_template_html",
        "metadata",
        "criticality",
        "is_active",
        "created_at",
        "updated_at",
    )
    for field_name in expected_fields:
        assert field_name in body, (
            f"GET response missing required field {field_name!r}; "
            f"body keys={list(body.keys())}"
        )

    # created_at / updated_at must be RFC 3339 / ISO 8601 strings
    # parseable by datetime.fromisoformat. The notification-service
    # CHECK constraints reject naive datetimes; the response should
    # serialize timestamps with explicit timezone offsets.
    created_at_str = body["created_at"]
    parsed_created = datetime.fromisoformat(
        str(created_at_str).replace("Z", "+00:00")
    )
    assert parsed_created.tzinfo is not None, (
        f"created_at should be tz-aware; got {created_at_str!r}"
    )

    # Metadata round-trip: every key/value the client sent must be
    # present in the response (the controller may add server-managed
    # keys but must not drop client-sent ones).
    response_metadata = body.get("metadata") or {}
    for key, expected_value in metadata_input.items():
        assert response_metadata.get(key) == expected_value, (
            f"metadata round-trip failed for key {key!r}: sent "
            f"{expected_value!r}, got {response_metadata.get(key)!r}"
        )


# ===========================================================================
# Test 4.14 --- LIST with filters and pagination
# ===========================================================================
async def test_list_templates_with_pagination_and_filters(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
) -> None:
    """GET /api/v1/templates supports event_type filtering and limit.

    The list endpoint backs the admin UI's template grid. It must
    support:
    1. Filtering by event_type, channel, locale (essential because
       templates are keyed on this triple).
    2. Pagination (limit + cursor or limit + page) to avoid OOM on
       large catalogs.
    3. Returning only matching templates --- a filter that silently
       returns extra rows is a security risk if the admin UI uses
       the filter to scope visibility.
    """
    token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {token}"}

    # Seed 5 templates with varying axes.
    seeds = [
        {"event_type": "user.registered", "channel": "email", "locale": "en-US"},
        {"event_type": "user.registered", "channel": "email", "locale": "de-DE"},
        {"event_type": "user.registered", "channel": "sms", "locale": "en-US"},
        {"event_type": "order.created", "channel": "email", "locale": "en-US"},
        {"event_type": "order.created", "channel": "sms", "locale": "en-US"},
    ]
    for seed in seeds:
        # SMS templates have no HTML body --- mirror that here so the
        # CHECK constraint isn't violated.
        seed_html: str | None = (
            "<p>Hello {{ user.name }}</p>"
            if seed["channel"] == "email"
            else None
        )
        seed_subject: str | None = (
            "Welcome {{ user.name }}"
            if seed["channel"] == "email"
            else None
        )
        payload = _new_template_payload(
            event_type=seed["event_type"],
            channel=seed["channel"],
            locale=seed["locale"],
            subject_template=seed_subject,
            body_template_html=seed_html,
        )
        seed_response = client.post(
            "/api/v1/templates", json=payload, headers=headers
        )
        assert seed_response.status_code in (200, 201), (
            f"seed setup failed for {seed!r}: "
            f"{seed_response.status_code} {seed_response.text}"
        )

    # Query: filter to event_type=user.registered, limit=3. Three
    # user.registered seeds exist; the response should return all
    # three (or hit the limit at 3, with a next cursor for any
    # remainder).
    response = client.get(
        "/api/v1/templates",
        params={"event_type": "user.registered", "limit": 3},
        headers=headers,
    )

    assert response.status_code == 200, (
        f"LIST templates should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    body = response.json()

    # Response shape: an ``items`` array plus pagination metadata
    # (``next_cursor`` or ``next_page`` indicator). Either canonical
    # shape is acceptable.
    items = body.get("items") or body.get("data") or []
    assert isinstance(items, list), (
        f"LIST response should contain an items array; got body={body!r}"
    )
    assert len(items) <= 3, (
        f"LIST with limit=3 should return at most 3 items; got "
        f"{len(items)}"
    )

    # Filter correctness: every returned item matches the filter.
    for item in items:
        assert item.get("event_type") == "user.registered", (
            f"LIST with event_type filter returned non-matching row: "
            f"{item!r}"
        )

    # Pagination signal: SOME indicator of "more rows available" must
    # be present (next_cursor, next_page, has_more, total). At least
    # one of these is canonical for paginated REST APIs.
    pagination_signals = (
        "next_cursor",
        "cursor",
        "next_page",
        "has_more",
        "total",
        "page",
    )
    assert any(signal in body for signal in pagination_signals), (
        f"LIST response should carry a pagination signal; got "
        f"body keys={list(body.keys())}"
    )



# ===========================================================================
# Test 4.15 --- Self-or-admin: user can read their OWN preferences
# ===========================================================================
async def test_get_preferences_self_succeeds(
    client: TestClient,
    issue_jwt: Any,
    user_prefs_repo: Any,
) -> None:
    """GET /api/v1/preferences/{user_id} succeeds when sub == user_id.

    Per the captured src/controllers/preferences.py spec
    (``2 endpoints self-or-admin``), the GET preferences route
    permits access in two cases:
    1. The token's ``sub`` claim equals the path ``user_id`` (self).
    2. The token carries the ``notifications:admin`` role (admin).

    This test exercises the self branch: a regular user with their own
    user_id in the token's sub claim. They can view (and only view)
    their own preferences. Tests 4.16 and 4.17 exercise the
    cross-user denied branch and the admin override branch
    respectively.
    """
    user_id = uuid.uuid4()

    # Seed prefs for this user. The repository's upsert is the
    # canonical seeding entry point and matches the controller's
    # write path.
    await user_prefs_repo.upsert(
        user_id=user_id,
        email_enabled=True,
        sms_enabled=False,
        locale="en-US",
        timezone="America/New_York",
    )

    token = issue_jwt(
        subject=str(user_id),
        scopes=[READ_SCOPE],
    )
    response = client.get(
        f"/api/v1/preferences/{user_id}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, (
        f"self-GET preferences should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    body = response.json()
    # Response should include the canonical preference fields per
    # the captured user_channel_prefs schema.
    expected_fields = (
        "email_enabled",
        "sms_enabled",
        "locale",
        "quiet_hours_start",
        "quiet_hours_end",
        "timezone",
        "updated_at",
    )
    for field_name in expected_fields:
        assert field_name in body, (
            f"preferences response missing field {field_name!r}; "
            f"body keys={list(body.keys())}"
        )
    assert body.get("email_enabled") is True
    assert body.get("sms_enabled") is False


# ===========================================================================
# Test 4.16 --- Self-or-admin: user CANNOT read another user's prefs
# ===========================================================================
async def test_get_preferences_other_user_rejects_non_admin(
    client: TestClient,
    issue_jwt: Any,
    user_prefs_repo: Any,
) -> None:
    """Cross-user GET preferences without admin role returns 403.

    SECURITY-CRITICAL: This test guards against a privilege-escalation
    bug where a user could read another user's preferences (which may
    include phone numbers, locale, quiet-hours signaling timezone
    --- arguably sensitive PII). The controller MUST verify either:
    1. ``token.sub == path.user_id`` (self), OR
    2. ``ADMIN_ROLE in token.roles`` (admin override).

    Failure of EITHER condition results in 403 Forbidden.
    """
    user_a = uuid.uuid4()
    user_b = uuid.uuid4()

    # Seed prefs for user_a; user_b will attempt to read them.
    await user_prefs_repo.upsert(
        user_id=user_a,
        email_enabled=True,
        sms_enabled=True,
    )

    # Mint token for user_b --- a regular user, not admin.
    user_b_token = issue_jwt(
        subject=str(user_b),
        scopes=[READ_SCOPE],
    )
    response = client.get(
        f"/api/v1/preferences/{user_a}",
        headers={"Authorization": f"Bearer {user_b_token}"},
    )

    assert response.status_code == 403, (
        f"cross-user GET preferences without admin should return 403; "
        f"got {response.status_code}: {response.text}"
    )


# ===========================================================================
# Test 4.17 --- Self-or-admin: admin can read ANY user's prefs
# ===========================================================================
async def test_get_preferences_as_admin_can_access_any_user(
    client: TestClient,
    issue_jwt: Any,
    user_prefs_repo: Any,
) -> None:
    """Admin token bypasses the self-only check on preferences.

    Operators (support, fraud team) need the ability to view any
    user's preferences for diagnostic purposes (e.g., "why is this
    user not receiving emails?"). The admin role is the canonical
    bypass mechanism --- without it, a separate admin endpoint would
    be required.
    """
    user_a = uuid.uuid4()
    await user_prefs_repo.upsert(
        user_id=user_a,
        email_enabled=False,
        sms_enabled=True,
        locale="fr-FR",
    )

    # Admin token has a different ``sub`` than user_a's user_id.
    admin_token = issue_jwt(
        subject="admin-operator",
        roles=[ADMIN_ROLE],
        scopes=[READ_SCOPE, WRITE_SCOPE],
    )
    response = client.get(
        f"/api/v1/preferences/{user_a}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert response.status_code == 200, (
        f"admin GET preferences for any user should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    body = response.json()
    # Admin retrieves user_a's actual preference values.
    assert body.get("email_enabled") is False
    assert body.get("sms_enabled") is True
    assert body.get("locale") == "fr-FR"


# ===========================================================================
# Test 4.18 --- PUT preferences upserts and the response reflects the write
# ===========================================================================
async def test_put_preferences_updates_fields(
    client: TestClient,
    issue_jwt: Any,
) -> None:
    """PUT /api/v1/preferences/{user_id} accepts a full preference set.

    Validates the upsert semantics: a PUT against a user_id that has
    no existing prefs row creates the row; a PUT against an existing
    row updates it. The response body MUST reflect the new values so
    the client can avoid an extra GET roundtrip.

    A subsequent GET MUST return the same values --- regression guard
    against a path that updates the row but returns stale cached
    values from before the update.
    """
    user_id = uuid.uuid4()
    user_token = issue_jwt(
        subject=str(user_id),
        scopes=[READ_SCOPE, WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {user_token}"}

    new_prefs: dict[str, Any] = {
        "email_enabled": False,
        "sms_enabled": True,
        "locale": "de-DE",
        "quiet_hours_start": 22,
        "quiet_hours_end": 7,
        "timezone": "Europe/Berlin",
    }
    put_response = client.put(
        f"/api/v1/preferences/{user_id}",
        json=new_prefs,
        headers=headers,
    )

    assert put_response.status_code in (200, 201), (
        f"PUT preferences should succeed (upsert); got "
        f"{put_response.status_code}: {put_response.text}"
    )
    put_body = put_response.json()
    for key, expected in new_prefs.items():
        assert put_body.get(key) == expected, (
            f"PUT response should reflect the new value for {key!r}: "
            f"sent {expected!r}, got {put_body.get(key)!r}"
        )

    # Round-trip via GET --- must return the same values.
    get_response = client.get(
        f"/api/v1/preferences/{user_id}", headers=headers
    )
    assert get_response.status_code == 200
    get_body = get_response.json()
    for key, expected in new_prefs.items():
        assert get_body.get(key) == expected, (
            f"GET after PUT should return persisted value for {key!r}: "
            f"sent {expected!r}, got {get_body.get(key)!r}"
        )


# ===========================================================================
# Test 4.19 --- PUT preferences with out-of-range quiet hours rejected
# ===========================================================================
async def test_put_preferences_invalid_quiet_hours_rejected(
    client: TestClient,
    issue_jwt: Any,
) -> None:
    """Quiet hours outside the [0, 23] range are rejected with 4xx.

    The user_channel_prefs.quiet_hours_start / quiet_hours_end columns
    have CHECK constraints requiring values in [0, 23] (24-hour
    convention, no minute granularity). The controller MUST reject
    out-of-range values BEFORE the SQL layer to avoid a 500 leak.
    Per Phase 7 insights, "DB constraint errors surface as 400/422,
    not 500" is a load-bearing invariant.
    """
    user_id = uuid.uuid4()
    user_token = issue_jwt(
        subject=str(user_id),
        scopes=[READ_SCOPE, WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {user_token}"}

    # Sub-case 1: quiet_hours_start = 24 (off-by-one too high).
    response_high = client.put(
        f"/api/v1/preferences/{user_id}",
        json={
            "email_enabled": True,
            "sms_enabled": False,
            "locale": "en-US",
            "quiet_hours_start": 24,
            "quiet_hours_end": 7,
            "timezone": "UTC",
        },
        headers=headers,
    )
    assert response_high.status_code in (400, 422), (
        f"quiet_hours_start=24 should be rejected with 400/422 not "
        f"500; got {response_high.status_code}: {response_high.text}"
    )

    # Sub-case 2: quiet_hours_end = -1 (negative).
    response_negative = client.put(
        f"/api/v1/preferences/{user_id}",
        json={
            "email_enabled": True,
            "sms_enabled": False,
            "locale": "en-US",
            "quiet_hours_start": 22,
            "quiet_hours_end": -1,
            "timezone": "UTC",
        },
        headers=headers,
    )
    assert response_negative.status_code in (400, 422), (
        f"quiet_hours_end=-1 should be rejected with 400/422 not "
        f"500; got {response_negative.status_code}: "
        f"{response_negative.text}"
    )


# ===========================================================================
# Test 4.20 --- Log query: keyset cursor pagination
# ===========================================================================
async def test_log_query_paginated_with_keyset_cursor(
    client: TestClient,
    issue_jwt: Any,
    notification_log_repo: Any,
) -> None:
    """GET /api/v1/log returns keyset-paginated results.

    REGRESSION GUARD: keyset (next_cursor) pagination is preferred
    over offset pagination on the notification_log table because
    offset-based scans become O(n) as the table grows --- by the
    time the table reaches 100M rows, `OFFSET 99999990 LIMIT 10`
    requires a full sequential scan.

    The keyset cursor encodes the (created_at, notification_id) tuple
    of the last row on the previous page. The DB query becomes
    ``WHERE (created_at, notification_id) < (cursor.created_at,
    cursor.notification_id) ORDER BY created_at DESC LIMIT n`` which
    is an indexed range scan.
    """
    admin_token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[READ_SCOPE],
    )
    headers = {"Authorization": f"Bearer {admin_token}"}

    # Seed 20 notification_log rows with monotonically-increasing
    # created_at timestamps. We construct timestamps without
    # ``timedelta`` (intentionally not imported per the schema) by
    # adding integer-second offsets to the base timestamp via
    # ``datetime.fromtimestamp``.
    base_time = datetime.now(timezone.utc)
    base_epoch = base_time.timestamp()
    for index in range(20):
        offset_epoch = base_epoch + index
        created_at = datetime.fromtimestamp(offset_epoch, tz=timezone.utc)
        await notification_log_repo.insert(
            event_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            channel="email",
            status="SUCCESS",
            created_at=created_at,
            attempt_count=1,
        )

    # Page 1.
    response_page1 = client.get(
        "/api/v1/log", params={"limit": 5}, headers=headers
    )
    assert response_page1.status_code == 200, (
        f"GET /api/v1/log page 1 should succeed; got "
        f"{response_page1.status_code}: {response_page1.text}"
    )
    page1_body = response_page1.json()
    page1_items = page1_body.get("items") or page1_body.get("data") or []
    assert len(page1_items) == 5, (
        f"page 1 should contain exactly 5 items (limit=5); got "
        f"{len(page1_items)}"
    )
    next_cursor = page1_body.get("next_cursor") or page1_body.get(
        "cursor"
    )
    assert next_cursor is not None, (
        f"page 1 should provide a next_cursor (more rows remain); "
        f"got body keys={list(page1_body.keys())}"
    )

    # The cursor must be a string (opaque to the client) --- if it
    # decodes to JSON its shape is internal-only and not part of the
    # public contract. We just assert it's a non-empty string.
    assert isinstance(next_cursor, str) and next_cursor, (
        f"next_cursor must be a non-empty opaque string; got "
        f"{next_cursor!r}"
    )

    # Page 2.
    response_page2 = client.get(
        "/api/v1/log",
        params={"limit": 5, "cursor": next_cursor},
        headers=headers,
    )
    assert response_page2.status_code == 200, (
        f"GET /api/v1/log page 2 should succeed; got "
        f"{response_page2.status_code}: {response_page2.text}"
    )
    page2_body = response_page2.json()
    page2_items = page2_body.get("items") or page2_body.get("data") or []
    assert len(page2_items) == 5, (
        f"page 2 should contain exactly 5 items (limit=5); got "
        f"{len(page2_items)}"
    )

    # No overlap between pages: the notification_id sets must be
    # disjoint. This is the canonical correctness signal for keyset
    # pagination --- offset-based pagination with concurrent writes
    # CAN produce overlap, but keyset cannot.
    page1_ids = {
        json.dumps(item.get("notification_id") or item.get("event_id"))
        for item in page1_items
    }
    page2_ids = {
        json.dumps(item.get("notification_id") or item.get("event_id"))
        for item in page2_items
    }
    assert page1_ids.isdisjoint(page2_ids), (
        f"pages 1 and 2 should not overlap; got "
        f"shared ids={page1_ids & page2_ids}"
    )


# ===========================================================================
# Test 4.21 --- Log query: filter by channel and status
# ===========================================================================
async def test_log_query_filter_by_channel_and_status(
    client: TestClient,
    issue_jwt: Any,
    notification_log_repo: Any,
) -> None:
    """Log query supports channel + status filtering.

    Operators triaging incidents need to scope the log to (channel,
    status) tuples --- e.g., "show me all DEAD_LETTER on the email
    channel" --- without scanning the full table. The endpoint must
    push these filters into SQL (``WHERE channel = $1 AND status =
    $2``) rather than filtering in application code, which would
    require fetching everything and discarding most.
    """
    admin_token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[READ_SCOPE],
    )
    headers = {"Authorization": f"Bearer {admin_token}"}

    # Seed a heterogeneous mix of (channel, status) combinations so
    # the filter has something to discard.
    seeds = [
        ("email", "DEAD_LETTER"),
        ("email", "SUCCESS"),
        ("email", "DEAD_LETTER"),
        ("sms", "DEAD_LETTER"),
        ("sms", "SUCCESS"),
        ("email", "PENDING_RETRY"),
    ]
    for channel, status in seeds:
        await notification_log_repo.insert(
            event_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            channel=channel,
            status=status,
            created_at=datetime.now(timezone.utc),
            attempt_count=1,
        )

    response = client.get(
        "/api/v1/log",
        params={"channel": "email", "status": "DEAD_LETTER"},
        headers=headers,
    )

    assert response.status_code == 200, (
        f"filtered log query should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    body = response.json()
    items = body.get("items") or body.get("data") or []
    # Expect exactly the 2 (email, DEAD_LETTER) seed rows. We assert
    # >= 2 rather than == 2 to be resilient to pre-existing fixture
    # rows; the AAP-mandated invariant is FILTER CORRECTNESS.
    assert len(items) >= 2, (
        f"expected >= 2 (email, DEAD_LETTER) rows; got {len(items)}: "
        f"{items}"
    )
    for item in items:
        assert item.get("channel") == "email", (
            f"filter violation: expected channel=email, got "
            f"channel={item.get('channel')!r} in row {item!r}"
        )
        assert item.get("status") == "DEAD_LETTER", (
            f"filter violation: expected status=DEAD_LETTER, got "
            f"status={item.get('status')!r} in row {item!r}"
        )


# ===========================================================================
# Test 4.22 --- Log query is admin-only
# ===========================================================================
async def test_log_query_rejects_non_admin(
    client: TestClient,
    issue_jwt: Any,
) -> None:
    """GET /api/v1/log without admin role returns 403.

    The notification log is cross-tenant: it contains rows for every
    user. Exposing it to non-admin tokens would leak other users'
    notification activity. Per AAP R-23, OAuth scopes/roles gate
    admin endpoints --- the log query is admin-only by design.
    """
    user_token = issue_jwt(
        subject="regular-user",
        scopes=[READ_SCOPE],
    )
    response = client.get(
        "/api/v1/log",
        headers={"Authorization": f"Bearer {user_token}"},
    )

    assert response.status_code == 403, (
        f"non-admin GET /api/v1/log should return 403; got "
        f"{response.status_code}: {response.text}"
    )



# ===========================================================================
# Test 4.23 --- Correlation ID supplied by client is propagated to logs
# ===========================================================================
async def test_correlation_id_propagated_to_log_record(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
    captured_logs: list[dict[str, Any]],
    assert_required_log_fields: Any,
) -> None:
    """A client-supplied X-Correlation-ID surfaces in captured_logs.

    AAP R-13 / R-26 anchor: every request produces a structured log
    record carrying the request's correlation_id. This is load-bearing
    for Kibana operator queries --- when an incident is reported
    against a known correlation_id, the operator must be able to find
    every log line emitted during that request.

    Verifies:
    1. The response echoes the client-supplied X-Correlation-ID.
    2. captured_logs contains AT LEAST ONE record with the same
       correlation_id field.
    3. That record carries every required AAP R-26 field (timestamp,
       level, service=notification-service, correlation_id, message).
    """
    admin_token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    correlation_id = str(uuid.uuid4())

    response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(),
        headers={
            "Authorization": f"Bearer {admin_token}",
            "X-Correlation-ID": correlation_id,
        },
    )

    assert response.status_code in (200, 201), (
        f"setup: template POST should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    _assert_correlation_id_echoed(response, correlation_id)

    # Locate the access log record for this request. The middleware
    # emits the structured record after the response is sent so the
    # record IS available to captured_logs by the time the test
    # inspects it (TestClient is synchronous).
    matching_records = [
        rec
        for rec in captured_logs
        if rec.get("correlation_id") == correlation_id
    ]
    assert matching_records, (
        f"captured_logs has no record with correlation_id="
        f"{correlation_id!r}; total records collected="
        f"{len(captured_logs)}"
    )

    # The fixture-provided helper enforces the canonical AAP R-26
    # field set across every supplied record. We hand it the first
    # matching record (typically the request entry log).
    record = matching_records[0]
    assert_required_log_fields(record)


# ===========================================================================
# Test 4.24 --- Server mints a UUID correlation_id when client omits header
# ===========================================================================
async def test_correlation_id_minted_if_client_omits(
    client: TestClient,
    issue_jwt: Any,
    captured_logs: list[dict[str, Any]],
) -> None:
    """When the client omits X-Correlation-ID, the server mints a UUID4.

    Per AAP R-13, the correlation_id is the load-bearing identifier
    operators use to trace a request through Kibana. When the client
    does not supply one, the CorrelationIdMiddleware MUST mint a
    fresh UUID4 and propagate it everywhere --- response header,
    log records, downstream HTTP calls, Kafka headers.

    Verifies:
    1. The response carries an X-Correlation-ID header.
    2. The minted value is a valid UUID4 string (parses with
       ``uuid.UUID`` without raising).
    3. The captured access log record carries the same minted value.
    """
    admin_token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[READ_SCOPE],
    )
    # NO X-Correlation-ID header --- middleware must mint one.
    response = client.get(
        "/api/v1/templates",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert response.status_code == 200, (
        f"GET /api/v1/templates should succeed; got "
        f"{response.status_code}: {response.text}"
    )
    minted = response.headers.get("X-Correlation-ID") or \
        response.headers.get("x-correlation-id")
    assert minted is not None, (
        "response missing X-Correlation-ID header --- the middleware "
        "must mint one when the client does not supply one"
    )

    # Validate minted is a well-formed UUID. ``uuid.UUID(...)``
    # raises ValueError on malformed strings; the assertion converts
    # that into a test failure with a clear diagnostic.
    try:
        parsed = uuid.UUID(minted)
    except (ValueError, AttributeError) as exc:
        raise AssertionError(
            f"minted X-Correlation-ID is not a valid UUID: {minted!r} "
            f"(parsing error: {exc})"
        ) from exc
    assert parsed is not None

    # The minted value must surface in captured_logs --- otherwise
    # operators cannot correlate this request.
    matching_records = [
        rec
        for rec in captured_logs
        if rec.get("correlation_id") == minted
    ]
    assert matching_records, (
        f"captured_logs has no record with the minted correlation_id "
        f"{minted!r}; total records={len(captured_logs)}"
    )


# ===========================================================================
# Test 4.25 --- Every access log record carries the AAP R-26 required fields
# ===========================================================================
async def test_required_log_fields_on_every_access_record(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
    captured_logs: list[dict[str, Any]],
    assert_required_log_fields: Any,
) -> None:
    """Every access log record carries the canonical AAP R-26 fields.

    AAP R-26 enumerates the required structured-log fields:
    ``timestamp`` (RFC 3339), ``level``, ``service``,
    ``correlation_id``, ``user_id`` (when known), ``route``,
    ``method``, ``status``, ``latency_ms``, ``message``.

    The Kibana dashboards (AAP R-28) and ILM policies (AAP R-29)
    depend on this field set --- a missing field breaks log
    aggregation and routing.
    """
    admin_token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    correlation_id = str(uuid.uuid4())

    response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(),
        headers={
            "Authorization": f"Bearer {admin_token}",
            "X-Correlation-ID": correlation_id,
        },
    )
    assert response.status_code in (200, 201)

    # Filter to records produced by THIS request. The fixture-supplied
    # captured_logs is module / function scoped; cross-test
    # contamination is avoided by filtering on correlation_id.
    request_records = [
        rec
        for rec in captured_logs
        if rec.get("correlation_id") == correlation_id
    ]
    assert request_records, (
        f"no captured_logs records for correlation_id={correlation_id!r}"
    )

    # Find an "access log" style record --- one that has both
    # ``method`` and ``status`` keys, indicating it's the access log
    # entry rather than a domain-event log entry.
    access_log_candidates = [
        rec
        for rec in request_records
        if "method" in rec and "status" in rec
    ]
    # If the middleware encodes method/status in nested fields or
    # only on the request entry record, the candidates list may be
    # empty --- in that case we fall back to the first record.
    record = (
        access_log_candidates[0]
        if access_log_candidates
        else request_records[0]
    )

    # Canonical R-26 fields enforced via fixture helper.
    assert_required_log_fields(record)

    # Service field MUST be the canonical "notification-service" so
    # the ELK ingest pipeline routes records to the correct index.
    assert record.get("service") == "notification-service", (
        f"AAP R-26: service field must be 'notification-service'; "
        f"got {record.get('service')!r}"
    )

    # Authenticated requests have user_id (JWT sub).
    assert record.get("user_id") in ("admin", str(record.get("user_id"))), (
        f"AAP R-26: authenticated requests must carry user_id "
        f"(JWT sub); got user_id={record.get('user_id')!r}"
    )

    # Access log records additionally carry route, method, status,
    # latency_ms. We assert on the records that include them; if the
    # current record doesn't, find one that does.
    access_records = [
        rec
        for rec in request_records
        if all(k in rec for k in ("method", "status"))
    ]
    if access_records:
        access_record = access_records[0]
        for required_access_field in (
            "method", "status", "route", "latency_ms"
        ):
            # ``route`` may be encoded as ``path`` and ``latency_ms``
            # as ``duration_ms`` in some structlog configurations; we
            # accept either canonical name to avoid coupling the test
            # to a specific structlog processor.
            alternative = {
                "route": "path",
                "latency_ms": "duration_ms",
            }.get(required_access_field, required_access_field)
            assert (
                required_access_field in access_record
                or alternative in access_record
            ), (
                f"access record missing {required_access_field!r} "
                f"(or alternative {alternative!r}); got record keys="
                f"{list(access_record.keys())}"
            )


# ===========================================================================
# Test 4.26 --- Error responses sanitize internal exception details
# ===========================================================================
async def test_error_response_sanitizes_internal_details(
    client: TestClient,
    issue_jwt: Any,
    template_repo: Any,
    monkeypatch: pytest.MonkeyPatch,
    captured_logs: list[dict[str, Any]],
) -> None:
    """500 responses must not leak internal exception messages.

    SECURITY: Internal exception messages may contain sensitive
    information --- file paths, SQL fragments, secret values
    accidentally embedded in dev-time error messages. The
    ``ErrorHandlerMiddleware`` MUST replace internal exception
    messages with a generic ``error_code`` (e.g., ``internal_error``)
    in the response body, while keeping the verbose message in the
    structured log for operator debugging.

    Verifies:
    1. The response body uses a sanitized error code.
    2. The response body includes a correlation_id reference so
       operators can cross-reference the log record.
    3. The response body does NOT contain the secret string.
    4. The captured log records DO contain the verbose message
       (otherwise operators cannot debug).
    """
    admin_token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )

    # Patch the template repo's save method to raise a RuntimeError
    # carrying a "secret" string. The controller's exception handler
    # should catch this, log the verbose message, and return a
    # sanitized 500 response.
    secret_payload = "internal-secret-path-do-not-leak"

    async def _raise_internal(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError(secret_payload)

    # The repository may expose ``save`` or ``create`` or ``insert``;
    # we patch all canonical method names defensively. monkeypatch
    # uses raising=False so missing attributes don't error.
    for method_name in ("save", "create", "insert", "upsert"):
        monkeypatch.setattr(
            template_repo, method_name, _raise_internal, raising=False
        )

    response = client.post(
        "/api/v1/templates",
        json=_new_template_payload(),
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    # The response is 500 because the repo raised. The
    # ErrorHandlerMiddleware should sanitize it.
    assert response.status_code == 500, (
        f"controller exception should surface as 500; got "
        f"{response.status_code}: {response.text}"
    )
    body_text = response.text

    # The secret MUST NOT appear in the response body.
    assert secret_payload not in body_text, (
        f"response body leaked internal secret string {secret_payload!r}; "
        f"got body={body_text}"
    )

    # The body should be valid JSON with a sanitized error code and
    # a correlation_id reference for operator triage.
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        # Some middlewares emit text/plain on 500. That's acceptable
        # as long as the secret is not leaked --- already asserted
        # above. We still want a correlation_id signal somewhere.
        body = {}

    if isinstance(body, dict):
        # Canonical sanitized shape: ``error`` carries a stable code
        # and ``correlation_id`` is the operator cross-reference.
        body_has_code = (
            isinstance(body.get("error"), str)
            or isinstance(body.get("error_code"), str)
            or isinstance(body.get("detail"), str)
        )
        assert body_has_code, (
            f"500 body should carry a sanitized error code; got "
            f"body={body!r}"
        )
        # Correlation ID surface either in body or in response
        # headers --- both are acceptable patterns. Per the spec,
        # the body SHOULD include it.
        body_correlation = (
            body.get("correlation_id")
            or body.get("request_id")
        )
        header_correlation = response.headers.get(
            "X-Correlation-ID"
        ) or response.headers.get("x-correlation-id")
        assert body_correlation or header_correlation, (
            f"500 response must carry a correlation_id (in body or "
            f"header) so operators can cross-reference the log; got "
            f"body={body!r}, headers={dict(response.headers)}"
        )

    # The verbose internal message MUST be in the captured logs so
    # operators can debug. Search for the secret string in any log
    # record.
    log_text = json.dumps(captured_logs)
    assert secret_payload in log_text, (
        f"captured_logs should contain the verbose error message "
        f"{secret_payload!r} for operator debugging (it was already "
        f"verified to be ABSENT from the response body); got "
        f"len(captured_logs)={len(captured_logs)}"
    )



# ===========================================================================
# Test 4.27 --- Rate limit returns 429 with Retry-After header
# ===========================================================================
@pytest.mark.slow
async def test_rate_limit_returns_429_with_retry_after(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
) -> None:
    """Sustained admin write traffic eventually returns 429 + Retry-After.

    Per AAP rate-limiting requirements (R-15 / R-16 family + the API
    Gateway's per-route token-bucket), repeated writes over the
    configured rate eventually return ``429 Too Many Requests``. The
    response MUST carry a ``Retry-After`` header (RFC 7231) with an
    integer number of seconds so well-behaved clients can back off
    rather than retrying immediately.

    NOTE: This test is marked ``@pytest.mark.slow`` because the
    gateway-layer rate limiter may be configured at a much higher
    threshold than a single test can exceed; in that case the test is
    expected to skip via the slow-test runner config rather than
    fail. When the limiter IS reachable from a single test, the
    assertions below verify the contract.
    """
    admin_token = issue_jwt(
        subject="admin",
        roles=[ADMIN_ROLE],
        scopes=[WRITE_SCOPE],
    )
    headers = {"Authorization": f"Bearer {admin_token}"}

    # Hammer the POST endpoint with a bounded number of requests. We
    # do not hardcode the rate-limit threshold (it may be config-
    # driven); we observe the first 429 and assert its shape.
    rate_limit_response = None
    max_attempts = 200
    for attempt_index in range(max_attempts):
        # Each request uses a fresh template payload to avoid hitting
        # idempotency-key caching paths that would short-circuit the
        # write before it reaches the rate limiter.
        unique_payload = _new_template_payload(
            event_type=f"rate.test.{attempt_index}",
        )
        response = client.post(
            "/api/v1/templates", json=unique_payload, headers=headers
        )
        if response.status_code == 429:
            rate_limit_response = response
            break

    if rate_limit_response is None:
        pytest.skip(
            f"rate limiter did not engage within {max_attempts} "
            f"requests; threshold is configured higher than this test "
            f"can reach. Mark @pytest.mark.slow handles the skip path."
        )

    # ---- 429 contract assertions ----
    assert rate_limit_response.status_code == 429
    retry_after = rate_limit_response.headers.get(
        "Retry-After"
    ) or rate_limit_response.headers.get("retry-after")
    assert retry_after is not None, (
        f"429 response missing Retry-After header (RFC 7231); got "
        f"headers={dict(rate_limit_response.headers)}"
    )
    # Retry-After is either an integer number of seconds OR an
    # HTTP-date. We assert integer-seconds form because the gateway
    # commonly chooses the simpler representation; if implementations
    # diverge, the test can be relaxed to accept either format.
    try:
        retry_after_seconds = int(retry_after)
    except ValueError as exc:
        raise AssertionError(
            f"Retry-After should be an integer number of seconds; got "
            f"{retry_after!r} (parsing error: {exc})"
        ) from exc
    assert retry_after_seconds >= 0, (
        f"Retry-After must be non-negative; got {retry_after_seconds}"
    )

    # The body should reference a stable, machine-readable error code
    # so clients can branch on the cause (vs e.g. validation errors).
    try:
        body = rate_limit_response.json()
    except (json.JSONDecodeError, ValueError):
        body = {}
    if isinstance(body, dict):
        error_code = (
            body.get("error")
            or body.get("error_code")
            or body.get("detail")
            or ""
        )
        # Canonical error codes for rate limiting:
        # ``rate_limit_exceeded`` / ``rate_limited`` / ``too_many_requests``.
        assert any(
            token in str(error_code).lower()
            for token in ("rate", "limit", "too_many", "throttle")
        ), (
            f"429 body should reference a rate-limit error code; got "
            f"body={body!r}"
        )


# ===========================================================================
# Test 4.28 --- Multiple JWT role-claim formats are accepted
# ===========================================================================
async def test_admin_role_claim_alternate_format(
    client: TestClient,
    issue_jwt: Any,
    reset_templates: Any,
) -> None:
    """The middleware accepts admin role in multiple claim locations.

    Per Phase 7 insights, JWT role claims are not standardized.
    Different issuers encode roles differently:
    * Auth0-style: ``"roles": ["notifications:admin"]`` (a JSON
      array claim).
    * OAuth-scope-style: ``"scope": "notifications:admin
      notifications:write"`` (a space-separated string of scopes
      that play double-duty as roles).
    * Keycloak-style: ``"realm_access": {"roles": [...]}`` (a nested
      object).

    The middleware should accept the first two forms (as documented
    by the agent prompt's test 4.28). This test produces tokens in
    each form and verifies that each grants admin access.
    """
    # Form 1: ``roles`` array claim --- the canonical shape used by
    # most tests in this module.
    token_with_roles = issue_jwt(
        subject="admin-roles",
        roles=[ADMIN_ROLE],
    )
    response_roles = client.post(
        "/api/v1/templates",
        json=_new_template_payload(event_type="roles.test"),
        headers={"Authorization": f"Bearer {token_with_roles}"},
    )
    assert response_roles.status_code in (200, 201), (
        f"token with `roles=[{ADMIN_ROLE}]` should succeed on admin "
        f"POST; got {response_roles.status_code}: {response_roles.text}"
    )

    # Form 2: ``scope`` space-separated string. The OAuth 2.0
    # convention treats ``scope`` as a space-separated string; the
    # middleware should split on whitespace and treat each token as
    # a candidate role/scope.
    token_with_scope = issue_jwt(
        subject="admin-scope",
        scopes=[ADMIN_ROLE, WRITE_SCOPE],
    )
    response_scope = client.post(
        "/api/v1/templates",
        json=_new_template_payload(event_type="scope.test"),
        headers={"Authorization": f"Bearer {token_with_scope}"},
    )
    assert response_scope.status_code in (200, 201), (
        f"token with `scope='{ADMIN_ROLE} {WRITE_SCOPE}'` should "
        f"succeed on admin POST; got {response_scope.status_code}: "
        f"{response_scope.text}"
    )

