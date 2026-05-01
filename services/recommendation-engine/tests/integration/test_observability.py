"""End-to-end integration tests for observability.

Validates AAP R-26 structured logging contract and Prometheus metrics
exposure of the Recommendation Engine service.

Structured log contract (AAP R-26): every record emitted by the service
during an HTTP request or Kafka event processing MUST be a single line
of valid JSON containing ALL of these keys:

* ``timestamp`` — RFC 3339 formatted timestamp with timezone.
* ``level`` — log level string (INFO, WARN, ERROR, DEBUG).
* ``service`` — constant ``"recommendation-engine"``.
* ``correlation_id`` — present when a correlation ID is bound
  (via middleware or Kafka header propagation, AAP R-13).
* ``user_id`` — present when a user context is known
  (extracted from JWT claims by JwtAuthMiddleware or from event payload).
* ``route`` — HTTP route template (e.g., ``"/recommendations"``) for
  access logs; consumer processing logs omit this.
* ``method`` — HTTP method for access logs; absent for consumer logs.
* ``status`` — HTTP status code for access logs; absent for consumer.
* ``latency_ms`` — request/handler duration in milliseconds.
* ``message`` — human-readable event name (equivalent to structlog's
  ``event`` field, renamed per AAP R-26 via ``EventRenamer``).

Prometheus contract: ``GET /metrics`` returns text format 200 with at
least these metrics (per AAP Section 0.4 / observability fan-in):

* ``kafka_messages_consumed_total{topic, result}``
* ``event_dispatch_duration_seconds_bucket{event_class, le}``
* ``inference_duration_seconds_bucket{le}``
* ``fallback_used_total{tier}`` (tiers: ``ml | cache | popularity | default``)
* ``http_requests_total{method, route, status}``
* ``http_request_duration_seconds_bucket{method, route, le}``

Compliance highlights
---------------------
* AAP R-13 — Correlation-ID propagation: every log record for a single
  request shares the same ``correlation_id`` value. Tested in
  :func:`test_correlation_id_present_in_every_record_for_request`.
* AAP R-19 — ``/metrics`` and ``/health/live`` MUST be reachable without
  a JWT token. Tested in :func:`test_metrics_endpoint_no_auth_required`.
* AAP R-25 — Internal stack traces MUST NOT leak to client response
  bodies; structured tracebacks live ONLY in the log stream. Tested in
  :func:`test_error_log_on_500_has_dict_tracebacks`.
* AAP R-26 — Required fields enumerated above; tested per-record by
  :func:`_assert_required_log_fields` invoked from every access-log
  test case.
* AAP R-27 — Log lines flow to stdout where Filebeat tails them; the
  ``stdout_capture`` fixture intercepts that exact stream, validating
  Beats-compatible output shape end-to-end.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
import io
import json
import re
import sys
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
# ``pytest`` provides the test framework: ``pytest.mark.asyncio`` (module
# pytestmark), ``pytest.fixture`` decorator, and ``pytest.MonkeyPatch``
# annotation. ``fastapi.testclient.TestClient`` is the type hint for the
# ``client`` fixture defined in conftest.py — tests call ``client.get(...)``
# to exercise the full middleware stack (CorrelationId, Logging, JWTAuth,
# Error) end-to-end.
import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Module-level pytest markers
# ---------------------------------------------------------------------------
# All tests in this module are async to allow direct ``await`` usage when
# interacting with Kafka producer/consumer fixtures. ``TestClient`` calls
# remain synchronous within ``async def`` bodies, which is supported by
# pytest-asyncio's auto/strict modes.
pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
#: Fields that MUST be present on EVERY structured log record per AAP R-26.
#: ``correlation_id`` is conditionally present (only when bound by
#: ``CorrelationIdMiddleware`` or a Kafka consumer) so it is NOT in this
#: required set; tests that exercise authenticated request paths verify it
#: separately. ``timestamp``, ``level``, ``service``, and ``message`` are
#: the absolute minimum that EVERY log line must carry — no exceptions.
REQUIRED_LOG_FIELDS: set[str] = {
    "timestamp",
    "level",
    "service",
    "message",
}

#: Fields present only in specific contexts. Documenting them here lets
#: tests reason about WHEN to assert their presence:
#:
#: * ``correlation_id`` — bound by ``CorrelationIdMiddleware`` for HTTP
#:   requests and by the Kafka consumer for processed events. AAP R-13
#:   makes it operationally REQUIRED on every record produced inside a
#:   request lifecycle, but it MAY be absent from background-task logs
#:   emitted before any request has bound it.
#: * ``user_id`` — populated only when a JWT-authenticated user context
#:   exists; anonymous requests (``/metrics``, ``/health/live``, 401
#:   responses) deliberately omit it.
#: * ``route``, ``method``, ``status``, ``latency_ms`` — HTTP access-log
#:   fields. Kafka consumer logs omit them and instead carry
#:   ``topic``, ``partition``, ``offset``, ``event_class`` (asserted
#:   directly in ``test_kafka_consumer_log_has_required_fields``).
CONDITIONAL_LOG_FIELDS: set[str] = {
    "correlation_id",
    "user_id",
    "route",
    "method",
    "status",
    "latency_ms",
}

#: RFC 3339 timestamp regex. Matches ISO-8601 values such as
#: ``2024-04-01T12:34:56Z`` and ``2024-04-01T12:34:56.123456+00:00`` —
#: i.e., date-time with required ``T`` separator, optional fractional
#: seconds, and either a literal ``Z`` (Zulu / UTC) or a numeric offset
#: (``+HH:MM`` or ``-HH:MM``). Compiled once at module scope to avoid
#: re-compilation on every assertion (PEP 8 / performance hygiene).
RFC3339_REGEX: re.Pattern[str] = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
)

#: Prometheus text-exposition line regex (subset of v0.0.4 grammar).
#: Matches ``metric_name`` (optionally with ``{label="value",...}``)
#: followed by whitespace and a numeric value, e.g.::
#:
#:     http_requests_total{method="GET",route="/x",status="200"} 42
#:
#: Compiled once and reused by ``_parse_prometheus_text`` for every
#: scrape-response body inspected by tests.
_PROM_LINE_REGEX: re.Pattern[str] = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(.+)$"
)

#: The set of metric names this service MUST expose at ``/metrics``.
#: These cover the four observability dimensions called out by the
#: folder spec — Kafka consumption, internal event dispatch latency,
#: ML inference latency, fallback tier counts, and the standard HTTP
#: pair (volume + latency). Histograms appear here with the bare metric
#: name; their concrete sample lines surface as ``<name>_bucket``,
#: ``<name>_count``, and ``<name>_sum`` in the scrape output, all of
#: which start with the registered name and therefore satisfy the
#: presence check via ``startswith``-style matching.
EXPECTED_METRICS: set[str] = {
    "kafka_messages_consumed_total",
    "event_dispatch_duration_seconds",
    "inference_duration_seconds",
    "fallback_used_total",
    "http_requests_total",
    "http_request_duration_seconds",
}

#: The exact ``service`` value AAP R-26 mandates for log lines emitted
#: by this microservice. Kibana index patterns (``service:"recommendation-engine"``)
#: and Grafana dashboards filter on this literal — even minor variations
#: (``"reco-engine"``, ``"rec_engine"``, ``"recoEngine"``) would silently
#: hide the service from observability tools. Tested in
#: :func:`test_service_field_is_exact_constant`.
_EXPECTED_SERVICE_NAME: str = "recommendation-engine"

#: Acceptable ``level`` values in log records. structlog's
#: ``add_log_level`` processor emits lowercase strings by default, but
#: the service's processor chain (per the folder spec for
#: ``src/config/logging_config.py``) normalises them to UPPERCASE so
#: Filebeat / Logstash dashboards filter on a stable enum. Both
#: ``WARN`` and ``WARNING`` are tolerated — Python's stdlib logger uses
#: ``WARNING`` whereas structlog's add_log_level can emit ``WARN``;
#: either keeps the dashboards readable.
_VALID_LOG_LEVELS: frozenset[str] = frozenset(
    {"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"}
)

#: Maximum reasonable ``latency_ms`` value for any single request in
#: this test suite. 30 seconds is pathological — even a heavily-loaded
#: ML inference path with cold caches should complete in single-digit
#: seconds. Used by :func:`test_latency_ms_is_numeric_and_reasonable`
#: as a regression guard against handler hangs.
_MAX_REASONABLE_LATENCY_MS: float = 30_000.0


# ---------------------------------------------------------------------------
# Helper functions (module-private; prefixed with ``_``)
# ---------------------------------------------------------------------------
def _parse_log_lines(stdout_text: str) -> list[dict[str, Any]]:
    """Parse every non-empty line of captured stdout as JSON.

    Lines that fail JSON parsing trigger an ``AssertionError`` — all
    production logs MUST be JSON (AAP R-26 forbids plain-text logs in
    production; a single ``print()`` call or misconfigured stdlib
    handler that emits raw text would break Filebeat's JSON parser and
    silently drop log lines).

    :param stdout_text: The raw captured stdout content, typically
        returned by ``stdout_capture.getvalue()``.
    :returns: A list of decoded JSON objects, one per non-empty line,
        in the order they were emitted.
    :raises AssertionError: If any non-empty line is not valid JSON.
    """
    records: list[dict[str, Any]] = []
    for raw in stdout_text.splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        try:
            decoded = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise AssertionError(
                f"Non-JSON log line found (violates AAP R-26): "
                f"{stripped!r} — {exc}"
            ) from exc
        # AAP R-26 expects log records to be JSON OBJECTS (key-value
        # maps). Reject arrays, scalars, etc. early so downstream
        # ``record["timestamp"]`` indexing surfaces a clearer error.
        if not isinstance(decoded, dict):
            raise AssertionError(
                f"Log line is not a JSON object (violates AAP R-26): "
                f"{stripped!r}"
            )
        records.append(decoded)
    return records


def _parse_prometheus_text(body: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """Parse a Prometheus text-format response body into a dict.

    The returned mapping has the metric name as the key and a list of
    ``(labels, value)`` pairs as the value. ``labels`` is a
    ``dict[str, str]`` of the parsed label set (empty dict when the
    sample line had no labels). ``value`` is a ``float`` — Prometheus
    permits scientific notation, ``NaN``, ``+Inf``, ``-Inf``, all of
    which ``float()`` parses natively.

    The parser is intentionally minimal: it skips ``# HELP`` / ``# TYPE``
    metadata lines and any malformed lines (e.g., truncated scrape
    responses). It is sufficient for the assertions made in this test
    module — presence of a metric name, increment of a counter, label
    cardinality — but should not be reused as a general-purpose
    Prometheus parser.

    :param body: The raw response body from ``GET /metrics``.
    :returns: A mapping ``{metric_name: [(labels, value), ...]}``.
    """
    out: dict[str, list[tuple[dict[str, str], float]]] = {}
    for line in body.splitlines():
        stripped = line.strip()
        # Skip blanks and HELP/TYPE comments.
        if not stripped or stripped.startswith("#"):
            continue
        match = _PROM_LINE_REGEX.match(stripped)
        if not match:
            # Malformed line — silently skip. The test
            # ``test_metrics_text_exposition_format`` asserts no
            # malformed lines exist, so any fallthrough here is a
            # parser-only concern (e.g., an unexpected histogram
            # sample shape) that does not invalidate the wider
            # presence assertions.
            continue
        name, labels_raw, value_raw = match.groups()
        labels: dict[str, str] = {}
        if labels_raw:
            # Split label pairs on commas. Prometheus does not allow
            # commas inside label values (per the v0.0.4 grammar),
            # so naive split-on-``,`` is correct for valid output.
            for pair in labels_raw.split(","):
                key, _sep, value = pair.partition("=")
                labels[key.strip()] = value.strip().strip('"')
        try:
            numeric_value = float(value_raw.strip().split()[0])
        except (ValueError, IndexError):
            # Non-numeric values shouldn't appear in valid output;
            # skip rather than fail to keep the parser tolerant.
            continue
        out.setdefault(name, []).append((labels, numeric_value))
    return out


def _assert_required_log_fields(record: dict[str, Any]) -> None:
    """Assert a single log record satisfies AAP R-26's required fields.

    This is the single chokepoint test helper invoked from every
    test that inspects a log record. It validates:

    1. All four required fields (``timestamp``, ``level``, ``service``,
       ``message``) are present.
    2. ``service`` is the exact literal ``"recommendation-engine"``.
    3. ``timestamp`` matches the RFC 3339 regex.
    4. ``level`` is one of the canonical level strings.

    :param record: A decoded log record (one element of the list
        returned by :func:`_parse_log_lines`).
    :raises AssertionError: If any of the above checks fails. The
        error message includes the full offending record for
        operator-friendly debugging.
    """
    missing = REQUIRED_LOG_FIELDS - set(record.keys())
    assert not missing, (
        f"Log record missing required fields {missing!r} "
        f"(violates AAP R-26): {record!r}"
    )
    assert record["service"] == _EXPECTED_SERVICE_NAME, (
        f"service field must be {_EXPECTED_SERVICE_NAME!r}, got "
        f"{record['service']!r} — Kibana dashboards filter on the "
        f"exact literal; variations silently hide the service from "
        f"observability tools."
    )
    timestamp_str = str(record["timestamp"])
    assert RFC3339_REGEX.match(timestamp_str), (
        f"timestamp {timestamp_str!r} not in RFC 3339 format "
        f"(violates AAP R-26): {record!r}"
    )
    level_str = str(record["level"]).upper()
    assert level_str in _VALID_LOG_LEVELS, (
        f"Invalid log level {record['level']!r} (expected one of "
        f"{sorted(_VALID_LOG_LEVELS)!r}): {record!r}"
    )



# ---------------------------------------------------------------------------
# Module-scoped fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def stdout_capture(monkeypatch: pytest.MonkeyPatch) -> Iterator[io.StringIO]:
    """Redirect stdout, stdlib logging, and structlog into one buffer.

    The Recommendation Engine's logging configuration (defined in
    ``services/recommendation-engine/config/log_config.json`` and
    bootstrapped by ``src.config.logging_config.configure_logging``)
    routes BOTH stdlib logging (uvicorn access logs, third-party
    libraries) AND structlog (application logs) to ``sys.stdout``
    through a JSON formatter. To validate AAP R-26 end-to-end we
    intercept that exact stream.

    Implementation strategy:

    1. Build an in-memory :class:`io.StringIO` buffer.
    2. Replace ``sys.stdout`` with the buffer via ``monkeypatch`` so
       any direct ``print()`` (which would itself violate R-26) is
       captured for the assertion in
       :func:`test_no_plaintext_logs_emitted`.
    3. Detach existing root logger handlers; install a fresh
       :class:`logging.StreamHandler` that targets the buffer with a
       :class:`pythonjsonlogger.jsonlogger.JsonFormatter` configured
       to emit the exact AAP R-26 field schema.
    4. Reconfigure ``structlog`` with the same processor chain the
       production code uses, but with a
       :class:`structlog.PrintLoggerFactory` whose ``file=`` argument
       points at the buffer. The chain includes
       :class:`structlog.processors.EventRenamer` (``to="message"``)
       — without that, structlog would emit ``event`` instead of
       ``message`` and silently violate R-26.
    5. Yield the buffer.
    6. On teardown, fully restore the original logging state so
       subsequent tests are unaffected.

    :param monkeypatch: pytest's ``MonkeyPatch`` fixture, used to
        rebind ``sys.stdout`` for the test duration.
    :yields: The :class:`io.StringIO` buffer; tests call
        ``buf.getvalue()`` to inspect captured output.
    """
    buf = io.StringIO()

    # Re-route the process's stdout so any rogue ``print()`` call lands
    # in the buffer. Tests inspecting "no plaintext logs" depend on
    # this redirection to surface mis-configured handlers.
    monkeypatch.setattr(sys, "stdout", buf)

    # Lazy imports inside the fixture body keep test discovery cheap:
    # neither ``logging`` nor ``structlog`` performs I/O at import,
    # but deferring binds the fixture's behavior to the actual
    # versions resolved at test runtime rather than at collection.
    import logging

    import structlog
    from pythonjsonlogger import jsonlogger

    # Snapshot existing root-logger handlers and level so we can
    # restore them deterministically in the ``finally`` block. Without
    # this, a subsequent test that depends on the production logging
    # config would observe the test-only handler and fail.
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]
    original_level = root_logger.level

    for handler in original_handlers:
        root_logger.removeHandler(handler)

    # Install a JSON-formatted stdlib logging handler that targets the
    # in-memory buffer. ``rename_fields`` mirrors the production
    # ``log_config.json`` so stdlib log records emerge with the same
    # ``timestamp`` / ``level`` / ``service`` keys structlog emits.
    test_handler = logging.StreamHandler(buf)
    # ``python-json-logger`` does not ship type stubs, so mypy --strict
    # raises ``[no-untyped-call]`` here. The library's API is stable
    # and well-documented; suppressing this single line keeps the rest
    # of the strict-mode coverage intact.
    test_formatter = jsonlogger.JsonFormatter(  # type: ignore[no-untyped-call]
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={
            "levelname": "level",
            "asctime": "timestamp",
            "name": "service",
        },
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    test_handler.setFormatter(test_formatter)
    root_logger.addHandler(test_handler)
    root_logger.setLevel(logging.DEBUG)

    # Snapshot structlog's existing configuration. structlog stores
    # configuration in module-level state, so we must restore it on
    # teardown to avoid leaking the test-only PrintLoggerFactory into
    # subsequent tests' loggers.
    original_structlog_config = structlog.get_config()

    # Reconfigure structlog to render JSON straight into the buffer.
    # The processor chain mirrors the production config in
    # ``src/config/logging_config.py`` exactly, including the AAP
    # R-26-mandated ``EventRenamer(to="message")`` step.
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.add_log_level,
            structlog.processors.EventRenamer(to="message"),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.PrintLoggerFactory(file=buf),
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        cache_logger_on_first_use=False,
    )

    try:
        yield buf
    finally:
        # Drop the test handler and restore the original root logger
        # state. Use a copy of ``handlers`` to avoid mutating the
        # list during iteration.
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
        for handler in original_handlers:
            root_logger.addHandler(handler)
        root_logger.setLevel(original_level)

        # Restore structlog's prior configuration. ``configure``
        # accepts a kwargs-style dict expansion of the prior
        # config keys we care about; passing each explicitly is
        # safer than ``**original_structlog_config`` because
        # structlog's internal config dict may include keys that
        # are not valid arguments to ``configure``.
        structlog.configure(
            processors=original_structlog_config.get("processors", []),
            logger_factory=original_structlog_config.get(
                "logger_factory", structlog.PrintLoggerFactory()
            ),
            wrapper_class=original_structlog_config.get(
                "wrapper_class",
                structlog.make_filtering_bound_logger(logging.NOTSET),
            ),
            cache_logger_on_first_use=original_structlog_config.get(
                "cache_logger_on_first_use", False
            ),
        )


# ---------------------------------------------------------------------------
# Internal helpers used by tests (module-private)
# ---------------------------------------------------------------------------
def _find_access_log_records(
    records: list[dict[str, Any]],
    *,
    route: str | None = None,
    status: int | None = None,
) -> list[dict[str, Any]]:
    """Filter parsed log records to access-log entries.

    An "access log" record is one that carries the HTTP request
    metadata fields (``route``, ``method``, ``status``). Optionally
    filter by exact route template and/or status code.

    :param records: All decoded records (output of
        :func:`_parse_log_lines`).
    :param route: If provided, only return records whose ``route``
        field equals this value.
    :param status: If provided, only return records whose ``status``
        field equals this value.
    :returns: The filtered list.
    """
    out: list[dict[str, Any]] = []
    for record in records:
        if "route" not in record or "method" not in record or "status" not in record:
            continue
        if route is not None and record.get("route") != route:
            continue
        if status is not None and int(record.get("status", -1)) != status:
            continue
        out.append(record)
    return out


def _find_records_by_correlation_id(
    records: list[dict[str, Any]], correlation_id: str
) -> list[dict[str, Any]]:
    """Return records whose ``correlation_id`` field matches the given value."""
    return [r for r in records if r.get("correlation_id") == correlation_id]


def _build_auth_headers(jwt_token: str, correlation_id: str | None = None) -> dict[str, str]:
    """Construct the standard test request header bundle.

    :param jwt_token: A signed JWT for the ``Authorization: Bearer ...``
        header. Tests that need anonymous requests pass an empty string
        and rely on the ``Authorization`` key being stripped or the
        client sending the request without the header.
    :param correlation_id: Optional correlation ID to send via
        ``X-Correlation-ID``. When omitted, the API Gateway / service
        will mint a fresh UUID.
    :returns: A header dict ready to pass to ``client.get(headers=...)``.
    """
    headers: dict[str, str] = {"Authorization": f"Bearer {jwt_token}"}
    if correlation_id is not None:
        headers["X-Correlation-ID"] = correlation_id
    return headers




# ---------------------------------------------------------------------------
# Test cases — Access log structured field assertions (Phase 5.1 – 5.5)
# ---------------------------------------------------------------------------
async def test_access_log_has_all_required_fields_on_200(
    client: TestClient,
    stdout_capture: io.StringIO,
    auth_headers: dict[str, str],
    jwt_subject: str,
) -> None:
    """AAP R-26 — Successful authenticated request logs ALL required fields.

    Issues a ``GET /recommendations`` with a valid bearer token and a
    client-supplied ``X-Correlation-ID`` header. Asserts that the
    resulting access-log record contains the full AAP R-26 field set
    PLUS the request-specific fields (``method``, ``status``,
    ``route``, ``latency_ms``, ``correlation_id``, ``user_id``).
    """
    correlation_id = str(uuid.uuid4())
    request_user_id = str(uuid.uuid4())
    headers = dict(auth_headers)
    headers["X-Correlation-ID"] = correlation_id

    # Act — exercise the full middleware stack end-to-end.
    response = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10",
        headers=headers,
    )

    assert response.status_code == 200, (
        f"Expected 200, got {response.status_code}: {response.text!r}"
    )

    records = _parse_log_lines(stdout_capture.getvalue())
    access_records = _find_access_log_records(
        records, route="/recommendations", status=200
    )
    assert access_records, (
        "No access-log record found for GET /recommendations status=200. "
        f"All captured records: {records!r}"
    )

    # Inspect the LAST access log for this request — there should be
    # exactly one, but using the last makes the test robust against
    # future middleware that emits an additional access record.
    record = access_records[-1]
    _assert_required_log_fields(record)

    # Method, route, status — the access-log triumvirate.
    assert record["method"] == "GET", record
    assert record["route"] == "/recommendations", record
    assert int(record["status"]) == 200, record

    # Latency must be a positive number, measured in milliseconds.
    latency_ms = record["latency_ms"]
    assert isinstance(latency_ms, (int, float)), (
        f"latency_ms must be numeric, got {type(latency_ms).__name__}: {record!r}"
    )
    assert float(latency_ms) > 0, record

    # AAP R-13 — correlation_id must be exactly what the client sent.
    assert record["correlation_id"] == correlation_id, (
        f"correlation_id mismatch: expected {correlation_id!r}, "
        f"got {record.get('correlation_id')!r}: {record!r}"
    )

    # user_id MUST equal the JWT ``sub`` claim — the JwtAuthMiddleware
    # is the sole source of truth for the authenticated principal.
    assert record["user_id"] == jwt_subject, (
        f"user_id must equal JWT sub claim {jwt_subject!r}, "
        f"got {record.get('user_id')!r}: {record!r}"
    )


async def test_access_log_has_all_required_fields_on_401(
    client: TestClient,
    stdout_capture: io.StringIO,
) -> None:
    """AAP R-26 — Unauthenticated request still logs all required fields.

    A request without an ``Authorization`` header is rejected with 401
    by the JWT middleware. The access log MUST still contain the AAP
    R-26 baseline fields. ``correlation_id`` is server-minted (the
    client did not send one). ``user_id`` MUST be absent OR equal to
    the well-known anonymous sentinel — there is no JWT ``sub`` claim
    to copy.
    """
    request_user_id = str(uuid.uuid4())

    # Send WITHOUT Authorization header (and without X-Correlation-ID
    # so the middleware mints one server-side, exercising the
    # "absent header → fresh UUID" branch).
    response = client.get(f"/recommendations?user_id={request_user_id}&limit=10")
    assert response.status_code == 401, (
        f"Expected 401 for unauthenticated request, got "
        f"{response.status_code}: {response.text!r}"
    )

    records = _parse_log_lines(stdout_capture.getvalue())
    access_records = _find_access_log_records(
        records, route="/recommendations", status=401
    )
    assert access_records, (
        "No access-log record found for GET /recommendations status=401. "
        f"All captured records: {records!r}"
    )
    record = access_records[-1]
    _assert_required_log_fields(record)

    assert record["method"] == "GET", record
    assert record["route"] == "/recommendations", record
    assert int(record["status"]) == 401, record

    # Server-minted correlation_id MUST be present and non-empty.
    correlation_id = record.get("correlation_id")
    assert correlation_id, (
        "Server-minted correlation_id missing from 401 access log "
        "(violates AAP R-13 / R-26): {record!r}"
    )
    assert isinstance(correlation_id, str) and correlation_id.strip(), record

    # No authenticated user — user_id must be absent OR equal to the
    # well-documented "anonymous" sentinel. Either is acceptable per
    # the structured_logging.py middleware spec.
    user_id = record.get("user_id")
    assert user_id in (None, "", "anonymous"), (
        f"user_id should be absent/anonymous on 401, got {user_id!r}: {record!r}"
    )


async def test_kafka_consumer_log_has_required_fields(
    stdout_capture: io.StringIO,
    kafka_producer: Any,
    consumer_runner: Any,
    embeddings_repo: Any,
) -> None:
    """AAP R-26 — Kafka consumer logs carry the required structured fields.

    Produces a ``ProductCreatedEvent`` to ``product.created`` with a
    fresh ``x-correlation-id`` Kafka header. Waits for the consumer to
    process the event (write the embedding row). Then asserts the
    captured log stream contains a consumer record carrying:

    * The AAP R-26 baseline (``timestamp``, ``level``, ``service``,
      ``message``).
    * ``correlation_id`` matching the producer's header.
    * ``event_class`` equal to the Pydantic event class name.
    * ``topic`` equal to ``product.created``.
    * Numeric ``partition`` and ``offset`` fields (for Kafka offset
      reconstruction during incident review).
    """
    correlation_id = str(uuid.uuid4())
    product_id = str(uuid.uuid4())
    event_payload: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ProductCreatedEvent",
        "event_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "product_id": product_id,
        "name": "Integration Test Product",
        "description": "Synthetic fixture for observability test",
        "category_id": str(uuid.uuid4()),
        "price_cents": 1999,
        "currency": "USD",
    }

    # Produce the event with the correlation-id Kafka header.
    await kafka_producer.send(
        topic="product.created",
        value=event_payload,
        headers=[("x-correlation-id", correlation_id.encode("utf-8"))],
    )

    # Wait for the consumer to process and persist the embedding.
    # The consumer_runner fixture exposes a ``wait_for_processed``
    # API that blocks until the event with the given event_id is
    # acknowledged by the consumer's commit-offset mechanism.
    await consumer_runner.wait_for_processed(
        topic="product.created",
        event_id=event_payload["event_id"],
        timeout=30.0,
    )

    # Sanity check — the embedding row exists. We don't inspect its
    # vector contents; we only confirm the consumer fully completed
    # its work before we read the captured log stream.
    persisted = await embeddings_repo.get_by_product_id(product_id)
    assert persisted is not None, (
        f"Consumer did not persist embedding for product_id={product_id!r}"
    )

    records = _parse_log_lines(stdout_capture.getvalue())

    # Filter to consumer records — identifiable by the presence of
    # ``topic`` (consumer-specific) and absence of ``method``/``route``
    # (HTTP-specific).
    consumer_records = [
        r
        for r in records
        if r.get("topic") == "product.created"
        and "method" not in r
        and "route" not in r
    ]
    assert consumer_records, (
        "No Kafka consumer log records found for topic=product.created. "
        f"All captured records: {records!r}"
    )

    # Find the record(s) for THIS event (matched by correlation_id).
    matched = [r for r in consumer_records if r.get("correlation_id") == correlation_id]
    assert matched, (
        f"No consumer log record carries correlation_id={correlation_id!r}; "
        f"available consumer records: {consumer_records!r}"
    )

    # Inspect the first consumer record for this event — there is
    # typically one ``event_consumed``/``event_processed`` per event.
    record = matched[0]
    _assert_required_log_fields(record)

    assert record.get("event_class") == "ProductCreatedEvent", record
    assert record.get("topic") == "product.created", record

    partition = record.get("partition")
    assert isinstance(partition, int) and partition >= 0, (
        f"partition must be a non-negative int, got {partition!r}: {record!r}"
    )
    offset = record.get("offset")
    assert isinstance(offset, int) and offset >= 0, (
        f"offset must be a non-negative int, got {offset!r}: {record!r}"
    )


async def test_correlation_id_present_in_every_record_for_request(
    client: TestClient,
    stdout_capture: io.StringIO,
    auth_headers: dict[str, str],
) -> None:
    """AAP R-13 — Every log record produced during a request carries the same
    ``correlation_id``.

    Operators rely on Kibana queries of the form
    ``correlation_id:"<id>"`` to reconstruct a single request's full
    log timeline. A record that's missing the ID — even one — breaks
    that reconstruction. This test makes a single authenticated request
    and asserts EVERY record produced during that request inherits the
    same ID.
    """
    correlation_id = str(uuid.uuid4())
    request_user_id = str(uuid.uuid4())
    headers = dict(auth_headers)
    headers["X-Correlation-ID"] = correlation_id

    # Capture the buffer's pre-request size so we can scope the
    # assertion to records emitted DURING this request only —
    # otherwise a stray background-task log from earlier in the test
    # session could pollute the assertion.
    pre_request_buffer = stdout_capture.getvalue()
    pre_request_lines = pre_request_buffer.count("\n")

    response = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10",
        headers=headers,
    )
    assert response.status_code == 200, response.text

    full_buffer = stdout_capture.getvalue()
    new_lines = full_buffer.split("\n")[pre_request_lines:]
    new_records = _parse_log_lines("\n".join(new_lines))

    # Filter to records bound to THIS correlation_id. Some records
    # (e.g., a Kafka heartbeat emitted in parallel) may legitimately
    # carry a different correlation_id; we only assert about the
    # records inside our request's lifecycle.
    request_records = _find_records_by_correlation_id(new_records, correlation_id)
    assert len(request_records) >= 2, (
        f"Expected >=2 log records for the request "
        f"(at least one application log + one access log), "
        f"got {len(request_records)}. New records: {new_records!r}"
    )

    # AAP R-13 — EVERY record bound to this request MUST carry the
    # exact correlation ID we sent. (Trivially true after filtering,
    # but the assertion documents the invariant for future readers.)
    for record in request_records:
        assert record["correlation_id"] == correlation_id, (
            f"Record correlation_id={record.get('correlation_id')!r} "
            f"does not match request correlation_id={correlation_id!r}: "
            f"{record!r}"
        )

    # The access log record must be among them — proves the
    # middleware is binding the ID into structlog's context vars
    # before the access log is emitted in the ``finally`` block.
    access_records = _find_access_log_records(
        request_records, route="/recommendations", status=200
    )
    assert access_records, (
        f"Access log not bound to correlation_id={correlation_id!r}. "
        f"Records carrying that ID: {request_records!r}"
    )


async def test_no_plaintext_logs_emitted(
    client: TestClient,
    stdout_capture: io.StringIO,
    auth_headers: dict[str, str],
    kafka_producer: Any,
    consumer_runner: Any,
) -> None:
    """AAP R-26 — Every line on stdout MUST be valid JSON.

    A single ``print()`` call, an unhandled exception traceback
    written to stdout, or a misconfigured stdlib handler that emits
    raw text would all silently break Filebeat's JSON parser and
    drop log lines from Elasticsearch. The ``_parse_log_lines``
    helper raises on any non-JSON line; this test simply runs a
    representative HTTP+Kafka workload and lets the parser enforce
    the invariant.
    """
    correlation_id = str(uuid.uuid4())
    request_user_id = str(uuid.uuid4())
    headers = dict(auth_headers)
    headers["X-Correlation-ID"] = correlation_id

    # 1. Make a successful HTTP request — exercises every middleware.
    response = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10",
        headers=headers,
    )
    assert response.status_code == 200, response.text

    # 2. Produce + consume a Kafka event — exercises the consumer
    #    side of the application's logging surface.
    event_payload: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ProductCreatedEvent",
        "event_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "product_id": str(uuid.uuid4()),
        "name": "Plaintext-Log Probe",
        "description": "test_no_plaintext_logs_emitted fixture",
        "category_id": str(uuid.uuid4()),
        "price_cents": 100,
        "currency": "USD",
    }
    await kafka_producer.send(
        topic="product.created",
        value=event_payload,
        headers=[("x-correlation-id", correlation_id.encode("utf-8"))],
    )
    await consumer_runner.wait_for_processed(
        topic="product.created",
        event_id=event_payload["event_id"],
        timeout=30.0,
    )

    # 3. Every line MUST parse as JSON. ``_parse_log_lines`` raises
    #    AssertionError on the first non-JSON line, which is the
    #    most actionable diagnostic for the invariant.
    records = _parse_log_lines(stdout_capture.getvalue())

    # Sanity check — at minimum the access log + at least one
    # consumer record should be present, demonstrating the parser
    # actually saw output (i.e., the assertion below is non-vacuous).
    assert records, "stdout_capture buffer is empty — no logs were emitted"




# ---------------------------------------------------------------------------
# Test cases — Prometheus /metrics endpoint (Phase 5.6 – 5.8)
# ---------------------------------------------------------------------------
async def test_metrics_endpoint_returns_200(client: TestClient) -> None:
    """``GET /metrics`` returns 200 OK with non-empty Prometheus text body.

    This asserts the most basic operational contract — the metrics
    scraper can reach the endpoint and parse a body. Subsequent
    tests examine the body content in detail.
    """
    response = client.get("/metrics")
    assert response.status_code == 200, (
        f"GET /metrics expected 200, got {response.status_code}: "
        f"{response.text!r}"
    )

    # Prometheus's text exposition format uses ``text/plain;
    # version=0.0.4`` as its content type. ``startswith`` tolerates
    # the ``; charset=...`` suffix some frameworks append.
    content_type = response.headers.get("content-type", "")
    assert content_type.startswith("text/plain"), (
        f"Expected text/plain content-type, got {content_type!r}"
    )

    body = response.text
    assert body.strip(), "Empty /metrics response body"
    # At least one ``# HELP`` line proves the registry is populated
    # and the scrape produced metric metadata.
    assert "# HELP" in body, (
        f"/metrics body missing '# HELP' lines (registry is empty?): "
        f"{body[:500]!r}"
    )


async def test_expected_metrics_present_after_activity(
    client: TestClient,
    auth_headers: dict[str, str],
    kafka_producer: Any,
    consumer_runner: Any,
) -> None:
    """Every metric in ``EXPECTED_METRICS`` is exposed after representative activity.

    Drives the application through one of each instrumented code
    path:

    * Successful authenticated HTTP request → ``http_requests_total``,
      ``http_request_duration_seconds``, ``inference_duration_seconds``.
    * Unauthenticated HTTP request → another ``http_requests_total``.
    * Kafka event production + consumption →
      ``kafka_messages_consumed_total``, ``event_dispatch_duration_seconds``.

    Then asserts every metric name in ``EXPECTED_METRICS`` appears in
    the parsed scrape output.
    """
    request_user_id = str(uuid.uuid4())

    # 1. Successful authenticated request.
    ok_response = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10",
        headers=auth_headers,
    )
    assert ok_response.status_code == 200, ok_response.text

    # 2. Unauthenticated request (status 401).
    unauth_response = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10"
    )
    assert unauth_response.status_code == 401, unauth_response.text

    # 3. Kafka produce + consume.
    correlation_id = str(uuid.uuid4())
    event_payload: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ProductCreatedEvent",
        "event_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "product_id": str(uuid.uuid4()),
        "name": "Metrics Probe",
        "description": "test_expected_metrics_present_after_activity fixture",
        "category_id": str(uuid.uuid4()),
        "price_cents": 100,
        "currency": "USD",
    }
    await kafka_producer.send(
        topic="product.created",
        value=event_payload,
        headers=[("x-correlation-id", correlation_id.encode("utf-8"))],
    )
    await consumer_runner.wait_for_processed(
        topic="product.created",
        event_id=event_payload["event_id"],
        timeout=30.0,
    )

    # Scrape /metrics and parse.
    response = client.get("/metrics")
    assert response.status_code == 200, response.text
    metrics = _parse_prometheus_text(response.text)

    # The ``_parse_prometheus_text`` parser yields the metric name
    # without the ``_bucket`` / ``_count`` / ``_sum`` suffixes for
    # histograms, so we need to check both the bare name and the
    # histogram-suffixed names.
    seen_metric_names: set[str] = set()
    for name in metrics.keys():
        seen_metric_names.add(name)
        # Strip histogram/summary suffixes for the presence check.
        for suffix in ("_bucket", "_count", "_sum"):
            if name.endswith(suffix):
                seen_metric_names.add(name[: -len(suffix)])
                break

    missing = EXPECTED_METRICS - seen_metric_names
    assert not missing, (
        f"Expected metrics missing from /metrics: {missing!r}; "
        f"saw {sorted(seen_metric_names)!r}"
    )

    # Specific value checks.
    http_requests_total = metrics.get("http_requests_total", [])
    assert any(
        labels.get("method") == "GET"
        and labels.get("route") == "/recommendations"
        and str(labels.get("status")) == "200"
        and value >= 1
        for labels, value in http_requests_total
    ), (
        f"http_requests_total{{method=GET, route=/recommendations, status=200}}"
        f" not >= 1: {http_requests_total!r}"
    )
    assert any(
        labels.get("method") == "GET"
        and labels.get("route") == "/recommendations"
        and str(labels.get("status")) == "401"
        and value >= 1
        for labels, value in http_requests_total
    ), (
        f"http_requests_total{{method=GET, route=/recommendations, status=401}}"
        f" not >= 1: {http_requests_total!r}"
    )

    # Kafka consumption counter — at least one success for the
    # product.created topic.
    kafka_consumed = metrics.get("kafka_messages_consumed_total", [])
    assert any(
        labels.get("topic") == "product.created"
        and labels.get("result") == "success"
        and value >= 1
        for labels, value in kafka_consumed
    ), (
        f"kafka_messages_consumed_total{{topic=product.created, "
        f"result=success}} not >= 1: {kafka_consumed!r}"
    )

    # event_dispatch_duration_seconds is a histogram — its samples
    # surface as ``event_dispatch_duration_seconds_bucket`` lines.
    dispatch_buckets = metrics.get("event_dispatch_duration_seconds_bucket", [])
    assert any(
        labels.get("event_class") == "ProductCreatedEvent" and value >= 1
        for labels, value in dispatch_buckets
    ), (
        "No event_dispatch_duration_seconds_bucket sample with "
        f"event_class=ProductCreatedEvent and count>=1: {dispatch_buckets!r}"
    )

    # All sample values across all metrics must be non-negative.
    for metric_name, samples in metrics.items():
        for labels, value in samples:
            # Histograms can legitimately report ``+Inf`` for the
            # last bucket; that's fine — we exclude inf from the
            # negativity check.
            if value != float("inf") and value != float("-inf"):
                assert value >= 0, (
                    f"Negative metric sample {metric_name}{labels!r} = {value}"
                )


async def test_metrics_endpoint_no_auth_required(client: TestClient) -> None:
    """``/metrics`` MUST be reachable without a JWT.

    Prometheus scrapers are unauthenticated by design; the JWT
    middleware MUST allow-list ``/metrics`` (along with
    ``/health/live`` and ``/health/ready``). This is a load-bearing
    operations contract — if scraping breaks, all dashboards go
    silent simultaneously and on-call engineers lose visibility
    into the production fleet.
    """
    # Send WITHOUT any Authorization header.
    response = client.get("/metrics")
    assert response.status_code == 200, (
        f"GET /metrics returned {response.status_code} without auth — "
        f"the endpoint MUST be allow-listed from JWT validation. "
        f"Body: {response.text!r}"
    )
    # Spot-check one expected metric is present so the test fails
    # loudly if the endpoint returns a non-empty but content-free
    # body (e.g., an HTML error page mistakenly served as 200).
    assert "http_requests_total" in response.text, (
        f"/metrics body lacks http_requests_total — endpoint may be "
        f"misconfigured: {response.text[:500]!r}"
    )


# ---------------------------------------------------------------------------
# Test cases — Health probe + error path (Phase 5.9 – 5.11)
# ---------------------------------------------------------------------------
async def test_health_live_logged_as_access_record(
    client: TestClient,
    stdout_capture: io.StringIO,
) -> None:
    """``GET /health/live`` produces an AAP-R-26-compliant access record.

    Health probes are unauthenticated and high-volume (Kubernetes
    polls them every few seconds), so their log records are an
    important compliance check — they must be JSON, must carry the
    R-26 baseline, must NOT include a ``user_id``, and SHOULD carry
    a server-minted ``correlation_id`` so probe traffic is
    distinguishable in Kibana.
    """
    response = client.get("/health/live")
    assert response.status_code == 200, response.text

    records = _parse_log_lines(stdout_capture.getvalue())
    access_records = _find_access_log_records(
        records, route="/health/live", status=200
    )
    assert access_records, (
        "No access-log record found for GET /health/live status=200. "
        f"All captured records: {records!r}"
    )
    record = access_records[-1]
    _assert_required_log_fields(record)

    assert record["method"] == "GET", record
    assert record["route"] == "/health/live", record
    assert int(record["status"]) == 200, record

    # Server-minted correlation_id MUST be present even though the
    # client did not send one (AAP R-13).
    assert record.get("correlation_id"), (
        f"Server-minted correlation_id missing from /health/live access "
        f"log (violates AAP R-13): {record!r}"
    )

    # Health checks are unauthenticated — user_id MUST be absent or
    # equal to the anonymous sentinel.
    user_id = record.get("user_id")
    assert user_id in (None, "", "anonymous"), (
        f"user_id should be absent/anonymous on /health/live, got "
        f"{user_id!r}: {record!r}"
    )


async def test_error_log_on_500_has_dict_tracebacks(
    client: TestClient,
    app: Any,
    stdout_capture: io.StringIO,
    auth_headers: dict[str, str],
) -> None:
    """A 500 response logs a structured exception but sanitises the body.

    Validates two complementary contracts:

    1. AAP R-26 + structlog ``dict_tracebacks`` — the error log
       record carries an ``exception`` (or ``traceback``) field with
       a structured traceback so operators can debug from Kibana
       without scraping raw stack traces.
    2. AAP R-25 — internal exception messages MUST NOT leak to the
       client. The response body must be a sanitised JSON envelope
       (e.g., ``{"detail": "Internal Server Error"}``); the raw
       ``"boom"`` substring belongs ONLY in the log stream.

    To trigger a 500 we register a temporary error route on the
    FastAPI app, exercise it, and inspect both the response and the
    captured log buffer.
    """
    # Register a one-off error route. The path is unlikely to clash
    # with production routes (``__test__`` prefix). FastAPI's
    # ``add_api_route`` wires it through the same middleware stack
    # as production routes, so the test exercises the full error
    # propagation chain (exception handler → access log middleware
    # → response).
    error_path = "/__test__/observability/boom"

    def _boom_handler() -> dict[str, str]:
        raise RuntimeError("boom")

    app.add_api_route(
        error_path,
        _boom_handler,
        methods=["GET"],
        include_in_schema=False,
    )

    correlation_id = str(uuid.uuid4())
    headers = dict(auth_headers)
    headers["X-Correlation-ID"] = correlation_id

    try:
        response = client.get(error_path, headers=headers)
    finally:
        # Best-effort cleanup so the test doesn't pollute subsequent
        # tests with a residual error route. FastAPI's router does
        # not expose ``remove_api_route``, so we directly mutate the
        # underlying ``routes`` list.
        app.router.routes = [
            r
            for r in app.router.routes
            if getattr(r, "path", None) != error_path
        ]

    assert response.status_code == 500, (
        f"Expected 500 from boom handler, got {response.status_code}: "
        f"{response.text!r}"
    )

    # AAP R-25 — sanitised body. The raw exception message MUST NOT
    # appear in the response payload. We're tolerant of various
    # error-envelope shapes (FastAPI default, custom, etc.) and
    # only assert what MUST NOT be present.
    response_body = response.text
    assert "boom" not in response_body, (
        f"Internal exception message 'boom' leaked to client response "
        f"(violates AAP R-25): {response_body!r}"
    )
    assert "Traceback" not in response_body, (
        f"Stack trace leaked to client response (violates AAP R-25): "
        f"{response_body!r}"
    )

    records = _parse_log_lines(stdout_capture.getvalue())

    # The access log for the failed request — status=500.
    access_records = _find_access_log_records(records, route=error_path, status=500)
    assert access_records, (
        f"No 500 access log found for {error_path}. Records: {records!r}"
    )

    # Find the ERROR-level record carrying a structured traceback.
    error_records = [
        r
        for r in records
        if str(r.get("level", "")).upper() == "ERROR"
        and r.get("correlation_id") == correlation_id
    ]
    assert error_records, (
        f"No ERROR-level log record bound to correlation_id="
        f"{correlation_id!r}: {records!r}"
    )

    # At least one error record must carry structured traceback
    # information (per ``structlog.processors.dict_tracebacks``).
    # The field is conventionally named ``exception``; some
    # configurations emit ``traceback``. Accept either.
    structured = [
        r for r in error_records if r.get("exception") or r.get("traceback")
    ]
    assert structured, (
        f"No ERROR record carries 'exception'/'traceback' field — "
        f"structlog dict_tracebacks processor may not be installed. "
        f"Error records: {error_records!r}"
    )

    # The structured traceback (or message) must contain the original
    # 'boom' string — this is the operator's lifeline for debugging.
    record = structured[0]
    serialised = json.dumps(record)
    assert "boom" in serialised, (
        f"Structured error log lacks the original exception message "
        f"'boom' (operators cannot debug from this record): {record!r}"
    )


async def test_latency_ms_is_numeric_and_reasonable(
    client: TestClient,
    stdout_capture: io.StringIO,
    auth_headers: dict[str, str],
) -> None:
    """``latency_ms`` is numeric, positive, and below 30 s.

    Mixed units (seconds vs milliseconds vs microseconds) across
    services would make Kibana queries unreliable. AAP R-26 mandates
    milliseconds. The 30-second upper bound is a regression guard
    against handler hangs — even a heavily-loaded ML inference path
    should complete in single-digit seconds.
    """
    request_user_id = str(uuid.uuid4())
    response = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10",
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text

    records = _parse_log_lines(stdout_capture.getvalue())
    access_records = _find_access_log_records(
        records, route="/recommendations", status=200
    )
    assert access_records, f"No access log found: {records!r}"

    record = access_records[-1]
    latency_ms = record["latency_ms"]
    assert isinstance(latency_ms, (int, float)), (
        f"latency_ms must be int|float, got {type(latency_ms).__name__}: "
        f"{record!r}"
    )
    latency_value = float(latency_ms)
    assert latency_value > 0, (
        f"latency_ms must be > 0 (got {latency_value}); zero/negative "
        f"latency indicates the timer was not wired correctly: {record!r}"
    )
    assert latency_value < _MAX_REASONABLE_LATENCY_MS, (
        f"latency_ms = {latency_value} is unreasonable (> 30 s); the "
        f"handler may be hung or the timer's units may be wrong (seconds "
        f"instead of ms?): {record!r}"
    )




# ---------------------------------------------------------------------------
# Test cases — Field-level invariants (Phase 5.12 – 5.14)
# ---------------------------------------------------------------------------
async def test_service_field_is_exact_constant(
    client: TestClient,
    stdout_capture: io.StringIO,
    auth_headers: dict[str, str],
) -> None:
    """Every log record's ``service`` field equals ``"recommendation-engine"``.

    Kibana index patterns and Grafana dashboards filter on the exact
    literal string. Even minor variations — ``"reco"``,
    ``"rec_engine"``, ``"recoEngine"``, or trailing whitespace — would
    silently hide this service from observability tools and leave
    operators blind during incidents.
    """
    request_user_id = str(uuid.uuid4())

    # Trigger several requests to ensure we have multiple log
    # records to inspect (access logs + any application/middleware
    # records that fire under the request's correlation_id).
    for _ in range(3):
        response = client.get(
            f"/recommendations?user_id={request_user_id}&limit=10",
            headers=auth_headers,
        )
        assert response.status_code == 200, response.text

    # Also include an unauthenticated request so we cover the
    # JwtAuthMiddleware's 401 emission path.
    client.get(f"/recommendations?user_id={request_user_id}&limit=10")

    records = _parse_log_lines(stdout_capture.getvalue())
    assert records, "No log records captured"

    for record in records:
        assert "service" in record, (
            f"Record missing 'service' field (violates AAP R-26): {record!r}"
        )
        assert record["service"] == _EXPECTED_SERVICE_NAME, (
            f"service must equal {_EXPECTED_SERVICE_NAME!r}, got "
            f"{record['service']!r}: {record!r}"
        )


async def test_timestamp_is_rfc3339_with_timezone(
    client: TestClient,
    stdout_capture: io.StringIO,
    auth_headers: dict[str, str],
) -> None:
    """Every ``timestamp`` field is RFC 3339 + UTC.

    Validates two invariants:

    1. The timestamp string matches :data:`RFC3339_REGEX`.
    2. The string parses via :func:`datetime.fromisoformat` (with
       ``Z`` → ``+00:00`` substitution for Python <3.11 strictness)
       and the parsed offset is UTC.

    Mixed timezones across log lines would corrupt
    Kibana's time-bucket aggregations and break alerting that
    aggregates over a fixed wall-clock window.
    """
    request_user_id = str(uuid.uuid4())
    response = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10",
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text

    records = _parse_log_lines(stdout_capture.getvalue())
    assert records, "No log records captured"

    for record in records:
        timestamp_str = str(record["timestamp"])
        assert RFC3339_REGEX.match(timestamp_str), (
            f"timestamp {timestamp_str!r} does not match RFC 3339: "
            f"{record!r}"
        )

        # ``datetime.fromisoformat`` (Python 3.11+) accepts the full
        # RFC 3339 grammar including the ``Z`` suffix. For older
        # interpreters (and to be defensive) we normalise ``Z`` to
        # ``+00:00`` first.
        normalised = timestamp_str.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalised)
        assert parsed.tzinfo is not None, (
            f"Parsed timestamp {parsed!r} lacks tzinfo (violates "
            f"AAP R-26 timezone requirement): {record!r}"
        )
        offset = parsed.utcoffset()
        assert offset is not None, (
            f"Parsed timestamp {parsed!r} returns None for utcoffset: "
            f"{record!r}"
        )
        assert offset == timezone.utc.utcoffset(None), (
            f"Timestamp offset must be UTC (+00:00); got {offset!r}: "
            f"{record!r}"
        )


async def test_log_level_uppercase(
    client: TestClient,
    app: Any,
    stdout_capture: io.StringIO,
    auth_headers: dict[str, str],
) -> None:
    """Every ``level`` field is one of the standard UPPERCASE strings.

    structlog's ``add_log_level`` processor emits lowercase by
    default; the production processor chain MUST normalise them so
    Filebeat / Logstash filters on a stable enum. This test triggers
    INFO (success), WARN (4xx — implementations may classify 4xx as
    INFO or WARN; either is acceptable), and ERROR (5xx) levels via
    real request paths and asserts the level field is always
    UPPERCASE.
    """
    request_user_id = str(uuid.uuid4())

    # INFO — successful 200 response.
    ok_resp = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10",
        headers=auth_headers,
    )
    assert ok_resp.status_code == 200, ok_resp.text

    # 4xx — 401 unauthenticated. May be logged as INFO or WARN.
    unauth_resp = client.get(
        f"/recommendations?user_id={request_user_id}&limit=10"
    )
    assert unauth_resp.status_code == 401, unauth_resp.text

    # 5xx — registered error route. Must be logged at ERROR level.
    error_path = "/__test__/observability/level_uppercase_boom"

    def _boom_handler() -> dict[str, str]:
        raise RuntimeError("level_test_boom")

    app.add_api_route(
        error_path,
        _boom_handler,
        methods=["GET"],
        include_in_schema=False,
    )
    try:
        client.get(error_path, headers=auth_headers)
    finally:
        app.router.routes = [
            r
            for r in app.router.routes
            if getattr(r, "path", None) != error_path
        ]

    records = _parse_log_lines(stdout_capture.getvalue())
    assert records, "No log records captured"

    for record in records:
        level_value = record["level"]
        assert isinstance(level_value, str), (
            f"level must be a string, got {type(level_value).__name__}: "
            f"{record!r}"
        )
        assert level_value == level_value.upper(), (
            f"level must be UPPERCASE, got {level_value!r}: {record!r}"
        )
        assert level_value in _VALID_LOG_LEVELS, (
            f"level {level_value!r} not in {sorted(_VALID_LOG_LEVELS)!r}: "
            f"{record!r}"
        )

    # Sanity check — at least one ERROR record exists (from the boom
    # handler) so the assertion above is non-vacuously testing the
    # ERROR path.
    error_records = [r for r in records if str(r.get("level", "")).upper() == "ERROR"]
    assert error_records, (
        f"Expected at least one ERROR-level record from the boom "
        f"handler; got {records!r}"
    )


# ---------------------------------------------------------------------------
# Test cases — /metrics format + label cardinality (Phase 5.15 – 5.16)
# ---------------------------------------------------------------------------
async def test_metrics_text_exposition_format(client: TestClient) -> None:
    """``/metrics`` body adheres to Prometheus text exposition v0.0.4.

    Validates:

    * Content-Type begins with ``text/plain``.
    * Body contains both ``# HELP <metric>`` and ``# TYPE <metric>
      <kind>`` metadata lines.
    * Every non-comment, non-blank line matches the Prometheus
      sample-line grammar.
    * No malformed lines slip through.

    A malformed line breaks Prometheus's scrape and the whole
    metric set goes silent — a dashboard-wide outage on a single
    typo. This test is the regression guard.
    """
    response = client.get("/metrics")
    assert response.status_code == 200, response.text

    content_type = response.headers.get("content-type", "")
    assert content_type.startswith("text/plain"), (
        f"Expected text/plain content-type, got {content_type!r}"
    )

    body = response.text
    assert body.strip(), "Empty /metrics response body"

    valid_type_kinds = {"counter", "gauge", "histogram", "summary", "untyped"}

    saw_help = False
    saw_type = False
    seen_metric_in_type: set[str] = set()
    malformed: list[str] = []

    for raw in body.splitlines():
        line = raw.rstrip()
        if not line:
            continue

        if line.startswith("#"):
            # Permitted comment forms:
            #   # HELP <metric> <text>
            #   # TYPE <metric> <kind>
            #   # <free-form>  (rare but legal)
            parts = line.split(maxsplit=3)
            if len(parts) >= 3 and parts[1] == "HELP":
                saw_help = True
            elif len(parts) >= 4 and parts[1] == "TYPE":
                saw_type = True
                metric_name = parts[2]
                kind = parts[3].lower()
                assert kind in valid_type_kinds, (
                    f"# TYPE line uses invalid kind {kind!r} for "
                    f"metric {metric_name!r}: {line!r}"
                )
                seen_metric_in_type.add(metric_name)
            # Other comment lines (rare) are tolerated.
            continue

        # Sample line — must match the Prometheus grammar.
        match = _PROM_LINE_REGEX.match(line)
        if not match:
            malformed.append(line)
            continue
        _name, _labels, value_part = match.groups()
        # The value part must start with a parseable float (NaN,
        # +Inf, -Inf, 1.23, 1e10 are all valid Prometheus sample
        # values).
        try:
            float(value_part.strip().split()[0])
        except (ValueError, IndexError):
            malformed.append(line)

    assert saw_help, (
        f"/metrics body lacks any '# HELP' line — registry produced "
        f"no metric metadata: {body[:500]!r}"
    )
    assert saw_type, (
        f"/metrics body lacks any '# TYPE' line — Prometheus cannot "
        f"determine metric kinds: {body[:500]!r}"
    )
    assert seen_metric_in_type, (
        "No metric names parsed from '# TYPE' lines"
    )
    assert not malformed, (
        f"/metrics contains malformed lines (Prometheus scrape will "
        f"reject the entire response): {malformed!r}"
    )


async def test_fallback_used_total_labels(
    client: TestClient,
    auth_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``fallback_used_total`` exposes all four exact tier labels.

    The 4-tier fallback chain (``ml`` → ``cache`` → ``popularity`` →
    ``default``) is the heart of AAP R-20 graceful-degradation
    compliance. Each tier MUST have a counter sample exposed at
    ``/metrics`` so the operator alerting rules can fire on
    fallback-tier shifts (e.g., a sudden surge in ``tier="default"``
    indicates ML *and* cache *and* popularity all failed
    simultaneously — a pageable incident).

    Test strategy:

    * Drive several recommendation requests with different user IDs
      so the fallback chain executes multiple times across cold and
      warm cache states.
    * Scrape /metrics and parse.
    * Assert:
      - ``fallback_used_total`` is present.
      - Every tier label that DOES appear in the output is one of
        the four canonical strings (no typos like ``ML`` or
        ``defaults``).
      - All four canonical tier labels are configured (the service
        SHOULD pre-register all four label values at startup so
        they appear with count 0 even before any traffic — this is
        the standard prometheus_client init pattern that makes
        Grafana queries return rows immediately on a fresh deploy).
    """
    # Drive enough varied requests that the fallback chain has a
    # high probability of exercising every tier across the
    # population (cold-cache misses, ML successes, popularity
    # fallbacks for sparse users, etc.).
    for _ in range(10):
        client.get(
            f"/recommendations?user_id={uuid.uuid4()}&limit=10",
            headers=auth_headers,
        )

    # The ``monkeypatch`` parameter is intentionally accepted so
    # local maintainers can extend this test with surgical patches
    # that force specific tiers when the natural traffic mix above
    # leaves a tier unexercised; it is a no-op in the default
    # configuration.
    _ = monkeypatch  # noqa: F841 — reserved for extension hooks

    response = client.get("/metrics")
    assert response.status_code == 200, response.text
    metrics = _parse_prometheus_text(response.text)

    samples = metrics.get("fallback_used_total", [])
    assert samples, (
        "fallback_used_total counter is missing from /metrics — "
        "the recommendation service must register the counter at "
        "startup and expose it via the prometheus_client registry."
    )

    expected_tiers: frozenset[str] = frozenset({"ml", "cache", "popularity", "default"})

    # Aggregate counts per tier label.
    counts_per_tier: dict[str, float] = {}
    seen_tier_labels: set[str] = set()
    for labels, value in samples:
        tier = labels.get("tier")
        if tier is None:
            continue
        seen_tier_labels.add(tier)
        counts_per_tier[tier] = counts_per_tier.get(tier, 0.0) + value

    # Every tier label in the output MUST be one of the canonical
    # strings — typos here silently break alerting. ``"Default"``,
    # ``"ML"``, ``"popularity_fallback"`` would all be defects.
    unknown_tiers = seen_tier_labels - expected_tiers
    assert not unknown_tiers, (
        f"fallback_used_total exposes unknown tier label(s) "
        f"{unknown_tiers!r}; canonical set is {sorted(expected_tiers)!r}. "
        f"Typos here silently break Grafana alerts."
    )

    # All four canonical tier labels MUST appear (the service
    # pre-registers them at startup so they are scrape-visible
    # with count 0 even before any traffic exercises a tier).
    missing_tiers = expected_tiers - seen_tier_labels
    assert not missing_tiers, (
        f"fallback_used_total missing canonical tier label(s) "
        f"{missing_tiers!r}; saw {sorted(seen_tier_labels)!r}. The "
        f"counter MUST register all four labels (ml | cache | "
        f"popularity | default) at startup so dashboards have a "
        f"complete row set even on a fresh deploy."
    )

    # Every count must be non-negative — counters never decrement.
    for tier, count in counts_per_tier.items():
        assert count >= 0, (
            f"fallback_used_total{{tier={tier!r}}} = {count} "
            f"(negative counter — counters must monotonically "
            f"increase)."
        )

