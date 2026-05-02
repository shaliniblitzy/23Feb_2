"""Kubernetes-style health probe endpoints for the Product Service.

This module implements the two HTTP probes that every microservice in the
e-commerce monorepo exposes per **AAP R-19** (Liveness AND readiness probes
are MANDATORY; every service must expose ``/health/live`` and
``/health/ready``; service must fail fast on missing critical dependencies
at startup):

* ``GET /health/live``  — process-alive probe; performs ZERO I/O; always
  returns HTTP 200 with body ``{"status": "alive"}``. A failing liveness
  probe causes Kubernetes to RESTART the pod, so this endpoint MUST never
  depend on backing-service state — that's the readiness probe's job.

* ``GET /health/ready`` — readiness probe; concurrently verifies the two
  steady-state dependencies of the Product Service (MongoDB and the Kafka
  producer) and returns HTTP 200 with ``{"status": "ready", "checks":
  [...]}`` when both succeed; HTTP 503 with ``{"status": "not_ready",
  "checks": [...]}`` and a per-dependency breakdown when any probe fails.
  A failing readiness probe causes Kubernetes to remove the pod from the
  Service rotation (NOT restart it) — appropriate behavior for transient
  downstream degradation.

Both routes are mounted UNPREFIXED at the FastAPI application level by
``src.main.create_app()`` via
``app.include_router(health_router, tags=["health"])`` with NO prefix,
so requests hit ``/health/live`` and ``/health/ready`` directly. They are
also explicitly allow-listed by ``src.middleware.jwt_auth.JWTAuthMiddleware``
so Kubernetes probes (which carry no token) can reach them without
authentication.

Architectural distinction from sibling Python services
------------------------------------------------------
Per **AAP R-7** the Product Service uses **MongoDB** (not PostgreSQL) for
its private ``product_db``, so the readiness probe issues
``await mongo_database.command("ping")`` against the motor handle —
NOT a ``SELECT 1`` against a PostgreSQL pool as the relational siblings
(``auth-service``, ``user-service``, ``inventory-service``,
``order-service``, ``payment-service``, ``notification-service``) do.

Per **AAP Section 0.4.2** the Product Service is a **pure event producer**
with NO Kafka consumer and NO scheduled background work. Consequently this
readiness probe verifies ONLY ``kafka_producer.list_topics(...)`` — there is
no consumer assignment to interrogate. This is the most important runtime
distinction from sibling Python services (``order-service``,
``notification-service``, ``inventory-service``, ``recommendation-engine``)
which all also probe Kafka consumer assignment / lag.

Probe set (intentionally minimal)
---------------------------------
1. ``mongodb`` — admin ``ping`` command via the motor database handle that
   ``src.repository.*`` modules use, so the probe exercises the same
   driver path as production traffic.
2. ``kafka_producer`` — synchronous ``list_topics(timeout=...)`` call on
   the confluent-kafka producer, offloaded to the default thread pool via
   ``asyncio.to_thread`` so it does not block the event loop.

Schema Registry, JWKS, and Auth Service are intentionally NOT probed —
JWKS is primed at startup by ``src.container.build_container.prime`` and
breaker-protected at runtime; readiness is about steady-state operability,
not key freshness. Schema Registry round trips happen on every produce so a
broken registry would surface as Kafka producer failures within seconds.

Compliance notes
----------------
* **AAP R-19** — Both probes are exposed; readiness reports degraded state
  with HTTP 503 and a structured per-check breakdown.
* **AAP R-7**  — MongoDB ping (not SQL) is the database probe.
* **AAP R-13** — Correlation-ID propagation is automatic: the structlog
  ``merge_contextvars`` processor (configured by
  ``src.observability.logging_config.configure_logging``) attaches the
  ``correlation_id`` set by ``CorrelationIdMiddleware`` to every log line
  emitted from this controller.
* **AAP R-25** — Error payloads NEVER contain connection strings,
  credentials, or message text. The ``error`` field on
  :class:`ReadinessCheck` is populated EXCLUSIVELY from
  ``exc.__class__.__name__`` (e.g. ``"ServerSelectionTimeoutError"``,
  ``"KafkaException"``); ``str(exc)`` and ``repr(exc)`` are deliberately
  forbidden because some driver exception messages embed connection
  string fragments (host, port, user).
* **AAP R-26** — Logger is obtained via ``structlog.get_logger(...)``.
  All log events use the snake_case dotted naming convention
  ``health.readiness.<event>`` and pass structured fields as kwargs
  (no f-strings) so the structlog processor pipeline can serialize
  them as JSON.

Cross-references
----------------
* Mounted by ``services/product-service/src/main.py`` via
  ``app.include_router(health_router, tags=["health"])`` UNPREFIXED.
* Allow-listed by ``services/product-service/src/middleware/jwt_auth.py``
  (paths ``/health/live`` and ``/health/ready`` are explicitly skipped
  from JWT validation).
* Imports from container (duck-typed only):
  ``request.app.state.container.mongo_database`` and
  ``request.app.state.container.kafka_producer``. The controller has
  ZERO ``motor.motor_asyncio.AsyncIOMotorDatabase`` and ZERO
  ``confluent_kafka.Producer`` import dependency, keeping the HTTP
  layer loosely coupled to the container's internal driver choice.
* Coordinates with
  ``services/product-service/src/observability/logging_config.py`` —
  provides the structlog processor chain (with ``merge_contextvars``)
  that this controller's logger uses.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------
# ``asyncio`` provides the async runtime primitives used throughout the
# readiness probe handler:
#   * ``asyncio.wait_for(...)`` bounds each per-probe duration via the
#     module-level timeout constants and the outer handler runtime via
#     ``READINESS_PROBE_TIMEOUT_SECONDS`` so a single hung dependency
#     cannot starve the others (AAP R-19 bounded-budget guarantee).
#   * ``asyncio.gather(...)`` runs the MongoDB and Kafka producer probes
#     concurrently so the worst-case readiness latency is the slower
#     probe's latency, not the sum.
#   * ``asyncio.to_thread(...)`` offloads the synchronous
#     ``confluent_kafka.Producer.list_topics(timeout=...)`` call to the
#     default thread pool so it does not block the event loop.
#   * ``asyncio.TimeoutError`` is caught explicitly as a defense-in-depth
#     safeguard if the outer ``wait_for`` fires before either inner
#     probe returns.
import asyncio

# ``Any`` is used as the parameter annotation on the duck-typed probe
# helpers ``_probe_mongo(mongo_database: Any)`` and
# ``_probe_kafka_producer(kafka_producer: Any)``. Typing as ``Any`` is
# deliberate: it ensures the controller has zero
# ``motor.motor_asyncio.AsyncIOMotorDatabase`` and zero
# ``confluent_kafka.Producer`` import dependency, keeping it loosely
# coupled to the container's internal driver choice. Duck-typing on the
# ``.command("ping")`` and ``.list_topics(timeout=...)`` attributes is
# sufficient for the readiness probe contract.
from typing import Any

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
# ``structlog`` powers the AAP R-26 structured JSON log lines emitted on
# any probe failure. The module-level logger is bound once at import time;
# the structlog ``merge_contextvars`` processor (configured at lifespan
# startup by ``src.observability.logging_config.configure_logging``)
# automatically attaches each request's correlation_id (set by
# ``CorrelationIdMiddleware``) to every log record per AAP R-13/R-26.
import structlog

# FastAPI primitives:
#   * ``APIRouter`` is the building block for the two health routes;
#     instantiated unprefixed (``router = APIRouter()``) so ``main.py``
#     can mount it without an additional prefix.
#   * ``Request`` is the parameter type on ``readiness(request: Request)``
#     so the handler can defensively access the DI container via
#     ``getattr(request.app.state, "container", None)`` — returns 503
#     ``ContainerNotInitialized`` during the cold-start race where lifespan
#     startup is still in progress (Kubernetes readiness probes hit this
#     regularly during pod startup).
#   * ``status`` provides the canonical HTTP status-code constants used by
#     both the route decorator declarations and the dynamic
#     ``JSONResponse(status_code=...)`` selection.
from fastapi import APIRouter, Request, status

# ``JSONResponse`` is returned DIRECTLY from the readiness handler — rather
# than the declared ``ReadinessResponse`` Pydantic model — so the handler
# can dynamically choose 200 vs. 503 based on probe outcomes. FastAPI's
# auto-200 path with ``response_model=...`` does not support per-call
# status code control; the ``response_model=ReadinessResponse`` decorator
# declaration remains for OpenAPI doc accuracy. The body is built from a
# ``ReadinessResponse`` instance via ``.model_dump()`` so Pydantic
# validation and serialization still apply.
from fastapi.responses import JSONResponse

# Pydantic v2 primitives for the three response body models:
#   * ``BaseModel`` is the base class for ``LivenessResponse``,
#     ``ReadinessCheck``, and ``ReadinessResponse``.
#   * ``ConfigDict(frozen=True, extra="forbid")`` enforces immutability
#     after construction and rejects unknown fields — guards against
#     accidental schema drift across handler invocations.
#   * ``Field(...)`` declares per-field metadata: defaults, descriptions
#     surfaced in the auto-generated OpenAPI document, and the explicit
#     R-25 contract documented at the schema layer for the ``error`` field.
from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
# Logger name follows the parent folder spec convention
# ``product_service.<package>.<module>``. The structlog lazy-proxy means
# changing the global processor chain in ``src.main.lifespan`` still
# affects log lines emitted afterward; do NOT re-bind inside handlers —
# structlog supports per-call ``contextvars`` and ``bind`` for
# request-scoped enrichment via the correlation-ID middleware.
logger = structlog.get_logger("product_service.controllers.health")


# ---------------------------------------------------------------------------
# Module-level constants — bounded probe timeouts (seconds)
# ---------------------------------------------------------------------------
# The OUTER ``asyncio.wait_for`` ensures the overall readiness handler
# completes within budget even if a synchronous library call hangs (e.g.,
# ``kafka_producer.list_topics`` on a broken socket). The per-probe inner
# timeouts (``MONGO_PROBE_TIMEOUT_SECONDS``, ``KAFKA_PROBE_TIMEOUT_SECONDS``)
# are slightly less than the top-level handler budget so a single hanging
# probe does not starve the other.
#
# Tuning rationale (AAP R-19 bounded-budget guarantee): MongoDB ping and
# Kafka cluster-metadata fetches are O(1) operations that should return in
# well under 100ms in a healthy cluster; 2.0s allows generous slack for
# transient network jitter while staying well under Kubernetes's default
# probe ``timeoutSeconds`` of 10s.
READINESS_PROBE_TIMEOUT_SECONDS: float = 2.5
MONGO_PROBE_TIMEOUT_SECONDS: float = 2.0
KAFKA_PROBE_TIMEOUT_SECONDS: float = 2.0


# ---------------------------------------------------------------------------
# Pydantic v2 response models
# ---------------------------------------------------------------------------


class LivenessResponse(BaseModel):
    """Body returned by ``GET /health/live``.

    Always returned with HTTP 200. The shape is deliberately minimal —
    a single ``status`` field with the literal value ``"alive"``.
    Liveness probes intentionally carry no dependency information; they
    answer only the question "is the ASGI process running?" and
    Kubernetes uses a non-200 response to RESTART the pod. We never want
    a transient external dependency outage to cause a pod restart loop,
    hence the absence of any check beyond the bare process-alive signal.
    """

    # ``frozen=True`` makes the model hashable and immutable — the
    # liveness payload is a constant value-type. ``extra="forbid"``
    # rejects any attempt to add fields outside the declared schema
    # (defensive serialization posture for an externally-visible probe
    # contract).
    model_config = ConfigDict(frozen=True, extra="forbid")

    # The default of ``"alive"`` lets the handler return
    # ``LivenessResponse()`` without arguments while still flowing the
    # field through Pydantic validation.
    status: str = Field(
        default="alive",
        description="Always 'alive' for a running process.",
    )


class ReadinessCheck(BaseModel):
    """Per-dependency readiness check result.

    Each :class:`ReadinessResponse` carries a list of these — one per
    probed dependency. The structure is intentionally narrow so the JSON
    body remains operator-friendly and parser-stable.
    """

    # Same immutability + strictness posture as :class:`LivenessResponse`.
    # The model is constructed exactly once per probe and never mutated
    # after handing back to the readiness aggregator.
    model_config = ConfigDict(frozen=True, extra="forbid")

    # Stable identifier used by monitoring rules and dashboards. Currently
    # one of ``"mongodb"``, ``"kafka_producer"``, or ``"container"`` (the
    # last only when the cold-start race window catches a probe before
    # ``lifespan`` startup attaches the container).
    name: str = Field(
        description="Logical dependency name, e.g., 'mongodb', 'kafka_producer'.",
    )

    # Binary verdict: ``True`` iff the probe returned within budget AND
    # without raising. Any exception, timeout, or driver-reported error
    # produces ``False``.
    healthy: bool = Field(
        description="True iff the dependency answered the probe successfully.",
    )

    # Exception class name when the probe failed. Populated EXCLUSIVELY
    # from ``exc.__class__.__name__`` per AAP R-25 — never from
    # ``str(exc)`` or ``repr(exc)`` because some driver exception
    # messages embed connection string fragments (host, port, user) that
    # would constitute a credential leak.
    error: str | None = Field(
        default=None,
        description=(
            "Exception class name when not healthy. NEVER includes "
            "connection strings, credentials, or message text per AAP R-25."
        ),
    )


class ReadinessResponse(BaseModel):
    """Body returned by ``GET /health/ready``.

    HTTP status 200 when all checks succeed; 503 when any check fails.
    The body always enumerates every probed dependency's verdict so
    operators can tell which one is down without scraping logs. This
    dual-channel design (HTTP status code AND per-check breakdown) lets
    Kubernetes / load balancers route on the status code while operators
    debug interactively via the JSON ``checks`` array.
    """

    # Same immutability + strictness posture as :class:`LivenessResponse`.
    model_config = ConfigDict(frozen=True, extra="forbid")

    # Two-state string verdict: ``"ready"`` (all probes succeeded) or
    # ``"not_ready"`` (one or more failed). The handler always
    # constructs this with an explicit value derived from the per-check
    # ``healthy`` aggregate.
    status: str = Field(
        description="'ready' when all checks pass; 'not_ready' otherwise.",
    )

    # Per-dependency verdict list. Always ordered MongoDB then Kafka
    # producer for steady stable-key positioning that monitoring tools
    # can pivot on.
    checks: list[ReadinessCheck] = Field(
        description="Per-dependency probe results.",
    )


# ---------------------------------------------------------------------------
# Per-dependency probe helpers
# ---------------------------------------------------------------------------
# Each probe is DEFENSIVE: it catches its own exceptions, reports
# ``healthy=False`` with the exception class name (NEVER the message), and
# returns a :class:`ReadinessCheck`. The probes are wrapped in
# ``asyncio.wait_for(...)`` to bound their duration. Probe helpers MUST
# never re-raise — the ``except Exception`` blocks are intentional and
# silenced via the BLE001 noqa marker because the readiness contract
# requires graceful degradation rather than handler crashes.


async def _probe_mongo(mongo_database: Any) -> ReadinessCheck:
    """Probe MongoDB cluster reachability via the canonical 'ping' command.

    Uses ``mongo_database.command("ping")`` which routes to the database's
    backing client. Bounded by :data:`MONGO_PROBE_TIMEOUT_SECONDS` to
    prevent a hung socket from blocking readiness. We deliberately access
    the database handle (rather than ``mongo_client.admin``) because it is
    the same handle that ``src.repository.*`` modules use, so a
    successful ping confirms the production code path's connectivity.

    Args:
        mongo_database: The container's ``mongo_database`` attribute (an
            ``AsyncIOMotorDatabase`` from motor). Typed as :data:`Any`
            so the controller has zero motor import dependency; the
            handle is duck-typed on its ``command(...)`` coroutine.

    Returns:
        :class:`ReadinessCheck` with ``name='mongodb'`` and
        ``healthy=True`` iff ping succeeded; ``healthy=False`` with
        ``error=<exception class name>`` otherwise.
    """
    try:
        # Bounded wait_for so a hung socket cannot starve the readiness
        # handler. The motor driver's command coroutine respects
        # cancellation via the asyncio event loop, so the wait_for
        # CancelledError will surface a TimeoutError to the caller —
        # which we catch below as a generic Exception.
        await asyncio.wait_for(
            mongo_database.command("ping"),
            timeout=MONGO_PROBE_TIMEOUT_SECONDS,
        )
        return ReadinessCheck(name="mongodb", healthy=True, error=None)
    except Exception as exc:  # noqa: BLE001
        # Defensive probe, must never re-raise.
        # AAP R-25: log and return ONLY the exception class name. Some
        # driver exceptions (notably motor / pymongo
        # ``ServerSelectionTimeoutError``) embed connection string
        # fragments in their ``str()`` representation; using the class
        # name alone keeps the response payload and log line clean while
        # still being informative enough for operator triage.
        logger.warning(
            "health.readiness.mongo_probe_failed",
            error_class=exc.__class__.__name__,
        )
        return ReadinessCheck(
            name="mongodb",
            healthy=False,
            error=exc.__class__.__name__,
        )


async def _probe_kafka_producer(kafka_producer: Any) -> ReadinessCheck:
    """Probe Kafka producer broker reachability via cluster metadata fetch.

    Uses ``confluent_kafka.Producer.list_topics(timeout=...)`` which is a
    SYNCHRONOUS call. We bounce it off the default thread pool via
    :func:`asyncio.to_thread` and bound the overall await with
    :data:`KAFKA_PROBE_TIMEOUT_SECONDS`. The driver-level timeout on the
    ``list_topics`` call (1.5 seconds) is intentionally smaller than the
    asyncio wrapper's timeout so a healthy driver returns within budget
    even when the asyncio scheduler is mildly contended; the asyncio
    wrapper provides defense in depth against a wedged librdkafka thread.

    Args:
        kafka_producer: The container's ``kafka_producer`` attribute (a
            ``confluent_kafka.Producer``). Typed as :data:`Any` so the
            controller has zero confluent-kafka import dependency; the
            producer is duck-typed on its synchronous
            ``list_topics(timeout=...)`` method.

    Returns:
        :class:`ReadinessCheck` with ``name='kafka_producer'`` and
        ``healthy=True`` iff the metadata call returned within budget;
        ``healthy=False`` with ``error=<exception class name>`` otherwise.
    """
    try:
        # Two-layer timeout: the inner ``timeout=1.5`` is the driver's
        # native timeout (passed straight through to librdkafka); the
        # outer ``asyncio.wait_for(..., timeout=KAFKA_PROBE_TIMEOUT_SECONDS)``
        # protects the event loop in the case where the driver thread
        # never returns (e.g., a deadlocked broker socket). The
        # ``asyncio.to_thread`` shim offloads the synchronous
        # ``list_topics`` call to the default executor so the event loop
        # stays responsive for other readiness handler concurrency.
        await asyncio.wait_for(
            asyncio.to_thread(kafka_producer.list_topics, timeout=1.5),
            timeout=KAFKA_PROBE_TIMEOUT_SECONDS,
        )
        return ReadinessCheck(name="kafka_producer", healthy=True, error=None)
    except Exception as exc:  # noqa: BLE001
        # Defensive probe, must never re-raise.
        # AAP R-25: same rationale as the MongoDB probe — only the
        # exception class name is surfaced. confluent-kafka's
        # ``KafkaException`` and ``KafkaError`` types render their
        # internal messages with rich detail (broker addresses, etc.)
        # which we MUST NOT leak.
        logger.warning(
            "health.readiness.kafka_probe_failed",
            error_class=exc.__class__.__name__,
        )
        return ReadinessCheck(
            name="kafka_producer",
            healthy=False,
            error=exc.__class__.__name__,
        )


# ---------------------------------------------------------------------------
# Router and route handlers
# ---------------------------------------------------------------------------
# The router is unprefixed — ``src.main.create_app()`` mounts it via
# ``app.include_router(health_router, tags=["health"])`` with NO prefix so
# probes hit ``/health/live`` and ``/health/ready`` directly at the app
# root. The ``tags`` argument applied at mount time groups both routes
# under "health" in the generated OpenAPI document.
router = APIRouter()


@router.get(
    "/health/live",
    response_model=LivenessResponse,
    status_code=status.HTTP_200_OK,
    summary="Liveness probe",
    description=(
        "Returns HTTP 200 unconditionally to indicate the process is alive. "
        "Performs ZERO I/O so it cannot be made to fail by transient backing-"
        "service degradation. AAP R-19."
    ),
    responses={status.HTTP_200_OK: {"model": LivenessResponse}},
)
async def liveness() -> LivenessResponse:
    """Liveness probe — always returns 200 with ``{"status": "alive"}``.

    Performs ZERO I/O: no container access, no awaits, no network calls.
    Liveness answers exactly one question — "is this process alive enough
    to respond to HTTP?" — and a non-200 response causes Kubernetes to
    RESTART the pod. Adding ANY dependency check would risk a restart
    storm during transient downstream outages, defeating the probe's
    purpose. Per AAP R-19, liveness MUST never report unhealthy due to
    transient downstream failures (that's readiness's job).
    """
    # Construct via the default ``status="alive"`` field; explicit value
    # is supplied for clarity at the call site even though it would be
    # filled in automatically by the Pydantic field default.
    return LivenessResponse(status="alive")


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    summary="Readiness probe",
    description=(
        "Concurrently probes MongoDB (ping) and Kafka producer (metadata). "
        "Returns HTTP 200 with status='ready' iff both probes succeed; "
        "HTTP 503 with status='not_ready' and per-check breakdown when any "
        "probe fails. AAP R-19."
    ),
    responses={
        status.HTTP_200_OK: {"model": ReadinessResponse},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse},
    },
)
async def readiness(request: Request) -> JSONResponse:
    """Readiness probe — verifies MongoDB + Kafka producer concurrently.

    This handler returns a :class:`fastapi.responses.JSONResponse`
    DIRECTLY (rather than the declared :class:`ReadinessResponse`
    Pydantic type) because we need to control the HTTP status code
    dynamically per probe outcome:

    * 200 when all dependencies are healthy.
    * 503 when any dependency is unhealthy.

    FastAPI's ``response_model=`` declaration above is for OpenAPI
    documentation accuracy; the actual response is built from a
    :class:`ReadinessResponse` instance via ``.model_dump()`` so Pydantic
    validation and serialization still apply.

    Args:
        request: The incoming FastAPI request whose ``app.state.container``
            attribute is checked for the long-lived ``mongo_database`` and
            ``kafka_producer`` resources.

    Returns:
        :class:`JSONResponse` with status 200 and body
        ``{"status": "ready", "checks": [...]}`` when all probes succeed;
        status 503 and body ``{"status": "not_ready", "checks": [...]}``
        when any probe fails or the container is not yet attached.
    """
    # Defensive container access — during a cold-start race the container
    # may not yet be attached to ``app.state``. In that case, return 503
    # with a clear ``ContainerNotInitialized`` check so the Kubernetes
    # readiness probe stays 503 and traffic is not routed to the pod.
    # Kubernetes readiness probes hit this regularly during pod startup;
    # the ``getattr(..., None)`` pattern avoids ``AttributeError`` from
    # inside the request lifecycle when ``app.state.container`` is
    # absent.
    container = getattr(request.app.state, "container", None)
    if container is None:
        body = ReadinessResponse(
            status="not_ready",
            checks=[
                ReadinessCheck(
                    name="container",
                    healthy=False,
                    error="ContainerNotInitialized",
                )
            ],
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=body.model_dump(),
        )

    # Run probes concurrently — AAP R-19's bounded-budget guarantee
    # depends on both probes being fired at the same time rather than
    # serialized. ``asyncio.gather(...)`` propagates the FIRST exception
    # from a child task, but our probe helpers swallow their own
    # exceptions and return :class:`ReadinessCheck` records, so
    # ``gather`` will always complete with a list of two
    # :class:`ReadinessCheck` instances unless the OUTER ``wait_for``
    # cancels them en masse via the ``asyncio.TimeoutError`` path
    # handled below.
    try:
        mongo_check, kafka_check = await asyncio.wait_for(
            asyncio.gather(
                _probe_mongo(container.mongo_database),
                _probe_kafka_producer(container.kafka_producer),
            ),
            timeout=READINESS_PROBE_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        # If the OUTER timeout fires before either probe returns, we
        # report both as unhealthy with a TimeoutError class name. The
        # individual probes' inner timeouts should normally fire first;
        # this is a defense-in-depth safeguard against a probe helper
        # that itself hangs waiting for a synchronous library call.
        logger.warning(
            "health.readiness.outer_timeout",
            timeout_seconds=READINESS_PROBE_TIMEOUT_SECONDS,
        )
        body = ReadinessResponse(
            status="not_ready",
            checks=[
                ReadinessCheck(name="mongodb", healthy=False, error="TimeoutError"),
                ReadinessCheck(
                    name="kafka_producer",
                    healthy=False,
                    error="TimeoutError",
                ),
            ],
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=body.model_dump(),
        )

    # Aggregate the per-check verdicts into the overall response. Order
    # is stable (mongodb, kafka_producer) so monitoring tooling can pivot
    # on positional access if needed; ``checks[0]`` always refers to
    # MongoDB and ``checks[1]`` to the Kafka producer.
    checks = [mongo_check, kafka_check]
    all_healthy = all(check.healthy for check in checks)

    body = ReadinessResponse(
        status="ready" if all_healthy else "not_ready",
        checks=checks,
    )

    if not all_healthy:
        # Log a single structured event so operators can see exactly
        # which dependency failed without parsing the JSON body. The
        # structlog ``merge_contextvars`` processor automatically merges
        # ``correlation_id`` from contextvars (set by
        # :class:`CorrelationIdMiddleware`) onto every log line per
        # AAP R-13/R-26.
        logger.warning(
            "health.readiness.unhealthy",
            unhealthy_checks=[check.name for check in checks if not check.healthy],
        )

    # Dynamic status code: 200 on success, 503 on any failure. The body
    # carries the same per-check breakdown in either case so operators
    # debugging via curl see a consistent payload shape regardless of
    # status. ``model_dump()`` serializes the Pydantic instance to a
    # plain dict for the JSONResponse body so FastAPI's default JSON
    # encoder handles the response identically to the auto-generated
    # path used when ``response_model`` is honored directly.
    return JSONResponse(
        status_code=(
            status.HTTP_200_OK if all_healthy else status.HTTP_503_SERVICE_UNAVAILABLE
        ),
        content=body.model_dump(),
    )


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------
# Only ``router`` is the public export per the file schema. Pydantic
# response models, probe helpers, and timeout constants are module-private
# implementation details — consumers (notably ``src.main.create_app()``)
# only need the router instance for ``app.include_router(...)``.
__all__: list[str] = ["router"]
