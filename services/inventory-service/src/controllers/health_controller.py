"""Liveness and readiness probes for the Inventory Service (AAP R-19).

Two endpoints are exposed:

* ``GET /health/live`` — Returns 200 when the process is alive. Performs
  zero I/O. Suitable as the Kubernetes ``livenessProbe`` target. The
  k8s liveness probe will restart the pod if this endpoint stops responding.

* ``GET /health/ready`` — Returns 200 only when ALL critical dependencies
  are reachable: PostgreSQL (via ``SELECT 1``), Apache Kafka (via
  ``AdminClient.list_topics`` with bounded timeout), Confluent Schema
  Registry (via ``get_subjects()``). Returns 503 with a structured error
  payload when any dependency check fails. Suitable as the Kubernetes
  ``readinessProbe`` target. Failing readiness pulls the pod out of the
  Service backend pool but does NOT restart the container.

The router is mounted with NO additional prefix in ``src.main``; the prefix
``/health`` is contributed by ``APIRouter(prefix="/health", ...)`` declared
at module scope below. The resulting absolute URLs are exactly
``/health/live`` and ``/health/ready`` — matching the
``JWTAuthMiddleware`` ``allow_paths`` whitelist verbatim so probes succeed
without a JWT (per AAP Section 0.4.5).

Design discipline
-----------------
* Per-dependency isolation — each check is wrapped in its OWN try/except
  so a Schema Registry failure cannot mask a healthy Postgres in the
  response. Operators triage outages off the ``checks`` dictionary.
* Bounded timeouts — every check is wrapped in :func:`asyncio.wait_for`
  with a hard 2.0-second ceiling so a hung dependency cannot stall the
  FastAPI worker indefinitely. Without this, ``list_topics`` against an
  unreachable broker would hang and turn the readiness probe itself into
  a denial-of-service vector against the orchestrator.
* Synchronous offload — ``confluent-kafka`` ``AdminClient`` and
  ``SchemaRegistryClient`` are blocking C-extension wrappers. Calling
  them directly on the event loop would block all other coroutines on
  this worker; we route them through :func:`asyncio.to_thread` so the
  event loop stays responsive.
* Exception class names only — error payloads include
  ``exc.__class__.__name__`` and never ``str(exc)``. The latter
  frequently embeds connection strings (with passwords) and internal
  hostnames in libraries like ``psycopg`` and ``confluent-kafka``,
  which would be a violation of AAP R-25 (no secrets in source or in
  publicly-scrapable surfaces).
* No HTTPException — readiness uses an explicit
  :class:`~fastapi.responses.ORJSONResponse` with a runtime-determined
  status code (200 vs. 503). Raising would yield a generic 500 and lose
  the structured ``checks`` payload that operators depend on.

AAP cross-references
--------------------
* **R-19** — Health probes are MANDATORY for orchestrator gating; the
  service must fail fast on missing critical dependencies at startup
  and continue to advertise readiness state at runtime.
* **R-25** — Secrets must never appear in source or in publicly visible
  surfaces; this controller scrubs exception messages from the response.
* **R-26** — Structured access logs are emitted by
  ``StructuredLoggingMiddleware`` for every request; this controller
  does NOT log per-request directly. The module-level :data:`_log` is
  reserved for unexpected internal anomalies.
* **Section 0.4.5** — Middleware allow-listing of ``/health/live`` and
  ``/health/ready`` exempts probes from the JWT auth middleware.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Request, status
from fastapi.responses import ORJSONResponse

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
# This logger is intentionally used sparingly. Per AAP R-26 the
# ``StructuredLoggingMiddleware`` (configured in ``src.main``) already emits
# a structured JSON access log for every request — including the health
# probes — so emitting an additional log line per request from this
# controller would only produce noise. The logger remains available for
# unexpected internal anomalies that are NOT covered by the request-scoped
# access log (e.g., truly unforeseen branches).
_log = logging.getLogger("inventory_service.controllers.health")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
# The router declares its OWN prefix ``/health`` so ``src.main`` may include
# it with no additional prefix:
#
#     app.include_router(health_router, tags=["health"])
#
# The resulting absolute paths are ``/health/live`` and ``/health/ready``,
# matching the ``JWTAuthMiddleware.allow_paths`` whitelist verbatim. Adding
# a second prefix at inclusion time would yield ``/health/health/live`` and
# break orchestrator probing — see Phase 7 of the implementation prompt.
router = APIRouter(prefix="/health", tags=["health"])


# ---------------------------------------------------------------------------
# Bounded-timeout constants
# ---------------------------------------------------------------------------
# Outer wait-for ceiling per dependency check. confluent-kafka
# AdminClient.list_topics carries its own 1.0-second internal timeout, but
# wait_for(2.0) provides a hard ceiling that protects against any pathology
# (DNS resolution stalls, TLS handshake hangs, library bugs) where the
# inner timeout fails to fire. Two seconds is the platform-wide standard
# (see docs/architecture/resilience-patterns.md): long enough to absorb
# transient blips, short enough to keep the readiness probe itself
# responsive under sustained dependency outages.
_OUTER_CHECK_TIMEOUT_S: float = 2.0

# Inner timeout passed to confluent-kafka AdminClient.list_topics. The
# library accepts a positional ``timeout`` argument expressed in seconds;
# 1.0 keeps the synchronous portion well under the outer 2.0-second
# ceiling so the outer wait_for never has to forcibly cancel a thread.
_KAFKA_INNER_TIMEOUT_S: float = 1.0


# ---------------------------------------------------------------------------
# Liveness probe
# ---------------------------------------------------------------------------
@router.get(
    "/live",
    status_code=status.HTTP_200_OK,
    summary="Liveness probe",
    description=(
        "Returns 200 OK if the Python process is alive. Performs zero I/O."
        " Used by Kubernetes ``livenessProbe`` to decide pod restarts."
    ),
)
async def live() -> dict[str, str]:
    """Liveness probe — process-alive check (no I/O).

    The Kubernetes livenessProbe consults this endpoint to decide whether
    to restart the container. A failing liveness probe causes the pod
    to be killed and recreated; therefore liveness MUST be a pure,
    near-zero-cost check that succeeds whenever the FastAPI worker is
    capable of routing a request. Any I/O here would risk killing pods
    that are merely waiting on a flaky downstream — that responsibility
    belongs to the readiness probe (:func:`ready`) which only removes
    the pod from the load-balancer pool.

    Returns:
        A minimal JSON envelope ``{"status": "alive"}``. FastAPI
        serializes this dict to JSON and returns it with HTTP 200.
    """
    return {"status": "alive"}


# ---------------------------------------------------------------------------
# Readiness probe
# ---------------------------------------------------------------------------
@router.get(
    "/ready",
    summary="Readiness probe",
    description=(
        "Returns 200 OK when ALL critical dependencies are reachable: "
        "PostgreSQL, Kafka, Schema Registry. Returns 503 Service "
        "Unavailable with per-check details when any dependency fails."
    ),
    responses={
        200: {"description": "All dependencies healthy"},
        503: {"description": "One or more dependencies unreachable"},
    },
)
async def ready(request: Request) -> ORJSONResponse:
    """Readiness probe — verifies Postgres, Kafka, and Schema Registry.

    This handler is the runtime arbiter of whether the Inventory Service
    can correctly serve traffic. It executes three independent dependency
    checks against the singletons resolved on ``request.app.state.container``:

    1. **PostgreSQL** — acquires a pooled connection and executes
       ``SELECT 1;``. Verifies that ``inventory_db`` is reachable, that
       the connection pool has capacity, and that the database accepts
       queries.
    2. **Apache Kafka** — invokes ``AdminClient.list_topics(timeout=1.0)``
       in a worker thread. Verifies broker reachability and the
       authentication / authorization configuration of the admin client.
    3. **Confluent Schema Registry** — invokes ``get_subjects()`` in a
       worker thread. Verifies that the registry endpoint is reachable
       and that the credentials are accepted; schema validation on the
       producer/consumer paths depends on this client per AAP R-14.

    Each check is wrapped in its own try/except so a single dependency
    failure does NOT short-circuit the others — the operator-facing
    ``checks`` dictionary always covers all three dependencies, allowing
    triage to identify which is unhealthy. Each check is also wrapped
    in :func:`asyncio.wait_for` with a hard 2.0-second ceiling so a hung
    dependency cannot stall the FastAPI worker indefinitely.

    Args:
        request: The inbound :class:`~fastapi.Request`. Used solely to
            access ``request.app.state.container``, which is the
            dependency-injection container constructed in
            ``src.main`` and carries the long-lived Postgres pool,
            Kafka admin client, and Schema Registry client. We access
            it via duck typing rather than importing ``Container``
            so this controller stays decoupled from the container's
            concrete shape.

    Returns:
        :class:`~fastapi.responses.ORJSONResponse` with status 200 when
        ALL dependencies are healthy, otherwise 503 with a structured
        body of the form
        ``{"status": "not_ready", "checks": {<dep>: {"status": "error",
        "error": <exception_class_name>}, ...}}``. Error payloads
        contain ONLY exception class names (never messages) per AAP R-25.
    """
    container = request.app.state.container

    checks: dict[str, dict[str, Any]] = {}
    overall_ok: bool = True

    # ------------------------------------------------------------------
    # 1. PostgreSQL — SELECT 1 with bounded timeout
    # ------------------------------------------------------------------
    # The ``inventory_db`` PostgreSQL pool is the canonical write store
    # for stock_items, reservations, warehouses, stock_movements
    # (AAP Section 0.4.4). A failing pool here means writes will fail
    # and the readiness probe must trip 503 so the orchestrator removes
    # the pod from the load-balancer pool.
    #
    # ``SELECT 1;`` is intentionally the cheapest possible verification:
    # it touches no business tables, takes no locks, and returns in
    # microseconds on a healthy connection. Anything heavier (a row
    # scan, a COUNT, a metadata query) would slow probes and occasionally
    # contend with real workload.
    try:
        async with container.pg_pool.connection() as conn:
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute("SELECT 1;"),
                    timeout=_OUTER_CHECK_TIMEOUT_S,
                )
        checks["postgres"] = {"status": "ok"}
    except asyncio.TimeoutError:
        checks["postgres"] = {"status": "error", "error": "timeout"}
        overall_ok = False
    except Exception as exc:  # noqa: BLE001 — readiness must report every failure mode
        # Use exc.__class__.__name__ (NOT str(exc)). psycopg.OperationalError
        # frequently embeds the full DSN, including the password, in its
        # message — leaking it here would be an immediate AAP R-25
        # violation and a recurring CVE class across services.
        checks["postgres"] = {"status": "error", "error": exc.__class__.__name__}
        overall_ok = False

    # ------------------------------------------------------------------
    # 2. Kafka — AdminClient.list_topics (synchronous; offload to thread)
    # ------------------------------------------------------------------
    # Apache Kafka is the platform-wide event backbone (AAP R-14, R-17,
    # R-30). The Inventory Service is both a producer (inventory.reserved,
    # inventory.released, inventory.low-stock) and a consumer
    # (order.created, order.cancelled, order.fulfilled). A broker outage
    # is an immediate readiness failure: writes that depend on event
    # emission would silently lose the saga signal, so the orchestrator
    # must pull this pod from rotation until the brokers are reachable.
    #
    # confluent-kafka's AdminClient.list_topics is a *synchronous* C
    # extension call — calling ``await`` on it directly would raise
    # TypeError. We wrap it with ``asyncio.to_thread`` so the worker
    # thread blocks, not the event loop. The library's own ``timeout``
    # argument (1.0s) provides the inner ceiling; ``asyncio.wait_for``
    # with a 2.0s outer ceiling is defense-in-depth against any
    # pathology where the inner timeout fails to fire.
    try:
        await asyncio.wait_for(
            asyncio.to_thread(
                container.kafka_admin.list_topics,
                timeout=_KAFKA_INNER_TIMEOUT_S,
            ),
            timeout=_OUTER_CHECK_TIMEOUT_S,
        )
        checks["kafka"] = {"status": "ok"}
    except asyncio.TimeoutError:
        checks["kafka"] = {"status": "error", "error": "timeout"}
        overall_ok = False
    except Exception as exc:  # noqa: BLE001
        # confluent_kafka.KafkaException messages can include broker
        # identities and SASL credentials in some failure modes —
        # exposing them in the response body would violate AAP R-25.
        checks["kafka"] = {"status": "error", "error": exc.__class__.__name__}
        overall_ok = False

    # ------------------------------------------------------------------
    # 3. Schema Registry — get_subjects() (synchronous; offload to thread)
    # ------------------------------------------------------------------
    # Confluent Schema Registry (AAP R-14) is required to validate every
    # produced and consumed Kafka event payload. If the registry is
    # unreachable, producers will reject new events and consumers may
    # fail to deserialize incoming messages — readiness must trip 503
    # so traffic is held off until the registry is reachable again.
    #
    # SchemaRegistryClient.get_subjects is HTTP-backed but the
    # confluent-kafka client wraps it in a synchronous interface; we
    # offload it to a worker thread for the same event-loop reason as
    # the Kafka admin call above.
    try:
        await asyncio.wait_for(
            asyncio.to_thread(container.schema_registry.get_subjects),
            timeout=_OUTER_CHECK_TIMEOUT_S,
        )
        checks["schema_registry"] = {"status": "ok"}
    except asyncio.TimeoutError:
        checks["schema_registry"] = {"status": "error", "error": "timeout"}
        overall_ok = False
    except Exception as exc:  # noqa: BLE001
        checks["schema_registry"] = {
            "status": "error",
            "error": exc.__class__.__name__,
        }
        overall_ok = False

    # ------------------------------------------------------------------
    # Build the response envelope
    # ------------------------------------------------------------------
    # Body shape:
    #   {
    #     "status": "ready" | "not_ready",
    #     "checks": {
    #       "postgres":         {"status": "ok"} | {"status": "error", "error": "<class>"},
    #       "kafka":            {"status": "ok"} | {"status": "error", "error": "<class>"},
    #       "schema_registry":  {"status": "ok"} | {"status": "error", "error": "<class>"},
    #     }
    #   }
    body: dict[str, Any] = {
        "status": "ready" if overall_ok else "not_ready",
        "checks": checks,
    }

    # Return ORJSONResponse directly so the controller — not FastAPI's
    # default automatic serializer — is in control of the status code.
    # Returning the dict with a hard-coded ``status_code=200`` decorator
    # would mask 503 outcomes; raising HTTPException would lose the
    # structured ``checks`` body that operators depend on for triage.
    if overall_ok:
        return ORJSONResponse(content=body, status_code=status.HTTP_200_OK)
    return ORJSONResponse(
        content=body, status_code=status.HTTP_503_SERVICE_UNAVAILABLE
    )


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------
# ``router`` is the single export consumed by ``src.main`` via
# ``app.include_router(router)``. ``live`` and ``ready`` are exposed via
# their decoration on the router and are not normally imported directly,
# but they remain accessible at module scope for unit tests that want to
# call them as plain coroutines without an HTTP layer.
__all__: list[str] = ["router", "live", "ready"]
