"""FastAPI controller for the Recommendation Engine's health probes.

Implements the two endpoints required by **AAP R-19**:

  * ``GET /health/live``   — process is running (cheap, always 200).
  * ``GET /health/ready``  — all critical dependencies are reachable.

The readiness endpoint checks four critical dependencies of the
Recommendation Engine:

  * **Postgres**  — ``SELECT 1`` via ``container.pg_pool``.
  * **Redis**     — ``container.redis.ping()``.
  * **Kafka**     — ``container.event_dispatcher.kafka_ready()`` —
                    delegates to the dispatcher's own readiness check
                    so a consumer that is up but unassigned still
                    reports ``down``.
  * **Model**     — ``container.model_loader.is_loaded`` truthy.

Each check is wrapped with a short timeout (default 1.5 s) so a slow
dependency cannot drag the probe's total latency past Kubernetes's
``timeoutSeconds`` grace. The four async checks run concurrently via
``asyncio.gather`` so wall-clock latency is bounded by the slowest
single dependency rather than the sum of all four.

On any failure the endpoint returns 503 with a body that still
enumerates EVERY dependency's state so operators can tell at a glance
which one is down without scraping logs.

Both endpoints are **allow-listed** from JWT middleware: they MUST
be reachable without a token. The allow-list is enforced in
``src.middleware.jwt_auth.JWTAuthMiddleware``, not here.

Compliance notes
----------------
- AAP R-19 — every service exposes ``/health/live`` and ``/health/ready``.
- AAP R-26 — structured JSON logs via :mod:`structlog`; no ``print()``.
- AAP R-27 — the ``/metrics`` endpoint is mounted at app level in
  ``src/main.py``, NOT here.

Cross-references
----------------
- Consumed by ``src/main.py`` via
  ``from src.controllers.health import router as health_router``.
- Depends on (runtime only) ``request.app.state.container`` which the
  lifespan manager attaches at startup. This module performs the
  attribute lookups defensively (``getattr`` with default ``None``)
  so an early-startup probe call cannot crash the handler.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard library imports (alphabetical)
# ---------------------------------------------------------------------------
# ``asyncio`` provides the async primitives used to run the three async
# dependency checks concurrently (``asyncio.create_task`` + ``asyncio.gather``),
# bound each call by a short timeout (``asyncio.wait_for``), and detect
# coroutine returns from ``kafka_ready`` (``asyncio.iscoroutine``) so the
# probe handles both sync and async implementations of the readiness hook.
import asyncio

# ``Literal`` constrains the response schemas to the exact set of allowed
# string values: LivenessResponse.status is always ``"alive"``,
# ReadinessResponse.status is one of ``"ready" | "not_ready"``, and
# the per-dependency ``DepStatus`` alias is one of ``"up" | "down"``.
# These constraints flow through to the auto-generated OpenAPI document.
# ``Any`` is used as the duck-typed annotation for container fields whose
# concrete classes (AsyncConnectionPool, redis.asyncio.Redis,
# EventDispatcher, ModelLoader) live in ``src.container`` — we deliberately
# avoid a runtime or TYPE_CHECKING import of that module to keep this
# controller trivially importable in any test environment.
from typing import Any, Literal

# ---------------------------------------------------------------------------
# Third-party imports (alphabetical)
# ---------------------------------------------------------------------------
# ``fastapi`` primitives:
#   - ``APIRouter`` is the building block for our two health routes.
#   - ``Request`` exposes ``request.app.state`` so we can pull the DI
#     container off the application state at runtime — this avoids a
#     module-import-time dependency on ``src.container``.
#   - ``Response`` is mutated by the readiness handler to set HTTP 503
#     while STILL preserving the response_model body (the canonical
#     FastAPI alternative to ``raise HTTPException``, which would bypass
#     the response model and lose the ``dependencies`` map).
#   - ``status`` provides the canonical HTTP status-code constants
#     (HTTP_200_OK, HTTP_503_SERVICE_UNAVAILABLE) for readability.
from fastapi import APIRouter, Request, Response, status

# ``pydantic`` v2 primitives for the response schemas:
#   - ``BaseModel`` is the root for the Live/Ready response payloads.
#   - ``ConfigDict(frozen=True, extra="forbid")`` makes the responses
#     immutable and rejects unknown fields — defensive serialization
#     posture appropriate for an externally-visible probe contract.
#   - ``Field`` declares default values and OpenAPI-surfaced descriptions.
from pydantic import BaseModel, ConfigDict, Field

# ``structlog`` powers the AAP R-26 structured JSON log lines emitted on
# any dependency failure. The module-level logger is bound once at import
# time; structlog's lazy proxy means changing the global processor chain
# in ``src.main.lifespan`` still affects log lines emitted afterward.
import structlog


# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
# Bound once at import; do NOT re-bind inside handlers — structlog supports
# per-call ``contextvars`` and ``bind`` for request-scoped enrichment.
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Constants and types
# ---------------------------------------------------------------------------
# Short per-dependency timeout. The probe's overall budget is bounded by the
# slowest single dependency (~1.5s) rather than the sum of all checks because
# we run them concurrently — well under Kubernetes's default
# ``timeoutSeconds`` of 10s for liveness/readiness probes. Tuned conservative:
# Postgres SELECT 1 / Redis PING / Kafka assignment query should each return
# in well under 100ms in a healthy state.
_DEP_TIMEOUT_SECONDS: float = 1.5

# Per-dependency status type. Surfaced through the OpenAPI schema via the
# Literal in :class:`ReadinessResponse.dependencies`. Operators reading the
# JSON body can rely on EXACTLY one of these two strings for any key.
DepStatus = Literal["up", "down"]


# ---------------------------------------------------------------------------
# Pydantic v2 response models
# ---------------------------------------------------------------------------


class LivenessResponse(BaseModel):
    """Body returned by ``GET /health/live``.

    The shape is deliberately minimal — a single ``status`` field with a
    literal value of ``"alive"``. Liveness probes intentionally carry no
    dependency information; they answer only the question "is the ASGI
    process running?" and Kubernetes uses a non-200 response to kill the
    pod. We never want a deadlocked external dependency to cause a pod
    restart loop, hence the absence of any other check here.
    """

    # ``frozen=True`` makes the model hashable and immutable, ensuring the
    # liveness payload is a constant value-type. ``extra="forbid"`` rejects
    # any client-side attempt to inject extra fields via the JSON body
    # (defense in depth — FastAPI does not let clients shape response bodies
    # anyway, but the strictness is cheap and explicit).
    model_config = ConfigDict(frozen=True, extra="forbid")

    # ``Literal["alive"]`` means OpenAPI consumers see the only allowed value
    # at the schema level; the default of ``"alive"`` lets the handler return
    # ``LivenessResponse()`` without arguments.
    status: Literal["alive"] = Field(
        default="alive",
        description="Liveness verdict; always 'alive' when the process is running.",
    )


class ReadinessResponse(BaseModel):
    """Body returned by ``GET /health/ready`` on success OR failure.

    The HTTP status code differentiates success (200) from failure
    (503). The body always enumerates every dependency's current
    state so operators can tell which one is down without scraping
    logs. This dual-channel design (status code AND body) means that
    LBs / Kubernetes can route on the status code while operators
    can debug interactively via the JSON dependencies map.
    """

    # Same immutability + strictness posture as :class:`LivenessResponse`.
    # The model is constructed exactly once per request and never mutated.
    model_config = ConfigDict(frozen=True, extra="forbid")

    # Two-state literal: ``"ready"`` (all deps up) or ``"not_ready"``
    # (one or more deps down). Required, no default — the handler always
    # constructs this with an explicit value derived from the deps map.
    status: Literal["ready", "not_ready"] = Field(
        ..., description="Overall readiness verdict."
    )

    # The per-dependency map. Keys are stable identifiers (``postgres``,
    # ``redis``, ``kafka``, ``model``) so monitoring rules can pivot on
    # them without breaking when new dependencies are added; values are
    # constrained to ``"up"`` / ``"down"`` for a binary verdict per
    # dependency. Pydantic v2's :class:`Literal` constraint flows through
    # the JSON Schema generator into the OpenAPI document.
    dependencies: dict[str, DepStatus] = Field(
        ...,
        description=(
            "Per-dependency status map. Keys: postgres, redis, kafka, model."
        ),
    )


# ---------------------------------------------------------------------------
# DI container access (runtime-only lookup)
# ---------------------------------------------------------------------------


def _get_container(request: Request) -> Any:
    """Pull the DI container off ``app.state``.

    Returns ``None`` when the lifespan manager has not yet attached it
    (cold-start race window). The readiness handler treats a missing
    container as "every dep is down" and returns 503, which is the
    correct verdict when the service has not finished startup.

    Args:
        request: The incoming FastAPI request whose ``app.state``
            attribute is checked for a ``container`` slot.

    Returns:
        The DI container instance attached by the lifespan manager,
        or ``None`` when the slot is absent.

    Note:
        We use :func:`getattr` with a ``None`` default rather than a
        bare attribute access so that an unconfigured app (e.g., in a
        unit test) does not raise ``AttributeError`` from inside the
        request lifecycle.
    """
    # The return type is intentionally ``Any`` because the actual
    # ``Container`` type lives in ``src.container`` and we deliberately
    # avoid a runtime import of that module (see TYPE_CHECKING block).
    # Defensive ``getattr`` so a missing ``container`` slot returns
    # ``None`` instead of raising ``AttributeError``.
    return getattr(request.app.state, "container", None)


# ---------------------------------------------------------------------------
# Per-dependency check helpers
# ---------------------------------------------------------------------------
# Each helper returns a boolean. Each helper SWALLOWS exceptions (returns
# False on any failure) so a dependency raising inside its health hook cannot
# crash the probe handler. Failures emit a structured WARNING log with the
# error class name so operators can see WHY the dependency is down without
# the response body leaking stack traces.


async def _check_postgres(pg_pool: Any) -> bool:
    """Return ``True`` when ``SELECT 1`` succeeds on the shared pool.

    Uses :func:`asyncio.wait_for` to bound the call to
    :data:`_DEP_TIMEOUT_SECONDS`. Any exception (timeout, connection
    refused, authentication failure, pool exhausted) is caught and
    converted to ``False`` so the probe handler is shielded from
    dependency-raised errors.

    Args:
        pg_pool: An :class:`psycopg_pool.AsyncConnectionPool`-shaped
            object exposing an async context-managed ``connection()``
            method. ``None`` is treated as a missing dependency and
            yields ``False``.

    Returns:
        ``True`` if the probe ran ``SELECT 1`` successfully and the
        scalar value came back as ``1``; ``False`` on any failure.
    """
    # A missing pool indicates the container has not finished wiring
    # Postgres. We treat this as "down" so operators can see the gap
    # in the readiness map.
    if pg_pool is None:
        return False
    try:
        # The probe coroutine is defined inline so the timeout wrapping
        # below covers the full borrow-connect-execute-fetch cycle as
        # one logical operation.
        async def _probe() -> bool:
            # ``pool.connection()`` is an async context manager that
            # borrows a pooled connection and returns it on exit.
            # ``conn.cursor()`` is the per-statement context manager.
            async with pg_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # ``SELECT 1`` is the canonical liveness query —
                    # it round-trips the wire protocol without
                    # touching any application data. We compare the
                    # scalar result to ``1`` to guard against a
                    # configured-but-misbehaving driver returning
                    # something else.
                    await cur.execute("SELECT 1")
                    row = await cur.fetchone()
                    return row is not None and row[0] == 1

        return await asyncio.wait_for(_probe(), timeout=_DEP_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 - intentional broad catch for probe
        # AAP R-26: structured JSON WARNING with stable event name so
        # Kibana queries can pivot on ``event=health_postgres_down``
        # to see Postgres outages across the fleet.
        logger.warning(
            "health_postgres_down",
            error=str(exc),
            error_class=type(exc).__name__,
        )
        return False


async def _check_redis(redis_client: Any) -> bool:
    """Return ``True`` when ``redis.ping()`` returns truthy within the timeout.

    :class:`redis.asyncio.Redis.ping()` returns ``True`` on success
    (the wire response is a ``PONG`` simple string). Any wire error
    or timeout is caught and converted to ``False``.

    Args:
        redis_client: A :class:`redis.asyncio.Redis`-shaped object with
            an awaitable ``ping()`` method. ``None`` is treated as a
            missing dependency and yields ``False``.

    Returns:
        ``True`` if ``ping()`` returned a truthy value within the
        timeout budget; ``False`` on any failure.
    """
    # A missing client indicates the container has not finished wiring
    # Redis. Same defensive posture as :func:`_check_postgres`.
    if redis_client is None:
        return False
    try:
        # ``ping()`` returns ``True`` on success for redis-py's async
        # client. We coerce with ``bool(...)`` because some compatible
        # clients return the byte string ``b"PONG"`` instead — both
        # of which are truthy and pass the check.
        pong = await asyncio.wait_for(
            redis_client.ping(), timeout=_DEP_TIMEOUT_SECONDS
        )
        return bool(pong)
    except Exception as exc:  # noqa: BLE001 - intentional broad catch for probe
        logger.warning(
            "health_redis_down",
            error=str(exc),
            error_class=type(exc).__name__,
        )
        return False


async def _check_kafka(event_dispatcher: Any) -> bool:
    """Return ``True`` when the Kafka consumer is assigned to its partitions.

    Delegates to ``container.event_dispatcher.kafka_ready()`` per the
    folder spec. The dispatcher's readiness check verifies consumer-group
    assignment so a consumer that is up-but-unassigned (e.g., during a
    rebalance) still reports ``down``.

    The ``kafka_ready`` attribute MAY be a sync function returning a
    bool, an async coroutine function, or even a property — we handle
    all three by checking :func:`asyncio.iscoroutine` on the result.

    Args:
        event_dispatcher: An event dispatcher instance exposing a
            ``kafka_ready`` callable or property. ``None`` and missing
            attribute both yield ``False``.

    Returns:
        ``True`` if the dispatcher reports the consumer is assigned
        and ready; ``False`` on any failure or missing probe hook.
    """
    # Missing dispatcher -> Kafka is functionally down for this service.
    if event_dispatcher is None:
        return False
    # ``getattr(...)`` with a default of ``None`` is the defensive form;
    # the alternative (``hasattr`` + attribute access) double-traverses.
    probe = getattr(event_dispatcher, "kafka_ready", None)
    if probe is None:
        # The dispatcher exists but doesn't expose the expected probe.
        # Log a one-shot warning so operators can fix the dispatcher
        # implementation; treat as "down" defensively.
        logger.warning("health_kafka_probe_missing")
        return False
    try:
        # ``kafka_ready`` may be a method (callable) returning either a
        # plain bool OR an awaitable, OR it may be a property already
        # holding a bool. Try calling first; if it isn't callable, the
        # bare value is what we use.
        if callable(probe):
            result = probe()
        else:
            result = probe
        # If the call returned a coroutine, await it bounded by the
        # short timeout so a hung consumer cannot drag out the probe.
        if asyncio.iscoroutine(result):
            result = await asyncio.wait_for(
                result, timeout=_DEP_TIMEOUT_SECONDS
            )
        return bool(result)
    except Exception as exc:  # noqa: BLE001 - intentional broad catch for probe
        logger.warning(
            "health_kafka_down",
            error=str(exc),
            error_class=type(exc).__name__,
        )
        return False


def _check_model(model_loader: Any) -> bool:
    """Return ``True`` when the ML model has been loaded into memory.

    ``model_loader.is_loaded`` is a boolean attribute set to ``True``
    by ``model_loader.load()`` on success and ``False`` until the
    artefact has been deserialized. We only read the in-memory flag
    here — the probe MUST NOT trigger model re-loading because
    Kubernetes hits readiness probes every ``periodSeconds`` (default
    10s) and a re-load would be catastrophic.

    Args:
        model_loader: A ``ModelLoader`` instance exposing an
            ``is_loaded`` attribute. ``None`` yields ``False``.

    Returns:
        ``True`` when ``model_loader.is_loaded`` is truthy; ``False``
        otherwise (including when the loader is missing or the
        attribute access raises).
    """
    # Missing loader -> ML inference is not ready -> "down" verdict.
    if model_loader is None:
        return False
    try:
        # ``getattr(..., False)`` returns False when the attribute is
        # absent, which is the desired "down" verdict for a misconfigured
        # loader. Wrapping in ``bool(...)`` normalizes truthy
        # implementations (e.g., a numpy boolean) to a Python bool.
        return bool(getattr(model_loader, "is_loaded", False))
    except Exception as exc:  # noqa: BLE001 - intentional broad catch for probe
        # Reading a plain attribute should not raise, but a property
        # implementation could. Catch defensively and emit a warning.
        logger.warning(
            "health_model_error",
            error=str(exc),
            error_class=type(exc).__name__,
        )
        return False


# ---------------------------------------------------------------------------
# Router and request handlers (AAP R-19)
# ---------------------------------------------------------------------------
# The module-level ``router`` is the canonical export. ``src/main.py``
# imports it as ``health_router`` and includes it on the FastAPI app.
# Both endpoints are allow-listed from JWT validation by
# ``JWTAuthMiddleware`` — that allow-list is defined there, NOT here, so
# this controller stays independent of the auth mechanism.
router = APIRouter()


@router.get(
    "/health/live",
    response_model=LivenessResponse,
    status_code=status.HTTP_200_OK,
    summary="Process-alive probe (AAP R-19).",
    response_description="Always returns 200 with status='alive' if the process is running.",
)
async def liveness() -> LivenessResponse:
    """Return 200 as long as the ASGI process is running.

    This endpoint performs no dependency checks and returns the same
    body regardless of the container's state. It is designed to be
    safe as a Kubernetes ``livenessProbe`` — a negative response MUST
    indicate that the process itself is stuck / deadlocked and should
    be killed by the kubelet.

    Returns:
        :class:`LivenessResponse` with ``status='alive'``. The HTTP
        status is always ``200 OK``.

    Note:
        Liveness MUST be unconditional with respect to dependencies.
        If we tied liveness to (e.g.) Postgres reachability, every
        Postgres outage would trigger a fleet-wide pod restart loop,
        which is exactly the failure mode liveness is supposed to
        prevent. Readiness (`/health/ready`) is the right place for
        dependency aggregation.
    """
    return LivenessResponse()


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    responses={
        # Document both possible status codes in the OpenAPI schema so
        # consumers (Kibana dashboards, alerting rules, K8s probes) know
        # to expect either 200 or 503 with the SAME body shape.
        status.HTTP_200_OK: {"model": ReadinessResponse},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse},
    },
    summary="Aggregate readiness probe (AAP R-19).",
    response_description=(
        "200 with status='ready' when all dependencies are up; "
        "503 with status='not_ready' otherwise. The body always "
        "enumerates every dependency's verdict."
    ),
)
async def readiness(request: Request, response: Response) -> ReadinessResponse:
    """Return 200 when every critical dependency is reachable, 503 otherwise.

    The response body enumerates every dependency's state even on
    failure so operators can tell at a glance which one is down. The
    four checks run concurrently to keep the probe's wall-clock
    latency close to the single slowest dependency rather than the
    sum of all four timeouts.

    Args:
        request: Injected by FastAPI; used to access
            ``request.app.state.container`` and
            ``request.state.correlation_id``.
        response: Injected by FastAPI; mutated to set HTTP 503 when
            one or more dependencies are down WITHOUT bypassing the
            ``response_model`` schema.

    Returns:
        :class:`ReadinessResponse` with status and per-dependency
        verdicts. The HTTP status code is set to 200 on full health,
        503 when at least one dependency is down.
    """
    # Pull the DI container off the app state. Cold-start race: the
    # lifespan manager may not have attached it yet on the very first
    # probe of a brand-new pod. Treat that as "every dep down".
    container = _get_container(request)

    if container is None:
        # No container means startup hasn't completed — fail readiness.
        # We still emit a structured WARNING so operators see the cold-
        # start window in Kibana and can correlate with pod create
        # timestamps if probes are firing too aggressively.
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        deps_all_down: dict[str, DepStatus] = {
            "postgres": "down",
            "redis": "down",
            "kafka": "down",
            "model": "down",
        }
        logger.warning(
            "health_ready_degraded",
            dependencies=deps_all_down,
            correlation_id=getattr(request.state, "correlation_id", None),
            reason="container_not_attached",
        )
        return ReadinessResponse(
            status="not_ready",
            dependencies=deps_all_down,
        )

    # Run the three async checks concurrently via tasks; do the sync
    # model check inline (wrapping a 1-statement bool read in a task
    # would just add scheduling overhead with no parallelism benefit).
    # ``getattr(container, ..., None)`` is defensive: if the container
    # is missing one of the expected fields (e.g., during a refactor
    # that adds new fields and a stale image is still running), the
    # corresponding check returns False rather than raising.
    postgres_ok_task = asyncio.create_task(
        _check_postgres(getattr(container, "pg_pool", None)),
        name="health-postgres",
    )
    redis_ok_task = asyncio.create_task(
        _check_redis(getattr(container, "redis", None)),
        name="health-redis",
    )
    kafka_ok_task = asyncio.create_task(
        _check_kafka(getattr(container, "event_dispatcher", None)),
        name="health-kafka",
    )
    # Sync check — runs on the event-loop thread directly. Reads a
    # cached boolean attribute, no I/O.
    model_ok: bool = _check_model(getattr(container, "model_loader", None))

    # ``return_exceptions=False`` is the safe choice here because each
    # helper already swallows its own exceptions and returns ``False``
    # on failure — no exception should bubble up. If one ever did, the
    # probe handler would surface it as a 500 (FastAPI default), which
    # is the correct behavior for a "the probe itself is broken" bug.
    postgres_ok, redis_ok, kafka_ok = await asyncio.gather(
        postgres_ok_task,
        redis_ok_task,
        kafka_ok_task,
        return_exceptions=False,
    )

    # Build the canonical dependencies map in a stable, predictable order.
    # The key set (postgres, redis, kafka, model) is part of the public
    # response contract — adding or renaming keys here is a breaking
    # change for downstream alerting rules.
    deps: dict[str, DepStatus] = {
        "postgres": "up" if postgres_ok else "down",
        "redis": "up" if redis_ok else "down",
        "kafka": "up" if kafka_ok else "down",
        "model": "up" if model_ok else "down",
    }

    # Aggregate verdict: every dependency must be ``"up"`` for the
    # service to be ready. A single ``"down"`` flips the overall result.
    all_up = all(v == "up" for v in deps.values())

    if not all_up:
        # Mutate the injected ``Response`` to carry HTTP 503 while
        # preserving the ``response_model`` body — the canonical
        # FastAPI pattern for "non-200 with structured body". Raising
        # ``HTTPException`` here would bypass ``response_model`` and
        # require a custom exception handler to keep the dependencies
        # map in the body.
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        # AAP R-26: structured WARNING with the correlation ID (when
        # the LoggingMiddleware has attached one) so operators can
        # pivot on ``event=health_ready_degraded`` and the dependencies
        # map to drill into specific outages.
        logger.warning(
            "health_ready_degraded",
            dependencies=deps,
            correlation_id=getattr(request.state, "correlation_id", None),
        )
        return ReadinessResponse(status="not_ready", dependencies=deps)

    # All dependencies up — return 200 with the affirmative verdict.
    # ``response.status_code`` defaults to 200 from the route decorator
    # so we don't need to set it explicitly here.
    return ReadinessResponse(status="ready", dependencies=deps)


# ---------------------------------------------------------------------------
# HealthController class wrapper
# ---------------------------------------------------------------------------
# The schema declares a ``HealthController`` export with ``router``,
# ``live``, and ``ready`` members exposed. We provide it as a thin grouping
# class around the module-level symbols so consumers that prefer an OOP
# handle (e.g., dependency-injection containers, test fixtures, or
# documentation generators) can grab a single object containing the router
# and both handler coroutines without reaching into module globals.
#
# The module-level ``router`` remains the canonical export — ``src/main.py``
# imports that directly per the consumer convention documented in
# ``src/controllers/__init__.py`` ("import router DIRECTLY from the concrete
# submodule"). The class is a convenience accessor with no runtime side
# effects beyond holding references to the same callables.


class HealthController:
    """Class accessor around the module-level health router and handlers.

    Provides a single OOP handle for the two health probe handlers and
    their shared :class:`fastapi.APIRouter`. The class holds class-level
    references to the same callables exposed at module scope; instantiation
    is supported but unnecessary for the typical use case (the canonical
    consumer does ``from src.controllers.health import router`` and never
    touches this class).

    Attributes:
        router: The :class:`fastapi.APIRouter` instance carrying the
            ``/health/live`` and ``/health/ready`` routes. Identical
            to the module-level ``router`` symbol.
        live: The async coroutine handler for ``GET /health/live``.
            Identical to the module-level :func:`liveness`.
        ready: The async coroutine handler for ``GET /health/ready``.
            Identical to the module-level :func:`readiness`.

    Note:
        This class is provided for API completeness — most callers
        should prefer the module-level ``router`` symbol per the
        import convention documented in
        ``src/controllers/__init__.py``.
    """

    # Class-level attributes hold references to the module-level symbols.
    # No instance state; the class is essentially a typed namespace.
    router: APIRouter = router
    live = staticmethod(liveness)
    ready = staticmethod(readiness)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
# ``__all__`` lists the canonical exports of this module. ``router`` is the
# primary symbol consumed by ``src/main.py``; ``HealthController`` is the
# OOP convenience grouping declared in the file schema. The Pydantic models
# are NOT re-exported here because they are FastAPI response_model
# implementation details — consumers reading the JSON body should rely on
# the published OpenAPI schema, not the Python class.
__all__ = ["router", "HealthController"]
