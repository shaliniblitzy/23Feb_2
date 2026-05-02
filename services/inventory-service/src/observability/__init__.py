"""Observability sub-package for the Inventory Service.

This package consolidates the three observability primitives the service
depends on at runtime:

logging_setup
    ``configure_logging(settings) -> None``
    ``get_logger(name) -> structlog.stdlib.BoundLogger``
    ``LoggingConfigurationError``
        Loads the dictConfig payload from ``services/inventory-service/config/
        log_config.json``, applies a runtime ``LOG_LEVEL`` override, and
        configures structlog so middleware-bound contextvars (``correlation_id``,
        ``user_id``) flow into every JSON log line. Implements AAP R-26
        (structured JSON logs with required fields) and the stdout-only
        emission contract that Filebeat consumes per AAP R-27.

metrics
    Single source of truth for every Prometheus metric this service
    emits. All metrics register on the default ``prometheus_client.REGISTRY``;
    ``prometheus_client.make_asgi_app()`` (mounted at ``/metrics`` by
    ``src.main.create_app()``) scrapes that registry directly. Eight
    metrics from the README's section 1.17 Observability + one
    supplementary scheduler-health counter. Implements the metrics-emission
    contract for the Kibana inventory dashboard (AAP R-28).

tracing
    ``setup_tracing(settings, app=None) -> None``
    ``get_tracer(name) -> Tracer``
        Configures OpenTelemetry: builds a ``TracerProvider`` carrying
        ``service.name``, ``service.version``, ``deployment.environment``;
        wires an OTLP-HTTP span exporter wrapped in a ``BatchSpanProcessor``;
        applies auto-instrumentation for HTTPX, SQLAlchemy, and FastAPI
        (when ``app`` is provided). Honors W3C Trace-Context propagation
        for correlation IDs across services (AAP R-13). NO-OP when the
        exporter endpoint is empty (local-dev default).

Usage convention
----------------
Callers MUST import directly from the specific sub-module rather than
from this package, to keep the package marker side-effect-free and
import-cheap::

    from src.observability.logging_setup import configure_logging, get_logger
    from src.observability.tracing import setup_tracing, get_tracer
    from src.observability import metrics

    log = get_logger("inventory_service.events.handlers.order_created")
    metrics.reservations_created_total.labels(
        warehouse="wh_us_east_1", outcome="success"
    ).inc()

Side-effect discipline
----------------------
This package marker is intentionally minimal:

* No submodule imports -- importing ``src.observability`` does NOT trigger
  Prometheus metric registration, structlog configuration, or OTLP
  exporter construction.
* No env-var reads.
* No file I/O.
* No logging.

These properties keep test fixtures, CLI tools, and the FastAPI app
factory cheap to bootstrap.

AAP cross-references
--------------------
* AAP R-13 -- correlation IDs propagated through every log line and
  outbound call (logging_setup + tracing).
* AAP R-19 -- fail-fast on missing log_config.json or invalid sampler
  (logging_setup + tracing).
* AAP R-26 -- structured JSON logs with required fields (logging_setup
  + ../config/log_config.json).
* AAP R-27 -- logs/metrics shipped via Filebeat/Metricbeat to ELK
  (logging_setup writes to stdout; metrics scraped by Prometheus).
* AAP R-28 -- per-domain Kibana dashboards consume the metrics defined
  in metrics.py.
"""

from __future__ import annotations

__all__: list[str] = []
