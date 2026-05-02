"""User Service observability subsystem.

This package configures the three pillars of observability for the
User Service per AAP Sections 0.4.5 and 0.5.2.4:

* ``logging`` -- Structured JSON logging configuration. Configures
  stdlib ``logging``, structlog, and ``python-json-logger`` so every
  log record is a single-line JSON object on stdout. Filebeat tails
  the stdout stream per AAP R-27.
* ``metrics`` -- Prometheus metric definitions. Module-level Counter,
  Gauge, and Histogram instruments registered against the default
  ``prometheus_client`` registry. Metricbeat scrapes the ``/metrics``
  endpoint per AAP R-27.
* ``tracing`` -- OpenTelemetry tracer + auto-instrumentation. Configures
  the TracerProvider, OTLP span exporter, and instrumentors for
  FastAPI, httpx, SQLAlchemy, and (optionally) confluent-kafka.

Public API
----------
This package intentionally exposes NO names. Consumers MUST import
specific submodules directly, for example::

    from src.observability.logging import configure_logging, get_logger
    from src.observability.metrics import HTTP_REQUESTS_TOTAL, USERS_REGISTERED_TOTAL
    from src.observability.tracing import configure_tracing, instrument_fastapi

This discipline:
  * Avoids importing all three submodules (and their heavy transitive
    dependencies) when only one is needed.
  * Prevents accidental side effects at package import time.
  * Mirrors the established pattern across the monorepo
    (order-service, notification-service, etc.).

AAP cross-references
--------------------
* AAP R-13  -- Correlation ID propagation (consumed by ``logging`` and
  ``tracing`` submodules via contextvars and span attributes).
* AAP R-26  -- Structured JSON logs (configured by ``logging``).
* AAP R-27  -- Filebeat / Metricbeat shipping (consumed by ``logging``
  and ``metrics`` respectively).
* AAP R-28  -- Per-domain Kibana dashboards (consume the metric names
  defined in ``metrics``).
* AAP Section 0.4.5  -- Cross-cutting interceptors.
* AAP Section 0.5.2.4 -- Observability backbone (ELK stack).
"""

from __future__ import annotations

__all__: list[str] = []
