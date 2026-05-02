"""Observability bootstrap for the Product Service.

This package contains the three observability primitives configured
during FastAPI lifespan startup:

    logging_config — Structured JSON logging to stdout with the field
                     set required by AAP R-26. Exports
                     ``configure_logging(settings)`` and ``get_logger(name)``.
                     Filename is ``logging_config.py`` to match the
                     companion ``config/log_config.json`` file (this is
                     INTENTIONALLY DIFFERENT from sibling services
                     which use ``logging_setup.py`` or ``logger.py``).

    metrics        — Prometheus metric registrations: HTTP request
                     counters/histograms, MongoDB operation
                     counters/histograms, Kafka producer counters,
                     circuit-breaker gauge, DLQ depth gauge,
                     catalog size gauge, and category tree LRU
                     cache hit/miss counters. All metrics are
                     declared at module-import time and registered
                     to the default ``prometheus_client.REGISTRY``.

    tracing        — OpenTelemetry tracing setup. Exports
                     ``setup_tracing(settings, app=None)`` which
                     configures the global TracerProvider, the
                     OTLP-HTTP exporter, and auto-instruments
                     FastAPI, HTTPX, and (UNIQUELY for this
                     service) PyMongo per AAP R-7. Tracing setup
                     is best-effort; failures log a warning and
                     continue, in deliberate contrast to logging
                     which fails-fast per AAP R-19.

This ``__init__.py`` is INTENTIONALLY minimal — it does NOT eagerly
import any submodule. Callers must import the symbols they need
explicitly::

    from src.observability.logging_config import configure_logging
    from src.observability.metrics import HTTP_REQUESTS_TOTAL
    from src.observability.tracing import setup_tracing

The minimal design avoids:

    * Side effects on package import (relevant for unit tests).
    * Circular-import risk between submodules and middleware.
      ``logging_config.py`` lazy-imports ``src.middleware.correlation_id``
      to break a known startup-order cycle; if this ``__init__.py``
      eagerly imported ``logging_config``, that workaround would be
      defeated.
    * Premature loading of the OpenTelemetry SDK (non-trivial cost).

AAP cross-references
--------------------
* Section 0.4.5  — Logging middleware (consumed by middleware).
* Section 0.5.2.4 — ELK stack endpoints (Logstash receives Beats input).
* Section 0.6.1  — ``services/product-service/**/*`` wildcard.
* R-7   — MongoDB-specific instrumentation (PymongoInstrumentor).
* R-13  — Correlation-id propagation via middleware contextvar.
* R-19  — Logging fails-fast at startup; tracing is best-effort.
* R-26  — Structured JSON logs with required field set.
* R-27  — Logs/metrics ship via Filebeat/Metricbeat to ELK.
* R-28  — Kibana dashboards consume these signals.
"""
from __future__ import annotations

# This package's submodules are imported explicitly by callers; we
# do NOT re-export them from ``__init__.py``. The empty ``__all__``
# tuple makes this intent explicit to static analyzers and to
# ``from src.observability import *`` users (who will get nothing).
__all__: tuple[str, ...] = ()
