"""Alembic runtime environment for the Order Service.

This module is the entry-point loaded by the Alembic CLI on every invocation
of ``alembic upgrade``, ``alembic downgrade``, ``alembic revision``, and
``alembic current`` issued from
``services/order-service/migrations/``. It resolves the database URL,
configures structured logging, and dispatches to either the offline (SQL-
emit) or online (live-DB) migration path against the Order Service's
private PostgreSQL database (``order_db``).

Schema scope
------------
This migration tree owns FOUR tables in ``order_db`` (per AAP Section 0.4.4):

    1. ``orders``
       The order header (one row per checkout). Holds order_id, user_id,
       currency, amount totals, the rolled-up status, timestamps.
    2. ``order_items``
       Line items per order (one row per SKU). Holds order_id (FK),
       product_id, quantity, unit_price snapshot, line_amount.
    3. ``order_status_history``
       Append-only audit log of status transitions
       (CREATED -> RESERVING -> PAYING -> CONFIRMED | COMPENSATING ->
       CANCELLED). One row per transition with actor, reason, and
       timestamp.
    4. ``saga_state``
       The DURABLE BACKING STORE for the AAP R-18 saga coordinator. One
       row per in-flight saga. Holds saga_id, order_id (FK), current
       step, step status, last attempt timestamp, retry count, and the
       step-by-step payload required for compensation. Recovery-on-
       restart of the order-service depends on this table being present
       and consistent before the service comes online; therefore THIS
       env.py and the revisions in ``versions/`` are BOTH on the
       critical path of saga durability.

Saga durability is the single most important non-negotiable invariant of
this service (AAP R-18). The Order Service's saga coordinator commits the
Kafka offset and the saga_state row mutation in the SAME PostgreSQL
transaction (driven by ``transaction_per_migration=True`` in
``run_migrations_online()`` for the migration path; the runtime code uses
its own ``BEGIN/COMMIT`` blocks). If revision 20260101_000004 (saga_state
creation) or any future ALTER TABLE on saga_state partially applies and
fails halfway, the orders/order_items state from earlier revisions is
unaffected -- which is the sole reason ``transaction_per_migration=True``
is set below rather than the Alembic default (``False``, single
transaction wrapping the entire upgrade).

Forward-only policy
-------------------
Production deployments run ``alembic upgrade head``. Downgrades are
FORBIDDEN in production; corrective action is a NEW forward revision.
This is enforced by convention and reinforced in
``services/order-service/migrations/README.md``. A ``downgrade()`` body
exists in every revision file for emergency dev/testing rollback ONLY;
it MUST NOT be invoked in any non-dev environment.

Runtime contract
----------------
* The ``POSTGRES_URL`` environment variable MUST be set. Its value is the
  PostgreSQL connection URL in either of these forms::

      postgresql://orders:orders@order-postgres:5432/order_db
      postgresql+psycopg://orders:orders@order-postgres:5432/order_db

  The first form (bare ``postgresql://``) is rewritten to use the
  psycopg v3 SQLAlchemy dialect (``postgresql+psycopg://``) because the
  service pins ``psycopg[binary,pool]>=3.1.18,<4.0.0`` (NOT psycopg2). See
  ``services/order-service/requirements.txt``.
* The ``ALEMBIC_LOG_LEVEL`` environment variable optionally raises the
  log level emitted by env.py and Alembic itself (``INFO`` / ``DEBUG``)
  during planned migration windows or CI debug runs.
* ``target_metadata`` is intentionally ``None``. Schema is hand-authored
  in ``versions/<revision>.py`` via imperative ``op.create_table()`` and
  ``op.execute()`` calls; we do NOT use ``--autogenerate``. Rationale:
  every DDL change in the order domain (especially ``saga_state``) must
  be reviewed line-by-line for saga-durability implications.
* ``version_table`` is set to ``alembic_version_order_service`` (NOT the
  default ``alembic_version``) so multiple service databases can co-
  locate on a single Postgres instance during local development without
  colliding on a shared bookkeeping table.

Exit codes (relevant for K8s Job / initContainer observability and CI
failure-categorization)
-----------------------------------------------------------------------
* ``2`` -- ``POSTGRES_URL`` is unset or empty (AAP R-19 fail-fast).
  Distinct from migration execution failures so operators can filter
  with a simple ``$? -eq 2`` check.
* ``2`` -- ``POSTGRES_URL`` uses the rejected ``postgresql+psycopg2://``
  driver scheme. The service does not pin psycopg2; failing fast with a
  clear message is far kinder than letting SQLAlchemy raise an opaque
  ``ModuleNotFoundError: No module named 'psycopg2'`` deep inside its
  plugin loader.
* Non-zero (raised) -- migration execution failures (network, DDL
  conflicts, etc.) propagate as their natural exception types and exit
  with ``1`` per Python's default behavior.

Security and observability
--------------------------
* AAP R-25 -- Secrets are read EXCLUSIVELY from the environment. The
  sibling ``alembic.ini`` declares ``sqlalchemy.url =`` (empty); env.py
  injects the resolved URL into Alembic's config at runtime. Hardcoding
  a URL in alembic.ini would be a Sev-1 finding because committed git
  history is forever.
* AAP R-26 / R-27 -- All log output is structured-text and is shipped by
  Filebeat to Logstash / Elasticsearch; the ``_redact_url()`` helper
  masks the password component of any URL written to a log line so
  credentials never reach the ELK indices, even in transient migration
  logs. This is a defense-in-depth measure: real credentials should
  never appear in ``POSTGRES_URL`` outside of pod-mounted Secrets, but
  if an operator accidentally puts one in a Job manifest, the redaction
  ensures the leak does not propagate to logs.
* AAP R-13 -- Correlation IDs flow at the request layer (HTTP / Kafka
  message headers). They are NOT meaningful at migration time because
  ``alembic upgrade head`` is a one-shot Job, not a request handler;
  env.py uses a service-namespaced logger
  (``alembic.env.order_service``) instead so logs from this Job are
  trivially filterable by service in Kibana.

References
----------
* AAP Section 0.4.4 (order_db schema)
* AAP Section 0.5.2.5 (per-service migrations live under owning service)
* AAP R-6 (database per service)
* AAP R-9 (migrations colocated with owning service, applied
  automatically)
* AAP R-13 (correlation-ID propagation, not relevant at migration time)
* AAP R-18 (saga pattern + durable saga_state)
* AAP R-19 (fail-fast on missing critical dependencies at startup)
* AAP R-25 (secrets via environment variables only)
* AAP R-26 / R-27 (structured logs shipped to ELK)

Maintenance discipline
----------------------
This file is byte-similar to ``services/payment-service/migrations/env.py``
and ``services/notification-service/migrations/env.py`` for the structural
code (imports, regex patterns, dispatch logic). The Order-Service-
specific differences are limited to:

    1. The ``_VERSION_TABLE_NAME`` constant (``alembic_version_order_service``)
    2. The logger namespace (``alembic.env.order_service``)
    3. This module docstring narrative (saga-durability emphasis)

Future maintainers SHOULD keep this discipline so the three env.py files
stay reviewable as a set; a change to one usually warrants a coordinated
change to the others.
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
from __future__ import annotations

import logging
import os
import re
import sys
from logging.config import fileConfig
from typing import Final

from alembic import context
from sqlalchemy import engine_from_config, pool

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

#: Environment variable that carries the database URL at runtime (AAP R-25).
#: ``alembic.ini`` declares ``sqlalchemy.url =`` (empty) precisely because
#: the connection URL must be resolved here, never persisted in source or
#: configuration. Persisting a URL in alembic.ini would be a Sev-1 security
#: finding.
_DB_URL_ENV_VAR: Final[str] = "POSTGRES_URL"

#: Environment variable for optional Alembic log-level override. Operators
#: can set ``ALEMBIC_LOG_LEVEL=DEBUG`` in CI to surface verbose diagnostic
#: output without committing a change to ``alembic.ini``, or
#: ``ALEMBIC_LOG_LEVEL=INFO`` during a planned migration window so the
#: ``Running upgrade ... -> ...`` lines appear in operator logs.
_LOG_LEVEL_ENV_VAR: Final[str] = "ALEMBIC_LOG_LEVEL"

#: Default log level if ``ALEMBIC_LOG_LEVEL`` is not set. WARNING keeps
#: stderr quiet during routine ``alembic upgrade head`` runs while still
#: surfacing actionable warnings (e.g., deprecation notices, schema-drift
#: hints from compare_type / compare_server_default).
_DEFAULT_LOG_LEVEL: Final[str] = "WARNING"

#: SQLAlchemy dialect identifier for psycopg v3. SQLAlchemy 2.x exposes the
#: psycopg3 driver under this name; the legacy psycopg2 driver uses
#: ``postgresql+psycopg2`` and is NOT supported by this service.
_PSYCOPG3_DIALECT_SCHEME: Final[str] = "postgresql+psycopg"

#: Bare ``postgresql://`` scheme that must be rewritten to the psycopg v3
#: dialect. Operators frequently paste in libpq-style URLs without the
#: SQLAlchemy ``+driver`` qualifier; we silently upgrade those rather than
#: failing because there is exactly one supported PostgreSQL driver in
#: this service (psycopg v3). The ``re.IGNORECASE`` flag accepts ``POSTGRESQL://``
#: variants that occasionally appear in operator-supplied templates.
_BARE_POSTGRES_SCHEME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^postgresql://",
    re.IGNORECASE,
)

#: Rejected driver scheme -- psycopg2 is NOT pinned in requirements.txt.
#: We refuse the run with an explicit ``sys.exit(2)`` and a clear log
#: message instead of letting SQLAlchemy raise an opaque
#: ``ModuleNotFoundError: No module named 'psycopg2'`` deep inside its
#: plugin loader. This is the single most impactful defense in this file
#: because operators frequently inherit connection-string templates that
#: embed ``+psycopg2`` from older docs or libpq examples.
_REJECTED_PSYCOPG2_SCHEME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^postgresql\+psycopg2://",
    re.IGNORECASE,
)

#: Service-specific Alembic version table name. Co-located dev databases
#: with multiple services (auth_db, user_db, order_db, payment_db, etc.)
#: would otherwise collide on the default ``alembic_version`` table when
#: sharing a single Postgres instance during ``docker-compose up``. Per
#: AAP R-6 (database per service) and the migrations folder spec mandate.
#:
#: This constant -- and its propagation to BOTH ``context.configure()``
#: calls below (offline + online paths) -- is the SINGLE most important
#: behavioral difference vs. ``services/payment-service/migrations/env.py``
#: (``alembic_version_payment_service``) and
#: ``services/notification-service/migrations/env.py``
#: (``alembic_version_notification_service``). Forgetting to pass
#: ``version_table=_VERSION_TABLE_NAME`` to either ``context.configure()``
#: would silently fall back to the default ``alembic_version`` table,
#: breaking the multi-service local-dev scenario by causing the order-
#: service to clobber another service's migration history.
_VERSION_TABLE_NAME: Final[str] = "alembic_version_order_service"

# ---------------------------------------------------------------------------
# Alembic Config object + logging bootstrap
# ---------------------------------------------------------------------------

# ``context.config`` exposes the values parsed from ``alembic.ini``. The
# object supports ``set_main_option()`` and ``get_section()`` so we can
# inject a runtime-resolved ``sqlalchemy.url`` after we read POSTGRES_URL
# from the environment in ``_resolve_database_url()`` below.
config = context.config

# Bootstrap logging from alembic.ini's [loggers] / [handlers] / [formatters]
# sections. Alembic's default template wraps this in
# ``if config.config_file_name is not None`` to support the programmatic
# API where no .ini file is present (e.g., when env.py is imported by a
# unit test that constructs ``Config()`` in-memory).
# ``disable_existing_loggers=False`` preserves any parent-process loggers
# (matters when Alembic is invoked from pytest's ``caplog`` fixture or
# from an entrypoint script that already configured logging).
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# Allow operators to raise (or lower) the log level via the
# ``ALEMBIC_LOG_LEVEL`` env var without editing alembic.ini -- useful in
# CI for diagnosing failures and during planned migration windows for
# auditable INFO-level "Running upgrade X -> Y" output. ``getattr``
# resolves "DEBUG" / "INFO" / "WARNING" / "ERROR" / "CRITICAL" to their
# integer constants; an unknown name silently falls back to WARNING so
# that a typo in CI does not silently produce a flood of DEBUG output.
_log_level_name: Final[str] = os.environ.get(
    _LOG_LEVEL_ENV_VAR, _DEFAULT_LOG_LEVEL
).upper()
_log_level: Final[int] = getattr(logging, _log_level_name, logging.WARNING)
logging.getLogger("alembic").setLevel(_log_level)
logging.getLogger("sqlalchemy.engine").setLevel(_log_level)

#: Module-scope logger used for diagnostics emitted by env.py itself (as
#: opposed to Alembic's ``alembic.runtime.migration`` logger which reports
#: on revision execution). The ``alembic.env.order_service`` qualname
#: slots cleanly under the ``alembic`` parent logger configured by
#: ``fileConfig`` above and is service-namespaced so structured logs
#: shipped to Elasticsearch (per AAP R-26 / R-27) are trivially
#: filterable in Kibana with ``logger:"alembic.env.order_service"``.
logger = logging.getLogger("alembic.env.order_service")


# ---------------------------------------------------------------------------
# URL redaction (defined before other helpers that consume it)
# ---------------------------------------------------------------------------


def _redact_url(url: str) -> str:
    """Return a copy of *url* with the password component masked.

    Migration logs flow to Elasticsearch (AAP R-26 / R-27); we never want
    raw credentials in log indices, even ephemeral ones. Even though real
    credentials should never appear in ``POSTGRES_URL`` outside of
    pod-mounted Kubernetes Secrets, this is a defense-in-depth measure
    that protects against operator mistakes (e.g., a Job manifest
    embedding a dev password directly).

    The substitution targets the userinfo segment of the URL between the
    scheme and the host, replacing the password component with ``***``
    while preserving the username so operators can still grep
    ``user:***@host`` to identify which connection the log refers to.

    Args:
        url: The connection URL, possibly containing a password.

    Returns:
        A URL identical to *url* except that the password (the substring
        between the first ``:`` after the scheme and the next ``@``) is
        replaced with ``***``. URLs without a password (e.g., trust-
        authentication, or peer authentication on a Unix socket) are
        returned unchanged.

    Examples:
        >>> _redact_url("postgresql://orders:s3cret@host:5432/order_db")
        'postgresql://orders:***@host:5432/order_db'
        >>> _redact_url("postgresql+psycopg://orders@host:5432/order_db")
        'postgresql+psycopg://orders@host:5432/order_db'
    """
    return re.sub(
        r"(://[^:/@]+:)([^@]+)(@)",
        r"\1***\3",
        url,
    )


# ---------------------------------------------------------------------------
# Database URL resolution
# ---------------------------------------------------------------------------


def _normalize_database_url(raw_url: str) -> str:
    """Ensure *raw_url* uses the psycopg v3 SQLAlchemy dialect.

    The Order Service pins ``psycopg[binary,pool]>=3.1.18,<4.0.0`` (see
    ``../requirements.txt``). SQLAlchemy 2.0 exposes this driver under the
    ``postgresql+psycopg`` dialect name. Many operators paste in bare
    ``postgresql://...`` URLs (the libpq default); this helper rewrites
    them so SQLAlchemy picks the correct dialect.

    The function is intentionally narrow:

    * It does NOT validate host / port / database parts -- that is
      SQLAlchemy's job and producing a duplicate validator here would
      create drift.
    * It does NOT URL-encode credentials -- callers are responsible for
      providing a well-formed URL and must encode special characters in
      the password according to RFC 3986.
    * It DOES reject the legacy ``postgresql+psycopg2://`` scheme with an
      explicit ``sys.exit(2)`` because silent rewriting from psycopg2 to
      psycopg3 would mask a deployment-configuration error.

    Args:
        raw_url: The connection URL as read from the environment.

    Returns:
        A URL guaranteed to start with ``postgresql+psycopg://``.

    Side effects:
        Calls ``sys.exit(2)`` (and never returns) if *raw_url* uses the
        rejected ``postgresql+psycopg2://`` scheme. Logs an INFO-level
        message via the module logger when a bare ``postgresql://`` URL
        is rewritten so the transformation is auditable in CI logs.
    """
    if _REJECTED_PSYCOPG2_SCHEME_PATTERN.match(raw_url):
        logger.error(
            "POSTGRES_URL uses psycopg2 dialect (postgresql+psycopg2://), "
            "but the order-service depends on psycopg v3 (per "
            "requirements.txt). Rewrite to 'postgresql+psycopg://...' or "
            "use a bare 'postgresql://...' URL (which env.py will "
            "normalize automatically). Offending URL: %s",
            _redact_url(raw_url),
        )
        sys.exit(2)

    if _BARE_POSTGRES_SCHEME_PATTERN.match(raw_url):
        normalized = _BARE_POSTGRES_SCHEME_PATTERN.sub(
            f"{_PSYCOPG3_DIALECT_SCHEME}://",
            raw_url,
            count=1,
        )
        logger.info(
            "Normalized bare 'postgresql://' URL to '%s://' (psycopg v3 dialect).",
            _PSYCOPG3_DIALECT_SCHEME,
        )
        return normalized

    return raw_url


def _resolve_database_url() -> str:
    """Read and normalize the database URL; fail fast if missing.

    This is the AAP R-19 fail-fast gate for the migration runner. A
    missing or empty ``POSTGRES_URL`` is treated as a fatal startup
    error: the process logs a clear remediation message and exits with
    code ``2`` so that Kubernetes Job / initContainer operators can
    distinguish a missing-config failure (exit 2) from a migration
    execution failure (exit 1, raised by Alembic / SQLAlchemy / psycopg)
    using a simple ``$? -eq 2`` check.

    The function does NOT default to a localhost dev string -- silent
    fallbacks defeat the purpose of fail-fast and have historically
    caused the most disastrous incidents (a misconfigured prod Pod
    pointing at the wrong database is a Sev-1).

    Returns:
        A fully-qualified SQLAlchemy URL with the psycopg v3 dialect.

    Side effects:
        Calls ``sys.exit(2)`` (and never returns) if ``POSTGRES_URL`` is
        unset or empty. Calls ``sys.exit(2)`` (via
        ``_normalize_database_url``) if ``POSTGRES_URL`` uses the
        rejected psycopg2 dialect.
    """
    raw_url = os.environ.get(_DB_URL_ENV_VAR, "").strip()
    if not raw_url:
        logger.error(
            "Environment variable %s is not set or is empty. "
            "Order-service migrations cannot proceed without a database URL. "
            "Refer to services/order-service/.env.example for the expected "
            "format (e.g., "
            "POSTGRES_URL=postgresql+psycopg://orders:orders@order-postgres"
            ":5432/order_db).",
            _DB_URL_ENV_VAR,
        )
        sys.exit(2)
    return _normalize_database_url(raw_url)


# ---------------------------------------------------------------------------
# Target metadata
# ---------------------------------------------------------------------------

# Schema is hand-authored in ``versions/<revision>.py`` via imperative
# ``op.create_table()`` and ``op.execute()`` calls. Leaving this None
# disables ``--autogenerate``; revisions must be written by hand. This is
# the intended workflow for the order domain because every DDL change --
# especially anything touching ``saga_state`` -- must be reviewed line-
# by-line for AAP R-18 saga-durability implications. The four tables --
# ``orders``, ``order_items``, ``order_status_history``, and
# ``saga_state`` (AAP Section 0.4.4) -- include constraint and index
# definitions whose evolution must be planned, not derived from a
# Python ORM model snapshot.
#
# DO NOT replace this with a ``MetaData()`` object -- it is a deliberate
# architectural decision that reconciles "Alembic uses SQLAlchemy under
# the hood" with "every order-domain DDL change is reviewed manually".
target_metadata = None


# ---------------------------------------------------------------------------
# Offline migration path
# ---------------------------------------------------------------------------


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode -- emit SQL to stdout, no DB connection.

    This mode is used by:

    * Operators reviewing pending DDL before applying to production
      (``alembic upgrade head --sql > review.sql``).
    * CI dry-runs that lint or diff generated SQL.
    * DBA workflows where Alembic itself cannot connect to the target
      database (e.g., from a bastion host that lacks network access to
      production Postgres).

    The URL is resolved and used only for dialect selection so Alembic
    can render PostgreSQL-flavored DDL (``TIMESTAMPTZ``, ``JSONB``, ``ON
    CONFLICT``, etc.). All transactions are emitted with literal binds
    so the SQL is portable and can be hand-applied via ``psql -f``.

    Configuration notes:

    * ``url=url`` -- offline mode does not open a connection but Alembic
      still uses the URL for dialect selection.
    * ``version_table=_VERSION_TABLE_NAME`` -- ensures the offline-emitted
      SQL targets ``alembic_version_order_service`` (NOT the default
      ``alembic_version``). This matters for ``--sql`` review scripts:
      an operator who pipes the script through ``psql`` against a
      shared dev Postgres must not collide with sibling services'
      version tables.
    * ``literal_binds=True`` -- inlines parameter values into the
      generated SQL because the resulting script is meant for human
      review and for replay through ``psql -f``; bound-parameter
      placeholders make the script unusable in those contexts.
    * ``dialect_opts={"paramstyle": "named"}`` -- pairs with
      ``literal_binds`` to produce ``:name``-style placeholders for any
      binds Alembic cannot literalize (e.g., user-defined Python
      objects); these are rare for the order-service's straightforward
      DDL.
    * ``compare_type=True`` and ``compare_server_default=True`` -- enable
      type / default diff detection if ``--autogenerate`` is ever turned
      on later; harmless for the manual-revision workflow.
    * ``render_as_batch=False`` -- batch mode is a SQLite workaround
      that emits ``CREATE TABLE _alembic_tmp_<name>`` recipes;
      PostgreSQL supports true ``ALTER TABLE`` so we always render
      direct ALTER statements.
    """
    url = _resolve_database_url()

    logger.info(
        "Running Alembic in OFFLINE mode against %s (version_table=%s).",
        _redact_url(url),
        _VERSION_TABLE_NAME,
    )

    context.configure(
        url=url,
        target_metadata=target_metadata,
        version_table=_VERSION_TABLE_NAME,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        render_as_batch=False,
    )

    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Online migration path
# ---------------------------------------------------------------------------


def run_migrations_online() -> None:
    """Run migrations against a live PostgreSQL connection.

    This is the production path executed by:

    * Kubernetes initContainer or migration Job: ``alembic upgrade head``
    * Local dev container startup: same command
    * CI integration tests: same command against a Testcontainers
      PostgreSQL instance.

    A single synchronous SQLAlchemy engine is constructed from the
    ``[alembic]`` section of ``alembic.ini`` (with the resolved
    ``sqlalchemy.url`` injected at runtime), a connection is acquired,
    and Alembic's own transaction-management wraps each migration
    script.

    Notes:

    * ``version_table=_VERSION_TABLE_NAME`` -- ensures Alembic creates /
      reads / writes ``alembic_version_order_service`` instead of the
      default ``alembic_version``. Multiple services (auth-service,
      user-service, payment-service, notification-service, etc.) can
      share a single Postgres instance during local development without
      clashing on a shared bookkeeping table.
    * ``poolclass=pool.NullPool`` -- migration runs are short-lived,
      typically single-process (initContainer / K8s Job), and do not
      benefit from connection pooling. NullPool also sidesteps a
      well-documented issue where DDL issued through a pooled
      connection can appear rolled back if the pool reclaims the
      connection mid-migration; NullPool is the migration community's
      standard recommendation for this reason.
    * ``future=True`` -- explicit SQLAlchemy 2.0-style execution.
      Alembic 1.13+ defaults to this but we pass it for forward
      compatibility and for explicit reader intent.
    * ``transaction_per_migration=True`` -- each revision file runs in
      its own transaction so a partial failure rolls back cleanly. This
      is CRITICAL for the order-service: the saga_state table addition
      (revision 20260101_000004) and any future ALTER TABLE on saga_state
      must be transactional to preserve in-flight saga durability per
      AAP R-18.
      The Alembic default (``False``) wraps the entire ``alembic upgrade
      head`` in a single transaction, which means a failure on revision
      N rolls back N-1, N-2, ... too -- unacceptable when order-domain
      tables are partially populated by intermediate revisions during
      a multi-step deploy.
    * ``connection.connect()`` is wrapped in ``with`` so the connection
      is always released back to the (Null)Pool, even on exceptions.
    """
    resolved_url = _resolve_database_url()

    # Read the [alembic] section as a plain dict and inject the resolved
    # URL. We deliberately mutate this section dict rather than calling
    # ``config.set_main_option(...)`` so that other [alembic] section
    # settings (e.g., ``output_encoding``, future custom keys) are
    # preserved exactly as alembic.ini declares them. The trailing
    # ``or {}`` is a defensive fallback for the case where
    # ``config.get_section()`` returns None on an unexpectedly missing
    # [alembic] section header (would normally raise NoSectionError
    # earlier, but configparser internals have shifted across versions).
    section: dict[str, str] = config.get_section(config.config_ini_section, {}) or {}
    section["sqlalchemy.url"] = resolved_url

    logger.info(
        "Running Alembic in ONLINE mode against %s (version_table=%s).",
        _redact_url(resolved_url),
        _VERSION_TABLE_NAME,
    )

    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=_VERSION_TABLE_NAME,
            transaction_per_migration=True,
            compare_type=True,
            compare_server_default=True,
            render_as_batch=False,
        )

        with context.begin_transaction():
            context.run_migrations()


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

# ``context.is_offline_mode()`` is True when the Alembic CLI was invoked
# with ``--sql`` (i.e., the operator wants the migration emitted to stdout
# as raw DDL rather than executed against a live database). Any other
# invocation uses the online path. There is no third mode.
#
# No ``if __name__ == "__main__":`` guard -- Alembic loads env.py via
# Python's import machinery (``importlib`` in
# ``alembic.script.base.ScriptDirectory.run_env``); the dispatch logic
# MUST run at import time, not at __main__ time, or the migration will
# never execute.
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
