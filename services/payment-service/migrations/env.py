"""Alembic runtime environment for the Payment Service.

This module is loaded by the Alembic CLI (``alembic upgrade``, ``alembic
downgrade``, ``alembic revision``, etc.). It configures the database
connection, logging, and migration execution mode (online vs. offline) for
the Payment Service's private PostgreSQL database (``payment_db``,
encrypted at rest per AAP R-8).

Runtime contract:
    * The ``POSTGRES_URL`` environment variable MUST be set. Its value is
      the PostgreSQL connection URL in either of these forms::

          postgresql://user:pass@host:port/payment_db
          postgresql+psycopg://user:pass@host:port/payment_db

      The first form (bare ``postgresql://``) is rewritten to use the
      psycopg v3 SQLAlchemy dialect (``postgresql+psycopg://``) because the
      service pins ``psycopg[binary,pool]>=3.1.18`` (NOT psycopg2). See
      ``services/payment-service/requirements.txt``.
    * ``target_metadata`` is intentionally ``None``. Schema is defined
      imperatively in ``versions/<revision>.py`` via ``op.create_table()``
      / ``op.execute()`` calls; we do NOT use ``--autogenerate`` because
      every DDL change in the payments domain must be reviewed line-by-line
      for compliance and encryption-at-rest implications (AAP R-8).
    * ``version_table`` is set to ``alembic_version_payment_service`` (NOT
      the default ``alembic_version``) so multiple services can co-locate
      their migrations on a single Postgres instance during local
      development without colliding on a shared version table. This is the
      ONLY substantive behavioral difference from
      ``services/notification-service/migrations/env.py``; everything else
      (imports, regex patterns, dispatch logic, exit-code semantics) is
      intentionally kept near-identical for monorepo consistency.

Exit codes (relevant for K8s Job / initContainer observability):
    * ``2`` -- ``POSTGRES_URL`` environment variable is unset or empty.
      This is a configuration error and is distinct from migration
      failures so operators can filter the two with a simple ``$? -eq 2``
      check.
    * Non-zero (raised) -- ``POSTGRES_URL`` uses the legacy
      ``postgresql+psycopg2://`` driver scheme. The service does not pin
      psycopg2 and the run is aborted with an actionable ``RuntimeError``.

References:
    * AAP Section 0.4.4   -- payment_db schema (5 tables: ``payments``,
                             ``payment_attempts``, ``refunds``,
                             ``provider_webhooks``, ``idempotency_keys``)
    * AAP Section 0.5.2.5 -- Per-service migrations live under the owning
                             service's migrations/ folder
    * AAP R-8             -- Payment Service database must be encrypted at
                             rest; idempotency keys persisted for every
                             outbound provider call to support safe
                             retries without duplicate charges
    * AAP R-9             -- Migrations under owning service, applied
                             automatically on service startup or via a
                             dedicated migration job
    * AAP R-19            -- Fail fast on missing critical dependencies at
                             startup
    * AAP R-25            -- Secrets via environment variables, never in
                             source or configuration files
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
#: config. The Payment Service treats this rule with extra care: payment_db
#: is encrypted at rest (AAP R-8) and stores idempotency keys, refund
#: records, and provider webhook payloads. A leaked URL would be a Sev-1
#: security incident.
_DB_URL_ENV_VAR: Final[str] = "POSTGRES_URL"

#: Environment variable for optional Alembic log-level override. Operators
#: can set ``ALEMBIC_LOG_LEVEL=DEBUG`` in CI to surface verbose diagnostic
#: output without committing a change to ``alembic.ini``.
_LOG_LEVEL_ENV_VAR: Final[str] = "ALEMBIC_LOG_LEVEL"

#: Default log level if ``ALEMBIC_LOG_LEVEL`` is not set. WARNING keeps
#: stderr quiet during routine ``alembic upgrade head`` runs while still
#: surfacing actionable warnings.
_DEFAULT_LOG_LEVEL: Final[str] = "WARNING"

#: SQLAlchemy dialect identifier for psycopg v3. SQLAlchemy 2.x exposes the
#: psycopg3 driver under this name; the legacy psycopg2 driver uses
#: ``postgresql+psycopg2``.
_PSYCOPG3_DIALECT_SCHEME: Final[str] = "postgresql+psycopg"

#: Bare ``postgresql://`` scheme that must be rewritten to the psycopg3
#: dialect. Operators frequently paste in libpq-style URLs without the
#: SQLAlchemy ``+driver`` qualifier; we silently upgrade those rather than
#: failing because there is exactly one supported PostgreSQL driver in this
#: service (psycopg3).
_BARE_POSTGRES_SCHEME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^postgresql://",
    re.IGNORECASE,
)

#: Rejected driver scheme -- psycopg2 is NOT pinned in requirements.txt.
#: We refuse the run with an actionable ``RuntimeError`` instead of letting
#: SQLAlchemy raise an opaque ``ModuleNotFoundError: No module named
#: 'psycopg2'`` deep inside its plugin loader. This is the single most
#: impactful defense in this file because operators frequently inherit
#: connection-string templates that embed ``+psycopg2``.
_REJECTED_PSYCOPG2_SCHEME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^postgresql\+psycopg2://",
    re.IGNORECASE,
)

#: Service-specific Alembic version table name. Co-located dev databases
#: with multiple services (auth_db, user_db, payment_db, etc.) would
#: otherwise collide on the default ``alembic_version`` table when sharing
#: a single Postgres instance. Per the migrations folder spec mandate.
#:
#: This constant -- and its propagation to BOTH ``context.configure()``
#: calls below (offline + online paths) -- is the SINGLE most important
#: behavioral difference vs. ``services/notification-service/migrations/
#: env.py``. Forgetting to pass ``version_table=_VERSION_TABLE_NAME`` to
#: either ``context.configure()`` would silently fall back to the default
#: ``alembic_version`` table, breaking the multi-service local-dev
#: scenario.
_VERSION_TABLE_NAME: Final[str] = "alembic_version_payment_service"

#: Module-scope logger used for diagnostics emitted by env.py itself (as
#: opposed to Alembic's ``alembic.runtime.migration`` logger which reports
#: on revision execution). The ``alembic.env`` qualname slots cleanly under
#: the ``alembic`` parent logger configured by ``fileConfig`` below.
logger = logging.getLogger("alembic.env")

# ---------------------------------------------------------------------------
# Alembic Config object
# ---------------------------------------------------------------------------

# ``context.config`` exposes the values parsed from ``alembic.ini``. The
# object supports ``set_main_option()`` so we can inject a runtime-resolved
# ``sqlalchemy.url`` after we read POSTGRES_URL from the environment below.
config = context.config

# Bootstrap logging from alembic.ini's [loggers] / [handlers] / [formatters]
# sections. Alembic's default template wraps this in
# ``if config.config_file_name is not None`` to support the ``--raiseerr``
# programmatic API where no .ini file is present (e.g., when env.py is
# imported by a unit test that constructs ``Config()`` in-memory).
# ``disable_existing_loggers=False`` preserves any parent-process loggers
# (matters when Alembic is invoked from pytest or from an entrypoint
# script that already configured logging).
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# Allow operators to raise the log level via the ``ALEMBIC_LOG_LEVEL`` env
# var without editing alembic.ini (useful in CI for diagnosing failures).
# ``getattr`` resolves "DEBUG" / "INFO" / "WARNING" / "ERROR" / "CRITICAL"
# to their integer constants; an unknown name silently falls back to
# WARNING so a typo in CI does not silently produce a flood of DEBUG
# output.
_log_level_name = os.environ.get(_LOG_LEVEL_ENV_VAR, _DEFAULT_LOG_LEVEL).upper()
_log_level = getattr(logging, _log_level_name, logging.WARNING)
logging.getLogger("alembic").setLevel(_log_level)


# ---------------------------------------------------------------------------
# Database URL resolution
# ---------------------------------------------------------------------------


def _normalize_database_url(raw_url: str) -> str:
    """Ensure the URL uses the psycopg v3 SQLAlchemy dialect.

    The Payment Service pins ``psycopg[binary,pool]>=3`` (see
    ``../requirements.txt``). SQLAlchemy 2.0 exposes this driver under the
    ``postgresql+psycopg`` dialect name. Many operators paste in bare
    ``postgresql://...`` URLs (the libpq default); this helper rewrites
    them so SQLAlchemy picks the correct dialect.

    The function is intentionally narrow:

    * It does NOT validate host/port/database parts -- that is
      SQLAlchemy's job and producing a duplicate validator here would
      create drift.
    * It does NOT URL-encode credentials -- callers are responsible for
      providing a well-formed URL and must encode special characters in
      the password according to RFC 3986.
    * It DOES reject the legacy ``postgresql+psycopg2://`` scheme with an
      explicit ``RuntimeError`` because silent rewriting from psycopg2 to
      psycopg3 would mask a deployment-configuration error.

    Args:
        raw_url: The connection URL as read from the environment.

    Returns:
        The URL guaranteed to start with ``postgresql+psycopg://``.

    Raises:
        RuntimeError: If the caller supplied a ``postgresql+psycopg2://``
            URL. psycopg2 is NOT in requirements.txt and the service will
            not function with it; the failure must be explicit, not
            silent.
    """
    if _REJECTED_PSYCOPG2_SCHEME_PATTERN.match(raw_url):
        raise RuntimeError(
            "POSTGRES_URL uses the legacy 'postgresql+psycopg2://' driver, "
            "but the Payment Service pins 'psycopg[binary,pool]>=3' in "
            "requirements.txt. Replace 'postgresql+psycopg2://' with either "
            "'postgresql://' (bare; auto-upgraded to psycopg v3) or "
            "'postgresql+psycopg://' (explicit psycopg v3)."
        )
    if _BARE_POSTGRES_SCHEME_PATTERN.match(raw_url):
        return _BARE_POSTGRES_SCHEME_PATTERN.sub(
            f"{_PSYCOPG3_DIALECT_SCHEME}://",
            raw_url,
            count=1,
        )
    return raw_url


def _resolve_database_url() -> str:
    """Load the database URL from the environment and normalize the dialect.

    This function is called once during Alembic env.py import. A missing
    env var is treated as a fatal startup error (AAP R-19, R-25): the
    process writes a clear remediation message to stderr and exits with
    code ``2`` so that Kubernetes Job / initContainer operators can
    distinguish a missing-config failure (exit 2) from a migration
    execution failure (exit 1, raised by Alembic / SQLAlchemy / psycopg).

    Returns:
        A fully-qualified SQLAlchemy URL with the psycopg v3 dialect.
    """
    raw_url = os.environ.get(_DB_URL_ENV_VAR)
    if not raw_url:
        sys.stderr.write(
            f"FATAL: environment variable {_DB_URL_ENV_VAR} is not set.\n"
            "Set POSTGRES_URL to the payment_db connection URL, e.g.:\n"
            "  POSTGRES_URL=postgresql://user:pass@host:5432/payment_db\n"
            "See services/payment-service/.env.example for the template.\n"
        )
        sys.exit(2)
    return _normalize_database_url(raw_url)


# Resolve the URL at module import time so both online/offline branches see
# it. Resolving once also makes the failure mode (missing POSTGRES_URL)
# happen before Alembic prints any banner output, which is the
# user-friendly behavior for ``alembic ...`` invocations.
_database_url: Final[str] = _resolve_database_url()

# Inject the resolved URL into Alembic's config object so engine_from_config()
# picks it up through the normal [alembic] section. ``alembic.ini`` declares
# ``sqlalchemy.url =`` (empty) specifically to require this injection path
# and ensure no committed config file ever embeds credentials (AAP R-25).
config.set_main_option("sqlalchemy.url", _database_url)


# ---------------------------------------------------------------------------
# Target metadata
# ---------------------------------------------------------------------------

# Schema is defined imperatively in ``versions/<revision>.py`` via
# ``op.create_table()`` and ``op.execute()`` calls. Leaving this None
# disables ``--autogenerate``; revisions must be written by hand. This is
# the intended workflow for the payments domain because every DDL change
# must be reviewed line-by-line for compliance and encryption-at-rest
# implications (AAP R-8). The five tables -- ``payments``,
# ``payment_attempts``, ``refunds``, ``provider_webhooks``,
# ``idempotency_keys`` (AAP Section 0.4.4) -- include encrypted columns
# whose envelope-encryption wrappers cannot be expressed as standard
# SQLAlchemy ``Column`` definitions; manual revisions allow explicit
# ``op.execute()`` calls that emit the exact PostgreSQL DDL needed.
#
# DO NOT replace this with a ``MetaData()`` object -- it is a deliberate
# architectural decision that reconciles "Alembic uses SQLAlchemy under
# the hood" with "the runtime payment service code is encrypted-at-rest
# and audited line-by-line".
target_metadata = None


# ---------------------------------------------------------------------------
# Offline migration path
# ---------------------------------------------------------------------------


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode -- emit SQL to stdout instead of a DB.

    Useful for generating a SQL review script via::

        alembic upgrade head --sql > migration.sql

    No connection is opened; the URL is used only for dialect selection so
    Alembic can render PostgreSQL-flavored DDL (e.g., ``TIMESTAMPTZ``,
    ``JSONB``, ``ON CONFLICT``, ``BYTEA`` for the encrypted columns).

    Configuration notes:
        * ``version_table=_VERSION_TABLE_NAME`` ensures the offline-emitted
          SQL targets ``alembic_version_payment_service`` (NOT the default
          ``alembic_version``). This matters for ``--sql`` review scripts:
          an operator who pipes the script through ``psql`` against a
          shared dev Postgres must not collide with sibling services'
          version tables.
        * ``literal_binds=True`` inlines parameter values into the
          generated SQL because the resulting script is meant for human
          review and for replay through ``psql -f``; bound-parameter
          placeholders make the script unusable in those contexts.
        * ``dialect_opts={"paramstyle": "named"}`` pairs with literal_binds
          to produce ``:name`` style placeholders for any binds Alembic
          cannot literalize (e.g., user-defined Python objects); these
          are rare for the Payment Service's straightforward DDL.
        * ``compare_type=True`` and ``compare_server_default=True`` enable
          type / default diff detection if ``--autogenerate`` is ever
          turned on later; they are harmless for the manual-revision
          workflow.
        * ``render_as_batch=False`` because PostgreSQL supports true ALTER
          statements; batch mode is a SQLite workaround that produces
          noisy ``CREATE TABLE _alembic_tmp_<name>`` recipes.
    """
    context.configure(
        url=_database_url,
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
    """Run migrations against a live database connection.

    This is the standard path used by ``alembic upgrade head`` and is the
    code path executed in production by the Kubernetes initContainer / Job
    that brings the payment_db schema up to date before the service pods
    come online.

    A single synchronous SQLAlchemy engine is created from the [alembic]
    section of ``alembic.ini`` (which now contains the resolved
    ``sqlalchemy.url``), a connection is acquired, and Alembic's own
    transaction-management wraps each migration script.

    Notes:
        * ``version_table=_VERSION_TABLE_NAME`` ensures Alembic creates /
          reads / writes ``alembic_version_payment_service`` instead of
          the default ``alembic_version``. Multiple services (auth-service,
          user-service, notification-service, etc.) can share a single
          Postgres instance during local development without clashing on
          a shared bookkeeping table.
        * We use ``pool.NullPool`` -- migration runs are short-lived,
          typically single-process (initContainer / K8s Job), and do not
          benefit from connection pooling. NullPool also sidesteps a
          well-documented issue where DDL issued through a pooled
          connection can appear rolled back if the pool reclaims the
          connection mid-migration; NullPool is the migration community's
          standard recommendation for this reason.
        * SQLAlchemy + psycopg3 fully supports synchronous engines; we do
          NOT use the async engine here because Alembic's upstream
          execution model is synchronous (``context.run_migrations()``
          calls into ``MigrationContext._do_run_migrations`` synchronously).
          Async support requires a separate ``async def
          run_migrations_online()`` + ``asyncio.run()`` wrapper, which we
          do not need for PostgreSQL-only DDL.
        * ``transaction_per_migration=True`` wraps each revision's
          upgrade / downgrade in its own transaction. This is critical
          for forward-only safety: if one of several queued revisions
          fails, only that revision's changes are rolled back;
          previously-applied revisions remain durable. The default
          (``False``) wraps the entire ``alembic upgrade head`` in a
          single transaction, which means a failure on revision N rolls
          back N-1, N-2, ... too -- unacceptable when payment-domain
          tables are partially populated by intermediate revisions.
    """
    # ``config.get_section`` returns the parsed section as a plain dict;
    # passing the empty dict default keeps us defensive against an
    # unexpectedly missing [alembic] section (which would otherwise raise
    # ``NoSectionError`` from configparser internals).
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        # Ensure future=True (SQLAlchemy 2.0 style); alembic defaults to
        # this in 1.13+ but we pass it explicitly for forward
        # compatibility and to make the intent obvious to readers.
        future=True,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=_VERSION_TABLE_NAME,
            compare_type=True,
            compare_server_default=True,
            transaction_per_migration=True,
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
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
