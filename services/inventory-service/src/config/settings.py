"""Pydantic-settings configuration loader for the Inventory Service.

This module is the **single source of truth for runtime configuration** of
the Inventory Service. It defines a single immutable :class:`Settings`
object that materializes from a layered source hierarchy and makes every
runtime knob — service identity, logging, health, metrics, PostgreSQL
connectivity, Kafka broker / producer / consumer / retry policy, Schema
Registry, topic names, reservation lifecycle policy, HTTP client
resilience, JWT validation, observability (OpenTelemetry + Prometheus),
feature flags, and database migrations — available to the rest of the
application via the :func:`get_settings` factory.

Layered source hierarchy (highest precedence first)
---------------------------------------------------
1. **Init kwargs** — keyword arguments passed to ``Settings(...)``; only
   used in tests that need to bypass YAML and env overrides.
2. **Environment variables** — flat ``UPPER_SNAKE_CASE`` names declared in
   ``services/inventory-service/.env.example``; the canonical surface for
   secrets and per-environment overrides per AAP R-25. Variables are
   mapped onto the nested model structure by the :meth:`Settings._inject_env_vars`
   ``model_validator(mode="before")``.
3. **.env file** — optional ``services/inventory-service/.env`` for local
   development; **never** present in production.
4. **YAML defaults** — ``services/inventory-service/config/default.yaml``
   plus an optional ``services/inventory-service/config/<env>.yaml``
   overlay where ``<env>`` is the lowercased value of ``SERVICE_ENV``.
   Loaded by the custom :class:`_YamlSettingsSource` registered via
   :meth:`Settings.settings_customise_sources` as the **lowest** precedence
   source.

Fail-fast validation contract (AAP R-19)
----------------------------------------
Construction performs the following sequence; **any failure raises a
``ValidationError`` and aborts startup**:

a. YAML load via :class:`_YamlSettingsSource` (parse + structural
   normalisation). Parse errors raise :class:`RuntimeError` which Pydantic
   surfaces as a validation error.
b. Environment-variable overlay via
   :meth:`Settings._inject_env_vars`. Per-variable type coercion happens
   in :func:`_coerce_int`, :func:`_coerce_float`, :func:`_coerce_bool`,
   and :func:`_parse_csv`; coercion errors raise :class:`ValueError`.
c. Pydantic field validation — types, ranges, ``Literal`` enums.
d. ``model_validator(mode="after")`` checks on every nested class; the
   keystone validators are
   :meth:`KafkaProducerSettings._enforce_durability`,
   :meth:`KafkaConsumerSettings._enforce_manual_commit`,
   :meth:`KafkaSettings._enforce_top_level_manual_commit`, and the
   per-class ``_validate_backoffs`` checks on retry policy classes.
e. :meth:`Settings._cross_field_invariants` re-checks the AAP R-14 / R-17
   keystones after the environment overlay and reconciles
   ``ObservabilitySettings.otel.service_name`` with
   :attr:`ServiceSettings.name`.

Secrets-handling contract (AAP R-25)
------------------------------------
Every credential field is a :class:`pydantic.SecretStr` (or
``SecretStr | None``) so that the default Pydantic ``__repr__`` renders
the value as ``SecretStr('**********')`` and the secret never leaks into
log output or error messages. Concrete fields:

* :attr:`DatabaseSettings.user` and :attr:`DatabaseSettings.password`
* :attr:`DatabaseSettings.url_override` (a single-DSN escape hatch)
* :attr:`KafkaSettings.sasl_username` and :attr:`KafkaSettings.sasl_password`
* :attr:`SchemaRegistrySettings.auth_username` and
  :attr:`SchemaRegistrySettings.auth_password`

Producer-durability and consumer-manual-commit contracts
---------------------------------------------------------
* **AAP R-14** — :class:`KafkaProducerSettings` validators reject any
  configuration where ``acks != "all"`` or ``enable_idempotence is False``.
  The check happens twice: in
  :meth:`KafkaProducerSettings._enforce_durability` (catches YAML-only
  misconfiguration) and again in
  :meth:`Settings._cross_field_invariants` (catches env-var-overlay
  misconfiguration). Belt-and-suspenders, deliberate.
* **AAP R-17** — :class:`KafkaConsumerSettings` validators reject any
  configuration where ``enable_auto_commit is True``. The check happens
  three times: in :meth:`KafkaConsumerSettings._enforce_manual_commit`,
  in :meth:`KafkaSettings._enforce_top_level_manual_commit`, and in
  :meth:`Settings._cross_field_invariants`. AAP R-17 is the keystone of
  at-least-once delivery semantics for the saga events; losing the guard
  would silently break the platform.

Usage
-----
The canonical entry point is :func:`get_settings`::

    from src.config.settings import get_settings

    settings = get_settings()  # cached singleton
    pool = await asyncpg.create_pool(dsn=settings.database.dsn,
                                     min_size=settings.database.pool_min,
                                     max_size=settings.database.pool_max)

The function is wrapped in :func:`functools.lru_cache` so exactly one
:class:`Settings` instance exists per process. Tests that need an
isolated instance can call ``get_settings.cache_clear()`` between cases
to force re-construction with fresh environment-variable overlays. Never
construct ``Settings()`` directly outside of this factory.

AAP cross-references
--------------------
* **AAP Section 0.4.3** — Per-service configuration loading + fail-fast
  startup contract.
* **AAP Section 0.4.5** — Cross-cutting interceptors (correlation ID,
  JWT, retry, circuit breaker, structured logging) all consume settings
  defined here.
* **AAP Section 0.5.2.2 bullet 5** — Inventory Service implementation
  directive (this module is the foundation).
* **AAP R-13** — Correlation ID header configurable via
  :attr:`ObservabilitySettings.correlation_id_header`.
* **AAP R-14** — Schema Registry validation; producer durability
  enforced by validators on :class:`KafkaProducerSettings`.
* **AAP R-15** — Retry with exponential backoff + jitter;
  :class:`HttpRetrySettings`, :class:`OptimisticLockSettings`, and
  :class:`KafkaRetrySettings` carry the policy fields.
* **AAP R-16** — Circuit breaker thresholds in
  :class:`HttpCircuitBreakerSettings`.
* **AAP R-17** — Kafka consumer manual-commit + DLQ topology validators.
* **AAP R-19 (CRITICAL)** — Fail-fast at startup; missing required env
  vars raise during ``Settings()`` construction.
* **AAP R-20** — :attr:`ReservationSettings.expiry_scheduler_poll_interval_ms`
  and :attr:`ReservationSettings.expiry_ms` drive the canonical fallback
  path (background expiration scheduler).
* **AAP R-22** — :attr:`AuthSettings.jwks_cache_ttl_seconds` bounds the
  JWKS cache freshness window.
* **AAP R-25 (CRITICAL)** — Secrets exclusively via env-loaded
  ``Settings``; ``SecretStr`` for every credential field.
* **AAP R-26** — Structured JSON logs; :attr:`LoggingSettings.format`
  drives formatter selection.
* **AAP R-27** — :attr:`MetricsSettings.path` and
  :attr:`MetricsSettings.port` drive the Prometheus scrape endpoint.
* **AAP R-30** — Topic names follow ``<domain>.<verb>`` and mirror the
  AAP Section 0.4.2 integration matrix; defaults set verbatim in
  :class:`ConsumedTopicsSettings`, :class:`ProducedTopicsSettings`, and
  :class:`DlqTopicsSettings`.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote_plus

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


# =============================================================================
# Module-Level Path Constants
# =============================================================================
# These resolve once at import time and remain immutable for the process
# lifetime. They are independent of the process's current working directory
# so the YAML defaults file is locatable from any callsite (uvicorn worker,
# pytest, alembic migration, ad-hoc REPL, etc.).
# =============================================================================

_THIS_DIR: Path = Path(__file__).resolve().parent
"""Absolute path to this file's directory: ``services/inventory-service/src/config/``."""

_SERVICE_ROOT: Path = _THIS_DIR.parent.parent
"""Absolute path to the service root: ``services/inventory-service/``."""

_CONFIG_DIR: Path = _SERVICE_ROOT / "config"
"""Absolute path to the YAML config directory: ``services/inventory-service/config/``."""

_DEFAULT_YAML: Path = _CONFIG_DIR / "default.yaml"
"""Absolute path to the canonical defaults YAML: ``services/inventory-service/config/default.yaml``."""

_LOCAL_YAML_NAME: str = "{env}.yaml"
"""Per-environment override YAML filename pattern (e.g., ``local.yaml``,
``dev.yaml``, ``staging.yaml``, ``prod.yaml``). The brace placeholder is
substituted with the lowercased value of ``SERVICE_ENV`` by
:meth:`_YamlSettingsSource.__call__` at load time.
"""


# =============================================================================
# Helper Functions (private)
# =============================================================================
# All env-var reads, YAML parsing, dict merging, and primitive coercion live
# here. They are deliberately small, dependency-free, and side-effect-free
# (apart from the env-var reads, which are read-only) so they can be unit
# tested without a Pydantic Settings instance.
# =============================================================================


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML file and return its top-level mapping.

    Returns an empty dict if the file does not exist or is empty/null.
    Raises :class:`RuntimeError` on parse errors or if the top-level YAML
    document is not a mapping.

    Parameters
    ----------
    path:
        Absolute or relative path to the YAML file. Existence is tested
        with :meth:`pathlib.Path.is_file`; non-existent files yield an
        empty dict so optional overlays do not break startup.

    Raises
    ------
    RuntimeError
        On OS-level read errors, YAML parse errors, or when the parsed
        document is not a top-level mapping (e.g., a YAML list at the
        root). The original exception is chained via ``raise ... from``.
    """
    if not path.is_file():
        return {}
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Cannot read YAML file at {path}: {exc}") from exc
    try:
        loaded = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Invalid YAML in {path}: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise RuntimeError(
            f"Top-level YAML in {path} must be a mapping, got {type(loaded).__name__}"
        )
    return loaded


def _peek_environment() -> str:
    """Detect the ``SERVICE_ENV`` environment variable.

    Returns the lowercased value of ``SERVICE_ENV`` (or ``"local"`` if
    absent). This is a *pre-Settings* peek used ONLY by
    :meth:`_YamlSettingsSource.__call__` to decide which ``<env>.yaml``
    overlay to load on top of ``default.yaml``. Because the YAML source
    runs **before** Pydantic's own environment-variable source, we cannot
    use the validated :attr:`ServiceSettings.environment` value here — we
    need to know which YAML to load before any Pydantic field validation
    has happened.
    """
    return (os.environ.get("SERVICE_ENV") or "local").strip().lower()


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``overlay`` into ``base``, returning a new dict.

    For nested mappings the function recurses; for all other types
    (scalars, lists, tuples, ``None``, etc.) ``overlay`` wins. The original
    ``base`` and ``overlay`` arguments are **not mutated** — a new dict is
    returned. Callers can therefore reuse either argument after the merge.

    Used by :meth:`_YamlSettingsSource.__call__` to merge an optional
    per-environment overlay (``<env>.yaml``) on top of the canonical
    ``default.yaml`` defaults.
    """
    result: dict[str, Any] = dict(base)
    for key, overlay_value in overlay.items():
        base_value = result.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            result[key] = _deep_merge(base_value, overlay_value)
        else:
            result[key] = overlay_value
    return result


def _strip_env_suffix_keys(obj: Any) -> Any:
    """Recursively remove keys ending with ``_env`` from a nested structure.

    Some monorepo YAML files use ``<key>_env`` keys as inline documentation
    of which environment variable overrides ``<key>`` (e.g.,
    ``level_env: LOG_LEVEL`` next to ``level: INFO``). Those documentation
    keys MUST NOT make it into the loaded config — they would be rejected
    by :class:`pydantic.ConfigDict` when ``extra="forbid"`` is set, and
    they are simply noise when ``extra="ignore"`` is set.

    This helper strips them defensively before the dict reaches Pydantic.
    Inventory's ``default.yaml`` does not currently use this convention
    (its env-var documentation lives in inline ``# env: VAR_NAME``
    comments), but the helper is retained for forward compatibility with
    overlays that might adopt the convention.
    """
    if isinstance(obj, dict):
        return {k: _strip_env_suffix_keys(v) for k, v in obj.items() if not k.endswith("_env")}
    if isinstance(obj, list):
        return [_strip_env_suffix_keys(item) for item in obj]
    return obj


def _normalize_yaml_structure(data: dict[str, Any]) -> dict[str, Any]:
    """Reshape the loaded YAML to match the Pydantic BaseModel class structure.

    Inventory's ``default.yaml`` is structured for human readability —
    section names align directly with the top-level Settings field names —
    so this helper is mostly a defensive pass-through. The single active
    transformation is renaming ``migrations.run_on_startup`` to
    ``migrations.auto_run`` if the former is present and the latter is
    not. ``run_on_startup`` is the more discoverable name for operators
    reading the YAML; ``auto_run`` is the field name used in
    :class:`MigrationsSettings` for symmetry with monorepo conventions.

    Pass-through sections (no key renames):

    * ``service`` — name, port, environment, graceful_shutdown_timeout_seconds
    * ``logging`` — level, format
    * ``health`` — liveness_path, readiness_path, ready_timeout_ms
    * ``metrics`` — enabled, port, path
    * ``database`` — host, port, name, ssl_mode, pool_min, pool_max,
      pool_timeout_ms, statement_timeout_ms, lock_timeout_ms,
      idle_in_transaction_timeout_ms, application_name
    * ``kafka`` — bootstrap, client_id, group_id, security_protocol,
      auto_offset_reset, enable_auto_commit, max_poll_interval_ms,
      session_timeout_ms, max_poll_records, producer{...}, retry{...}
      (NOTE: producer/consumer/retry stay NESTED under ``kafka``).
    * ``schema_registry`` — url, cache_capacity (TOP-LEVEL, not nested).
    * ``topics`` — consumed{...}, produced{...}, dlq{...} (TOP-LEVEL with
      consumed/produced/dlq grouping preserved for clarity).
    * ``reservation`` — pass-through with ``optimistic_lock`` nested.
    * ``http_client`` — pass-through with ``circuit_breaker`` nested.
    * ``auth`` — pass-through (FLAT — audience, issuer, public_key_url,
      jwks_cache_ttl_seconds, leeway_seconds, required_scope_admin,
      required_scope_read; NO nested ``jwt`` sub-mapping).
    * ``observability`` — pass-through with ``otel`` nested.
    * ``features`` — pass-through (4 boolean flags).

    Returns a NEW dict; the input is not mutated.
    """
    if not isinstance(data, dict):
        return data
    out: dict[str, Any] = {}
    for top_key, top_value in data.items():
        if isinstance(top_value, dict):
            # Defensive shallow copy of every nested mapping so the
            # in-place migrations rename below cannot accidentally
            # mutate the caller's input.
            out[top_key] = dict(top_value)
        else:
            out[top_key] = top_value

    # Migrations: optional rename run_on_startup -> auto_run if present.
    migrations = out.get("migrations")
    if isinstance(migrations, dict):
        if "run_on_startup" in migrations and "auto_run" not in migrations:
            migrations["auto_run"] = migrations.pop("run_on_startup")

    return out


def _set_nested(values: dict[str, Any], dotted_path: str, value: Any) -> None:
    """Set a value into a nested dict using a dotted path.

    Example: ``_set_nested(v, "kafka.producer.acks", "all")`` writes the
    string ``"all"`` to ``v["kafka"]["producer"]["acks"]`` and creates
    intermediate ``"kafka"`` and ``"producer"`` dicts if they are absent
    or are not already mappings.

    Used by :meth:`Settings._inject_env_vars` to map flat
    ``UPPER_SNAKE_CASE`` environment variables onto the nested
    BaseModel structure declared on :class:`Settings`.
    """
    parts = dotted_path.split(".")
    cursor = values
    for part in parts[:-1]:
        existing = cursor.get(part)
        if not isinstance(existing, dict):
            existing = {}
            cursor[part] = existing
        cursor = existing
    cursor[parts[-1]] = value


def _coerce_int(raw: str | None) -> int | None:
    """Parse a string env var into an :class:`int`.

    Returns ``None`` if ``raw`` is ``None`` or empty/whitespace-only.
    Raises :class:`ValueError` on any non-integer string (delegated from
    :class:`int`).
    """
    if raw is None:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    return int(stripped)


def _coerce_float(raw: str | None) -> float | None:
    """Parse a string env var into a :class:`float`.

    Returns ``None`` if ``raw`` is ``None`` or empty/whitespace-only.
    Raises :class:`ValueError` on any non-numeric string (delegated from
    :class:`float`).
    """
    if raw is None:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    return float(stripped)


def _coerce_bool(raw: str | None) -> bool | None:
    """Parse a string env var into a :class:`bool`.

    Returns ``None`` if ``raw`` is ``None`` or empty/whitespace-only.

    * Truthy values (case-insensitive): ``true``, ``1``, ``yes``, ``on``,
      ``y``, ``t``.
    * Falsy values (case-insensitive): ``false``, ``0``, ``no``, ``off``,
      ``n``, ``f``.
    * Any other value raises :class:`ValueError`.
    """
    if raw is None:
        return None
    stripped = raw.strip().lower()
    if not stripped:
        return None
    if stripped in {"true", "1", "yes", "on", "y", "t"}:
        return True
    if stripped in {"false", "0", "no", "off", "n", "f"}:
        return False
    raise ValueError(f"Cannot coerce {raw!r} to bool")


def _parse_csv(raw: str | None) -> list[str] | None:
    """Parse a comma-separated env var into a list of stripped strings.

    Returns ``None`` if ``raw`` is ``None`` or empty/whitespace-only.
    Empty fragments after splitting (e.g. consecutive commas, trailing
    commas) are dropped so callers receive a clean list.
    """
    if raw is None:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    return [item.strip() for item in stripped.split(",") if item.strip()]


# =============================================================================
# Custom YAML Settings Source
# =============================================================================


class _YamlSettingsSource(PydanticBaseSettingsSource):
    """Custom :class:`PydanticBaseSettingsSource` that loads YAML defaults.

    Source ordering reproduced for context (see
    :meth:`Settings.settings_customise_sources`):

    1. ``init_settings`` — kwargs passed to ``Settings(...)`` (highest).
    2. ``env_settings`` — environment variables.
    3. ``dotenv_settings`` — optional ``.env`` file (dev only).
    4. **THIS source** — YAML defaults (lowest).

    On every ``Settings()`` construction this source loads:

    a. ``services/inventory-service/config/default.yaml`` (always, if it
       exists; absent or empty file yields an empty dict).
    b. An optional per-environment overlay
       ``services/inventory-service/config/<env>.yaml`` where ``<env>`` is
       the lowercased :func:`_peek_environment` result. The two files are
       deep-merged via :func:`_deep_merge`.
    c. The merged dict is run through :func:`_strip_env_suffix_keys` to
       drop any ``_env`` documentation keys (defensive — inventory's YAML
       does not use the convention) and through
       :func:`_normalize_yaml_structure` to rename
       ``migrations.run_on_startup`` to ``migrations.auto_run`` when
       present.

    AAP refs: 0.4.3 (config loading), R-19 (validation runs after merge).
    """

    def get_field_value(
        self,
        field: Any,
        field_name: str,
    ) -> tuple[Any, str, bool]:
        """Per-field accessor required by :class:`PydanticBaseSettingsSource`.

        Pydantic-settings calls this for each model field when iterating
        sources. Because we override :meth:`__call__` to return the entire
        merged dict in one pass (so Pydantic's own field-level merging
        handles the nesting), this method is a no-op that returns
        ``(None, field_name, False)`` to signal "no per-field value
        available — defer to the dict returned by ``__call__``".
        """
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Load and merge YAML sources, returning a Pydantic-ready dict.

        The returned mapping is suitable for direct merge with
        ``init_settings``, ``env_settings``, and ``dotenv_settings``: keys
        present in higher-precedence sources will override the values
        produced here.
        """
        defaults = _read_yaml(_DEFAULT_YAML)
        env = _peek_environment()
        overlay_path = _CONFIG_DIR / _LOCAL_YAML_NAME.format(env=env)
        # Skip the overlay step if the overlay path resolves to the
        # default file (would otherwise cause double-merge of identical
        # content).
        overlay = _read_yaml(overlay_path) if overlay_path != _DEFAULT_YAML else {}
        merged = _deep_merge(defaults, overlay)
        cleaned = _strip_env_suffix_keys(merged)
        normalized = _normalize_yaml_structure(cleaned)
        return normalized if isinstance(normalized, dict) else {}


# =============================================================================
# Nested BaseModel Settings Classes
# =============================================================================
# These classes form the structured surface of :class:`Settings`. They are
# declared in dependency-respecting order — leaf classes first, composite
# classes that aggregate leaves second — so the top-level :class:`Settings`
# class at the bottom can reference all of them without forward references.
#
# Each class follows the same pattern:
#
#  * ``model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)``
#    — ignore unknown fields (forward compat), strip surrounding whitespace
#    on string values.
#  * Class docstring naming the AAP rules satisfied.
#  * Field-level docstrings via ``Field(..., description="...")``.
#  * After-validators that enforce business invariants (AAP R-14, R-17).
#
# Importantly, NONE of these classes inherit from :class:`BaseSettings`.
# Only the top-level :class:`Settings` class is a :class:`BaseSettings`
# subclass — nested classes are :class:`BaseModel`. This is deliberate:
# :class:`BaseSettings` injects an env-var source into every model, and
# nested settings classes would fight the top-level :class:`Settings` for
# environment ownership.
# =============================================================================


class ServiceSettings(BaseModel):
    """Service identity and runtime parameters.

    Drives the ``service`` log field (AAP R-26), the FastAPI ``title`` in
    OpenAPI metadata, the production-mode docs/redoc disablement
    (consumed by ``src.main.create_app``), and the SIGTERM graceful
    shutdown deadline (consumed by ``src.main.lifespan``).
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    name: str = Field(
        default="inventory-service",
        description=(
            "Service name; used as the static log field 'service' (AAP "
            "R-13, R-26) and as the FastAPI app title."
        ),
    )
    port: int = Field(
        default=8000,
        ge=1,
        le=65535,
        description=(
            "HTTP port for FastAPI; matches Dockerfile EXPOSE 8000. "
            "Override via SERVICE_PORT for non-standard local-dev ports."
        ),
    )
    environment: Literal["local", "dev", "stage", "staging", "prod", "production", "test"] = Field(
        default="local",
        description=(
            "Deployment environment; gates docs_url/redoc_url disablement "
            "in production (see src.main.create_app). Both 'stage'/"
            "'staging' and 'prod'/'production' are accepted to match "
            "differing operator conventions."
        ),
    )
    graceful_shutdown_timeout_seconds: int = Field(
        default=30,
        ge=1,
        le=300,
        description=(
            "Maximum seconds to wait for in-flight HTTP requests + Kafka "
            "consumer drain on SIGTERM. Bounded [1, 300] to keep "
            "shutdown deterministic under k8s preStop hook constraints."
        ),
    )


class LoggingSettings(BaseModel):
    """Logging level and format (AAP R-26 structured JSON logs)."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    level: Literal["DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description=(
            "Python logging level. ``WARN`` is normalized to ``WARNING`` "
            "by :meth:`_uppercase_level` so operators can use either "
            "spelling. AAP R-26 mandates structured JSON logs at INFO+."
        ),
    )
    format: Literal["json", "text"] = Field(
        default="json",
        description=(
            "Log formatter selector consumed by "
            "``src.observability.logging_setup`` (AAP R-26). ``text`` is "
            "permitted for local development only — production "
            "deployments MUST emit JSON for Logstash ingestion."
        ),
    )

    @field_validator("level", mode="before")
    @classmethod
    def _uppercase_level(cls, v: Any) -> Any:
        """Normalize log level to uppercase and map ``WARN`` -> ``WARNING``.

        Accepts any case so operators can write ``info``, ``Info``,
        ``INFO``, etc. The ``WARN`` -> ``WARNING`` mapping aligns with
        Python stdlib's preferred spelling.
        """
        if isinstance(v, str):
            up = v.strip().upper()
            return "WARNING" if up == "WARN" else up
        return v


class HealthSettings(BaseModel):
    """Liveness and readiness probe paths (AAP R-19).

    Consumed by ``src.controllers.health.HealthController`` to mount the
    probe routes and by ``src.main.lifespan`` for the readiness deadline
    on dependency probing.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    liveness_path: str = Field(
        default="/health/live",
        description=(
            "Path served by ``HealthController`` for the k8s "
            "``livenessProbe`` (AAP R-19). Returns 200 OK as long as the "
            "process is alive — no dependency checks."
        ),
    )
    readiness_path: str = Field(
        default="/health/ready",
        description=(
            "Path served by ``HealthController`` for the k8s "
            "``readinessProbe`` (AAP R-19). Returns 200 OK only when "
            "PostgreSQL, Kafka, and the Schema Registry are all "
            "reachable."
        ),
    )
    ready_timeout_ms: int = Field(
        default=2000,
        ge=100,
        description=(
            "Maximum time in ms to spend probing dependencies during "
            "/health/ready. Beyond this, individual probes time out and "
            "the route returns 503."
        ),
    )


class MetricsSettings(BaseModel):
    """Prometheus metrics endpoint (AAP R-27).

    Consumed by ``src.observability.metrics`` to mount the
    ``CollectorRegistry`` route and by Metricbeat for scrape config.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    enabled: bool = Field(
        default=True,
        description=(
            "When False, the metrics route returns 404. Used by unit "
            "tests that need to spin up the FastAPI app without a "
            "registered metrics registry."
        ),
    )
    port: int = Field(
        default=9090,
        ge=1,
        le=65535,
        description=(
            "Optional separate port; the FastAPI app currently mounts "
            "the metrics route on the SERVICE_PORT, but a separate "
            "scrape port is supported for clusters that isolate "
            "scrape traffic."
        ),
    )
    path: str = Field(
        default="/metrics",
        description=(
            "HTTP path Metricbeat scrapes per AAP R-27. Standard "
            "Prometheus convention; do not change without "
            "coordinating with the scrape config in "
            "``infrastructure/elk/metricbeat/metricbeat.yml``."
        ),
    )


class DatabaseSettings(BaseModel):
    """PostgreSQL connection parameters for ``inventory_db`` (AAP 0.4.4, R-7).

    Inventory owns ``inventory_db`` with tables ``stock_items``,
    ``reservations``, ``warehouses``, and ``stock_movements``. Per AAP
    R-25, the ``user`` and ``password`` fields are
    :class:`pydantic.SecretStr` and MUST be supplied via environment
    variables (``POSTGRES_USER``, ``POSTGRES_PASSWORD``); the YAML never
    carries credentials.

    The container consumes :attr:`dsn` (a computed property) which
    constructs the libpq connection string from
    host/port/name/user/password/ssl_mode/application_name. Setting
    ``POSTGRES_URL`` short-circuits the construction: the override is
    returned verbatim, useful for secret managers that ship a single DSN
    string.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    host: str = Field(
        default="localhost",
        description="PostgreSQL host (env: POSTGRES_HOST).",
    )
    port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        description="PostgreSQL port (env: POSTGRES_PORT).",
    )
    name: str = Field(
        default="inventory_db",
        description=(
            "Database name (env: POSTGRES_DB). Must match the database "
            "created by Alembic migrations under "
            "``services/inventory-service/migrations/``."
        ),
    )
    user: SecretStr = Field(
        default=SecretStr(""),
        description=(
            "PostgreSQL user (env: POSTGRES_USER, secret per AAP R-25). "
            "Empty default forces a missing-credential failure when the "
            "DSN is consumed by the connection pool — fail-fast at "
            "first DB call rather than at startup so tests that mock "
            "the pool can run without setting this var."
        ),
    )
    password: SecretStr = Field(
        default=SecretStr(""),
        description=(
            "PostgreSQL password (env: POSTGRES_PASSWORD, secret per "
            "AAP R-25). Empty default; see ``user`` for rationale."
        ),
    )
    url_override: SecretStr | None = Field(
        default=None,
        description=(
            "If set (env: POSTGRES_URL), used directly as the DSN; "
            "takes precedence over the host/port/name/user/password "
            "construction. Useful when secret managers ship a single "
            "DSN string. SecretStr-typed because it embeds credentials."
        ),
    )
    ssl_mode: Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"] = Field(
        default="prefer",
        description=(
            "libpq sslmode (env: POSTGRES_SSL_MODE). Production should "
            "use ``require`` or stricter (AAP R-24). ``prefer`` keeps "
            "local-dev ergonomic when the dev PostgreSQL is plain TCP."
        ),
    )
    pool_min: int = Field(
        default=2,
        ge=1,
        description=(
            "Minimum connections in the asyncpg pool (env: "
            "POSTGRES_POOL_MIN). Bound below by 1 because zero would "
            "defeat the keep-alive purpose of a pool."
        ),
    )
    pool_max: int = Field(
        default=20,
        ge=1,
        description=(
            "Maximum connections in the asyncpg pool (env: "
            "POSTGRES_POOL_MAX). Should comfortably exceed the "
            "expected concurrent reservation throughput."
        ),
    )
    pool_timeout_ms: int = Field(
        default=5000,
        ge=100,
        description=(
            "Maximum ms to wait for a free connection from the pool "
            "before raising ``asyncio.TimeoutError`` (env: "
            "POSTGRES_POOL_TIMEOUT_MS)."
        ),
    )
    statement_timeout_ms: int = Field(
        default=5000,
        ge=100,
        description=(
            "Per-statement timeout (postgres ``SET statement_timeout``; "
            "env: POSTGRES_STATEMENT_TIMEOUT_MS). Caps the duration of "
            "any single SQL statement to defend against runaway queries."
        ),
    )
    lock_timeout_ms: int = Field(
        default=2000,
        ge=100,
        description=(
            "Per-statement lock timeout (postgres ``SET lock_timeout``; "
            "env: POSTGRES_LOCK_TIMEOUT_MS). Used by the optimistic-"
            "locking reservation path to bound time spent waiting on "
            "contended row locks."
        ),
    )
    idle_in_transaction_timeout_ms: int = Field(
        default=10000,
        ge=100,
        description=(
            "postgres ``SET idle_in_transaction_session_timeout`` "
            "(env: POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_MS) — caps "
            "stale TX duration to prevent long-running transactions "
            "from blocking VACUUM."
        ),
    )
    application_name: str = Field(
        default="inventory-service",
        description=(
            "Set on every connection via ``application_name=...``; "
            "visible in ``pg_stat_activity`` (env: "
            "POSTGRES_APPLICATION_NAME). Helps DB operators correlate "
            "connections to services."
        ),
    )

    @model_validator(mode="after")
    def _validate_pool_sizes(self) -> "DatabaseSettings":
        """Ensure ``pool_max >= pool_min``.

        A pool sized below its minimum would be under-provisioned and
        could fail to start under contention. We catch the
        misconfiguration at startup rather than at first DB call.
        """
        if self.pool_max < self.pool_min:
            raise ValueError(
                f"DatabaseSettings.pool_max ({self.pool_max}) "
                f"must be >= pool_min ({self.pool_min})"
            )
        return self

    @property
    def dsn(self) -> str:
        """Construct a libpq DSN string for psycopg / asyncpg.

        If ``url_override`` is set, returns it verbatim; otherwise
        constructs from host/port/name/user/password/ssl_mode. The
        DSN includes ``application_name`` so all connections appear
        with the inventory-service identity in ``pg_stat_activity``.

        Credentials are URL-encoded via :func:`urllib.parse.quote_plus`
        so passwords containing reserved characters (``:`` ``/`` ``@``
        ``?`` ``&`` ``=`` ``%`` etc.) do not break DSN parsing.

        AAP R-25: The DSN contains a secret password and MUST NOT be
        logged. Callers that emit this value to logs are violating the
        secret-handling contract.
        """
        override = (
            self.url_override.get_secret_value().strip()
            if self.url_override is not None
            else ""
        )
        if override:
            return override
        user = self.user.get_secret_value()
        password = self.password.get_secret_value()
        # Percent-encode credentials in case they contain reserved chars.
        u = quote_plus(user) if user else ""
        p = quote_plus(password) if password else ""
        if u and p:
            creds = f"{u}:{p}@"
        elif u:
            creds = f"{u}@"
        else:
            creds = ""
        return (
            f"postgresql://{creds}{self.host}:{self.port}/{self.name}"
            f"?sslmode={self.ssl_mode}"
            f"&application_name={quote_plus(self.application_name)}"
        )


class KafkaProducerSettings(BaseModel):
    """Kafka producer configuration (AAP R-14: idempotent + ``acks=all``).

    The :meth:`_enforce_durability` after-validator REJECTS any
    configuration that would violate AAP R-14 producer durability —
    specifically, ``acks != "all"`` or ``enable_idempotence is False``.
    The check is duplicated in :meth:`Settings._cross_field_invariants`
    to catch env-var-overlay misconfiguration that bypasses this
    nested-class validator (which fires only on YAML-supplied values).
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    acks: Literal["0", "1", "all"] = Field(
        default="all",
        description=(
            "Producer ``acks``; AAP R-14 requires ``\"all\"`` so the "
            "producer waits for all in-sync replicas to acknowledge "
            "before the send completes — guarantees no message loss "
            "on a leader broker failure."
        ),
    )
    enable_idempotence: bool = Field(
        default=True,
        description=(
            "AAP R-14 requires the idempotent producer (delivery "
            "guarantees of exactly-once when combined with a "
            "transactional producer)."
        ),
    )
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = Field(
        default="zstd",
        description=(
            "Compression codec; ``zstd`` offers the best ratio/CPU "
            "tradeoff for event payloads carrying JSON or Avro-encoded "
            "domain events."
        ),
    )
    linger_ms: int = Field(
        default=10,
        ge=0,
        le=10000,
        description=(
            "Producer batching linger in ms. Higher values improve "
            "throughput at the cost of latency."
        ),
    )
    batch_size: int = Field(
        default=65536,
        ge=1,
        description="Producer batch size in bytes.",
    )
    delivery_timeout_ms: int = Field(
        default=120000,
        ge=1000,
        description=(
            "Producer end-to-end delivery timeout. Must be larger than "
            "``request_timeout_ms`` plus any retry budget."
        ),
    )
    request_timeout_ms: int = Field(
        default=30000,
        ge=1000,
        description="Per-request timeout for individual Kafka produces.",
    )
    max_in_flight_requests_per_connection: int = Field(
        default=5,
        ge=1,
        le=5,
        description=(
            "MUST be <= 5 when ``enable_idempotence=True`` (Kafka "
            "constraint to keep batches in-order on retries)."
        ),
    )

    @model_validator(mode="after")
    def _enforce_durability(self) -> "KafkaProducerSettings":
        """REJECT misconfiguration that would violate AAP R-14 durability.

        Producer durability is the keystone of the saga events' delivery
        guarantee — a non-``all`` ack or non-idempotent producer would
        permit silent message loss on a broker failure, which would
        manifest as orphaned reservations or duplicate stock decrements.
        """
        if self.acks != "all":
            raise ValueError(
                f"KafkaProducerSettings.acks must be 'all' per AAP R-14 "
                f"(got: {self.acks!r})"
            )
        if not self.enable_idempotence:
            raise ValueError(
                "KafkaProducerSettings.enable_idempotence must be True "
                "per AAP R-14"
            )
        return self


class KafkaConsumerSettings(BaseModel):
    """Kafka consumer configuration (AAP R-17: manual commit + DLQ routing).

    The :meth:`_enforce_manual_commit` after-validator REJECTS any
    configuration where ``enable_auto_commit is True``. The check is
    duplicated in :meth:`KafkaSettings._enforce_top_level_manual_commit`
    and :meth:`Settings._cross_field_invariants` to catch
    env-var-overlay misconfiguration. AAP R-17 is the keystone of
    at-least-once delivery semantics for the saga events.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    auto_offset_reset: Literal["earliest", "latest", "none"] = Field(
        default="earliest",
        description=(
            "Where to start when the consumer has no committed offset. "
            "``earliest`` is correct for the inventory consumer because "
            "missing a saga step's ``order.created`` event would leave "
            "stock un-reserved indefinitely."
        ),
    )
    enable_auto_commit: bool = Field(
        default=False,
        description=(
            "MUST be False per AAP R-17 — the consumer commits offsets "
            "manually only after the reservation transaction is "
            "durably persisted, ensuring at-least-once delivery."
        ),
    )
    max_poll_interval_ms: int = Field(
        default=300000,
        ge=1000,
        description="Max time between consumer polls before rebalance.",
    )
    session_timeout_ms: int = Field(
        default=45000,
        ge=1000,
        description=(
            "Consumer session timeout. After this period of inactivity "
            "the broker considers the consumer dead and triggers a "
            "rebalance."
        ),
    )
    max_poll_records: int = Field(
        default=100,
        ge=1,
        le=10000,
        description=(
            "Max records returned per poll. Bounded to keep individual "
            "poll cycles short and reduce rebalance latency."
        ),
    )

    @model_validator(mode="after")
    def _enforce_manual_commit(self) -> "KafkaConsumerSettings":
        """REJECT auto-commit; AAP R-17 mandates manual commit.

        Auto-commit violates the at-least-once contract because the
        consumer can advance the offset *before* the message has been
        durably persisted. A crash mid-processing would silently lose
        the unprocessed message — manifesting as missing reservations
        or unreleased stock.
        """
        if self.enable_auto_commit:
            raise ValueError(
                "KafkaConsumerSettings.enable_auto_commit must be False "
                "per AAP R-17 (manual offset commit only after "
                "reservation transaction is durably persisted)"
            )
        return self




class KafkaRetrySettings(BaseModel):
    """Kafka consumer retry / DLQ topology (AAP R-17).

    Defines the suffixes used to derive the retry topic name and the
    DLQ topic name from a source topic, plus the bounded exponential
    backoff applied between attempts. The retry topic and DLQ topic
    for a given source ``order.created`` are
    ``order.created.retry`` and ``order.created.dlq`` respectively
    (with the default suffix configuration).
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    topic_suffix: str = Field(
        default=".retry",
        description=(
            "Suffix appended to the source topic name to form the "
            "retry topic (e.g., ``order.created`` -> "
            "``order.created.retry``)."
        ),
    )
    dlq_topic_suffix: str = Field(
        default=".dlq",
        description=(
            "Suffix appended to the source topic name to form the "
            "per-source DLQ topic (e.g., ``order.created`` -> "
            "``order.created.dlq``). Distinct from the service-wide "
            "DLQ ``inventory.dlq`` configured in :class:`DlqTopicsSettings`."
        ),
    )
    max_attempts: int = Field(
        default=5,
        ge=1,
        le=20,
        description=(
            "Max retry attempts before the message is routed to the "
            "DLQ. Bounded [1, 20] to prevent runaway retry loops on "
            "permanently poison messages."
        ),
    )
    backoff_initial_ms: int = Field(
        default=500,
        ge=10,
        description="Initial retry backoff in ms.",
    )
    backoff_max_ms: int = Field(
        default=30000,
        ge=100,
        description="Max retry backoff in ms (caps exponential growth).",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        description=(
            "Exponential backoff multiplier. ``2.0`` doubles the delay "
            "between attempts (capped by ``backoff_max_ms``)."
        ),
    )
    jitter_ms: int = Field(
        default=250,
        ge=0,
        description=(
            "Jitter added uniformly to each backoff sleep to prevent "
            "thundering-herd retries on a recovering broker."
        ),
    )

    @model_validator(mode="after")
    def _validate_backoffs(self) -> "KafkaRetrySettings":
        """Ensure ``backoff_max_ms >= backoff_initial_ms``."""
        if self.backoff_max_ms < self.backoff_initial_ms:
            raise ValueError(
                f"KafkaRetrySettings.backoff_max_ms ({self.backoff_max_ms}) "
                f"must be >= backoff_initial_ms ({self.backoff_initial_ms})"
            )
        return self


class KafkaSettings(BaseModel):
    """Kafka top-level configuration (AAP R-14, R-17).

    Aggregates broker connectivity (bootstrap, security, SASL) plus the
    nested :class:`KafkaProducerSettings`, :class:`KafkaConsumerSettings`,
    and :class:`KafkaRetrySettings`. The top-level ``enable_auto_commit``
    field is duplicated from :class:`KafkaConsumerSettings.enable_auto_commit`
    purely for direct ``settings.kafka.enable_auto_commit`` access in the
    container; both must be False per AAP R-17.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        protected_namespaces=(),
    )

    bootstrap: str = Field(
        default="localhost:9092",
        description=(
            "Comma-separated broker list (env: KAFKA_BOOTSTRAP). "
            "Example: ``broker-1:9092,broker-2:9092,broker-3:9092``."
        ),
    )
    client_id: str = Field(
        default="inventory-service",
        description=(
            "Kafka ``client.id``; appears in broker logs and metrics "
            "for traffic attribution."
        ),
    )
    group_id: str = Field(
        default="inventory-service",
        description=(
            "Kafka consumer group; the consumer uses this to track "
            "offsets across rebalances. Stable across deployments — "
            "changing the group id resets all consumer offsets."
        ),
    )
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = Field(
        default="PLAINTEXT",
        description=(
            "Wire-protocol security; production deployments MUST use "
            "``SASL_SSL`` (AAP R-24). ``PLAINTEXT`` is permitted only "
            "on a private cluster network."
        ),
    )
    sasl_mechanism: str | None = Field(
        default=None,
        description=(
            "SASL mechanism (e.g., ``PLAIN``, ``SCRAM-SHA-256``, "
            "``SCRAM-SHA-512``). Required when ``security_protocol`` is "
            "``SASL_PLAINTEXT`` or ``SASL_SSL``."
        ),
    )
    sasl_username: SecretStr | None = Field(
        default=None,
        description=(
            "SASL username (env: KAFKA_SASL_USERNAME, secret per "
            "AAP R-25)."
        ),
    )
    sasl_password: SecretStr | None = Field(
        default=None,
        description=(
            "SASL password (env: KAFKA_SASL_PASSWORD, secret per "
            "AAP R-25)."
        ),
    )
    ssl_ca_location: str | None = Field(
        default=None,
        description=(
            "Path to the CA bundle for TLS verification. Required "
            "when ``security_protocol`` includes ``SSL``."
        ),
    )

    auto_offset_reset: Literal["earliest", "latest", "none"] = Field(
        default="earliest",
        description=(
            "Where to start when the consumer has no committed offset. "
            "Top-level convenience for direct container access; "
            "mirrored to :attr:`KafkaConsumerSettings.auto_offset_reset` "
            "by :meth:`Settings._inject_env_vars`."
        ),
    )
    enable_auto_commit: bool = Field(
        default=False,
        description=(
            "MUST be False per AAP R-17. Top-level convenience for "
            "direct container access. Mirrored to "
            ":attr:`KafkaConsumerSettings.enable_auto_commit`."
        ),
    )
    max_poll_interval_ms: int = Field(
        default=300000,
        ge=1000,
        description="Top-level convenience mirror of consumer setting.",
    )
    session_timeout_ms: int = Field(
        default=45000,
        ge=1000,
        description="Top-level convenience mirror of consumer setting.",
    )
    max_poll_records: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Top-level convenience mirror of consumer setting.",
    )

    producer: KafkaProducerSettings = Field(
        default_factory=KafkaProducerSettings,
        description="Producer configuration (AAP R-14 enforcer).",
    )
    consumer: KafkaConsumerSettings = Field(
        default_factory=KafkaConsumerSettings,
        description="Consumer configuration (AAP R-17 enforcer).",
    )
    retry: KafkaRetrySettings = Field(
        default_factory=KafkaRetrySettings,
        description="Consumer retry / DLQ topology (AAP R-17).",
    )

    @model_validator(mode="after")
    def _enforce_top_level_manual_commit(self) -> "KafkaSettings":
        """Ensure the top-level ``enable_auto_commit`` is False.

        AAP R-17 keystone — duplicates the check on
        :class:`KafkaConsumerSettings` and :class:`Settings` so that any
        of the three layers can catch the misconfiguration first
        depending on how the value flows in (YAML, env var, init kwargs).
        """
        if self.enable_auto_commit:
            raise ValueError(
                "KafkaSettings.enable_auto_commit must be False per AAP R-17"
            )
        return self


class SchemaRegistrySettings(BaseModel):
    """Confluent Schema Registry configuration (AAP R-14).

    Schema Registry hosts the canonical Avro / JSON Schema definitions
    for every Kafka event the platform produces; producers fetch the
    schema before encoding and consumers fetch it before decoding. The
    cache capacity bounds in-memory schema retention.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    url: str = Field(
        default="http://localhost:8081",
        description=(
            "Schema Registry URL (env: SCHEMA_REGISTRY_URL). Production "
            "deployments use the cluster-internal HTTPS URL."
        ),
    )
    auth_username: SecretStr | None = Field(
        default=None,
        description=(
            "Optional basic-auth username (env: "
            "SCHEMA_REGISTRY_AUTH_USERNAME, secret per AAP R-25)."
        ),
    )
    auth_password: SecretStr | None = Field(
        default=None,
        description=(
            "Optional basic-auth password (env: "
            "SCHEMA_REGISTRY_AUTH_PASSWORD, secret per AAP R-25)."
        ),
    )
    cache_capacity: int = Field(
        default=1000,
        ge=1,
        description=(
            "In-memory schema cache size. Bounded to prevent unbounded "
            "growth on long-running consumers that see many schema "
            "evolutions."
        ),
    )


class ConsumedTopicsSettings(BaseModel):
    """Topic names this service consumes (AAP Section 0.4.2).

    The Inventory Service is a saga **participant** — it consumes
    ``order.*`` events and emits ``inventory.*`` outcome events. The
    three topics here are the saga steps that drive the reservation
    state machine forward.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    order_created: str = Field(
        default="order.created",
        description=(
            "Saga reservation trigger. The consumer attempts to reserve "
            "stock atomically and emits ``inventory.reserved`` on "
            "success or ``inventory.reservation_failed`` on insufficient "
            "stock."
        ),
    )
    order_cancelled: str = Field(
        default="order.cancelled",
        description=(
            "Saga compensation trigger. Releases an active reservation "
            "back to available stock and emits ``inventory.released``."
        ),
    )
    order_fulfilled: str = Field(
        default="order.fulfilled",
        description=(
            "Saga finalize trigger. Terminally releases a reservation "
            "(stock has shipped — NOT returned to availability) and "
            "emits ``inventory.released`` with the final flag set."
        ),
    )


class ProducedTopicsSettings(BaseModel):
    """Topic names this service produces (AAP R-30: ``<domain>.<verb>``).

    The four ``inventory.*`` topics produced by this service drive the
    saga forward (``inventory.reserved`` and
    ``inventory.reservation_failed`` advance the saga step), trigger
    notifications (``inventory.released``), and feed operational alerts
    (``inventory.low-stock``).
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    inventory_reserved: str = Field(
        default="inventory.reserved",
        description=(
            "Reservation success outcome. Consumed by the Order Service "
            "to advance the saga to the next step (typically payment)."
        ),
    )
    inventory_reservation_failed: str = Field(
        default="inventory.reservation_failed",
        description=(
            "Reservation failure outcome (insufficient stock). Consumed "
            "by the Order Service for saga compensation (typically "
            "marks the order as cancelled and notifies the customer)."
        ),
    )
    inventory_released: str = Field(
        default="inventory.released",
        description=(
            "Stock release on cancel/fulfill/expire. Carries an "
            "``expired=true`` flag for releases driven by the "
            "expiration scheduler (AAP R-20)."
        ),
    )
    inventory_low_stock: str = Field(
        default="inventory.low-stock",
        description=(
            "Low-stock alert (NOTE: hyphenated ``low-stock`` per AAP "
            "Section 0.4.2 verbatim). Consumed by the Notification "
            "Service for ops alerts and by the Recommendation Engine "
            "to suppress recommendations for low-stock items."
        ),
    )


class DlqTopicsSettings(BaseModel):
    """Service-wide DLQ topic names (AAP R-17).

    The single ``inventory.dlq`` topic is the terminal destination for
    poison messages — events that fail Schema Registry validation
    entirely or whose payload cannot be parsed. Per-source DLQ topics
    (``<source>.dlq``) are derived dynamically by appending
    :attr:`KafkaRetrySettings.dlq_topic_suffix` to each consumed topic.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    inventory_dlq: str = Field(
        default="inventory.dlq",
        description=(
            "Service-wide DLQ for poison messages. Alerted on; replayed "
            "via the operational runbook in "
            "``docs/runbook/inventory-service.md``."
        ),
    )


class TopicsSettings(BaseModel):
    """All Kafka topic names (consumed + produced + DLQ).

    Top-level container that groups :class:`ConsumedTopicsSettings`,
    :class:`ProducedTopicsSettings`, and :class:`DlqTopicsSettings`.
    Consumers and producers reference these via the Pydantic model
    rather than literal strings to avoid drift between the schema
    definitions in ``infrastructure/kafka/schemas/`` and the runtime.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    consumed: ConsumedTopicsSettings = Field(
        default_factory=ConsumedTopicsSettings,
        description="Topics this service consumes (saga input events).",
    )
    produced: ProducedTopicsSettings = Field(
        default_factory=ProducedTopicsSettings,
        description="Topics this service produces (saga output events).",
    )
    dlq: DlqTopicsSettings = Field(
        default_factory=DlqTopicsSettings,
        description="Service-wide DLQ topic name(s).",
    )




class OptimisticLockSettings(BaseModel):
    """Stock optimistic-lock retry policy (AAP R-15).

    The reservation update path uses the ``stock_items.version`` column
    for optimistic concurrency control. On version conflict the
    repository retries up to :attr:`max_retries` times with bounded
    exponential backoff plus jitter before raising ``OptimisticLockError``
    (which is ``is_retryable=True`` and routes the offending Kafka
    message to the retry topic for a slower retry).

    The default ``max_retries=5`` with ``backoff_initial_ms=25`` and
    ``jitter_ms=10`` is calibrated for typical e-commerce SKU
    contention. Flash-sale workloads that spike contention may demand
    higher ``max_retries`` to keep success rates high; tune via the
    ``STOCK_OPTIMISTIC_LOCK_*`` env vars.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    max_retries: int = Field(
        default=5,
        ge=1,
        le=20,
        description=(
            "Max retries before raising ``OptimisticLockError``. "
            "Bounded [1, 20] to keep retry storms bounded under "
            "sustained contention."
        ),
    )
    backoff_initial_ms: int = Field(
        default=25,
        ge=1,
        description=(
            "Initial retry backoff in ms (env: "
            "STOCK_OPTIMISTIC_LOCK_BACKOFF_INITIAL_MS). Small default "
            "because optimistic-lock conflicts typically resolve in "
            "the next attempt."
        ),
    )
    backoff_max_ms: int = Field(
        default=500,
        ge=10,
        description=(
            "Max retry backoff in ms. Caps exponential growth; the "
            "default ``500`` keeps the worst-case retry budget under "
            "~5s for the default 5 attempts."
        ),
    )
    jitter_ms: int = Field(
        default=10,
        ge=0,
        description=(
            "Jitter added uniformly to each backoff sleep to break "
            "lockstep retries between concurrent contenders."
        ),
    )

    @model_validator(mode="after")
    def _validate_backoffs(self) -> "OptimisticLockSettings":
        """Ensure ``backoff_max_ms >= backoff_initial_ms``."""
        if self.backoff_max_ms < self.backoff_initial_ms:
            raise ValueError(
                f"OptimisticLockSettings.backoff_max_ms "
                f"({self.backoff_max_ms}) must be >= "
                f"backoff_initial_ms ({self.backoff_initial_ms})"
            )
        return self


class ReservationSettings(BaseModel):
    """Reservation lifecycle policy (AAP R-20 — canonical fallback path).

    Drives the reservation engine and the background expiration
    scheduler. Without these settings, a stuck saga upstream — for
    example, a Payment Service outage between ``inventory.reserved`` and
    ``payment.succeeded`` — would freeze stock indefinitely. The
    expiration scheduler is the deadline-driven complement to the
    saga's explicit compensation events.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    low_stock_threshold_default: int = Field(
        default=10,
        ge=0,
        description=(
            "Default per-SKU/per-warehouse threshold for emitting "
            "``inventory.low-stock`` events. Per-row override lives in "
            "``stock_items.low_stock_threshold``; this default applies "
            "when the column is NULL."
        ),
    )
    expiry_ms: int = Field(
        default=900000,
        ge=1000,
        description=(
            "Reservation TTL in ms (default 15 min). Reservations not "
            "finalized by ``order.fulfilled`` within this window are "
            "released by the expiration scheduler. Do not set below "
            "1s — the scheduler poll interval would dominate."
        ),
    )
    expiry_scheduler_poll_interval_ms: int = Field(
        default=60000,
        ge=1000,
        description=(
            "How often the expiry scheduler scans for expired "
            "reservations. The default ``60000`` (60s) balances "
            "release latency against DB scan load."
        ),
    )
    expiry_scheduler_batch_size: int = Field(
        default=200,
        ge=1,
        le=10000,
        description=(
            "Max reservations released per scheduler tick. Bounded "
            "[1, 10000] to keep individual transactions short and "
            "the lock window narrow."
        ),
    )
    default_warehouse_adapter: str = Field(
        default="database",
        description=(
            "Adapter type used when ``warehouses.adapter_type`` is "
            "NULL. Maps to the registered ``WarehouseAdapter`` "
            "implementation key (``database`` for the default "
            "in-DB adapter; future ``external_wms`` for a third-party "
            "WMS bridge)."
        ),
    )

    optimistic_lock: OptimisticLockSettings = Field(
        default_factory=OptimisticLockSettings,
        description=(
            "Stock optimistic-lock retry policy (AAP R-15). Drives "
            "the bounded retry behavior on ``stock_items.version`` "
            "conflicts."
        ),
    )


class HttpRetrySettings(BaseModel):
    """Outbound HTTP retry policy (AAP R-15).

    Currently consumed only by the JWKS fetch from the Auth Service;
    reserved for future external warehouse adapters. The bounded
    exponential backoff + jitter pattern matches the platform-wide
    convention defined in
    ``docs/architecture/resilience-patterns.md``.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    max_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description=(
            "Max retry attempts. Bounded [1, 10]; HTTP retries are "
            "more expensive than Kafka retries because they tie up "
            "an HTTP client connection during the wait."
        ),
    )
    backoff_initial_ms: int = Field(
        default=100,
        ge=10,
        description="Initial retry backoff in ms.",
    )
    backoff_max_ms: int = Field(
        default=2000,
        ge=100,
        description="Max retry backoff in ms.",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        description="Exponential backoff multiplier.",
    )
    backoff_jitter_ms: int = Field(
        default=50,
        ge=0,
        description=(
            "Jitter added uniformly to each backoff sleep to prevent "
            "thundering-herd retries on a recovering upstream."
        ),
    )

    @model_validator(mode="after")
    def _validate_backoffs(self) -> "HttpRetrySettings":
        """Ensure ``backoff_max_ms >= backoff_initial_ms``."""
        if self.backoff_max_ms < self.backoff_initial_ms:
            raise ValueError(
                f"HttpRetrySettings.backoff_max_ms ({self.backoff_max_ms}) "
                f"must be >= backoff_initial_ms ({self.backoff_initial_ms})"
            )
        return self


class HttpCircuitBreakerSettings(BaseModel):
    """Circuit breaker thresholds for outbound HTTP (AAP R-16).

    The breaker has three states — CLOSED (passing through),
    OPEN (failing fast for ``reset_timeout_ms``), and HALF_OPEN
    (allowing :attr:`half_open_max_calls` probe calls before
    transitioning to CLOSED on success or back to OPEN on failure).
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    failure_threshold: int = Field(
        default=5,
        ge=1,
        description=(
            "Consecutive failures before the breaker transitions "
            "from CLOSED to OPEN."
        ),
    )
    reset_timeout_ms: int = Field(
        default=30000,
        ge=100,
        description=(
            "Time in ms to wait in OPEN state before transitioning "
            "to HALF_OPEN to send a probe."
        ),
    )
    half_open_max_calls: int = Field(
        default=3,
        ge=1,
        description=(
            "Max calls allowed in HALF_OPEN state before either "
            "closing the breaker (on success) or re-opening it (on "
            "failure)."
        ),
    )


class HttpClientSettings(BaseModel):
    """Outbound HTTP client configuration (AAP R-15, R-16).

    Currently used only for JWKS fetch from the Auth Service; reserved
    for future external warehouse adapters that bridge to a third-party
    WMS. The top-level retry / backoff fields are mirrored from the
    nested :class:`HttpRetrySettings` for direct container access; the
    nested instance carries the canonical values.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    connect_timeout_ms: int = Field(
        default=2000,
        ge=100,
        description="Connection establishment timeout.",
    )
    read_timeout_ms: int = Field(
        default=5000,
        ge=100,
        description=(
            "Read timeout per request — caps the wait for the first "
            "byte of the response."
        ),
    )
    total_timeout_ms: int = Field(
        default=10000,
        ge=100,
        description=(
            "End-to-end request budget — caps the total time spent "
            "on a single request including any retries."
        ),
    )
    user_agent: str = Field(
        default="inventory-service/1.0",
        description=(
            "User-Agent header for outbound requests. Helps upstream "
            "services attribute traffic for ops dashboards."
        ),
    )
    max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description=(
            "Convenience top-level retry count (also see the nested "
            ":attr:`retry` field for the canonical policy). Mirrored "
            "from :attr:`HttpRetrySettings.max_attempts` by "
            ":meth:`Settings._inject_env_vars`."
        ),
    )
    backoff_initial_ms: int = Field(
        default=100,
        ge=10,
        description="Mirror of :attr:`HttpRetrySettings.backoff_initial_ms`.",
    )
    backoff_max_ms: int = Field(
        default=2000,
        ge=100,
        description="Mirror of :attr:`HttpRetrySettings.backoff_max_ms`.",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        description="Mirror of :attr:`HttpRetrySettings.backoff_multiplier`.",
    )
    backoff_jitter_ms: int = Field(
        default=50,
        ge=0,
        description="Mirror of :attr:`HttpRetrySettings.backoff_jitter_ms`.",
    )

    retry: HttpRetrySettings = Field(
        default_factory=HttpRetrySettings,
        description="Canonical retry policy (AAP R-15).",
    )
    circuit_breaker: HttpCircuitBreakerSettings = Field(
        default_factory=HttpCircuitBreakerSettings,
        description="Canonical circuit-breaker thresholds (AAP R-16).",
    )


class AuthSettings(BaseModel):
    """JWT validation parameters (AAP R-21, R-22, R-23).

    Inventory uses a FLAT auth structure (NOT a nested ``auth.jwt.*``
    layout). The container reads ``settings.auth.public_key_url``,
    ``settings.auth.jwks_cache_ttl_seconds``, etc. directly. The Auth
    Service is the SOLE issuer of JWTs (AAP R-21); this service only
    validates them and never mints a token.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    audience: str = Field(
        default="inventory-service",
        description=(
            "Expected JWT ``aud`` claim (env: JWT_AUDIENCE). Tokens "
            "without this audience are rejected with 401 Unauthorized."
        ),
    )
    issuer: str = Field(
        default="https://auth.example.com",
        description=(
            "Expected JWT ``iss`` claim (env: JWT_ISSUER). Tokens with "
            "any other issuer are rejected — defends against confused-"
            "deputy attacks across multiple auth surfaces."
        ),
    )
    public_key_url: str = Field(
        default="https://auth.example.com/.well-known/jwks.json",
        description=(
            "JWKS endpoint URL (env: JWT_PUBLIC_KEY_URL). The container "
            "primes this cache at startup (AAP R-22) so the first "
            "incoming request does not pay the JWKS fetch cost."
        ),
    )
    jwks_cache_ttl_seconds: int = Field(
        default=3600,
        ge=60,
        le=86400,
        description=(
            "JWKS cache TTL in seconds (env: JWT_JWKS_CACHE_TTL_SECONDS, "
            "default 1 hour per AAP R-22). Bounded [60s, 24h] — too "
            "short defeats caching, too long delays key-rotation "
            "propagation."
        ),
    )
    leeway_seconds: int = Field(
        default=30,
        ge=0,
        le=300,
        description=(
            "Leeway in seconds for ``nbf``/``exp`` claim validation "
            "(env: JWT_LEEWAY_SECONDS). Accommodates clock skew "
            "between the Auth Service and this service."
        ),
    )
    required_scope_admin: str = Field(
        default="inventory:admin",
        description=(
            "Required JWT scope for admin endpoints "
            "(warehouse + threshold management, replenishment "
            "recording). Tokens missing this scope receive 403 "
            "Forbidden."
        ),
    )
    required_scope_read: str = Field(
        default="inventory:read",
        description=(
            "Required JWT scope for protected read endpoints "
            "(reservation lookup by order id). Public stock-lookup "
            "endpoints do not require any scope; they are rate-limited "
            "at the Gateway."
        ),
    )
    algorithms: tuple[str, ...] = Field(
        default=("RS256", "ES256"),
        description=(
            "Allowed JWT signing algorithms (env: JWT_ALGORITHMS, "
            "comma-separated). Tokens signed with any other algorithm "
            "are rejected. Symmetric algorithms (HS*) are excluded "
            "because they cannot be verified with a JWKS."
        ),
    )




class OtelSettings(BaseModel):
    """OpenTelemetry exporter configuration (AAP R-27 — telemetry to ELK).

    OpenTelemetry traces are exported via OTLP/gRPC or OTLP/HTTP to the
    cluster collector, which forwards them to Elastic APM (the trace
    surface of the ELK stack used for distributed tracing). When
    :attr:`exporter_otlp_endpoint` is empty the exporter is disabled —
    appropriate for local development without a collector.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    service_name: str = Field(
        default="inventory-service",
        description=(
            "OTel resource ``service.name`` attribute. Defaults to "
            ":attr:`ServiceSettings.name`; "
            ":meth:`Settings._cross_field_invariants` keeps the two "
            "synchronized when the user did not override "
            "``OTEL_SERVICE_NAME`` explicitly."
        ),
    )
    exporter_otlp_endpoint: str = Field(
        default="",
        description=(
            "OTLP/gRPC or OTLP/HTTP collector endpoint (env: "
            "OTEL_EXPORTER_OTLP_ENDPOINT). Empty disables export — "
            "appropriate for local dev without a collector."
        ),
    )
    exporter_otlp_insecure: bool = Field(
        default=True,
        description=(
            "Whether to skip TLS verification on the OTLP exporter "
            "(env: OTEL_EXPORTER_OTLP_INSECURE; default ``True`` for "
            "in-cluster gRPC where the collector is reached over a "
            "private network). Production with TLS-terminated "
            "collectors should set this to ``False``."
        ),
    )
    traces_sampler: str = Field(
        default="parentbased_traceidratio",
        description=(
            "Trace sampler (env: OTEL_TRACES_SAMPLER). The default "
            "honors upstream sampling decisions while applying a "
            "ratio sample to root spans."
        ),
    )
    traces_sampler_arg: float = Field(
        default=0.10,
        ge=0.0,
        le=1.0,
        description=(
            "Sampler argument; ``0.10`` = 10%% sampling for root "
            "spans (env: OTEL_TRACES_SAMPLER_ARG). Bounded [0.0, 1.0]."
        ),
    )
    resource_attributes: str = Field(
        default="service.name=inventory-service,service.namespace=ecommerce",
        description=(
            "OTel resource attributes (env: OTEL_RESOURCE_ATTRIBUTES, "
            "comma-separated ``key=value`` pairs). Surface attributes "
            "for filtering and grouping in the trace UI."
        ),
    )
    enabled: bool = Field(
        default=False,
        description=(
            "Convenience flag; True when "
            ":attr:`exporter_otlp_endpoint` is non-empty (auto-set by "
            ":meth:`Settings._cross_field_invariants`)."
        ),
    )


class ObservabilitySettings(BaseModel):
    """Cross-cutting observability settings (AAP R-13, R-26, R-27).

    Drives correlation-ID middleware, structured-log redaction,
    Prometheus multi-process directory, and the OpenTelemetry tracer
    via the nested :class:`OtelSettings`.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    correlation_id_header: str = Field(
        default="X-Correlation-ID",
        description=(
            "HTTP header name for correlation ID propagation (AAP "
            "R-13). The middleware extracts this header from the "
            "incoming request and propagates it to downstream HTTP "
            "calls and Kafka message headers."
        ),
    )
    correlation_id_required: bool = Field(
        default=False,
        description=(
            "When True, requests without the correlation ID header "
            "are rejected with 400 Bad Request; when False, the "
            "middleware generates one. Production typically allows "
            "missing headers (False) so that direct curl tests "
            "remain ergonomic."
        ),
    )
    log_include_request_body: bool = Field(
        default=False,
        description=(
            "When True, request bodies are included in structured "
            "log entries (subject to redaction). DO NOT enable in "
            "production — request bodies frequently carry PII."
        ),
    )
    log_include_response_body: bool = Field(
        default=False,
        description=(
            "When True, response bodies are included in structured "
            "log entries. DO NOT enable in production — risk of "
            "logging large payloads and PII."
        ),
    )
    log_redact_fields: tuple[str, ...] = Field(
        default=(
            "password",
            "token",
            "secret",
            "authorization",
            "cookie",
            "api_key",
            "apikey",
        ),
        description=(
            "Field names redacted from structured logs by the "
            "logging middleware (case-insensitive match against "
            "JSON keys and HTTP header names)."
        ),
    )
    prometheus_multiproc_dir: str = Field(
        default="/app/tmp/prometheus",
        description=(
            "Directory for Prometheus multiprocess metrics "
            "(env: PROMETHEUS_MULTIPROC_DIR). Required when running "
            "uvicorn with multiple worker processes — the registry "
            "writes per-process metric files here for aggregation "
            "at scrape time."
        ),
    )

    otel: OtelSettings = Field(
        default_factory=OtelSettings,
        description="OpenTelemetry exporter configuration (AAP R-27).",
    )


class FeaturesSettings(BaseModel):
    """Feature flags for runtime behavior toggles.

    Per-environment toggles for the four most operationally significant
    behaviors. All defaults are ``True`` (production-safe). The most
    significant flag is :attr:`reservation_expiry_scheduler_enabled`:
    setting it to ``False`` disables the AAP R-20 fallback path (the
    background expiration scheduler that releases stuck reservations);
    do this only in test environments or when an alternate release
    mechanism (e.g., Kafka-only release on a dedicated topic) is
    operational.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    reservation_expiry_scheduler_enabled: bool = Field(
        default=True,
        description=(
            "When False, the expiry scheduler is NOT started at "
            "lifespan startup (AAP R-20 fallback disabled). Test "
            "environments using ``freezegun`` may set this to "
            "``False`` to prevent the scheduler from interfering "
            "with deterministic time."
        ),
    )
    low_stock_events_enabled: bool = Field(
        default=True,
        description=(
            "When False, ``inventory.low-stock`` events are not "
            "emitted (still computed, just suppressed at the Kafka "
            "produce step). Useful in seeded test environments "
            "where every SKU starts low."
        ),
    )
    stock_movement_audit_enabled: bool = Field(
        default=True,
        description=(
            "When False, ``stock_movements`` rows are NOT inserted "
            "(audit trail disabled). ONLY acceptable for tests that "
            "exercise the reservation logic in isolation; production "
            "MUST keep the audit trail."
        ),
    )
    admin_endpoints_enabled: bool = Field(
        default=True,
        description=(
            "When False, admin routes (warehouse + threshold "
            "management, replenishment recording, reservation "
            "lookup by order id) return 404. Used to harden "
            "internet-exposed deployments where admin operations "
            "must not appear in the route table."
        ),
    )


class MigrationsSettings(BaseModel):
    """Database migration configuration.

    Inventory uses Alembic for schema migrations. The
    :attr:`auto_run` flag (default ``False``) gates running ``alembic
    upgrade head`` at container startup; production-grade deployments
    typically use a dedicated migration job and leave the in-container
    auto-run disabled. The flag exists for local-dev convenience.
    """

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    auto_run: bool = Field(
        default=False,
        description=(
            "When True, the container runs ``alembic upgrade head`` at "
            "startup. Default ``False``; production should use a "
            "dedicated migration job. (env: RUN_MIGRATIONS_ON_STARTUP)"
        ),
    )
    dir: str = Field(
        default="migrations",
        description=(
            "Path to the alembic migrations directory relative to "
            "the service root (env: MIGRATIONS_DIR)."
        ),
    )
    version_table_name: str = Field(
        default="alembic_version_inventory_service",
        description=(
            "Per-service alembic version table name to avoid "
            "cross-service collisions. Without this, two services "
            "sharing a Postgres instance would clobber each other's "
            "migration history."
        ),
    )




# =============================================================================
# Top-Level Settings (BaseSettings)
# =============================================================================
# This class is the only :class:`BaseSettings` in the module — it owns
# the layered source resolution (init kwargs / env / .env / YAML),
# performs the environment-variable overlay onto the nested model
# structure, runs the :meth:`_cross_field_invariants` re-checks, and
# exposes the entire validated configuration tree to the application.
#
# Consumers reach this class only through :func:`get_settings` (which is
# wrapped in :func:`functools.lru_cache`). Direct ``Settings()``
# construction outside of :func:`get_settings` is discouraged because it
# bypasses the singleton guarantee and may produce inconsistent state
# if env vars change mid-process.
# =============================================================================


class Settings(BaseSettings):
    """Top-level Inventory Service settings.

    Loaded from (highest precedence first):

    1. **Init kwargs** — arguments passed to ``Settings(...)`` (used in
       tests).
    2. **Environment variables** — flat ``UPPER_SNAKE_CASE`` names
       documented in ``.env.example``.
    3. **.env file** — optional ``services/inventory-service/.env`` for
       local development.
    4. **YAML defaults** — ``services/inventory-service/config/default.yaml``
       plus an optional per-environment overlay.

    Construction performs:

    a. YAML load via :class:`_YamlSettingsSource`.
    b. Environment-variable overlay via :meth:`_inject_env_vars`
       (``model_validator(mode="before")``) — flat env vars are mapped
       onto the nested fields.
    c. Pydantic field validation — types, ranges, ``Literal`` values.
    d. After-validators on nested classes
       (:meth:`KafkaProducerSettings._enforce_durability`,
       :meth:`KafkaConsumerSettings._enforce_manual_commit`,
       :meth:`KafkaSettings._enforce_top_level_manual_commit`, etc.).
    e. :meth:`_cross_field_invariants` on the top-level Settings
       (re-checks the AAP R-14 / R-17 keystones after env overlay).

    Any failure in (a)-(e) raises :class:`pydantic.ValidationError` and
    aborts startup (AAP R-19 fail-fast).
    """

    model_config = SettingsConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        protected_namespaces=(),
    )

    # ------------------------------------------------------------------
    # Top-level fields — all 14 sections from default.yaml
    # ------------------------------------------------------------------
    service: ServiceSettings = Field(
        default_factory=ServiceSettings,
        description="Service identity and runtime parameters.",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description="Logging level and format (AAP R-26).",
    )
    health: HealthSettings = Field(
        default_factory=HealthSettings,
        description="Liveness and readiness probe paths (AAP R-19).",
    )
    metrics: MetricsSettings = Field(
        default_factory=MetricsSettings,
        description="Prometheus metrics endpoint (AAP R-27).",
    )

    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,
        description=(
            "PostgreSQL connection parameters for ``inventory_db`` "
            "(AAP 0.4.4, R-7). Per AAP R-25, credentials must come "
            "from env vars, not the YAML defaults."
        ),
    )
    kafka: KafkaSettings = Field(
        default_factory=KafkaSettings,
        description=(
            "Kafka top-level configuration (broker connectivity + "
            "nested producer/consumer/retry policy). AAP R-14, R-17."
        ),
    )
    schema_registry: SchemaRegistrySettings = Field(
        default_factory=SchemaRegistrySettings,
        description="Confluent Schema Registry configuration (AAP R-14).",
    )
    topics: TopicsSettings = Field(
        default_factory=TopicsSettings,
        description=(
            "All Kafka topic names (consumed + produced + DLQ; "
            "AAP Section 0.4.2, R-30)."
        ),
    )

    reservation: ReservationSettings = Field(
        default_factory=ReservationSettings,
        description=(
            "Reservation lifecycle policy (AAP R-20 — canonical "
            "fallback path)."
        ),
    )
    http_client: HttpClientSettings = Field(
        default_factory=HttpClientSettings,
        description="Outbound HTTP client configuration (AAP R-15, R-16).",
    )
    auth: AuthSettings = Field(
        default_factory=AuthSettings,
        description="JWT validation parameters (AAP R-21, R-22, R-23).",
    )
    observability: ObservabilitySettings = Field(
        default_factory=ObservabilitySettings,
        description="Cross-cutting observability settings (AAP R-13, R-26, R-27).",
    )
    features: FeaturesSettings = Field(
        default_factory=FeaturesSettings,
        description="Feature flags for runtime behavior toggles.",
    )
    migrations: MigrationsSettings = Field(
        default_factory=MigrationsSettings,
        description="Database migration configuration.",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @model_validator(mode="before")
    @classmethod
    def _inject_env_vars(cls, values: Any) -> Any:
        """Map flat environment variables onto the nested model structure.

        Pydantic-settings's ``env_nested_delimiter="__"`` natively
        handles ``KAFKA__BOOTSTRAP``-style variables, but the canonical
        ``.env.example`` uses FLAT names (e.g. ``KAFKA_BOOTSTRAP``,
        ``STOCK_OPTIMISTIC_LOCK_MAX_RETRIES``). This validator bridges
        the two conventions.

        Every variable documented in ``.env.example`` has a corresponding
        explicit lookup here. The verbosity is deliberate: it keeps the
        env-var -> field mapping discoverable and unambiguous, makes
        unsupported env vars visible (they will simply be ignored
        instead of silently mapping to an unintended field), and
        provides a single place to audit the env-var contract.

        For mirror fields — Kafka top-level / nested duplicates and
        HTTP client top-level / nested duplicates — the env value is
        propagated to BOTH locations so direct ``settings.kafka.<f>``
        access and ``settings.kafka.consumer.<f>`` access yield the
        same value.
        """
        if not isinstance(values, dict):
            return values
        v = dict(values)

        # --------------------------------------------------------------
        # Group A: Service Runtime
        # --------------------------------------------------------------
        if (raw := os.environ.get("SERVICE_NAME")) is not None and raw != "":
            _set_nested(v, "service.name", raw)
        if (raw := os.environ.get("SERVICE_PORT")) is not None and raw != "":
            _set_nested(v, "service.port", _coerce_int(raw))
        if (raw := os.environ.get("SERVICE_ENV")) is not None and raw != "":
            _set_nested(v, "service.environment", raw)
        if (raw := os.environ.get("LOG_LEVEL")) is not None and raw != "":
            _set_nested(v, "logging.level", raw)
        if (raw := os.environ.get("LOG_FORMAT")) is not None and raw != "":
            _set_nested(v, "logging.format", raw)
        if (raw := os.environ.get("HEALTH_LIVENESS_PATH")) is not None and raw != "":
            _set_nested(v, "health.liveness_path", raw)
        if (raw := os.environ.get("HEALTH_READINESS_PATH")) is not None and raw != "":
            _set_nested(v, "health.readiness_path", raw)
        if (raw := os.environ.get("METRICS_PORT")) is not None and raw != "":
            _set_nested(v, "metrics.port", _coerce_int(raw))
        if (raw := os.environ.get("METRICS_PATH")) is not None and raw != "":
            _set_nested(v, "metrics.path", raw)
        if (raw := os.environ.get("GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS")) is not None and raw != "":
            _set_nested(v, "service.graceful_shutdown_timeout_seconds", _coerce_int(raw))

        # --------------------------------------------------------------
        # Group B: PostgreSQL
        # --------------------------------------------------------------
        if (raw := os.environ.get("POSTGRES_HOST")) is not None and raw != "":
            _set_nested(v, "database.host", raw)
        if (raw := os.environ.get("POSTGRES_PORT")) is not None and raw != "":
            _set_nested(v, "database.port", _coerce_int(raw))
        if (raw := os.environ.get("POSTGRES_DB")) is not None and raw != "":
            _set_nested(v, "database.name", raw)
        # POSTGRES_USER / POSTGRES_PASSWORD: accept empty strings as
        # explicit-but-empty values. The DSN constructor treats empty
        # creds as "no credentials"; tests can rely on that behavior.
        if (raw := os.environ.get("POSTGRES_USER")) is not None:
            _set_nested(v, "database.user", raw)
        if (raw := os.environ.get("POSTGRES_PASSWORD")) is not None:
            _set_nested(v, "database.password", raw)
        if (raw := os.environ.get("POSTGRES_URL")) is not None and raw != "":
            _set_nested(v, "database.url_override", raw)
        if (raw := os.environ.get("POSTGRES_SSL_MODE")) is not None and raw != "":
            _set_nested(v, "database.ssl_mode", raw)
        if (raw := os.environ.get("POSTGRES_POOL_MIN")) is not None and raw != "":
            _set_nested(v, "database.pool_min", _coerce_int(raw))
        if (raw := os.environ.get("POSTGRES_POOL_MAX")) is not None and raw != "":
            _set_nested(v, "database.pool_max", _coerce_int(raw))
        if (raw := os.environ.get("POSTGRES_POOL_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "database.pool_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("POSTGRES_STATEMENT_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "database.statement_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("POSTGRES_LOCK_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "database.lock_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("POSTGRES_IDLE_IN_TRANSACTION_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "database.idle_in_transaction_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("POSTGRES_APPLICATION_NAME")) is not None and raw != "":
            _set_nested(v, "database.application_name", raw)

        # --------------------------------------------------------------
        # Group C: Kafka — top-level + producer + consumer + retry
        # --------------------------------------------------------------
        if (raw := os.environ.get("KAFKA_BOOTSTRAP")) is not None and raw != "":
            _set_nested(v, "kafka.bootstrap", raw)
        if (raw := os.environ.get("KAFKA_CLIENT_ID")) is not None and raw != "":
            _set_nested(v, "kafka.client_id", raw)
        if (raw := os.environ.get("KAFKA_GROUP_ID")) is not None and raw != "":
            _set_nested(v, "kafka.group_id", raw)
        if (raw := os.environ.get("KAFKA_SECURITY_PROTOCOL")) is not None and raw != "":
            _set_nested(v, "kafka.security_protocol", raw)
        if (raw := os.environ.get("KAFKA_SASL_MECHANISM")) is not None and raw != "":
            _set_nested(v, "kafka.sasl_mechanism", raw)
        if (raw := os.environ.get("KAFKA_SASL_USERNAME")) is not None and raw != "":
            _set_nested(v, "kafka.sasl_username", raw)
        if (raw := os.environ.get("KAFKA_SASL_PASSWORD")) is not None and raw != "":
            _set_nested(v, "kafka.sasl_password", raw)
        if (raw := os.environ.get("KAFKA_SSL_CA_LOCATION")) is not None and raw != "":
            _set_nested(v, "kafka.ssl_ca_location", raw)
        if (raw := os.environ.get("KAFKA_AUTO_OFFSET_RESET")) is not None and raw != "":
            _set_nested(v, "kafka.auto_offset_reset", raw)
            _set_nested(v, "kafka.consumer.auto_offset_reset", raw)
        if (raw := os.environ.get("KAFKA_ENABLE_AUTO_COMMIT")) is not None and raw != "":
            coerced_bool = _coerce_bool(raw)
            _set_nested(v, "kafka.enable_auto_commit", coerced_bool)
            _set_nested(v, "kafka.consumer.enable_auto_commit", coerced_bool)
        if (raw := os.environ.get("KAFKA_MAX_POLL_INTERVAL_MS")) is not None and raw != "":
            coerced_int = _coerce_int(raw)
            _set_nested(v, "kafka.max_poll_interval_ms", coerced_int)
            _set_nested(v, "kafka.consumer.max_poll_interval_ms", coerced_int)
        if (raw := os.environ.get("KAFKA_SESSION_TIMEOUT_MS")) is not None and raw != "":
            coerced_int = _coerce_int(raw)
            _set_nested(v, "kafka.session_timeout_ms", coerced_int)
            _set_nested(v, "kafka.consumer.session_timeout_ms", coerced_int)
        if (raw := os.environ.get("KAFKA_MAX_POLL_RECORDS")) is not None and raw != "":
            coerced_int = _coerce_int(raw)
            _set_nested(v, "kafka.max_poll_records", coerced_int)
            _set_nested(v, "kafka.consumer.max_poll_records", coerced_int)

        # Kafka producer
        if (raw := os.environ.get("KAFKA_PRODUCER_ACKS")) is not None and raw != "":
            _set_nested(v, "kafka.producer.acks", raw)
        if (raw := os.environ.get("KAFKA_PRODUCER_ENABLE_IDEMPOTENCE")) is not None and raw != "":
            _set_nested(v, "kafka.producer.enable_idempotence", _coerce_bool(raw))
        if (raw := os.environ.get("KAFKA_PRODUCER_COMPRESSION_TYPE")) is not None and raw != "":
            _set_nested(v, "kafka.producer.compression_type", raw)
        if (raw := os.environ.get("KAFKA_PRODUCER_LINGER_MS")) is not None and raw != "":
            _set_nested(v, "kafka.producer.linger_ms", _coerce_int(raw))
        if (raw := os.environ.get("KAFKA_PRODUCER_BATCH_SIZE")) is not None and raw != "":
            _set_nested(v, "kafka.producer.batch_size", _coerce_int(raw))
        if (raw := os.environ.get("KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "kafka.producer.delivery_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("KAFKA_PRODUCER_REQUEST_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "kafka.producer.request_timeout_ms", _coerce_int(raw))

        # Kafka retry / DLQ
        if (raw := os.environ.get("KAFKA_RETRY_TOPIC_SUFFIX")) is not None and raw != "":
            _set_nested(v, "kafka.retry.topic_suffix", raw)
        if (raw := os.environ.get("KAFKA_DLQ_TOPIC_SUFFIX")) is not None and raw != "":
            _set_nested(v, "kafka.retry.dlq_topic_suffix", raw)
        if (raw := os.environ.get("KAFKA_RETRY_MAX_ATTEMPTS")) is not None and raw != "":
            _set_nested(v, "kafka.retry.max_attempts", _coerce_int(raw))
        if (raw := os.environ.get("KAFKA_RETRY_BACKOFF_INITIAL_MS")) is not None and raw != "":
            _set_nested(v, "kafka.retry.backoff_initial_ms", _coerce_int(raw))
        if (raw := os.environ.get("KAFKA_RETRY_BACKOFF_MAX_MS")) is not None and raw != "":
            _set_nested(v, "kafka.retry.backoff_max_ms", _coerce_int(raw))
        if (raw := os.environ.get("KAFKA_RETRY_BACKOFF_MULTIPLIER")) is not None and raw != "":
            _set_nested(v, "kafka.retry.backoff_multiplier", _coerce_float(raw))
        if (raw := os.environ.get("KAFKA_RETRY_JITTER_MS")) is not None and raw != "":
            _set_nested(v, "kafka.retry.jitter_ms", _coerce_int(raw))

        # --------------------------------------------------------------
        # Group D: Schema Registry
        # --------------------------------------------------------------
        if (raw := os.environ.get("SCHEMA_REGISTRY_URL")) is not None and raw != "":
            _set_nested(v, "schema_registry.url", raw)
        if (raw := os.environ.get("SCHEMA_REGISTRY_AUTH_USERNAME")) is not None and raw != "":
            _set_nested(v, "schema_registry.auth_username", raw)
        if (raw := os.environ.get("SCHEMA_REGISTRY_AUTH_PASSWORD")) is not None and raw != "":
            _set_nested(v, "schema_registry.auth_password", raw)
        if (raw := os.environ.get("SCHEMA_REGISTRY_CACHE_CAPACITY")) is not None and raw != "":
            _set_nested(v, "schema_registry.cache_capacity", _coerce_int(raw))

        # --------------------------------------------------------------
        # Group E: Topic Names
        # --------------------------------------------------------------
        if (raw := os.environ.get("KAFKA_TOPIC_ORDER_CREATED")) is not None and raw != "":
            _set_nested(v, "topics.consumed.order_created", raw)
        if (raw := os.environ.get("KAFKA_TOPIC_ORDER_CANCELLED")) is not None and raw != "":
            _set_nested(v, "topics.consumed.order_cancelled", raw)
        if (raw := os.environ.get("KAFKA_TOPIC_ORDER_FULFILLED")) is not None and raw != "":
            _set_nested(v, "topics.consumed.order_fulfilled", raw)
        if (raw := os.environ.get("KAFKA_TOPIC_INVENTORY_RESERVED")) is not None and raw != "":
            _set_nested(v, "topics.produced.inventory_reserved", raw)
        if (raw := os.environ.get("KAFKA_TOPIC_INVENTORY_RESERVATION_FAILED")) is not None and raw != "":
            _set_nested(v, "topics.produced.inventory_reservation_failed", raw)
        if (raw := os.environ.get("KAFKA_TOPIC_INVENTORY_RELEASED")) is not None and raw != "":
            _set_nested(v, "topics.produced.inventory_released", raw)
        if (raw := os.environ.get("KAFKA_TOPIC_INVENTORY_LOW_STOCK")) is not None and raw != "":
            _set_nested(v, "topics.produced.inventory_low_stock", raw)
        if (raw := os.environ.get("KAFKA_TOPIC_INVENTORY_DLQ")) is not None and raw != "":
            _set_nested(v, "topics.dlq.inventory_dlq", raw)

        # --------------------------------------------------------------
        # Group F: Reservation
        # --------------------------------------------------------------
        if (raw := os.environ.get("LOW_STOCK_THRESHOLD_DEFAULT")) is not None and raw != "":
            _set_nested(v, "reservation.low_stock_threshold_default", _coerce_int(raw))
        if (raw := os.environ.get("RESERVATION_EXPIRY_MS")) is not None and raw != "":
            _set_nested(v, "reservation.expiry_ms", _coerce_int(raw))
        if (raw := os.environ.get("RESERVATION_EXPIRY_SCHEDULER_POLL_INTERVAL_MS")) is not None and raw != "":
            _set_nested(v, "reservation.expiry_scheduler_poll_interval_ms", _coerce_int(raw))
        if (raw := os.environ.get("RESERVATION_EXPIRY_SCHEDULER_BATCH_SIZE")) is not None and raw != "":
            _set_nested(v, "reservation.expiry_scheduler_batch_size", _coerce_int(raw))
        if (raw := os.environ.get("DEFAULT_WAREHOUSE_ADAPTER")) is not None and raw != "":
            _set_nested(v, "reservation.default_warehouse_adapter", raw)
        if (raw := os.environ.get("STOCK_OPTIMISTIC_LOCK_MAX_RETRIES")) is not None and raw != "":
            _set_nested(v, "reservation.optimistic_lock.max_retries", _coerce_int(raw))
        if (raw := os.environ.get("STOCK_OPTIMISTIC_LOCK_BACKOFF_INITIAL_MS")) is not None and raw != "":
            _set_nested(v, "reservation.optimistic_lock.backoff_initial_ms", _coerce_int(raw))
        if (raw := os.environ.get("STOCK_OPTIMISTIC_LOCK_BACKOFF_MAX_MS")) is not None and raw != "":
            _set_nested(v, "reservation.optimistic_lock.backoff_max_ms", _coerce_int(raw))
        if (raw := os.environ.get("STOCK_OPTIMISTIC_LOCK_JITTER_MS")) is not None and raw != "":
            _set_nested(v, "reservation.optimistic_lock.jitter_ms", _coerce_int(raw))

        # --------------------------------------------------------------
        # Group G: HTTP Client + Circuit Breaker
        # --------------------------------------------------------------
        if (raw := os.environ.get("HTTP_CLIENT_CONNECT_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "http_client.connect_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("HTTP_CLIENT_READ_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "http_client.read_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("HTTP_CLIENT_TOTAL_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "http_client.total_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("HTTP_CLIENT_MAX_RETRIES")) is not None and raw != "":
            coerced_int = _coerce_int(raw)
            _set_nested(v, "http_client.max_retries", coerced_int)
            _set_nested(v, "http_client.retry.max_attempts", coerced_int)
        if (raw := os.environ.get("HTTP_CLIENT_BACKOFF_INITIAL_MS")) is not None and raw != "":
            coerced_int = _coerce_int(raw)
            _set_nested(v, "http_client.backoff_initial_ms", coerced_int)
            _set_nested(v, "http_client.retry.backoff_initial_ms", coerced_int)
        if (raw := os.environ.get("HTTP_CLIENT_BACKOFF_MAX_MS")) is not None and raw != "":
            coerced_int = _coerce_int(raw)
            _set_nested(v, "http_client.backoff_max_ms", coerced_int)
            _set_nested(v, "http_client.retry.backoff_max_ms", coerced_int)
        if (raw := os.environ.get("HTTP_CLIENT_BACKOFF_MULTIPLIER")) is not None and raw != "":
            coerced_float = _coerce_float(raw)
            _set_nested(v, "http_client.backoff_multiplier", coerced_float)
            _set_nested(v, "http_client.retry.backoff_multiplier", coerced_float)
        if (raw := os.environ.get("HTTP_CLIENT_BACKOFF_JITTER_MS")) is not None and raw != "":
            coerced_int = _coerce_int(raw)
            _set_nested(v, "http_client.backoff_jitter_ms", coerced_int)
            _set_nested(v, "http_client.retry.backoff_jitter_ms", coerced_int)

        # Circuit breaker
        if (raw := os.environ.get("CIRCUIT_BREAKER_FAILURE_THRESHOLD")) is not None and raw != "":
            _set_nested(v, "http_client.circuit_breaker.failure_threshold", _coerce_int(raw))
        if (raw := os.environ.get("CIRCUIT_BREAKER_RESET_TIMEOUT_MS")) is not None and raw != "":
            _set_nested(v, "http_client.circuit_breaker.reset_timeout_ms", _coerce_int(raw))
        if (raw := os.environ.get("CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS")) is not None and raw != "":
            _set_nested(v, "http_client.circuit_breaker.half_open_max_calls", _coerce_int(raw))

        # --------------------------------------------------------------
        # Group H: Auth (JWT)
        # --------------------------------------------------------------
        if (raw := os.environ.get("JWT_AUDIENCE")) is not None and raw != "":
            _set_nested(v, "auth.audience", raw)
        if (raw := os.environ.get("JWT_ISSUER")) is not None and raw != "":
            _set_nested(v, "auth.issuer", raw)
        if (raw := os.environ.get("JWT_PUBLIC_KEY_URL")) is not None and raw != "":
            _set_nested(v, "auth.public_key_url", raw)
        if (raw := os.environ.get("JWT_JWKS_CACHE_TTL_SECONDS")) is not None and raw != "":
            _set_nested(v, "auth.jwks_cache_ttl_seconds", _coerce_int(raw))
        if (raw := os.environ.get("JWT_LEEWAY_SECONDS")) is not None and raw != "":
            _set_nested(v, "auth.leeway_seconds", _coerce_int(raw))
        if (raw := os.environ.get("JWT_REQUIRED_SCOPE_ADMIN")) is not None and raw != "":
            _set_nested(v, "auth.required_scope_admin", raw)
        if (raw := os.environ.get("JWT_REQUIRED_SCOPE_READ")) is not None and raw != "":
            _set_nested(v, "auth.required_scope_read", raw)
        if (raw := os.environ.get("JWT_ALGORITHMS")) is not None and raw != "":
            algos = _parse_csv(raw)
            if algos:
                _set_nested(v, "auth.algorithms", tuple(algos))

        # --------------------------------------------------------------
        # Group I: Observability
        # --------------------------------------------------------------
        if (raw := os.environ.get("CORRELATION_ID_HEADER")) is not None and raw != "":
            _set_nested(v, "observability.correlation_id_header", raw)
        if (raw := os.environ.get("CORRELATION_ID_REQUIRED")) is not None and raw != "":
            _set_nested(v, "observability.correlation_id_required", _coerce_bool(raw))
        if (raw := os.environ.get("LOG_INCLUDE_REQUEST_BODY")) is not None and raw != "":
            _set_nested(v, "observability.log_include_request_body", _coerce_bool(raw))
        if (raw := os.environ.get("LOG_INCLUDE_RESPONSE_BODY")) is not None and raw != "":
            _set_nested(v, "observability.log_include_response_body", _coerce_bool(raw))
        if (raw := os.environ.get("LOG_REDACT_FIELDS")) is not None and raw != "":
            redact = _parse_csv(raw)
            if redact:
                _set_nested(v, "observability.log_redact_fields", tuple(redact))
        if (raw := os.environ.get("OTEL_SERVICE_NAME")) is not None and raw != "":
            _set_nested(v, "observability.otel.service_name", raw)
        if (raw := os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")) is not None and raw != "":
            _set_nested(v, "observability.otel.exporter_otlp_endpoint", raw)
        if (raw := os.environ.get("OTEL_EXPORTER_OTLP_INSECURE")) is not None and raw != "":
            _set_nested(v, "observability.otel.exporter_otlp_insecure", _coerce_bool(raw))
        if (raw := os.environ.get("OTEL_TRACES_SAMPLER")) is not None and raw != "":
            _set_nested(v, "observability.otel.traces_sampler", raw)
        if (raw := os.environ.get("OTEL_TRACES_SAMPLER_ARG")) is not None and raw != "":
            _set_nested(v, "observability.otel.traces_sampler_arg", _coerce_float(raw))
        if (raw := os.environ.get("OTEL_RESOURCE_ATTRIBUTES")) is not None and raw != "":
            _set_nested(v, "observability.otel.resource_attributes", raw)
        if (raw := os.environ.get("PROMETHEUS_MULTIPROC_DIR")) is not None and raw != "":
            _set_nested(v, "observability.prometheus_multiproc_dir", raw)

        # --------------------------------------------------------------
        # Group J: Features
        # --------------------------------------------------------------
        if (raw := os.environ.get("FEATURE_RESERVATION_EXPIRY_SCHEDULER_ENABLED")) is not None and raw != "":
            _set_nested(v, "features.reservation_expiry_scheduler_enabled", _coerce_bool(raw))
        if (raw := os.environ.get("FEATURE_LOW_STOCK_EVENTS_ENABLED")) is not None and raw != "":
            _set_nested(v, "features.low_stock_events_enabled", _coerce_bool(raw))
        if (raw := os.environ.get("FEATURE_STOCK_MOVEMENT_AUDIT_ENABLED")) is not None and raw != "":
            _set_nested(v, "features.stock_movement_audit_enabled", _coerce_bool(raw))
        if (raw := os.environ.get("FEATURE_ADMIN_ENDPOINTS_ENABLED")) is not None and raw != "":
            _set_nested(v, "features.admin_endpoints_enabled", _coerce_bool(raw))

        # --------------------------------------------------------------
        # Group K: Migrations
        # --------------------------------------------------------------
        if (raw := os.environ.get("RUN_MIGRATIONS_ON_STARTUP")) is not None and raw != "":
            _set_nested(v, "migrations.auto_run", _coerce_bool(raw))
        if (raw := os.environ.get("MIGRATIONS_DIR")) is not None and raw != "":
            _set_nested(v, "migrations.dir", raw)

        return v

    @model_validator(mode="after")
    def _cross_field_invariants(self) -> "Settings":
        """Cross-field invariants that span multiple nested classes.

        Ordering and rationale:

        1. **Re-confirm AAP R-17 keystone** at top level (top-level
           ``kafka.enable_auto_commit`` and nested
           ``kafka.consumer.enable_auto_commit`` must both be False).
           This is a belt-and-suspenders check after env-var overlay
           in case the caller passed a True value via env that
           bypassed the per-class validator.
        2. **OTel ``service_name`` synchronization**: when the user
           did not override ``OTEL_SERVICE_NAME`` (or left it at the
           default), align it with :attr:`ServiceSettings.name` so
           traces render with the correct service identity.
        3. **OTel auto-enable**: when an OTLP endpoint is configured,
           flip :attr:`OtelSettings.enabled` to True so downstream
           code can branch on the single boolean rather than
           re-checking the endpoint string.
        4. **AAP R-14 producer durability re-check**: ``acks`` must be
           ``"all"`` and ``enable_idempotence`` must be True.
        """
        # 1. Belt-and-suspenders manual-commit check (AAP R-17 keystone).
        if self.kafka.enable_auto_commit:
            raise ValueError(
                "Settings.kafka.enable_auto_commit must be False per AAP R-17"
            )
        if self.kafka.consumer.enable_auto_commit:
            raise ValueError(
                "Settings.kafka.consumer.enable_auto_commit must be False "
                "per AAP R-17"
            )

        # 2. OTel service_name fallback to service.name when the user
        # has not provided an explicit override (i.e., the OTel value
        # is empty or still the BaseModel default placeholder).
        otel_default_name = "inventory-service"
        if (
            not self.observability.otel.service_name
            or self.observability.otel.service_name == otel_default_name
        ):
            object.__setattr__(
                self.observability.otel,
                "service_name",
                self.service.name,
            )

        # 3. OTel auto-enable when endpoint is set.
        if self.observability.otel.exporter_otlp_endpoint:
            object.__setattr__(self.observability.otel, "enabled", True)

        # 4. Producer durability re-check (AAP R-14).
        if self.kafka.producer.acks != "all":
            raise ValueError(
                f"Settings.kafka.producer.acks must be 'all' per AAP R-14 "
                f"(got: {self.kafka.producer.acks!r})"
            )
        if not self.kafka.producer.enable_idempotence:
            raise ValueError(
                "Settings.kafka.producer.enable_idempotence must be True "
                "per AAP R-14"
            )

        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Customize the settings source ordering.

        Returns a tuple of sources in *highest-precedence-first* order:

        1. ``init_settings`` — kwargs passed to ``Settings(...)``.
        2. ``env_settings`` — environment variables.
        3. ``dotenv_settings`` — ``.env`` file.
        4. :class:`_YamlSettingsSource` — YAML defaults (lowest).

        The default :meth:`BaseSettings.settings_customise_sources`
        returns ``(init, env, dotenv, file_secret)``; we **omit**
        ``file_secret_settings`` (we rely on env vars for secrets per
        AAP R-25 — a separate file-based secret loader is unnecessary)
        and **append** :class:`_YamlSettingsSource` so YAML defaults
        are used for any field that none of the higher-precedence
        sources supplies.
        """
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            _YamlSettingsSource(settings_cls),
        )


# =============================================================================
# Cached factory
# =============================================================================
# :func:`get_settings` is the canonical entry point for the rest of the
# application. It is wrapped in :func:`functools.lru_cache(maxsize=1)`
# so exactly one :class:`Settings` instance exists per process.
# =============================================================================


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached :class:`Settings` singleton.

    Wrapped in :func:`functools.lru_cache` to ensure exactly one
    :class:`Settings` instance exists per process. Test code that needs
    an isolated :class:`Settings` instance can call
    ``get_settings.cache_clear()`` between tests to force re-construction
    with fresh environment-variable overlays.

    Per AAP R-19, the first call to this function during application
    startup is the **fail-fast gate** — any missing required env var or
    invalid value will raise here and abort the process. Subsequent
    calls return the cached instance and never re-validate.

    Returns
    -------
    Settings
        A validated, immutable settings instance with all 14 top-level
        sections populated.
    """
    return Settings()


# =============================================================================
# Public surface (`__all__`)
# =============================================================================
# Alphabetically sorted to match the re-export order in
# ``services/inventory-service/src/config/__init__.py``. Includes the 25
# nested BaseModel classes plus the top-level :class:`Settings` and
# :func:`get_settings` factory — 26 entries total.
# =============================================================================

__all__: list[str] = [
    "AuthSettings",
    "ConsumedTopicsSettings",
    "DatabaseSettings",
    "DlqTopicsSettings",
    "FeaturesSettings",
    "HealthSettings",
    "HttpCircuitBreakerSettings",
    "HttpClientSettings",
    "HttpRetrySettings",
    "KafkaConsumerSettings",
    "KafkaProducerSettings",
    "KafkaRetrySettings",
    "KafkaSettings",
    "LoggingSettings",
    "MetricsSettings",
    "MigrationsSettings",
    "ObservabilitySettings",
    "OptimisticLockSettings",
    "OtelSettings",
    "ProducedTopicsSettings",
    "ReservationSettings",
    "SchemaRegistrySettings",
    "ServiceSettings",
    "Settings",
    "TopicsSettings",
    "get_settings",
]

