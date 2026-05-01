"""Typed, validated, cached configuration for the Notification Service.

Exposes a single :class:`Settings` model tree plus a :func:`get_settings`
factory that loads and caches it. The loader composes three sources in
this precedence order (highest wins):

    3. environment variables                       (highest precedence)
    2. services/notification-service/config/<environment>.yaml
       (merged onto #1 when an overlay file is present, e.g.,
        ``local.yaml`` for ``ENVIRONMENT=local``)
    1. services/notification-service/config/default.yaml  (base layer)

The module relies on ``pydantic-settings`` >= 2.0's ``BaseSettings`` with
a custom YAML source adapter (see :class:`_YamlSettingsSource` below).

Fail-fast semantics (AAP R-19)
------------------------------
The first invocation of :func:`get_settings` validates every field and
every cross-field invariant. If a required env var is missing or a
validator rejects a value, ``pydantic.ValidationError`` is raised and
propagates to the caller (``src.main.lifespan`` via ``build_container``),
causing the process to exit non-zero per AAP R-19.

Required-at-startup environment variables (AAP R-19, R-25):

* ``POSTGRES_URL``           — full DSN for the ``notification_db``
* ``KAFKA_BOOTSTRAP``        — comma-separated broker list
* ``JWT_PUBLIC_KEY_URL``     — JWKS endpoint for JWT signature validation
* ``JWT_ISSUER``             — expected ``iss`` claim
* ``SENDGRID_API_KEY``       — when ``EMAIL_PROVIDER=sendgrid`` (default)
* ``TWILIO_ACCOUNT_SID`` +
  ``TWILIO_AUTH_TOKEN`` +
  ``TWILIO_FROM_NUMBER``     — when ``SMS_PROVIDER=twilio`` (default)
* ``AWS_REGION`` (or
  ``AWS_SES_REGION`` /
  ``AWS_SNS_REGION``)        — when SES / SNS is selected as the
                                provider for the corresponding channel.

Secrets policy (AAP R-25)
-------------------------
No field in this module reads a secret value from YAML. Secret-shaped
fields (DB URLs, Kafka SASL passwords, provider API keys) are populated
exclusively from environment variables via the ``_inject_env_vars``
model_validator. YAML files therefore contain only non-secret defaults
(timeouts, retry budgets, ports, TTLs, log level, provider choice,
booleans). Sensitive secrets are wrapped in :class:`pydantic.SecretStr`
so they mask themselves in ``repr()`` and structured logs.

Cached access
-------------
:func:`get_settings` is decorated with ``@functools.lru_cache(maxsize=1)``
so it is evaluated exactly once per process. Tests may call
``get_settings.cache_clear()`` to force re-evaluation (e.g., after
monkeypatching ``os.environ``).

Cross-References
----------------
Consumed by:

* ``src.config.logging_config.configure_logging`` — receives the Settings
  instance and uses ``settings.logging.{level,format}`` (AAP R-26).
* ``src.container.build_container`` — receives the Settings instance and
  constructs DB pools, Kafka clients, provider adapters, JWKS clients,
  and the retry scheduler.
* ``src.main.lifespan`` — calls :func:`get_settings` as the first startup
  step and propagates any ``ValidationError`` to terminate the process.

Consumes (files):

* ``services/notification-service/config/default.yaml`` — authoritative
  base configuration (always loaded).
* ``services/notification-service/config/<env>.yaml`` — environment
  overlay (loaded when present; the file name is derived from the
  ``ENVIRONMENT`` env var).
* Environment variables — highest precedence.

Architectural rules referenced (AAP Section 0.7)
------------------------------------------------
* R-11  — Dual-provider concurrency: ``email.provider``∈{sendgrid,ses}
          and ``sms.provider``∈{twilio,sns}; selection is data-driven.
* R-14  — Schema Registry URL surfaced on :class:`KafkaSettings`.
* R-15  — Retry policy (max_attempts, initial_delay_ms, multiplier,
          max_delay_ms, jitter_pct).
* R-16  — Circuit-breaker thresholds.
* R-17  — Retry topic + DLQ topic suffixes; per-channel DLQ topics.
* R-19  — Fail-fast on missing required dependencies.
* R-21  — Auth Service is the SOLE token issuer; this service only
          validates JWTs.
* R-22  — Bounded TTL for the JWKS cache.
* R-25  — Secrets via env vars only; never in YAML / source.
* R-26  — Structured JSON logs (``logging.level``, ``logging.format``).
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

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

# ---------------------------------------------------------------------------
# Module-level path constants
# ---------------------------------------------------------------------------
# Resolved relative to THIS file so loading works regardless of the process'
# current working directory. The Notification Service is normally launched
# from ``services/notification-service/`` (Dockerfile WORKDIR), but unit
# tests, IDE runners, and ad-hoc invocations may set CWD elsewhere.
#
# Layout assumed:
#   services/notification-service/
#       config/
#           default.yaml                 <- _DEFAULT_YAML
#           <ENVIRONMENT>.yaml           <- optional overlay (e.g., local.yaml)
#           log_config.json
#       src/
#           config/
#               settings.py              <- THIS FILE (_THIS_DIR)
_THIS_DIR: Path = Path(__file__).resolve().parent
# This file lives at services/notification-service/src/config/settings.py;
# walking up two levels (config/ -> src/) lands on the service root.
_SERVICE_ROOT: Path = _THIS_DIR.parent.parent
_CONFIG_DIR: Path = _SERVICE_ROOT / "config"
_DEFAULT_YAML: Path = _CONFIG_DIR / "default.yaml"
# Format string interpolated with the environment classifier at load time;
# for ``ENVIRONMENT=local`` this resolves to ``local.yaml`` and so on.
_LOCAL_YAML_NAME: str = "{env}.yaml"


# ---------------------------------------------------------------------------
# YAML helpers (module-level so they are unit-testable in isolation).
# ---------------------------------------------------------------------------
def _read_yaml(path: Path) -> dict[str, Any]:
    """Return the parsed YAML mapping at ``path``.

    Returns an empty dict when the file is missing, empty, malformed, or
    does not parse to a mapping at the document root. The choice to fall
    through silently (rather than raise) is deliberate: the YAML layer is
    a NON-CRITICAL source. Required fields are enforced exclusively at
    the Settings layer via Pydantic validation, so a missing or corrupt
    YAML file simply forces all values to come from defaults / env vars.

    Args:
        path: Filesystem path of the YAML file to read.

    Returns:
        Top-level YAML mapping as a Python dict, or ``{}`` on any error.
    """
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fp:
            data = yaml.safe_load(fp) or {}
    except (OSError, yaml.YAMLError):
        # Falling back to an empty dict here means the Settings layer
        # will fail fast on missing required fields with a precise error
        # message rather than a confusing low-level YAML traceback.
        return {}
    return data if isinstance(data, dict) else {}


def _peek_environment() -> str:
    """Return the current deployment environment classifier.

    Reads directly from ``os.environ`` (NOT settings) because this is
    invoked BEFORE :class:`Settings` is constructed — its job is to tell
    :class:`_YamlSettingsSource` which per-environment YAML overlay to
    merge. Defaults to ``"local"`` when ``ENVIRONMENT`` is unset or
    blank.

    Returns:
        The environment classifier — one of ``local``, ``dev``,
        ``stage``, ``staging``, ``prod``, ``production``, ``test`` in
        normal operation. Strict membership validation happens later in
        :class:`ServiceSettings`.
    """
    raw = (os.environ.get("ENVIRONMENT") or "local").strip()
    return raw or "local"


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``overlay`` onto a shallow copy of ``base``.

    Behavior:

    * Matching dict values at the same key are merged key-by-key.
    * Matching non-dict values are REPLACED by the overlay value
      (lists are not concatenated — a local override list fully
      replaces the base list).
    * Keys present only in ``overlay`` are inserted into the result.
    * Neither ``base`` nor ``overlay`` is mutated.

    Args:
        base: The lower-precedence mapping (e.g., default.yaml).
        overlay: The higher-precedence mapping (e.g., local.yaml).

    Returns:
        A new dict containing the merged result.
    """
    result: dict[str, Any] = dict(base)
    for key, overlay_val in overlay.items():
        base_val = result.get(key)
        if isinstance(base_val, dict) and isinstance(overlay_val, dict):
            result[key] = _deep_merge(base_val, overlay_val)
        else:
            result[key] = overlay_val
    return result


def _strip_env_suffix_keys(obj: Any) -> Any:
    """Recursively remove any mapping key whose name ends in ``_env``.

    Some sibling services (notably ``services/recommendation-engine``)
    use the convention ``<name>_env: VAR_NAME`` in YAML to document
    that a field's value should be resolved from an environment
    variable at runtime. Because every nested settings model in this
    file declares ``extra="forbid"``, leaving those informational keys
    in the merged dict would cause every load to fail with "extra
    fields not permitted".

    The Notification Service's ``default.yaml`` does not currently use
    this convention, but stripping ``_env`` keys is a harmless, cheap
    compatibility shim that keeps the structure of this file uniform
    with sibling services and lets operators copy YAML snippets between
    services without surprise validation failures.

    Args:
        obj: Arbitrary YAML-derived value. Recurses into dicts and
            lists; primitives pass through unchanged.

    Returns:
        A structurally identical value with every ``_env``-suffixed
        mapping key removed at every nesting level.
    """
    if isinstance(obj, dict):
        return {
            k: _strip_env_suffix_keys(v)
            for k, v in obj.items()
            if not k.endswith("_env")
        }
    if isinstance(obj, list):
        return [_strip_env_suffix_keys(x) for x in obj]
    return obj


def _set_nested(values: dict[str, Any], dotted_path: str, value: Any) -> None:
    """Set ``values[a][b][c] = value`` given ``dotted_path='a.b.c'``.

    Creates intermediate dicts on demand. Used by
    :meth:`Settings._inject_env_vars` to map flat environment variables
    (e.g. ``KAFKA_BOOTSTRAP``, ``EMAIL_RETRY_MAX_ATTEMPTS``) onto the
    nested settings tree (``kafka.bootstrap_servers``,
    ``email.retry.max_attempts``).

    If a path component already exists but is not a dict (e.g., a
    fully-constructed BaseModel passed via init kwargs), the helper
    overwrites it with a fresh dict. This is acceptable because
    ``_inject_env_vars`` runs in ``mode="before"`` BEFORE any nested
    model has been constructed; the inputs at that point are always
    plain dicts produced by either YAML loading or the env-var settings
    source.

    Args:
        values: The root mutable dict.
        dotted_path: The dotted Python attribute path to set.
        value: The value to assign at the leaf.
    """
    parts = dotted_path.split(".")
    cursor: dict[str, Any] = values
    for part in parts[:-1]:
        existing = cursor.get(part)
        if not isinstance(existing, dict):
            existing = {}
            cursor[part] = existing
        cursor = existing
    cursor[parts[-1]] = value


def _coerce_int(raw: str | None) -> int | None:
    """Safely parse an int from an env-var string.

    Returns None when:

    * the input is None,
    * the input is an empty / whitespace-only string, or
    * the string cannot be parsed as a base-10 integer.

    Returning None signals "no operator override" to the caller, which
    in turn means "let the YAML / default value win". Pydantic's per-
    field validators take care of subsequent type / range enforcement.
    """
    if raw is None or raw.strip() == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _coerce_float(raw: str | None) -> float | None:
    """Safely parse a float from an env-var string; mirrors :func:`_coerce_int`."""
    if raw is None or raw.strip() == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _coerce_bool(raw: str | None) -> bool | None:
    """Safely parse a 12-factor-style boolean from an env-var string.

    Recognises the canonical truthy spellings (``true``, ``1``, ``yes``,
    ``on``) and falsy spellings (``false``, ``0``, ``no``, ``off``)
    case-insensitively. Returns None for unset, blank, or unrecognised
    values so the caller can fall through to defaults.
    """
    if raw is None:
        return None
    low = raw.strip().lower()
    if low in {"true", "1", "yes", "on"}:
        return True
    if low in {"false", "0", "no", "off"}:
        return False
    return None


# ---------------------------------------------------------------------------
# Custom pydantic-settings source: layered YAML loader.
# ---------------------------------------------------------------------------
class _YamlSettingsSource(PydanticBaseSettingsSource):
    """Load YAML files as a pydantic-settings source.

    Reads ``default.yaml`` first, then overlays ``<ENVIRONMENT>.yaml``
    when present, then strips any ``_env``-suffixed documentation keys.
    The resulting dict is exposed to pydantic-settings as a settings
    source whose priority slot is configured by
    :meth:`Settings.settings_customise_sources` (immediately after
    init kwargs and environment variables, both of which override it).

    Caching: the merge happens once per :class:`_YamlSettingsSource`
    instance — pydantic-settings constructs a fresh instance every time
    :class:`Settings` is built, so cache invalidation simply happens
    automatically when ``get_settings.cache_clear()`` is invoked from a
    test.
    """

    def __init__(self, settings_cls: type[BaseSettings]) -> None:
        super().__init__(settings_cls)
        self._yaml_data: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        """Load, merge, and sanitise the YAML configuration once.

        Returns:
            The merged, ``_env``-key-stripped dict ready to feed back to
            pydantic-settings as the YAML source contribution.
        """
        base = _read_yaml(_DEFAULT_YAML)
        env = _peek_environment()
        overlay_path = _CONFIG_DIR / _LOCAL_YAML_NAME.format(env=env)
        merged: dict[str, Any] = base
        # Guard against the (impossible-in-practice) case that someone
        # set ENVIRONMENT=default and the overlay path collides with
        # _DEFAULT_YAML — re-merging would still be correct, but it is
        # cheap to short-circuit.
        if overlay_path.exists() and overlay_path.resolve() != _DEFAULT_YAML.resolve():
            overlay = _read_yaml(overlay_path)
            merged = _deep_merge(base, overlay)
        sanitised = _strip_env_suffix_keys(merged)
        # _strip_env_suffix_keys preserves dict shape on dict input;
        # narrow the type for mypy / pyright.
        if not isinstance(sanitised, dict):
            return {}
        return sanitised

    def get_field_value(
        self,
        field: Any,
        field_name: str,
    ) -> tuple[Any, str, bool]:
        """Look up a single top-level field from the merged YAML dict.

        Args:
            field: The pydantic ``FieldInfo`` for the requested field.
                Unused — pydantic-settings invokes this once per
                top-level field on :class:`Settings`.
            field_name: The Python attribute name of the field.

        Returns:
            A tuple ``(value, field_name, is_complex)`` where
            ``is_complex`` advises pydantic-settings whether to recurse
            into the value as a nested mapping. We always return False
            because the YAML data is already a plain Python value.
        """
        del field  # unused; kept to honour PydanticBaseSettingsSource ABC
        value = self._yaml_data.get(field_name)
        return value, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return the full merged YAML payload as a dict.

        pydantic-settings v2 calls this once per :class:`Settings`
        construction to obtain the source's contribution to the merged
        kwargs. The result is the dict produced by :meth:`_load`,
        already merged across ``default.yaml`` + ``<env>.yaml`` and
        sanitised of ``_env``-suffixed documentation keys.
        """
        return self._yaml_data


# ===========================================================================
# Nested settings models — one per top-level YAML section.
# ===========================================================================
# Every nested model uses ``ConfigDict(extra="forbid", str_strip_whitespace=True)``
# so a typo in YAML / env (``mxa_attempts`` instead of ``max_attempts``)
# raises a clear validation error at startup rather than silently being
# ignored. ``str_strip_whitespace=True`` strips accidental whitespace in
# string fields (e.g., trailing newline on a Helm-templated value).
# ===========================================================================


class RetryConfig(BaseModel):
    """Exponential-backoff + jitter retry policy (AAP R-15).

    Used by :class:`EmailSettings` and :class:`SmsSettings` for outbound
    HTTP retries against the active provider, and (in transformed form)
    by the internal scheduler. The Kafka consumer retry budget lives on
    :class:`ConsumerRetrySettings` because it has different semantics
    (per-message attempts rather than per-call attempts).

    Attributes:
        max_attempts: Total attempts INCLUDING the initial call. Capped
            at 20 to bound worst-case latency.
        initial_delay_ms: Backoff before the first retry; subsequent
            delays grow geometrically by ``multiplier``.
        multiplier: Exponential growth factor between successive
            retries. ``1.0`` means constant delay (no exponential
            growth); typical production value is 2.0.
        max_delay_ms: Upper bound on any single backoff window. Caps
            the unbounded growth that ``multiplier`` would otherwise
            produce.
        jitter_pct: Fraction of the computed delay added as uniform
            random jitter to avoid synchronised retry storms across
            multiple service replicas (``0.2`` = ±20%).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    max_attempts: int = Field(
        default=5,
        ge=1,
        le=20,
        description=(
            "Maximum total attempts (initial + retries) before giving "
            "up (AAP R-15)."
        ),
    )
    initial_delay_ms: int = Field(
        default=200,
        ge=10,
        description="Initial backoff delay in milliseconds (AAP R-15).",
    )
    multiplier: float = Field(
        default=2.0,
        ge=1.0,
        description=(
            "Exponential growth factor applied between successive "
            "retries (AAP R-15)."
        ),
    )
    max_delay_ms: int = Field(
        default=10_000,
        ge=100,
        description=(
            "Upper bound on any single backoff delay; caps the "
            "exponential growth (AAP R-15)."
        ),
    )
    jitter_pct: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description=(
            "Random jitter fraction applied to each computed delay "
            "(AAP R-15 — prevents synchronised retry storms)."
        ),
    )

    @model_validator(mode="after")
    def _validate_delays(self) -> RetryConfig:
        """Reject configurations where ``max_delay_ms < initial_delay_ms``.

        Such a configuration is nonsensical (the cap would clamp every
        retry to the cap value, defeating exponential backoff) and
        almost always reflects an operator typo. Catching it here
        produces a precise startup error per AAP R-19.
        """
        if self.max_delay_ms < self.initial_delay_ms:
            raise ValueError(
                f"RetryConfig.max_delay_ms ({self.max_delay_ms}) must be >= "
                f"initial_delay_ms ({self.initial_delay_ms}) (AAP R-15)."
            )
        return self


class CircuitBreakerConfig(BaseModel):
    """Circuit-breaker thresholds (AAP R-16).

    Used by :class:`EmailSettings` and :class:`SmsSettings` to protect
    outbound provider calls. The breaker integrates with the resilience
    middleware (``src.resilience.circuit_breaker``) which exposes its
    state on the ``/health/ready`` probe so that an OPEN breaker
    propagates through Kubernetes probes without taking the whole pod
    offline.

    Attributes:
        failure_rate_threshold_pct: Percentage of failed calls within
            the rolling window that trips the breaker to OPEN.
        call_volume_threshold: Minimum number of calls required in the
            rolling window before the failure-rate threshold is
            evaluated (avoids tripping on tiny samples).
        open_duration_ms: How long the breaker stays fully OPEN before
            transitioning to HALF-OPEN to attempt a probe.
        half_open_permitted_calls: Maximum number of probe calls allowed
            during the HALF-OPEN state.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    failure_rate_threshold_pct: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Failure-rate percentage that trips the breaker OPEN (AAP R-16).",
    )
    call_volume_threshold: int = Field(
        default=20,
        ge=1,
        description=(
            "Minimum call count in the sample window before the "
            "failure rate is evaluated (AAP R-16)."
        ),
    )
    open_duration_ms: int = Field(
        default=30_000,
        ge=100,
        description=(
            "Milliseconds the breaker stays OPEN before a HALF-OPEN "
            "probe is attempted (AAP R-16)."
        ),
    )
    half_open_permitted_calls: int = Field(
        default=1,
        ge=1,
        description="Number of probe calls allowed during HALF-OPEN (AAP R-16).",
    )


class ServiceSettings(BaseModel):
    """Top-level service identity (name, version, port, environment).

    Attributes:
        name: Logical service name; appears in every structured log line
            (AAP R-26 ``service`` field) and as the ``otel.service.name``
            resource attribute.
        version: Semantic version. MUST stay in sync with the
            ``__version__`` constant in ``src/__init__.py`` so the
            startup log line and Prometheus ``info`` metric agree.
        port: HTTP listen port. MUST match the ``EXPOSE`` directive in
            ``Dockerfile`` and the ``livenessProbe`` /
            ``readinessProbe`` httpGet port in the K8s manifests.
        environment: Deployment environment classifier; overridden by
            the ``ENVIRONMENT`` env var.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(
        default="notification-service",
        description=(
            "Logical service name; appears in every log line "
            "(AAP R-26 'service' field)."
        ),
    )
    version: str = Field(
        default="1.0.0",
        description=(
            "Semantic version; MUST match src/__init__.py __version__ "
            "and the Dockerfile LABEL."
        ),
    )
    port: int = Field(
        default=8000,
        ge=1,
        le=65535,
        description=(
            "HTTP listen port; MUST match Dockerfile EXPOSE and the "
            "K8s probe ports."
        ),
    )
    environment: Literal[
        "local",
        "dev",
        "stage",
        "staging",
        "prod",
        "production",
        "test",
    ] = Field(
        default="local",
        description=(
            "Deployment environment classifier; overridden by the "
            "ENVIRONMENT env var."
        ),
    )


class LoggingSettings(BaseModel):
    """Logging subsystem configuration (AAP R-26).

    The Notification Service emits structured JSON logs (AAP R-26)
    enriched with the correlation ID propagated by the correlation-ID
    middleware (AAP Section 0.4.5). Filebeat ships those logs to
    Logstash for ingestion into Elasticsearch (AAP R-27).

    Attributes:
        level: Minimum log level. Accepts the canonical Python ``WARN``
            spelling and normalises it to ``WARNING`` for consistency
            with :mod:`logging`.
        format: Log line format. ``json`` is mandatory in non-local
            environments per AAP R-26; ``text`` is permitted only for
            local debugging.
        include_correlation_id: When True, the structured logger
            unconditionally injects the request-scope correlation ID
            into every log line (AAP Section 0.4.5).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    level: Literal[
        "DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"
    ] = Field(
        default="INFO",
        description=(
            "Minimum log level (overridden by LOG_LEVEL env var); "
            "WARN is normalised to WARNING."
        ),
    )
    format: Literal["json", "text"] = Field(
        default="json",
        description=(
            "Log format; MUST be 'json' in non-local environments "
            "per AAP R-26."
        ),
    )
    include_correlation_id: bool = Field(
        default=True,
        description=(
            "Whether the correlation-ID middleware appends "
            "correlation_id on every log line (AAP Section 0.4.5)."
        ),
    )

    @field_validator("level", mode="before")
    @classmethod
    def _uppercase_level(cls, v: Any) -> Any:
        """Normalise ``level`` to upper-case and resolve ``WARN`` -> ``WARNING``."""
        if isinstance(v, str):
            upper = v.strip().upper()
            return "WARNING" if upper == "WARN" else upper
        return v


class DatabaseSettings(BaseModel):
    """PostgreSQL connection pool configuration (AAP Section 0.4.4).

    The Notification Service owns its private ``notification_db``
    PostgreSQL instance per AAP R-6 (database-per-service). The DSN is
    secret-shaped (it embeds credentials) so it is wrapped in
    :class:`pydantic.SecretStr` and supplied via the ``POSTGRES_URL``
    env var only — never via YAML — per AAP R-25.

    Operators access the raw value via
    ``settings.database.url.get_secret_value()`` at the driver boundary
    when constructing a psycopg / SQLAlchemy connection.

    Attributes:
        url: Full libpq-compatible connection URL.
        pool_min_size: Minimum idle connections kept warm in the pool.
        pool_max_size: Maximum concurrent connections per service
            instance. Sized higher than read-heavy services because
            notification dispatch produces a sustained write workload
            (insert into ``delivery_attempts`` on every send + retry).
        statement_timeout_ms: Server-side ``statement_timeout`` (ms),
            applied via ``SET statement_timeout`` on connection
            checkout to bound worst-case query latency.
        connect_timeout_seconds: TCP / TLS handshake timeout when
            establishing a new connection; gives up on a dead host
            quickly.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    url: SecretStr = Field(
        ...,
        description=(
            "Full DSN for notification_db, e.g. "
            "postgresql://user:pass@host:5432/notification_db. MUST be "
            "supplied via the POSTGRES_URL env var (AAP R-25 — no DSN "
            "in YAML)."
        ),
    )
    pool_min_size: int = Field(
        default=2,
        ge=1,
        description="Minimum idle connections kept warm in the pool.",
    )
    pool_max_size: int = Field(
        default=20,
        ge=1,
        description=(
            "Maximum concurrent connections per service instance "
            "(write-heavy workload)."
        ),
    )
    statement_timeout_ms: int = Field(
        default=5_000,
        ge=100,
        description=(
            "Server-side statement_timeout (ms); enforced via "
            "SET statement_timeout."
        ),
    )
    connect_timeout_seconds: int = Field(
        default=5,
        ge=1,
        description=(
            "TCP / TLS handshake timeout (seconds) before giving up "
            "on a dead host."
        ),
    )

    @field_validator("url")
    @classmethod
    def _check_url_scheme(cls, v: SecretStr) -> SecretStr:
        """Reject database URLs that do not use a postgres-compatible scheme.

        Defense-in-depth: catches the common misconfiguration where a
        Kafka or Redis URL is accidentally placed in ``POSTGRES_URL``.
        ``SecretStr`` is a thin string wrapper, so we extract the value,
        run a light syntactic check, and rewrap.
        """
        raw = v.get_secret_value() if isinstance(v, SecretStr) else str(v)
        if not raw:
            raise ValueError("database.url must be a non-empty string")
        if not raw.startswith(("postgresql://", "postgresql+", "postgres://")):
            raise ValueError(
                "database.url must use a postgresql:// (or "
                "postgresql+driver://) scheme; got an unrecognised prefix."
            )
        return v

    @model_validator(mode="after")
    def _validate_pool_sizes(self) -> DatabaseSettings:
        """Reject configurations where ``pool_min_size > pool_max_size``."""
        if self.pool_min_size > self.pool_max_size:
            raise ValueError(
                f"DatabaseSettings.pool_min_size ({self.pool_min_size}) "
                f"must be <= pool_max_size ({self.pool_max_size})."
            )
        return self


class ConsumerRetrySettings(BaseModel):
    """Kafka consumer retry budget before DLQ routing (AAP R-17).

    On handler failure, a poll record is republished to the partner
    ``<topic>.retry`` topic (see :attr:`KafkaSettings.retry_topic_suffix`).
    After ``max_attempts`` failed handler invocations the message is
    routed to the corresponding ``<topic>.dlq`` (see
    :attr:`KafkaSettings.dlq_topic_suffix`) for offline triage.

    The actual exponential backoff for retry-topic re-attempts is
    governed by application code (the consumer client uses
    ``backoff_ms`` as the initial delay and the application multiplier).

    Attributes:
        max_attempts: Per-message delivery attempts before DLQ routing.
        backoff_ms: Initial backoff (ms) between retry-topic attempts.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    max_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description=(
            "Per-message delivery attempts before DLQ routing "
            "(AAP R-17)."
        ),
    )
    backoff_ms: int = Field(
        default=500,
        ge=10,
        description="Initial backoff (ms) between retry-topic attempts.",
    )


class KafkaSettings(BaseModel):
    """Kafka producer/consumer configuration (AAP R-14, R-17, R-30).

    The Notification Service is a TERMINAL CONSUMER (it produces no
    DOMAIN events, only DLQ control-plane messages). The
    ``topics_consumed`` list is authoritative and matches the seven
    events enumerated in AAP Section 0.4.2 + 0.5.2.2 bullet 8. The
    ``dlq_topics_produced`` list is the per-channel DLQ pair from AAP
    Section 0.4.2.

    All inbound payloads are validated against Schema Registry per AAP
    R-14 (the Schema Registry URL is supplied via the
    ``SCHEMA_REGISTRY_URL`` env var when the deployment uses one).

    Attributes:
        bootstrap_servers: Comma-separated broker list — supplied via
            the ``KAFKA_BOOTSTRAP`` env var per AAP R-25.
        schema_registry_url: Confluent Schema Registry URL (optional;
            inbound events are validated against it when present).
        group_id: Kafka consumer group ID — uniquely identifies this
            service's consumer instances within the cluster.
        client_id: Kafka client ID — embedded in broker metrics and
            access logs for traceability.
        auto_offset_reset: Behaviour on first connect with no committed
            offset. ``earliest`` (default) replays from the head of the
            topic so a fresh deployment of this terminal consumer
            catches up on past events.
        enable_auto_commit: MUST be False — commits happen manually
            after successful dispatch to enforce at-least-once
            semantics.
        max_poll_records: Batch size per poll iteration.
        session_timeout_ms: Kafka consumer session timeout. Members
            that fail to heartbeat within this window are evicted
            from the group and trigger a rebalance.
        heartbeat_interval_ms: Heartbeat cadence. Per Kafka protocol,
            this MUST be strictly less than ``session_timeout_ms``;
            the cross-field validator on :class:`Settings` enforces
            the invariant.
        retry_topic_suffix: Naming convention for retry partner topics
            (AAP R-17). Default ``.retry`` -> ``<topic>.retry``.
        dlq_topic_suffix: Naming convention for dead-letter topics
            (AAP R-17). Default ``.dlq`` -> ``<topic>.dlq``.
        dlq_email_topic: Channel-specific DLQ topic written when an
            email-channel send exhausts its retry budget (AAP Section
            0.4.2 fallback).
        dlq_sms_topic: Channel-specific DLQ topic written when an
            SMS-channel send exhausts its retry budget (AAP Section
            0.4.2 fallback).
        consumer_retry: Per-message consumer retry budget.
        topics_consumed: AUTHORITATIVE list of the seven domain event
            topics this service subscribes to per AAP Section 0.4.2.
        dlq_topics_produced: AUTHORITATIVE list of the two per-channel
            DLQs this service publishes to per AAP Section 0.4.2.
        security_protocol: Optional Kafka security protocol
            (``PLAINTEXT`` | ``SASL_SSL`` | ``SASL_PLAINTEXT`` |
            ``SSL``). Unset by default for local-dev.
        sasl_mechanism: Optional SASL mechanism (``PLAIN`` |
            ``SCRAM-SHA-256`` | ``SCRAM-SHA-512`` | ``OAUTHBEARER`` |
            ``GSSAPI``).
        sasl_username: SASL username — supplied via
            ``KAFKA_SASL_USERNAME`` env var.
        sasl_password: SASL password — supplied via
            ``KAFKA_SASL_PASSWORD`` env var (AAP R-25; SecretStr).
        producer_extra_conf: Additional librdkafka producer config
            for tuning (``linger.ms``, ``compression.type``, etc.).
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        protected_namespaces=(),
    )

    bootstrap_servers: str = Field(
        ...,
        description=(
            "Comma-separated broker list (host:port,host:port). MUST "
            "be supplied via the KAFKA_BOOTSTRAP env var (AAP R-25)."
        ),
    )
    schema_registry_url: str | None = Field(
        default=None,
        description=(
            "Confluent Schema Registry URL (AAP R-14); supplied via "
            "the SCHEMA_REGISTRY_URL env var when present."
        ),
    )
    group_id: str = Field(
        default="notification-service",
        description="Kafka consumer group ID.",
    )
    client_id: str = Field(
        default="notification-service",
        description="Kafka client ID (used for broker metrics and logs).",
    )
    auto_offset_reset: Literal["earliest", "latest"] = Field(
        default="earliest",
        description=(
            "Offset-reset behaviour on first connect with no committed "
            "offset."
        ),
    )
    enable_auto_commit: bool = Field(
        default=False,
        description=(
            "Must be False — commits happen manually after successful "
            "dispatch (at-least-once semantics)."
        ),
    )
    max_poll_records: int = Field(
        default=50,
        ge=1,
        le=1_000,
        description="Batch size per poll iteration.",
    )
    session_timeout_ms: int = Field(
        default=30_000,
        ge=1_000,
        description=(
            "Kafka consumer session timeout (ms). Members that fail to "
            "heartbeat within this window are evicted from the group."
        ),
    )
    heartbeat_interval_ms: int = Field(
        default=10_000,
        ge=100,
        description=(
            "Heartbeat cadence (ms). MUST be strictly less than "
            "session_timeout_ms (Kafka protocol requirement)."
        ),
    )
    retry_topic_suffix: str = Field(
        default=".retry",
        description=(
            "AAP R-17 — retry topic naming convention "
            "(e.g., 'order.created.retry')."
        ),
    )
    dlq_topic_suffix: str = Field(
        default=".dlq",
        description="AAP R-17 — dead-letter topic naming convention.",
    )
    dlq_email_topic: str = Field(
        default="notifications.email.dlq",
        description=(
            "AAP Section 0.4.2 — channel-specific DLQ topic for "
            "exhausted email sends."
        ),
    )
    dlq_sms_topic: str = Field(
        default="notifications.sms.dlq",
        description=(
            "AAP Section 0.4.2 — channel-specific DLQ topic for "
            "exhausted SMS sends."
        ),
    )
    consumer_retry: ConsumerRetrySettings = Field(
        default_factory=ConsumerRetrySettings,
        description=(
            "Per-message consumer retry budget before DLQ routing "
            "(AAP R-17)."
        ),
    )
    topics_consumed: list[str] = Field(
        default_factory=lambda: [
            "user.registered",
            "order.created",
            "order.cancelled",
            "order.fulfilled",
            "payment.succeeded",
            "payment.failed",
            "payment.refunded",
        ],
        description=(
            "AAP Section 0.4.2 + 0.5.2.2 bullet 8 — exactly seven "
            "consumed events."
        ),
    )
    dlq_topics_produced: list[str] = Field(
        default_factory=lambda: [
            "notifications.email.dlq",
            "notifications.sms.dlq",
        ],
        description=(
            "AAP Section 0.4.2 — DLQ topics this service writes to "
            "after retry-budget exhaustion."
        ),
    )
    security_protocol: str | None = Field(
        default=None,
        description=(
            "Kafka security protocol "
            "(PLAINTEXT | SASL_SSL | SASL_PLAINTEXT | SSL)."
        ),
    )
    sasl_mechanism: str | None = Field(
        default=None,
        description=(
            "SASL mechanism "
            "(PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512 | OAUTHBEARER | GSSAPI)."
        ),
    )
    sasl_username: str | None = Field(
        default=None,
        description=(
            "SASL username; supplied via the KAFKA_SASL_USERNAME env var."
        ),
    )
    sasl_password: SecretStr | None = Field(
        default=None,
        description=(
            "SASL password; supplied via the KAFKA_SASL_PASSWORD env "
            "var (AAP R-25)."
        ),
    )
    producer_extra_conf: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Additional librdkafka producer config (linger.ms, "
            "compression.type, etc.)."
        ),
    )

    @field_validator("bootstrap_servers")
    @classmethod
    def _validate_bootstrap_servers(cls, v: str) -> str:
        """Reject empty / whitespace-only ``bootstrap_servers``.

        ``BaseSettings`` will already reject ``None`` because the field
        is required, but it would silently accept an empty string. The
        validator surfaces that misconfiguration at startup with a
        precise error message per AAP R-19.
        """
        if not v or not v.strip():
            raise ValueError(
                "kafka.bootstrap_servers must be non-empty; supply "
                "the KAFKA_BOOTSTRAP env var (AAP R-19, R-25)."
            )
        return v

    @field_validator("topics_consumed")
    @classmethod
    def _no_duplicate_topics(cls, v: list[str]) -> list[str]:
        """Reject duplicate entries in ``topics_consumed``."""
        if len(v) != len(set(v)):
            raise ValueError(
                "KafkaSettings.topics_consumed contains duplicate entries."
            )
        return v

    @field_validator("dlq_topics_produced")
    @classmethod
    def _no_duplicate_dlq_topics(cls, v: list[str]) -> list[str]:
        """Reject duplicate entries in ``dlq_topics_produced``."""
        if len(v) != len(set(v)):
            raise ValueError(
                "KafkaSettings.dlq_topics_produced contains duplicate entries."
            )
        return v



class EmailSettings(BaseModel):
    """Email channel configuration (AAP R-11 — dual-provider concurrency).

    Two providers are supported concurrently per AAP R-11: SendGrid
    (default; global) and AWS SES (HTTPS API). Selection at runtime is
    data-driven via the ``provider`` field, populated from the
    ``EMAIL_PROVIDER`` env var. The container builds the corresponding
    adapter at startup and passes it to the dispatcher.

    The cross-field validator :meth:`_validate_provider_credentials`
    enforces the principle that the credentials matching the selected
    provider MUST be present at startup (AAP R-19 fail-fast). For SES,
    the AWS Region is required; the access keys are optional because
    boto3 also resolves them via IAM instance profiles / IRSA.

    Attributes:
        provider: Active provider identifier (``"sendgrid"`` |
            ``"ses"``).
        sendgrid_api_key: SendGrid API key (SecretStr; AAP R-25);
            REQUIRED when ``provider == "sendgrid"``.
        sendgrid_webhook_verification_key: SendGrid webhook signature
            verification key. Recommended in production for defense
            against spoofed delivery callbacks.
        aws_region: AWS region for SES; REQUIRED when
            ``provider == "ses"``.
        aws_ses_access_key_id: AWS SES access key id (SecretStr).
            Optional when running with an IAM instance profile / IRSA.
        aws_ses_secret_access_key: AWS SES secret access key
            (SecretStr). Optional when running with an IAM instance
            profile / IRSA.
        from_address: RFC 5322 ``From:`` address used for outbound
            messages.
        from_name: Human-friendly display name paired with
            ``from_address``.
        connect_timeout_ms: TCP connect timeout for outbound provider
            calls.
        read_timeout_ms: HTTP read timeout for outbound provider calls.
        retry: Per-call retry policy (AAP R-15).
        circuit_breaker: Per-call circuit-breaker thresholds (AAP R-16).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    provider: Literal["sendgrid", "ses"] = Field(
        default="sendgrid",
        description=(
            "AAP R-11 — provider adapter selection; overridden by the "
            "EMAIL_PROVIDER env var."
        ),
    )

    # SendGrid
    sendgrid_api_key: SecretStr | None = Field(
        default=None,
        description=(
            "SendGrid API key (AAP R-25); required when "
            "provider='sendgrid'."
        ),
    )
    sendgrid_webhook_verification_key: SecretStr | None = Field(
        default=None,
        description=(
            "SendGrid webhook signature verification key; recommended "
            "in production for tamper-proof delivery callbacks."
        ),
    )

    # AWS SES
    aws_region: str | None = Field(
        default=None,
        description=(
            "AWS region for SES; required when provider='ses'."
        ),
    )
    aws_ses_access_key_id: SecretStr | None = Field(
        default=None,
        description=(
            "AWS SES access key id (AAP R-25); optional when running "
            "with an IAM instance profile / IRSA."
        ),
    )
    aws_ses_secret_access_key: SecretStr | None = Field(
        default=None,
        description=(
            "AWS SES secret access key (AAP R-25); optional when "
            "running with an IAM instance profile / IRSA."
        ),
    )

    # Common
    from_address: str = Field(
        default="noreply@example.com",
        description="RFC 5322 From address used for outbound messages.",
    )
    from_name: str = Field(
        default="Notification Service",
        description="Display name paired with from_address.",
    )
    connect_timeout_ms: int = Field(
        default=2_000,
        ge=100,
        description="TCP connect timeout for outbound provider calls.",
    )
    read_timeout_ms: int = Field(
        default=5_000,
        ge=100,
        description="HTTP read timeout for outbound provider calls.",
    )
    retry: RetryConfig = Field(
        default_factory=RetryConfig,
        description="Per-call retry policy (AAP R-15).",
    )
    circuit_breaker: CircuitBreakerConfig = Field(
        default_factory=CircuitBreakerConfig,
        description="Per-call circuit-breaker thresholds (AAP R-16).",
    )

    @model_validator(mode="after")
    def _validate_provider_credentials(self) -> EmailSettings:
        """Enforce provider-credential presence per AAP R-19.

        For SendGrid, ``sendgrid_api_key`` is required.

        For SES, the AWS Region is required. Access keys are optional
        because boto3 will fall back to the AWS default credential
        provider chain (IAM instance profile, IRSA, environment, etc.)
        when the explicit fields are unset — this is the desired
        behaviour in production where IAM roles are the preferred
        credential source.
        """
        if self.provider == "sendgrid":
            if self.sendgrid_api_key is None:
                raise ValueError(
                    "EmailSettings.provider='sendgrid' requires the "
                    "SENDGRID_API_KEY env var (AAP R-19 fail-fast; "
                    "AAP R-25 secrets via env only)."
                )
        elif self.provider == "ses":
            if not self.aws_region:
                raise ValueError(
                    "EmailSettings.provider='ses' requires the "
                    "AWS_SES_REGION (or AWS_REGION) env var (AAP R-19)."
                )
            # Access keys are intentionally optional for IAM / IRSA.
        return self


class SmsSettings(BaseModel):
    """SMS channel configuration (AAP R-11 — dual-provider concurrency).

    Two providers are supported concurrently per AAP R-11: Twilio
    (default; global REST API) and AWS SNS (HTTPS API for SMS). Their
    configuration shapes deliberately mirror :class:`EmailSettings`.

    The cross-field validator :meth:`_validate_provider_credentials`
    enforces credential presence per AAP R-19 fail-fast semantics.

    Attributes:
        provider: Active provider identifier (``"twilio"`` | ``"sns"``).
        twilio_account_sid: Twilio Account SID (SecretStr; AAP R-25);
            REQUIRED when ``provider == "twilio"``.
        twilio_auth_token: Twilio Auth Token (SecretStr; AAP R-25);
            REQUIRED when ``provider == "twilio"``.
        from_number: E.164-formatted sender phone number; REQUIRED when
            ``provider == "twilio"``.
        aws_region: AWS region for SNS; REQUIRED when
            ``provider == "sns"``.
        aws_sns_access_key_id: AWS SNS access key id (SecretStr;
            optional when using IAM instance profile / IRSA).
        aws_sns_secret_access_key: AWS SNS secret access key
            (SecretStr; optional when using IAM instance profile /
            IRSA).
        sns_sender_id: SNS SMS sender ID / alphanumeric sender name
            (varies by region; some regions disallow alphanumeric IDs).
        connect_timeout_ms: TCP connect timeout for outbound provider
            calls.
        read_timeout_ms: HTTP read timeout for outbound provider calls.
        retry: Per-call retry policy (AAP R-15).
        circuit_breaker: Per-call circuit-breaker thresholds (AAP R-16).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    provider: Literal["twilio", "sns"] = Field(
        default="twilio",
        description=(
            "AAP R-11 — provider adapter selection; overridden by the "
            "SMS_PROVIDER env var."
        ),
    )

    # Twilio
    twilio_account_sid: SecretStr | None = Field(
        default=None,
        description=(
            "Twilio Account SID (AAP R-25); required when "
            "provider='twilio'."
        ),
    )
    twilio_auth_token: SecretStr | None = Field(
        default=None,
        description=(
            "Twilio Auth Token (AAP R-25); required when "
            "provider='twilio'."
        ),
    )
    from_number: str = Field(
        default="",
        description=(
            "E.164 sender phone number; required for Twilio. Supplied "
            "via TWILIO_FROM_NUMBER (or SMS_FROM_NUMBER) env var."
        ),
    )

    # AWS SNS
    aws_region: str | None = Field(
        default=None,
        description=(
            "AWS region for SNS; required when provider='sns'."
        ),
    )
    aws_sns_access_key_id: SecretStr | None = Field(
        default=None,
        description=(
            "AWS SNS access key id (AAP R-25); optional when running "
            "with an IAM instance profile / IRSA."
        ),
    )
    aws_sns_secret_access_key: SecretStr | None = Field(
        default=None,
        description=(
            "AWS SNS secret access key (AAP R-25); optional when "
            "running with an IAM instance profile / IRSA."
        ),
    )
    sns_sender_id: str | None = Field(
        default=None,
        description=(
            "SNS SMS sender ID / alphanumeric sender name (region-dependent)."
        ),
    )

    # Common
    connect_timeout_ms: int = Field(
        default=2_000,
        ge=100,
        description="TCP connect timeout for outbound provider calls.",
    )
    read_timeout_ms: int = Field(
        default=5_000,
        ge=100,
        description="HTTP read timeout for outbound provider calls.",
    )
    retry: RetryConfig = Field(
        default_factory=RetryConfig,
        description="Per-call retry policy (AAP R-15).",
    )
    circuit_breaker: CircuitBreakerConfig = Field(
        default_factory=CircuitBreakerConfig,
        description="Per-call circuit-breaker thresholds (AAP R-16).",
    )

    @model_validator(mode="after")
    def _validate_provider_credentials(self) -> SmsSettings:
        """Enforce provider-credential presence per AAP R-19.

        For Twilio, ``account_sid``, ``auth_token``, and ``from_number``
        are all required.

        For SNS, the AWS Region is required. Access keys are optional
        because boto3 will fall back to the AWS default credential
        provider chain (IAM instance profile, IRSA, environment, etc.)
        when the explicit fields are unset.
        """
        if self.provider == "twilio":
            if self.twilio_account_sid is None or self.twilio_auth_token is None:
                raise ValueError(
                    "SmsSettings.provider='twilio' requires the "
                    "TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN env vars "
                    "(AAP R-19 fail-fast; AAP R-25 secrets via env only)."
                )
            if not self.from_number:
                raise ValueError(
                    "SmsSettings.provider='twilio' requires the "
                    "TWILIO_FROM_NUMBER env var (AAP R-19)."
                )
        elif self.provider == "sns":
            if not self.aws_region:
                raise ValueError(
                    "SmsSettings.provider='sns' requires the "
                    "AWS_SNS_REGION (or AWS_REGION) env var (AAP R-19)."
                )
            # Access keys are intentionally optional for IAM / IRSA.
        return self


class QuietHoursConfig(BaseModel):
    """Local-time window during which non-critical sends are suppressed.

    The window is expressed in the user's LOCAL timezone (resolved via
    the user's profile / locale), not in UTC. Sends that fall inside the
    window are deferred to the next outside-window slot unless the
    enclosing :class:`ChannelRoutingSettings` flags the event as
    critical and ``critical_overrides_opt_out`` is True.

    Attributes:
        local_start: Inclusive start hour (0–23) in the user's local
            timezone.
        local_end: Exclusive end hour (0–23) in the user's local
            timezone. May be numerically less than ``local_start`` to
            denote an overnight window (e.g., 22 → 7 = 22:00 to 07:00).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    local_start: int = Field(
        default=22,
        ge=0,
        le=23,
        description="Inclusive start hour (0–23) in the user's local timezone.",
    )
    local_end: int = Field(
        default=7,
        ge=0,
        le=23,
        description="Exclusive end hour (0–23) in the user's local timezone.",
    )


class ChannelRoutingSettings(BaseModel):
    """Channel selection policy (AAP R-11 — data-driven routing).

    These defaults are consulted ONLY when a notification template or
    per-user preference record does not otherwise specify the channel
    set. Per-user records in the ``user_channel_prefs`` table override
    these defaults at runtime when ``respect_user_prefs`` is True.

    The ``critical_overrides_opt_out`` flag is the escape hatch for
    security-critical events (e.g., ``payment.failed``) which must
    always reach the user even if they have opted out of a channel.

    Attributes:
        default_email_enabled: Whether the email channel is enabled by
            default for users with no explicit preference.
        default_sms_enabled: Whether the SMS channel is enabled by
            default. Default False (opt-in) to reduce cost and avoid
            unwanted messages.
        respect_user_prefs: When True, ``user_channel_prefs`` table
            entries override these defaults at runtime. Always True in
            production.
        critical_overrides_opt_out: When True, security-critical events
            (e.g., ``payment.failed``) bypass user opt-outs.
        quiet_hours: Local-time window when non-critical sends are
            suppressed.
        default_locale: Fallback locale tag (BCP 47, e.g., ``en-US``)
            when the user has no preference set.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    default_email_enabled: bool = Field(
        default=True,
        description=(
            "Whether the email channel is enabled by default for "
            "users with no explicit preference (AAP R-11)."
        ),
    )
    default_sms_enabled: bool = Field(
        default=False,
        description=(
            "Whether the SMS channel is enabled by default; default "
            "False (opt-in) to avoid unwanted messages."
        ),
    )
    respect_user_prefs: bool = Field(
        default=True,
        description=(
            "When True, user_channel_prefs entries override defaults; "
            "always True in production."
        ),
    )
    critical_overrides_opt_out: bool = Field(
        default=True,
        description=(
            "When True, security-critical events bypass user opt-outs "
            "(e.g., payment.failed can still SMS)."
        ),
    )
    quiet_hours: QuietHoursConfig = Field(
        default_factory=QuietHoursConfig,
        description="Local-time window when non-critical sends are suppressed.",
    )
    default_locale: str = Field(
        default="en-US",
        description="Fallback locale tag (BCP 47) when the user has no preference.",
    )


class TemplatesSettings(BaseModel):
    """Jinja2 template engine configuration.

    Notification body rendering uses a sandboxed template engine.
    Templates are stored in the ``templates`` table (AAP Section
    0.4.4) and compiled into an in-process LRU cache on first use.
    ``strict_undefined: true`` guarantees that templates referencing
    missing context variables fail loudly during testing rather than
    silently shipping ``{{var}}`` tokens to users.

    Attributes:
        engine: Template engine identifier. Currently fixed to
            ``"jinja2"`` for symmetry with sibling services.
        cache_enabled: Whether to cache compiled templates in process.
        cache_max_size: Maximum number of compiled templates kept
            warm in the LRU cache.
        cache_ttl_seconds: TTL applied to cached entries; supports
            hot template updates without restart.
        strict_undefined: When True, rendering a template against a
            context missing a referenced variable raises an error.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    engine: Literal["jinja2"] = Field(
        default="jinja2",
        description="Template engine identifier (currently fixed to 'jinja2').",
    )
    cache_enabled: bool = Field(
        default=True,
        description="Whether to cache compiled templates in process.",
    )
    cache_max_size: int = Field(
        default=500,
        ge=1,
        description="Maximum number of compiled templates kept warm in the LRU cache.",
    )
    cache_ttl_seconds: int = Field(
        default=600,
        ge=1,
        description=(
            "TTL applied to cached entries; supports hot template "
            "updates without restart."
        ),
    )
    strict_undefined: bool = Field(
        default=True,
        description=(
            "When True, rendering a template against a context missing "
            "a referenced variable raises an error (surfaces bugs early)."
        ),
    )


class SchedulerSettings(BaseModel):
    """Internal retry scheduler configuration.

    The scheduler polls ``delivery_attempts`` for messages whose
    next-attempt timestamp has elapsed and re-dispatches them through
    the active provider. After exhausting the per-channel retry budget
    (``email.retry.max_attempts`` / ``sms.retry.max_attempts``) the
    message is published to its channel DLQ
    (``notifications.email.dlq`` / ``notifications.sms.dlq``).

    Attributes:
        enabled: When False, the scheduler does not start (used in
            test environments to disable background work).
        poll_interval_ms: How frequently the scheduler polls the
            ``delivery_attempts`` table for due retries.
        batch_size: Maximum number of pending sends processed per
            poll iteration.
        max_concurrent_sends: Upper bound on concurrent outbound
            provider calls; protects providers from accidental DoS
            during recovery from a backlog.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    enabled: bool = Field(
        default=True,
        description=(
            "When False, the scheduler does not start (used in test "
            "environments)."
        ),
    )
    poll_interval_ms: int = Field(
        default=1_000,
        ge=100,
        description=(
            "How frequently the scheduler polls delivery_attempts "
            "for due retries."
        ),
    )
    batch_size: int = Field(
        default=50,
        ge=1,
        description="Maximum pending sends processed per poll iteration.",
    )
    max_concurrent_sends: int = Field(
        default=10,
        ge=1,
        description=(
            "Upper bound on concurrent outbound provider calls; "
            "protects providers during backlog recovery."
        ),
    )


class ObservabilitySettings(BaseModel):
    """Metrics and tracing configuration (AAP R-26, R-27).

    Logs and metrics ship via Filebeat / Metricbeat to Logstash and
    Elasticsearch (AAP R-27). The ``/metrics`` endpoint exposes
    Prometheus format on a dedicated port so application traffic
    (port 8000) is never scraped together with management telemetry.

    Attributes:
        metrics_port: TCP port for the Prometheus ``/metrics``
            endpoint (scraped by Metricbeat per AAP R-27).
        otel_service_name: ``otel.service.name`` resource attribute
            applied to every emitted span. The Settings cross-field
            validator normalises this to match :attr:`ServiceSettings.name`
            so traces and logs share a single service identifier.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    metrics_port: int = Field(
        default=9090,
        ge=1,
        le=65535,
        description="Prometheus /metrics endpoint port (AAP R-27).",
    )
    otel_service_name: str = Field(
        default="notification-service",
        description=(
            "OpenTelemetry service.name resource attribute; cross-field "
            "validator aligns it with service.name."
        ),
    )


class AuthSettings(BaseModel):
    """JWT / JWKS validation settings (AAP R-21, R-22).

    The Notification Service VALIDATES JWTs minted by the Auth Service.
    It is NEVER an issuer (AAP R-21). Public keys are fetched from the
    Auth Service JWKS endpoint and cached with a bounded TTL (AAP R-22)
    so that key rotation propagates without service restart.

    Attributes:
        jwks_url: JWKS endpoint URL — REQUIRED, supplied via
            ``JWT_PUBLIC_KEY_URL`` env var (AAP R-25).
        jwks_cache_ttl_seconds: Bounded TTL for the in-process JWKS
            key cache (AAP R-22). Lower-bounded at 60 to prevent a
            mistyped value from causing a JWKS storm against the Auth
            Service.
        audience: Expected ``aud`` claim on validated JWTs.
        issuer: Expected ``iss`` claim on validated JWTs — REQUIRED,
            supplied via ``JWT_ISSUER`` env var (AAP R-25).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    jwks_url: str = Field(
        ...,
        description=(
            "JWKS URL for JWT signature validation (AAP R-22). MUST "
            "be supplied via the JWT_PUBLIC_KEY_URL env var "
            "(AAP R-25)."
        ),
    )
    jwks_cache_ttl_seconds: int = Field(
        default=3_600,
        ge=60,
        description=(
            "Bounded TTL for the JWKS key cache (AAP R-22). Lower "
            "bound prevents JWKS storms."
        ),
    )
    audience: str = Field(
        default="notification-service",
        description="Expected 'aud' JWT claim (AAP R-23 / RFC 7519).",
    )
    issuer: str = Field(
        ...,
        description=(
            "Expected 'iss' JWT claim; supplied via the JWT_ISSUER "
            "env var (AAP R-25)."
        ),
    )

    @field_validator("jwks_url")
    @classmethod
    def _validate_jwks_url(cls, v: str) -> str:
        """Reject empty / non-HTTP(S) JWKS URLs.

        ``BaseSettings`` will already reject ``None`` because the field
        is required, but it would silently accept an empty string or a
        bare hostname. The validator surfaces the misconfiguration at
        startup with a precise error message per AAP R-19.
        """
        if not v or not v.strip():
            raise ValueError(
                "auth.jwks_url must be non-empty; supply the "
                "JWT_PUBLIC_KEY_URL env var (AAP R-19, R-25)."
            )
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError(
                "auth.jwks_url must use the http:// or https:// scheme; "
                "got an unrecognised prefix."
            )
        return v

    @field_validator("issuer")
    @classmethod
    def _validate_issuer(cls, v: str) -> str:
        """Reject an empty / whitespace-only issuer."""
        if not v or not v.strip():
            raise ValueError(
                "auth.issuer must be non-empty; supply the JWT_ISSUER "
                "env var (AAP R-19, R-25)."
            )
        return v


class HttpSettings(BaseModel):
    """Default HTTP client timeouts and User-Agent.

    Default values applied to outbound HTTP calls that do not override
    them. Provider-specific overrides (Email / SMS) live in their own
    settings sections. The User-Agent identifies our traffic to
    upstream providers during incident triage.

    Attributes:
        user_agent: ``User-Agent`` header applied to every outbound
            HTTP call.
        default_connect_timeout_ms: Default TCP connect timeout for
            non-provider HTTP calls (e.g., the JWKS fetch path).
        default_read_timeout_ms: Default HTTP read timeout for
            non-provider HTTP calls.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    user_agent: str = Field(
        default="notification-service/1.0",
        description="User-Agent header applied to every outbound HTTP call.",
    )
    default_connect_timeout_ms: int = Field(
        default=1_000,
        ge=100,
        description="Default TCP connect timeout for non-provider HTTP calls.",
    )
    default_read_timeout_ms: int = Field(
        default=5_000,
        ge=100,
        description="Default HTTP read timeout for non-provider HTTP calls.",
    )




# ===========================================================================
# Top-level Settings (BaseSettings) — composes the 12 nested sections.
# ===========================================================================
class Settings(BaseSettings):
    """Runtime settings for the Notification Service — single source of truth.

    Loaded once per process via :func:`get_settings`. Values come from
    (highest precedence first):

      1. Init kwargs to ``Settings(...)`` — used by tests.
      2. Environment variables — POSTGRES_URL, KAFKA_BOOTSTRAP,
         JWT_PUBLIC_KEY_URL, JWT_ISSUER, plus optional overrides
         (ENVIRONMENT, LOG_LEVEL, EMAIL_PROVIDER, SMS_PROVIDER, etc.).
      3. ``config/<env>.yaml`` — merged onto ``default.yaml`` when an
         overlay file is present for the active environment.
      4. ``config/default.yaml`` — base layer.

    Env-var injection happens in :meth:`_inject_env_vars`
    (``mode="before"``) which runs after the YAML sources have produced
    a dict and BEFORE nested model construction. Cross-field
    invariants (heartbeat < session_timeout, topics_consumed disjoint
    from dlq_topics_produced) run in ``mode="after"`` validators.

    AAP traceability:

    * AAP Section 0.4.3 — per-service config loader.
    * AAP R-19 — fail-fast on missing critical dependencies.
    * AAP R-25 — secrets supplied via env vars; SecretStr fields.
    * AAP R-26 — ``logging.level`` / ``logging.format`` consumed by
      ``configure_logging``.

    Raises:
        pydantic.ValidationError: On the first invocation, any missing
            required env var or any failed cross-field invariant raises
            ``ValidationError``. The error propagates out of
            :func:`get_settings` -> ``build_container`` -> ``main`` and
            terminates the process non-zero per AAP R-19.
    """

    model_config = SettingsConfigDict(
        # ``extra="ignore"`` is REQUIRED at the top level: the process
        # inherits many unrelated env vars (PATH, HOME, LANG, ...) that
        # pydantic-settings would otherwise see and reject. Nested
        # models still use ``extra="forbid"`` because only YAML keys
        # flow into them.
        extra="ignore",
        str_strip_whitespace=True,
        # Allows ``KAFKA__BOOTSTRAP_SERVERS`` to map to
        # ``settings.kafka.bootstrap_servers``. Operators may also use
        # the flat names via :meth:`_inject_env_vars` (KAFKA_BOOTSTRAP).
        env_nested_delimiter="__",
        # ``.env`` is for local-dev convenience only. In production,
        # secrets come from Kubernetes Secrets / Vault into the process
        # environment by the platform; ``.env`` is never deployed.
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        # Allow the field name ``model``-prefixed names without
        # Pydantic-v2 deprecation warnings. None are used in this
        # module today, but the safety knob is cheap and keeps test
        # output clean.
        protected_namespaces=(),
    )

    # ----------------------------------------------------------------
    # Twelve top-level sections, mirroring default.yaml's structure.
    # The five required-field sections (database, kafka, auth) carry
    # NO ``default_factory`` — fail-fast per AAP R-19 if any required
    # env var is unset.
    # ----------------------------------------------------------------
    service: ServiceSettings = Field(default_factory=ServiceSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    database: DatabaseSettings
    kafka: KafkaSettings
    email: EmailSettings = Field(default_factory=EmailSettings)
    sms: SmsSettings = Field(default_factory=SmsSettings)
    channel_routing: ChannelRoutingSettings = Field(
        default_factory=ChannelRoutingSettings,
    )
    templates: TemplatesSettings = Field(default_factory=TemplatesSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    observability: ObservabilitySettings = Field(
        default_factory=ObservabilitySettings,
    )
    auth: AuthSettings
    http: HttpSettings = Field(default_factory=HttpSettings)

    # ------------------------------------------------------------------
    # Env-var injection (mode="before")
    # ------------------------------------------------------------------
    @model_validator(mode="before")
    @classmethod
    def _inject_env_vars(cls, values: Any) -> Any:
        """Map flat, legacy-style env vars onto nested fields.

        Complements pydantic-settings's built-in nested-delimiter
        parsing: even though ``env_nested_delimiter="__"`` lets
        ``KAFKA__BOOTSTRAP_SERVERS`` map to
        ``settings.kafka.bootstrap_servers``, the project-wide
        ``.env.example`` catalog uses flat names (``KAFKA_BOOTSTRAP``,
        ``POSTGRES_URL``, ``SENDGRID_API_KEY``, ...). This validator
        translates each flat name to its nested path so both
        conventions work interchangeably.

        Args:
            values: Either a dict produced by the YAML / env source
                merger (the normal path) or a fully-constructed
                :class:`Settings` instance / arbitrary object passed
                via ``Settings.model_copy``. Only dict inputs are
                rewritten; other inputs are returned unchanged so
                ``model_copy``'s contract is preserved.

        Returns:
            The (possibly augmented) input value.
        """
        if not isinstance(values, dict):
            return values

        env = os.environ

        # --- service ---
        if (v := env.get("SERVICE_NAME")) is not None:
            _set_nested(values, "service.name", v)
        if (p := _coerce_int(env.get("SERVICE_PORT"))) is not None:
            _set_nested(values, "service.port", p)
        if (v := env.get("ENVIRONMENT")) is not None:
            _set_nested(values, "service.environment", v.lower())
        if (v := env.get("SERVICE_VERSION")) is not None:
            _set_nested(values, "service.version", v)

        # --- logging ---
        if (v := env.get("LOG_LEVEL")) is not None:
            _set_nested(values, "logging.level", v.upper())
        if (v := env.get("LOG_FORMAT")) is not None:
            _set_nested(values, "logging.format", v.lower())

        # --- database ---
        if (v := env.get("POSTGRES_URL")) is not None:
            _set_nested(values, "database.url", v)
        # Accept both POSTGRES_POOL_MIN_SIZE (prompt) and POSTGRES_POOL_MIN
        # (.env.example) for operator convenience.
        if (p := _coerce_int(env.get("POSTGRES_POOL_MIN_SIZE"))) is not None:
            _set_nested(values, "database.pool_min_size", p)
        elif (p := _coerce_int(env.get("POSTGRES_POOL_MIN"))) is not None:
            _set_nested(values, "database.pool_min_size", p)
        if (p := _coerce_int(env.get("POSTGRES_POOL_MAX_SIZE"))) is not None:
            _set_nested(values, "database.pool_max_size", p)
        elif (p := _coerce_int(env.get("POSTGRES_POOL_MAX"))) is not None:
            _set_nested(values, "database.pool_max_size", p)
        if (p := _coerce_int(env.get("POSTGRES_STATEMENT_TIMEOUT_MS"))) is not None:
            _set_nested(values, "database.statement_timeout_ms", p)
        if (p := _coerce_int(env.get("POSTGRES_CONNECT_TIMEOUT_SECONDS"))) is not None:
            _set_nested(values, "database.connect_timeout_seconds", p)

        # --- kafka ---
        # KAFKA_BOOTSTRAP_SERVERS (canonical) takes precedence over the
        # shorter KAFKA_BOOTSTRAP alias (used in .env.example).
        if (v := env.get("KAFKA_BOOTSTRAP_SERVERS")) is not None:
            _set_nested(values, "kafka.bootstrap_servers", v)
        elif (v := env.get("KAFKA_BOOTSTRAP")) is not None:
            _set_nested(values, "kafka.bootstrap_servers", v)
        if (v := env.get("SCHEMA_REGISTRY_URL")) is not None:
            _set_nested(values, "kafka.schema_registry_url", v)
        if (v := env.get("KAFKA_GROUP_ID")) is not None:
            _set_nested(values, "kafka.group_id", v)
        if (v := env.get("KAFKA_CLIENT_ID")) is not None:
            _set_nested(values, "kafka.client_id", v)
        if (v := env.get("KAFKA_AUTO_OFFSET_RESET")) is not None:
            _set_nested(values, "kafka.auto_offset_reset", v.lower())
        if (b := _coerce_bool(env.get("KAFKA_ENABLE_AUTO_COMMIT"))) is not None:
            _set_nested(values, "kafka.enable_auto_commit", b)
        if (p := _coerce_int(env.get("KAFKA_MAX_POLL_RECORDS"))) is not None:
            _set_nested(values, "kafka.max_poll_records", p)
        elif (p := _coerce_int(env.get("KAFKA_CONSUMER_MAX_POLL_RECORDS"))) is not None:
            # .env.example variant
            _set_nested(values, "kafka.max_poll_records", p)
        if (p := _coerce_int(env.get("KAFKA_SESSION_TIMEOUT_MS"))) is not None:
            _set_nested(values, "kafka.session_timeout_ms", p)
        if (p := _coerce_int(env.get("KAFKA_HEARTBEAT_INTERVAL_MS"))) is not None:
            _set_nested(values, "kafka.heartbeat_interval_ms", p)
        if (v := env.get("KAFKA_RETRY_TOPIC_SUFFIX")) is not None:
            _set_nested(values, "kafka.retry_topic_suffix", v)
        if (v := env.get("KAFKA_DLQ_TOPIC_SUFFIX")) is not None:
            _set_nested(values, "kafka.dlq_topic_suffix", v)
        if (v := env.get("KAFKA_DLQ_EMAIL_TOPIC")) is not None:
            _set_nested(values, "kafka.dlq_email_topic", v)
        if (v := env.get("KAFKA_DLQ_SMS_TOPIC")) is not None:
            _set_nested(values, "kafka.dlq_sms_topic", v)
        if (v := env.get("KAFKA_SECURITY_PROTOCOL")) is not None:
            _set_nested(values, "kafka.security_protocol", v)
        if (v := env.get("KAFKA_SASL_MECHANISM")) is not None:
            _set_nested(values, "kafka.sasl_mechanism", v)
        if (v := env.get("KAFKA_SASL_USERNAME")) is not None:
            _set_nested(values, "kafka.sasl_username", v)
        if (v := env.get("KAFKA_SASL_PASSWORD")) is not None:
            _set_nested(values, "kafka.sasl_password", v)
        if (p := _coerce_int(env.get("CONSUMER_RETRY_MAX_ATTEMPTS"))) is not None:
            _set_nested(values, "kafka.consumer_retry.max_attempts", p)
        elif (p := _coerce_int(env.get("KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS"))) is not None:
            # .env.example variant
            _set_nested(values, "kafka.consumer_retry.max_attempts", p)
        if (p := _coerce_int(env.get("CONSUMER_RETRY_BACKOFF_MS"))) is not None:
            _set_nested(values, "kafka.consumer_retry.backoff_ms", p)
        elif (p := _coerce_int(env.get("KAFKA_CONSUMER_RETRY_BACKOFF_MS"))) is not None:
            # .env.example variant
            _set_nested(values, "kafka.consumer_retry.backoff_ms", p)

        # --- email ---
        if (v := env.get("EMAIL_PROVIDER")) is not None:
            _set_nested(values, "email.provider", v.lower())
        if (v := env.get("SENDGRID_API_KEY")) is not None:
            _set_nested(values, "email.sendgrid_api_key", v)
        if (v := env.get("SENDGRID_WEBHOOK_VERIFICATION_KEY")) is not None:
            _set_nested(values, "email.sendgrid_webhook_verification_key", v)
        # Region resolution: prefer the SES-specific name, then the
        # generic AWS_REGION, so a multi-AWS-service deployment can use
        # one variable for everything.
        if (v := env.get("AWS_SES_REGION")) is not None:
            _set_nested(values, "email.aws_region", v)
        elif (v := env.get("AWS_REGION")) is not None:
            _set_nested(values, "email.aws_region", v)
        if (v := env.get("AWS_SES_ACCESS_KEY_ID")) is not None:
            _set_nested(values, "email.aws_ses_access_key_id", v)
        if (v := env.get("AWS_SES_SECRET_ACCESS_KEY")) is not None:
            _set_nested(values, "email.aws_ses_secret_access_key", v)
        if (v := env.get("EMAIL_FROM_ADDRESS")) is not None:
            _set_nested(values, "email.from_address", v)
        if (v := env.get("EMAIL_FROM_NAME")) is not None:
            _set_nested(values, "email.from_name", v)
        if (p := _coerce_int(env.get("EMAIL_CONNECT_TIMEOUT_MS"))) is not None:
            _set_nested(values, "email.connect_timeout_ms", p)
        if (p := _coerce_int(env.get("EMAIL_READ_TIMEOUT_MS"))) is not None:
            _set_nested(values, "email.read_timeout_ms", p)
        # email.retry — accept both EMAIL_MAX_ATTEMPTS (prompt) and
        # EMAIL_RETRY_MAX_ATTEMPTS (.env.example).
        if (p := _coerce_int(env.get("EMAIL_MAX_ATTEMPTS"))) is not None:
            _set_nested(values, "email.retry.max_attempts", p)
        elif (p := _coerce_int(env.get("EMAIL_RETRY_MAX_ATTEMPTS"))) is not None:
            _set_nested(values, "email.retry.max_attempts", p)
        if (p := _coerce_int(env.get("EMAIL_INITIAL_DELAY_MS"))) is not None:
            _set_nested(values, "email.retry.initial_delay_ms", p)
        elif (p := _coerce_int(env.get("EMAIL_RETRY_INITIAL_DELAY_MS"))) is not None:
            _set_nested(values, "email.retry.initial_delay_ms", p)
        if (f := _coerce_float(env.get("EMAIL_MULTIPLIER"))) is not None:
            _set_nested(values, "email.retry.multiplier", f)
        elif (f := _coerce_float(env.get("EMAIL_RETRY_MULTIPLIER"))) is not None:
            _set_nested(values, "email.retry.multiplier", f)
        if (p := _coerce_int(env.get("EMAIL_MAX_DELAY_MS"))) is not None:
            _set_nested(values, "email.retry.max_delay_ms", p)
        elif (p := _coerce_int(env.get("EMAIL_RETRY_MAX_DELAY_MS"))) is not None:
            _set_nested(values, "email.retry.max_delay_ms", p)
        if (f := _coerce_float(env.get("EMAIL_JITTER_PCT"))) is not None:
            _set_nested(values, "email.retry.jitter_pct", f)
        elif (f := _coerce_float(env.get("EMAIL_RETRY_JITTER_PCT"))) is not None:
            _set_nested(values, "email.retry.jitter_pct", f)
        # email.circuit_breaker — accept both EMAIL_FAILURE_RATE_THRESHOLD
        # (prompt) and EMAIL_CB_FAILURE_RATE_THRESHOLD (.env.example).
        if (p := _coerce_int(env.get("EMAIL_FAILURE_RATE_THRESHOLD"))) is not None:
            _set_nested(values, "email.circuit_breaker.failure_rate_threshold_pct", p)
        elif (p := _coerce_int(env.get("EMAIL_CB_FAILURE_RATE_THRESHOLD"))) is not None:
            _set_nested(values, "email.circuit_breaker.failure_rate_threshold_pct", p)
        if (p := _coerce_int(env.get("EMAIL_CALL_VOLUME_THRESHOLD"))) is not None:
            _set_nested(values, "email.circuit_breaker.call_volume_threshold", p)
        elif (p := _coerce_int(env.get("EMAIL_CB_CALL_VOLUME_THRESHOLD"))) is not None:
            _set_nested(values, "email.circuit_breaker.call_volume_threshold", p)
        if (p := _coerce_int(env.get("EMAIL_OPEN_DURATION_MS"))) is not None:
            _set_nested(values, "email.circuit_breaker.open_duration_ms", p)
        elif (p := _coerce_int(env.get("EMAIL_CB_OPEN_DURATION_MS"))) is not None:
            _set_nested(values, "email.circuit_breaker.open_duration_ms", p)
        if (p := _coerce_int(env.get("EMAIL_HALF_OPEN_PERMITTED_CALLS"))) is not None:
            _set_nested(values, "email.circuit_breaker.half_open_permitted_calls", p)
        elif (p := _coerce_int(env.get("EMAIL_CB_HALF_OPEN_PERMITTED_CALLS"))) is not None:
            _set_nested(values, "email.circuit_breaker.half_open_permitted_calls", p)

        # --- sms ---
        if (v := env.get("SMS_PROVIDER")) is not None:
            _set_nested(values, "sms.provider", v.lower())
        if (v := env.get("TWILIO_ACCOUNT_SID")) is not None:
            _set_nested(values, "sms.twilio_account_sid", v)
        if (v := env.get("TWILIO_AUTH_TOKEN")) is not None:
            _set_nested(values, "sms.twilio_auth_token", v)
        if (v := env.get("TWILIO_FROM_NUMBER")) is not None:
            _set_nested(values, "sms.from_number", v)
        elif (v := env.get("SMS_FROM_NUMBER")) is not None:
            _set_nested(values, "sms.from_number", v)
        if (v := env.get("AWS_SNS_REGION")) is not None:
            _set_nested(values, "sms.aws_region", v)
        elif (v := env.get("AWS_REGION")) is not None:
            _set_nested(values, "sms.aws_region", v)
        if (v := env.get("AWS_SNS_ACCESS_KEY_ID")) is not None:
            _set_nested(values, "sms.aws_sns_access_key_id", v)
        if (v := env.get("AWS_SNS_SECRET_ACCESS_KEY")) is not None:
            _set_nested(values, "sms.aws_sns_secret_access_key", v)
        if (v := env.get("SNS_SENDER_ID")) is not None:
            _set_nested(values, "sms.sns_sender_id", v)
        elif (v := env.get("AWS_SNS_SENDER_ID")) is not None:
            # .env.example variant
            _set_nested(values, "sms.sns_sender_id", v)
        if (p := _coerce_int(env.get("SMS_CONNECT_TIMEOUT_MS"))) is not None:
            _set_nested(values, "sms.connect_timeout_ms", p)
        if (p := _coerce_int(env.get("SMS_READ_TIMEOUT_MS"))) is not None:
            _set_nested(values, "sms.read_timeout_ms", p)
        # sms.retry — accept both SMS_MAX_ATTEMPTS (prompt) and
        # SMS_RETRY_MAX_ATTEMPTS (.env.example).
        if (p := _coerce_int(env.get("SMS_MAX_ATTEMPTS"))) is not None:
            _set_nested(values, "sms.retry.max_attempts", p)
        elif (p := _coerce_int(env.get("SMS_RETRY_MAX_ATTEMPTS"))) is not None:
            _set_nested(values, "sms.retry.max_attempts", p)
        if (p := _coerce_int(env.get("SMS_INITIAL_DELAY_MS"))) is not None:
            _set_nested(values, "sms.retry.initial_delay_ms", p)
        elif (p := _coerce_int(env.get("SMS_RETRY_INITIAL_DELAY_MS"))) is not None:
            _set_nested(values, "sms.retry.initial_delay_ms", p)
        if (f := _coerce_float(env.get("SMS_MULTIPLIER"))) is not None:
            _set_nested(values, "sms.retry.multiplier", f)
        elif (f := _coerce_float(env.get("SMS_RETRY_MULTIPLIER"))) is not None:
            _set_nested(values, "sms.retry.multiplier", f)
        if (p := _coerce_int(env.get("SMS_MAX_DELAY_MS"))) is not None:
            _set_nested(values, "sms.retry.max_delay_ms", p)
        elif (p := _coerce_int(env.get("SMS_RETRY_MAX_DELAY_MS"))) is not None:
            _set_nested(values, "sms.retry.max_delay_ms", p)
        if (f := _coerce_float(env.get("SMS_JITTER_PCT"))) is not None:
            _set_nested(values, "sms.retry.jitter_pct", f)
        elif (f := _coerce_float(env.get("SMS_RETRY_JITTER_PCT"))) is not None:
            _set_nested(values, "sms.retry.jitter_pct", f)
        # sms.circuit_breaker — accept both SMS_FAILURE_RATE_THRESHOLD
        # (prompt) and SMS_CB_FAILURE_RATE_THRESHOLD (.env.example).
        if (p := _coerce_int(env.get("SMS_FAILURE_RATE_THRESHOLD"))) is not None:
            _set_nested(values, "sms.circuit_breaker.failure_rate_threshold_pct", p)
        elif (p := _coerce_int(env.get("SMS_CB_FAILURE_RATE_THRESHOLD"))) is not None:
            _set_nested(values, "sms.circuit_breaker.failure_rate_threshold_pct", p)
        if (p := _coerce_int(env.get("SMS_CALL_VOLUME_THRESHOLD"))) is not None:
            _set_nested(values, "sms.circuit_breaker.call_volume_threshold", p)
        elif (p := _coerce_int(env.get("SMS_CB_CALL_VOLUME_THRESHOLD"))) is not None:
            _set_nested(values, "sms.circuit_breaker.call_volume_threshold", p)
        if (p := _coerce_int(env.get("SMS_OPEN_DURATION_MS"))) is not None:
            _set_nested(values, "sms.circuit_breaker.open_duration_ms", p)
        elif (p := _coerce_int(env.get("SMS_CB_OPEN_DURATION_MS"))) is not None:
            _set_nested(values, "sms.circuit_breaker.open_duration_ms", p)
        if (p := _coerce_int(env.get("SMS_HALF_OPEN_PERMITTED_CALLS"))) is not None:
            _set_nested(values, "sms.circuit_breaker.half_open_permitted_calls", p)
        elif (p := _coerce_int(env.get("SMS_CB_HALF_OPEN_PERMITTED_CALLS"))) is not None:
            _set_nested(values, "sms.circuit_breaker.half_open_permitted_calls", p)

        # --- channel_routing ---
        # Accept short (DEFAULT_*) or namespaced (CHANNEL_ROUTING_*)
        # spellings.
        if (b := _coerce_bool(env.get("DEFAULT_EMAIL_ENABLED"))) is not None:
            _set_nested(values, "channel_routing.default_email_enabled", b)
        elif (b := _coerce_bool(env.get("CHANNEL_ROUTING_DEFAULT_EMAIL_ENABLED"))) is not None:
            _set_nested(values, "channel_routing.default_email_enabled", b)
        if (b := _coerce_bool(env.get("DEFAULT_SMS_ENABLED"))) is not None:
            _set_nested(values, "channel_routing.default_sms_enabled", b)
        elif (b := _coerce_bool(env.get("CHANNEL_ROUTING_DEFAULT_SMS_ENABLED"))) is not None:
            _set_nested(values, "channel_routing.default_sms_enabled", b)
        if (b := _coerce_bool(env.get("RESPECT_USER_PREFS"))) is not None:
            _set_nested(values, "channel_routing.respect_user_prefs", b)
        elif (b := _coerce_bool(env.get("CHANNEL_ROUTING_RESPECT_USER_PREFS"))) is not None:
            _set_nested(values, "channel_routing.respect_user_prefs", b)
        if (b := _coerce_bool(env.get("CRITICAL_OVERRIDES_OPT_OUT"))) is not None:
            _set_nested(values, "channel_routing.critical_overrides_opt_out", b)
        elif (b := _coerce_bool(env.get("CHANNEL_ROUTING_CRITICAL_OVERRIDES_OPT_OUT"))) is not None:
            _set_nested(values, "channel_routing.critical_overrides_opt_out", b)
        if (p := _coerce_int(env.get("QUIET_HOURS_LOCAL_START"))) is not None:
            _set_nested(values, "channel_routing.quiet_hours.local_start", p)
        elif (p := _coerce_int(env.get("CHANNEL_ROUTING_QUIET_HOURS_LOCAL_START"))) is not None:
            _set_nested(values, "channel_routing.quiet_hours.local_start", p)
        if (p := _coerce_int(env.get("QUIET_HOURS_LOCAL_END"))) is not None:
            _set_nested(values, "channel_routing.quiet_hours.local_end", p)
        elif (p := _coerce_int(env.get("CHANNEL_ROUTING_QUIET_HOURS_LOCAL_END"))) is not None:
            _set_nested(values, "channel_routing.quiet_hours.local_end", p)
        if (v := env.get("DEFAULT_LOCALE")) is not None:
            _set_nested(values, "channel_routing.default_locale", v)
        elif (v := env.get("CHANNEL_ROUTING_DEFAULT_LOCALE")) is not None:
            _set_nested(values, "channel_routing.default_locale", v)

        # --- templates ---
        if (v := env.get("TEMPLATE_ENGINE")) is not None:
            _set_nested(values, "templates.engine", v.lower())
        if (b := _coerce_bool(env.get("TEMPLATE_CACHE_ENABLED"))) is not None:
            _set_nested(values, "templates.cache_enabled", b)
        if (p := _coerce_int(env.get("TEMPLATE_CACHE_MAX_SIZE"))) is not None:
            _set_nested(values, "templates.cache_max_size", p)
        if (p := _coerce_int(env.get("TEMPLATE_CACHE_TTL_SECONDS"))) is not None:
            _set_nested(values, "templates.cache_ttl_seconds", p)
        if (b := _coerce_bool(env.get("TEMPLATE_STRICT_UNDEFINED"))) is not None:
            _set_nested(values, "templates.strict_undefined", b)

        # --- scheduler ---
        if (b := _coerce_bool(env.get("SCHEDULER_ENABLED"))) is not None:
            _set_nested(values, "scheduler.enabled", b)
        if (p := _coerce_int(env.get("SCHEDULER_POLL_INTERVAL_MS"))) is not None:
            _set_nested(values, "scheduler.poll_interval_ms", p)
        if (p := _coerce_int(env.get("SCHEDULER_BATCH_SIZE"))) is not None:
            _set_nested(values, "scheduler.batch_size", p)
        if (p := _coerce_int(env.get("SCHEDULER_MAX_CONCURRENT_SENDS"))) is not None:
            _set_nested(values, "scheduler.max_concurrent_sends", p)

        # --- observability ---
        if (p := _coerce_int(env.get("METRICS_PORT"))) is not None:
            _set_nested(values, "observability.metrics_port", p)
        if (v := env.get("OTEL_SERVICE_NAME")) is not None:
            _set_nested(values, "observability.otel_service_name", v)

        # --- auth ---
        # Prefer JWT_PUBLIC_KEY_URL (canonical / .env.example) over the
        # generic JWKS_URL alias; either name supplies the same value.
        if (v := env.get("JWT_PUBLIC_KEY_URL")) is not None:
            _set_nested(values, "auth.jwks_url", v)
        elif (v := env.get("JWKS_URL")) is not None:
            _set_nested(values, "auth.jwks_url", v)
        if (v := env.get("JWT_ISSUER")) is not None:
            _set_nested(values, "auth.issuer", v)
        if (v := env.get("JWT_AUDIENCE")) is not None:
            _set_nested(values, "auth.audience", v)
        if (p := _coerce_int(env.get("JWT_JWKS_CACHE_TTL_SECONDS"))) is not None:
            _set_nested(values, "auth.jwks_cache_ttl_seconds", p)

        # --- http ---
        if (v := env.get("HTTP_USER_AGENT")) is not None:
            _set_nested(values, "http.user_agent", v)
        if (p := _coerce_int(env.get("HTTP_DEFAULT_CONNECT_TIMEOUT_MS"))) is not None:
            _set_nested(values, "http.default_connect_timeout_ms", p)
        if (p := _coerce_int(env.get("HTTP_DEFAULT_READ_TIMEOUT_MS"))) is not None:
            _set_nested(values, "http.default_read_timeout_ms", p)

        return values

    # ------------------------------------------------------------------
    # Cross-field invariants (mode="after")
    # ------------------------------------------------------------------
    @model_validator(mode="after")
    def _cross_field_invariants(self) -> Settings:
        """Enforce invariants that span two or more nested sections.

        Currently checks:

        1. ``kafka.heartbeat_interval_ms < kafka.session_timeout_ms``
           (Kafka protocol requirement — heartbeat must fit within the
           session window or the consumer will be evicted between
           heartbeats).
        2. ``observability.otel_service_name`` is normalised to match
           ``service.name`` so traces and logs share a single
           identifier. This is a soft fix-up (not a fail-fast error)
           because the mismatch is a quality-of-life issue, not a
           correctness one.
        3. ``kafka.topics_consumed`` is disjoint from
           ``kafka.dlq_topics_produced`` — terminal consumers must
           never re-consume their own DLQ output (would create an
           infinite loop on poison messages).
        """
        # 1. Heartbeat must fit strictly within the session window.
        if self.kafka.heartbeat_interval_ms >= self.kafka.session_timeout_ms:
            raise ValueError(
                f"kafka.heartbeat_interval_ms "
                f"({self.kafka.heartbeat_interval_ms}) must be < "
                f"kafka.session_timeout_ms "
                f"({self.kafka.session_timeout_ms}) (Kafka protocol)."
            )

        # 2. Align OTel service.name with service.name (soft fix-up).
        if self.observability.otel_service_name != self.service.name:
            # Pydantic v2 models are mutable by default; a direct
            # assignment is preferred here over rebuilding the model
            # because we want the change to persist on the existing
            # nested instance (and through any references).
            self.observability.otel_service_name = self.service.name

        # 3. Topics-consumed must be disjoint from DLQ topics produced.
        dlq_set = set(self.kafka.dlq_topics_produced)
        overlap = dlq_set.intersection(self.kafka.topics_consumed)
        if overlap:
            raise ValueError(
                "kafka.topics_consumed must not overlap with "
                "kafka.dlq_topics_produced; terminal consumers never "
                f"re-consume their own DLQs. Offending topic(s): "
                f"{sorted(overlap)}."
            )

        return self

    # ------------------------------------------------------------------
    # Settings source customisation (pydantic-settings v2)
    # ------------------------------------------------------------------
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Configure the source chain.

        Priority order (first wins):

            1. ``init_settings``       — kwargs passed to ``Settings(...)``.
            2. ``env_settings``        — environment variables.
            3. ``_YamlSettingsSource`` — default.yaml + <env>.yaml merged.

        ``dotenv_settings`` and ``file_secret_settings`` are intentionally
        omitted from the returned tuple — pydantic-settings still loads
        ``.env`` via ``env_file=".env"`` declared in
        :class:`SettingsConfigDict` (which feeds the ``env_settings``
        source), and file-secret resolution is not used by this service.
        """
        # Suppress unused-arg lint without changing the public signature.
        del dotenv_settings, file_secret_settings
        return (
            init_settings,
            env_settings,
            _YamlSettingsSource(settings_cls),
        )


# ---------------------------------------------------------------------------
# Public factory — singleton config loader.
# ---------------------------------------------------------------------------
@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached, validated :class:`Settings` for this process.

    On first call, loads YAML + env vars, validates every field and
    every cross-field invariant, and caches the result. Subsequent
    calls return the same object (``id()`` identical).

    Tests that need to re-evaluate after monkeypatching ``os.environ``
    or the working directory should call ``get_settings.cache_clear()``
    before invoking :func:`get_settings` again.

    Returns:
        The cached :class:`Settings` instance.

    Raises:
        pydantic.ValidationError: If any required env var is missing or
            any validator rejects a value. This is the AAP R-19
            fail-fast behaviour — the exception propagates out of
            ``build_container(...)`` and causes the process to exit
            non-zero.
    """
    # mypy / pyright cannot trace the env-var injection performed by
    # :meth:`Settings._inject_env_vars` (model_validator(mode="before")),
    # so they flag the no-arg ``Settings()`` call as missing required
    # arguments. The behaviour is correct at runtime — pydantic-settings
    # populates the required fields from env vars and YAML before the
    # __init__ completes.
    return Settings()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------
# Symbols listed here form the formal public API consumed by the rest of
# the service (``container.py``, ``main.py``, ``logging_config.py``, every
# controller / middleware / repository that reads config). Private helpers
# (``_YamlSettingsSource``, ``_deep_merge``, ``_strip_env_suffix_keys``,
# ``_read_yaml``, ``_peek_environment``, ``_set_nested``, ``_coerce_*``)
# remain accessible for unit tests but are NOT considered part of the
# stable interface.
__all__: list[str] = [
    "AuthSettings",
    "ChannelRoutingSettings",
    "CircuitBreakerConfig",
    "ConsumerRetrySettings",
    "DatabaseSettings",
    "EmailSettings",
    "HttpSettings",
    "KafkaSettings",
    "LoggingSettings",
    "ObservabilitySettings",
    "QuietHoursConfig",
    "RetryConfig",
    "SchedulerSettings",
    "ServiceSettings",
    "Settings",
    "SmsSettings",
    "TemplatesSettings",
    "get_settings",
]

