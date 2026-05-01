"""Typed, validated, cached configuration for the Recommendation Engine.

Exposes a single :class:`Settings` model tree plus a :func:`get_settings`
factory that loads and caches it. The loader composes three sources in
this precedence order (highest wins):

    3. environment variables                       (highest precedence)
    2. services/recommendation-engine/config/local.yaml
       (merged onto #1 when ENVIRONMENT=local and the file exists)
    1. services/recommendation-engine/config/default.yaml  (base layer)

The module relies on ``pydantic-settings`` >= 2.0's ``BaseSettings`` with
a custom YAML source adapter (see :class:`_YamlSettingsSource` below).

Fail-fast semantics (AAP R-19)
------------------------------
The first invocation of :func:`get_settings` validates every field and
every cross-field invariant. If a required env var is missing or a
validator rejects a value, ``pydantic.ValidationError`` is raised and
propagates to the caller (``src.main.lifespan`` via ``build_container``),
causing the process to exit non-zero.

Secrets policy (AAP R-25)
-------------------------
No field in this module reads a secret value from YAML. Secret-shaped
fields (DB URLs, Kafka bootstrap, API keys, JWT issuer, etc.) are
populated exclusively from environment variables via the
``_inject_env_vars`` model_validator. YAML files reference env var
NAMES via the ``_env`` convention (e.g. ``base_url_env: PRODUCT_SERVICE_URL``);
the loader strips those suffixed keys via :func:`_strip_env_suffix_keys`
and lets the env-var injector populate the actual values from
``os.environ``.

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
  instance.
* ``src.container.build_container`` — receives the Settings instance and
  constructs all clients.
* ``src.main.lifespan`` — calls :func:`get_settings` as the first startup
  step.

Consumes (files):

* ``services/recommendation-engine/config/default.yaml`` — authoritative
  base configuration (always loaded).
* ``services/recommendation-engine/config/local.yaml`` — local-dev
  overrides (loaded only when ENVIRONMENT=local).
* Environment variables — highest precedence (POSTGRES_URL, REDIS_URL,
  KAFKA_BOOTSTRAP, MODEL_PATH, SCHEMA_REGISTRY_URL, PRODUCT_SERVICE_URL,
  JWT_PUBLIC_KEY_URL, JWT_ISSUER, plus optional overrides ENVIRONMENT,
  LOG_LEVEL).
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import (
    AnyHttpUrl,
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
# Path constants
# ---------------------------------------------------------------------------
# Resolved relative to THIS file so loading works regardless of the process'
# current working directory. The Recommendation Engine is normally launched
# from ``services/recommendation-engine/`` (Dockerfile WORKDIR), but unit
# tests, IDE runners, and ad-hoc invocations may set CWD elsewhere.
#
# Layout assumed:
#   services/recommendation-engine/
#       config/
#           default.yaml           <- _DEFAULT_YAML
#           local.yaml             <- only when ENVIRONMENT=local
#       src/
#           config/
#               settings.py        <- THIS FILE
_THIS_DIR: Path = Path(__file__).resolve().parent
# This file lives at services/recommendation-engine/src/config/settings.py.
# Walking up two levels (config/ -> src/) lands on the service root.
_SERVICE_ROOT: Path = _THIS_DIR.parent.parent
_CONFIG_DIR: Path = _SERVICE_ROOT / "config"
_DEFAULT_YAML: Path = _CONFIG_DIR / "default.yaml"
_LOCAL_YAML_NAME: str = "local.yaml"


# ---------------------------------------------------------------------------
# Module-level type aliases & default factories
# ---------------------------------------------------------------------------
# Defined at module level (rather than inline) so static type checkers
# (mypy --strict, pyright) can infer the correct ``list[_JWTAlgorithm]``
# return type for the default_factory below — an inline lambda would be
# inferred as ``list[str]`` and rejected by mypy.
_JWTAlgorithm = Literal["RS256", "ES256", "RS384", "RS512", "ES384", "ES512"]


def _default_jwt_algorithms() -> list[_JWTAlgorithm]:
    """Return the canonical default JWT signing algorithms.

    RS256 (RSA + SHA-256) and ES256 (ECDSA P-256 + SHA-256) are the two
    asymmetric schemes most commonly emitted by OAuth 2.0 / OIDC
    authorization servers. HS-family symmetric schemes are deliberately
    omitted because they would require this service to hold a shared
    secret (incompatible with AAP R-21 — Auth Service is the SOLE token
    issuer).
    """
    algos: list[_JWTAlgorithm] = ["RS256", "ES256"]
    return algos


# ---------------------------------------------------------------------------
# YAML helpers (module-level so they are unit-testable in isolation).
# ---------------------------------------------------------------------------
def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML file and return its top-level mapping.

    Args:
        path: Filesystem path of the YAML file to read.

    Returns:
        Parsed mapping. An empty file resolves to an empty dict.

    Raises:
        RuntimeError: If the file cannot be read, is malformed YAML, or
            does not parse to a mapping at the document root.
    """
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"Unable to read {path}: {exc}") from exc
    try:
        data = yaml.safe_load(content) or {}
    except yaml.YAMLError as exc:
        raise RuntimeError(f"Invalid YAML at {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"YAML root at {path} must be a mapping")
    return data


def _peek_environment(default_cfg: dict[str, Any]) -> str:
    """Best-effort peek at the effective environment classifier.

    Used by :class:`_YamlSettingsSource` BEFORE the full Settings tree is
    constructed, so it must return a string without invoking pydantic
    validation. The lookup follows the same precedence used by the rest
    of the loader: env var first, then YAML's ``service.environment``
    field, then ``"local"`` as a final default.

    Args:
        default_cfg: Already-parsed ``default.yaml`` mapping.

    Returns:
        Lower-cased environment classifier (one of ``local``/``dev``/
        ``stage``/``prod`` in normal operation; the loader does not
        enforce that set here — strict validation happens later when
        :class:`ServiceSettings` is instantiated).
    """
    # Environment variable wins (highest precedence) even at peek time.
    env_from_env = os.environ.get("ENVIRONMENT")
    if env_from_env:
        return env_from_env.lower()
    service = default_cfg.get("service")
    if isinstance(service, dict):
        env = service.get("environment")
        if isinstance(env, str):
            return env.lower()
    return "local"


def _deep_merge(
    base: dict[str, Any],
    override: dict[str, Any],
) -> dict[str, Any]:
    """Recursively merge ``override`` onto a deep copy of ``base``.

    Dictionaries at the same path are merged key-by-key. Non-dict values
    in ``override`` (including lists) replace the corresponding ``base``
    value entirely; we deliberately do NOT concatenate lists because the
    YAML files use lists for things like ``allowed_origins`` and
    ``algorithms`` where local overrides should fully replace the base.

    The deep-copy step uses a JSON round-trip rather than
    :func:`copy.deepcopy` because YAML content is JSON-compatible by
    construction in this project — every value is a primitive, list, or
    dict — and JSON serialization is materially faster than deepcopy
    while avoiding a third-party ``deepmerge`` dependency.

    Args:
        base: The base mapping (e.g., contents of ``default.yaml``).
        override: The mapping to layer on top (e.g., ``local.yaml``).

    Returns:
        A new dict containing the merged result. Neither input is
        mutated.
    """
    result: dict[str, Any] = json.loads(json.dumps(base))
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _strip_env_suffix_keys(data: Any) -> Any:
    """Recursively remove any mapping key whose name ends in ``_env``.

    YAML files in this service use the convention
    ``<name>_env: VAR_NAME`` to indicate that a field's value should be
    resolved from an environment variable at runtime. The Settings model
    declares ``extra="forbid"`` on every nested model, so leaving these
    informational keys in the dict would cause every load to fail with
    "extra fields not permitted".

    The corresponding env-var values are injected by
    :meth:`Settings._inject_env_vars` BEFORE field validation runs.

    Args:
        data: Arbitrary YAML-derived value. Recurses into dicts and
            lists; primitives pass through unchanged.

    Returns:
        A structurally identical copy of ``data`` with every ``_env``
        key removed at every nesting level.
    """
    if isinstance(data, dict):
        return {
            k: _strip_env_suffix_keys(v)
            for k, v in data.items()
            if not k.endswith("_env")
        }
    if isinstance(data, list):
        return [_strip_env_suffix_keys(v) for v in data]
    return data


# ---------------------------------------------------------------------------
# Section 1 — Shared resilience primitives (RetryDefaults, CircuitBreakerDefaults)
# ---------------------------------------------------------------------------
class RetryDefaults(BaseModel):
    """Shared retry parameters (AAP R-15).

    Used as the default for any caller that does not declare its own
    retry policy. Per-call-site overrides take precedence.

    Attributes:
        max_attempts: Total attempts including the initial call.
        initial_delay_ms: Backoff before the first retry.
        multiplier: Exponential growth factor between successive
            retries.
        max_delay_ms: Upper bound on any single backoff window.
        jitter_pct: Symmetric jitter applied to each backoff to avoid
            retry storms.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    max_attempts: int = Field(default=3, ge=1, le=10)
    initial_delay_ms: int = Field(default=100, ge=0)
    multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    max_delay_ms: int = Field(default=2000, ge=0)
    jitter_pct: int = Field(default=25, ge=0, le=100)

    @model_validator(mode="after")
    def _check_bounds(self) -> RetryDefaults:
        """Reject configurations where ``max_delay_ms < initial_delay_ms``."""
        if self.max_delay_ms < self.initial_delay_ms:
            raise ValueError(
                "retry.max_delay_ms must be >= initial_delay_ms",
            )
        return self


class CircuitBreakerDefaults(BaseModel):
    """Shared circuit-breaker parameters (AAP R-16).

    Used as the default for any caller that does not declare its own
    circuit-breaker policy. Per-call-site overrides take precedence.

    Attributes:
        failure_rate_threshold_pct: Percentage of failed calls in the
            rolling window that trips the breaker to OPEN.
        call_volume_threshold: Minimum number of calls required in the
            rolling window before the failure-rate threshold is
            evaluated (avoids tripping on tiny samples).
        open_duration_ms: How long the breaker stays OPEN before a
            single probe is allowed (transition to HALF_OPEN).
        half_open_permitted_calls: Maximum concurrent probe calls in
            the HALF_OPEN state.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    failure_rate_threshold_pct: int = Field(default=50, ge=1, le=100)
    call_volume_threshold: int = Field(default=20, ge=1)
    open_duration_ms: int = Field(default=30_000, ge=0)
    half_open_permitted_calls: int = Field(default=1, ge=1)


# ---------------------------------------------------------------------------
# Section 2 — Database (PostgreSQL + pgvector)
# ---------------------------------------------------------------------------
class VectorSettings(BaseModel):
    """pgvector column configuration (AAP Section 0.4.4).

    The ``embedding_dim`` field is cross-validated against
    :class:`ModelSettings.embedding_dim` at the Settings level — see
    :meth:`Settings._check_embedding_dim_consistency`.

    Attributes:
        embedding_dim: Number of components in each stored embedding
            vector. MUST equal ``model.embedding_dim`` and the
            ``vector(N)`` column dimension in the migration scripts.
        similarity_op: pgvector operator used for KNN queries.
            ``cosine`` => ``<=>``, ``inner_product`` => ``<#>``,
            ``L2`` => ``<->``.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    embedding_dim: int = Field(default=128, ge=1, le=4096)
    similarity_op: Literal["cosine", "inner_product", "L2"] = "cosine"


class DatabasePoolSettings(BaseModel):
    """Connection-pool tuning knobs for PostgreSQL.

    Attributes:
        min_size: Minimum idle connections kept warm in the pool.
        max_size: Upper bound on concurrent connections per instance.
        connect_timeout_ms: TCP connect timeout to PostgreSQL.
        statement_timeout_ms: Per-statement timeout enforced server-side
            via ``SET statement_timeout``.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    min_size: int = Field(default=1, ge=0)
    max_size: int = Field(default=10, ge=1)
    connect_timeout_ms: int = Field(default=3000, ge=0)
    statement_timeout_ms: int = Field(default=3000, ge=0)

    @model_validator(mode="after")
    def _check_pool_sizes(self) -> DatabasePoolSettings:
        """Reject ``max_size < min_size``."""
        if self.max_size < self.min_size:
            raise ValueError("database.pool.max_size must be >= min_size")
        return self


class DatabaseSettings(BaseModel):
    """PostgreSQL + pgvector connection settings (AAP Section 0.4.4, R-6).

    The ``url`` is required and SecretStr-wrapped so it is masked in
    ``repr(settings)`` output. Operators call
    ``settings.database.url.get_secret_value()`` at the driver boundary
    when constructing a psycopg/SQLAlchemy connection.

    Flat property accessors (``pool_min_size``, ``pool_max_size``,
    ``connect_timeout_seconds``, ``statement_timeout_ms``) are provided
    so ``container.py`` can use either the nested or the flat shape.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    url: SecretStr = Field(
        ...,
        description=(
            "libpq-compatible connection URL. Sourced from POSTGRES_URL; "
            "never committed to YAML per AAP R-25."
        ),
    )
    driver: str = Field(default="postgresql+pgvector")
    pool: DatabasePoolSettings = Field(default_factory=DatabasePoolSettings)
    vector: VectorSettings = Field(default_factory=VectorSettings)

    @field_validator("url")
    @classmethod
    def _check_url_scheme(cls, v: SecretStr) -> SecretStr:
        """Reject database URLs that do not use a postgres-compatible scheme.

        Defense in depth: catches misconfiguration where REDIS_URL or a
        Kafka URL is accidentally placed in POSTGRES_URL. pydantic's
        SecretStr is a thin string wrapper so we extract the value, run
        a light syntactic check, and rewrap.
        """
        raw = v.get_secret_value() if isinstance(v, SecretStr) else str(v)
        if not raw:
            raise ValueError("database.url must be a non-empty string")
        if not raw.startswith(("postgresql://", "postgresql+", "postgres://")):
            raise ValueError(
                "database.url must use a postgresql:// (or postgresql+driver://) "
                "scheme; got an unrecognized prefix.",
            )
        return v

    # ------------------------------------------------------------------
    # Flat accessors that mirror container.py's expected attribute shape.
    # ------------------------------------------------------------------
    @property
    def pool_min_size(self) -> int:
        """Convenience alias for ``self.pool.min_size``."""
        return self.pool.min_size

    @property
    def pool_max_size(self) -> int:
        """Convenience alias for ``self.pool.max_size``."""
        return self.pool.max_size

    @property
    def connect_timeout_seconds(self) -> float:
        """Connect timeout converted from milliseconds to fractional seconds."""
        return self.pool.connect_timeout_ms / 1000.0

    @property
    def statement_timeout_ms(self) -> int:
        """Convenience alias for ``self.pool.statement_timeout_ms``."""
        return self.pool.statement_timeout_ms



# ---------------------------------------------------------------------------
# Section 3 — Cache (Redis)
# ---------------------------------------------------------------------------
class CacheNamespaces(BaseModel):
    """Key prefixes used by the Recommendation Engine's Redis cache.

    Keeping the prefixes in one place avoids drift between repository,
    inference, and fallback modules — each reads
    ``settings.cache.namespaces.<name>`` rather than hard-coding strings.

    Attributes:
        recommendations: Prefix for cached recommendation lists keyed
            by user identifier.
        product_metadata: Prefix for cached Product Service metadata
            (used by the popularity-fallback hydration path).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    recommendations: str = Field(default="reco:user:")
    product_metadata: str = Field(default="prod:meta:")


class CacheSettings(BaseModel):
    """Redis cache configuration (AAP Section 0.4.4).

    The ``redis_url`` is required and SecretStr-wrapped; operators call
    ``settings.cache.redis_url.get_secret_value()`` at the driver
    boundary.

    Attributes:
        redis_url: Full Redis connection URL (e.g.,
            ``redis://host:6379/0`` or ``rediss://host:6380/0``).
            Sourced from the ``REDIS_URL`` env var per AAP R-25.
        driver: Logical driver name (informational).
        default_ttl_seconds: Default TTL applied to cached recommendation
            lists.
        connect_timeout_ms: Connect timeout before Redis is treated as
            unavailable; on timeout the engine degrades to the
            in-process fallback path (AAP R-20).
        read_timeout_ms: Read timeout for Redis commands.
        pool_max_size: Maximum connections in the redis-py connection
            pool.
        namespaces: Logical key-prefix groups owned by this service.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    redis_url: SecretStr = Field(
        ...,
        description=(
            "Redis connection URL. Sourced from REDIS_URL; never committed "
            "to YAML per AAP R-25."
        ),
    )
    driver: str = Field(default="redis")
    default_ttl_seconds: int = Field(default=600, ge=0)
    connect_timeout_ms: int = Field(default=2000, ge=0)
    read_timeout_ms: int = Field(default=3000, ge=0)
    pool_max_size: int = Field(default=20, ge=1)
    namespaces: CacheNamespaces = Field(default_factory=CacheNamespaces)

    @field_validator("redis_url")
    @classmethod
    def _check_redis_url_scheme(cls, v: SecretStr) -> SecretStr:
        """Reject Redis URLs that do not use a redis:// or rediss:// scheme.

        Catches misconfiguration where POSTGRES_URL or a Kafka URL is
        accidentally placed in REDIS_URL. We allow ``redis://`` (TCP)
        and ``rediss://`` (TLS); other schemes (HTTP, raw host:port)
        will fail-fast at startup per AAP R-19.
        """
        raw = v.get_secret_value() if isinstance(v, SecretStr) else str(v)
        if not raw:
            raise ValueError("cache.redis_url must be a non-empty string")
        if not raw.startswith(("redis://", "rediss://", "unix://")):
            raise ValueError(
                "cache.redis_url must use redis://, rediss://, or unix:// "
                "scheme; got an unrecognized prefix.",
            )
        return v

    # ------------------------------------------------------------------
    # Flat accessors used by container.py.
    # ------------------------------------------------------------------
    @property
    def connect_timeout_seconds(self) -> float:
        """Connect timeout in fractional seconds."""
        return self.connect_timeout_ms / 1000.0

    @property
    def read_timeout_seconds(self) -> float:
        """Read timeout in fractional seconds."""
        return self.read_timeout_ms / 1000.0


# ---------------------------------------------------------------------------
# Section 4 — Kafka (consumer-only; AAP R-14, R-17, R-30)
# ---------------------------------------------------------------------------
class KafkaTopics(BaseModel):
    """Kafka topic catalog for this service.

    The Recommendation Engine is a TERMINAL CONSUMER (AAP Section
    0.5.2.2 bullet 9): it consumes ``product.*``, ``order.*``, and
    ``user.*`` events and emits NO Kafka events of its own. The
    ``produce`` field is therefore guarded by
    :meth:`_check_terminal_consumer` to remain empty.

    Attributes:
        consume: Authoritative list of topics this service subscribes
            to. The default mirrors the AAP-mandated consumer set.
        produce: MUST be empty for the Recommendation Engine.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    consume: list[str] = Field(
        default_factory=lambda: [
            "product.created",
            "product.updated",
            "order.created",
            "order.fulfilled",
            "user.registered",
            "user.updated",
        ],
        min_length=1,
    )
    produce: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_terminal_consumer(self) -> KafkaTopics:
        """Reject any non-empty ``produce`` list."""
        if self.produce:
            raise ValueError(
                "kafka.topics.produce must be empty; the Recommendation "
                "Engine is a terminal consumer (AAP Section 0.5.2.2).",
            )
        return self


class KafkaRetrySettings(BaseModel):
    """AAP R-17: retry + DLQ topic conventions.

    The Recommendation Engine consumes domain events. On handler
    failure, messages are routed first to the ``<topic>.retry`` partner
    topic; after exhausting attempts, to ``<topic>.dlq``. This block
    parameterizes that pipeline; the wiring lives in ``src/events/``.

    Attributes:
        topic_suffix: Suffix appended to a topic name to derive its
            retry partner (default ``.retry``).
        dlq_suffix: Suffix appended to a topic name to derive its
            dead-letter partner (default ``.dlq``).
        max_attempts: Maximum redelivery attempts before forwarding to
            the DLQ.
        backoff_ms: Initial backoff between retry attempts.
        max_backoff_ms: Upper bound on any single backoff window.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    topic_suffix: str = Field(default=".retry")
    dlq_suffix: str = Field(default=".dlq")
    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff_ms: int = Field(default=500, ge=0)
    max_backoff_ms: int = Field(default=8000, ge=0)

    @model_validator(mode="after")
    def _check_backoff_bounds(self) -> KafkaRetrySettings:
        """Reject ``max_backoff_ms < backoff_ms``."""
        if self.max_backoff_ms < self.backoff_ms:
            raise ValueError(
                "kafka.retry.max_backoff_ms must be >= backoff_ms",
            )
        return self


class KafkaSchemaRegistrySettings(BaseModel):
    """AAP R-14: Schema Registry endpoint.

    Every Kafka event consumed by this service is validated against a
    schema registered in Confluent Schema Registry. The registry URL is
    populated from the ``SCHEMA_REGISTRY_URL`` env var per AAP R-25.

    Attributes:
        url: HTTP(S) URL of the Schema Registry instance.
        serializer: Wire format expected from Schema Registry.
            ``json_schema`` is the default per the service's YAML config;
            ``avro`` is supported for environments that standardize on
            Avro.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    url: AnyHttpUrl = Field(
        ...,
        description=(
            "Schema Registry URL. Sourced from SCHEMA_REGISTRY_URL; never "
            "committed to YAML per AAP R-25."
        ),
    )
    serializer: Literal["json_schema", "avro"] = "json_schema"


class KafkaSettings(BaseModel):
    """Apache Kafka consumer configuration (AAP R-14, R-17, R-30).

    Attributes:
        bootstrap_servers: Comma-separated broker list (e.g.,
            ``kafka-1:9092,kafka-2:9092``). Sourced from
            ``KAFKA_BOOTSTRAP`` per AAP R-25.
        client_id: Logical client identifier reported to brokers.
        consumer_group: Consumer group identifier — all replicas of the
            Recommendation Engine share this group so partitions are
            distributed across pods.
        auto_offset_reset: Behavior when no committed offset exists for
            the consumer group.
        enable_auto_commit: Disabled by default to enforce at-least-once
            semantics — offsets are committed manually after handler
            success.
        max_poll_records: Maximum records returned by a single ``poll``
            call.
        session_timeout_ms: Group rebalance trigger threshold.
        heartbeat_interval_ms: Heartbeat cadence within a session;
            constrained to be strictly less than session_timeout_ms.
        topics: Topic catalog (consumed + produced).
        retry: Retry/DLQ topic conventions (AAP R-17).
        schema_registry: Schema Registry endpoint (AAP R-14).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    bootstrap_servers: str = Field(
        ...,
        description=(
            "Comma-separated Kafka broker list. Sourced from KAFKA_BOOTSTRAP; "
            "never committed to YAML per AAP R-25."
        ),
        min_length=1,
    )
    client_id: str = Field(default="recommendation-engine")
    consumer_group: str = Field(default="recommendation-engine")
    auto_offset_reset: Literal["earliest", "latest", "none"] = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = Field(default=100, ge=1, le=10_000)
    session_timeout_ms: int = Field(default=45_000, ge=1000)
    heartbeat_interval_ms: int = Field(default=15_000, ge=100)
    topics: KafkaTopics = Field(default_factory=KafkaTopics)
    retry: KafkaRetrySettings = Field(default_factory=KafkaRetrySettings)
    schema_registry: KafkaSchemaRegistrySettings

    @model_validator(mode="after")
    def _check_heartbeat_bounds(self) -> KafkaSettings:
        """Reject ``heartbeat_interval_ms >= session_timeout_ms``.

        Kafka consumer rebalances trigger when the broker stops hearing
        heartbeats within session_timeout. If the heartbeat interval is
        not strictly less than the session timeout, every consumer will
        be considered dead the instant after its first heartbeat slips.
        """
        if self.heartbeat_interval_ms >= self.session_timeout_ms:
            raise ValueError(
                "kafka.heartbeat_interval_ms must be < session_timeout_ms",
            )
        return self


# ---------------------------------------------------------------------------
# Section 5 — Product Service Client
# ---------------------------------------------------------------------------
class ProductServiceClientRetry(BaseModel):
    """Retry policy specific to the Product Service client (AAP R-15).

    Distinct from the shared :class:`RetryDefaults` so the per-call-site
    override pattern called out in default.yaml's ``resilience`` block
    has a concrete model to bind to.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    max_attempts: int = Field(default=3, ge=1, le=10)
    initial_delay_ms: int = Field(default=100, ge=0)
    multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    max_delay_ms: int = Field(default=2000, ge=0)
    jitter_pct: int = Field(default=25, ge=0, le=100)

    @model_validator(mode="after")
    def _check_bounds(self) -> ProductServiceClientRetry:
        """Reject ``max_delay_ms < initial_delay_ms``."""
        if self.max_delay_ms < self.initial_delay_ms:
            raise ValueError(
                "product_service_client.retry.max_delay_ms must be >= "
                "initial_delay_ms",
            )
        return self


class ProductServiceCircuitBreaker(BaseModel):
    """Circuit breaker specific to the Product Service client (AAP R-16).

    Distinct from the shared :class:`CircuitBreakerDefaults` so call-
    site-specific tuning (e.g., a tighter failure-rate threshold for a
    flaky upstream) does not require editing the shared defaults.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    failure_rate_threshold_pct: int = Field(default=50, ge=1, le=100)
    call_volume_threshold: int = Field(default=20, ge=1)
    open_duration_ms: int = Field(default=30_000, ge=0)
    half_open_permitted_calls: int = Field(default=1, ge=1)


class ProductServiceClientSettings(BaseModel):
    """Outbound REST client for the Product Service (AAP Section 0.4.2).

    The Recommendation Engine calls the Product Service to hydrate
    recommendation responses with metadata (name, image URL, price).
    On failure the engine degrades to popularity-based fallback served
    from Redis (AAP R-20).

    Attributes:
        base_url: Base URL of the Product Service. Sourced from
            ``PRODUCT_SERVICE_URL`` per AAP R-25.
        connect_timeout_ms: TCP connect timeout for the HTTP client.
        read_timeout_ms: Response timeout for the HTTP client.
        retry: Retry policy applied to idempotent GETs.
        circuit_breaker: Circuit-breaker policy applied to every call.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    base_url: AnyHttpUrl = Field(
        ...,
        description=(
            "Base URL of the Product Service. Sourced from "
            "PRODUCT_SERVICE_URL; never committed to YAML per AAP R-25."
        ),
    )
    connect_timeout_ms: int = Field(default=1000, ge=0)
    read_timeout_ms: int = Field(default=2000, ge=0)
    retry: ProductServiceClientRetry = Field(
        default_factory=ProductServiceClientRetry,
    )
    circuit_breaker: ProductServiceCircuitBreaker = Field(
        default_factory=ProductServiceCircuitBreaker,
    )


# ---------------------------------------------------------------------------
# Section 6 — Authentication (JWT validation only; AAP R-21, R-22, R-23)
# ---------------------------------------------------------------------------
class JWTSettings(BaseModel):
    """JWT validation settings (AAP R-21, R-22).

    The Recommendation Engine VALIDATES JWTs but never issues them.
    Public keys are fetched at runtime from the Auth Service's JWKS
    endpoint and cached with a bounded TTL (AAP R-22). The signing
    algorithms accepted by this service are restricted to asymmetric
    schemes; HS-family symmetric schemes are deliberately excluded
    because they would require this service to hold a shared secret
    (AAP R-21).

    Attributes:
        public_key_url: JWKS endpoint published by the Auth Service.
            Sourced from ``JWT_PUBLIC_KEY_URL`` per AAP R-25.
        jwks_cache_ttl_seconds: Bounded TTL for the cached JWKS
            document; key rotation in the Auth Service propagates
            within this window (AAP R-22).
        issuer: Expected ``iss`` claim. Sourced from ``JWT_ISSUER``.
        audience: Expected ``aud`` claim — must match
            ``service.name``.
        algorithms: Accepted signature algorithms.
        clock_skew_seconds: Tolerance for clock drift between the
            Auth Service and this service.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    public_key_url: AnyHttpUrl = Field(
        ...,
        description=(
            "JWKS endpoint. Sourced from JWT_PUBLIC_KEY_URL; never committed "
            "to YAML per AAP R-25."
        ),
    )
    jwks_cache_ttl_seconds: int = Field(default=3600, ge=60)
    issuer: str = Field(
        ...,
        description=(
            "Expected iss claim. Sourced from JWT_ISSUER; never committed "
            "to YAML per AAP R-25."
        ),
        min_length=1,
    )
    audience: str = Field(default="recommendation-engine")
    algorithms: list[_JWTAlgorithm] = Field(
        default_factory=_default_jwt_algorithms,
        min_length=1,
    )
    clock_skew_seconds: int = Field(default=30, ge=0, le=300)


class AuthSettings(BaseModel):
    """Authentication + authorization settings.

    Attributes:
        jwt: JWT validation parameters.
        public_routes: Routes that bypass JWT validation. Includes
            ``/health/live``, ``/health/ready``, and ``/metrics`` by
            default so probes and Prometheus scraping continue to work
            without authenticated tokens.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    jwt: JWTSettings
    public_routes: list[str] = Field(
        default_factory=lambda: ["/health/live", "/health/ready", "/metrics"],
    )


# ---------------------------------------------------------------------------
# Section 7 — Model (ML inference)
# ---------------------------------------------------------------------------
class ModelSettings(BaseModel):
    """ML inference model configuration.

    The ``embedding_dim`` is cross-validated against
    :class:`VectorSettings.embedding_dim` at the Settings level — a
    mismatch makes inference impossible at the type boundary because the
    pgvector column shape would not accept the model's output.

    Attributes:
        path: Filesystem path to the serialized model directory or
            artifact. Sourced from ``MODEL_PATH`` per AAP R-25.
        embedding_dim: Number of components produced by the embedding
            head. MUST equal ``database.vector.embedding_dim``.
        top_k_default: Default recommendation list size returned by the
            inference endpoint when the client does not specify
            ``top_k``.
        top_k_max: Hard cap on client-requested ``top_k`` values.
        warmup_on_startup: When True, runs a dummy inference during
            startup so the readiness probe (AAP R-19) only flips green
            once the model is hot.
        reload_endpoint_enabled: Toggle for the internal model-reload
            HTTP endpoint. Disabled by default to reduce attack surface;
            operators flip it on in dev/stage to support A/B model
            evaluation.
        versions_to_keep: Local filesystem retention count for
            previously-served model versions; older versions are
            garbage-collected after each rollout.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        # Pydantic v2 reserves the ``model_`` prefix for internal use; this
        # nested model also lives at the top-level field name ``model`` on
        # Settings. Setting protected_namespaces to () silences the
        # otherwise-noisy DeprecationWarning at instantiation.
        protected_namespaces=(),
    )

    path: str = Field(
        ...,
        description=(
            "Filesystem path to the serialized model. Sourced from MODEL_PATH; "
            "never committed to YAML per AAP R-25."
        ),
        min_length=1,
    )
    embedding_dim: int = Field(default=128, ge=1, le=4096)
    top_k_default: int = Field(default=20, ge=1, le=1000)
    top_k_max: int = Field(default=100, ge=1, le=10_000)
    warmup_on_startup: bool = True
    reload_endpoint_enabled: bool = False
    versions_to_keep: int = Field(default=2, ge=1, le=20)

    @model_validator(mode="after")
    def _check_top_k(self) -> ModelSettings:
        """Reject ``top_k_default > top_k_max`` (would always over-trim)."""
        if self.top_k_default > self.top_k_max:
            raise ValueError("model.top_k_default must be <= top_k_max")
        return self


# ---------------------------------------------------------------------------
# Section 8 — Fallback (AAP R-20)
# ---------------------------------------------------------------------------
class FallbackSettings(BaseModel):
    """Three-tier fallback chain: ML -> Redis cache -> popularity (AAP R-20).

    The chain is consulted in order:

      1. Run ML inference. If confidence >= ``min_inference_confidence``,
         return the result.
      2. Otherwise, look up a cached prior result in Redis (when
         ``cache_enabled``).
      3. Otherwise, fall back to popularity-based recommendations
         derived from the rolling ``popularity_window_days`` window.
      4. As a last resort, return ``default_product_ids`` truncated to
         ``default_list_size``.

    Attributes:
        cache_enabled: When False, skip step 2 entirely (useful when
            Redis is itself unavailable).
        popularity_enabled: When False, skip step 3.
        min_inference_confidence: Confidence threshold below which the
            ML output is rejected.
        popularity_refresh_interval_seconds: How often the popularity
            ranking is recomputed.
        popularity_window_days: Rolling window used to compute
            popularity.
        default_list_size: Size of the returned list when falling back.
        default_product_ids: Final-resort static product list used when
            popularity itself fails (e.g., on a cold cluster bring-up).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cache_enabled: bool = True
    popularity_enabled: bool = True
    min_inference_confidence: float = Field(default=0.3, ge=0.0, le=1.0)
    popularity_refresh_interval_seconds: int = Field(default=300, ge=1)
    popularity_window_days: int = Field(default=30, ge=1, le=365)
    default_list_size: int = Field(default=10, ge=1, le=1000)
    default_product_ids: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Section 9 — Resilience (shared defaults; AAP R-15, R-16)
# ---------------------------------------------------------------------------
class ResilienceSettings(BaseModel):
    """Service-wide defaults for retry and circuit breaker (AAP R-15/R-16).

    Per-call-site blocks (e.g., ``product_service_client.retry``) take
    precedence; this section is the inheritance source for new clients
    that have not yet declared their own policies.

    Attributes:
        retry_default: Default retry policy.
        circuit_breaker_default: Default circuit-breaker policy.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    retry_default: RetryDefaults = Field(default_factory=RetryDefaults)
    circuit_breaker_default: CircuitBreakerDefaults = Field(
        default_factory=CircuitBreakerDefaults,
    )


# ---------------------------------------------------------------------------
# Section 10 — Observability (Prometheus + OpenTelemetry; AAP R-26, R-27)
# ---------------------------------------------------------------------------
class PrometheusSettings(BaseModel):
    """Prometheus /metrics exposure (AAP R-27).

    Attributes:
        enabled: Master switch for Prometheus exposition.
        port: TCP port for the metrics endpoint. Scraped by Metricbeat
            per AAP R-27.
        path: HTTP path for the metrics endpoint.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    enabled: bool = True
    port: int = Field(default=9090, ge=1, le=65535)
    path: str = Field(default="/metrics")


class OTelSettings(BaseModel):
    """OpenTelemetry tracing (optional; AAP R-13 correlation).

    Attributes:
        enabled: Master switch for OpenTelemetry instrumentation. The
            default is False because a local laptop typically has no
            OTLP collector running; it is flipped on in stage/prod via
            environment-specific YAML or env var.
        service_name: Service identifier reported on every span.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    enabled: bool = False
    service_name: str = Field(default="recommendation-engine")


class ObservabilitySettings(BaseModel):
    """Observability — metrics + tracing.

    Attributes:
        prometheus: /metrics endpoint configuration.
        otel: OpenTelemetry tracing configuration.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    prometheus: PrometheusSettings = Field(default_factory=PrometheusSettings)
    otel: OTelSettings = Field(default_factory=OTelSettings)


# ---------------------------------------------------------------------------
# Section 11 — HTTP (FastAPI defaults)
# ---------------------------------------------------------------------------
class CorsSettings(BaseModel):
    """CORS configuration for FastAPI.

    Attributes:
        enabled: Master switch for the CORS middleware.
        allowed_origins: Origins allowed to access the API. Closed by
            default (empty list); local.yaml seeds two common dev ports
            for laptop development.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    enabled: bool = True
    allowed_origins: list[str] = Field(default_factory=list)


class HttpSettings(BaseModel):
    """HTTP-related defaults.

    Attributes:
        cors: CORS middleware configuration.
        user_agent: User-Agent header set on outbound HTTP requests.
        request_timeout_seconds: Default request timeout for outbound
            HTTP calls that do not specify their own.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    cors: CorsSettings = Field(default_factory=CorsSettings)
    user_agent: str = Field(default="recommendation-engine/1.0")
    request_timeout_seconds: int = Field(default=10, ge=1, le=600)


# ---------------------------------------------------------------------------
# Section 12 — Service identity & Logging
# ---------------------------------------------------------------------------
class ServiceSettings(BaseModel):
    """Service identity.

    Attributes:
        name: Logical service name embedded in every log line and
            metric tag (AAP R-26). MUST match the folder name
            ``services/recommendation-engine/``.
        version: Service semantic version. Bumped on every backward-
            incompatible change.
        port: HTTP listen port for the inference API. Must match
            Dockerfile EXPOSE and the container's livenessProbe /
            readinessProbe httpGet port (AAP R-19).
        environment: Logical environment classifier. Allowed values:
            ``local`` | ``dev`` | ``stage`` | ``prod``. Overridden by
            the ``ENVIRONMENT`` env var.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(default="recommendation-engine", min_length=1)
    version: str = Field(default="1.0.0", min_length=1)
    port: int = Field(default=8000, ge=1, le=65535)
    environment: Literal["local", "dev", "stage", "prod"] = "local"


class LoggingSettings(BaseModel):
    """Structured logging configuration (AAP R-26).

    Per AAP R-26, all logs are emitted as structured JSON with the
    canonical fields enumerated in ``include_fields``. ``format`` is
    locked to ``"json"`` because the Filebeat -> Logstash pipeline
    parses JSON (text logs would break the pipeline at AAP R-27).

    Attributes:
        level: Log verbosity. Allowed values: ``DEBUG`` | ``INFO`` |
            ``WARN`` | ``WARNING`` | ``ERROR`` | ``CRITICAL``.
        format: Log shape. MUST remain ``"json"`` per AAP R-26.
        include_fields: Required field set on every log line.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    level: Literal[
        "DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"
    ] = "INFO"
    format: Literal["json"] = "json"  # MUST remain "json" per AAP R-26
    include_fields: list[str] = Field(
        default_factory=lambda: [
            "timestamp",
            "level",
            "service",
            "correlation_id",
            "user_id",
            "route",
            "method",
            "status",
            "latency_ms",
            "message",
        ],
    )



# ---------------------------------------------------------------------------
# YAML settings source for pydantic-settings v2
# ---------------------------------------------------------------------------
class _YamlSettingsSource(PydanticBaseSettingsSource):
    """Load settings from the service's YAML files.

    This source loads ``default.yaml`` (always) and (when
    ``ENVIRONMENT=local`` and the file exists) ``local.yaml``, merges
    them via :func:`_deep_merge`, then strips keys ending in ``_env``
    via :func:`_strip_env_suffix_keys`.

    The merged dict is then handed to pydantic-settings, which uses it
    as one input among several (init kwargs, env vars). Per AAP R-25,
    secret-shaped fields rely on the env-var injection performed by
    :meth:`Settings._inject_env_vars` rather than direct YAML values.

    Note:
        The ``yaml_dir`` parameter is exposed for tests; production
        code always uses the module-level :data:`_CONFIG_DIR`.
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        yaml_dir: Path | None = None,
    ) -> None:
        super().__init__(settings_cls)
        self._yaml_dir: Path = yaml_dir if yaml_dir is not None else _CONFIG_DIR
        self._data: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # PydanticBaseSettingsSource API
    # ------------------------------------------------------------------
    def get_field_value(
        self,
        field: Any,
        field_name: str,
    ) -> tuple[Any, str, bool]:
        """Look up a single top-level field from the merged YAML dict.

        Args:
            field: The pydantic ``FieldInfo`` for the requested field.
                Unused — pydantic-settings invokes this for every
                top-level field in turn.
            field_name: The Python attribute name of the field.

        Returns:
            Tuple of (value-or-None, key-name, is-complex). The third
            element tells pydantic-settings whether to recurse into the
            value as a nested mapping.
        """
        # Suppress unused-arg lint without changing the public signature.
        del field
        data = self._load()
        if field_name in data:
            value = data[field_name]
            return value, field_name, isinstance(value, (dict, list))
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Return the full merged YAML payload as a dict.

        Pydantic-settings v2 calls this once per Settings construction
        to obtain the source's contribution to the merged kwargs.
        """
        return self._load()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _load(self) -> dict[str, Any]:
        """Load, merge, and sanitize the YAML configuration once.

        Cached on the instance so repeated calls during a single
        Settings construction do not re-parse the files.
        """
        if self._data is not None:
            return self._data

        default_path = self._yaml_dir / _DEFAULT_YAML.name
        default_cfg: dict[str, Any] = (
            _read_yaml(default_path) if default_path.exists() else {}
        )

        local_cfg: dict[str, Any] = {}
        environment = _peek_environment(default_cfg)
        if environment == "local":
            local_path = self._yaml_dir / _LOCAL_YAML_NAME
            if local_path.exists():
                local_cfg = _read_yaml(local_path)

        merged = _deep_merge(default_cfg, local_cfg)
        merged = _strip_env_suffix_keys(merged)
        # _strip_env_suffix_keys preserves the dict type; assert for mypy.
        assert isinstance(merged, dict)
        self._data = merged
        return merged


# ---------------------------------------------------------------------------
# Top-level Settings
# ---------------------------------------------------------------------------
class Settings(BaseSettings):
    """Top-level service settings — single source of truth.

    Loaded once per process via :func:`get_settings`. Values come from
    (highest precedence first):

      1. Init kwargs to ``Settings(...)`` — used by tests.
      2. Environment variables — POSTGRES_URL, REDIS_URL,
         KAFKA_BOOTSTRAP, MODEL_PATH, SCHEMA_REGISTRY_URL,
         PRODUCT_SERVICE_URL, JWT_PUBLIC_KEY_URL, JWT_ISSUER, plus
         optional overrides (ENVIRONMENT, LOG_LEVEL).
      3. ``config/local.yaml`` — merged onto ``default.yaml`` only when
         ``ENVIRONMENT=local``.
      4. ``config/default.yaml`` — base layer.

    The env-var injection happens in :meth:`_inject_env_vars`
    (``mode="before"``) which runs after the YAML sources have produced
    a dict and BEFORE nested model construction. The cross-field
    invariants (embedding-dim consistency, kafka terminal-consumer
    guard) run in ``mode="after"`` validators on this same class.

    Raises:
        pydantic.ValidationError: On the first invocation, any missing
            required env var or any failed cross-field invariant raises
            ``ValidationError``. The error propagates out of
            :func:`get_settings` -> ``build_container`` -> ``main`` and
            terminates the process non-zero per AAP R-19.
    """

    # ``protected_namespaces=()`` disables Pydantic v2's ``model_`` namespace
    # protection so the field name ``model`` (which collides with Pydantic's
    # internal namespace prefix) does not emit a deprecation warning.
    model_config = SettingsConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        case_sensitive=False,
        env_file=None,
        env_nested_delimiter="__",
        env_prefix="",
        protected_namespaces=(),
    )

    # The twelve top-level sections, mirroring default.yaml's structure.
    service: ServiceSettings = Field(default_factory=ServiceSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    database: DatabaseSettings
    cache: CacheSettings
    kafka: KafkaSettings
    product_service_client: ProductServiceClientSettings
    auth: AuthSettings
    model: ModelSettings
    fallback: FallbackSettings = Field(default_factory=FallbackSettings)
    resilience: ResilienceSettings = Field(default_factory=ResilienceSettings)
    observability: ObservabilitySettings = Field(
        default_factory=ObservabilitySettings,
    )
    http: HttpSettings = Field(default_factory=HttpSettings)

    # ------------------------------------------------------------------
    # Env-var injection (mode="before")
    # ------------------------------------------------------------------
    @model_validator(mode="before")
    @classmethod
    def _inject_env_vars(cls, values: Any) -> Any:
        """Inject env-var values into the nested config dict.

        Runs AFTER ``_YamlSettingsSource`` has produced the YAML dict
        and BEFORE nested models are constructed, so env-var values
        appear at the right paths as if they had originated in YAML.

        Implementation note:
            Pydantic-settings's built-in env-var source does not
            natively populate fields nested two or three levels deep
            (e.g., ``database.url``, ``kafka.schema_registry.url``,
            ``auth.jwt.issuer``). Walking the merged dict in this
            "before" validator is cleaner than declaring
            ``validation_alias`` on every nested field, and it keeps
            the env-var contract centralized in one method.
        """
        # If pydantic hands us a non-dict (e.g., another Settings instance
        # during model_copy), we have nothing to inject — return as-is.
        if not isinstance(values, dict):
            return values

        def _section_as_dict(section: str) -> dict[str, Any] | None:
            """Return ``values[section]`` as a mutable dict, or None.

            Behavior:
              * If the section is absent or None, create an empty dict
                in ``values[section]`` and return it.
              * If the section is already a dict (the normal YAML/env
                code path), return it directly so the caller can mutate
                in place.
              * If the section is anything else (typically a fully
                constructed BaseModel instance passed via init_kwargs),
                return None to signal that the caller should NOT inject
                env vars for that section — the user has supplied the
                section explicitly and we must not overwrite it.
            """
            node = values.get(section)
            if node is None:
                new_node: dict[str, Any] = {}
                values[section] = new_node
                return new_node
            if isinstance(node, dict):
                return node
            return None

        def _set_if_dict(
            section_dict: dict[str, Any] | None,
            key: str,
            value: str | None,
        ) -> None:
            """Inject ``value`` into ``section_dict[key]`` when both are valid.

            No-op when:
              * ``section_dict`` is None (section was supplied as a
                fully constructed model — must not be modified), or
              * ``value`` is None (env var unset — leave whatever the
                YAML provided so :func:`_check_url_scheme` and other
                validators emit precise error messages).
            """
            if section_dict is None or value is None:
                return
            section_dict[key] = value

        # Top-level sections — may be dicts (normal path) or None
        # (init_kwargs supplied a fully constructed model — leave it).
        db = _section_as_dict("database")
        cache_section = _section_as_dict("cache")
        kafka = _section_as_dict("kafka")
        psc = _section_as_dict("product_service_client")
        auth = _section_as_dict("auth")
        model = _section_as_dict("model")

        # Nested sub-sections under kafka and auth — only navigate when
        # the parent itself is a dict (otherwise the parent is a model
        # instance and we must not touch its internals).
        sr: dict[str, Any] | None
        if kafka is None:
            sr = None
        else:
            sr_existing = kafka.get("schema_registry")
            sr = sr_existing if isinstance(sr_existing, dict) else {}
            kafka["schema_registry"] = sr

        jwt: dict[str, Any] | None
        if auth is None:
            jwt = None
        else:
            jwt_existing = auth.get("jwt")
            jwt = jwt_existing if isinstance(jwt_existing, dict) else {}
            auth["jwt"] = jwt

        # ----------------------------------------------------------------
        # REQUIRED env vars (fail-fast per AAP R-19).
        # The pattern is: env var if set, otherwise keep whatever the YAML
        # provided (typically None for these secret-shaped fields). Setting
        # None on a required field causes Pydantic to raise ValidationError
        # at construction time, which is the desired behavior.
        # ----------------------------------------------------------------
        _set_if_dict(db, "url", os.environ.get("POSTGRES_URL"))
        _set_if_dict(cache_section, "redis_url", os.environ.get("REDIS_URL"))
        _set_if_dict(kafka, "bootstrap_servers", os.environ.get("KAFKA_BOOTSTRAP"))
        _set_if_dict(model, "path", os.environ.get("MODEL_PATH"))

        # ----------------------------------------------------------------
        # CONDITIONALLY-required env vars — fail-fast if absent.
        # ----------------------------------------------------------------
        _set_if_dict(sr, "url", os.environ.get("SCHEMA_REGISTRY_URL"))
        _set_if_dict(psc, "base_url", os.environ.get("PRODUCT_SERVICE_URL"))
        _set_if_dict(jwt, "public_key_url", os.environ.get("JWT_PUBLIC_KEY_URL"))
        _set_if_dict(jwt, "issuer", os.environ.get("JWT_ISSUER"))

        # ----------------------------------------------------------------
        # OPTIONAL operator overrides — only applied when the section is
        # itself a dict (env vars never override an init_kwarg-supplied
        # model).
        # ----------------------------------------------------------------
        env_environment = os.environ.get("ENVIRONMENT")
        if env_environment:
            service = _section_as_dict("service")
            _set_if_dict(service, "environment", env_environment.lower())

        env_log_level = os.environ.get("LOG_LEVEL")
        if env_log_level:
            log = _section_as_dict("logging")
            _set_if_dict(log, "level", env_log_level.upper())

        return values

    # ------------------------------------------------------------------
    # Cross-field validators (mode="after")
    # ------------------------------------------------------------------
    @model_validator(mode="after")
    def _check_embedding_dim_consistency(self) -> Settings:
        """Reject ``database.vector.embedding_dim != model.embedding_dim``.

        This invariant is also reflected in the ``vector(N)`` column
        dimension declared by
        ``migrations/V002__create_embeddings_table.sql``. All three
        values must agree, otherwise inference fails at the type
        boundary when a model output cannot be inserted into the
        database column.
        """
        db_dim = self.database.vector.embedding_dim
        model_dim = self.model.embedding_dim
        if db_dim != model_dim:
            raise ValueError(
                "Configuration mismatch: database.vector.embedding_dim="
                f"{db_dim} must equal model.embedding_dim={model_dim}. "
                "These two values plus the vector(N) column in "
                "migrations/V002__create_embeddings_table.sql must all "
                "agree.",
            )
        return self

    @model_validator(mode="after")
    def _check_kafka_terminal_consumer(self) -> Settings:
        """Reject any non-empty ``kafka.topics.produce`` list.

        Re-asserts the architectural invariant declared on
        :class:`KafkaTopics`. Even though the nested model also enforces
        this, the duplicate check here ensures that a future refactor
        that bypasses :class:`KafkaTopics` (e.g., if ``produce`` is
        promoted to the top level) still trips the guardrail.
        """
        if self.kafka.topics.produce:
            raise ValueError(
                "kafka.topics.produce must be empty; the Recommendation "
                "Engine is a terminal consumer.",
            )
        return self

    # ------------------------------------------------------------------
    # Custom source chain (pydantic-settings v2 hook)
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

        Precedence (first wins):
            1. ``init_settings``      (kwargs to ``Settings(...)``)
            2. ``env_settings``       (environment variables)
            3. ``_YamlSettingsSource`` (default.yaml + local.yaml merged)

        ``dotenv_settings`` and ``file_secret_settings`` are
        intentionally omitted because this service does NOT load
        ``.env`` files at runtime (AAP R-25 — ``.env`` is for local
        dev only, loaded by the developer's shell prior to launching
        the service).
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
            fail-fast behavior — the exception propagates out of
            ``build_container(...)`` and causes the process to exit
            non-zero.
    """
    # mypy/pyright cannot trace the env-var injection performed by
    # :meth:`Settings._inject_env_vars` (model_validator(mode="before")),
    # so they flag the no-arg ``Settings()`` call as missing required
    # arguments. The behavior is correct at runtime — pydantic-settings
    # populates the required fields from env vars and YAML before the
    # __init__ completes.
    return Settings()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------
# Symbols listed here are the formal public API consumed by the rest of the
# service (``container.py``, ``main.py``, ``logging_config.py``, every
# controller / middleware / repository that reads config). Private helpers
# (``_YamlSettingsSource``, ``_deep_merge``, ``_strip_env_suffix_keys``,
# ``_read_yaml``, ``_peek_environment``) remain accessible for unit tests but
# are NOT considered part of the stable interface.
__all__ = [
    "Settings",
    "get_settings",
    "ServiceSettings",
    "LoggingSettings",
    "DatabaseSettings",
    "CacheSettings",
    "KafkaSettings",
    "ProductServiceClientSettings",
    "AuthSettings",
    "ModelSettings",
    "FallbackSettings",
    "ResilienceSettings",
    "ObservabilitySettings",
    "HttpSettings",
]

