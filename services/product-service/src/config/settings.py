"""Typed, layered configuration loader for the Product Service.

Purpose
-------
This module is the **single source of truth for runtime configuration** of
the Product Service. It exposes a single immutable :class:`Settings` object
that materializes from a layered source hierarchy and makes every runtime
knob — service identity, logging, health, MongoDB connectivity, Kafka
producer policy, Schema Registry, Kafka topic names, JWT validation, HTTP
client resilience, catalog domain knobs, idempotency policy, observability
(OpenTelemetry + Prometheus), and feature flags — available to the rest of
the application via the :func:`get_settings` factory.

Loading hierarchy (highest precedence first)
--------------------------------------------
1. **Init kwargs** — keyword arguments passed to ``Settings(...)``;
   primarily used in tests that need to bypass the env-var / YAML overlay.
2. **Environment variables** — flat ``UPPER_SNAKE_CASE`` names declared in
   ``services/product-service/.env.example``; the canonical surface for
   secrets and per-environment overrides per **AAP R-25**. Variables are
   mapped onto the nested model structure by the
   :meth:`Settings._inject_env_vars` ``model_validator(mode="before")``.
3. **.env file** — optional ``services/product-service/.env`` for local
   development; never present in production deployments.
4. **YAML defaults** — ``services/product-service/config/default.yaml``
   plus an optional ``services/product-service/config/<environment>.yaml``
   overlay where ``<environment>`` is the lowercased value of the
   ``ENVIRONMENT`` env var (``local``, ``dev``, ``staging``, ``prod``).
   Loaded by the custom :class:`_YamlSettingsSource` registered via
   :meth:`Settings.settings_customise_sources` as the **lowest**
   precedence source.

Fail-fast guarantee (AAP R-19)
------------------------------
Construction performs the following sequence; **any failure raises a
``pydantic.ValidationError`` and aborts startup** before the HTTP listener
binds:

a. YAML load via :class:`_YamlSettingsSource` (parse + structural
   normalization). Parse errors raise :class:`ValueError` which Pydantic
   surfaces as a validation error.
b. Environment-variable overlay via :meth:`Settings._inject_env_vars`.
   Per-variable type coercion happens in :func:`_coerce_int`,
   :func:`_coerce_float`, :func:`_coerce_bool`, and :func:`_parse_csv`;
   coercion errors raise :class:`ValueError`.
c. Pydantic field validation — types, ranges, ``Literal`` enums, custom
   ``field_validator`` checks.
d. ``model_validator(mode="after")`` checks on every nested class — the
   keystone validators are
   :meth:`KafkaProducerSettings._enforce_durability` (AAP R-30 producer
   durability) and :class:`AuthSettings` JWT-algorithm asymmetry (AAP
   R-23).
e. :meth:`Settings._cross_field_invariants` re-checks the AAP R-23 / R-30
   keystones after the environment overlay AND enforces production-only
   guards (TLS to MongoDB per AAP R-24, MongoDB pool sanity, catalog
   page-size sanity). Belt-and-suspenders defense: every keystone rule is
   enforced at BOTH the nested-class level AND the top-level Settings
   level so no path can bypass it.

Secrets-handling contract (AAP R-25)
------------------------------------
Every credential field is a :class:`pydantic.SecretStr` (or
``SecretStr | None``) so that the default Pydantic ``__repr__`` renders
the value as ``SecretStr('**********')`` and the secret never leaks into
log output, error messages, or ``Settings.model_dump_json()`` output.
Concrete fields:

* :attr:`MongoDbSettings.url` — full MongoDB connection URI with
  embedded credentials (sourced from the ``MONGODB_URL`` env var).
* :attr:`SchemaRegistrySettings.auth` — optional ``user:password`` Basic
  auth string for Confluent Schema Registry (sourced from the
  ``SCHEMA_REGISTRY_AUTH`` env var).

YAML files contain ZERO secret values; secret-shaped fields are populated
exclusively from environment variables via the env-var injector.

MongoDB-specific note (AAP R-7)
-------------------------------
This service uses **MongoDB**, not PostgreSQL — flexible schema is
required because product attributes vary widely by category (apparel
needs size/color, electronics needs voltage/capacity, groceries needs
weight/expiry). The ``mongodb`` settings section therefore differs from
the ``database`` section in sibling Postgres-using services
(inventory-service, order-service, notification-service, payment-service):

* Field is named ``url`` (not ``dsn``); pool fields use Mongo terminology
  (``min_pool_size`` / ``max_pool_size``, not ``min_size`` / ``max_size``).
* Mongo-specific timeouts: ``connect_timeout_ms``, ``socket_timeout_ms``,
  ``server_selection_timeout_ms``.
* ``read_preference`` (``primaryPreferred`` default) and ``write_concern``
  (``majority`` default) are surfaced as configuration knobs.
* ``app_name`` field surfaces in MongoDB server logs for traffic
  attribution.
* Nested ``collections`` block holds the three collection names enumerated
  by **AAP Section 0.4.4**: ``products``, ``categories``, ``product_media``.

Pure-producer note (AAP Section 0.5.2.2 bullet 4)
-------------------------------------------------
The Product Service is a **pure event producer** — it emits
``product.created`` and ``product.updated`` on every successful admin
write but consumes ZERO Kafka topics. There is therefore intentionally
no ``consumer`` block, no ``group_id``, no ``enable_auto_commit``, no
``auto_offset_reset``, and no ``max_poll_records`` on the
:class:`KafkaSettings` model. Adding any consumer-related knob requires
architectural review (AAP R-32 — producers must not know consumers).

Cross-references to AAP rules
-----------------------------
* **AAP Section 0.4.3** — Per-service configuration loading + fail-fast
  startup contract.
* **AAP Section 0.4.4** — ``product_db`` (MongoDB) with three collections
  ``products``, ``categories``, ``product_media``.
* **AAP Section 0.5.2.5** — ``services/*/config/default.yaml`` non-secret
  defaults.
* **AAP R-7** — MongoDB chosen for flexible product schema.
* **AAP R-13** — Correlation-ID propagation; header name configurable
  via :attr:`ObservabilitySettings.correlation_id_header` and
  :attr:`LoggingSettings.correlation_id_header`.
* **AAP R-14** — Schema Registry validation on every Kafka produce;
  URL configurable via :attr:`SchemaRegistrySettings.url`.
* **AAP R-15** — Retry with exponential backoff + jitter; surfaced via
  :class:`RetryConfig` (HTTP client) and the
  :class:`KafkaProducerSettings` backoff fields.
* **AAP R-16** — Circuit breaker thresholds in
  :class:`CircuitBreakerConfig` (shared between HTTP client and Kafka
  producer guard).
* **AAP R-17** — Retry topics + DLQ topics with ``.retry`` / ``.dlq``
  suffixes surfaced via :attr:`KafkaSettings.retry_topic_suffix` /
  :attr:`KafkaSettings.dlq_topic_suffix`.
* **AAP R-19 (CRITICAL)** — Fail-fast at startup. The first call to
  :func:`get_settings` validates every required field and every
  cross-field invariant; missing/invalid values raise
  :class:`pydantic.ValidationError` BEFORE the HTTP listener binds.
* **AAP R-21** — JWT validation only; this service NEVER signs JWTs.
* **AAP R-22** — JWT validation via JWKS with bounded TTL caching;
  :attr:`AuthSettings.jwks_cache_ttl_seconds` carries the TTL.
* **AAP R-23** — OAuth 2.0 / RFC 6749 / RFC 6750 conformance; only
  asymmetric algorithms in :attr:`AuthSettings.algorithms`. Symmetric
  ``HS*`` algorithms are rejected by both the field validator AND the
  top-level cross-field invariant.
* **AAP R-25** — Secrets never in source. :attr:`MongoDbSettings.url`
  and :attr:`SchemaRegistrySettings.auth` are :class:`SecretStr`.
* **AAP R-26** — Structured JSON logs; :attr:`LoggingSettings.level`,
  :attr:`LoggingSettings.format`, and the correlation-ID header drive
  formatter selection.
* **AAP R-30** — Event names ``product.created``, ``product.updated``;
  :attr:`KafkaProducerSettings.acks` MUST be ``"all"`` and
  :attr:`KafkaProducerSettings.enable_idempotence` MUST be ``True`` —
  enforced by both the nested-class validator AND the top-level
  cross-field invariant.

Usage
-----
The canonical entry point is :func:`get_settings`::

    from src.config.settings import get_settings

    settings = get_settings()  # cached singleton; validated once per process
    mongo_uri = settings.mongodb.url.get_secret_value()  # SecretStr unwrap
    bootstrap = settings.kafka.bootstrap

The function is wrapped in :func:`functools.lru_cache(maxsize=1)` so
exactly one :class:`Settings` instance exists per process. Tests that
need an isolated instance can call ``get_settings.cache_clear()``
between cases to force re-construction with fresh environment-variable
overlays. Never construct ``Settings()`` directly outside of this
factory.

Foundational module — no internal imports
-----------------------------------------
This module has **NO internal imports** — it does not import from
``src.domain``, ``src.repository``, ``src.events``, ``src.middleware``,
``src.observability``, ``src.controllers``, ``src.container``, or
``src.main``. Every other module in this package imports from
``src.config.settings``. Adding an internal import here would create a
circular dependency at runtime.
"""

from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Final

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
# Module-Level Logger and Path Constants
# =============================================================================
# These resolve once at import time and remain immutable for the process
# lifetime. They are independent of the process's current working directory
# so the YAML defaults file is locatable from any callsite (uvicorn worker,
# pytest, ad-hoc REPL, etc.).
# =============================================================================

# Module-level logger for source-loading diagnostics. NEVER configures logging
# itself (that lives in src.observability.logging_config). This logger only
# emits during settings construction, which happens BEFORE configure_logging
# runs, so messages flow through stdlib logging defaults — acceptable because
# they are internal-only and rare (one-time at startup).
logger: Final[logging.Logger] = logging.getLogger("product_service.config.settings")

# Resolve the service root regardless of where the process is launched.
# This file lives at services/product-service/src/config/settings.py
# Walk up THREE levels: settings.py -> config/ -> src/ -> product-service/
# (parents[0]=config, parents[1]=src, parents[2]=product-service).
_THIS_FILE: Final[Path] = Path(__file__).resolve()
"""Absolute path to this file."""

SERVICE_ROOT: Final[Path] = _THIS_FILE.parents[2]
"""Absolute path to the service root: ``services/product-service/``."""

CONFIG_DIR: Final[Path] = SERVICE_ROOT / "config"
"""Absolute path to the YAML config directory: ``services/product-service/config/``."""

DEFAULT_YAML: Final[Path] = CONFIG_DIR / "default.yaml"
"""Absolute path to the canonical defaults YAML."""


# =============================================================================
# Helper Functions (private)
# =============================================================================
# All env-var reads, YAML parsing, dict merging, primitive coercion, and CSV
# parsing live here. They are deliberately small, dependency-free, and
# side-effect-free (apart from the env-var reads in _peek_environment, which
# are read-only) so they can be unit tested without instantiating Settings.
# =============================================================================


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML file and return its top-level mapping.

    Returns an empty dict if the file does not exist or is empty/null.
    Raises :class:`ValueError` on parse errors or if the top-level YAML
    document is not a mapping.

    Args:
        path: Absolute or relative path to the YAML file. Existence is
            tested with :meth:`pathlib.Path.is_file`; non-existent files
            yield an empty dict so optional overlays do not break startup.

    Returns:
        Parsed top-level mapping, or ``{}`` if the file is missing/empty.

    Raises:
        ValueError: On YAML parse errors, OS-level read errors, or when
            the parsed document is not a top-level mapping (e.g., a YAML
            list at the root). The original exception is chained via
            ``raise ... from``.
    """
    if not path.is_file():
        return {}
    try:
        with path.open(encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
    except OSError as exc:
        raise ValueError(f"Cannot read YAML file at {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in {path}: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError(
            f"Top-level YAML in {path} must be a mapping, "
            f"got {type(loaded).__name__}"
        )
    return loaded


def _peek_environment() -> str:
    """Detect the ``ENVIRONMENT`` environment variable for YAML overlay selection.

    Returns the lowercased value of ``ENVIRONMENT`` (or ``"local"`` if
    absent or empty). This is a *pre-Settings* peek used ONLY by
    :class:`_YamlSettingsSource.__call__` to decide which
    ``<environment>.yaml`` overlay to load on top of ``default.yaml``.
    Because the YAML source runs before Pydantic's own environment-variable
    source, the validated :attr:`ServiceSettings.environment` value is not
    yet available — we need to know which YAML to load before any field
    validation has happened.

    Returns:
        Lowercased environment classifier (``local``/``dev``/``staging``/
        ``stage``/``prod``/``production`` in normal operation; the loader
        does not enforce that set here — strict validation happens later
        when :class:`ServiceSettings` is instantiated).
    """
    return (os.environ.get("ENVIRONMENT") or "local").strip().lower() or "local"


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``overlay`` onto ``base``, returning a new dict.

    For nested mappings the function recurses; for all other types
    (scalars, lists, tuples, ``None``, etc.) the ``overlay`` value wins.
    The original ``base`` and ``overlay`` arguments are **not mutated** —
    a new dict is returned. Callers can therefore reuse either argument
    after the merge.

    Used by :meth:`_YamlSettingsSource.__call__` to merge an optional
    per-environment overlay (``<environment>.yaml``) on top of the
    canonical ``default.yaml`` defaults.

    Args:
        base: The base mapping (e.g., contents of ``default.yaml``).
        overlay: The mapping to layer on top (e.g., ``production.yaml``).

    Returns:
        A new dict containing the merged result. Neither input is mutated.
    """
    result: dict[str, Any] = dict(base)
    for key, overlay_value in overlay.items():
        base_value = result.get(key)
        if isinstance(base_value, dict) and isinstance(overlay_value, dict):
            result[key] = _deep_merge(base_value, overlay_value)
        else:
            result[key] = overlay_value
    return result


def _set_nested(
    target: dict[str, Any],
    path: tuple[str, ...],
    value: Any,
) -> None:
    """Set a value into a nested dict using a tuple path.

    Walks ``target`` along ``path``, creating intermediate dicts as
    needed, and assigns ``value`` at the leaf key. Used by
    :meth:`Settings._inject_env_vars` to push env-var values into nested
    settings paths (e.g., ``("kafka", "producer", "acks")`` writes the
    string ``"all"`` to ``target["kafka"]["producer"]["acks"]``).

    If an intermediate path component already exists but is not a dict
    (e.g., the user passed a fully constructed BaseModel via init kwargs),
    the existing non-dict value is preserved and a new dict is created in
    its place to receive the leaf value. This is acceptable because the
    env-var overlay is the highest-precedence source after init kwargs.

    Args:
        target: The dict to mutate in place.
        path: Tuple of nested keys from outermost to leaf.
        value: The value to assign at the leaf key.
    """
    if not path:
        return
    cursor = target
    for part in path[:-1]:
        existing = cursor.get(part)
        if not isinstance(existing, dict):
            existing = {}
            cursor[part] = existing
        cursor = existing
    cursor[path[-1]] = value


def _strip_env_suffix_keys(d: dict[str, Any]) -> dict[str, Any]:
    """Recursively remove keys ending with ``_env`` from a nested structure.

    Some monorepo YAML files use ``<key>_env`` keys as inline documentation
    of which environment variable overrides ``<key>`` (e.g.,
    ``level_env: LOG_LEVEL`` next to ``level: INFO``). Those documentation
    keys MUST NOT make it into the loaded config — they would be rejected
    by :class:`pydantic.ConfigDict` when ``extra="forbid"`` is set, and
    they are simply noise when ``extra="ignore"`` is set.

    This helper strips them defensively before the dict reaches Pydantic.
    The Product Service ``default.yaml`` does not currently use this
    convention (its env-var documentation lives in inline ``# env: VAR_NAME``
    comments and ``${VAR:default}`` placeholders), but the helper is
    retained for forward compatibility with overlays that might adopt the
    convention.

    Args:
        d: Arbitrary YAML-derived value. Recurses into dicts and lists;
            primitives pass through unchanged.

    Returns:
        A structurally identical copy of ``d`` with every ``_env`` key
        removed at every nesting level.
    """
    if isinstance(d, dict):
        return {
            k: _strip_env_suffix_keys(v)
            for k, v in d.items()
            if not (isinstance(k, str) and k.endswith("_env"))
        }
    if isinstance(d, list):
        return [_strip_env_suffix_keys(item) for item in d]
    return d


def _coerce_int(
    value: str | int | None,
    default: int | None = None,
) -> int | None:
    """Parse a string env var (or pre-parsed int) into an :class:`int`.

    Returns ``default`` if ``value`` is ``None`` or an empty/whitespace-only
    string. Returns ``int(value)`` for numeric strings or pass-through for
    already-int values. Raises :class:`ValueError` on any non-integer
    string (delegated from :class:`int`).

    Args:
        value: Raw env-var string, an already-parsed int, or None.
        default: Fallback returned when ``value`` is None or empty.

    Returns:
        Parsed int, or ``default`` when the input is missing.

    Raises:
        ValueError: When the input string cannot be parsed as an int.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        # bool is a subclass of int but treating True/False as 1/0 here
        # would silently coerce env-var strings like "True" elsewhere;
        # raise to surface the misuse.
        raise ValueError(f"Cannot coerce bool {value!r} to int")
    if isinstance(value, int):
        return value
    stripped = value.strip()
    if not stripped:
        return default
    return int(stripped)


def _coerce_float(
    value: str | float | None,
    default: float | None = None,
) -> float | None:
    """Parse a string env var (or pre-parsed float) into a :class:`float`.

    Same shape as :func:`_coerce_int` but returns float. Returns
    ``default`` if ``value`` is ``None`` or an empty/whitespace-only
    string. Raises :class:`ValueError` on non-numeric strings (delegated
    from :class:`float`).

    Args:
        value: Raw env-var string, an already-parsed float, or None.
        default: Fallback returned when ``value`` is None or empty.

    Returns:
        Parsed float, or ``default`` when the input is missing.

    Raises:
        ValueError: When the input string cannot be parsed as a float.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError(f"Cannot coerce bool {value!r} to float")
    if isinstance(value, (int, float)):
        return float(value)
    stripped = value.strip()
    if not stripped:
        return default
    return float(stripped)


def _coerce_bool(
    value: str | bool | None,
    default: bool | None = None,
) -> bool | None:
    """Parse a string env var (or pre-parsed bool) into a :class:`bool`.

    Returns ``default`` if ``value`` is None or empty/whitespace-only.

    * Truthy values (case-insensitive): ``true``, ``1``, ``yes``, ``on``.
    * Falsy values (case-insensitive): ``false``, ``0``, ``no``, ``off``.
    * Any other string raises :class:`ValueError`.

    Args:
        value: Raw env-var string, an already-parsed bool, or None.
        default: Fallback returned when ``value`` is None or empty.

    Returns:
        Parsed bool, or ``default`` when the input is missing.

    Raises:
        ValueError: When the input string is neither truthy nor falsy.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    stripped = value.strip().lower()
    if not stripped:
        return default
    if stripped in {"true", "1", "yes", "on"}:
        return True
    if stripped in {"false", "0", "no", "off"}:
        return False
    raise ValueError(f"Cannot coerce {value!r} to bool")


def _parse_csv(
    value: str | list[str] | None,
) -> list[str] | None:
    """Parse a comma-separated env var into a list of stripped strings.

    Returns ``None`` if ``value`` is None. If ``value`` is already a list,
    it is returned as-is (no copy). For comma-separated strings, splits
    on ``,`` and strips whitespace; empty fragments after splitting (e.g.,
    consecutive commas, trailing commas) are dropped so callers receive
    a clean list.

    Used to parse :attr:`AuthSettings.algorithms` from
    ``JWT_ALGORITHMS=RS256,ES256`` and
    :attr:`CatalogSettings.slug_reserved_prefixes` from
    ``CATALOG_SLUG_RESERVED_PREFIXES=admin,api,health,metrics``.

    Args:
        value: Raw env-var string, an already-parsed list, or None.

    Returns:
        Parsed list of non-empty stripped strings, or ``None`` when the
        input is None.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return value
    stripped = value.strip()
    if not stripped:
        return []
    return [item.strip() for item in stripped.split(",") if item.strip()]


# Regex matching the ``${VAR}`` or ``${VAR:default}`` placeholder pattern
# used in default.yaml. Variable names follow the standard env-var
# convention (uppercase letters, digits, underscores; must start with a
# letter or underscore). The optional default literal extends from the
# ``:`` to the closing brace and may contain any character except ``}``.
_PLACEHOLDER_RE: Final[re.Pattern[str]] = re.compile(
    r"^\$\{([A-Za-z_][A-Za-z_0-9]*)(?::([^}]*))?\}$"
)


def _expand_yaml_placeholders(obj: Any) -> Any:
    """Resolve ``${VAR}`` and ``${VAR:default}`` placeholders in YAML strings.

    The Product Service's ``default.yaml`` uses the convention
    ``${VAR}`` (or ``${VAR:default}``) to indicate that a field's value
    should come from the environment variable named ``VAR``, optionally
    falling back to the literal ``default`` if the env var is unset.
    This helper walks an arbitrary YAML-derived structure and replaces
    every fully-matching placeholder string with the resolved value.

    Resolution rules:

    * ``${VAR}`` (no default): if ``os.environ[VAR]`` is set and non-empty,
      substitute its value; otherwise REMOVE the containing key from the
      enclosing dict so pydantic sees the field as absent (triggering
      either a field default or, for required fields, a fail-fast
      ValidationError per AAP R-19).
    * ``${VAR:default}``: if ``os.environ[VAR]`` is set and non-empty,
      substitute its value; otherwise substitute the literal ``default``
      string. Pydantic's coerce-from-string conversion handles type
      coercion for booleans/ints later in the pipeline.

    Strings that do not match the placeholder pattern (i.e., normal
    config values) pass through unchanged. The env-var overlay performed
    by :meth:`Settings._inject_env_vars` later in the pipeline can still
    override these values; this expansion is a YAML-side convenience for
    operators who prefer to keep the env-var contract documented inline
    in the YAML file.

    Args:
        obj: Arbitrary YAML-derived value. Recurses into dicts and lists;
            strings are pattern-matched; other primitives pass through
            unchanged.

    Returns:
        A structurally identical copy of ``obj`` with every placeholder
        string replaced and key entries pointing to unresolved
        placeholders dropped.
    """
    if isinstance(obj, dict):
        result: dict[str, Any] = {}
        for key, value in obj.items():
            replaced = _expand_yaml_placeholders(value)
            # Sentinel: a placeholder for an unset env var with no
            # default returns the special object _UNRESOLVED — drop the
            # entry entirely so pydantic sees the field as absent.
            if replaced is _UNRESOLVED:
                continue
            result[key] = replaced
        return result
    if isinstance(obj, list):
        # Lists drop unresolved placeholder entries similarly so a
        # required-but-unset env var inside an algorithms list doesn't
        # create a None entry that would later fail validation.
        out: list[Any] = []
        for item in obj:
            replaced = _expand_yaml_placeholders(item)
            if replaced is _UNRESOLVED:
                continue
            out.append(replaced)
        return out
    if isinstance(obj, str):
        match = _PLACEHOLDER_RE.match(obj.strip())
        if match is None:
            return obj
        var_name = match.group(1)
        default = match.group(2)
        env_value = os.environ.get(var_name)
        if env_value is not None and env_value != "":
            return env_value
        if default is not None:
            return default
        return _UNRESOLVED
    return obj


# Sentinel object used by :func:`_expand_yaml_placeholders` to signal
# that a placeholder could not be resolved (no env var, no default).
# The enclosing dict/list comprehension drops the entry so pydantic
# sees the field as absent.
_UNRESOLVED: Final[object] = object()


# =============================================================================
# Custom YAML Settings Source for pydantic-settings v2
# =============================================================================


class _YamlSettingsSource(PydanticBaseSettingsSource):
    """Custom :class:`PydanticBaseSettingsSource` that loads YAML defaults.

    Source ordering reproduced for context (see
    :meth:`Settings.settings_customise_sources`):

    1. ``init_settings`` — kwargs passed to ``Settings(...)`` (highest).
    2. ``env_settings`` — environment variables.
    3. ``dotenv_settings`` — optional ``.env`` file (dev only).
    4. **THIS source** — YAML defaults.
    5. ``file_secret_settings`` — Pydantic file-secret loader (lowest).

    On every ``Settings()`` construction this source loads:

    a. ``services/product-service/config/default.yaml`` (always, if it
       exists; absent or empty file yields an empty dict — supports
       running tests without a YAML file present).
    b. An optional per-environment overlay
       ``services/product-service/config/<environment>.yaml`` where
       ``<environment>`` is the lowercased :func:`_peek_environment`
       result. The two files are deep-merged via :func:`_deep_merge`.
    c. The merged dict is run through :func:`_strip_env_suffix_keys` to
       drop any ``_env`` documentation keys (defensive — Product
       Service's ``default.yaml`` does not currently use the convention).

    Failures during YAML loading are logged via the module-level
    :data:`logger` as warnings rather than raising exceptions, so the
    service can still start when YAML files are absent or temporarily
    unreadable, falling back to env vars + hard-coded defaults. This
    mirrors the pattern in sibling services (recommendation-engine,
    inventory-service) for operational consistency.

    AAP refs: 0.4.3 (config loading), 0.5.2.5 (default.yaml shape),
    R-19 (validation runs after merge).
    """

    def __init__(
        self,
        settings_cls: type[BaseSettings],
        config_dir: Path = CONFIG_DIR,
    ) -> None:
        """Initialize the source with a settings class and config directory.

        Args:
            settings_cls: The :class:`Settings` class this source feeds.
            config_dir: Directory containing ``default.yaml`` and any
                ``<environment>.yaml`` overlays. Defaults to the module-
                level :data:`CONFIG_DIR`; tests may override.
        """
        super().__init__(settings_cls)
        self._config_dir: Path = config_dir
        self._cached: dict[str, Any] | None = None

    # ------------------------------------------------------------------
    # PydanticBaseSettingsSource API
    # ------------------------------------------------------------------
    def get_field_value(
        self,
        field: Any,  # noqa: ARG002 — required by base class signature
        field_name: str,
    ) -> tuple[Any, str, bool]:
        """Per-field accessor required by :class:`PydanticBaseSettingsSource`.

        Pydantic-settings calls this for each model field when iterating
        sources. Because we override :meth:`__call__` to return the entire
        merged dict in one pass (so Pydantic's own field-level merging
        handles the nesting), this method is a no-op that returns
        ``(None, field_name, False)`` to signal "no per-field value
        available — defer to the dict returned by ``__call__``".

        Args:
            field: The pydantic ``FieldInfo`` for the requested field.
                Unused — pydantic-settings invokes this for every
                top-level field in turn.
            field_name: The Python attribute name of the field.

        Returns:
            Tuple of ``(None, field_name, False)``.
        """
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Load and merge YAML sources, returning a Pydantic-ready dict.

        Pydantic-settings v2 calls this once per ``Settings()`` construction
        to obtain the source's contribution to the merged kwargs.
        Subsequent calls within the same construction reuse the cached
        result on ``self._cached`` to avoid re-parsing the YAML files.

        Returns:
            Merged dict of YAML-derived configuration with ``_env``
            documentation keys stripped. Empty dict if all YAML files
            are missing/empty/unreadable.
        """
        if self._cached is not None:
            return self._cached

        merged: dict[str, Any] = {}
        try:
            default_path = self._config_dir / "default.yaml"
            default_cfg = _read_yaml(default_path)
        except ValueError as exc:
            # YAML parse error — log and degrade rather than crash. Env
            # vars must supply every required field for startup to
            # succeed; the resulting ValidationError will surface the
            # missing values clearly.
            logger.warning(
                "Failed to load default.yaml at %s: %s; "
                "continuing with env-var overlay only",
                self._config_dir / "default.yaml",
                exc,
            )
            default_cfg = {}

        environment = _peek_environment()
        overlay_cfg: dict[str, Any] = {}
        if environment:
            try:
                overlay_path = self._config_dir / f"{environment}.yaml"
                overlay_cfg = _read_yaml(overlay_path)
            except ValueError as exc:
                logger.warning(
                    "Failed to load %s.yaml at %s: %s; "
                    "continuing with default.yaml + env vars only",
                    environment,
                    self._config_dir / f"{environment}.yaml",
                    exc,
                )
                overlay_cfg = {}

        if default_cfg or overlay_cfg:
            merged = _deep_merge(default_cfg, overlay_cfg)
            cleaned = _strip_env_suffix_keys(merged)
            # Expand ${VAR} / ${VAR:default} placeholders so YAML strings
            # that reference env vars (e.g., 'database: ${MONGODB_DATABASE:
            # product_db}') resolve to either the env value or the
            # documented default. Unresolved placeholders (env unset, no
            # default) are dropped so pydantic sees the field as absent
            # — triggering field defaults for optional fields and AAP
            # R-19 fail-fast for required ones.
            expanded = _expand_yaml_placeholders(cleaned)
            merged = expanded if isinstance(expanded, dict) else {}

        self._cached = merged
        return merged


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
#  * ``model_config = ConfigDict(extra="ignore", str_strip_whitespace=True,
#    validate_assignment=False)``
#    — ignore unknown fields (forward compat), strip surrounding whitespace
#    on string values, do NOT re-validate on attribute assignment (validators
#    that synchronize fields use object.__setattr__).
#  * Class docstring naming the AAP rules satisfied.
#  * Field-level documentation via ``Field(..., description="...")``.
#  * Field validators for normalization (case folding, allowed-value sets).
#  * Model validators for cross-field invariants (AAP R-30 producer
#    durability, AAP R-23 algorithm asymmetry, pool-size sanity, etc.).
#
# Importantly, NONE of these classes inherit from :class:`BaseSettings`.
# Only the top-level :class:`Settings` class is a :class:`BaseSettings`
# subclass — nested classes are plain :class:`BaseModel`. This is
# deliberate: :class:`BaseSettings` injects an env-var source into every
# model instance, and nested settings classes would fight the top-level
# :class:`Settings` for environment ownership.
# =============================================================================


# Allowed values for the ``log_level`` and ``logging.level`` fields. Case
# folded to upper case by the relevant field validators before comparison.
_ALLOWED_LOG_LEVELS: Final[frozenset[str]] = frozenset(
    {"DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"}
)

# Allowed values for the ``log_format`` and ``logging.format`` fields.
# Case folded to lower case by the relevant field validators.
_ALLOWED_LOG_FORMATS: Final[frozenset[str]] = frozenset({"json", "text"})

# Allowed values for the ``service.environment`` field. Case folded to
# lower case; both ``stage``/``staging`` and ``prod``/``production`` are
# accepted to match differing operator conventions across environments.
_ALLOWED_ENVIRONMENTS: Final[frozenset[str]] = frozenset(
    {"local", "dev", "staging", "stage", "prod", "production", "test"}
)

# Allowed values for ``mongodb.read_preference``. Unrecognized values
# are rejected at construction time (pymongo would otherwise raise at
# the driver boundary, much later in startup).
_ALLOWED_READ_PREFERENCES: Final[frozenset[str]] = frozenset(
    {"primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"}
)

# Allowed Kafka producer compression types accepted by librdkafka /
# confluent-kafka. ``none`` disables compression entirely.
_ALLOWED_COMPRESSION_TYPES: Final[frozenset[str]] = frozenset(
    {"none", "gzip", "snappy", "lz4", "zstd"}
)

# Allowed Kafka producer ack levels. AAP R-30 mandates ``"all"`` for the
# product service; the field validator allows {"0", "1", "all"} but the
# ``_enforce_durability`` model_validator rejects anything other than
# ``"all"`` at construction time.
_ALLOWED_ACKS: Final[frozenset[str]] = frozenset({"0", "1", "all"})

# Allowed asymmetric JWT signing algorithms (AAP R-23). HS-family
# (symmetric) algorithms are explicitly rejected because they would
# require this service to share a signing secret with the Auth Service —
# incompatible with AAP R-21 (Auth Service is the SOLE issuer).
_ALLOWED_ASYMMETRIC_ALGS: Final[frozenset[str]] = frozenset(
    {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512"}
)


# -----------------------------------------------------------------------------
# 5.1 RetryConfig (shared resilience primitive — AAP R-15)
# -----------------------------------------------------------------------------
class RetryConfig(BaseModel):
    """Shared retry parameters (AAP R-15: exponential backoff + jitter).

    Used as the default for any caller that does not declare its own
    retry policy. Per-call-site overrides take precedence. The Product
    Service uses this primarily for outbound HTTP calls (JWKS fetch,
    Schema Registry HTTP API).

    Fields apply to the standard exponential-backoff schedule::

        delay = min(initial_delay_ms * (multiplier ** attempt), max_delay_ms)
        delay *= 1 + uniform(-jitter_pct, +jitter_pct)

    Attributes:
        max_attempts: Total attempts including the initial call (1..20).
        initial_delay_ms: Backoff before the first retry, in milliseconds.
        multiplier: Exponential growth factor between successive retries.
        max_delay_ms: Upper bound on any single backoff window. Must be
            >= ``initial_delay_ms`` (enforced by ``_validate_backoffs``).
        jitter_pct: Symmetric jitter applied to each backoff to avoid
            retry storms; expressed as a fraction of the computed delay
            in the range ``[0.0, 1.0]``.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    max_attempts: int = Field(
        default=3,
        ge=1,
        le=20,
        description="AAP R-15: max retry attempts including the initial call.",
    )
    initial_delay_ms: int = Field(
        default=200,
        ge=0,
        description="AAP R-15: initial backoff delay in milliseconds.",
    )
    multiplier: float = Field(
        default=2.0,
        gt=0,
        description="AAP R-15: exponential multiplier between successive retries.",
    )
    max_delay_ms: int = Field(
        default=5000,
        ge=0,
        description="AAP R-15: upper bound on any single backoff window.",
    )
    jitter_pct: float = Field(
        default=0.2,
        ge=0.0,
        le=1.0,
        description="AAP R-15: jitter expressed as a fraction of computed delay.",
    )

    @model_validator(mode="after")
    def _validate_backoffs(self) -> RetryConfig:
        """Reject configurations where ``max_delay_ms < initial_delay_ms``.

        An exponential schedule that capped below the starting delay
        would never grow — a configuration error rather than a tunable
        knob.
        """
        if self.max_delay_ms < self.initial_delay_ms:
            raise ValueError(
                "RetryConfig.max_delay_ms must be >= initial_delay_ms "
                f"(got max={self.max_delay_ms!r}, "
                f"initial={self.initial_delay_ms!r})"
            )
        return self


# -----------------------------------------------------------------------------
# 5.2 CircuitBreakerConfig (shared resilience primitive — AAP R-16)
# -----------------------------------------------------------------------------
class CircuitBreakerConfig(BaseModel):
    """Shared circuit-breaker parameters (AAP R-16).

    Used as the default for any caller that does not declare its own
    circuit-breaker policy. Per-call-site overrides take precedence. The
    Product Service uses this for outbound HTTP traffic (JWKS fetch) and
    for the Kafka produce path so a flapping broker cannot exhaust the
    request thread pool.

    Attributes:
        failure_threshold: Consecutive failures required to trip the
            breaker into the OPEN state. Must be >= 1.
        reset_timeout_ms: Duration the breaker stays OPEN before a single
            probe is allowed (transition to HALF_OPEN). On a successful
            probe the breaker transitions back to CLOSED; on failure it
            returns to OPEN.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    failure_threshold: int = Field(
        default=5,
        ge=1,
        description="AAP R-16: consecutive failures to open the breaker.",
    )
    reset_timeout_ms: int = Field(
        default=30_000,
        ge=1,
        description="AAP R-16: open-state duration before probing.",
    )


# -----------------------------------------------------------------------------
# 5.3 ServiceSettings — process identity (AAP R-19, R-26)
# -----------------------------------------------------------------------------
class ServiceSettings(BaseModel):
    """Service identity and runtime parameters.

    Drives the ``service`` log field (AAP R-26), the FastAPI ``title``
    in OpenAPI metadata, the production-mode docs/redoc disablement
    (consumed by ``src.main.create_app``), and the liveness/readiness
    probe paths (AAP R-19).

    Attributes:
        name: Logical service name. Embedded in every log line and
            metric tag; MUST match the folder name
            ``services/product-service/`` for monorepo consistency.
        environment: Deployment environment classifier (``local``,
            ``dev``, ``staging``/``stage``, ``prod``/``production``,
            ``test``). Both ``stage``/``staging`` and ``prod``/
            ``production`` are accepted to match differing operator
            conventions. **REQUIRED** — no default; supplied via
            YAML or the ``ENVIRONMENT`` env var.
        port: HTTP listen port for the public catalog API. Must match
            the Dockerfile EXPOSE directive and the container's
            livenessProbe / readinessProbe httpGet port (AAP R-19).
        log_level: Logging verbosity (mirrors :attr:`LoggingSettings.level`
            for back-compat with operators who set ``LOG_LEVEL``).
        log_format: Log shape (``json`` or ``text``); mirrors
            :attr:`LoggingSettings.format`.
        health_liveness_path: Path served by the kubelet liveness probe
            (AAP R-19).
        health_readiness_path: Path served by the kubelet readiness
            probe (AAP R-19); 200 only after MongoDB pool, Kafka
            producer, and JWKS cache are ready.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    name: str = Field(
        default="product-service",
        description=(
            "Logical service name; AAP R-26 log field 'service'. MUST "
            "match the folder name services/product-service/."
        ),
    )
    environment: str = Field(
        ...,
        description=(
            "Deployment environment: local | dev | stage[-ing] | "
            "prod[uction] | test. Required — no default; supplied via "
            "YAML or the ENVIRONMENT env var."
        ),
    )
    port: int = Field(
        default=8000,
        ge=1,
        le=65535,
        description=(
            "HTTP listen port; matches Dockerfile EXPOSE 8000. Override "
            "via SERVICE_PORT for non-standard local-dev ports."
        ),
    )
    log_level: str = Field(
        default="INFO",
        description=(
            "Mirrors logging.level for back-compat with operators who "
            "set LOG_LEVEL. Allowed: DEBUG | INFO | WARNING | WARN | "
            "ERROR | CRITICAL (case-insensitive)."
        ),
    )
    log_format: str = Field(
        default="json",
        description=(
            "Mirrors logging.format for back-compat. Allowed: json | "
            "text (case-insensitive). AAP R-26."
        ),
    )
    health_liveness_path: str = Field(
        default="/health/live",
        description=(
            "Path served by the kubelet liveness probe (AAP R-19). "
            "Must return 200 OK whenever the process is running and "
            "the event loop is responsive."
        ),
    )
    health_readiness_path: str = Field(
        default="/health/ready",
        description=(
            "Path served by the kubelet readiness probe (AAP R-19). "
            "Returns 200 OK only when MongoDB pool, Kafka producer, "
            "and JWKS cache are ready; 503 otherwise."
        ),
    )

    @field_validator("environment", mode="before")
    @classmethod
    def _normalize_environment(cls, v: Any) -> Any:
        """Lowercase and validate the environment classifier.

        Accepts the full set of conventional names — both ``stage``/
        ``staging`` and ``prod``/``production`` are tolerated to match
        deployment platforms that use either spelling. Rejection is at
        the field level (not via :class:`Literal`) so the error message
        is descriptive.
        """
        if v is None:
            return v
        if not isinstance(v, str):
            return v
        normalized = v.strip().lower()
        if normalized and normalized not in _ALLOWED_ENVIRONMENTS:
            raise ValueError(
                f"service.environment must be one of "
                f"{sorted(_ALLOWED_ENVIRONMENTS)} (got {v!r})"
            )
        return normalized

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, v: Any) -> Any:
        """Uppercase and validate the log level."""
        if v is None or not isinstance(v, str):
            return v
        normalized = v.strip().upper()
        if normalized and normalized not in _ALLOWED_LOG_LEVELS:
            raise ValueError(
                f"service.log_level must be one of "
                f"{sorted(_ALLOWED_LOG_LEVELS)} (got {v!r})"
            )
        return normalized

    @field_validator("log_format", mode="before")
    @classmethod
    def _normalize_log_format(cls, v: Any) -> Any:
        """Lowercase and validate the log format."""
        if v is None or not isinstance(v, str):
            return v
        normalized = v.strip().lower()
        if normalized and normalized not in _ALLOWED_LOG_FORMATS:
            raise ValueError(
                f"service.log_format must be one of "
                f"{sorted(_ALLOWED_LOG_FORMATS)} (got {v!r})"
            )
        return normalized


# -----------------------------------------------------------------------------
# 5.4 LoggingSettings — structured JSON logs (AAP R-26)
# -----------------------------------------------------------------------------
class LoggingSettings(BaseModel):
    """Structured logging configuration (AAP R-26).

    Per AAP R-26, all logs are emitted as structured JSON with the
    canonical fields enumerated by the AAP. The :attr:`format` field is
    typically locked to ``"json"`` because the Filebeat -> Logstash
    pipeline parses JSON (text logs would break the pipeline at AAP R-27).

    Attributes:
        level: Log verbosity. Allowed: DEBUG, INFO, WARNING, WARN, ERROR,
            CRITICAL (case-insensitive; normalized to upper).
        format: Log shape. Allowed: json, text (case-insensitive).
        correlation_id_header: HTTP header name carrying the correlation
            ID across services (AAP R-13).
        redact_fields: Field names to redact in JSON logs to prevent
            accidental secret leakage. Surrounding strings matching any
            of these names will have their values replaced with
            ``"[REDACTED]"`` by the structured logger.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    level: str = Field(
        default="INFO",
        description="AAP R-26 log level (DEBUG | INFO | WARNING | ERROR | CRITICAL).",
    )
    format: str = Field(
        default="json",
        description=(
            "AAP R-26 log shape. JSON is mandatory in non-local "
            "environments; text is permitted only for local debugging."
        ),
    )
    correlation_id_header: str = Field(
        default="X-Correlation-ID",
        description="AAP R-13 — HTTP header name for correlation ID propagation.",
    )
    redact_fields: list[str] = Field(
        default_factory=lambda: [
            "password",
            "secret",
            "token",
            "authorization",
            "api_key",
        ],
        description=(
            "Field names whose values are replaced with [REDACTED] in "
            "JSON log output to prevent accidental secret leakage."
        ),
    )

    @field_validator("level", mode="before")
    @classmethod
    def _normalize_level(cls, v: Any) -> Any:
        """Uppercase and validate the log level."""
        if v is None or not isinstance(v, str):
            return v
        normalized = v.strip().upper()
        if normalized and normalized not in _ALLOWED_LOG_LEVELS:
            raise ValueError(
                f"logging.level must be one of "
                f"{sorted(_ALLOWED_LOG_LEVELS)} (got {v!r})"
            )
        return normalized

    @field_validator("format", mode="before")
    @classmethod
    def _normalize_format(cls, v: Any) -> Any:
        """Lowercase and validate the log format."""
        if v is None or not isinstance(v, str):
            return v
        normalized = v.strip().lower()
        if normalized and normalized not in _ALLOWED_LOG_FORMATS:
            raise ValueError(
                f"logging.format must be one of "
                f"{sorted(_ALLOWED_LOG_FORMATS)} (got {v!r})"
            )
        return normalized


# -----------------------------------------------------------------------------
# 5.5 CollectionsSettings — Mongo collection name overrides (AAP Section 0.4.4)
# -----------------------------------------------------------------------------
class CollectionsSettings(BaseModel):
    """MongoDB collection name overrides (AAP Section 0.4.4).

    The Product Service owns three collections in ``product_db`` per
    AAP Section 0.4.4: ``products``, ``categories``, ``product_media``.
    These names are surfaced as configuration knobs (rather than hard-
    coded constants) primarily to support multi-tenant deployments where
    collection names may carry a tenant prefix, and to facilitate
    integration tests that use distinct collection names per test run.

    Attributes:
        products: Collection name for the product catalog documents.
        categories: Collection name for the category tree documents.
        product_media: Collection name for product media metadata
            (image URLs, captions, alt text, ordering).
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    products: str = Field(
        default="products",
        min_length=1,
        description="AAP Section 0.4.4 — products collection name.",
    )
    categories: str = Field(
        default="categories",
        min_length=1,
        description="AAP Section 0.4.4 — categories collection name.",
    )
    product_media: str = Field(
        default="product_media",
        min_length=1,
        description="AAP Section 0.4.4 — product_media collection name.",
    )


# -----------------------------------------------------------------------------
# 5.6 MongoDbSettings — product_db driver settings (AAP R-7, Section 0.4.4)
# -----------------------------------------------------------------------------
class MongoDbSettings(BaseModel):
    """MongoDB connection settings for ``product_db`` (AAP R-7, Section 0.4.4).

    The Product Service owns its private ``product_db`` MongoDB instance
    per AAP R-6 (database-per-service). The connection URI is supplied
    via the ``MONGODB_URL`` env var per AAP R-25 and stored as a
    :class:`SecretStr` so the value never appears in
    ``Settings.model_dump_json()`` output, ``__repr__``, or error
    messages.

    Pool sizing (``min_pool_size`` / ``max_pool_size``) and the three
    timeout fields (``connect_timeout_ms``, ``socket_timeout_ms``,
    ``server_selection_timeout_ms``) match the motor / pymongo driver's
    parameter names verbatim; values flow directly into the driver
    constructor in ``src/repository/mongo_client.py``.

    Attributes:
        url: Full MongoDB connection URI (e.g.,
            ``mongodb://user:pass@host:27017/?authSource=admin`` or
            ``mongodb+srv://...``). **REQUIRED** — sourced from the
            ``MONGODB_URL`` env var per AAP R-25.
        database: Logical database name (default ``product_db``).
        min_pool_size: Driver ``minPoolSize`` — minimum idle connections
            kept warm in the pool.
        max_pool_size: Driver ``maxPoolSize`` — upper bound on
            concurrent connections per process. Must be >=
            ``min_pool_size`` (enforced by ``_validate_pool_sizes``).
        connect_timeout_ms: Driver ``connectTimeoutMS`` — initial TCP
            connect timeout to MongoDB.
        socket_timeout_ms: Driver ``socketTimeoutMS`` — per-operation
            socket timeout.
        server_selection_timeout_ms: Driver ``serverSelectionTimeoutMS``
            — how long the driver waits for a primary during reads /
            writes / replica-set failover events.
        retry_writes: Driver ``retryWrites``; AAP R-15 driver-level
            retry for transient write failures.
        retry_reads: Driver ``retryReads``; AAP R-15 driver-level retry
            for transient read failures.
        read_preference: Read preference: ``primary`` |
            ``primaryPreferred`` | ``secondary`` | ``secondaryPreferred``
            | ``nearest``.
        write_concern: Write concern: ``majority`` | ``1`` | ``2`` |
            ``3`` | any other value passed through to motor.
        tls_enabled: Whether to enable TLS in transit (AAP R-24).
            Defaults to True; the cross-field validator on
            :class:`Settings` rejects ``tls_enabled=False`` when
            ``service.environment`` is ``prod`` / ``production``.
        tls_ca_file: Optional path to a CA bundle for TLS verification.
        app_name: MongoDB ``appName`` — appears in MongoDB server logs
            for client traffic attribution.
        collections: Collection name overrides (AAP Section 0.4.4).
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    url: SecretStr = Field(
        ...,
        description=(
            "MongoDB connection URI (mongodb:// or mongodb+srv://). "
            "REQUIRED — sourced from MONGODB_URL env var per AAP R-25. "
            "Wrapped in SecretStr so the value never leaks into log "
            "output or Settings.model_dump_json()."
        ),
    )
    database: str = Field(
        default="product_db",
        min_length=1,
        description=(
            "Logical database name (AAP Section 0.4.4 default: "
            "product_db). Override via MONGODB_DATABASE for multi-"
            "tenant deployments."
        ),
    )
    min_pool_size: int = Field(
        default=2,
        ge=0,
        description=(
            "Driver minPoolSize — minimum idle connections kept warm "
            "in the pool."
        ),
    )
    max_pool_size: int = Field(
        default=50,
        ge=1,
        description=(
            "Driver maxPoolSize — upper bound on concurrent connections "
            "per process. Must be >= min_pool_size."
        ),
    )
    connect_timeout_ms: int = Field(
        default=5000,
        ge=1,
        description="Driver connectTimeoutMS — initial TCP connect timeout.",
    )
    socket_timeout_ms: int = Field(
        default=10_000,
        ge=1,
        description="Driver socketTimeoutMS — per-operation socket timeout.",
    )
    server_selection_timeout_ms: int = Field(
        default=5000,
        ge=1,
        description=(
            "Driver serverSelectionTimeoutMS — how long the driver "
            "waits for a primary during reads/writes/failovers."
        ),
    )
    retry_writes: bool = Field(
        default=True,
        description="Driver retryWrites; AAP R-15 driver-level retry for writes.",
    )
    retry_reads: bool = Field(
        default=True,
        description="Driver retryReads; AAP R-15 driver-level retry for reads.",
    )
    read_preference: str = Field(
        default="primaryPreferred",
        description=(
            "Read preference: primary | primaryPreferred | secondary | "
            "secondaryPreferred | nearest. Default primaryPreferred — "
            "read from primary, fall back to secondary on outage."
        ),
    )
    write_concern: str = Field(
        default="majority",
        min_length=1,
        description=(
            "Write concern (majority | 1 | 2 | 3 | ...). Default "
            "'majority' — acknowledged by majority of replica-set "
            "members for durability."
        ),
    )
    tls_enabled: bool = Field(
        default=True,
        description=(
            "AAP R-24 TLS in transit. The Settings cross-field "
            "validator rejects False when environment is prod/production."
        ),
    )
    tls_ca_file: str | None = Field(
        default=None,
        description="Optional path to a CA bundle for TLS verification.",
    )
    app_name: str = Field(
        default="product-service",
        min_length=1,
        description="MongoDB appName — surfaces in server logs for traffic attribution.",
    )
    collections: CollectionsSettings = Field(
        default_factory=CollectionsSettings,
        description="Collection name overrides (AAP Section 0.4.4).",
    )

    @field_validator("read_preference", mode="before")
    @classmethod
    def _normalize_read_preference(cls, v: Any) -> Any:
        """Validate read preference against the pymongo accepted set.

        Catches typos like ``primaryPrefered`` (missing one ``r``) at
        configuration time rather than at the driver boundary.
        """
        if v is None or not isinstance(v, str):
            return v
        stripped = v.strip()
        if stripped and stripped not in _ALLOWED_READ_PREFERENCES:
            raise ValueError(
                f"mongodb.read_preference must be one of "
                f"{sorted(_ALLOWED_READ_PREFERENCES)} (got {v!r})"
            )
        return stripped

    @field_validator("url", mode="before")
    @classmethod
    def _check_url_nonempty(cls, v: Any) -> Any:
        """Reject empty MongoDB URLs at field validation time.

        SecretStr permits empty strings; we explicitly reject them so
        the resulting :class:`pydantic.ValidationError` clearly names
        ``mongodb.url`` rather than allowing the empty string to flow
        through to the motor driver where it would surface as an
        opaque connection error much later in startup.
        """
        if v is None:
            return v
        if isinstance(v, SecretStr):
            raw = v.get_secret_value()
        elif isinstance(v, str):
            raw = v
        else:
            return v
        if not raw or not raw.strip():
            raise ValueError(
                "mongodb.url must be a non-empty string (sourced from "
                "MONGODB_URL env var per AAP R-25)"
            )
        return v

    @model_validator(mode="after")
    def _validate_pool_sizes(self) -> MongoDbSettings:
        """Reject ``max_pool_size < min_pool_size``.

        A pool whose maximum is below its minimum can never satisfy
        the minimum constraint — a configuration error that pymongo
        would surface as an obscure runtime error.
        """
        if self.max_pool_size < self.min_pool_size:
            raise ValueError(
                "mongodb.max_pool_size must be >= min_pool_size "
                f"(got max={self.max_pool_size!r}, "
                f"min={self.min_pool_size!r})"
            )
        return self



# -----------------------------------------------------------------------------
# 5.7 KafkaProducerSettings — producer policy (AAP R-14, R-15, R-30)
# -----------------------------------------------------------------------------
class KafkaProducerSettings(BaseModel):
    """Kafka producer configuration (AAP R-14, R-15, R-30).

    The Product Service is a pure event producer; this class carries the
    full producer policy. AAP R-30 mandates that ``acks`` MUST be
    ``"all"`` and ``enable_idempotence`` MUST be ``True`` for the
    ``product.*`` topics — these are enforced by the
    :meth:`_enforce_durability` model_validator on this class AND
    re-checked at the top level by
    :meth:`Settings._cross_field_invariants`. Defense in depth — neither
    layer alone is sufficient because env vars can bypass the YAML-only
    check at the nested-class level if mis-applied at the top level.

    Attributes:
        acks: Producer ack level (``"0"`` | ``"1"`` | ``"all"``). MUST
            be ``"all"`` per AAP R-30 (enforced by validator).
        enable_idempotence: When True, the producer guarantees exactly-
            once delivery within a session. MUST be True per AAP R-30.
        compression_type: Wire compression (``none`` | ``gzip`` |
            ``snappy`` | ``lz4`` | ``zstd``). ``lz4`` is the default —
            best balance of CPU and compression ratio for catalog
            payloads.
        linger_ms: Batching window — how long the producer waits to
            accumulate records before sending the batch.
        batch_size: Maximum batch buffer size in bytes (default 32 KiB).
        request_timeout_ms: Per-request timeout to the broker.
        delivery_timeout_ms: Total per-event budget across all retries
            (AAP R-15) — once exceeded, the producer stops retrying and
            surfaces the failure.
        max_send_attempts: AAP R-15 — maximum produce attempts before
            routing to ``<topic>.retry`` and ultimately ``<topic>.dlq``.
        backoff_initial_ms: AAP R-15 — initial backoff delay between
            produce retries.
        backoff_multiplier: AAP R-15 — exponential multiplier between
            successive produce retries.
        backoff_max_ms: AAP R-15 — upper bound on any single backoff
            window between produce retries.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    acks: str = Field(
        default="all",
        description=(
            "Producer ack level. AAP R-30 enforced: must be 'all' for "
            "product.* events."
        ),
    )
    enable_idempotence: bool = Field(
        default=True,
        description=(
            "AAP R-30 enforced: must be True for product.* events to "
            "guarantee exactly-once-within-session delivery."
        ),
    )
    compression_type: str = Field(
        default="lz4",
        description=(
            "Wire compression type. Allowed: none | gzip | snappy | "
            "lz4 | zstd. Default lz4 — best balance for catalog payloads."
        ),
    )
    linger_ms: int = Field(
        default=10,
        ge=0,
        description="Batching window — how long the producer waits to fill a batch.",
    )
    batch_size: int = Field(
        default=32_768,
        ge=1,
        description="Maximum batch buffer size in bytes (default 32 KiB).",
    )
    request_timeout_ms: int = Field(
        default=30_000,
        ge=1,
        description="Per-request timeout to the broker.",
    )
    delivery_timeout_ms: int = Field(
        default=120_000,
        ge=1,
        description=(
            "Total per-event budget across all retries (AAP R-15). "
            "Once exceeded, the producer stops retrying."
        ),
    )
    max_send_attempts: int = Field(
        default=5,
        ge=1,
        description="AAP R-15 max produce attempts before retry/DLQ routing.",
    )
    backoff_initial_ms: int = Field(
        default=100,
        ge=0,
        description="AAP R-15 initial backoff between produce retries.",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        gt=0,
        description="AAP R-15 exponential multiplier between produce retries.",
    )
    backoff_max_ms: int = Field(
        default=30_000,
        ge=0,
        description="AAP R-15 max single backoff window between produce retries.",
    )

    @field_validator("acks", mode="before")
    @classmethod
    def _normalize_acks(cls, v: Any) -> Any:
        """Coerce numeric acks values to strings and validate against allowed set.

        Operators sometimes set ``KAFKA_PRODUCER_ACKS=1`` (int) instead
        of ``"1"`` (string); coerce so the strict equality check in
        :meth:`_enforce_durability` against ``"all"`` works correctly.
        """
        if v is None:
            return v
        if isinstance(v, bool):
            # Avoid bool->str coercion silently passing.
            raise ValueError(f"kafka.producer.acks must not be a bool (got {v!r})")
        if isinstance(v, int):
            v = str(v)
        if not isinstance(v, str):
            return v
        stripped = v.strip()
        if stripped and stripped not in _ALLOWED_ACKS:
            raise ValueError(
                f"kafka.producer.acks must be one of {sorted(_ALLOWED_ACKS)} "
                f"(got {v!r})"
            )
        return stripped

    @field_validator("compression_type", mode="before")
    @classmethod
    def _normalize_compression(cls, v: Any) -> Any:
        """Lowercase and validate the producer compression type."""
        if v is None or not isinstance(v, str):
            return v
        normalized = v.strip().lower()
        if normalized and normalized not in _ALLOWED_COMPRESSION_TYPES:
            raise ValueError(
                f"kafka.producer.compression_type must be one of "
                f"{sorted(_ALLOWED_COMPRESSION_TYPES)} (got {v!r})"
            )
        return normalized

    @model_validator(mode="after")
    def _enforce_durability(self) -> KafkaProducerSettings:
        """Reject any configuration that would weaken event durability (AAP R-30).

        AAP R-30 keystone — the Product Service emits ``product.created``
        and ``product.updated`` which feed the Recommendation Engine,
        Inventory Service catalog sync, and the search index. Lost or
        duplicated events would cause silent catalog drift across the
        platform. The two non-negotiable settings are:

        * ``acks="all"`` — wait for all in-sync replicas to acknowledge
          before considering a produce successful.
        * ``enable_idempotence=True`` — exactly-once delivery within a
          producer session, eliminating duplicates introduced by
          transient retries.

        Both checks are duplicated in
        :meth:`Settings._cross_field_invariants` so a top-level explicit
        assignment cannot bypass the guard.
        """
        if self.acks != "all":
            raise ValueError(
                "AAP R-30 violation: kafka.producer.acks must be 'all' "
                f"for product.* events (got {self.acks!r})"
            )
        if not self.enable_idempotence:
            raise ValueError(
                "AAP R-30 violation: kafka.producer.enable_idempotence "
                "must be True for product.* events"
            )
        return self


# -----------------------------------------------------------------------------
# 5.8 KafkaSettings — top-level Kafka configuration (AAP R-14, R-17)
# -----------------------------------------------------------------------------
class KafkaSettings(BaseModel):
    """Kafka top-level configuration (AAP R-14, R-17).

    Aggregates broker connectivity (``bootstrap``, ``client_id``), the
    DLQ / retry topic-suffix conventions (AAP R-17), the nested
    :class:`KafkaProducerSettings`, and a Kafka-specific
    :class:`CircuitBreakerConfig` that wraps the produce path.

    The Product Service is a **pure producer** — there is intentionally
    NO ``consumer`` / ``group_id`` / ``enable_auto_commit`` /
    ``auto_offset_reset`` field on this class. Adding any consumer-
    related knob requires architectural review (AAP R-32 — producers
    must not know consumers).

    Attributes:
        bootstrap: Comma-separated broker list (``broker-1:9092,
            broker-2:9092,...``). **REQUIRED** — sourced from the
            ``KAFKA_BOOTSTRAP`` env var per AAP R-25.
        client_id: Producer ``client.id``; appears in broker logs and
            metrics for traffic attribution.
        retry_topic_suffix: AAP R-17 — suffix appended to the original
            topic name for the retry destination (default ``.retry``).
        dlq_topic_suffix: AAP R-17 — suffix appended to the original
            topic name for the dead-letter destination (default ``.dlq``).
        producer: Nested producer policy (AAP R-14, R-15, R-30 enforcer).
        circuit_breaker: Circuit-breaker thresholds for the produce path
            (AAP R-16) so a flapping broker cannot exhaust the request
            thread pool.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    bootstrap: str = Field(
        ...,
        description=(
            "Comma-separated broker list (env: KAFKA_BOOTSTRAP). "
            "REQUIRED — no default; supplied via env var per AAP R-25. "
            "Example: broker-1:9092,broker-2:9092,broker-3:9092."
        ),
    )
    client_id: str = Field(
        default="product-service",
        min_length=1,
        description=(
            "Producer client.id; appears in broker logs/metrics for "
            "traffic attribution."
        ),
    )
    retry_topic_suffix: str = Field(
        default=".retry",
        min_length=1,
        description=(
            "AAP R-17 — suffix appended to the original topic name "
            "for the retry destination."
        ),
    )
    dlq_topic_suffix: str = Field(
        default=".dlq",
        min_length=1,
        description=(
            "AAP R-17 — suffix appended to the original topic name "
            "for the dead-letter destination."
        ),
    )
    producer: KafkaProducerSettings = Field(
        default_factory=KafkaProducerSettings,
        description="Producer policy (AAP R-14, R-15, R-30 enforcer).",
    )
    circuit_breaker: CircuitBreakerConfig = Field(
        default_factory=lambda: CircuitBreakerConfig(
            failure_threshold=5,
            reset_timeout_ms=30_000,
        ),
        description=(
            "Circuit breaker for the produce path (AAP R-16). Default "
            "thresholds match HTTP client circuit breaker."
        ),
    )

    @field_validator("bootstrap", mode="before")
    @classmethod
    def _check_bootstrap_nonempty(cls, v: Any) -> Any:
        """Reject empty Kafka bootstrap strings at field validation time."""
        if v is None:
            return v
        if not isinstance(v, str):
            return v
        stripped = v.strip()
        if not stripped:
            raise ValueError(
                "kafka.bootstrap must be a non-empty comma-separated "
                "broker list (sourced from KAFKA_BOOTSTRAP env var)"
            )
        return stripped


# -----------------------------------------------------------------------------
# 5.9 SchemaRegistrySettings — Confluent Schema Registry (AAP R-14)
# -----------------------------------------------------------------------------
class SchemaRegistrySettings(BaseModel):
    """Confluent Schema Registry configuration (AAP R-14).

    Schema Registry hosts the canonical Avro / JSON Schema definitions
    for every Kafka event the platform produces; the Product Service
    fetches its ``product.created`` and ``product.updated`` schemas
    before encoding. AAP R-14 mandates that every produced event be
    validated against its registered schema.

    The optional ``auth`` field carries a ``user:password`` Basic-auth
    string for clusters that require authentication; it is wrapped in
    :class:`SecretStr` per AAP R-25 so the value never leaks into log
    output.

    Attributes:
        url: Schema Registry HTTP(S) URL. **REQUIRED** — sourced from
            the ``SCHEMA_REGISTRY_URL`` env var.
        auth: Optional Basic-auth credentials in the form
            ``user:password``. Sourced from ``SCHEMA_REGISTRY_AUTH``
            per AAP R-25.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    url: str = Field(
        ...,
        description=(
            "Confluent Schema Registry URL (env: SCHEMA_REGISTRY_URL). "
            "REQUIRED — no default. Production deployments use the "
            "cluster-internal HTTPS URL."
        ),
    )
    auth: SecretStr | None = Field(
        default=None,
        description=(
            "Optional Schema Registry Basic-auth credentials in the "
            "form 'user:password'. Sourced from SCHEMA_REGISTRY_AUTH "
            "per AAP R-25."
        ),
    )

    @field_validator("url", mode="before")
    @classmethod
    def _check_url(cls, v: Any) -> Any:
        """Validate that the Schema Registry URL is a syntactic HTTP(S) URL."""
        if v is None:
            return v
        if not isinstance(v, str):
            return v
        stripped = v.strip()
        if not stripped:
            raise ValueError(
                "schema_registry.url must be a non-empty HTTP(S) URL "
                "(sourced from SCHEMA_REGISTRY_URL env var)"
            )
        if not stripped.startswith(("http://", "https://")):
            raise ValueError(
                f"schema_registry.url must use http:// or https:// "
                f"(got {v!r})"
            )
        return stripped


# -----------------------------------------------------------------------------
# 5.10 ProducedTopicsSettings — topic name catalog (AAP R-30)
# -----------------------------------------------------------------------------
class ProducedTopicsSettings(BaseModel):
    """Topic names this service produces (AAP R-30: ``<domain>.<verb>``).

    The Product Service emits exactly two topics, both keyed by
    ``product_id`` for strict per-product ordering:

    * ``product.created`` — admin-initiated catalog creation.
    * ``product.updated`` — admin-initiated catalog edit, deprecation,
      or media change.

    Default topic names follow AAP R-30 verbatim; operators can override
    via env vars without recompiling, but doing so requires coordinated
    updates in every consumer (Recommendation Engine, Inventory
    Service, search index pipeline) that subscribes to these topics.

    Attributes:
        product_created: Topic for ``product.created`` events. Default
            matches AAP R-30 / AAP Section 0.4.2.
        product_updated: Topic for ``product.updated`` events. Default
            matches AAP R-30 / AAP Section 0.4.2.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    product_created: str = Field(
        default="product.created",
        min_length=1,
        description=(
            "AAP R-30 — topic for product.created events. Consumed by "
            "Recommendation Engine and Inventory Service."
        ),
    )
    product_updated: str = Field(
        default="product.updated",
        min_length=1,
        description=(
            "AAP R-30 — topic for product.updated events. Consumed by "
            "Recommendation Engine and Inventory Service."
        ),
    )


# -----------------------------------------------------------------------------
# 5.11 DlqTopicsSettings — DLQ topic catalog (AAP R-17)
# -----------------------------------------------------------------------------
class DlqTopicsSettings(BaseModel):
    """Per-topic DLQ destinations (AAP R-17).

    Each produced topic has a partner DLQ topic that catches messages
    that failed every produce retry. Operators inspect the DLQ via the
    runbook in ``docs/runbook/product-service.md`` and either replay
    or discard manually.

    Attributes:
        product_created_dlq: DLQ destination for ``product.created``
            messages that exhausted produce retries.
        product_updated_dlq: DLQ destination for ``product.updated``
            messages that exhausted produce retries.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    product_created_dlq: str = Field(
        default="product.created.dlq",
        min_length=1,
        description="AAP R-17 — DLQ for product.created produce failures.",
    )
    product_updated_dlq: str = Field(
        default="product.updated.dlq",
        min_length=1,
        description="AAP R-17 — DLQ for product.updated produce failures.",
    )


# -----------------------------------------------------------------------------
# 5.12 TopicsSettings — top-level container for produced + DLQ topic catalogs
# -----------------------------------------------------------------------------
class TopicsSettings(BaseModel):
    """All Kafka topic names (produced + DLQ).

    Top-level container that groups :class:`ProducedTopicsSettings` and
    :class:`DlqTopicsSettings`. Producers and DLQ routers reference
    these via the Pydantic model rather than literal strings to avoid
    drift between the schema definitions in
    ``infrastructure/kafka/schemas/`` and the runtime.

    NOTE: There is intentionally NO ``consumed`` block — the Product
    Service consumes ZERO Kafka topics. See class docstring for
    :class:`KafkaSettings`.

    Attributes:
        produced: Topics this service produces (AAP R-30).
        dlq: DLQ destinations (AAP R-17).
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    produced: ProducedTopicsSettings = Field(
        default_factory=ProducedTopicsSettings,
        description="Topics this service produces (AAP R-30).",
    )
    dlq: DlqTopicsSettings = Field(
        default_factory=DlqTopicsSettings,
        description="DLQ destinations for produce failures (AAP R-17).",
    )


# -----------------------------------------------------------------------------
# 5.13 AuthSettings — JWT validation (AAP R-21, R-22, R-23)
# -----------------------------------------------------------------------------
class AuthSettings(BaseModel):
    """JWT validation parameters (AAP R-21, R-22, R-23).

    The Product Service VALIDATES JWTs minted by the Auth Service. Per
    AAP R-21 the Auth Service is the SOLE issuer of JWT tokens; this
    service NEVER mints them. Public keys are fetched from the JWKS
    endpoint and cached with a bounded TTL per AAP R-22. Asymmetric
    algorithms only per AAP R-23 — symmetric algorithms (HS256/384/512)
    are FORBIDDEN here because they would imply this service shares a
    signing secret with the Auth Service.

    Attributes:
        auth_service_url: Base URL of the Auth Service (used for token
            introspection and metadata discovery). **REQUIRED**.
        public_key_url: JWKS endpoint URL from which public verification
            keys are fetched. **REQUIRED**. AAP R-22.
        jwks_cache_ttl_seconds: Bounded TTL on the cached JWKS keys
            (AAP R-22). Refreshes happen lazily after this window
            elapses; key rotation does not require service restart.
        issuer: Expected ``iss`` claim on validated JWTs. **REQUIRED**.
        audience: Expected ``aud`` claim on validated JWTs. Default
            ``product-service`` matches the conventional AAP audience
            scheme.
        algorithms: List of accepted JWT signing algorithms.
            **AAP R-23: ASYMMETRIC ONLY**. Default ``["RS256",
            "ES256"]``. Symmetric ``HS*`` algorithms are rejected by
            both the field validator AND the cross-field invariant on
            :class:`Settings`.
        required_scope_admin: OAuth 2.0 scope required to access admin
            write endpoints (POST/PUT/DELETE on /products and
            /categories).
        leeway_seconds: Clock-skew tolerance applied to the ``exp``
            and ``nbf`` claims during validation.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    auth_service_url: str = Field(
        ...,
        description=(
            "Base URL of the Auth Service (env: AUTH_SERVICE_URL). "
            "REQUIRED."
        ),
    )
    public_key_url: str = Field(
        ...,
        description=(
            "JWKS endpoint URL (env: JWT_PUBLIC_KEY_URL). REQUIRED. "
            "AAP R-22."
        ),
    )
    jwks_cache_ttl_seconds: int = Field(
        default=3600,
        ge=1,
        description=(
            "Bounded TTL on the JWKS key cache (AAP R-22). Default "
            "3600s (1h)."
        ),
    )
    issuer: str = Field(
        ...,
        description=(
            "Expected JWT `iss` claim (env: JWT_ISSUER). REQUIRED. "
            "AAP R-22."
        ),
    )
    audience: str = Field(
        default="product-service",
        min_length=1,
        description="Expected JWT `aud` claim. Default matches service.name.",
    )
    algorithms: list[str] = Field(
        default_factory=lambda: ["RS256", "ES256"],
        min_length=1,
        description=(
            "Allowed JWT signing algorithms. AAP R-23: asymmetric only "
            "(RS*/ES*/PS*). Symmetric HS* algorithms are rejected."
        ),
    )
    required_scope_admin: str = Field(
        default="products:admin",
        min_length=1,
        description=(
            "OAuth 2.0 scope required for admin write endpoints "
            "(POST/PUT/DELETE on /products and /categories)."
        ),
    )
    leeway_seconds: int = Field(
        default=30,
        ge=0,
        description="Clock-skew tolerance on JWT exp/nbf claims (seconds).",
    )

    @field_validator("auth_service_url", "public_key_url", "issuer", mode="before")
    @classmethod
    def _check_required_str(cls, v: Any) -> Any:
        """Reject empty strings for required URL/issuer fields."""
        if v is None:
            return v
        if not isinstance(v, str):
            return v
        stripped = v.strip()
        if not stripped:
            raise ValueError(
                "Required auth field must be a non-empty string "
                "(sourced from env var per AAP R-25)"
            )
        return stripped

    @field_validator("algorithms", mode="before")
    @classmethod
    def _normalize_algorithms(cls, v: Any) -> Any:
        """Parse CSV strings into a list and validate against allowed set.

        Operators set ``JWT_ALGORITHMS=RS256,ES256`` as a CSV string;
        :func:`_parse_csv` splits on commas and strips whitespace.
        Already-list values pass through unchanged. Each element is
        upper-cased so case differences in input don't slip through.
        Symmetric (HS*) algorithms are rejected per AAP R-23.
        """
        if v is None:
            return v
        # Accept CSV strings or already-parsed lists.
        parsed = _parse_csv(v) if isinstance(v, str) else (
            list(v) if isinstance(v, list) else v
        )
        if not isinstance(parsed, list):
            return parsed
        normalized: list[str] = []
        for alg in parsed:
            if not isinstance(alg, str):
                raise ValueError(
                    f"auth.algorithms entries must be strings (got {alg!r})"
                )
            upper_alg = alg.strip().upper()
            if not upper_alg:
                continue
            if upper_alg.startswith("HS"):
                raise ValueError(
                    f"AAP R-23 violation: auth.algorithms cannot include "
                    f"symmetric algorithm {alg!r}; only asymmetric "
                    f"algorithms (RS*/ES*/PS*) are permitted"
                )
            if upper_alg not in _ALLOWED_ASYMMETRIC_ALGS:
                raise ValueError(
                    f"auth.algorithms entry {alg!r} not recognized; "
                    f"allowed: {sorted(_ALLOWED_ASYMMETRIC_ALGS)}"
                )
            normalized.append(upper_alg)
        if not normalized:
            raise ValueError(
                "auth.algorithms must be a non-empty list of asymmetric "
                "JWT signing algorithms (AAP R-23)"
            )
        return normalized


# -----------------------------------------------------------------------------
# 5.14 HttpClientSettings — outbound HTTP (AAP R-13, R-15, R-16)
# -----------------------------------------------------------------------------
class HttpClientSettings(BaseModel):
    """Outbound HTTP client configuration (AAP R-13, R-15, R-16).

    The Product Service has minimal external HTTP surface area — primarily
    JWKS fetches from the Auth Service per AAP R-22, plus optional Schema
    Registry HTTP API calls when the bundled transport is bypassed.
    Despite the small surface, retries (AAP R-15) and circuit breaker
    (AAP R-16) are still required for the JWKS fetch path to keep token
    validation robust against intermittent Auth Service outages.

    Attributes:
        user_agent: Outbound ``User-Agent`` header (default
            ``product-service/1.0``).
        connect_timeout_ms: httpx connect timeout (milliseconds).
        read_timeout_ms: httpx read timeout (milliseconds).
        max_connections: Maximum total connections in the httpx pool.
        max_keepalive_connections: Maximum keep-alive connections in
            the httpx pool.
        retry: Nested retry policy (AAP R-15).
        circuit_breaker: Nested circuit-breaker thresholds (AAP R-16).
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    user_agent: str = Field(
        default="product-service/1.0",
        min_length=1,
        description="Outbound User-Agent header.",
    )
    connect_timeout_ms: int = Field(
        default=2000,
        ge=1,
        description="httpx connect timeout in milliseconds.",
    )
    read_timeout_ms: int = Field(
        default=5000,
        ge=1,
        description="httpx read timeout in milliseconds.",
    )
    max_connections: int = Field(
        default=100,
        ge=1,
        description="Maximum total connections in the httpx pool.",
    )
    max_keepalive_connections: int = Field(
        default=20,
        ge=0,
        description="Maximum keep-alive connections in the httpx pool.",
    )
    retry: RetryConfig = Field(
        default_factory=RetryConfig,
        description="Retry policy with exponential backoff + jitter (AAP R-15).",
    )
    circuit_breaker: CircuitBreakerConfig = Field(
        default_factory=CircuitBreakerConfig,
        description="Circuit-breaker thresholds (AAP R-16).",
    )


# -----------------------------------------------------------------------------
# 5.15 CatalogSettings — catalog domain knobs
# -----------------------------------------------------------------------------
class CatalogSettings(BaseModel):
    """Catalog domain configuration (browse / search / slug allocation).

    These knobs tune the read-side behaviour: how many products to return
    per page, how to sort by default, which slug prefixes are reserved
    for system endpoints (so user-generated content can't accidentally
    shadow ``/admin/...``), and the soft-delete grace period during which
    deprecated products remain queryable for admins before being purged.

    The :attr:`text_search_language` field is consumed by
    ``container.product_repository`` to drive the MongoDB text-index
    language at index creation time (English stemming for ``"english"``,
    none for ``"none"``, etc.).

    Attributes:
        default_page_size: Default page size for product / category list
            endpoints (1..1000).
        max_page_size: Hard cap on page size to prevent expensive scans
            (1..10000). Must be >= ``default_page_size`` (enforced by
            ``_validate_page_sizes``).
        default_sort: Default sort spec for list endpoints (e.g.,
            ``"created_at:desc"``).
        slug_reserved_prefixes: Slugs that may not be used for user-
            authored products / categories. Defaults to common system
            paths.
        soft_delete_grace_days: Days a deprecated product remains
            queryable for admins before purge.
        text_search_language: MongoDB text-index language. Consumed by
            ``container.product_repository`` at index-creation time.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    default_page_size: int = Field(
        default=20,
        ge=1,
        le=1000,
        description="Default page size for product/category list endpoints.",
    )
    max_page_size: int = Field(
        default=100,
        ge=1,
        le=10_000,
        description="Hard cap on page size to prevent expensive scans.",
    )
    default_sort: str = Field(
        default="created_at:desc",
        min_length=1,
        description="Default sort spec for list endpoints.",
    )
    slug_reserved_prefixes: list[str] = Field(
        default_factory=lambda: ["admin", "api", "health", "metrics"],
        description=(
            "Reserved slug prefixes — user-authored slugs may not start "
            "with any of these to avoid shadowing system endpoints."
        ),
    )
    soft_delete_grace_days: int = Field(
        default=30,
        ge=0,
        description=(
            "Days a deprecated product remains queryable for admins "
            "before purge."
        ),
    )
    text_search_language: str = Field(
        default="english",
        min_length=1,
        description=(
            "MongoDB text-index language. Consumed by container."
            "product_repository at index-creation time."
        ),
    )

    @field_validator("slug_reserved_prefixes", mode="before")
    @classmethod
    def _normalize_slug_reserved(cls, v: Any) -> Any:
        """Parse CSV strings into a list and lowercase each entry.

        Operators may set
        ``CATALOG_SLUG_RESERVED_PREFIXES=admin,api,health,metrics`` as a
        CSV string; :func:`_parse_csv` splits on commas. Each entry is
        lowercased so case-differences in input are normalized away.
        """
        if v is None:
            return v
        parsed = _parse_csv(v) if isinstance(v, str) else (
            list(v) if isinstance(v, list) else v
        )
        if not isinstance(parsed, list):
            return parsed
        return [
            item.strip().lower()
            for item in parsed
            if isinstance(item, str) and item.strip()
        ]

    @model_validator(mode="after")
    def _validate_page_sizes(self) -> CatalogSettings:
        """Reject ``default_page_size > max_page_size``.

        A default that exceeds the hard cap would mean the cap is never
        meaningful for clients that don't supply an explicit page size —
        a configuration error.
        """
        if self.default_page_size > self.max_page_size:
            raise ValueError(
                "catalog.default_page_size must be <= max_page_size "
                f"(got default={self.default_page_size!r}, "
                f"max={self.max_page_size!r})"
            )
        return self


# -----------------------------------------------------------------------------
# 5.16 IdempotencySettings — admin-write replay protection
# -----------------------------------------------------------------------------
class IdempotencySettings(BaseModel):
    """Idempotency-key configuration for admin writes.

    Admin writes (POST/PUT/DELETE on /products and /categories) accept
    an ``Idempotency-Key`` header so a client can retry a failed call
    safely without producing duplicate ``product.*`` Kafka events. The
    key plus a hash of the request body is persisted in MongoDB
    briefly; matching keys return the original response, mismatched
    hashes return ``409 Conflict``.

    Attributes:
        key_header: HTTP header name for the inbound idempotency key.
            Default ``Idempotency-Key`` follows the IETF draft
            convention.
        key_ttl_hours: How long to remember a key. Admin writes are
            infrequent so 24h is a comfortable default.
        request_hash_algo: Algorithm used to fingerprint request bodies
            for replay-mismatch detection. Allowed: ``sha256`` |
            ``sha512``.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    key_header: str = Field(
        default="Idempotency-Key",
        min_length=1,
        description=(
            "HTTP header name for the inbound idempotency key. Default "
            "follows the IETF draft convention."
        ),
    )
    key_ttl_hours: int = Field(
        default=24,
        ge=1,
        description="How long to remember an idempotency key.",
    )
    request_hash_algo: str = Field(
        default="sha256",
        description=(
            "Hash algorithm used to fingerprint request bodies. "
            "Allowed: sha256 | sha512."
        ),
    )

    @field_validator("request_hash_algo", mode="before")
    @classmethod
    def _normalize_hash_algo(cls, v: Any) -> Any:
        """Lowercase and validate the request-fingerprint hash algorithm."""
        if v is None or not isinstance(v, str):
            return v
        normalized = v.strip().lower()
        allowed = {"sha256", "sha512"}
        if normalized and normalized not in allowed:
            raise ValueError(
                f"idempotency.request_hash_algo must be one of "
                f"{sorted(allowed)} (got {v!r})"
            )
        return normalized


# -----------------------------------------------------------------------------
# 5.17 ObservabilitySettings — tracing and metrics (AAP R-13, R-26, R-27)
# -----------------------------------------------------------------------------
class ObservabilitySettings(BaseModel):
    """Observability settings (AAP R-13, R-26, R-27).

    Drives correlation-ID header propagation (AAP R-13), the
    OpenTelemetry tracer setup, and the Prometheus exposition endpoint
    (AAP R-27).

    Attributes:
        correlation_id_header: HTTP header name carrying the correlation
            ID (AAP R-13). Mirrors :attr:`LoggingSettings.correlation_id_header`
            for back-compat with operators who configure either field.
        otel_exporter_otlp_endpoint: Optional OpenTelemetry collector
            endpoint. When set, the OTel SDK exports traces (and
            optionally metrics) to this endpoint via OTLP/gRPC.
        otel_service_name: OpenTelemetry ``service.name`` resource
            attribute. Default matches :attr:`ServiceSettings.name`.
        otel_traces_sampler: OpenTelemetry sampler strategy. Default
            ``parentbased_always_on`` honours the parent span's sampling
            decision (or always samples for root spans), which is the
            sane default for HTTP-driven services.
        metrics_port: Prometheus exposition port if separate from the
            main app port (default 9090).
        metrics_path: Prometheus exposition path (AAP R-27, default
            ``/metrics``).
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    correlation_id_header: str = Field(
        default="X-Correlation-ID",
        min_length=1,
        description="AAP R-13 — HTTP header name for correlation-ID propagation.",
    )
    otel_exporter_otlp_endpoint: str | None = Field(
        default=None,
        description=(
            "Optional OpenTelemetry collector OTLP endpoint. When set, "
            "traces are exported via OTLP/gRPC."
        ),
    )
    otel_service_name: str = Field(
        default="product-service",
        min_length=1,
        description="OTel service.name resource attribute.",
    )
    otel_traces_sampler: str = Field(
        default="parentbased_always_on",
        min_length=1,
        description=(
            "OTel sampler strategy. Default parentbased_always_on "
            "honours the parent span's decision."
        ),
    )
    metrics_port: int = Field(
        default=9090,
        ge=1,
        le=65535,
        description="Prometheus exposition port (if separate from app).",
    )
    metrics_path: str = Field(
        default="/metrics",
        min_length=1,
        description="AAP R-27 — Prometheus exposition path.",
    )


# -----------------------------------------------------------------------------
# 5.18 FeaturesSettings — runtime feature flags
# -----------------------------------------------------------------------------
class FeaturesSettings(BaseModel):
    """Runtime feature flags for the catalog domain.

    Allows operators to toggle expensive features (faceted search) or
    in-process caches (category tree LRU+TTL) without redeploying. The
    cache fields are consumed by ``container.category_tree_cache``
    (a ``cachetools.TTLCache``) at startup time.

    Attributes:
        text_search_enabled: Enable MongoDB text-index search across
            product titles and descriptions.
        faceted_search_enabled: Enable expensive faceted aggregation
            queries. Default False — turn on selectively per
            environment based on observed query latency.
        category_tree_cache_enabled: Enable the in-process LRU+TTL
            cache for the category tree. Default True — the category
            tree is read-mostly and the cache eliminates 90%+ of the
            tree-fetch latency from the hot path.
        category_tree_cache_ttl_seconds: TTL on the category tree cache.
            Bounded so admin edits propagate within this window.
        category_tree_cache_max_size: LRU max size on the category tree
            cache.
    """

    model_config = ConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=False,
    )

    text_search_enabled: bool = Field(
        default=True,
        description="Enable MongoDB text-index search.",
    )
    faceted_search_enabled: bool = Field(
        default=False,
        description=(
            "Enable expensive faceted aggregations. Default off; "
            "turn on selectively per environment."
        ),
    )
    category_tree_cache_enabled: bool = Field(
        default=True,
        description="Enable the in-process LRU+TTL cache for the category tree.",
    )
    category_tree_cache_ttl_seconds: int = Field(
        default=300,
        ge=1,
        description="TTL on the category-tree cache (seconds).",
    )
    category_tree_cache_max_size: int = Field(
        default=1024,
        ge=1,
        description="LRU max size on the category-tree cache.",
    )



# =============================================================================
# Top-Level Settings (BaseSettings)
# =============================================================================
# Orchestrates the layered loading hierarchy described at the module top:
# init_kwargs > env vars > .env > YAML > file_secret. Composes 12 nested
# sections, runs the env-var injection in a model_validator(mode="before"),
# and re-checks the AAP R-30 / R-23 keystones plus the production TLS guard
# in a model_validator(mode="after").
# =============================================================================


class Settings(BaseSettings):
    """Top-level settings for the Product Service.

    Loaded once per process by :func:`get_settings`. Field values come
    from (highest precedence first):

    1. **Init kwargs** to ``Settings(...)`` — used by tests.
    2. **Environment variables** — flat ``UPPER_SNAKE_CASE`` names
       documented in ``services/product-service/.env.example``.
       Mapped onto the nested model structure by
       :meth:`_inject_env_vars`.
    3. **.env file** — optional ``services/product-service/.env`` for
       local development; never present in production.
    4. **YAML defaults** —
       ``services/product-service/config/<environment>.yaml`` (if present)
       overlaid on ``services/product-service/config/default.yaml``.
       Loaded by :class:`_YamlSettingsSource`.
    5. **File secret settings** — Pydantic file-secret loader (lowest).

    Required fields (no defaults — must come from env or YAML):
        * ``service.environment``  — sourced from ``ENVIRONMENT``
        * ``mongodb.url``          — sourced from ``MONGODB_URL``
        * ``kafka.bootstrap``      — sourced from ``KAFKA_BOOTSTRAP``
        * ``schema_registry.url``  — sourced from ``SCHEMA_REGISTRY_URL``
        * ``auth.auth_service_url`` — sourced from ``AUTH_SERVICE_URL``
        * ``auth.public_key_url``  — sourced from ``JWT_PUBLIC_KEY_URL``
        * ``auth.issuer``          — sourced from ``JWT_ISSUER``

    Missing any required field raises ``pydantic.ValidationError``
    BEFORE the HTTP listener binds (AAP R-19 fail-fast).

    Construction sequence:

    a. :class:`_YamlSettingsSource` loads ``default.yaml`` and the
       per-environment overlay.
    b. :meth:`_inject_env_vars` (``model_validator(mode="before")``)
       maps flat env vars onto the nested settings dict.
    c. Pydantic field validation — types, ranges, custom field validators.
    d. Per-class ``model_validator(mode="after")`` — most importantly
       :meth:`KafkaProducerSettings._enforce_durability` (AAP R-30) and
       :class:`AuthSettings` algorithm-asymmetry checks (AAP R-23).
    e. :meth:`_cross_field_invariants` (``model_validator(mode="after")``
       on this class) — re-checks AAP R-30 / R-23 at the top level
       AND enforces the production TLS guard (AAP R-24).

    Any failure in (a)-(e) raises :class:`pydantic.ValidationError` and
    aborts startup (AAP R-19 fail-fast).
    """

    # Note: `protected_namespaces=()` disables Pydantic v2's `model_*`
    # namespace protection — not strictly needed here (no field is named
    # `model_*`) but kept for monorepo consistency with sibling services.
    model_config = SettingsConfigDict(
        extra="ignore",
        str_strip_whitespace=True,
        env_nested_delimiter="__",
        env_file=(
            str(SERVICE_ROOT / ".env")
            if (SERVICE_ROOT / ".env").is_file()
            else None
        ),
        env_file_encoding="utf-8",
        case_sensitive=False,
        protected_namespaces=(),
    )

    # --------------------------------------------------------------------
    # The 12 top-level sections, mirroring default.yaml's structure
    # --------------------------------------------------------------------
    # NOTE on ``service``: the nested :class:`ServiceSettings` requires
    # ``environment`` (no default at the field level), so this top-level
    # section is REQUIRED — it must come from YAML or env. With no
    # ``default_factory`` here, omitting both YAML and env causes
    # ``ServiceSettings`` construction to fail with a "missing field"
    # ValidationError, which is the AAP R-19 fail-fast we want.
    service: ServiceSettings = Field(
        ...,
        description="Service identity and runtime parameters.",
    )
    logging: LoggingSettings = Field(
        default_factory=LoggingSettings,
        description="Structured JSON logging configuration (AAP R-26).",
    )
    mongodb: MongoDbSettings = Field(
        ...,
        description=(
            "MongoDB connection settings for product_db (AAP R-7, "
            "Section 0.4.4). url is REQUIRED via env var per AAP R-25."
        ),
    )
    kafka: KafkaSettings = Field(
        ...,
        description=(
            "Kafka top-level configuration (AAP R-14, R-17). bootstrap "
            "is REQUIRED via env var per AAP R-25."
        ),
    )
    schema_registry: SchemaRegistrySettings = Field(
        ...,
        description=(
            "Confluent Schema Registry configuration (AAP R-14). url "
            "is REQUIRED via env var."
        ),
    )
    topics: TopicsSettings = Field(
        default_factory=TopicsSettings,
        description="Kafka topic catalog (produced + DLQ; AAP R-17, R-30).",
    )
    auth: AuthSettings = Field(
        ...,
        description=(
            "JWT validation parameters (AAP R-21, R-22, R-23). "
            "auth_service_url, public_key_url, and issuer are REQUIRED."
        ),
    )
    http_client: HttpClientSettings = Field(
        default_factory=HttpClientSettings,
        description="Outbound HTTP client configuration (AAP R-13, R-15, R-16).",
    )
    catalog: CatalogSettings = Field(
        default_factory=CatalogSettings,
        description="Catalog domain knobs (page size, slugs, soft-delete).",
    )
    idempotency: IdempotencySettings = Field(
        default_factory=IdempotencySettings,
        description="Idempotency-key configuration for admin writes.",
    )
    observability: ObservabilitySettings = Field(
        default_factory=ObservabilitySettings,
        description="Observability settings (AAP R-13, R-26, R-27).",
    )
    features: FeaturesSettings = Field(
        default_factory=FeaturesSettings,
        description="Runtime feature flags for the catalog domain.",
    )

    # --------------------------------------------------------------------
    # Validators
    # --------------------------------------------------------------------
    @model_validator(mode="before")
    @classmethod
    def _inject_env_vars(cls, values: Any) -> Any:
        """Map flat environment variables onto the nested model structure.

        Pydantic-settings's ``env_nested_delimiter="__"`` natively handles
        ``KAFKA__BOOTSTRAP``-style variables, but the canonical
        ``.env.example`` for the Product Service uses FLAT names (e.g.,
        ``KAFKA_BOOTSTRAP``, ``MONGODB_URL``, ``JWT_PUBLIC_KEY_URL``).
        This validator bridges the two conventions.

        Every variable documented in ``.env.example`` has a corresponding
        explicit lookup here. The verbosity is deliberate: it keeps the
        env-var -> field mapping discoverable and unambiguous, makes
        unsupported env vars visible (they will simply be ignored
        instead of silently mapping to an unintended field), and provides
        a single place to audit the env-var contract per AAP R-25.

        Args:
            values: Raw kwargs dict from upstream sources (init,
                env_settings, dotenv, YAML). May be a non-dict if
                pydantic is performing a model_copy or validate_python
                with an existing model instance.

        Returns:
            The (possibly modified) values dict with env-var values
            written into nested paths.
        """
        if not isinstance(values, dict):
            return values
        # Make a copy we can mutate without affecting upstream sources.
        merged: dict[str, Any] = dict(values)
        env = os.environ

        # Mapping of (env_var_name, dotted_path, coercer) tuples. The
        # coercer is a callable that takes a string and returns the
        # parsed value; ``str`` is used as a passthrough sentinel
        # meaning "use the raw string as-is" (avoids the str(str)
        # round-trip).
        mapping: list[tuple[str, tuple[str, ...], Any]] = [
            # ----------------------------------------------------------
            # Group A: Service Runtime
            # ----------------------------------------------------------
            ("SERVICE_NAME", ("service", "name"), str),
            ("SERVICE_PORT", ("service", "port"), _coerce_int),
            ("ENVIRONMENT", ("service", "environment"), str),
            ("LOG_LEVEL", ("service", "log_level"), str),
            ("LOG_LEVEL", ("logging", "level"), str),
            ("LOG_FORMAT", ("service", "log_format"), str),
            ("LOG_FORMAT", ("logging", "format"), str),
            ("HEALTH_LIVENESS_PATH", ("service", "health_liveness_path"), str),
            ("HEALTH_READINESS_PATH", ("service", "health_readiness_path"), str),
            # ----------------------------------------------------------
            # Group B: MongoDB (AAP R-7, R-25, Section 0.4.4)
            # ----------------------------------------------------------
            ("MONGODB_URL", ("mongodb", "url"), str),
            ("MONGODB_DATABASE", ("mongodb", "database"), str),
            ("MONGODB_MIN_POOL_SIZE", ("mongodb", "min_pool_size"), _coerce_int),
            ("MONGODB_MAX_POOL_SIZE", ("mongodb", "max_pool_size"), _coerce_int),
            (
                "MONGODB_CONNECT_TIMEOUT_MS",
                ("mongodb", "connect_timeout_ms"),
                _coerce_int,
            ),
            (
                "MONGODB_SOCKET_TIMEOUT_MS",
                ("mongodb", "socket_timeout_ms"),
                _coerce_int,
            ),
            (
                "MONGODB_SERVER_SELECTION_TIMEOUT_MS",
                ("mongodb", "server_selection_timeout_ms"),
                _coerce_int,
            ),
            ("MONGODB_RETRY_WRITES", ("mongodb", "retry_writes"), _coerce_bool),
            ("MONGODB_RETRY_READS", ("mongodb", "retry_reads"), _coerce_bool),
            ("MONGODB_READ_PREFERENCE", ("mongodb", "read_preference"), str),
            ("MONGODB_WRITE_CONCERN", ("mongodb", "write_concern"), str),
            ("MONGODB_TLS_ENABLED", ("mongodb", "tls_enabled"), _coerce_bool),
            ("MONGODB_TLS_CA_FILE", ("mongodb", "tls_ca_file"), str),
            ("MONGODB_APP_NAME", ("mongodb", "app_name"), str),
            # ----------------------------------------------------------
            # Group C: Kafka (AAP R-14, R-17, R-30) — producer-only
            # ----------------------------------------------------------
            ("KAFKA_BOOTSTRAP", ("kafka", "bootstrap"), str),
            ("KAFKA_CLIENT_ID", ("kafka", "client_id"), str),
            ("KAFKA_RETRY_TOPIC_SUFFIX", ("kafka", "retry_topic_suffix"), str),
            ("KAFKA_DLQ_TOPIC_SUFFIX", ("kafka", "dlq_topic_suffix"), str),
            ("KAFKA_PRODUCER_ACKS", ("kafka", "producer", "acks"), str),
            (
                "KAFKA_PRODUCER_ENABLE_IDEMPOTENCE",
                ("kafka", "producer", "enable_idempotence"),
                _coerce_bool,
            ),
            (
                "KAFKA_PRODUCER_COMPRESSION_TYPE",
                ("kafka", "producer", "compression_type"),
                str,
            ),
            (
                "KAFKA_PRODUCER_LINGER_MS",
                ("kafka", "producer", "linger_ms"),
                _coerce_int,
            ),
            (
                "KAFKA_PRODUCER_BATCH_SIZE",
                ("kafka", "producer", "batch_size"),
                _coerce_int,
            ),
            (
                "KAFKA_PRODUCER_REQUEST_TIMEOUT_MS",
                ("kafka", "producer", "request_timeout_ms"),
                _coerce_int,
            ),
            (
                "KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS",
                ("kafka", "producer", "delivery_timeout_ms"),
                _coerce_int,
            ),
            # ----------------------------------------------------------
            # Group D: Schema Registry (AAP R-14)
            # ----------------------------------------------------------
            ("SCHEMA_REGISTRY_URL", ("schema_registry", "url"), str),
            ("SCHEMA_REGISTRY_AUTH", ("schema_registry", "auth"), str),
            # ----------------------------------------------------------
            # Group E: Catalog domain
            # ----------------------------------------------------------
            (
                "CATALOG_DEFAULT_PAGE_SIZE",
                ("catalog", "default_page_size"),
                _coerce_int,
            ),
            (
                "CATALOG_MAX_PAGE_SIZE",
                ("catalog", "max_page_size"),
                _coerce_int,
            ),
            ("CATALOG_DEFAULT_SORT", ("catalog", "default_sort"), str),
            (
                "CATALOG_SLUG_RESERVED_PREFIXES",
                ("catalog", "slug_reserved_prefixes"),
                _parse_csv,
            ),
            (
                "CATALOG_SOFT_DELETE_GRACE_DAYS",
                ("catalog", "soft_delete_grace_days"),
                _coerce_int,
            ),
            (
                "CATALOG_TEXT_SEARCH_LANGUAGE",
                ("catalog", "text_search_language"),
                str,
            ),
            # ----------------------------------------------------------
            # Group F: Idempotency
            # ----------------------------------------------------------
            ("IDEMPOTENCY_KEY_HEADER", ("idempotency", "key_header"), str),
            (
                "IDEMPOTENCY_KEY_TTL_HOURS",
                ("idempotency", "key_ttl_hours"),
                _coerce_int,
            ),
            (
                "IDEMPOTENCY_REQUEST_HASH_ALGO",
                ("idempotency", "request_hash_algo"),
                str,
            ),
            # ----------------------------------------------------------
            # Group G: HTTP client (AAP R-13, R-15, R-16)
            # ----------------------------------------------------------
            (
                "HTTP_DEFAULT_CONNECT_TIMEOUT_MS",
                ("http_client", "connect_timeout_ms"),
                _coerce_int,
            ),
            (
                "HTTP_DEFAULT_READ_TIMEOUT_MS",
                ("http_client", "read_timeout_ms"),
                _coerce_int,
            ),
            ("HTTP_USER_AGENT", ("http_client", "user_agent"), str),
            (
                "HTTP_RETRY_MAX_ATTEMPTS",
                ("http_client", "retry", "max_attempts"),
                _coerce_int,
            ),
            (
                "HTTP_RETRY_INITIAL_DELAY_MS",
                ("http_client", "retry", "initial_delay_ms"),
                _coerce_int,
            ),
            (
                "HTTP_RETRY_MULTIPLIER",
                ("http_client", "retry", "multiplier"),
                _coerce_float,
            ),
            (
                "HTTP_RETRY_MAX_DELAY_MS",
                ("http_client", "retry", "max_delay_ms"),
                _coerce_int,
            ),
            (
                "HTTP_RETRY_JITTER_PCT",
                ("http_client", "retry", "jitter_pct"),
                _coerce_float,
            ),
            (
                "HTTP_CB_FAILURE_THRESHOLD",
                ("http_client", "circuit_breaker", "failure_threshold"),
                _coerce_int,
            ),
            (
                "HTTP_CB_OPEN_DURATION_MS",
                ("http_client", "circuit_breaker", "reset_timeout_ms"),
                _coerce_int,
            ),
            # ----------------------------------------------------------
            # Group H: Auth / JWT (AAP R-21, R-22, R-23)
            # ----------------------------------------------------------
            ("AUTH_SERVICE_URL", ("auth", "auth_service_url"), str),
            ("JWT_PUBLIC_KEY_URL", ("auth", "public_key_url"), str),
            (
                "JWT_JWKS_CACHE_TTL_SECONDS",
                ("auth", "jwks_cache_ttl_seconds"),
                _coerce_int,
            ),
            ("JWT_ISSUER", ("auth", "issuer"), str),
            ("JWT_AUDIENCE", ("auth", "audience"), str),
            ("JWT_ALGORITHMS", ("auth", "algorithms"), _parse_csv),
            (
                "JWT_REQUIRED_SCOPE_ADMIN",
                ("auth", "required_scope_admin"),
                str,
            ),
            ("JWT_LEEWAY_SECONDS", ("auth", "leeway_seconds"), _coerce_int),
            # ----------------------------------------------------------
            # Group I: Observability (AAP R-13, R-26, R-27)
            # ----------------------------------------------------------
            (
                "CORRELATION_ID_HEADER",
                ("observability", "correlation_id_header"),
                str,
            ),
            (
                "CORRELATION_ID_HEADER",
                ("logging", "correlation_id_header"),
                str,
            ),
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                ("observability", "otel_exporter_otlp_endpoint"),
                str,
            ),
            (
                "OTEL_SERVICE_NAME",
                ("observability", "otel_service_name"),
                str,
            ),
            (
                "OTEL_TRACES_SAMPLER",
                ("observability", "otel_traces_sampler"),
                str,
            ),
            ("METRICS_PORT", ("observability", "metrics_port"), _coerce_int),
            ("METRICS_PATH", ("observability", "metrics_path"), str),
            # ----------------------------------------------------------
            # Group J: Feature flags
            # ----------------------------------------------------------
            (
                "FEATURE_TEXT_SEARCH_ENABLED",
                ("features", "text_search_enabled"),
                _coerce_bool,
            ),
            (
                "FEATURE_FACETED_SEARCH_ENABLED",
                ("features", "faceted_search_enabled"),
                _coerce_bool,
            ),
            (
                "FEATURE_CATEGORY_TREE_CACHE_ENABLED",
                ("features", "category_tree_cache_enabled"),
                _coerce_bool,
            ),
            (
                "CATEGORY_TREE_CACHE_TTL_SECONDS",
                ("features", "category_tree_cache_ttl_seconds"),
                _coerce_int,
            ),
            (
                "CATEGORY_TREE_CACHE_MAX_SIZE",
                ("features", "category_tree_cache_max_size"),
                _coerce_int,
            ),
        ]

        for env_name, path, coercer in mapping:
            raw = env.get(env_name)
            # Skip when the env var is unset OR present-but-empty. An
            # empty env var should NOT override a non-empty YAML default.
            if raw is None or (isinstance(raw, str) and raw.strip() == ""):
                continue
            # Resolve the value via the coercer. A coercer of ``str`` is
            # the passthrough sentinel; everything else is a callable.
            try:
                if coercer is str:
                    value: Any = raw
                else:
                    value = coercer(raw)
            except (TypeError, ValueError):
                # Defer to the field validator for clearer error messages.
                # Writing the raw string lets pydantic's own validator
                # produce a precise error message naming the field.
                value = raw
            _set_nested(merged, path, value)

        return merged

    @model_validator(mode="after")
    def _cross_field_invariants(self) -> Settings:
        """Cross-field invariants that span multiple nested classes.

        Re-checks the AAP keystone rules at the top level after the
        env-var overlay has run. Belt-and-suspenders defense: every
        keystone is enforced both at the nested-class level and here so
        no path can bypass it.

        Ordering and rationale:

        1. **AAP R-30 keystone re-check** — Kafka producer durability
           (``acks="all"``, ``enable_idempotence=True``). Already
           checked by :meth:`KafkaProducerSettings._enforce_durability`;
           re-checked here to catch top-level explicit assignment that
           bypasses the per-class validator (e.g., a test that builds
           ``Settings(kafka=KafkaSettings(producer=KafkaProducerSettings(
           acks="0", enable_idempotence=False))`` and then mutates the
           producer object).
        2. **AAP R-23 algorithm asymmetry** — Defense in depth against
           future code paths that bypass the field validator on
           :attr:`AuthSettings.algorithms`.
        3. **MongoDB pool sanity** — ``min_pool_size <= max_pool_size``.
           Already checked by :meth:`MongoDbSettings._validate_pool_sizes`;
           re-checked here for completeness.
        4. **Catalog page-size sanity** — ``default_page_size <=
           max_page_size``. Already checked by
           :meth:`CatalogSettings._validate_page_sizes`; re-checked here
           for completeness.
        5. **Production TLS guard (AAP R-24)** — ``mongodb.tls_enabled``
           MUST be True when ``service.environment`` is ``prod`` /
           ``production``. This is a service-specific posture for AAP
           R-24 ("All inter-service traffic must be TLS-encrypted").
        """
        # 1. AAP R-30 keystone re-check (Kafka producer durability).
        if self.kafka.producer.acks != "all":
            raise ValueError(
                "AAP R-30 violation: kafka.producer.acks must be 'all' "
                f"for product.* events (got {self.kafka.producer.acks!r})"
            )
        if not self.kafka.producer.enable_idempotence:
            raise ValueError(
                "AAP R-30 violation: kafka.producer.enable_idempotence "
                "must be True for product.* events"
            )

        # 2. AAP R-23 — only asymmetric algorithms in JWT validation set.
        forbidden = {
            alg
            for alg in self.auth.algorithms
            if isinstance(alg, str) and alg.upper().startswith("HS")
        }
        if forbidden:
            raise ValueError(
                f"AAP R-23 violation: auth.algorithms contains symmetric "
                f"(HS*) algorithm(s) {sorted(forbidden)}; only "
                f"asymmetric algorithms (RS*/ES*/PS*) are allowed"
            )

        # 3. MongoDB pool sanity (defense in depth).
        if self.mongodb.min_pool_size > self.mongodb.max_pool_size:
            raise ValueError(
                "mongodb.min_pool_size must be <= mongodb.max_pool_size "
                f"(got min={self.mongodb.min_pool_size!r}, "
                f"max={self.mongodb.max_pool_size!r})"
            )

        # 4. Catalog page-size sanity (defense in depth).
        if self.catalog.default_page_size > self.catalog.max_page_size:
            raise ValueError(
                "catalog.default_page_size must be <= max_page_size "
                f"(got default={self.catalog.default_page_size!r}, "
                f"max={self.catalog.max_page_size!r})"
            )

        # 5. Production TLS guard (AAP R-24).
        env_lower = self.service.environment.strip().lower()
        if env_lower in {"prod", "production"} and not self.mongodb.tls_enabled:
            raise ValueError(
                "AAP R-24 violation: mongodb.tls_enabled must be True "
                f"in production environment (got environment="
                f"{self.service.environment!r}, tls_enabled="
                f"{self.mongodb.tls_enabled!r})"
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
        """Configure the source chain.

        Source precedence (highest first):

        1. ``init_settings``  — kwargs to ``Settings(...)``.
        2. ``env_settings``   — environment variables.
        3. ``dotenv_settings`` — optional ``.env`` file.
        4. :class:`_YamlSettingsSource` — YAML defaults (default.yaml +
           per-environment overlay).
        5. ``file_secret_settings`` — Pydantic file-secret loader (lowest).

        YAML provides operational defaults; env vars override on a
        per-key basis. The Pydantic file-secret source is included for
        completeness even though the Product Service does not currently
        use it — operators can mount Kubernetes-style file secrets at
        ``/run/secrets/<field-name>`` and they will be picked up
        automatically.

        Args:
            settings_cls: The :class:`Settings` class being constructed.
            init_settings: Source for keyword arguments passed to
                ``Settings(...)``.
            env_settings: Source for environment variables.
            dotenv_settings: Source for ``.env`` file contents.
            file_secret_settings: Source for Kubernetes-style file
                secrets at ``/run/secrets/<field>``.

        Returns:
            Tuple of sources in highest-precedence-first order.
        """
        yaml_source = _YamlSettingsSource(settings_cls)
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            yaml_source,
            file_secret_settings,
        )


# =============================================================================
# Cached Factory
# =============================================================================
# :func:`get_settings` is the canonical entry point for the rest of the
# application. It is wrapped in :func:`functools.lru_cache(maxsize=1)`
# so exactly one :class:`Settings` instance exists per process.
# =============================================================================


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Construct (or return cached) :class:`Settings` instance.

    Cached with :func:`functools.lru_cache(maxsize=1)` so that repeated
    calls within the same process return the same object (``id()``
    identical). ``src.container.build_container`` is the canonical
    caller; downstream modules SHOULD inject ``Settings`` via the
    container rather than calling :func:`get_settings` directly to keep
    the dependency graph explicit.

    Per AAP R-19, the first call to this function during application
    startup is the **fail-fast gate** — any missing required env var
    or invalid value raises here and aborts the process. Subsequent
    calls return the cached instance and never re-validate.

    Tests that need to re-evaluate after monkeypatching ``os.environ``
    should call ``get_settings.cache_clear()`` between cases to force
    re-construction with fresh environment-variable overlays.

    Returns:
        The cached :class:`Settings` instance with all 12 top-level
        sections populated.

    Raises:
        pydantic.ValidationError: If any required field is missing or
            any value is invalid. AAP R-19 fail-fast — the exception
            propagates up through ``src.main.lifespan`` so the process
            exits non-zero BEFORE the HTTP listener binds.
    """
    # mypy/pyright cannot trace the env-var injection performed by
    # :meth:`Settings._inject_env_vars` (model_validator(mode="before")),
    # so they may flag the no-arg ``Settings()`` call as missing required
    # arguments. The behavior is correct at runtime — pydantic-settings
    # populates the required fields from env vars and YAML before the
    # __init__ completes.
    return Settings()  # type: ignore[call-arg]


# =============================================================================
# Public surface (`__all__`)
# =============================================================================
# Symbols listed here form the formal public API consumed by the rest of
# the service (``container.py``, ``main.py``, every controller /
# middleware / repository that reads config). Private helpers
# (``_YamlSettingsSource``, ``_read_yaml``, the ``_coerce_*`` helpers,
# ``_inject_env_vars``, etc.) remain accessible for unit tests but are
# NOT considered part of the stable interface — they may change in
# minor revisions.
# =============================================================================

__all__: list[str] = [
    "AuthSettings",
    "CatalogSettings",
    "CircuitBreakerConfig",
    "CollectionsSettings",
    "DlqTopicsSettings",
    "FeaturesSettings",
    "HttpClientSettings",
    "IdempotencySettings",
    "KafkaProducerSettings",
    "KafkaSettings",
    "LoggingSettings",
    "MongoDbSettings",
    "ObservabilitySettings",
    "ProducedTopicsSettings",
    "RetryConfig",
    "SchemaRegistrySettings",
    "ServiceSettings",
    "Settings",
    "TopicsSettings",
    "get_settings",
]

