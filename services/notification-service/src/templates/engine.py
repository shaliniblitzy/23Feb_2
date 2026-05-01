"""Jinja2 ``Environment`` wrapper + Babel-backed filters for the Notification Service.

This module encapsulates the notification service's template-rendering
engine. It is a thin, configuration-driven facade over a single
:class:`jinja2.Environment` (or a pair of environments to allow
per-call autoescape toggling). It knows NOTHING about the database,
HTTP, Kafka, or any other notification-service concern.

Construction
------------
Constructed once at application startup by
:func:`src.container.build_container` (Step 8) with the ``TemplatesSettings``
instance as the sole positional argument::

    engine = TemplateEngine(settings.templates)

The engine is then wrapped by :class:`src.templates.renderer.TemplateRenderer`,
which is the only caller in production code. Tests may instantiate the
engine directly.

Public API
----------
- :meth:`TemplateEngine.render_string` --- compile + render a template
  string against a context. The primary API used by the renderer.
- :meth:`TemplateEngine.from_string` --- compile a template string to a
  :class:`jinja2.Template` object without rendering (for tests and
  advanced callers).
- :attr:`TemplateEngine.env` --- read-only access to the underlying
  :class:`jinja2.Environment` (autoescape=True variant). Exposed for
  advanced use cases; NOT used on the hot path.

Autoescape Semantics
--------------------
Two environments are maintained internally:

- ``_env_autoescape`` --- ``autoescape=True``; applied to email subject
  and HTML body.
- ``_env_no_autoescape`` --- ``autoescape=False``; applied to plain-text
  email body and SMS body.

The caller selects via the ``autoescape`` keyword on ``render_string``
and ``from_string``. Both environments share the same filters, tests,
and extensions; only the autoescape flag differs.

Strict Undefined
----------------
When ``settings.strict_undefined=True`` (the AAP-mandated default),
accessing an undefined variable raises :class:`jinja2.UndefinedError`
--- the renderer wraps this as
:class:`src.domain.errors.TemplateRenderError` at its boundary. When
``strict_undefined=False``, :class:`jinja2.ChainableUndefined` is used
(missing variables render as empty strings).

Locale-Aware Filters (Babel)
----------------------------
- ``format_currency(value, currency="USD", locale="en-US")``
- ``format_datetime(value, format="medium", locale="en-US")``
- ``pluralize(count, singular, plural=None, locale="en-US")``

All three accept BCP-47 locale tags (e.g. ``"en-US"``, ``"de-DE"``)
and convert to Babel's POSIX form (``"en_US"``) internally.

Custom Test
-----------
- ``is_critical`` --- ``{{ criticality is is_critical }}`` --- returns
  ``True`` when ``criticality == "critical"`` (the
  :class:`src.domain.channel_types.CriticalFlag.CRITICAL` value).

Authority
---------
- AAP Section 0.5.2.2 bullet 8 --- ``template renderer`` (the engine is
  part of this deliverable).
- AAP R-26 --- Structured JSON logs.
- Folder spec --- ``engine.py`` contract (strict undefined, autoescape,
  trim_blocks, Babel filters, i18n extension, no FS loader).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Standard-library imports (alphabetized)
# ---------------------------------------------------------------------------
# ``date``, ``datetime``, ``time`` — argument types accepted by the
# ``_format_datetime`` filter; the filter dispatches by ``isinstance``
# checks to the appropriate Babel formatter (datetime → format_datetime,
# date → format_date, time → format_time).
from datetime import date, datetime, time

# ``Decimal`` — preferred over ``float`` for monetary amounts to avoid
# floating-point precision artifacts (``0.1 + 0.2 != 0.3``). The
# ``_format_currency`` filter coerces ``int``, ``float``, ``Decimal``,
# and numeric strings to ``Decimal`` before passing to Babel.
# ``InvalidOperation`` is the precise exception class raised by
# :class:`Decimal` on parse failure; catching it (rather than a bare
# :class:`Exception`) keeps the error-mapping surface clean.
from decimal import Decimal, InvalidOperation

# ``Any`` — types the dynamic filter inputs (Jinja2 filter callables
# accept any context value).
# ``Final`` — marks immutable module-level constants for static analysis.
# ``Mapping`` — accepts any read-only mapping (dict, MappingProxyType,
# Pydantic-model dump) as the render context.
# ``Protocol`` + ``runtime_checkable`` — define the duck-typed
# :class:`TemplatesSettingsProtocol` that types the constructor argument
# without a hard dependency on :mod:`src.config.settings`.
from typing import Any, Final, Mapping, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Third-party imports (alphabetized)
# ---------------------------------------------------------------------------
# ``babel.dates`` / ``babel.numbers`` — locale-aware formatting
# primitives backing the Babel filters. Imported as submodules so the
# call sites read naturally (``babel.numbers.format_currency(...)``).
import babel.dates
import babel.numbers

# ``jinja2`` — the template engine itself. Imported as a top-level
# module so call sites read ``jinja2.Environment``, ``jinja2.StrictUndefined``,
# etc. Two environments are constructed: one with autoescape=True for
# HTML email content, and an overlay with autoescape=False for plain-
# text and SMS content.
import jinja2

# ``structlog`` — canonical structured-JSON logger across the
# Notification Service per AAP R-26. The engine emits a single
# ``template_engine_initialized`` INFO event during construction; per-
# render DEBUG events may be added in future without API changes.
import structlog

# ``Locale`` — used by the ``_pluralize`` filter to resolve CLDR plural
# rules for the target locale. ``Locale.parse`` is the entry point and
# raises :class:`UnknownLocaleError` on unrecognized BCP-47 / POSIX tags.
from babel import Locale

# ``UnknownLocaleError`` — caught uniformly across all three filters so
# locale-resolution failures translate to ``ValueError`` at the filter
# boundary (consumers of the engine see a single exception type for all
# bad input).
from babel.core import UnknownLocaleError

# ---------------------------------------------------------------------------
# Module-level logger and constants
# ---------------------------------------------------------------------------
# Module-scoped logger. ``structlog.get_logger(__name__)`` produces a
# :class:`structlog.stdlib.BoundLogger` bound to this module's name,
# inheriting whatever JSON / processor configuration is installed at
# application startup (see :mod:`src.config.logging_config`).
logger = structlog.get_logger(__name__)

# Default Babel locale used when the caller does not specify one. BCP-47
# form (``en-US``); converted to POSIX form (``en_US``) at the filter
# boundary via :func:`_to_babel_locale`.
_DEFAULT_LOCALE: Final[str] = "en-US"

# Centralized list of filter names registered on both internal Jinja2
# environments. Kept here so unit tests can assert the exact registration
# set without hard-coding the names a second time.
_ENGINE_FILTER_NAMES: Final[tuple[str, ...]] = (
    "format_currency",
    "format_datetime",
    "pluralize",
)

# Centralized list of test names registered on both internal Jinja2
# environments. Mirrors ``_ENGINE_FILTER_NAMES`` for symmetry and
# testability.
_ENGINE_TEST_NAMES: Final[tuple[str, ...]] = (
    "is_critical",
)


# ---------------------------------------------------------------------------
# Locale conversion helper
# ---------------------------------------------------------------------------
def _to_babel_locale(locale: str) -> str:
    """Convert BCP-47 (``en-US``) to Babel's POSIX form (``en_US``).

    Babel accepts underscores in locale tags; the Notification Service
    uses BCP-47 hyphenated tags throughout settings, payloads, and
    template metadata for consistency with HTTP ``Accept-Language``
    headers and the JSON event schemas. This helper performs the single
    conversion at the filter boundary so the rest of the engine speaks
    BCP-47 uniformly.

    Already-POSIX inputs (``en_US``) pass through unchanged because
    ``str.replace`` is a no-op when the search character is absent.

    Args:
        locale: BCP-47 tag (e.g. ``"en-US"``, ``"de-DE"``) or already-
            POSIX-style tag (e.g. ``"en_US"``, ``"de_DE"``).

    Returns:
        The POSIX form suitable for Babel's :func:`Locale.parse` and the
        ``locale=`` keyword of :func:`babel.numbers.format_currency`,
        :func:`babel.dates.format_datetime`, and friends.
    """
    return locale.replace("-", "_")


# ---------------------------------------------------------------------------
# Filter implementations (private; registered on the Environment in __init__)
# ---------------------------------------------------------------------------
def _format_currency(
    value: Any,
    currency: str = "USD",
    locale: str = _DEFAULT_LOCALE,
) -> str:
    """Format a numeric ``value`` as a localized currency string.

    Wraps :func:`babel.numbers.format_currency` with type-tolerant
    input handling. The function accepts any of:

    * :class:`int` --- coerced to :class:`Decimal` via ``Decimal(str(...))``
      to avoid the float-imprecision intermediate step.
    * :class:`float` --- same coercion path; documented note that
      :class:`Decimal` is the preferred input for money values.
    * :class:`Decimal` --- passed through.
    * :class:`str` --- parsed via :class:`Decimal`.

    Locale handling: BCP-47 inputs are converted to POSIX via
    :func:`_to_babel_locale`. Unknown locales raise
    :class:`UnknownLocaleError`, which is mapped to :class:`ValueError`
    so the renderer can surface a single exception type at its boundary.

    Args:
        value: A numeric amount. ``int``, ``float``, :class:`Decimal`,
            or a numeric string (parsed via :class:`Decimal`).
        currency: ISO 4217 currency code (e.g. ``"USD"``, ``"EUR"``,
            ``"INR"``). Unknown codes are NOT validated by Babel and
            will appear verbatim in the output (e.g. ``"ZZZ1.00"``);
            currency-code validation is the renderer/template author's
            responsibility.
        locale: BCP-47 tag (e.g. ``"en-US"``, ``"de-DE"``). Converted to
            Babel's POSIX form internally.

    Returns:
        The formatted currency string, e.g. ``"$1,234.50"`` for
        ``(1234.5, "USD", "en-US")`` or ``"1.234,50\xa0€"`` for
        ``(1234.5, "EUR", "de-DE")``.

    Raises:
        ValueError: If ``value`` cannot be parsed as a number, or the
            ``locale`` is invalid. Currency-code errors are NOT
            surfaced — Babel renders unknown codes verbatim.
    """
    # Stage 1: coerce ``value`` to Decimal. ``Decimal(str(x))`` is the
    # canonical pattern that avoids float-precision intermediate
    # representations (Decimal(0.1) is NOT 0.1; Decimal(str(0.1)) IS).
    try:
        if isinstance(value, Decimal):
            amount: Decimal = value
        elif isinstance(value, (int, float)):
            amount = Decimal(str(value))
        else:
            # ``str(value)`` covers the numeric-string case directly and
            # raises a :class:`TypeError` for non-stringifiable inputs
            # (e.g. ``object()``); both are caught below.
            amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(
            f"format_currency: cannot parse {value!r} as a number",
        ) from exc

    # Stage 2: format via Babel. Locale errors map to ValueError so the
    # caller sees a uniform exception type for all input-validation
    # failures. ValueError is also caught to defend against
    # internal Babel argument-validation paths.
    try:
        babel_locale = _to_babel_locale(locale)
        return babel.numbers.format_currency(amount, currency, locale=babel_locale)
    except (UnknownLocaleError, ValueError) as exc:
        raise ValueError(
            f"format_currency: invalid locale {locale!r} or currency {currency!r}",
        ) from exc


def _format_datetime(
    value: Any,
    format: str = "medium",  # noqa: A002 - shadows builtin; matches Babel's API
    locale: str = _DEFAULT_LOCALE,
) -> str:
    """Format a :class:`datetime`, :class:`date`, or :class:`time`.

    Dispatches by input type:

    * :class:`datetime.datetime` → :func:`babel.dates.format_datetime`
    * :class:`datetime.date` → :func:`babel.dates.format_date`
    * :class:`datetime.time` → :func:`babel.dates.format_time`
    * ``None`` → returns ``""`` (defensive default for templates that
      may receive a nullable timestamp from a Pydantic model dump).

    Args:
        value: A :class:`datetime.datetime`, :class:`datetime.date`,
            :class:`datetime.time`, or ``None``. Aware datetimes are
            formatted as-is; naive datetimes are passed through to
            Babel which assumes the locale's local time semantics.
        format: Babel format string --- one of the symbolic forms
            ``"short" | "medium" | "long" | "full"`` or a CLDR pattern
            like ``"yyyy-MM-dd"``.
        locale: BCP-47 locale tag.

    Returns:
        The formatted datetime string, or ``""`` if ``value is None``.

    Raises:
        ValueError: If ``value`` is not a supported type, the locale is
            invalid, or Babel rejects the format string.
    """
    # Defensive null-handling: templates often pass ``order.shipped_at``
    # or similar nullable timestamps. Returning empty string is more
    # template-friendly than raising on a legitimate null.
    if value is None:
        return ""

    # NOTE: ``isinstance(value, datetime)`` MUST be checked BEFORE
    # ``isinstance(value, date)`` because ``datetime`` is a subclass of
    # ``date``; reversing the order would route every datetime through
    # the date formatter and silently strip the time component.
    try:
        babel_locale = _to_babel_locale(locale)
        if isinstance(value, datetime):
            return babel.dates.format_datetime(value, format=format, locale=babel_locale)
        if isinstance(value, date):
            return babel.dates.format_date(value, format=format, locale=babel_locale)
        if isinstance(value, time):
            return babel.dates.format_time(value, format=format, locale=babel_locale)
    except (UnknownLocaleError, ValueError) as exc:
        raise ValueError(
            f"format_datetime: invalid locale {locale!r} or format {format!r}",
        ) from exc

    # Fall-through for unsupported types (e.g. ``str``, ``int``).
    raise ValueError(
        f"format_datetime: unsupported type {type(value).__name__}",
    )


def _pluralize(
    count: Any,
    singular: str,
    plural: str | None = None,
    locale: str = _DEFAULT_LOCALE,
) -> str:
    """Select singular or plural form based on ``count`` and ``locale``.

    Uses Babel's CLDR plural rules to determine the appropriate form.
    For two-form languages (English, German, Spanish, etc.), the
    ``"one"`` plural-form returns ``singular`` and any other category
    (``"zero"``, ``"two"``, ``"few"``, ``"many"``, ``"other"``) returns
    the plural.

    When the caller omits ``plural``, the function derives it by
    appending ``"s"`` to ``singular`` --- a crude English-only rule.
    For non-English locales callers MUST pass ``plural`` explicitly
    (e.g. ``"child" / "children"``, ``"das Kind" / "die Kinder"``).

    Args:
        count: Integer count. Accepts any value convertible via
            :func:`int` --- ``int``, ``float`` (truncated), numeric
            string. Non-numeric values raise :class:`ValueError`.
        singular: The singular form.
        plural: OPTIONAL plural form. If ``None``, derived as
            ``singular + "s"``.
        locale: BCP-47 locale tag.

    Returns:
        The appropriate form for ``count`` under the target locale's
        plural rules.

    Raises:
        ValueError: If ``count`` is not integer-convertible or the
            locale is invalid.
    """
    # Stage 1: integer coercion. Templates commonly pass ``order.count``
    # which may be an int already, but Pydantic model dumps occasionally
    # surface numeric strings; ``int(count)`` accommodates both.
    try:
        n = int(count)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"pluralize: count {count!r} is not an integer") from exc

    # Stage 2: derive an effective plural form. The English ``"s"``
    # heuristic is intentionally simple --- non-English templates MUST
    # pass both forms explicitly.
    effective_plural: str = plural if plural is not None else f"{singular}s"

    # Stage 3: look up the locale's CLDR plural rules. Locale errors map
    # to ValueError for consistency with the other filters.
    try:
        babel_locale = Locale.parse(_to_babel_locale(locale))
    except (UnknownLocaleError, ValueError) as exc:
        raise ValueError(f"pluralize: invalid locale {locale!r}") from exc

    # Babel's plural-form selector returns one of "zero", "one", "two",
    # "few", "many", "other". The two-form mapping below handles English
    # cleanly and degrades gracefully for languages with more
    # categories: "one" → singular, everything else → plural. A future
    # enhancement could accept a full {form: text} mapping for full
    # CLDR support.
    form: str = babel_locale.plural_form(n)
    return singular if form == "one" else effective_plural


# ---------------------------------------------------------------------------
# Test implementations (private; registered on the Environment in __init__)
# ---------------------------------------------------------------------------
def _is_critical(value: Any) -> bool:
    """Jinja2 test: ``{{ x is is_critical }}``.

    Returns ``True`` when ``value`` represents the
    :class:`src.domain.channel_types.CriticalFlag.CRITICAL` enum value
    (which is the literal string ``"critical"`` because ``CriticalFlag``
    is a :class:`StrEnum`). Comparison is case-insensitive against the
    string representation of ``value``, so both
    :class:`src.domain.channel_types.CriticalFlag.CRITICAL` (StrEnum
    instance) and the bare string ``"critical"`` evaluate truthy.

    Booleans are handled explicitly: ``True`` → ``True``, ``False`` →
    ``False``. ``None`` → ``False``. The string comparison avoids
    importing ``CriticalFlag`` from :mod:`src.domain.channel_types`,
    keeping the engine module standalone-testable without a full
    ``src.*`` import graph.

    Args:
        value: Any Python value. Typically a string or
            :class:`StrEnum` member from the template context.

    Returns:
        ``True`` if ``value`` represents the CRITICAL flag; ``False``
        otherwise.
    """
    # ``None`` is never critical. Cheap fast-path that avoids a
    # ``str(None) == "none"`` comparison surprise.
    if value is None:
        return False

    # Booleans short-circuit to themselves. Without this branch a
    # ``True`` would route through ``str(True).lower() == "critical"``
    # which would return ``False``, contradicting the documented
    # contract that truthy booleans test as critical.
    if isinstance(value, bool):
        return value is True

    # Generic case: stringify and compare case-insensitively. Wrapped in
    # a broad try/except because user-provided objects in the template
    # context may have a __str__ that raises (e.g. partially-initialised
    # SQLAlchemy models). Returning ``False`` on stringification failure
    # is the conservative choice for a security-critical predicate.
    try:
        return str(value).lower() == "critical"
    except Exception:  # noqa: BLE001 — defensive boundary for arbitrary __str__
        return False


# ---------------------------------------------------------------------------
# TemplatesSettingsProtocol — duck-typed view of TemplatesSettings.
# ---------------------------------------------------------------------------
@runtime_checkable
class TemplatesSettingsProtocol(Protocol):
    """Duck-typed view of :class:`src.config.settings.TemplatesSettings`.

    Any object exposing these five attributes --- a Pydantic
    :class:`BaseModel`, a :class:`dataclasses.dataclass`, a plain
    namespace, or a :class:`typing.NamedTuple` --- is acceptable as the
    constructor argument to :class:`TemplateEngine`. Decoupling from
    the concrete settings type prevents a hard import cycle during
    early-boot partial scaffolding (the engine module is loaded before
    every settings sub-model is finalized in some test layouts) and
    keeps the engine standalone-testable with a minimal mock.

    The five attributes mirror the production
    :class:`TemplatesSettings` Pydantic model verbatim:

    * ``engine`` --- expected to be the literal ``"jinja2"``.
    * ``cache_enabled`` --- whether the LOADER (not Jinja2) caches
      template records; informational only at the engine layer.
    * ``cache_max_size`` --- LOADER cache size; the engine caps Jinja2's
      own compiled-template LRU cache at ``min(cache_max_size, 400)``.
    * ``cache_ttl_seconds`` --- LOADER cache TTL; not used by the engine
      directly.
    * ``strict_undefined`` --- selects :class:`jinja2.StrictUndefined`
      when True, :class:`jinja2.ChainableUndefined` when False.

    Attributes:
        engine: Template engine identifier (must be ``"jinja2"``).
        cache_enabled: Loader cache toggle; informational at this layer.
        cache_max_size: Loader cache size; capped at 400 for Jinja2's
            internal cache.
        cache_ttl_seconds: Loader cache TTL; unused by the engine.
        strict_undefined: Selects strict (True) vs chainable (False)
            undefined handling.
    """

    engine: str
    cache_enabled: bool
    cache_max_size: int
    cache_ttl_seconds: int
    strict_undefined: bool


# ---------------------------------------------------------------------------
# TemplateEngine class
# ---------------------------------------------------------------------------
class TemplateEngine:
    """Jinja2 ``Environment`` wrapper with Babel filters and per-call autoescape.

    Constructed once at application startup by
    :func:`src.container.build_container` Step 8 and then wrapped by
    :class:`src.templates.renderer.TemplateRenderer`. Holds two
    :class:`jinja2.Environment` instances (one with autoescape=True,
    one with autoescape=False); the :meth:`render_string` and
    :meth:`from_string` methods dispatch by the ``autoescape`` keyword.

    Both internal environments share the same filters
    (``format_currency``, ``format_datetime``, ``pluralize``), tests
    (``is_critical``), and the ``jinja2.ext.i18n`` extension. The
    overlay environment is created via :meth:`jinja2.Environment.overlay`
    so it shares the parent's compiled-template cache --- compile cost
    is paid once per source string regardless of which autoescape mode
    renders it.

    Args:
        settings: A :class:`TemplatesSettingsProtocol`-shaped object
            --- typically ``settings.templates`` from
            :class:`src.config.settings.Settings`.

    Attributes:
        env: (read-only property) The autoescape=True
            :class:`jinja2.Environment`. Exposed for advanced use
            cases (e.g. :func:`pytest.fixture` that injects globals via
            ``env.globals[...]``); NOT used on the hot path.

    Raises:
        ValueError: If ``settings.engine != "jinja2"`` (the only
            supported engine identifier) or
            ``settings.cache_max_size < 1``.

    Invariants:
        - ``settings.engine == "jinja2"`` (the only supported engine).
        - ``settings.cache_max_size >= 1``.

    Example:
        Construct directly in a test::

            from types import SimpleNamespace
            settings = SimpleNamespace(
                engine="jinja2",
                cache_enabled=True,
                cache_max_size=500,
                cache_ttl_seconds=600,
                strict_undefined=True,
            )
            engine = TemplateEngine(settings)
            engine.render_string("Hello {{ name }}", {"name": "World"})
            # → "Hello World"
    """

    # ``__slots__`` declares every instance attribute up-front, blocking
    # accidental attribute assignment ('typo guard') and shaving a small
    # amount of per-instance memory. The engine is a singleton so the
    # memory delta is negligible; the typo-guard property is the real
    # value: a renderer that tries to read ``engine.something_missing``
    # gets an :class:`AttributeError` immediately instead of a
    # ``None``-returning attribute access.
    __slots__ = (
        "_env_autoescape",
        "_env_no_autoescape",
        "_strict_undefined",
    )

    def __init__(self, settings: TemplatesSettingsProtocol) -> None:
        """Construct the engine from a :class:`TemplatesSettingsProtocol`.

        Performs strict validation of the settings (engine identifier
        and cache size), constructs both Jinja2 environments,
        registers filters and tests, and emits a single
        ``template_engine_initialized`` INFO log event. Subsequent
        renders do not log at INFO level (DEBUG-level per-render logs
        may be added in future).

        Args:
            settings: A :class:`TemplatesSettingsProtocol`-shaped
                object. The five required attributes are read once at
                construction; mutating ``settings`` after construction
                has no effect on the engine.

        Raises:
            ValueError: If ``settings.engine != "jinja2"`` or
                ``settings.cache_max_size < 1``.
        """
        # Stage 1: settings validation. Fail-fast (AAP R-19 spirit) on
        # invalid input so the operator sees a precise error at startup
        # rather than a confusing rendering failure later.
        if settings.engine != "jinja2":
            raise ValueError(
                "TemplateEngine: only engine=jinja2 is supported; "
                f"got {settings.engine!r}",
            )
        if settings.cache_max_size < 1:
            raise ValueError(
                "TemplateEngine: cache_max_size must be >= 1; "
                f"got {settings.cache_max_size}",
            )

        # Stage 2: snapshot the strict_undefined flag. ``bool(...)`` is
        # a defensive coercion in case a non-bool truthy value is passed
        # (the protocol declares ``bool`` but duck-typing accepts e.g.
        # ``int(1)``).
        self._strict_undefined: bool = bool(settings.strict_undefined)

        # Stage 3: select the undefined class. ``StrictUndefined`` raises
        # immediately on attribute access; ``ChainableUndefined`` allows
        # ``{{ x.y.z }}`` to silently render as empty string when any
        # link in the chain is missing.
        undefined_cls: type[jinja2.Undefined] = (
            jinja2.StrictUndefined
            if self._strict_undefined
            else jinja2.ChainableUndefined
        )

        # Stage 4: derive Jinja2's internal compiled-template cache size.
        # Jinja2's documented default is 400; setting it higher rarely
        # helps because compile cost is amortized over many renders of
        # the same template. We cap at 400 and let the loader's separate
        # TTL cache (settings.cache_ttl_seconds) handle the
        # template-record dimension.
        jinja_cache_size: int = min(int(settings.cache_max_size), 400)

        # Stage 5: build the primary autoescape=True environment. This
        # is used for HTML email subjects and HTML email bodies where
        # ``{{ user_input }}`` MUST be HTML-escaped to prevent XSS.
        #
        # ``loader=jinja2.BaseLoader()`` is a no-op loader: we never
        # call ``env.get_template("filename")``; all templates flow
        # through ``env.from_string(...)``. ``BaseLoader`` raises
        # ``TemplateNotFound`` on ``get_source()``, which is the correct
        # fail mode for any accidental ``get_template`` call.
        #
        # ``trim_blocks`` + ``lstrip_blocks`` produce uniform whitespace
        # in rendered output --- ``{% if x %}\n   text\n{% endif %}``
        # renders as ``text\n`` rather than ``\n   text\n``.
        #
        # ``extensions=["jinja2.ext.i18n"]`` enables ``{% trans %}``
        # blocks; we install null translations below so ``{% trans %}``
        # passes through unchanged (a future enhancement would load PO
        # catalogs).
        #
        # ``enable_async=False`` keeps rendering synchronous --- every
        # filter is sync and there is no value in forcing the renderer
        # to ``await`` Jinja2.
        self._env_autoescape: jinja2.Environment = jinja2.Environment(
            loader=jinja2.BaseLoader(),
            autoescape=jinja2.select_autoescape(enabled_extensions=("html", "htm")),
            undefined=undefined_cls,
            trim_blocks=True,
            lstrip_blocks=True,
            extensions=["jinja2.ext.i18n"],
            cache_size=jinja_cache_size,
            enable_async=False,
        )

        # Stage 6: install null translations on the primary env. The
        # i18n extension exposes ``install_null_translations`` as an
        # instance method; mypy's Jinja2 stubs may not know about it
        # because it's an extension-injected method, hence the
        # ``# type: ignore[attr-defined]`` comment.
        self._env_autoescape.install_null_translations()  # type: ignore[attr-defined]

        # Stage 7: build the autoescape=False overlay. Jinja2's
        # :meth:`Environment.overlay` returns a linked environment that
        # shares the compiled-template cache and globals with the
        # parent --- cheap to construct, cheap to keep around. Used for
        # plain-text email bodies and SMS bodies where HTML escaping is
        # WRONG (the ampersand in ``you & I`` should NOT become
        # ``you &amp; I`` in an SMS).
        self._env_no_autoescape: jinja2.Environment = self._env_autoescape.overlay(
            autoescape=False,
        )

        # Stage 8: re-install null translations on the overlay. Behavior
        # of overlay() with respect to extension state has changed
        # across Jinja2 versions; explicit re-installation is the
        # belt-and-braces approach.
        self._env_no_autoescape.install_null_translations()  # type: ignore[attr-defined]

        # Stage 9: register filters and tests on BOTH environments.
        # While ``overlay()`` shares many attributes between the parent
        # and child, the safest pattern is to register each filter on
        # each env explicitly; this is also what the unit tests assert
        # (assertion of registration on both envs).
        for env in (self._env_autoescape, self._env_no_autoescape):
            env.filters["format_currency"] = _format_currency
            env.filters["format_datetime"] = _format_datetime
            env.filters["pluralize"] = _pluralize
            env.tests["is_critical"] = _is_critical

        # Stage 10: emit the construction log event. AAP R-26 mandates
        # structured JSON logs; ``structlog.get_logger`` returns a
        # :class:`structlog.stdlib.BoundLogger` that emits JSON when the
        # service's logging configuration is loaded (see
        # :mod:`src.config.logging_config`).
        logger.info(
            "template_engine_initialized",
            engine=settings.engine,
            strict_undefined=self._strict_undefined,
            cache_enabled=bool(settings.cache_enabled),
            cache_max_size=int(settings.cache_max_size),
            cache_ttl_seconds=int(settings.cache_ttl_seconds),
            jinja_cache_size=jinja_cache_size,
            filters=list(_ENGINE_FILTER_NAMES),
            tests=list(_ENGINE_TEST_NAMES),
        )

    # -----------------------------------------------------------------
    # Public read-only property
    # -----------------------------------------------------------------
    @property
    def env(self) -> jinja2.Environment:
        """Read-only access to the autoescape=True :class:`Environment`.

        Exposed for tests and advanced callers (e.g. to inject globals
        via ``env.globals[...]``, or to verify filter registration in a
        unit test). Production code should use :meth:`render_string` or
        :meth:`from_string` directly.

        Returns:
            The autoescape=True :class:`jinja2.Environment` instance.
        """
        return self._env_autoescape

    # -----------------------------------------------------------------
    # Public render method
    # -----------------------------------------------------------------
    def render_string(
        self,
        template_source: str,
        context: Mapping[str, Any],
        *,
        locale: str = _DEFAULT_LOCALE,
        autoescape: bool = True,
    ) -> str:
        """Compile and render a template string against ``context``.

        Synchronous and CPU-bound. A typical template renders in well
        under 1 ms; the dominant cost is the initial compile, which
        happens once per ``template_source`` per process (Jinja2 caches
        compiled templates internally up to ``cache_size``).

        The ``locale`` argument is auto-injected into the render
        context as the ``_locale`` variable. Templates can therefore
        write::

            Total: {{ order.total|format_currency(locale=_locale) }}

        without the caller having to thread ``locale`` through every
        filter call.

        The ``autoescape`` argument selects between the two internal
        environments. Use ``autoescape=True`` for email subjects and
        HTML email bodies (where ``{{ user_input }}`` must be
        HTML-escaped); use ``autoescape=False`` for plain-text email
        bodies and SMS bodies (where ampersands and angle brackets must
        appear verbatim).

        Args:
            template_source: The Jinja2 template source string ---
                typically one of ``Template.subject_template``,
                ``Template.body_template_html``, or
                ``Template.body_template_text`` from a database
                template record.
            context: Render context. Jinja2 variable references
                resolve against this mapping. The mapping is copied
                before injecting ``_locale`` to avoid mutating the
                caller's data.
            locale: BCP-47 locale tag. Made available to the template
                as the ``_locale`` context variable. Defaults to
                ``"en-US"``.
            autoescape: When True (default), HTML-escapes ``{{ x }}``
                substitutions. When False, emits raw text.

        Returns:
            The rendered string.

        Raises:
            jinja2.UndefinedError: If ``settings.strict_undefined=True``
                and a referenced variable is not in ``context``.
            jinja2.TemplateSyntaxError: If ``template_source`` is not
                valid Jinja2.
            jinja2.TemplateError: Other Jinja2 rendering errors,
                including filter-raised :class:`ValueError` re-wrapped
                by Jinja2.
        """
        # Compile via from_string (cheap on cache hit, ~ms cost on miss).
        template = self.from_string(template_source, autoescape=autoescape)

        # Inject ``_locale`` into the context so filter calls can pick it
        # up without the caller threading it through. ``dict(context)``
        # produces a shallow copy that is safe to mutate. We do NOT
        # rebind ``_locale`` if the caller has explicitly supplied it
        # in the context already (the caller's value wins via ordering).
        merged_context: dict[str, Any] = {"_locale": locale, **dict(context)}
        return template.render(merged_context)

    # -----------------------------------------------------------------
    # Public compile method
    # -----------------------------------------------------------------
    def from_string(
        self,
        template_source: str,
        *,
        autoescape: bool = True,
    ) -> jinja2.Template:
        """Compile a template string to a :class:`jinja2.Template`.

        Exposed for tests and advanced callers that want to pre-compile
        a template (e.g. compile once at startup, render many times).
        Production code typically uses :meth:`render_string`, which
        calls this method internally.

        Args:
            template_source: The Jinja2 template source string.
            autoescape: When True (default), selects the autoescape
                environment. When False, selects the no-autoescape
                overlay.

        Returns:
            The compiled :class:`jinja2.Template`. Each call to
            ``from_string`` produces a fresh :class:`Template` instance
            (Jinja2's compile cache is keyed on bytecode, not on the
            Template object identity).

        Raises:
            jinja2.TemplateSyntaxError: If ``template_source`` is not
                valid Jinja2.
        """
        env: jinja2.Environment = (
            self._env_autoescape if autoescape else self._env_no_autoescape
        )
        return env.from_string(template_source)


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------
# Alphabetized per the project's style rules. Both symbols are public:
# ``TemplateEngine`` is the concrete engine; ``TemplatesSettingsProtocol`` is
# the duck-typed argument shape that lets tests mock the settings object
# without importing the full :mod:`src.config.settings` module.
__all__: list[str] = [
    "TemplateEngine",
    "TemplatesSettingsProtocol",
]
