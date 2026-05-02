"""Order Service — :class:`SagaStep` enum and :class:`SagaState` aggregate.

This module is a foundational leaf of the Order Service's domain
package. It defines:

1. :class:`SagaStep` — a :class:`enum.StrEnum` enumerating the seven
   workflow stages an in-flight checkout saga progresses through
   (``CREATE_ORDER``, ``AWAIT_INVENTORY``, ``AWAIT_PAYMENT``,
   ``CONFIRM_ORDER``, ``COMPENSATE_INVENTORY``,
   ``COMPENSATE_PAYMENT``, ``TERMINATED``). The string values are an
   EXTERNAL CONTRACT: they appear as the ``current_step`` value in the
   ``saga_state`` table's CHECK constraint and as a structured-log
   field used by Kibana dashboards (AAP R-18 / R-26 / R-28).

2. :class:`SagaState` — a Pydantic v2 frozen :class:`pydantic.BaseModel`
   that mirrors a single row of the ``saga_state`` table (per AAP
   Section 0.4.4) PLUS two operational fields not in the on-disk
   schema: ``correlation_id`` (request-scoped tracing identifier per
   AAP R-13) and ``version`` (optimistic-lock counter incremented on
   every transition). It is the in-memory representation of the
   durable saga progress checkpoint and the keystone of saga
   recoverability (AAP R-18) — a crashed coordinator must not lose
   in-flight saga progress, hence the durability of every transition.

3. :func:`is_terminal` — a module-level predicate returning ``True``
   when a :class:`SagaState` is in the ``TERMINATED`` step. Used by
   the scheduler and tests to skip rows that need no further
   processing. Module-level (not a method) to keep :class:`SagaState`
   a thin data record.

4. :func:`next_deadline` — a module-level pure helper that computes
   ``now + step_timeout_ms`` as a timezone-aware UTC datetime, with
   defensive validation: rejects naive ``now``, rejects negative
   ``step_timeout_ms``, and caps ``step_timeout_ms`` at 24 hours
   (defensive bound against misconfiguration). Used by the saga
   coordinator and scheduler to compute the ``deadline_at`` field
   for the next saga step (AAP R-20 — timeout-driven compensation).

Design invariants
-----------------
* **Foundational module** — this file MUST NOT import from any other
  ``src/*`` module (not even peer domain modules such as ``order.py``,
  ``order_item.py``, ``order_status.py``, or ``errors.py``). The
  bridge between :class:`SagaStep` (the saga coordinator's INTERNAL
  workflow stage) and :class:`OrderStatus` (the order's PUBLIC
  lifecycle status) is encoded in ``saga/state_machine.py``, which
  consumes both. Inverting that direction here would create a circular
  dependency.

* **Self-contained** — :class:`SagaStep` and :class:`SagaState` ship
  in the SAME module because they are tightly coupled
  (:class:`SagaState` ALWAYS has a ``current_step`` of type
  :class:`SagaStep`). Splitting them into separate ``saga_step.py``
  and ``saga_state.py`` modules would force every importer to do two
  imports without conceptual benefit.

* **Frozen / immutable** — :class:`SagaState` is configured with
  ``model_config.frozen=True`` so every saga transition produces a
  NEW instance via :meth:`pydantic.BaseModel.model_copy` with an
  ``update={...}`` mapping that includes an incremented ``version``.
  The repository layer then writes the new instance to PostgreSQL
  with a ``WHERE version = expected_version`` clause; if PostgreSQL
  reports zero rows updated, another writer raced and the repository
  raises ``OptimisticLockFailure``. Without immutability, two
  concurrent transitions could mutate the same instance and silently
  desynchronize from the database.

* **Strict typing** — :class:`uuid.UUID` for every identifier (never
  ``str``); timezone-aware :class:`datetime.datetime` for every
  timestamp (per AAP R-26, RFC 3339); bounded integers for retry
  counts and version; bounded-length strings for diagnostic free text
  (``last_error`` ≤ 500 chars, ``awaiting_event`` ≤ 128 chars).

* **No I/O at import time** — only enum construction, model
  construction, and constant evaluation happen at module load. No
  logging, no HTTP framework, no DB driver, no Kafka client.

* **No** :func:`enum.auto` — every :class:`SagaStep` member assigns
  its string value EXPLICITLY. The values are externally visible
  (PostgreSQL CHECK constraint, structured-log fields); :func:`enum.auto`
  would derive values from member names and silently break the
  contract on rename.

External contract
-----------------
The seven string values declared by :class:`SagaStep` are pinned and
must remain stable across releases:

* **Database** — :class:`SagaStep` mirrors the ``saga_state.current_step``
  column in the Order Service's private ``order_db`` (AAP Section
  0.4.4). The DDL declares an inline CHECK constraint
  ``ck_saga_state__current_step_enum`` of the form::

      current_step IN (
          'CREATE_ORDER', 'AWAIT_INVENTORY', 'AWAIT_PAYMENT',
          'CONFIRM_ORDER', 'COMPENSATE_INVENTORY',
          'COMPENSATE_PAYMENT', 'TERMINATED'
      )

  Renaming any value REQUIRES a coordinated DDL migration plus a
  backfill of existing rows.

* **Structured logs** — emitted as the ``saga_step`` log field
  (AAP R-26) and surfaced in Kibana dashboards (AAP R-28). Renaming
  invalidates saved queries and dashboard filters.

* **Kafka events** — saga transitions trigger emission of
  ``order.created`` / ``order.cancelled`` / ``order.fulfilled``
  events; while :class:`SagaStep` itself is NOT in the public event
  payload (the public surface is :class:`OrderStatus`), the saga
  coordinator's internal logging includes the step name for
  cross-system tracing.

Authoritative references
------------------------
* AAP Section 0.1.1 Component #6 — Order Service overview.
* AAP Section 0.4.4 — ``order_db.saga_state`` schema with columns
  ``(order_id, saga_id, current_step, awaiting_event, retry_count,
  deadline_at, compensation_required, last_error, correlation_id,
  updated_at)``. This module's :class:`SagaState` mirrors that shape
  and ADDS ``version`` for optimistic locking.
* AAP Section 0.5.2.2 bullet 6 — Order Service implementation plan
  with saga coordinator (``create -> reserve-inventory ->
  take-payment -> confirm | compensate``).
* AAP R-13 — correlation-ID propagation (the ``correlation_id``
  field originates at the API Gateway and propagates through every
  saga step's events and HTTP calls).
* AAP R-18 (CRITICAL) — saga pattern with explicit compensation
  steps; :class:`SagaStep` enumerates the workflow stages and
  :class:`SagaState` persists progress.
* AAP R-19 — fail-fast at startup; saga state durability is the
  reason ``/health/ready`` checks Postgres write-ability.
* AAP R-20 — fallback / timeout-driven compensation; :func:`next_deadline`
  computes ``deadline_at`` for the next step.
* AAP R-26 — structured logs with RFC 3339 timestamps (timezone-aware
  datetimes mandatory).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Final
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SagaStep(StrEnum):
    """Workflow stage of a checkout saga.

    Mirrors the ``saga_state.current_step`` column in PostgreSQL (per
    AAP Section 0.4.4). The string values are part of an EXTERNAL
    CONTRACT enforced by an inline CHECK constraint
    (``ck_saga_state__current_step_enum``) on the ``saga_state``
    table; drift between this enum and the CHECK constraint would
    cause runtime errors on insert.

    Members (UPPERCASE — must match the DDL CHECK constraint values
    exactly; member NAMES match VALUES for symmetry):

      * ``CREATE_ORDER`` — Initial step; the order has been recorded
        locally in the ``orders`` table and the ``order.created``
        event has been emitted to Kafka. The saga is awaiting a
        downstream signal (typically the Inventory Service's
        ``inventory.reserved`` event) to advance.
      * ``AWAIT_INVENTORY`` — Explicit await state for the
        ``inventory.reserved`` or ``inventory.reservation_failed``
        event from the Inventory Service. Distinct from
        ``CREATE_ORDER`` so retry counting can be scoped to the
        inventory step specifically (e.g., three inventory retries
        before compensating).
      * ``AWAIT_PAYMENT`` — Inventory has been reserved; the saga is
        awaiting a ``payment.succeeded`` or ``payment.failed`` event
        from the Payment Service.
      * ``CONFIRM_ORDER`` — Payment has been captured; the saga is
        finalizing the order and emitting ``order.fulfilled``. This
        is a brief in-flight state — the saga transitions out of it
        as soon as the confirmation write + Kafka emit complete.
      * ``COMPENSATE_INVENTORY`` — Failure path; the saga is
        releasing previously-reserved inventory by emitting
        ``order.cancelled`` (which the Inventory Service consumes to
        release stock).
      * ``COMPENSATE_PAYMENT`` — Failure path; the saga is reversing
        a captured payment by emitting ``order.cancelled`` (which the
        Payment Service consumes to issue a refund). Followed by
        ``COMPENSATE_INVENTORY`` if inventory was also reserved.
      * ``TERMINATED`` — Terminal step. The saga has reached either
        success (``order.fulfilled`` emitted) or failure
        (compensation complete and ``order.cancelled`` emitted) and
        no further transitions occur. The saga scheduler skips rows
        in this state via the partial index
        ``idx_saga_state__deadline WHERE current_step <> 'TERMINATED'``.

    Note:
        The textual values are part of an EXTERNAL CONTRACT: they
        appear as ``saga_state.current_step`` rows in PostgreSQL AND
        as the ``saga_step`` field in structured logs. Any rename
        REQUIRES a coordinated DDL CHECK-constraint update plus a
        backfill migration of existing rows.

    See AAP R-18 for the saga pattern overview and
    ``saga/state_machine.py`` for the (current_step, event)
    transition table.

    Example:
        >>> SagaStep.CREATE_ORDER == "CREATE_ORDER"
        True
        >>> SagaStep("AWAIT_INVENTORY") is SagaStep.AWAIT_INVENTORY
        True
        >>> isinstance(SagaStep.TERMINATED, str)
        True
        >>> str(SagaStep.CONFIRM_ORDER)
        'CONFIRM_ORDER'
    """

    CREATE_ORDER = "CREATE_ORDER"
    AWAIT_INVENTORY = "AWAIT_INVENTORY"
    AWAIT_PAYMENT = "AWAIT_PAYMENT"
    CONFIRM_ORDER = "CONFIRM_ORDER"
    COMPENSATE_INVENTORY = "COMPENSATE_INVENTORY"
    COMPENSATE_PAYMENT = "COMPENSATE_PAYMENT"
    TERMINATED = "TERMINATED"


class SagaState(BaseModel):
    """Durable progress record for a checkout saga.

    Mirrors the ``saga_state`` table (per AAP Section 0.4.4) in the
    Order Service's private ``order_db``. The ``saga_state`` table
    is the **keystone** of saga durability (AAP R-18) — without it,
    a crashed coordinator would lose in-flight saga progress and
    the explicit-compensation guarantee of AAP R-18 would be
    unrealizable across pod restarts and Kafka consumer rebalances.

    Lifecycle and persistence pattern
    ---------------------------------
    The class is FROZEN. Every transition produces a NEW instance
    via :meth:`pydantic.BaseModel.model_copy` with an ``update={...}``
    mapping that includes an incremented ``version``::

        next_state = current_state.model_copy(
            update={
                "current_step": SagaStep.AWAIT_INVENTORY,
                "awaiting_event": "inventory.reserved",
                "deadline_at": next_deadline(now, step_timeout_ms),
                "updated_at": now,
                "version": current_state.version + 1,
            }
        )

    The repository layer then writes the new instance to PostgreSQL
    with a ``WHERE order_id = :id AND version = :expected`` clause.
    PostgreSQL returns 0 rows updated when another writer has
    raced ahead; the repository raises ``OptimisticLockFailure`` and
    the caller retries by re-reading and re-applying the transition.

    Field semantics
    ---------------
    * ``order_id`` — :class:`uuid.UUID` reference to the ``orders``
      row this saga drives. ONE-TO-ONE relationship: at most one
      active saga per order. Encoded as both the PRIMARY KEY and a
      FOREIGN KEY in the DDL (no separate surrogate ``saga_state.id``).
    * ``saga_id`` — Stable :class:`uuid.UUID` identifier for THIS
      specific saga instance, embedded in event payloads so consumers
      can correlate events back to the saga without knowing
      ``order_id``. A new ``saga_id`` is generated if the order is
      re-attempted (rare, e.g., after a manual reconciliation).
    * ``current_step`` — Current :class:`SagaStep` value; advances
      per the transition table in ``saga/state_machine.py``.
    * ``awaiting_event`` — Optional Kafka topic name (e.g.,
      ``"inventory.reserved"``) the saga is currently waiting for.
      ``None`` when the saga is in a synchronous step
      (``CONFIRM_ORDER`` is a DB write + Kafka emit, not an
      event-await) or terminal (``TERMINATED``).
    * ``retry_count`` — How many times the current step has been
      retried after a transient failure. Bounded by the saga
      configuration (typically ``saga.max_compensation_attempts``);
      exceeding the bound triggers a transition to
      ``COMPENSATE_*`` or ``TERMINATED``.
    * ``deadline_at`` — Absolute UTC timestamp after which the
      scheduler triggers timeout-based compensation (AAP R-20).
      ``None`` for terminal steps. Computed at every transition by
      :func:`next_deadline`.
    * ``compensation_required`` — ``True`` when the saga has entered
      a compensating step (``COMPENSATE_*``) or will enter one when
      the next failure event arrives. Used by the scheduler to
      distinguish forward-progress retries from compensation retries.
    * ``last_error`` — Optional human-readable last error message
      for diagnostics. Truncated to 500 characters to keep the
      ``saga_state`` row compact (full stack traces should live in
      the ELK log stream per AAP R-26 / R-27, not in PostgreSQL
      ``TEXT`` columns where they bloat row size and slow scans).
    * ``correlation_id`` — :class:`uuid.UUID` request-scoped
      correlation identifier propagated from the API Gateway
      (AAP R-13). REQUIRED — every saga is created in response to
      an API request and the correlation_id is mandatory for
      end-to-end tracing across the saga's events and HTTP calls.
    * ``updated_at`` — Last modification timestamp; updated on every
      transition. Powers operator dashboards ("which sagas have
      not progressed in 5+ minutes?") and supports debugging.
    * ``version`` — Optimistic-lock counter. Starts at ``1`` on
      saga creation and increments by 1 per transition. The
      repository's ``UPDATE ... WHERE version = :expected`` clause
      uses this column to detect concurrent writers.

    Why ``version`` is in the in-memory class but NOT in the DDL
    of revision 20260101_000004
    -----------------------------------------------------------
    The ``version`` column is added by a forthcoming migration
    revision (or by extending revision 20260101_000004 if not yet
    applied to production). Until then, the repository may simulate
    optimistic locking via ``updated_at`` comparison or a SELECT
    FOR UPDATE. The in-memory class declares the field upfront so
    application code doesn't churn when the column lands.

    Why ``correlation_id`` is REQUIRED (not optional)
    -------------------------------------------------
    Per AAP R-13. Every saga is created in response to an API
    request; the API Gateway always generates a correlation_id
    (RFC 4122 UUID) and propagates it via the ``X-Correlation-ID``
    header. If the field is missing on saga creation, that's a bug
    we want surfaced loudly via Pydantic's ``ValidationError``,
    not silently swallowed.

    Validators enforce
    ------------------
    * ``retry_count >= 0`` (Pydantic ``Field(ge=0)``).
    * ``version >= 1`` (Pydantic ``Field(ge=1)``).
    * ``last_error`` is ``None`` or a non-empty string ≤ 500 chars
      after stripping whitespace; whitespace-only is normalized to
      ``None``; longer values are truncated to 500 chars.
    * ``awaiting_event`` is ``None`` or a non-empty stripped string
      ≤ 128 chars; whitespace-only is REJECTED (a deliberate empty
      topic name is a programmer error, distinct from "no event").
    * ``deadline_at`` and ``updated_at`` are timezone-aware UTC
      datetimes (AAP R-26 — RFC 3339).

    Example:
        >>> from datetime import datetime, timezone
        >>> from uuid import uuid4
        >>> s = SagaState(
        ...     order_id=uuid4(),
        ...     saga_id=uuid4(),
        ...     current_step=SagaStep.CREATE_ORDER,
        ...     correlation_id=uuid4(),
        ...     updated_at=datetime.now(timezone.utc),
        ...     version=1,
        ... )
        >>> s.retry_count
        0
        >>> s.compensation_required
        False
        >>> next_s = s.model_copy(
        ...     update={
        ...         "current_step": SagaStep.AWAIT_INVENTORY,
        ...         "awaiting_event": "inventory.reserved",
        ...         "version": s.version + 1,
        ...     }
        ... )
        >>> next_s.version
        2
        >>> next_s.current_step
        <SagaStep.AWAIT_INVENTORY: 'AWAIT_INVENTORY'>
    """

    model_config = ConfigDict(
        frozen=True,
        str_strip_whitespace=True,
        extra="forbid",
        populate_by_name=True,
        validate_assignment=True,
        arbitrary_types_allowed=False,
    )

    order_id: UUID = Field(
        ...,
        description="UUID of the orders row this saga coordinates.",
    )
    saga_id: UUID = Field(
        ...,
        description="Stable identifier for this specific saga instance.",
    )
    current_step: SagaStep = Field(
        ...,
        description="Current workflow stage per the SagaStep enum.",
    )
    awaiting_event: str | None = Field(
        default=None,
        max_length=128,
        description=(
            "Kafka topic name the saga is currently awaiting "
            "(e.g., 'inventory.reserved'). None when synchronous "
            "or terminal."
        ),
    )
    retry_count: int = Field(
        default=0,
        ge=0,
        description="How many times the current step has been retried.",
    )
    deadline_at: datetime | None = Field(
        default=None,
        description=(
            "UTC deadline after which the scheduler triggers "
            "timeout-based compensation (AAP R-20). None for "
            "terminal steps."
        ),
    )
    compensation_required: bool = Field(
        default=False,
        description=(
            "True when the saga is entering or has entered a "
            "compensating step."
        ),
    )
    last_error: str | None = Field(
        default=None,
        max_length=500,
        description="Human-readable last error message for diagnostics.",
    )
    correlation_id: UUID = Field(
        ...,
        description=(
            "Request-scoped correlation identifier propagated "
            "from the API Gateway (AAP R-13)."
        ),
    )
    updated_at: datetime = Field(
        ...,
        description="Last modification timestamp (UTC, RFC 3339).",
    )
    version: int = Field(
        ...,
        ge=1,
        description=(
            "Optimistic-lock counter; incremented on every transition."
        ),
    )

    # ----- Field validators ------------------------------------------

    @field_validator("updated_at")
    @classmethod
    def _updated_at_tzaware(cls, value: datetime) -> datetime:
        """Reject naive datetimes; canonicalize to UTC (AAP R-26).

        AAP R-26 mandates RFC 3339 timestamps for structured logs;
        the canonical RFC 3339 representation of a saga timestamp
        is UTC (``...Z`` or ``...+00:00``). This validator:

          1. REJECTS naive datetimes outright (``tzinfo is None``)
             — failing fast surfaces programmer errors at the point
             of construction rather than at the point of
             serialization.
          2. NORMALIZES tz-aware non-UTC datetimes to UTC via
             :meth:`datetime.astimezone`. This guarantees that any
             two :class:`SagaState` instances persisted with the
             "same" wall-clock instant compare equal regardless of
             their original timezone, simplifying downstream
             reconciliation.

        Args:
            value: Candidate ``updated_at`` value passed to the
                constructor or to ``model_copy(update=...)``.

        Returns:
            ``value`` converted to UTC if it carries a non-UTC
            ``tzinfo``; otherwise ``value`` unchanged.

        Raises:
            ValueError: ``value`` is naive (``tzinfo is None``).
        """
        if value.tzinfo is None:
            raise ValueError(
                "updated_at must be timezone-aware (UTC expected)"
            )
        return value.astimezone(timezone.utc)

    @field_validator("deadline_at")
    @classmethod
    def _deadline_at_tzaware(
        cls, value: datetime | None
    ) -> datetime | None:
        """Reject naive datetimes; canonicalize to UTC if set.

        Allows ``None`` (terminal sagas have no deadline) but, when
        a value IS supplied:

          1. REJECTS naive datetimes outright (AAP R-26).
          2. NORMALIZES tz-aware non-UTC datetimes to UTC, matching
             :meth:`_updated_at_tzaware`. The scheduler's
             ``WHERE deadline_at < now()`` polling query expects
             UTC; preserving a non-UTC offset would silently break
             the comparison if PostgreSQL's session timezone ever
             drifted.

        Args:
            value: Candidate ``deadline_at`` value or ``None``.

        Returns:
            ``None`` if ``value`` is ``None``; otherwise ``value``
            converted to UTC.

        Raises:
            ValueError: ``value`` is a non-``None`` naive datetime.
        """
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(
                "deadline_at must be timezone-aware (UTC expected)"
            )
        return value.astimezone(timezone.utc)

    @field_validator("awaiting_event")
    @classmethod
    def _validate_awaiting_event(cls, value: str | None) -> str | None:
        """Strip whitespace; reject empty-after-strip values.

        ``None`` is preserved (the saga is in a synchronous or
        terminal step and is not blocked on any event). A
        whitespace-only string is REJECTED — it is almost certainly
        a programmer error (e.g., a templating bug producing
        ``""`` instead of the actual topic name) and silently
        accepting it would leave the saga stuck because no Kafka
        message ever has a blank topic name.

        Args:
            value: Candidate ``awaiting_event`` value or ``None``.

        Returns:
            The stripped non-empty topic name, or ``None``.

        Raises:
            ValueError: ``value`` is a non-``None`` string that is
                empty after stripping whitespace.
        """
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError(
                "awaiting_event, when set, must be a non-empty topic name"
            )
        return stripped

    @field_validator("last_error", mode="before")
    @classmethod
    def _validate_last_error(cls, value: object) -> str | None:
        """Strip whitespace; normalize empty to None; truncate to 500 chars.

        Runs in ``mode="before"`` so it executes BEFORE Pydantic's
        built-in ``max_length=500`` constraint. This ordering is
        DELIBERATE: the spec requires that long error messages be
        SILENTLY TRUNCATED to 500 characters rather than rejected
        outright (callers raising the SagaState transition often
        embed full exception messages, which are unbounded). The
        ``max_length=500`` on the field then acts as a safety net
        that confirms the validator did its job.

        Diagnostic free text is intentionally lenient:

          * ``None`` stays ``None``.
          * Whitespace-only strings are normalized to ``None``
            (callers occasionally pass ``""`` from templated
            error-message construction; we don't want that to
            occupy a row column with no information).
          * Strings longer than 500 chars are truncated to 500
            chars; long stack traces belong in the ELK log stream
            (Filebeat → Logstash → Elasticsearch per AAP R-27),
            not in this column.
          * Non-string non-None inputs are returned unchanged so
            Pydantic's type coercion can produce the canonical
            ``ValidationError`` for "expected string".

        Args:
            value: Candidate ``last_error`` value (raw input,
                pre-coercion). Typed as :class:`object` because
                ``mode="before"`` validators receive whatever the
                caller supplied, before type coercion.

        Returns:
            ``None`` when the input is ``None`` or whitespace-only;
            otherwise the stripped error message truncated to 500
            chars.
        """
        if value is None:
            return None
        if not isinstance(value, str):
            # Pass non-strings through unchanged; the subsequent
            # str type-coercion step will produce the canonical
            # Pydantic ValidationError ("input should be a valid
            # string"). This keeps error messages consistent with
            # pure-Pydantic field declarations.
            return value  # type: ignore[return-value]
        stripped = value.strip()
        if not stripped:
            return None  # treat empty after strip as unset
        if len(stripped) > 500:
            return stripped[:500]
        return stripped


# ----- Module-level helpers ------------------------------------------

#: Maximum bound on per-step deadline computations to avoid integer
#: overflow when ``step_timeout_ms`` is misconfigured. 24 hours is
#: well beyond any reasonable saga step (the slowest realistic step
#: is the inventory-reservation await, which under normal load
#: completes in seconds and even under degraded conditions should
#: complete in minutes). Capping here catches misconfigurations
#: such as ``Settings.saga.step_timeout_ms = 2**63`` (resulting in
#: a deadline computed in the year 30000+) early at the point of
#: deadline construction rather than later when the resulting
#: Postgres TIMESTAMPTZ overflow would corrupt the row.
_MAX_STEP_TIMEOUT_MS: Final[int] = 24 * 60 * 60 * 1000


def is_terminal(state: SagaState) -> bool:
    """Return ``True`` if ``state`` is in the ``TERMINATED`` step.

    Convenience helper used by the scheduler and tests to skip rows
    that don't need any further processing. The scheduler's primary
    deadline-poll query already excludes terminated rows via the
    partial index ``idx_saga_state__deadline (deadline_at) WHERE
    current_step <> 'TERMINATED'``; this helper is used in narrower
    code paths AFTER a row has been materialized into a
    :class:`SagaState` (e.g., post-event-correlation paths, retry
    decisions, and unit tests).

    Args:
        state: A materialized :class:`SagaState` instance.

    Returns:
        ``True`` if and only if ``state.current_step`` equals
        :attr:`SagaStep.TERMINATED`.

    Example:
        >>> from datetime import datetime, timezone
        >>> from uuid import uuid4
        >>> s = SagaState(
        ...     order_id=uuid4(),
        ...     saga_id=uuid4(),
        ...     current_step=SagaStep.TERMINATED,
        ...     correlation_id=uuid4(),
        ...     updated_at=datetime.now(timezone.utc),
        ...     version=5,
        ... )
        >>> is_terminal(s)
        True
    """
    return state.current_step == SagaStep.TERMINATED


def next_deadline(now: datetime, step_timeout_ms: int) -> datetime:
    """Compute the ``deadline_at`` for the next saga step.

    Pure function used by the saga coordinator and scheduler to
    derive the deadline for a newly-entered step. The caller passes
    a ``now`` it has already determined (in production, from
    ``utils.time.utcnow()``; in tests, from ``freezegun`` or a
    fixed reference) and a per-step timeout in milliseconds.

    The function performs three defensive validations:

      1. ``now`` MUST be timezone-aware (AAP R-26). A naive
         ``now`` would silently produce a naive ``deadline_at``,
         which would in turn fail the :class:`SagaState` field
         validator at construction time. Failing here surfaces the
         problem at the call site rather than at the
         :class:`SagaState` construction site.
      2. ``step_timeout_ms`` MUST be ``>= 0``. Negative timeouts
         have no meaningful interpretation; the only reason to
         pass ``0`` is the fast-fail use case (see below).
      3. ``step_timeout_ms`` MUST be ``<= _MAX_STEP_TIMEOUT_MS``
         (24 hours). Defensive bound against
         :class:`Settings`-driven misconfiguration.

    The ``step_timeout_ms == 0`` case returns ``now`` exactly,
    which the scheduler interprets as "already overdue" on the next
    poll cycle. This is the fast-fail use case: a saga step that
    should compensate immediately upon receipt of a specific event
    can be created with a zero deadline so the scheduler picks it
    up on the very next poll.

    Args:
        now: Current UTC timestamp. MUST be timezone-aware. Tests
            typically pass a deterministic value via ``freezegun``
            or ``utils.time``.
        step_timeout_ms: Number of milliseconds until the deadline.
            MUST be ``>= 0`` and ``<= _MAX_STEP_TIMEOUT_MS`` (24
            hours, defensive bound).

    Returns:
        ``now + timedelta(milliseconds=step_timeout_ms)`` — a
        timezone-aware UTC datetime.

    Raises:
        ValueError: ``now`` is naive, OR ``step_timeout_ms`` is
            negative, OR ``step_timeout_ms`` exceeds the defensive
            24-hour bound.

    Example:
        >>> from datetime import datetime, timezone
        >>> n = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        >>> next_deadline(n, 10_000).isoformat()
        '2025-01-01T00:00:10+00:00'
        >>> next_deadline(n, 0) == n
        True
    """
    if now.tzinfo is None:
        raise ValueError(
            "next_deadline `now` must be timezone-aware (UTC expected)"
        )
    if step_timeout_ms < 0:
        raise ValueError(
            f"step_timeout_ms must be >= 0; got {step_timeout_ms}"
        )
    if step_timeout_ms > _MAX_STEP_TIMEOUT_MS:
        raise ValueError(
            f"step_timeout_ms {step_timeout_ms} exceeds maximum "
            f"{_MAX_STEP_TIMEOUT_MS} ms (24 hours)"
        )
    return now + timedelta(milliseconds=step_timeout_ms)


__all__ = [
    "SagaState",
    "SagaStep",
    "is_terminal",
    "next_deadline",
]

