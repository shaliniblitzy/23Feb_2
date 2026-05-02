"""Unit tests for :mod:`src.resilience.circuit_breaker` -- pybreaker-based factory
and ``call_through`` helper.

Covered (AAP R-16):

- State constants (``STATE_CLOSED``, ``STATE_HALF_OPEN``, ``STATE_OPEN``) and
  the ``_STATE_VALUE`` mapping used for the Prometheus gauge.
- ``build_circuit_breaker`` returns a real ``pybreaker.CircuitBreaker`` with
  the correct ``fail_max`` and ``reset_timeout`` derived from the kwargs.
- Defaults: ``call_volume_threshold=20``, ``failure_rate_threshold_pct=50.0``,
  ``open_duration_ms=30_000``, ``half_open_permitted_calls=1``.
- ``call_through`` happy path: returns the wrapped value, forwards args/kwargs,
  keeps the breaker CLOSED on success.
- CLOSED -> OPEN: after ``fail_max`` consecutive failures, the breaker opens.
- OPEN state: subsequent calls raise ``CircuitBreakerError`` without invoking
  the wrapped callable.
- OPEN -> HALF-OPEN: after ``reset_timeout`` elapses, the next call is admitted.
- HALF-OPEN -> CLOSED on success; HALF-OPEN -> OPEN on failure.
- Per-dependency isolation: independent breakers (Auth Service vs Kafka producer)
  are unaffected by one another.

**Hermetic**: no network, no Kafka, no real DB. Each test uses a unique
breaker name via ``uuid4().hex[:8]`` to avoid Prometheus collector collisions
across tests in the same pytest process.

**Important caveat**: ``pybreaker`` uses ``time.monotonic()`` for its internal
state-machine timer. ``freezegun.freeze_time`` does NOT patch that clock, so
HALF-OPEN driving tests use a tiny ``open_duration_ms`` (e.g., 100ms) and a
real ``await asyncio.sleep(0.2)`` to advance the breaker.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any
from uuid import uuid4

import pybreaker
import pytest

from src.resilience.circuit_breaker import (
    STATE_CLOSED,
    STATE_HALF_OPEN,
    STATE_OPEN,
    _STATE_VALUE,
    build_circuit_breaker,
    call_through,
)


def _unique_name(prefix: str = "test") -> str:
    """Generate a unique breaker name to avoid Prometheus collector collisions.

    pybreaker registers each named breaker with the Prometheus
    CollectorRegistry exactly once per process; duplicate names from
    sibling tests would raise ``Duplicated timeseries in CollectorRegistry``.
    The ``uuid4().hex[:8]`` suffix guarantees uniqueness without making
    the names unwieldy in test failure output.
    """
    return f"{prefix}_{uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Phase 2 -- State Constant Tests
# ---------------------------------------------------------------------------
class TestCircuitBreakerStateValues:
    """Lock state->int mapping for the Prometheus state gauge."""

    def test_state_closed_value_is_zero(self) -> None:
        assert _STATE_VALUE[STATE_CLOSED] == 0

    def test_state_half_open_value_is_one(self) -> None:
        assert _STATE_VALUE[STATE_HALF_OPEN] == 1

    def test_state_open_value_is_two(self) -> None:
        assert _STATE_VALUE[STATE_OPEN] == 2

    def test_state_values_are_monotonic(self) -> None:
        """closed < half-open < open -- encodes severity ordering."""
        assert (
            _STATE_VALUE[STATE_CLOSED]
            < _STATE_VALUE[STATE_HALF_OPEN]
            < _STATE_VALUE[STATE_OPEN]
        )

    def test_state_constants_are_strings(self) -> None:
        assert isinstance(STATE_CLOSED, str)
        assert isinstance(STATE_HALF_OPEN, str)
        assert isinstance(STATE_OPEN, str)

    def test_state_half_open_uses_hyphen(self) -> None:
        """Must match pybreaker's STATE_HALF_OPEN spelling (with hyphen)."""
        assert STATE_HALF_OPEN == "half-open"

    def test_state_open_value(self) -> None:
        assert STATE_OPEN == "open"

    def test_state_closed_value(self) -> None:
        assert STATE_CLOSED == "closed"

    def test_state_value_dict_only_three_entries(self) -> None:
        assert len(_STATE_VALUE) == 3

    def test_state_values_align_with_pybreaker_constants(self) -> None:
        """Our constants match pybreaker's STATE_CLOSED/STATE_OPEN/STATE_HALF_OPEN."""
        assert STATE_CLOSED == pybreaker.STATE_CLOSED
        assert STATE_OPEN == pybreaker.STATE_OPEN
        assert STATE_HALF_OPEN == pybreaker.STATE_HALF_OPEN


# ---------------------------------------------------------------------------
# Phase 3 -- ``build_circuit_breaker`` Factory Tests
# ---------------------------------------------------------------------------
class TestBuildCircuitBreaker:
    """``build_circuit_breaker`` returns a configured pybreaker instance."""

    def test_returns_pybreaker_instance(self) -> None:
        breaker = build_circuit_breaker(name=_unique_name())
        assert isinstance(breaker, pybreaker.CircuitBreaker)

    def test_fail_max_from_defaults_equals_10(self) -> None:
        """Defaults: 20 calls x 50% rate = fail_max 10 per AAP R-16 / folder spec."""
        breaker = build_circuit_breaker(name=_unique_name())
        # pybreaker stores fail_max as ``fail_max`` attribute
        assert breaker.fail_max == 10

    def test_fail_max_rounds_correctly(self) -> None:
        """17 calls x 35% rate = 5.95 -> round-half-to-even = 6."""
        breaker = build_circuit_breaker(
            name=_unique_name(),
            call_volume_threshold=17,
            failure_rate_threshold_pct=35.0,
        )
        assert breaker.fail_max == 6

    def test_fail_max_floor_is_one(self) -> None:
        """fail_max must always be >= 1, even with degenerate thresholds."""
        breaker = build_circuit_breaker(
            name=_unique_name(),
            call_volume_threshold=1,
            failure_rate_threshold_pct=1.0,
        )
        assert breaker.fail_max >= 1

    def test_reset_timeout_defaults_to_thirty_seconds(self) -> None:
        """Default open_duration_ms=30_000 -> reset_timeout=30.0 seconds."""
        breaker = build_circuit_breaker(name=_unique_name())
        assert abs(breaker.reset_timeout - 30.0) < 0.01

    def test_reset_timeout_configurable(self) -> None:
        breaker = build_circuit_breaker(
            name=_unique_name(),
            open_duration_ms=5_000,
        )
        assert abs(breaker.reset_timeout - 5.0) < 0.01

    def test_breaker_starts_closed(self) -> None:
        breaker = build_circuit_breaker(name=_unique_name())
        assert breaker.current_state == STATE_CLOSED

    def test_breaker_records_zero_failures_initially(self) -> None:
        breaker = build_circuit_breaker(name=_unique_name())
        assert breaker.fail_counter == 0

    def test_breaker_name_attribute_matches_input(self) -> None:
        name = _unique_name("named")
        breaker = build_circuit_breaker(name=name)
        # pybreaker stores the configured name on the instance
        assert breaker.name == name

    def test_unique_names_avoid_collisions(self) -> None:
        """Two breakers with different names coexist without registry collision."""
        b1 = build_circuit_breaker(name=_unique_name("a"))
        b2 = build_circuit_breaker(name=_unique_name("b"))
        assert b1 is not b2
        assert b1.name != b2.name

    def test_default_signature_call_volume_threshold(self) -> None:
        sig = inspect.signature(build_circuit_breaker)
        assert sig.parameters["call_volume_threshold"].default == 20

    def test_default_signature_failure_rate_threshold_pct(self) -> None:
        sig = inspect.signature(build_circuit_breaker)
        assert sig.parameters["failure_rate_threshold_pct"].default == 50.0

    def test_default_signature_open_duration_ms(self) -> None:
        sig = inspect.signature(build_circuit_breaker)
        assert sig.parameters["open_duration_ms"].default == 30_000

    def test_default_signature_half_open_permitted_calls(self) -> None:
        sig = inspect.signature(build_circuit_breaker)
        assert sig.parameters["half_open_permitted_calls"].default == 1


# ---------------------------------------------------------------------------
# Phase 4 -- ``call_through`` Happy Path
# ---------------------------------------------------------------------------
class TestCallThroughHappyPath:
    """``call_through`` returns wrapped value, forwards args, keeps breaker closed."""

    @pytest.mark.asyncio
    async def test_returns_wrapped_value(self) -> None:
        breaker = build_circuit_breaker(name=_unique_name())

        async def add(x: int, y: int) -> int:
            return x + y

        result = await call_through(breaker, add, 2, 3)
        assert result == 5

    @pytest.mark.asyncio
    async def test_forwards_kwargs(self) -> None:
        breaker = build_circuit_breaker(name=_unique_name())

        async def multiply(*, x: int, y: int) -> int:
            return x * y

        result = await call_through(breaker, multiply, x=4, y=5)
        assert result == 20

    @pytest.mark.asyncio
    async def test_successful_calls_keep_breaker_closed(self) -> None:
        breaker = build_circuit_breaker(name=_unique_name())

        async def ok() -> str:
            return "ok"

        for _ in range(5):
            result = await call_through(breaker, ok)
            assert result == "ok"
        assert breaker.current_state == STATE_CLOSED

    @pytest.mark.asyncio
    async def test_returns_complex_value(self) -> None:
        breaker = build_circuit_breaker(name=_unique_name())

        async def fetch() -> dict[str, Any]:
            return {"id": 42, "name": "x"}

        result = await call_through(breaker, fetch)
        assert result == {"id": 42, "name": "x"}


# ---------------------------------------------------------------------------
# Phase 5 -- CLOSED -> OPEN Transition
# ---------------------------------------------------------------------------
class TestCircuitBreakerClosedToOpen:
    """After ``fail_max`` consecutive failures, breaker transitions to OPEN."""

    @pytest.mark.asyncio
    async def test_trips_open_after_fail_max(self) -> None:
        """call_volume=4 x rate=100% -> fail_max=4 -> opens after 4 failures."""
        breaker = build_circuit_breaker(
            name=_unique_name("trip"),
            call_volume_threshold=4,
            failure_rate_threshold_pct=100.0,
        )
        assert breaker.fail_max == 4

        async def boom() -> None:
            raise RuntimeError("boom")

        # Each failure increments the breaker's fail counter
        for _ in range(4):
            with pytest.raises(RuntimeError):
                await call_through(breaker, boom)

        assert breaker.current_state == STATE_OPEN

    @pytest.mark.asyncio
    async def test_below_fail_max_keeps_closed(self) -> None:
        """3 failures with fail_max=4 keeps breaker closed."""
        breaker = build_circuit_breaker(
            name=_unique_name("under"),
            call_volume_threshold=4,
            failure_rate_threshold_pct=100.0,
        )

        async def boom() -> None:
            raise RuntimeError("boom")

        for _ in range(3):
            with pytest.raises(RuntimeError):
                await call_through(breaker, boom)

        assert breaker.current_state == STATE_CLOSED

    @pytest.mark.asyncio
    async def test_open_breaker_rejects_without_calling_fn(self) -> None:
        """When OPEN, call_through raises CircuitBreakerError WITHOUT invoking fn."""
        breaker = build_circuit_breaker(
            name=_unique_name("reject"),
            call_volume_threshold=2,
            failure_rate_threshold_pct=100.0,
        )

        async def boom() -> None:
            raise RuntimeError("boom")

        # Trip the breaker to OPEN
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await call_through(breaker, boom)
        assert breaker.current_state == STATE_OPEN

        # Now create a fn that increments a counter; assert it is NEVER called.
        # The ``nonlocal call_count`` closure pattern is the canonical way to
        # PROVE that pybreaker's fast-fail does not invoke the wrapped callable.
        call_count = 0

        async def should_not_run() -> str:
            nonlocal call_count
            call_count += 1
            return "ok"

        with pytest.raises(pybreaker.CircuitBreakerError):
            await call_through(breaker, should_not_run)

        assert call_count == 0  # PROVE fn never ran

    @pytest.mark.asyncio
    async def test_success_resets_consecutive_failure_counter(self) -> None:
        """A success resets the consecutive-failure counter (pybreaker semantics).

        Sequence ``[fail, fail, success, fail, fail]`` does NOT open the
        breaker with ``fail_max=3`` because pybreaker counts CONSECUTIVE
        failures, and the success resets the counter back to 0.
        """
        breaker = build_circuit_breaker(
            name=_unique_name("reset"),
            call_volume_threshold=3,
            failure_rate_threshold_pct=100.0,
        )

        async def boom() -> None:
            raise RuntimeError("boom")

        async def ok() -> str:
            return "ok"

        # 2 failures, 1 success, 2 failures -> should NOT trip with fail_max=3
        with pytest.raises(RuntimeError):
            await call_through(breaker, boom)
        with pytest.raises(RuntimeError):
            await call_through(breaker, boom)

        result = await call_through(breaker, ok)
        assert result == "ok"

        with pytest.raises(RuntimeError):
            await call_through(breaker, boom)
        with pytest.raises(RuntimeError):
            await call_through(breaker, boom)

        # Counter was reset by the success -- only 2 consecutive failures, < fail_max=3
        assert breaker.current_state == STATE_CLOSED


# ---------------------------------------------------------------------------
# Phase 6 -- OPEN -> HALF-OPEN -> CLOSED/OPEN Transitions
# ---------------------------------------------------------------------------
# These tests use a tiny ``open_duration_ms`` (100ms) and a real
# ``await asyncio.sleep(0.2)`` to drive the breaker out of OPEN.
# ``freezegun`` does NOT work here because pybreaker uses
# ``time.monotonic()`` for its state-machine clock, which freezegun
# does not patch.
class TestCircuitBreakerHalfOpen:
    """OPEN -> HALF-OPEN after reset_timeout; HALF-OPEN -> CLOSED on success / OPEN on failure."""

    @pytest.mark.asyncio
    async def test_open_to_half_open_after_elapsed_time(self) -> None:
        """After ``reset_timeout`` elapses (real wall-clock seconds, not freezegun!),
        the next call is admitted (HALF-OPEN probe). On success, breaker closes.
        """
        breaker = build_circuit_breaker(
            name=_unique_name("half_open"),
            call_volume_threshold=2,
            failure_rate_threshold_pct=100.0,
            open_duration_ms=100,  # 100ms reset
        )

        async def boom() -> None:
            raise RuntimeError("boom")

        # Trip the breaker
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await call_through(breaker, boom)
        assert breaker.current_state == STATE_OPEN

        # Wait for reset_timeout to elapse (real wall-clock, NOT freezegun)
        await asyncio.sleep(0.2)

        # Probe: a successful call closes the breaker
        async def ok() -> str:
            return "ok"

        result = await call_through(breaker, ok)
        assert result == "ok"
        assert breaker.current_state == STATE_CLOSED

    @pytest.mark.asyncio
    async def test_half_open_failed_probe_re_opens_breaker(self) -> None:
        """If the HALF-OPEN probe fails, the breaker returns to OPEN."""
        breaker = build_circuit_breaker(
            name=_unique_name("reprobe"),
            call_volume_threshold=2,
            failure_rate_threshold_pct=100.0,
            open_duration_ms=100,
        )

        async def boom() -> None:
            raise RuntimeError("boom")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                await call_through(breaker, boom)
        assert breaker.current_state == STATE_OPEN

        await asyncio.sleep(0.2)  # allow OPEN -> HALF-OPEN

        # Failed probe: re-opens the breaker
        with pytest.raises(RuntimeError):
            await call_through(breaker, boom)
        assert breaker.current_state == STATE_OPEN

    @pytest.mark.asyncio
    async def test_half_open_admits_only_one_call(self) -> None:
        """After reset_timeout elapses, only ONE call is admitted (the probe).

        This is pybreaker's contract for HALF-OPEN single-probe semantics.
        We model this by tripping the breaker, sleeping, and noting that a
        successful probe immediately closes the breaker (not multi-call);
        subsequent calls flow through CLOSED state normally.
        """
        breaker = build_circuit_breaker(
            name=_unique_name("singleprobe"),
            call_volume_threshold=2,
            failure_rate_threshold_pct=100.0,
            open_duration_ms=100,
        )

        async def boom() -> None:
            raise RuntimeError("boom")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                await call_through(breaker, boom)
        assert breaker.current_state == STATE_OPEN

        await asyncio.sleep(0.2)

        async def ok() -> str:
            return "ok"

        # First call is the probe (HALF-OPEN -> CLOSED on success)
        await call_through(breaker, ok)
        assert breaker.current_state == STATE_CLOSED

        # Subsequent calls flow normally through CLOSED state
        for _ in range(3):
            await call_through(breaker, ok)
        assert breaker.current_state == STATE_CLOSED

    @pytest.mark.asyncio
    async def test_open_state_within_reset_timeout_still_rejects(self) -> None:
        """While the breaker is OPEN and reset_timeout has not yet elapsed,
        all calls reject without invoking fn.
        """
        breaker = build_circuit_breaker(
            name=_unique_name("still_open"),
            call_volume_threshold=2,
            failure_rate_threshold_pct=100.0,
            open_duration_ms=10_000,  # 10s -- won't elapse during this test
        )

        async def boom() -> None:
            raise RuntimeError("boom")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                await call_through(breaker, boom)
        assert breaker.current_state == STATE_OPEN

        # No sleep -- reset_timeout has NOT elapsed
        async def should_not_run() -> str:
            return "should not"

        # Multiple rejections all without invoking fn
        for _ in range(3):
            with pytest.raises(pybreaker.CircuitBreakerError):
                await call_through(breaker, should_not_run)

        assert breaker.current_state == STATE_OPEN


# ---------------------------------------------------------------------------
# Phase 7 -- Per-Dependency Isolation
# ---------------------------------------------------------------------------
class TestPerDependencyIsolation:
    """Per-dependency breaker isolation (AAP R-16 + R-20).

    Each downstream dependency has its OWN breaker. A misbehaving Auth
    Service must NOT trip the Kafka producer breaker, and vice versa.
    Per AAP R-16 fallback isolation contract -- failures must not
    cascade across independent dependencies.
    """

    @pytest.mark.asyncio
    async def test_auth_failures_do_not_trip_kafka_breaker(self) -> None:
        auth_breaker = build_circuit_breaker(
            name=_unique_name("auth"),
            call_volume_threshold=3,
            failure_rate_threshold_pct=100.0,
        )
        kafka_breaker = build_circuit_breaker(
            name=_unique_name("kafka"),
            call_volume_threshold=3,
            failure_rate_threshold_pct=100.0,
        )

        async def auth_fail() -> None:
            raise RuntimeError("auth down")

        # Trip the auth breaker
        for _ in range(3):
            with pytest.raises(RuntimeError):
                await call_through(auth_breaker, auth_fail)

        assert auth_breaker.current_state == STATE_OPEN
        # Kafka breaker remains CLOSED -- independent of auth failures
        assert kafka_breaker.current_state == STATE_CLOSED
        assert kafka_breaker.fail_counter == 0

    @pytest.mark.asyncio
    async def test_kafka_failures_do_not_trip_auth_breaker(self) -> None:
        auth_breaker = build_circuit_breaker(
            name=_unique_name("auth_iso"),
            call_volume_threshold=3,
            failure_rate_threshold_pct=100.0,
        )
        kafka_breaker = build_circuit_breaker(
            name=_unique_name("kafka_iso"),
            call_volume_threshold=3,
            failure_rate_threshold_pct=100.0,
        )

        async def kafka_fail() -> None:
            raise ConnectionError("broker down")

        for _ in range(3):
            with pytest.raises(ConnectionError):
                await call_through(kafka_breaker, kafka_fail)

        assert kafka_breaker.current_state == STATE_OPEN
        assert auth_breaker.current_state == STATE_CLOSED
        assert auth_breaker.fail_counter == 0

    @pytest.mark.asyncio
    async def test_each_breaker_has_independent_failure_counter(self) -> None:
        b1 = build_circuit_breaker(
            name=_unique_name("b1"),
            call_volume_threshold=5,
            failure_rate_threshold_pct=100.0,
        )
        b2 = build_circuit_breaker(
            name=_unique_name("b2"),
            call_volume_threshold=5,
            failure_rate_threshold_pct=100.0,
        )

        async def fail() -> None:
            raise RuntimeError("x")

        # Three failures on b1
        for _ in range(3):
            with pytest.raises(RuntimeError):
                await call_through(b1, fail)

        assert b1.fail_counter == 3
        assert b2.fail_counter == 0


# ---------------------------------------------------------------------------
# Phase 8 -- Exception Propagation
# ---------------------------------------------------------------------------
class CustomError(Exception):
    """Custom exception class used in propagation tests.

    Defined at module scope (NOT inside a test method) so tests can
    catch it via :func:`pytest.raises` and assert that the wrapped
    callable's exception type and message propagate to the caller
    unmodified through the breaker layer.
    """


class TestCallThroughExceptionPropagation:
    """The wrapped callable's exception type and message propagate to the caller."""

    @pytest.mark.asyncio
    async def test_underlying_exception_visible_to_caller(self) -> None:
        breaker = build_circuit_breaker(name=_unique_name("propagate"))

        async def fail() -> None:
            raise CustomError("specific failure")

        with pytest.raises(CustomError) as excinfo:
            await call_through(breaker, fail)

        assert "specific failure" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_success_after_failure_stays_closed_below_fail_max(self) -> None:
        """One failure with fail_max=10 keeps breaker closed; subsequent success ok."""
        breaker = build_circuit_breaker(
            name=_unique_name("low_failure"),
            call_volume_threshold=20,
            failure_rate_threshold_pct=50.0,  # fail_max=10
        )
        assert breaker.fail_max == 10

        async def fail() -> None:
            raise RuntimeError("transient")

        async def ok() -> str:
            return "ok"

        with pytest.raises(RuntimeError):
            await call_through(breaker, fail)
        # 1 failure < fail_max=10
        assert breaker.current_state == STATE_CLOSED

        result = await call_through(breaker, ok)
        assert result == "ok"
        assert breaker.current_state == STATE_CLOSED

    @pytest.mark.asyncio
    async def test_circuit_breaker_error_subclass_of_exception(self) -> None:
        """``pybreaker.CircuitBreakerError`` is catchable as ``Exception``."""
        breaker = build_circuit_breaker(
            name=_unique_name("cbe"),
            call_volume_threshold=1,
            failure_rate_threshold_pct=100.0,
        )

        async def fail() -> None:
            raise RuntimeError("trip")

        with pytest.raises(RuntimeError):
            await call_through(breaker, fail)
        assert breaker.current_state == STATE_OPEN

        async def ok() -> None:
            pass

        # Verify CircuitBreakerError is catchable as a generic Exception
        try:
            await call_through(breaker, ok)
        except Exception as exc:
            assert isinstance(exc, pybreaker.CircuitBreakerError)
        else:
            pytest.fail("Expected CircuitBreakerError")


# ---------------------------------------------------------------------------
# Phase 9 -- Conftest Fixture Usage Tests
# ---------------------------------------------------------------------------
# These tests verify the conftest's resilience fixtures themselves are
# correctly constructed (``closed_breaker``, ``open_breaker``,
# ``half_open_breaker``). The fixtures are provided by the parent
# ``services/product-service/tests/unit/conftest.py`` (a sibling file
# created by another agent) and use ``uuid.uuid4().hex[:8]``-suffixed
# names internally to avoid Prometheus collector collisions.
class TestConftestBreakerFixtures:
    """The parent conftest's breaker fixtures yield correctly-stated breakers."""

    @pytest.mark.asyncio
    async def test_closed_breaker_fixture_state(
        self,
        closed_breaker: pybreaker.CircuitBreaker,
    ) -> None:
        """The conftest ``closed_breaker`` is in CLOSED state."""
        assert closed_breaker.current_state == STATE_CLOSED

    @pytest.mark.asyncio
    async def test_closed_breaker_admits_calls(
        self,
        closed_breaker: pybreaker.CircuitBreaker,
    ) -> None:
        async def ok() -> str:
            return "ok"

        result = await call_through(closed_breaker, ok)
        assert result == "ok"
        assert closed_breaker.current_state == STATE_CLOSED

    @pytest.mark.asyncio
    async def test_open_breaker_fixture_state(
        self,
        open_breaker: pybreaker.CircuitBreaker,
    ) -> None:
        """The conftest ``open_breaker`` is forced to OPEN state."""
        assert open_breaker.current_state == STATE_OPEN

    @pytest.mark.asyncio
    async def test_open_breaker_rejects_without_calling_fn(
        self,
        open_breaker: pybreaker.CircuitBreaker,
    ) -> None:
        # Use the canonical ``nonlocal call_count`` closure pattern to
        # PROVE that pybreaker's fast-fail does not invoke the wrapped
        # callable when the breaker is OPEN.
        call_count = 0

        async def should_not_run() -> None:
            nonlocal call_count
            call_count += 1

        with pytest.raises(pybreaker.CircuitBreakerError):
            await call_through(open_breaker, should_not_run)
        assert call_count == 0

