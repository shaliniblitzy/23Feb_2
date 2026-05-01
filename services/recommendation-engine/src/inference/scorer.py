"""Pure math helpers that convert pgvector cosine distances to similarity scores.

This module is the **most foundational** unit of the ``src.inference`` package:
it has ZERO internal dependencies (not even on :mod:`src.domain`). It contains
deterministic, pure-function math that maps the ``[0, 2]`` cosine-distance range
returned by pgvector's ``<=>`` operator to a normalized ``[0, 1]`` similarity
score consumed by the recommendation runtime.

Why a separate module?
----------------------
:class:`src.domain.models.Neighbor` already exposes a strict
``Neighbor.from_distance`` classmethod that enforces the cosine invariant
(``similarity == 1 - distance``) and may raise on noisy inputs. That is the
right behavior when **constructing a canonical persisted record**. The helpers
in this module take the **opposite stance**: they are *lenient*, never raise,
and clamp out-of-range or non-finite inputs to safe sentinel values so a
single bad distance from a downstream system can never poison a top-K sort.

In short:

* :meth:`Neighbor.from_distance` — strict; for canonical record construction.
* :func:`distance_to_similarity` — lenient; for live scoring at the boundary.

Cosine-only semantics
---------------------
Per the inference-package folder spec and AAP Section 0.4.4 (pgvector +
``vector_cosine_ops`` HNSW index), cosine is the **only** supported metric.
For unit-normalized vectors, pgvector's ``<=>`` operator returns:

* ``0`` -> identical direction
* ``1`` -> orthogonal
* ``2`` -> diametrically opposite

We map that to a ``[0, 1]`` similarity using ``1 - distance`` and clamp.
There is intentionally no L2 helper here; if an L2 model is ever added it
will get its own function (``l2_distance_to_similarity``) with its own
unbounded-distance scaling (e.g., ``1 / (1 + d)`` or ``exp(-d)``) so the
two metrics remain easy to audit.

Compliance highlights
---------------------
- AAP R-19 — Fail-fast on startup: this module is **import-time pure** and
  performs no I/O, so importing it can never block startup or fail.
- AAP R-20 — Fallback declared for every dependency: the fail-safe
  ``NaN -> 0.0`` mapping IS the fallback for a corrupt distance value;
  the offending neighbor sinks to the bottom of the ranked output and is
  naturally excluded by the top-K cut-off.
- AAP R-26 — Structured logs: not applicable here; this file logs nothing
  because it's pure math and is called in a hot path.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
# ``__all__`` is alphabetized and lists ONLY the two public callables. The
# private ``_DIST_*`` / ``_SIM_*`` constants are intentionally excluded so
# ``from src.inference.scorer import *`` does not leak implementation detail.
__all__ = [
    "distance_to_similarity",
    "distances_to_similarities",
]


# ---------------------------------------------------------------------------
# Module-level clamp bounds
# ---------------------------------------------------------------------------
#: Minimum legitimate cosine distance value.
#:
#: For unit-normalized vectors the cosine distance ``1 - cos(theta)`` is
#: bounded below by ``0`` (identical direction). pgvector occasionally
#: returns slightly negative values (~ -1e-9) due to floating-point noise;
#: we clamp those up to ``_DIST_MIN`` so the resulting similarity never
#: exceeds ``_SIM_MAX``.
_DIST_MIN: float = 0.0

#: Maximum legitimate cosine distance value (diametrically opposite vectors).
#:
#: Distances above ``2.0`` are unphysical for unit-normalized inputs but may
#: occur if a caller forgets to normalize. We clamp rather than reject so a
#: single anomaly never aborts an entire ranking request.
_DIST_MAX: float = 2.0

#: Lower bound of the similarity output range. ``0.0`` represents "as
#: dissimilar as we are willing to express" — orthogonal vectors and any
#: input that produced a negative or non-finite raw similarity.
_SIM_MIN: float = 0.0

#: Upper bound of the similarity output range. ``1.0`` represents identical
#: direction. Anything mathematically larger (which can only arise from a
#: distance below ``_DIST_MIN``) is clamped down to keep the contract clean.
_SIM_MAX: float = 1.0


# ---------------------------------------------------------------------------
# Scalar conversion
# ---------------------------------------------------------------------------
def distance_to_similarity(distance: float) -> float:
    """Convert a single pgvector cosine distance to a similarity in ``[0, 1]``.

    pgvector's ``<=>`` cosine distance operator returns ``1 - cos(theta)``,
    which for unit-normalized vectors lies in ``[0, 2]``:

    * ``0`` -> identical direction
    * ``1`` -> orthogonal
    * ``2`` -> diametrically opposite

    We map that to ``[0, 1]`` similarity using ``1 - distance`` and clamp any
    out-of-range input to the interval bounds. ``NaN`` / ``+/-Inf`` inputs
    degrade to ``0.0`` (fail-safe) so they never poison a downstream sort
    by floating to the top of the results.

    The function is **deterministic** (same input -> same output, every run,
    every platform) and **never raises** — these properties are critical
    because the function sits in a hot path and is called once per neighbor
    on every recommendation request.

    Args:
        distance: A cosine distance returned by pgvector. ``int`` is also
            accepted and silently coerced to ``float``. Non-finite values
            (``NaN``, ``+/-Inf``) and values outside ``[0, 2]`` are
            tolerated.

    Returns:
        A similarity score in ``[0, 1]``, where ``1.0`` means identical
        direction and ``0.0`` means orthogonal or worse. Always a built-in
        Python :class:`float` (never :class:`numpy.float64`).
    """
    # Coerce ``int``/``numpy`` scalars to a built-in float so the return
    # type is always a plain Python float. ``float(numpy.float64(x))``
    # round-trips exactly for any value representable in IEEE-754 double.
    d = float(distance)

    # Fail-safe path for ``NaN`` / ``+/-Inf``. We cannot meaningfully
    # compute a similarity from a non-finite distance, so we return the
    # most-pessimistic legitimate similarity (``_SIM_MIN``). This guarantees
    # the offending neighbor sinks to the bottom of any rank-by-similarity
    # sort and is excluded from the top-K cut-off without raising.
    if not math.isfinite(d):
        return _SIM_MIN

    # Clamp the distance into ``[_DIST_MIN, _DIST_MAX]`` BEFORE the
    # subtraction. This ordering makes the algorithm easier to reason about
    # when debugging ("the distance was clamped to 0, which mapped to a
    # similarity of 1.0") and keeps slight negative noise from the database
    # (e.g., ``-1e-12``) from producing a similarity > 1.
    if d < _DIST_MIN:
        d = _DIST_MIN
    elif d > _DIST_MAX:
        d = _DIST_MAX

    # Map distance in ``[0, 2]`` to a raw similarity in ``[-1, 1]``. Negative
    # similarities are still valid mathematically (they correspond to
    # cos(theta) < 0, i.e., obtuse angles) but the public contract of this
    # function caps similarity at ``_SIM_MIN = 0.0`` so callers can treat
    # the output as a unit-interval probability-like score without special-
    # casing the negative range.
    sim = _SIM_MAX - d

    # Final clamp into ``[_SIM_MIN, _SIM_MAX]``. After the distance was
    # already clamped above, this branch can only fire for ``d > 1.0``
    # (giving negative ``sim``) — the extra check costs effectively nothing
    # and is kept for defense-in-depth so that future refactors of either
    # bound cannot silently produce out-of-range output.
    if sim < _SIM_MIN:
        return _SIM_MIN
    if sim > _SIM_MAX:
        return _SIM_MAX
    return sim


# ---------------------------------------------------------------------------
# Vectorized conversion
# ---------------------------------------------------------------------------
def distances_to_similarities(
    distances: Sequence[float] | np.ndarray,
) -> np.ndarray:
    """Vectorized variant of :func:`distance_to_similarity`.

    Accepts any 1-D numeric sequence (``list``, ``tuple``, ``np.ndarray``,
    etc.) and returns a numpy ``float64`` array of similarities of the
    same length. Useful for batch consumers (e.g., a backfill or warm-up
    script) that already hold a numpy array of distances and want to avoid
    a Python-level loop.

    Numerical equivalence with the scalar variant is guaranteed within
    IEEE-754 double precision (``< 1e-12`` absolute tolerance) for every
    input value; the unit-test suite asserts equivalence over a grid of
    representative distances.

    Args:
        distances: 1-D numeric sequence or numpy array of cosine distances.
            Empty inputs are accepted and produce an empty output array.
            ``NaN`` / ``+/-Inf`` entries degrade to ``0.0`` similarity.

    Returns:
        ``np.ndarray`` of dtype :class:`numpy.float64` with similarities
        clamped to ``[0, 1]``. Shape equals ``np.asarray(distances).shape``;
        for the supported 1-D input contract that means ``(len(distances),)``.
    """
    # Coerce to a contiguous float64 array. ``np.asarray`` is a no-op when
    # the input is already ``np.ndarray[float64]`` and avoids an unnecessary
    # copy. ``dtype=np.float64`` standardises the output regardless of the
    # input dtype (lists, ``int`` arrays, ``float32`` arrays, etc.).
    arr = np.asarray(distances, dtype=np.float64)

    # Replace non-finite entries (``NaN``, ``+/-Inf``) with ``_DIST_MAX``.
    # After the upcoming ``_SIM_MAX - arr`` step that yields a similarity of
    # ``-1.0``, which the final ``np.clip`` then bounds to ``_SIM_MIN``.
    # Net effect: non-finite distances map to similarity ``0.0``, identical
    # to the scalar fail-safe path.
    arr = np.where(np.isfinite(arr), arr, _DIST_MAX)

    # Clamp the (now-finite) distances into ``[_DIST_MIN, _DIST_MAX]`` so
    # negative noise cannot lift similarity above ``_SIM_MAX`` and absurd
    # large values cannot push it below the lower bound after subtraction.
    arr = np.clip(arr, _DIST_MIN, _DIST_MAX)

    # Map distance -> similarity. Note that this raw similarity may still
    # span ``[-1, 1]`` before the final clamp.
    sims = _SIM_MAX - arr

    # Final clamp into ``[_SIM_MIN, _SIM_MAX]`` to enforce the public
    # contract. ``np.clip`` returns a fresh ``ndarray``, so callers may
    # safely mutate the result without aliasing the input.
    sims = np.clip(sims, _SIM_MIN, _SIM_MAX)

    return sims
