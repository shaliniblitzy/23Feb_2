"""ML inference primitives for the Recommendation Engine.

This package hosts the model-serving runtime adapters and helpers used
to compute personalised recommendations.

Modules
-------
* ``src.inference.scorer``
    Lenient cosine-distance/similarity helpers that never raise on
    edge cases (e.g. zero vectors). Used by the inference path to
    rank candidate products against a query vector.

Public API
----------
This package marker re-exports nothing. Callers MUST import the
specific helper they need:

    from src.inference.scorer import cosine_similarity, distance_to_similarity

Keeping ``__all__`` empty preserves the explicit-import discipline
documented at the package root (see ``src/__init__.py``).

Compliance notes
----------------
- AAP R-15 / R-16 — Inference invocations are wrapped by retry +
  circuit-breaker primitives from :mod:`src.resilience` at the call
  site, not inside this package.
- AAP R-20 — A model miss / inference exception triggers the
  popularity-based fallback path documented in
  ``docs/architecture/resilience-patterns.md`` (created in CP4).
"""

from __future__ import annotations

# ``__all__`` is intentionally empty. This package marker exists solely so that
# ``services/recommendation-engine/src/inference/`` is recognised as a regular
# Python package and so that the explicit-import discipline above is preserved
# across the codebase.
__all__: list[str] = []
