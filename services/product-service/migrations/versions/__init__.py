"""Migration scripts for the Product Service's MongoDB database.

This package contains the **ordered, forward-only, idempotent** migration
modules that ``../runner.py`` discovers and applies on every service
container start (or via a dedicated K8s migration Job).

Package contents
----------------

The only files in this package that are loaded as migrations are those
whose filenames match the runner's regex::

    ^(\\d{4,})_([a-z0-9_]+)\\.py$

This means ``__init__.py`` (this file) is **silently skipped** by the
runner's discovery loop and exists purely as the standard Python
package marker so that ``migrations.versions`` is recognisable as a
regular package by import tools, IDEs, type checkers, and packaging
machinery.

Module loading mechanism
------------------------

Migration modules are loaded individually via ``importlib.util``::

    spec = importlib.util.spec_from_file_location(
        name,
        path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

This sidesteps the fact that Python identifier syntax forbids names
starting with digits, so a literal ``from migrations.versions import
0001_create_collections_and_indexes`` is a SyntaxError. Tests that need
to load a specific migration module use the same ``importlib.util``
mechanism the runner uses.

Migration module contract
-------------------------

Every file in this package whose filename matches the runner regex MUST
expose a single top-level callable::

    def apply(
        database: pymongo.database.Database,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        ...

That callable MUST be:

* **Idempotent** -- running twice has no additional effect.
* **Forward-only** -- no ``downgrade()`` is provided; corrections are
  authored as new migrations.
* **Self-contained** -- no imports from ``services/product-service/src/``
  (the runtime application package). Migrations may import only from
  the Python standard library, ``pymongo``, and other public packages
  declared in ``../../requirements.txt``.

This ``__init__.py`` deliberately exposes **no public API** -- it is a
package marker only. Importing this module has no side effects and
performs no I/O.

References
----------

* Parent folder ``runner.py``: ``services/product-service/migrations/runner.py``
* AAP Section 0.5.2.5 -- per-service migrations template
* AAP Section 0.6.1   -- in-scope: ``services/*/migrations/**/*``
* AAP R-7             -- MongoDB chosen for flexible product schema
* AAP R-9             -- migrations under owning service, auto-applied
"""
