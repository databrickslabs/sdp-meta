"""Backward-compatibility ``src.*`` re-export package for v0.0.10 customers.

What this is for
----------------
v0.0.10 of dlt-meta shipped its source tree as a ``src/`` package
(``src/dataflow_pipeline.py``, ``src/cli.py``, etc.). Customer runner
notebooks therefore import as::

    from src.dataflow_pipeline import DataflowPipeline

The v0.1.0 package rename to ``databricks.labs.sdp_meta`` would
normally break those notebooks. This package preserves them: it ships
inside the v0.1.0 main wheel as a top-level ``src`` namespace so the
legacy import keeps resolving to the same class objects.

Why a real package, not a ``.pth`` trick
----------------------------------------
The earlier iteration of this surface relied on a ``dlt_meta.pth``
file dropped at the wheel's purelib root, exec'd by CPython's
``site.py`` at interpreter startup. That worked on a normal
``pip install`` followed by a fresh ``python`` process, but NOT on
serverless DLT: the platform's ``%pip install`` notebook magic lays
the wheel's files into site-packages and runs the next cell in the
SAME interpreter without re-firing ``site.py``'s ``.pth`` scan, so the
freshly-installed ``.pth`` is silently ignored and ``src.*`` aliases
are never registered. Resolving through a real Python package -- this
file -- sidesteps the entire ``.pth`` lifecycle: ``from src.X import …``
just walks the standard import machinery, which locates ``src/`` in
site-packages and runs this ``__init__.py``.

Single source of truth
-----------------------
This module deliberately contains **no** alias-registration logic of
its own. All of it -- the canonical ``src.<sub>`` ->
``databricks.labs.sdp_meta.<sub>`` submodule list, the per-alias
``DeprecationWarning``, the ``SDP_META_DISABLE_SRC_ALIAS`` opt-out, and
(critically) the actionable stub modules that raise a clear "requires
the Lakeflow SDP runtime" error when ``pyspark.pipelines`` is missing
-- lives in :mod:`dlt_meta`. Importing it below runs that registration
as an import side effect.

This used to be a second, independent implementation. The two drifted:
``compat/src`` carried a shorter submodule list and silently *skipped*
modules whose canonical target failed to import, so on a non-SDP
runtime a customer's ``from src.dataflow_pipeline import …`` raised the
confusing ``ModuleNotFoundError: No module named 'src.dataflow_pipeline'``
-- exactly the failure mode the shim is supposed to eliminate.
Delegating to :mod:`dlt_meta` keeps both surfaces in lockstep.

Opt-out
-------
Set ``SDP_META_DISABLE_SRC_ALIAS=1`` in the environment to skip alias
registration (and the deprecation warning). :mod:`dlt_meta` honours the
same variable, so importing it below becomes a no-op for the ``src.*``
surface. Useful if a project has its own (unrelated) ``src/`` directory
it wants to import from.
"""

# Marker consumed by ``dlt_meta._register_src_aliases``: it tells the
# registration walk that the ``src`` object occupying ``sys.modules`` is
# THIS bundled compat package (safe to alias into and bind children
# onto), not an unrelated customer-owned ``src`` package (which must be
# left completely alone). Must be set BEFORE the ``import dlt_meta``
# below — we are mid-import, so dlt_meta sees this module object in
# ``sys.modules['src']`` with whatever attributes exist at that point.
__sdp_meta_compat__ = True

# Importing ``dlt_meta`` triggers its registration walk, which wires
# ``src.<sub>`` into ``sys.modules`` (with actionable stub modules on a
# non-SDP runtime), binds the renamed symbols, honours
# ``SDP_META_DISABLE_SRC_ALIAS``, and emits the deprecation warning.
# Its ``sys.modules.setdefault("src", …)`` is a no-op here because we
# (the real ``src`` package) are already mid-import in ``sys.modules``,
# so this package object stays the canonical ``src`` and merely gains
# the ``src.<sub>`` entries dlt_meta registers (plus, via
# ``_bind_on_parent``, the matching attributes on this package itself).
import dlt_meta  # noqa: F401  (imported for its registration side effects)
