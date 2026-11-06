"""Regression tests for ``compat.dlt_meta._optional_runtime_import_error``.

The shim runs as an import side-effect of ``import dlt_meta`` (and
transitively, ``import src.<sub>`` via the bundled ``compat/src``
package). On serverless DLT runtimes that lack Lakeflow SDP, the
top-level ``from pyspark import pipelines as dp`` line in
``databricks.labs.sdp_meta.dataflow_pipeline`` blows up. We must
recognize every shape that failure takes and route to a stub instead
of bubbling the exception out -- otherwise the customer's
``from src.dataflow_spec import …`` (which doesn't even need
``pyspark.pipelines``) crashes during alias registration.

The runtime-shape catalogue is:

1. ``ModuleNotFoundError(name='pyspark')`` -- no pyspark at all.
   CPython reports the *top-level* package it failed to find, so
   ``from pyspark import pipelines`` on a machine without pyspark
   surfaces as ``name='pyspark'`` (NOT ``'pyspark.pipelines'`` -- the
   original predicate missed this, which made the ``dlt_meta.pth``
   startup hook print a traceback on every ``python3`` launch on
   laptops/CI that pip-installed the shim). ``name='pyspark.pipelines'``
   / ``'pipelines'`` are also recognized for runtimes where the parent
   package resolves but the submodule lookup fails.
2. ``ImportError("cannot import name 'pipelines' from 'pyspark'")``
   -- pyspark exists, ``pipelines`` submodule is absent.
3. **The case this file specifically guards against:** a runtime
   where ``pyspark.pipelines`` exists *as a module*, attempts
   ``from dlt import *`` at module load, falls through to
   ``raise PySparkException(errorClass="PIPELINES_NOT_SUPPORTED")``,
   and PySpark's *own* exception constructor crashes with a
   ``TypeError`` because the runtime's ``error-conditions.json`` for
   ``PIPELINES_NOT_SUPPORTED`` requires substitutions that PySpark
   didn't supply (``messageParameters=None`` -> ``set(None)``).

The CSV from a real customer-simulated phase 2 run exhibits case
(3): the inner exception is ``TypeError: 'NoneType' object is not
iterable`` thrown from ``pyspark/errors/utils.py`` while raising
``PIPELINES_NOT_SUPPORTED``. Only catching ``ImportError`` lets that
``TypeError`` escape and the ``import src.dataflow_spec`` line in
``validate_phase2.py`` fails even though dataflow_spec doesn't need
``pyspark.pipelines`` at all.

These tests pin the predicate behaviour for all three shapes so a
future refactor can't silently regress (3).
"""
from __future__ import annotations

import os
import sys
import types
import unittest

# The compat package lives in ``compat/`` at the repo root. Add it to
# ``sys.path`` exactly like ``test_compat.py`` does. We import the
# predicate directly rather than going through ``import dlt_meta``,
# which would trigger the registration walk and (on a normal CI
# runner) succeed -- this file is testing the predicate itself, not
# the registration.
_compat_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "compat")
if _compat_dir not in sys.path:
    sys.path.insert(0, _compat_dir)


def _load_predicate():
    """Import ``_optional_runtime_import_error`` without firing the
    package's import-time registration walk.

    We can't ``from dlt_meta import _optional_runtime_import_error``
    directly because the module's body calls ``_flat_reexport_or_stub``
    and ``_register_src_aliases`` at import time. To get just the
    function, we read the source and exec it in a fresh module
    namespace seeded with the stdlib imports the function needs. The
    function is pure (it only inspects the exception's traceback
    chain), so this is safe and avoids carrying any compat-package
    side effects across tests.
    """
    pkg_init = os.path.join(_compat_dir, "dlt_meta", "__init__.py")
    with open(pkg_init, encoding="utf-8") as fh:
        source = fh.read()

    namespace: dict = {
        "__name__": "_dlt_meta_predicate_under_test",
        "__file__": pkg_init,
    }
    # The function only needs ``os`` from the module-level imports;
    # injecting it explicitly keeps us from running the rest of the
    # module body.
    namespace["os"] = os

    # Slice out just the predicate definition. The function ends at
    # the next top-level ``def`` (we look for the line that follows
    # ``return False`` and starts with ``def `` at column 0).
    start = source.index("def _optional_runtime_import_error")
    # End at the start of the NEXT top-level def, which is
    # ``def _make_stub_module``.
    end = source.index("def _make_stub_module", start)
    exec(source[start:end], namespace)  # noqa: S102 (intentional)
    return namespace["_optional_runtime_import_error"]


_optional_runtime_import_error = _load_predicate()


def _make_traceback_through(filename: str) -> types.TracebackType:
    """Build an exception with a traceback that passes through *filename*.

    We can't fabricate a TracebackType from scratch (the type isn't
    constructible from Python). Instead, we exec a tiny snippet whose
    ``co_filename`` we set to *filename*, raise inside it, and
    capture the resulting traceback. The snippet itself is trivial.
    """
    src = "raise RuntimeError('synthetic')\n"
    code = compile(src, filename, "exec")
    try:
        exec(code, {})  # noqa: S102 (synthetic frame, intentional)
    except RuntimeError as exc:
        return exc.__traceback__  # type: ignore[return-value]
    raise AssertionError("synthetic exception was not raised")


class TestMissingPysparkEntirely(unittest.TestCase):
    """No pyspark at all — the laptop / CI ``pip install dlt-meta`` case.

    Regression guard for the v0.1.0 bug where ``name='pyspark'`` was not
    recognized, so the ``.pth`` startup hook printed a
    ``ModuleNotFoundError`` traceback on every interpreter launch and an
    explicit ``import dlt_meta`` failed outright.
    """

    def test_module_not_found_top_level_pyspark(self):
        exc = ModuleNotFoundError("No module named 'pyspark'")
        exc.name = "pyspark"
        self.assertTrue(_optional_runtime_import_error(exc))

    def test_real_cpython_shape_without_pyspark(self):
        """Derive the exception from CPython itself, not by hand.

        The original tests fabricated ``exc.name = 'pyspark.pipelines'``
        for the "no pyspark" case — mirroring the same wrong assumption
        the predicate encoded — so they passed while real machines
        failed. Here we actually execute ``from pyspark import
        pipelines`` in a subprocess started with ``-I -S`` (no
        site-packages, so pyspark cannot resolve even on dev machines
        that have it installed) and feed the predicate an exception
        carrying exactly the ``name`` CPython produced.
        """
        import json
        import subprocess

        code = (
            "import json, sys\n"
            "try:\n"
            "    from pyspark import pipelines  # noqa: F401\n"
            "except BaseException as e:\n"
            "    print(json.dumps({'type': type(e).__name__,"
            " 'name': getattr(e, 'name', None), 'msg': str(e)}))\n"
            "else:\n"
            "    print(json.dumps({'type': None}))\n"
        )
        out = subprocess.run(
            [sys.executable, "-I", "-S", "-c", code],
            capture_output=True, text=True, check=True,
        )
        shape = json.loads(out.stdout.strip())
        if shape["type"] is None:  # pragma: no cover - exotic layout
            self.skipTest("pyspark importable even with -I -S")
        self.assertEqual(shape["type"], "ModuleNotFoundError")
        self.assertEqual(shape["name"], "pyspark")  # the shape that escaped

        exc = ModuleNotFoundError(shape["msg"])
        exc.name = shape["name"]
        self.assertTrue(_optional_runtime_import_error(exc))


class TestKnownImportErrorShapes(unittest.TestCase):
    """The two ImportError shapes the predicate has always recognized."""

    def test_module_not_found_pyspark_pipelines(self):
        exc = ModuleNotFoundError("No module named 'pyspark.pipelines'")
        exc.name = "pyspark.pipelines"
        self.assertTrue(_optional_runtime_import_error(exc))

    def test_module_not_found_pipelines_short_name(self):
        exc = ModuleNotFoundError("No module named 'pipelines'")
        exc.name = "pipelines"
        self.assertTrue(_optional_runtime_import_error(exc))

    def test_cannot_import_name_pipelines_from_pyspark(self):
        exc = ImportError("cannot import name 'pipelines' from 'pyspark'")
        self.assertTrue(_optional_runtime_import_error(exc))


class TestBuggyPySparkErrorFormatter(unittest.TestCase):
    """The case from ``backward_compat_phase2_905606092b4e.csv``.

    Specifically: a ``TypeError`` raised from inside
    ``pyspark/pipelines/__init__.py`` (whose own raise-line crashes
    in the PySpark error formatter). The exception type is *not*
    ``ImportError``, but the traceback frame is in
    ``pyspark/pipelines/``, so the predicate must still recognize it
    as the SDP-runtime-missing case.
    """

    def test_typeerror_with_traceback_through_pyspark_pipelines(self):
        tb = _make_traceback_through(
            "/databricks/python/lib/python3.12/site-packages/"
            "pyspark/pipelines/__init__.py"
        )
        exc = TypeError("'NoneType' object is not iterable")
        exc.__traceback__ = tb
        self.assertTrue(_optional_runtime_import_error(exc))

    def test_typeerror_through_chained_context(self):
        """Predicate also walks ``__context__`` (PEP 3134 chaining)."""
        inner_tb = _make_traceback_through(
            "/some/runtime/pyspark/pipelines/__init__.py"
        )
        inner = ModuleNotFoundError("No module named 'dlt'")
        inner.__traceback__ = inner_tb

        outer = TypeError("'NoneType' object is not iterable")
        outer.__context__ = inner
        # outer.__traceback__ stays None, so the predicate must reach
        # ``inner`` via __context__ to spot the pyspark.pipelines frame.
        self.assertTrue(_optional_runtime_import_error(outer))


class TestUnrelatedExceptionsAreReRaised(unittest.TestCase):
    """The predicate must NOT swallow real bugs."""

    def test_typeerror_outside_pyspark_pipelines_returns_false(self):
        tb = _make_traceback_through("/home/user/myproject/utils.py")
        exc = TypeError("real bug, please fix")
        exc.__traceback__ = tb
        self.assertFalse(_optional_runtime_import_error(exc))

    def test_unrelated_module_not_found_returns_false(self):
        exc = ModuleNotFoundError("No module named 'totally_made_up_pkg'")
        exc.name = "totally_made_up_pkg"
        self.assertFalse(_optional_runtime_import_error(exc))

    def test_value_error_with_no_traceback_returns_false(self):
        exc = ValueError("not even close")
        self.assertFalse(_optional_runtime_import_error(exc))


if __name__ == "__main__":
    unittest.main()
