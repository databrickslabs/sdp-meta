"""Startup-safety tests for the ``dlt_meta`` compat shim.

The shim ships a ``dlt_meta.pth`` whose single line runs
``import dlt_meta`` at interpreter startup. ``site.py`` prints any
exception a ``.pth`` line raises on EVERY ``python3`` launch — so the
one hard invariant of this package is: **importing ``dlt_meta`` never
raises**, whatever is missing from the environment. Failures must
degrade to stubs that raise an actionable error at first attribute
access instead.

v0.1.0 violated this on any machine without pyspark (the
``ModuleNotFoundError: name='pyspark'`` shape escaped the
optional-runtime predicate), which broke ``import dlt_meta`` outright
and spammed a traceback at every interpreter start for anyone who
``pip install dlt-meta``-ed the shim on a laptop or CI box.

Each test runs a real subprocess with an import blocker installed via
``sys.meta_path`` (mimicking the missing package) and asserts on the
subprocess's stdout/stderr — the same observable surface site.py uses.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_COMPAT_DIR = os.path.join(_REPO_ROOT, "compat")
_SRC_DIR = os.path.join(_REPO_ROOT, "src")

# The driver script run in each subprocess. Substitutions:
#   {blocked}      - python tuple literal of top-level module names to block
#   {preload_src}  - when True, occupy sys.modules['src'] with a real
#                    customer-style package before importing dlt_meta
_DRIVER = """
import importlib.abc
import json
import sys
import types
import warnings

sys.path.insert(0, {compat_dir!r})
sys.path.insert(0, {src_dir!r})

_BLOCKED = {blocked}

_real_src = None
if {preload_src}:
    _real_src = types.ModuleType("src")
    _real_src.MARKER = "customer-owned"
    sys.modules["src"] = _real_src


class _Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        top = fullname.split(".")[0]
        if top in _BLOCKED:
            raise ModuleNotFoundError(
                "No module named " + repr(top), name=top
            )
        return None


sys.meta_path.insert(0, _Blocker())
warnings.simplefilter("ignore", DeprecationWarning)

result = {{}}
try:
    import dlt_meta  # the exact statement dlt_meta.pth executes
    result["import_raised"] = None
except BaseException as exc:  # noqa: BLE001
    result["import_raised"] = f"{{type(exc).__name__}}: {{exc}}"
    print(json.dumps(result))
    sys.exit(0)

try:
    dlt_meta.DataflowPipeline
    result["attr_raised"] = None
except Exception as exc:  # noqa: BLE001
    result["attr_raised"] = f"{{type(exc).__name__}}: {{exc}}"

try:
    import src.dataflow_pipeline
    result["src_alias_kind"] = type(src.dataflow_pipeline).__name__
except Exception as exc:  # noqa: BLE001
    result["src_alias_kind"] = f"raised {{type(exc).__name__}}: {{exc}}"

if _real_src is not None:
    result["real_src_preserved"] = sys.modules.get("src") is _real_src
    result["alias_registered"] = "src.dataflow_pipeline" in sys.modules
    result["bound_on_real_src"] = "dataflow_pipeline" in vars(_real_src)
    result["bound_on_dlt_meta"] = "dataflow_pipeline" in vars(dlt_meta)

print(json.dumps(result))
"""


def _run_driver(blocked: tuple, preload_src: bool = False) -> tuple:
    """Run the driver subprocess; return (parsed stdout json, stderr)."""
    script = _DRIVER.format(
        compat_dir=_COMPAT_DIR, src_dir=_SRC_DIR, blocked=repr(blocked),
        preload_src=preload_src,
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=120,
    )
    if proc.returncode != 0 and not proc.stdout.strip():
        raise AssertionError(
            f"driver crashed (rc={proc.returncode}):\n{proc.stderr}"
        )
    return json.loads(proc.stdout.strip().splitlines()[-1]), proc.stderr


def _deps_available() -> bool:
    """The generic-failure paths still need yaml/databricks-sdk importable."""
    try:
        import yaml  # noqa: F401
        import databricks.sdk  # noqa: F401
        return True
    except Exception:  # noqa: BLE001
        return False


class TestImportNeverRaises(unittest.TestCase):
    """``import dlt_meta`` must not raise, whatever is missing."""

    @unittest.skipUnless(_deps_available(), "yaml/databricks-sdk not installed")
    def test_no_pyspark_import_succeeds_with_runtime_stub(self):
        """The v0.1.0 regression: no pyspark at all."""
        result, stderr = _run_driver(blocked=("pyspark",))
        self.assertIsNone(
            result["import_raised"],
            f"import dlt_meta raised without pyspark: {result['import_raised']}",
        )
        # Attribute access fails with the actionable SDP-runtime message.
        self.assertIsNotNone(result["attr_raised"])
        self.assertIn("Lakeflow SDP runtime", result["attr_raised"])
        # No traceback noise on the observable startup surface.
        self.assertNotIn("Traceback", stderr)

    @unittest.skipUnless(_deps_available(), "yaml/databricks-sdk not installed")
    def test_no_pyspark_src_alias_is_stub_not_crash(self):
        result, _ = _run_driver(blocked=("pyspark",))
        # src.dataflow_pipeline resolves to a module object (stub), it
        # does not blow up alias registration.
        self.assertNotIn("raised", str(result["src_alias_kind"]))

    def test_unrelated_missing_dep_import_still_succeeds(self):
        """Generic failure (e.g. stripped image without yaml): still no raise."""
        result, stderr = _run_driver(blocked=("yaml",))
        self.assertIsNone(
            result["import_raised"],
            f"import dlt_meta raised without yaml: {result['import_raised']}",
        )
        self.assertIsNotNone(result["attr_raised"])
        # Generic message, not the SDP-runtime one — yaml missing is a
        # dependency problem, not a Lakeflow-runtime problem.
        self.assertIn("unavailable because importing", result["attr_raised"])
        self.assertNotIn("Traceback", stderr)


class TestForeignSrcPackage(unittest.TestCase):
    """A customer-owned ``src`` package must be left completely alone.

    When ``sys.modules['src']`` is already occupied by a real customer
    package (monorepo-style notebook layouts), the shim must not
    register ``src.<sub>`` aliases (they would shadow the customer's own
    submodules), must not bind anything onto the customer's package, and
    must not bind submodule stubs onto ``dlt_meta`` either — ``import
    src.x`` never consults ``dlt_meta`` when the parent isn't the shim.
    """

    def test_customer_src_package_is_left_alone(self):
        result, stderr = _run_driver(blocked=("pyspark",), preload_src=True)
        self.assertIsNone(result["import_raised"])
        self.assertTrue(result["real_src_preserved"])
        self.assertFalse(result["alias_registered"])
        self.assertFalse(result["bound_on_real_src"])
        self.assertFalse(result["bound_on_dlt_meta"])
        self.assertNotIn("Traceback", stderr)
        # The customer's package resolves imports through its own
        # machinery; with no __path__ on the synthetic package this is a
        # plain failed import, never a shim stub.
        self.assertIn("raised", str(result["src_alias_kind"]))


if __name__ == "__main__":
    unittest.main()
