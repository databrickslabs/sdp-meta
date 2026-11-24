"""Control-plane modules must remain importable without PySpark."""

import os
import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class SparkFreeImportTests(unittest.TestCase):
    def _assert_imports_without_pyspark(self, modules):
        script = f"""
import builtins
real_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name == "pyspark" or name.startswith("pyspark."):
        raise ModuleNotFoundError("pyspark intentionally blocked")
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
for module in {modules!r}:
    __import__(module)
"""
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([
                str(REPO_ROOT / "src"),
                str(REPO_ROOT),
                str(REPO_ROOT / "databricks_app"),
            ]),
        }
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_bundle_import_is_spark_free(self):
        self._assert_imports_without_pyspark([
            "databricks.labs.sdp_meta.bundle",
        ])

    def test_app_quality_preflight_imports_are_spark_free(self):
        self._assert_imports_without_pyspark([
            "databricks_app.routes.spec_editor",
            "databricks_app.services.onboarding.quality_preflight",
        ])
