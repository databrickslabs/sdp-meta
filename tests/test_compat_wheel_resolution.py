"""Clean-environment test for the ``dlt-meta`` compatibility redirect."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import venv
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent


def _wheel_version(wheel_name):
    """Version field of a wheel filename (``dist-version-py-abi-plat.whl``)."""
    return wheel_name.split("-")[1]


def _run(command, *, cwd=None, env=None, timeout=300):
    result = subprocess.run(
        [str(part) for part in command],
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"Command failed ({result.returncode}): {' '.join(map(str, command))}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


@unittest.skipIf(
    sys.version_info >= (3, 13),
    "The v0.1.0 primary and compatibility distributions require Python <3.13.",
)
class CompatibilityWheelResolutionTests(unittest.TestCase):
    """Prove pip resolves the legacy distribution to the local primary wheel."""

    def test_dlt_meta_resolves_primary_wheel_from_local_wheelhouse(self):
        if os.environ.get("SDP_META_SKIP_NETWORK_TESTS") == "1":
            self.skipTest("SDP_META_SKIP_NETWORK_TESTS=1")

        with tempfile.TemporaryDirectory(prefix="sdp-meta-compat-resolve-") as tmp:
            tmp_path = Path(tmp)
            checkout = tmp_path / "checkout"
            dist = tmp_path / "dist"
            wheelhouse = tmp_path / "wheelhouse"
            environment = tmp_path / "venv"

            shutil.copytree(
                REPO_ROOT,
                checkout,
                symlinks=True,
                ignore=shutil.ignore_patterns(
                    ".git",
                    ".venv*",
                    ".coverage-venv",
                    ".databricks",
                    ".bc_wheels",
                    ".pytest_cache",
                    "__pycache__",
                    "node_modules",
                    "build",  # Includes docs/build.
                    "dist",
                    "*.egg-info",
                ),
            )
            dist.mkdir()
            wheelhouse.mkdir()

            for build_dir in (checkout, checkout / "compat"):
                _run(
                    [
                        sys.executable,
                        "setup.py",
                        "bdist_wheel",
                        "--dist-dir",
                        dist,
                    ],
                    cwd=build_dir,
                )

            project_wheels = sorted(dist.glob("*.whl"))
            download_command = [
                sys.executable,
                "-m",
                "pip",
                "download",
                "--dest",
                wheelhouse,
                *project_wheels,
            ]
            try:
                _run(download_command)
            except subprocess.TimeoutExpired:
                self.skipTest("PyPI dependency download timed out")
            except AssertionError as exc:
                error = str(exc).lower()
                network_errors = (
                    "could not fetch url",
                    "connection refused",
                    "connecttimeouterror",
                    "max retries exceeded",
                    "name or service not known",
                    "network is unreachable",
                    "proxyerror",
                    "readtimeouterror",
                    "temporary failure in name resolution",
                )
                if any(marker in error for marker in network_errors):
                    self.skipTest("PyPI dependency download unavailable")
                raise

            built_wheels = {path.name for path in wheelhouse.glob("*.whl")}
            # Derive the expected version from the wheels we just built
            # rather than hardcoding it, so a version bump in setup.py can't
            # leave this test asserting a stale literal. Both the redirect
            # and the primary wheel must carry the same version for the
            # ``dlt-meta==<version>`` install below to be satisfiable.
            compat_wheel = next(
                (n for n in built_wheels if n.startswith("dlt_meta-")), None
            )
            primary_wheel = next(
                (
                    n
                    for n in built_wheels
                    if n.startswith("databricks_labs_sdp_meta-")
                ),
                None,
            )
            self.assertIsNotNone(compat_wheel, built_wheels)
            self.assertIsNotNone(primary_wheel, built_wheels)
            target_version = _wheel_version(compat_wheel)
            self.assertEqual(
                target_version, _wheel_version(primary_wheel), built_wheels
            )
            primary_wheel_path = wheelhouse / primary_wheel
            with zipfile.ZipFile(primary_wheel_path) as archive:
                packaged_files = set(archive.namelist())
            self.assertIn(
                "databricks/labs/sdp_meta/templates/dab/"
                "databricks_template_schema.json",
                packaged_files,
            )
            self.assertTrue(
                any(
                    name.endswith(
                        "notebooks/init_sdp_meta_pipeline.py.tmpl"
                    )
                    for name in packaged_files
                ),
                "The primary wheel must include the runnable DAB template.",
            )
            self.assertTrue(
                any(name.startswith("databricks_sdk-") for name in built_wheels),
                built_wheels,
            )

            venv.EnvBuilder(with_pip=True).create(environment)
            pip = environment / ("Scripts/pip.exe" if os.name == "nt" else "bin/pip")
            python = environment / (
                "Scripts/python.exe" if os.name == "nt" else "bin/python"
            )
            clean_env = {
                **os.environ,
                # Keep the .pth hook from registering src.*: those aliases
                # traverse the complete Spark-dependent module surface, while
                # this deliberately minimal venv does not install PySpark.
                # src.* is covered by test_compat_src_aliases.py and the
                # serverless Phase 2 integration run. Importing dlt_meta or
                # src.* here would execute the eager re-export walk and require
                # pyspark; instead, the check below proves both installed
                # legacy packages are discoverable without executing them.
                "SDP_META_DISABLE_SRC_ALIAS": "1",
                "PIP_DISABLE_PIP_VERSION_CHECK": "1",
            }

            install = _run(
                [
                    pip,
                    "install",
                    "--force-reinstall",
                    "--no-index",
                    "--find-links",
                    wheelhouse,
                    f"dlt-meta=={target_version}",
                ],
                env=clean_env,
            )
            self.assertIn("databricks-labs-sdp-meta", install.stdout)

            result = _run(
                [
                    python,
                    "-c",
                    (
                        "import databricks.sdk; "
                        "import importlib.util; "
                        "import json; "
                        "from importlib.metadata import version; "
                        "assert importlib.util.find_spec('dlt_meta') is not None; "
                        "assert importlib.util.find_spec('src') is not None; "
                        "print(json.dumps({"
                        "'compat': version('dlt-meta'), "
                        "'primary': version('databricks-labs-sdp-meta')"
                        "}))"
                    ),
                ],
                env=clean_env,
            )
            self.assertEqual(
                json.loads(result.stdout),
                {"compat": target_version, "primary": target_version},
            )


if __name__ == "__main__":
    unittest.main()
