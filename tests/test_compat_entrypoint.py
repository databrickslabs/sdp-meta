"""Regression coverage for the legacy ``dlt-meta`` wheel-task entry point."""

import configparser
import shutil
import subprocess
import sys
import tempfile
import zipfile
from importlib.metadata import EntryPoint
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
COMPAT_ROOT = REPO_ROOT / "compat"


class CompatibilityWheelEntrypointTests(TestCase):

    @staticmethod
    def _build_compat_wheel(output_dir: Path) -> Path:
        """Build from an isolated copy so repository build state is untouched."""
        build_root = output_dir / "compat"
        build_root.mkdir()
        for name in ("setup.py", "dlt_meta.pth"):
            shutil.copy2(COMPAT_ROOT / name, build_root / name)
        result = subprocess.run(
            [
                sys.executable,
                "setup.py",
                "bdist_wheel",
                "--dist-dir",
                output_dir,
            ],
            cwd=build_root,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            raise AssertionError(
                f"compat wheel build failed:\n{result.stdout}\n{result.stderr}"
            )
        wheels = list(output_dir.glob("dlt_meta-*.whl"))
        if len(wheels) != 1:
            raise AssertionError(f"expected one compat wheel, got {wheels}")
        return wheels[0]

    @staticmethod
    def _run_entrypoint_from_wheel(wheel: Path) -> EntryPoint:
        with zipfile.ZipFile(wheel) as archive:
            metadata_path = next(
                name
                for name in archive.namelist()
                if name.endswith(".dist-info/entry_points.txt")
            )
            parser = configparser.ConfigParser()
            parser.read_string(archive.read(metadata_path).decode("utf-8"))
        return EntryPoint(
            name="run",
            value=parser["group_1"]["run"],
            group="group_1",
        )

    def test_built_compat_entrypoint_accepts_v0010_onboarding_arguments(self):
        with tempfile.TemporaryDirectory(
            prefix="sdp-meta-compat-entrypoint-"
        ) as tmp:
            wheel = self._build_compat_wheel(Path(tmp))
            entrypoint = self._run_entrypoint_from_wheel(wheel)
            self.assertEqual(
                entrypoint.value,
                "databricks.labs.sdp_meta.__main__:main",
            )
            main = entrypoint.load()

            argv = [
                "dlt-meta-run",
                "--onboard_layer",
                "bronze_silver",
                "--onboarding_file_path",
                "/Volumes/catalog/schema/volume/onboarding.json",
                "--database",
                "catalog.schema",
                "--env",
                "it",
                "--bronze_dataflowspec_table",
                "bronze_dataflowspec",
                "--bronze_dataflowspec_path",
                "",
                "--silver_dataflowspec_table",
                "silver_dataflowspec",
                "--silver_dataflowspec_path",
                "",
                "--import_author",
                "backward_compat",
                "--version",
                "v1",
                "--overwrite",
                "False",
                "--uc_enabled",
                "True",
            ]
            with (
                patch.object(sys, "argv", argv),
                patch(
                    "databricks.labs.sdp_meta.__main__.onboard_dataflowspecs"
                ) as onboard,
            ):
                main()

            args = onboard.call_args.args[0]
            self.assertEqual(args.onboard_layer, "bronze_silver")
            self.assertEqual(args.database, "catalog.schema")
            self.assertEqual(args.overwrite, "False")
            self.assertEqual(args.uc_enabled, "True")
