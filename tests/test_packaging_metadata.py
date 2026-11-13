"""Tests that the distribution advertises consistent package metadata.

The supported interpreter range is written in setup.py and the Labs CLI
manifest. pip only enforces ``python_requires``, so stale classifiers or CLI
metadata would ship unnoticed.

The setup.py files are read with ``ast`` rather than imported -- executing them
at test time would need setuptools' build context and would run the README
rewriting in the primary setup.py for no benefit.
"""
import ast
import importlib
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PRIMARY_SETUP = REPO_ROOT / "setup.py"

# ">=3.10, <3.13" -- an inclusive floor and an exclusive ceiling, both 3.x.
PYTHON_REQUIRES_RE = re.compile(r"^>=3\.(\d+),\s*<3\.(\d+)$")
VERSION_CLASSIFIER_RE = re.compile(r"^Programming Language :: Python :: 3\.(\d+)$")


def _setup_kwargs(setup_py: Path) -> dict:
    """Return the literal keyword arguments of the ``setup()`` call."""
    tree = ast.parse(setup_py.read_text(encoding="utf-8"), str(setup_py))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "setup":
            kwargs = {}
            for keyword in node.keywords:
                if keyword.arg is None:
                    continue
                try:
                    kwargs[keyword.arg] = ast.literal_eval(keyword.value)
                except ValueError:
                    # Computed values such as long_description; not under test.
                    continue
            return kwargs
    raise AssertionError(f"no setup() call found in {setup_py}")


class PythonSupportMetadataTests(unittest.TestCase):

    def setUp(self):
        self.primary = _setup_kwargs(PRIMARY_SETUP)

    def _declared_range(self, kwargs: dict, label: str) -> range:
        """Minor versions covered by ``python_requires``, e.g. range(10, 13)."""
        python_requires = kwargs.get("python_requires")
        self.assertIsNotNone(python_requires, f"{label} declares no python_requires")
        match = PYTHON_REQUIRES_RE.match(python_requires)
        self.assertIsNotNone(
            match,
            f"{label} python_requires={python_requires!r} is not of the form "
            "'>=3.X, <3.Y'; update PYTHON_REQUIRES_RE if this is intentional",
        )
        floor, ceiling = int(match.group(1)), int(match.group(2))
        self.assertLess(floor, ceiling, f"{label} declares an empty version range")
        return range(floor, ceiling)

    def _version_classifiers(self, kwargs: dict) -> set:
        return {
            int(match.group(1))
            for match in (
                VERSION_CLASSIFIER_RE.match(c) for c in kwargs.get("classifiers", [])
            )
            if match
        }

    def test_version_classifiers_cover_the_declared_range(self):
        expected = set(self._declared_range(self.primary, "setup.py"))
        self.assertEqual(
            self._version_classifiers(self.primary),
            expected,
            "setup.py version classifiers do not match python_requires",
        )

    def test_base_python3_classifiers_are_present(self):
        classifiers = self.primary.get("classifiers", [])
        self.assertIn("Programming Language :: Python :: 3", classifiers)
        self.assertIn("Programming Language :: Python :: 3 :: Only", classifiers)

    def test_supported_range_matches_databricks_sdk_floor(self):
        self.assertEqual(
            self._declared_range(self.primary, "setup.py"),
            range(10, 13),
            "setup.py must not advertise Python versions rejected by "
            "databricks-sdk>=0.138.0",
        )

    def test_labs_manifests_match_supported_python_floor(self):
        manifest = (REPO_ROOT / "labs.yml").read_text(encoding="utf-8")
        self.assertIn("min_python: 3.10", manifest)
        self.assertNotIn("min_python: 3.8", manifest)

    def test_conda_environment_uses_supported_python(self):
        environment = (
            REPO_ROOT / "environment.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("python=3.10.*", environment)
        self.assertNotIn("python=3.9.*", environment)


class DatabricksSdkDependencyMetadataTests(unittest.TestCase):

    def test_minimum_sdk_matches_runtime_requirements_file(self):
        expected = "databricks-sdk>=0.138.0,<1"
        setup_text = PRIMARY_SETUP.read_text(encoding="utf-8")
        requirements = (
            REPO_ROOT / "requirements.txt"
        ).read_text(encoding="utf-8").splitlines()
        app_requirements = (
            REPO_ROOT / "databricks_app" / "requirements.txt"
        ).read_text(encoding="utf-8").splitlines()

        self.assertIn(f'"{expected}"', setup_text)
        self.assertIn(expected, requirements)
        self.assertIn(expected, app_requirements)
        self.assertNotIn("databricks-sdk>=0.20,<1", setup_text)
        self.assertNotIn("databricks-sdk>=0.20,<1", requirements)


class SetuptoolsDependencyMetadataTests(unittest.TestCase):

    def test_direct_install_uses_tested_release_within_wheel_compatibility_range(self):
        setup_text = PRIMARY_SETUP.read_text(encoding="utf-8")
        requirements = (
            REPO_ROOT / "requirements.txt"
        ).read_text(encoding="utf-8").splitlines()

        self.assertIn('"setuptools>=65,<85"', setup_text)
        self.assertIn("setuptools>=84.0.0,<85", requirements)

    def test_integration_extra_declares_packaging_parser(self):
        setup_text = PRIMARY_SETUP.read_text(encoding="utf-8")
        self.assertIn('"packaging>=24.0,<27"', setup_text)


class VersionSynchronizationTests(unittest.TestCase):
    """The primary distribution and ``__about__`` must move in lockstep."""

    def setUp(self):
        self.primary = _setup_kwargs(PRIMARY_SETUP)

    def test_about_version_matches_primary_setup(self):
        about = (
            REPO_ROOT / "src" / "databricks" / "labs" / "sdp_meta" / "__about__.py"
        ).read_text(encoding="utf-8")
        match = re.search(r"__version__\s*=\s*'([^']+)'", about)
        self.assertIsNotNone(match, "__about__.py has no __version__")
        self.assertEqual(
            match.group(1), self.primary.get("version"),
            "__about__.__version__ must match setup.py's version",
        )


class CanonicalPackageLayoutTests(unittest.TestCase):
    """The v0.2 distribution must not restore removed compatibility roots."""

    def test_compatibility_source_tree_is_absent(self):
        self.assertFalse((REPO_ROOT / "compat").exists())

    def test_setup_discovers_only_canonical_namespace(self):
        setup_text = PRIMARY_SETUP.read_text(encoding="utf-8")
        self.assertIn('package_dir={"": "src"}', setup_text)
        self.assertIn(
            'find_namespace_packages(where="src", include=["databricks.*"])',
            setup_text,
        )
        self.assertNotIn('find_packages(', setup_text)
        self.assertNotIn('"dlt_meta": "compat/', setup_text)

    def test_incremental_build_cleans_only_project_owned_package_roots(self):
        setup_text = PRIMARY_SETUP.read_text(encoding="utf-8")
        self.assertIn("for owned_build_root in owned_build_roots:", setup_text)
        self.assertIn('build_lib / "dlt_meta"', setup_text)
        self.assertIn('build_lib / "src"', setup_text)
        self.assertIn(
            'build_lib / "databricks" / "labs" / "sdp_meta"',
            setup_text,
        )
        self.assertNotIn("shutil.rmtree(build_lib)", setup_text)


class WheelTaskEntryPointTests(unittest.TestCase):
    """The canonical package must expose the migrated wheel-task entry point."""

    def test_run_entry_point_targets_importable_canonical_main(self):
        entry_points = _setup_kwargs(PRIMARY_SETUP)["entry_points"]
        self.assertIn(
            "run=databricks.labs.sdp_meta.__main__:main",
            entry_points["group_1"],
        )
        module = importlib.import_module(
            "databricks.labs.sdp_meta.__main__"
        )
        self.assertTrue(callable(module.main))


class ReleaseDocumentationVersionTests(unittest.TestCase):
    """Release-facing DAB guidance must follow the package version."""

    RELEASE_FACING_DAB_FILES = (
        "DAB_README.md",
        "docs/docs/faq.md",
        "docs/docs/getting-started/dabs.md",
        "docs/docs/getting-started/quickstart.md",
        "docs/docs/operations/troubleshooting.md",
        "docs/docs/reference/dab-parameters.md",
        "skills/sdp-meta/references/migration.md",
        "src/databricks/labs/sdp_meta/bundle.py",
        "src/databricks/labs/sdp_meta/cli.py",
        "src/databricks/labs/sdp_meta/templates/dab/databricks_template_schema.json",
        "src/databricks/labs/sdp_meta/templates/dab/template/"
        "{{.bundle_name}}/README.md.tmpl",
        "src/databricks/labs/sdp_meta/templates/dab/template/"
        "{{.bundle_name}}/notebooks/init_sdp_meta_pipeline.py.tmpl",
    )
    RELEASE_VERSION_EXAMPLE_FILES = (
        "demo/launch_interactive_demo.py",
        "demo/SDP_META_INTERACTIVE_DEMO.py",
    )

    def test_dab_install_examples_use_current_release(self):
        version = _setup_kwargs(PRIMARY_SETUP)["version"]
        expected = f"databricks-labs-sdp-meta=={version}"
        for relative_path in self.RELEASE_FACING_DAB_FILES:
            with self.subTest(path=relative_path):
                text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn(expected, text)
                self.assertNotIn("databricks-labs-sdp-meta==0.1.0", text)

    def test_demo_examples_use_current_release(self):
        version = _setup_kwargs(PRIMARY_SETUP)["version"]
        stale_install_examples = (
            "databricks_labs_sdp_meta-0.1.0-py3-none-any.whl",
            "(e.g. `0.1.0`)",
            "(e.g. ``0.1.0``)",
        )
        for relative_path in self.RELEASE_VERSION_EXAMPLE_FILES:
            with self.subTest(path=relative_path):
                text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn(version, text)
                for stale_example in stale_install_examples:
                    self.assertNotIn(stale_example, text)


class CiPythonMatrixTests(unittest.TestCase):
    """CI must resolve the wheel on every advertised interpreter."""

    def test_dependency_resolution_matrix_matches_python_metadata(self):
        workflow = (
            REPO_ROOT / ".github" / "workflows" / "onpush.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("dependency-resolution:", workflow)
        self.assertIn("python-version: ['3.10', '3.11', '3.12']", workflow)
        self.assertIn(
            "bash .github/scripts/verify-installed-wheel.sh", workflow
        )
        self.assertNotIn("import dlt_meta", workflow)
        self.assertNotIn("dist/compat", workflow)


class ReleaseWorkflowTests(unittest.TestCase):
    """The release dry run must inspect only the canonical artifacts."""

    def test_release_workflow_verifies_wheel_assets_and_imports(self):
        workflow = (
            REPO_ROOT / ".github" / "workflows" / "release.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("Verify primary wheel assets and import", workflow)
        self.assertIn("templates/dab/databricks_template_schema.json", workflow)
        self.assertIn('name.startswith(("dlt_meta/", "src/"))', workflow)
        self.assertIn('name == "dlt_meta.pth"', workflow)
        self.assertIn(
            "bash .github/scripts/verify-installed-wheel.sh", workflow
        )
        self.assertNotIn("dist/compat", workflow)
        self.assertNotIn("Publish dlt-meta", workflow)

    def test_shared_installed_wheel_verifier_checks_startup_and_entry_point(self):
        script = (
            REPO_ROOT / ".github" / "scripts" / "verify-installed-wheel.sh"
        ).read_text(encoding="utf-8")
        self.assertIn('if [[ -s "${startup_stderr}" ]]', script)
        self.assertIn('entry.group == "group_1"', script)
        self.assertIn('"run": "databricks.labs.sdp_meta.__main__:main"', script)
        self.assertIn(
            '"stage_conf": "databricks.labs.sdp_meta.stage_conf:main"',
            script,
        )
        self.assertIn("import databricks.labs.sdp_meta", script)
        self.assertIn(
            "import databricks.labs.sdp_meta._packaged_examples",
            script,
        )
        self.assertIn('if [[ "${python_bin}" != /* ]]', script)
        self.assertIn('Path(os.environ["SDP_META_REPO_ROOT"])', script)
        self.assertNotIn("len(templates) == 34", script)


class CodeqlWorkflowTests(unittest.TestCase):
    """CodeQL must run on the protected runner for supported branches."""

    def test_codeql_workflow_uses_protected_runner_and_expected_triggers(self):
        workflow = (
            REPO_ROOT / ".github" / "workflows" / "codeql-analysis.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("branches: [ main, v0.1.1 ]", workflow)
        self.assertIn(
            "group: databrickslabs-protected-runner-group",
            workflow,
        )
        self.assertIn("labels: linux-ubuntu-latest", workflow)
        self.assertEqual(
            workflow.count(
                "github/codeql-action/"
                "init@1c5b675653bb5c22dbe9b12b556ec555138e09fd"
            ),
            1,
        )
        self.assertEqual(
            workflow.count(
                "github/codeql-action/"
                "analyze@1c5b675653bb5c22dbe9b12b556ec555138e09fd"
            ),
            1,
        )


class McpDependencyMetadataTests(unittest.TestCase):

    def test_runtime_extra_preserves_mcp_2_x_compatibility(self):
        expected = "mcp>=2.0.0,<3.0"
        setup_text = PRIMARY_SETUP.read_text(encoding="utf-8")
        self.assertIn(f'MCP_REQUIREMENTS = ["{expected}"]', setup_text)

    def test_development_environment_uses_mcp_2_2_or_newer(self):
        expected = "mcp>=2.2.0,<3.0"
        requirements_text = (
            REPO_ROOT / "requirements-dev.txt"
        ).read_text(encoding="utf-8")
        self.assertIn(expected, requirements_text.splitlines())


if __name__ == "__main__":
    unittest.main()
