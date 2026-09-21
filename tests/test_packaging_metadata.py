"""Tests that both distributions advertise the same Python support.

The supported interpreter range is written in setup.py, compat/setup.py, and
both Labs CLI manifests. pip only enforces ``python_requires``, so stale
classifiers, CLI metadata, or a compat wrapper that drifts from the package it
forwards to would ship unnoticed.

The setup.py files are read with ``ast`` rather than imported -- executing them
at test time would need setuptools' build context and would run the README
rewriting in the primary setup.py for no benefit.
"""
import ast
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PRIMARY_SETUP = REPO_ROOT / "setup.py"
COMPAT_SETUP = REPO_ROOT / "compat" / "setup.py"

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
        self.compat = _setup_kwargs(COMPAT_SETUP)

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

    def test_both_distributions_declare_the_same_python_requires(self):
        self.assertEqual(
            self.primary.get("python_requires"),
            self.compat.get("python_requires"),
            "setup.py and compat/setup.py must accept the same interpreters -- the "
            "compat wrapper depends on the primary package, so a wider floor or "
            "ceiling there installs on interpreters its dependency rejects",
        )

    def test_version_classifiers_cover_the_declared_range(self):
        for label, kwargs in (("setup.py", self.primary), ("compat/setup.py", self.compat)):
            with self.subTest(setup=label):
                expected = set(self._declared_range(kwargs, label))
                self.assertEqual(
                    self._version_classifiers(kwargs),
                    expected,
                    f"{label} version classifiers do not match python_requires; "
                    "PyPI's Programming Language facet would advertise the wrong "
                    "interpreters",
                )

    def test_base_python3_classifiers_are_present(self):
        for label, kwargs in (("setup.py", self.primary), ("compat/setup.py", self.compat)):
            with self.subTest(setup=label):
                classifiers = kwargs.get("classifiers", [])
                self.assertIn("Programming Language :: Python :: 3", classifiers)
                self.assertIn("Programming Language :: Python :: 3 :: Only", classifiers)

    def test_supported_range_matches_databricks_sdk_floor(self):
        for label, kwargs in (("setup.py", self.primary), ("compat/setup.py", self.compat)):
            with self.subTest(setup=label):
                self.assertEqual(
                    self._declared_range(kwargs, label),
                    range(10, 13),
                    f"{label} must not advertise Python versions rejected by "
                    "databricks-sdk>=0.138.0",
                )

    def test_labs_manifests_match_supported_python_floor(self):
        for relative_path in ("labs.yml", "compat/labs.yml"):
            with self.subTest(path=relative_path):
                manifest = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn("min_python: 3.10", manifest)
                self.assertNotIn("min_python: 3.8", manifest)

    def test_conda_environment_uses_supported_python(self):
        environment = (
            REPO_ROOT / "environment.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("python=3.10.*", environment)
        self.assertNotIn("python=3.9.*", environment)


class CompatibilityDependencyMetadataTests(unittest.TestCase):

    def test_compat_wrapper_is_bounded_to_legacy_compatible_release_series(self):
        compat = _setup_kwargs(COMPAT_SETUP)
        self.assertEqual(
            compat.get("install_requires"),
            ["databricks-labs-sdp-meta>=0.1.1,<0.2.0"],
            "dlt-meta 0.1.x must not resolve to a primary 0.2+ release after "
            "the legacy dlt_meta and src.* compatibility surfaces are removed, "
            "and must not resolve below 0.1.1 (the primary wheel carries the "
            "dlt_meta/src compat files, and pre-0.1.1 copies print a startup "
            "traceback on machines without pyspark)",
        )

    def test_compat_wrapper_preserves_legacy_wheel_task_entrypoint(self):
        compat = _setup_kwargs(COMPAT_SETUP)
        self.assertEqual(
            compat.get("entry_points"),
            {
                "group_1": (
                    "run=databricks.labs.sdp_meta.__main__:main"
                )
            },
        )

    def test_primary_setup_documents_current_compat_requirement(self):
        setup_text = PRIMARY_SETUP.read_text(encoding="utf-8")
        self.assertIn(
            'install_requires=["databricks-labs-sdp-meta>=0.1.1,<0.2.0"]',
            setup_text,
        )
        self.assertNotIn(
            'install_requires=["databricks-labs-sdp-meta>=0.1.0"]',
            setup_text,
        )


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


class VersionSynchronizationTests(unittest.TestCase):
    """The two distributions and ``__about__`` must move in lockstep.

    The primary wheel physically carries the ``dlt_meta`` / ``src``
    compat packages, so a compat fix cannot ship unless BOTH
    distributions re-release at the same version and the compat
    wrapper's dependency floor is raised to match — v0.1.1 was nearly
    mis-released with only ``compat/setup.py`` bumped.
    """

    _FLOOR_RE = re.compile(
        r"^databricks-labs-sdp-meta>=(?P<floor>[0-9.]+),<(?P<ceiling>[0-9.]+)$"
    )

    def setUp(self):
        self.primary = _setup_kwargs(PRIMARY_SETUP)
        self.compat = _setup_kwargs(COMPAT_SETUP)

    def test_primary_and_compat_versions_are_synchronized(self):
        self.assertEqual(
            self.primary.get("version"), self.compat.get("version"),
            "setup.py and compat/setup.py must release at the same version",
        )

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

    def test_compat_dependency_floor_is_current_primary_version(self):
        (requirement,) = self.compat.get("install_requires")
        match = self._FLOOR_RE.match(requirement)
        self.assertIsNotNone(
            match, f"unexpected requirement shape: {requirement!r}"
        )
        self.assertEqual(
            match.group("floor"), self.primary.get("version"),
            "the compat wrapper must require the primary release it ships "
            "with — an older primary wheel carries older compat files",
        )


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
    """CI must resolve both wheels on every advertised interpreter."""

    def test_dependency_resolution_matrix_matches_python_metadata(self):
        workflow = (
            REPO_ROOT / ".github" / "workflows" / "onpush.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("dependency-resolution:", workflow)
        self.assertIn("python-version: ['3.10', '3.11', '3.12']", workflow)
        self.assertIn("import databricks.labs.sdp_meta", workflow)
        self.assertIn("import dlt_meta", workflow)


class ReleaseWorkflowTests(unittest.TestCase):
    """The release dry run must inspect and import both wheel artifacts."""

    def test_release_workflow_verifies_wheel_assets_and_imports(self):
        workflow = (
            REPO_ROOT / ".github" / "workflows" / "release.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("Verify primary wheel assets and import", workflow)
        self.assertIn("templates/dab/databricks_template_schema.json", workflow)
        self.assertIn("Verify compatibility wheel import and startup", workflow)
        self.assertIn("import databricks.labs.sdp_meta", workflow)
        self.assertIn("import dlt_meta", workflow)

    def test_obsolete_codeql_workflow_is_removed(self):
        self.assertFalse(
            (
                REPO_ROOT
                / ".github"
                / "workflows"
                / "codeql-analysis.yml"
            ).exists()
        )


class McpDependencyMetadataTests(unittest.TestCase):

    def test_mcp_sdk_range_matches_development_requirements(self):
        expected = "mcp>=2.0.0,<3.0"
        setup_text = PRIMARY_SETUP.read_text(encoding="utf-8")
        requirements_text = (
            REPO_ROOT / "requirements-dev.txt"
        ).read_text(encoding="utf-8")
        self.assertIn(f'MCP_REQUIREMENTS = ["{expected}"]', setup_text)
        self.assertIn(expected, requirements_text.splitlines())


if __name__ == "__main__":
    unittest.main()
