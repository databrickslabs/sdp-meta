"""Setup file for SDP-META (primary package).

This is the primary package following Databricks Labs namespace conventions.
Package structure: databricks.labs.sdp_meta

For backwards compatibility wrapper (dlt-meta package), see the compat/ directory.

Legacy v0.0.10 ``src.*`` import compatibility
---------------------------------------------
The wheel produced by this setup also bundles two compat surfaces
sourced from ``compat/``:

  * ``dlt_meta`` (from ``compat/dlt_meta/``) -- flat re-export
    package for v0.0.10 users who already migrated to
    ``from dlt_meta import …``. Shipping it inside the v0.1.0 main
    wheel means ``pip install databricks-labs-sdp-meta`` is the only
    thing customers need; they don't have to track a second package.
  * ``src`` (from ``compat/src/``) -- real Python package whose
    ``__init__.py`` populates ``sys.modules`` with ``src.<sub>`` ->
    ``databricks.labs.sdp_meta.<sub>`` aliases at import time. This
    is what makes a v0.0.10 customer's runner notebook keep working
    unchanged after their wheel is upgraded to v0.1.0. The
    customer's ``from src.dataflow_pipeline import …`` line resolves
    through normal Python import machinery: load ``src/__init__.py``,
    register aliases, fetch the canonical module from
    ``sys.modules``.

The earlier iteration of this surface relied on a ``dlt_meta.pth``
file at the wheel's purelib root to lazy-load the alias map at
interpreter startup via CPython's ``site.py``. That worked on a
normal ``pip install`` followed by a fresh ``python`` process, but
serverless DLT's ``%pip install`` magic does NOT trigger a fresh
``site.py`` ``.pth`` scan -- it lays files into site-packages and
hands the next cell back to the SAME interpreter, so the freshly-
installed ``.pth`` is silently ignored. Resolving through a real
package (``compat/src/``) sidesteps the ``.pth`` lifecycle entirely.

The standalone ``compat/`` package remains as a no-op PyPI redirect
(``install_requires=["databricks-labs-sdp-meta>=0.1.0"]``) so
``pip install dlt-meta`` keeps working from PyPI; it ships a
duplicate of the same shim it would otherwise install transitively,
which pip detects as already-satisfied and no-ops.
"""
from pathlib import Path
import shutil

import re
from setuptools import find_namespace_packages, find_packages, setup
from setuptools.command.build_py import build_py

with open("README.md", "r") as fh:
    content = fh.read()
# Strip the top bar section flagged for exclusion (nav links not meaningful on PyPI)
content = re.sub(
    r'<!-- Dont remove: exclude package -->.*?<!-- Dont remove: end exclude package -->',
    '', content, flags=re.DOTALL
)

# PyPI renders the description standalone: relative image srcs and relative
# markdown links (which work on the GitHub repo page) resolve to nothing
# there. Absolutize them against the repo so the PyPI page has a working
# banner and working links.
_REPO = "https://github.com/databrickslabs/sdp-meta"
_RAW = "https://raw.githubusercontent.com/databrickslabs/sdp-meta/main"
# <img src="docs/..."> -> raw.githubusercontent (must serve image bytes)
content = re.sub(r'src="(?!https?://)([^"]+)"', rf'src="{_RAW}/\1"', content)
# [text](GETTING_STARTED.md...) -> github blob URL; leave http(s) and
# in-page (#anchor) links alone
content = re.sub(r'\]\((?!https?://|#)([^)]+)\)', rf']({_REPO}/blob/main/\1)', content)

long_description = content

INSTALL_REQUIRES = [
    "setuptools>=65,<83",
    "databricks-sdk>=0.20,<1",
    "PyYAML>=6.0,<7",
]

DEV_REQUIREMENTS = [
    "flake8==7.3.0",
    "delta-spark==3.0.0",
    "pytest>=7.0.0",
    "coverage>=7.0.0",
    "pyspark==4.2.0"
]

IT_REQUIREMENTS = ["typer[all]==0.27.2"]

MCP_REQUIREMENTS = ["mcp>=2.0.0,<3.0"]


class BuildPyWithExamples(build_py):
    """Copy onboarding examples into the importable package in release wheels."""

    def run(self):
        source_root = Path(__file__).resolve().parent / "examples"
        missing_directories = [
            name for name in ("json", "yml") if not (source_root / name).is_dir()
        ]
        if missing_directories:
            raise FileNotFoundError(
                "Cannot package MCP examples: expected examples/json and "
                f"examples/yml below {source_root}; missing {missing_directories}."
            )
        package_root = (
            Path(self.build_lib) / "databricks" / "labs" / "sdp_meta"
        )
        # Incremental builds can retain deleted modules (including the removed
        # ``sdp_meta.mcp`` package). Rebuild this package subtree from source.
        if package_root.exists():
            shutil.rmtree(package_root)
        super().run()
        destination_root = package_root / "_packaged_examples"
        for directory_name in ("json", "yml"):
            source = source_root / directory_name
            destination = destination_root / directory_name
            if destination.exists():
                shutil.rmtree(destination)
            shutil.copytree(source, destination)


setup(
    name="databricks-labs-sdp-meta",
    version="0.1.0",
    # Ceiling matches compat/setup.py: the pyspark 3.5.5 stack this framework
    # runs against is incompatible with Python 3.13+ (pickle/cloudpickle
    # changes; see GETTING_STARTED.md prerequisites). Keeping both packages'
    # ceilings identical means `pip install dlt-meta` and
    # `pip install databricks-labs-sdp-meta` succeed/fail on the same
    # interpreters. Re-evaluate when pyspark ships a 3.13-compatible release.
    python_requires=">=3.8, <3.13",
    # No ``setup_requires``: it makes setuptools fetch build deps from PyPI
    # mid-build, which breaks the release workflow's ``--no-isolation`` build
    # (nothing may be fetched at build time). Build deps are supplied by
    # .github/requirements-build.txt instead.
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIREMENTS, "IT": IT_REQUIREMENTS, "mcp": MCP_REQUIREMENTS},
    cmdclass={"build_py": BuildPyWithExamples},
    author="Ravi Gawai",
    author_email="databrickslabs@databricks.com",
    license="Databricks License",
    description="Databricks Labs SDP-META Framework (formerly DLT-META)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/sdp-meta",
    project_urls={
        "Documentation": "https://databrickslabs.github.io/sdp-meta/",
        "Source": "https://github.com/databrickslabs/sdp-meta",
        "Issues": "https://github.com/databrickslabs/sdp-meta/issues",
        "Changelog": "https://github.com/databrickslabs/sdp-meta/blob/main/CHANGELOG.md",
    },
    # Three roots, one wheel:
    #   ``""`` -> ``src/``           : canonical ``databricks.labs.sdp_meta``
    #   ``dlt_meta`` -> ``compat/dlt_meta``  : flat re-export shim
    #   ``src`` -> ``compat/src``    : v0.0.10 ``src.*`` re-export package
    #
    # The exact-match keys (``dlt_meta``, ``src``) win over the ``""``
    # glob, so the source-layout ``src/`` dir (which holds the
    # canonical namespace) is scanned via ``find_namespace_packages``
    # while the legacy ``src`` PACKAGE is sourced from
    # ``compat/src/__init__.py``. There is no on-disk collision -- the
    # source-layout ``src/`` directory has no ``__init__.py`` so
    # ``find_namespace_packages(where="src")`` can't accidentally pick
    # it up as a package itself.
    package_dir={
        "": "src",
        "dlt_meta": "compat/dlt_meta",
        "src": "compat/src",
    },
    packages=(
        find_namespace_packages(where="src", include=["databricks.*"])
        + find_packages(
            where="compat",
            include=["dlt_meta", "dlt_meta.*", "src", "src.*"],
        )
    ),
    entry_points={
        "console_scripts": [
            "sdp-meta=databricks.labs.sdp_meta.__main__:main",
            # `stage_conf` is invoked by the bundle onboarding job's first task
            # (python_wheel_task entry_point: stage_conf) to copy conf/ onto a
            # UC Volume so serverless Spark can read it.
            "stage_conf=databricks.labs.sdp_meta.stage_conf:main",
        ],
        "group_1": [
            "run=databricks.labs.sdp_meta.__main__:main",
            "stage_conf=databricks.labs.sdp_meta.stage_conf:main",
        ],
    },
    # Per-version classifiers must cover exactly the range declared in
    # ``python_requires`` above; tests/test_packaging_metadata.py enforces it.
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Testing",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators"
    ],
)
