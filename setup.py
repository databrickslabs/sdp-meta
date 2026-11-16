"""Setup file for the canonical SDP-META package."""
from pathlib import Path
import shutil

import re
from setuptools import find_namespace_packages, setup
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
    "setuptools>=65,<85",
    "databricks-sdk>=0.138.0,<1",
    "PyYAML>=6.0,<7",
]

DEV_REQUIREMENTS = [
    "flake8==7.3.0",
    "delta-spark==3.0.0",
    "pytest>=7.0.0",
    "coverage>=7.0.0",
    "pyspark==3.5.5"
]

IT_REQUIREMENTS = [
    "typer[all]==0.27.2",
    "packaging>=24.0,<27",
]

MCP_REQUIREMENTS = ["mcp>=2.0.0,<3.0"]


class BuildPyWithExamples(build_py):
    """Copy non-package runtime assets into release wheels."""

    def run(self):
        source_root = Path(__file__).resolve().parent / "examples"
        templates_source = (
            Path(__file__).resolve().parent
            / "src"
            / "databricks"
            / "labs"
            / "sdp_meta"
            / "templates"
        )
        missing_directories = [
            name for name in ("json", "yml") if not (source_root / name).is_dir()
        ]
        if missing_directories:
            raise FileNotFoundError(
                "Cannot package MCP examples: expected examples/json and "
                f"examples/yml below {source_root}; missing {missing_directories}."
            )
        if not templates_source.is_dir():
            raise FileNotFoundError(
                f"Cannot package DAB templates: expected {templates_source}."
            )
        build_lib = Path(self.build_lib)
        # Incremental builds can retain any package or module deleted from the
        # source tree. Clean only the package roots this project owns: deleting
        # all of build_lib could remove output produced by another build step.
        owned_build_roots = (
            build_lib / "databricks" / "labs" / "sdp_meta",
            build_lib / "dlt_meta",
            build_lib / "src",
        )
        for owned_build_root in owned_build_roots:
            if owned_build_root.exists():
                shutil.rmtree(owned_build_root)
        package_root = build_lib / "databricks" / "labs" / "sdp_meta"
        super().run()
        destination_root = package_root / "_packaged_examples"
        for directory_name in ("json", "yml"):
            source = source_root / directory_name
            destination = destination_root / directory_name
            if destination.exists():
                shutil.rmtree(destination)
            shutil.copytree(source, destination)
        shutil.copytree(
            templates_source,
            package_root / "templates",
            dirs_exist_ok=True,
        )


setup(
    name="databricks-labs-sdp-meta",
    version="0.2.0",
    # databricks-sdk>=0.138.0 requires Python 3.10+. The pyspark 3.5.5 stack
    # this framework runs against is incompatible with Python 3.13+
    # (pickle/cloudpickle changes). Re-evaluate when the runtime stack supports
    # Python 3.13.
    python_requires=">=3.10, <3.13",
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
    description="Databricks Labs SDP-META Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/sdp-meta",
    project_urls={
        "Documentation": "https://databrickslabs.github.io/sdp-meta/",
        "Source": "https://github.com/databrickslabs/sdp-meta",
        "Issues": "https://github.com/databrickslabs/sdp-meta/issues",
        "Changelog": "https://github.com/databrickslabs/sdp-meta/blob/main/CHANGELOG.md",
    },
    package_dir={"": "src"},
    packages=find_namespace_packages(where="src", include=["databricks.*"]),
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
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Testing",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators"
    ],
)
