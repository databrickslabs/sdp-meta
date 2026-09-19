"""Backward-compatibility integration test: generic SOURCE -> TARGET upgrade.

Goal
----
A customer's existing pipeline running on SOURCE_VERSION must keep working
unchanged when the wheel is swapped to TARGET_VERSION. No notebook edits,
no onboarding redo, and no DLT checkpoint resets.

Two profiles ship out of the box -- see ``version_profiles.py``:

  * ``LEGACY``   -- v0.0.1 through v0.0.10 (dlt_meta dist, ``from src.*``
                    runner imports, ``dlt_meta_whl`` config key).
  * ``CURRENT``  -- v0.1.0 and later (databricks_labs_sdp_meta dist,
                    ``from databricks.labs.sdp_meta.*`` runner imports,
                    ``sdp_meta_whl`` config key). The v0.1.0 main wheel
                    BUNDLES a legacy-namespace compat surface: a real
                    top-level ``src`` package (plus a ``dlt_meta``
                    package), both re-exporting
                    ``databricks.labs.sdp_meta.*``. So a LEGACY-source
                    customer who flips their ``dlt_meta_whl`` config from
                    a v0.0.10 wheel to a v0.1.0 wheel keeps working
                    without any other change -- their
                    ``from src.* import …`` resolves through the real
                    ``src`` package via normal import machinery.

Profile resolution is git-ref-prefix based. Pass ``--source_version`` and
``--target_version`` (any git tag, branch, or SHA the wheel builder can
check out). The orchestrator infers the profile for each ref; override
via ``--source_profile`` / ``--target_profile`` if a custom branch needs
explicit pinning.

Two-phase contract
------------------

Phase 1 (SOURCE wheel):
  1. Build SOURCE wheel from --source_version git ref.
  2. Onboard A1 (initial) using SOURCE-shape onboarding template.
  3. Run bronze A1 pipeline + silver pipeline.
  4. Onboard A2 (incremental).
  5. Run bronze A2 + silver again.
  6. Validate row counts and persist them for Phase 2.

[Local: build TARGET wheel, upload to the SAME UC volume, then
 ``pipelines.update()`` each pipeline's configuration to swap the value
 behind the SOURCE profile's pipeline-config key from the SOURCE wheel
 path to the TARGET wheel path. Pipeline IDs DO NOT change, so DLT
 checkpoints (which are pipeline-scoped) are preserved.]

Phase 2 (TARGET wheel):
  1. Drop a small new batch of customers + transactions JSON files
     into the original source paths (5 each).
  2. Re-run bronze pipeline. Auto Loader sees the new files via the
     existing checkpoint and ingests them on top of SOURCE's tables.
  3. Re-run silver pipeline.
  4. Validate: every Phase 1 row still present (compared to the
     persisted Phase 1 counts), bronze grew by >= 5 rows per source,
     silver did not regress, dataflowspec persisted by SOURCE reads
     cleanly through TARGET's dataclasses.

One config key, one wheel, one ``%pip install``
-----------------------------------------------
The runner notebook is the SOURCE profile's runner -- byte-for-byte the
file the customer would have on the source version. It reads exactly
ONE pipeline-config key (``dlt_meta_whl`` for LEGACY) and runs ONE
``%pip install $dlt_meta_whl`` line. For LEGACY -> CURRENT cross-
namespace upgrades the source runner's ``from src.* import …`` keeps
resolving because the v0.1.0 wheel bundles a real top-level ``src``
package: once ``%pip install`` lands the wheel, the runner's
``from src.dataflow_pipeline import …`` walks normal import machinery,
finds ``src/`` in site-packages, runs its ``__init__`` (which registers
the ``src.<sub>`` -> ``databricks.labs.sdp_meta.<sub>`` aliases), and
resolves. This does NOT depend on a ``.pth`` firing -- serverless
``%pip install`` does not re-trigger ``site.py``'s ``.pth`` scan, which
is exactly why resolution goes through a real package rather than a
startup hook.

An opt-in ``compat_wheelhouse`` target surface separately exercises the
pre-release PyPI redirect contract. It builds the primary and compatibility
wheels, downloads the primary wheel's complete runtime dependency set, uploads
the resulting wheelhouse to one UC Volume directory, and changes Phase 2's
value behind ``dlt_meta_whl`` to ``dlt-meta==<version>``. The uploaded notebook
copy gets literal ``--force-reinstall --no-index --find-links`` arguments
because Databricks treats a substituted ``$var`` as one pip argument; source
files in the working tree are unchanged.

Usage
-----

    # Default: v0.0.10 -> v0.1.0 (legacy -> current)
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog>

    # Future: v0.1.0 -> v0.1.1 (current -> current)
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog> \\
        --source_version=v0.1.0 \\
        --target_version=v0.1.1

    # Custom branches with explicit profile pins
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog> \\
        --source_version=feature/legacy-bugfix --source_profile=legacy \\
        --target_version=feature/sdp-meta     --target_profile=current

    # Pre-release: exercise ``pip install dlt-meta==0.1.0`` using local wheels
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog> \\
        --install_mode=local \\
        --build_target_from_worktree \\
        --target_install_surface=compat_wheelhouse
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import uuid
import warnings
import webbrowser
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, List, Optional

# Add project root to Python path so ``databricks.labs.sdp_meta`` resolves
# (we only use it for the WorkspaceInstaller helper, not anything that's
# version-sensitive between phases).
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service import compute, jobs  # noqa: E402
from databricks.sdk.service.catalog import SchemasAPI, VolumeInfo, VolumeType  # noqa: E402
from databricks.sdk.service.pipelines import (  # noqa: E402
    NotebookLibrary,
    PipelineCluster,
    PipelineLibrary,
)
from databricks.sdk.service.workspace import ImportFormat, Language  # noqa: E402

from databricks.labs.sdp_meta.identifiers import validate_uc_identifier  # noqa: E402
# Aliased to disambiguate from the class-level ``BCRunner.create_pipeline``
# wrapper below; both symbols resolve correctly via Python scoping, but
# calling ``create_pipeline`` inside a method called ``create_pipeline``
# reads like recursion.
from integration_tests.run_integration_tests import (  # noqa: E402
    create_pipeline as sdk_create_pipeline,
)

from integration_tests.version_profiles import (  # noqa: E402
    DEFAULT_SOURCE_REF,
    DEFAULT_TARGET_REF,
    KNOWN_PROFILES,
    VersionProfile,
    is_cross_namespace_upgrade,
    resolve_profile,
)
from integration_tests.wheel_builder import GitRefWheelBuilder  # noqa: E402


@dataclass
class BCRunnerConf:
    """Per-run state for the backward-compat orchestrator.

    Mirrors ``SDPMetaRunnerConf`` from ``run_integration_tests.py`` but
    pared down to the surface this test actually uses (cloudfiles +
    bronze/silver), and adds the wheel-swap fields. Profile-driven --
    every version-line knob (distribution, runner notebook, onboarding
    template, pipeline-config key, compat shim) lives on
    ``source_profile`` / ``target_profile``, never on this dataclass.
    """

    run_id: str
    uc_catalog_name: str
    source_ref: str
    target_ref: str
    source_profile: VersionProfile
    target_profile: VersionProfile
    profile: Optional[str] = None  # databricks-cli profile (NOT the version profile)
    username: Optional[str] = None

    # Where wheels come from at install time.
    #
    #   "local"  -- build wheels from <source_ref> + <target_ref> with
    #               GitRefWheelBuilder, upload to UC volume, runners
    #               and wheel-tasks reference the UC volume paths
    #               (default; matches what real customers do --
    #               install a pre-built artifact).
    #   "git"    -- skip the local build entirely. Runners
    #               ``%pip install git+<git_repo_url>@<ref>`` directly
    #               and the Phase 1 onboarding wheel-task resolves
    #               the same git URL via JobEnvironment.dependencies.
    #               Faster local iteration; requires the cluster to
    #               have egress to ``git_repo_url``.
    #   ``pypi``:     post-release verification. No wheels are built or
    #                 uploaded; Phase 1 installs
    #                 ``dlt-meta==<source_package_version>`` and Phase 2
    #                 installs ``dlt-meta==<target_package_version>``
    #                 straight from the live PyPI index, proving the
    #                 published redirect + primary packages upgrade a
    #                 running deployment in place. Requires cluster
    #                 egress to pypi.org.
    install_mode: str = "local"
    git_repo_url: str = "https://github.com/databrickslabs/sdp-meta.git"
    # Exact ``dlt-meta`` version Phase 1 installs when
    # ``install_mode == "pypi"``. Derived from ``source_ref`` (a ``vX.Y.Z``
    # tag with the ``v`` stripped) unless passed explicitly.
    source_package_version: Optional[str] = None
    # Pipeline execution and publishing mode:
    #
    #   ``serverless_dpm`` (default) creates serverless pipelines with
    #   ``schema=``. This is the existing fast-path coverage.
    #
    #   ``standard_legacy`` creates pipeline-managed standard compute with
    #   ``target=``. It exercises an existing legacy publishing-mode
    #   pipeline being upgraded in place to the current wheel.
    pipeline_mode: str = "serverless_dpm"
    pipeline_num_workers: Optional[int] = None
    # Opt-in Issue #460 coverage: Phase 2 re-runs onboarding with the
    # target wheel and overwrite=False against the physical SOURCE tables.
    phase2_append_onboarding: bool = False
    # When True, skip the git-worktree checkout for the TARGET main
    # wheel and run ``setup.py bdist_wheel`` against the developer's
    # working tree instead. Use ONLY for iterating on uncommitted
    # target-side changes (bundled compat shim, post-rename CLI
    # aliases, etc.) before they're pushed to ``target_ref``. Source
    # ALWAYS comes from the pinned ref -- the source side is the
    # customer's already-released wheel and has nothing to do with
    # local edits.
    build_target_from_worktree: bool = False
    # Phase 2 install surface:
    #
    #   ``primary_wheel`` (default) installs the target primary wheel
    #   directly, preserving the original backward-compatibility test.
    #
    #   ``compat_wheelhouse`` builds and uploads both the target primary
    #   wheel and the ``compat/`` dlt-meta redirect wheel, together with
    #   the primary wheel's runtime dependencies. Phase 2 then installs
    #   ``dlt-meta==<target_package_version>`` through pip's resolver,
    #   proving the pre-release PyPI redirect path without publishing.
    target_install_surface: str = "primary_wheel"
    # Exact ``dlt-meta`` version the compat_wheelhouse offline install
    # pins (``dlt-meta==<target_package_version>``). ``None`` means
    # "derive it from the version the TARGET wheels actually build" --
    # ``build_wheels`` fills this in and verifies the primary and compat
    # wheels agree, so the pinned version can never drift from the built
    # artifacts. Pass an explicit value only to assert a specific version
    # (a mismatch with the built wheels then fails the run at build time).
    target_package_version: Optional[str] = None
    # Interpreter the compat_wheelhouse is downloaded for. Drives both the
    # ``--python-version`` and the ``cpXY`` ABI tag passed to ``pip
    # download``. Override when serverless/DBR runs a different CPython
    # minor version than the default so ABI-specific dependency wheels
    # (e.g. PyYAML's C extension) resolve for the right interpreter.
    compat_python_version: str = "3.12"

    # Schema names (one per layer + one for dataflowspec). Per-run
    # suffix avoids collisions when two devs hit the same UC catalog.
    sdp_meta_schema: str = ""
    bronze_schema: str = ""
    silver_schema: str = ""

    # Volume + workspace paths.
    uc_volume_name: str = "dlt_meta_files"
    volume_info: Optional[VolumeInfo] = None
    uc_volume_path: str = ""
    runners_nb_path: str = ""
    int_tests_dir: str = "integration_tests"

    # Local + remote wheel paths.
    #
    # ``source_main_whl`` -- main wheel built from --source_version.
    # ``target_main_whl`` -- main wheel built from --target_version.
    #
    # The target wheel bundles the legacy Python import surface. The separate
    # compatibility distribution is built only for ``compat_wheelhouse`` so
    # Phase 2 can also exercise the legacy ``package_name=dlt_meta`` Python
    # wheel task contract.
    source_main_whl_local: str = ""
    target_main_whl_local: str = ""
    target_compat_whl_local: str = ""
    target_dependency_whls_local: List[str] = field(default_factory=list)
    source_main_whl_remote: str = ""
    target_main_whl_remote: str = ""
    target_compat_whl_remote: str = ""
    target_wheelhouse_remote: str = ""

    # Generated onboarding-file output paths (one per group) --
    # gitignored.
    a1_onboarding_file: str = ""
    a2_onboarding_file: str = ""
    phase2_onboarding_file: str = ""

    # Pipeline IDs (created in Phase 1, reused in Phase 2).
    bronze_a1_pipeline_id: str = ""
    bronze_a2_pipeline_id: str = ""
    silver_pipeline_id: str = ""

    # Job IDs (one job per phase).
    phase1_job_id: Optional[int] = None
    phase2_job_id: Optional[int] = None

    # Phase output CSV paths inside the workspace + local copies.
    phase1_output_ws: str = ""
    phase2_output_ws: str = ""

    # Incremental seed shape. The two staging dirs we ship in the repo
    # contain exactly this many rows; the validator uses these as the
    # >= floor for bronze growth in Phase 2.
    phase2_customer_delta: int = 5
    phase2_transaction_delta: int = 5

    # Cached per-pipeline configuration dictionaries. Built once in
    # Phase 1 (under SOURCE) and again in Phase 2 (under TARGET).
    # Stored on the conf so logging + error messages can introspect.
    _phase1_pipeline_configs: Dict[str, Dict[str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.sdp_meta_schema = self.sdp_meta_schema or f"sdp_meta_dataflowspecs_bc_{self.run_id}"
        self.bronze_schema = self.bronze_schema or f"sdp_meta_bronze_bc_{self.run_id}"
        self.silver_schema = self.silver_schema or f"sdp_meta_silver_bc_{self.run_id}"
        self.a1_onboarding_file = (
            f"integration_tests/conf/json/backward_compat_onboarding_{self.run_id}.json"
        )
        self.a2_onboarding_file = (
            f"integration_tests/conf/json/backward_compat_onboarding_A2_{self.run_id}.json"
        )
        self.phase2_onboarding_file = (
            "integration_tests/conf/json/"
            f"backward_compat_onboarding_phase2_{self.run_id}.json"
        )

    @property
    def is_cross_namespace_upgrade(self) -> bool:
        """Whether SOURCE and TARGET use different runner-import namespaces.

        Used purely for logging and gate decisions; no separate compat
        shim wheel is built or installed -- the target wheel is
        responsible for bundling its own legacy-namespace compat
        surface when the namespace changes.
        """
        return is_cross_namespace_upgrade(
            source=self.source_profile, target=self.target_profile
        )

    @property
    def is_standard_legacy_mode(self) -> bool:
        return self.pipeline_mode == "standard_legacy"

    @property
    def source_supports_append_flow_source_metadata(self) -> bool:
        """Whether SOURCE includes the append-flow metadata fix from #444."""
        if self.source_profile.name != "current":
            return False
        match = re.match(r"^v?(\d+)\.(\d+)\.(\d+)(?:$|[.+-])", self.source_ref)
        if match is None:
            # Custom CURRENT branches represent the latest contract unless
            # the caller explicitly pins an older release ref.
            return True
        return tuple(int(part) for part in match.groups()) >= (0, 1, 1)


class BackwardCompatRunner:
    """Two-phase orchestrator: SOURCE -> TARGET wheel swap.

    Mirrors ``SDPMETARunner`` from ``run_integration_tests.py`` for the
    upload + workflow primitives, but adds the phase boundary and the
    pipeline reconfig step. Kept as a separate class so the existing
    test runner stays untouched.
    """

    # Per-phase wait ceiling for ``jobs.run_now().result(...)``. Phase 1
    # is the long pole: six pipeline runs in sequence (onboard A1 ->
    # bronze A1 -> silver -> onboard A2 -> bronze A2 -> silver) plus
    # the validate notebook task. Each serverless DLT pipeline run
    # incurs a cold-start charge (~1-2 min) on top of its actual
    # processing window. Override at runtime with --phase_timeout_min
    # if a slow workspace pushes Phase 1 past the wall.
    DEFAULT_PHASE_TIMEOUT_MIN = 30

    # Wall-clock ceiling for the ``pip download`` that populates the
    # compat_wheelhouse. Without it a stalled PyPI/proxy fetch would hang
    # the whole orchestrator with no diagnostic.
    COMPAT_DOWNLOAD_TIMEOUT_SEC = 600
    PHASE_OUTPUT_DOWNLOAD_ATTEMPTS = 3
    PHASE_OUTPUT_RETRY_DELAY_SEC = 2
    # Contract with validate_phase1.py and validate_phase2.py: validation
    # failures are persisted as human-readable rows containing this marker.
    VALIDATION_FAILURE_MARKER = "Failed!"

    def __init__(self, args: dict, ws: WorkspaceClient) -> None:
        self.args = args
        self.ws = ws
        self.base_dir = "integration_tests"
        self.phase_timeout_min = int(
            args.get("phase_timeout_min") or self.DEFAULT_PHASE_TIMEOUT_MIN
        )

    # ----- helpers --------------------------------------------------------

    def _my_username(self) -> str:
        return self.ws.current_user.me().user_name

    def _build_runner_conf(self) -> BCRunnerConf:
        run_id = uuid.uuid4().hex[:12]
        username = self._my_username()

        source_ref = self.args.get("source_version") or DEFAULT_SOURCE_REF
        target_ref = self.args.get("target_version") or DEFAULT_TARGET_REF
        source_profile = resolve_profile(
            source_ref, profile_override=self.args.get("source_profile")
        )
        target_profile = resolve_profile(
            target_ref, profile_override=self.args.get("target_profile")
        )

        install_mode = (self.args.get("install_mode") or "local").lower()
        if install_mode not in ("local", "git", "pypi"):
            raise ValueError(
                "--install_mode must be 'local', 'git', or 'pypi'; "
                f"got {install_mode!r}"
            )

        def _pypi_version_from_ref(ref: str, *, flag: str) -> str:
            """Derive a PyPI version from a ``vX.Y.Z``-style tag."""
            match = re.fullmatch(r"v?(\d+(?:\.\d+)+(?:[.\-+][A-Za-z0-9.]+)?)", ref)
            if not match:
                raise ValueError(
                    f"--install_mode=pypi could not derive a PyPI version "
                    f"from {flag}={ref!r}. Pass a release tag (e.g. "
                    f"'v0.1.0') or set the corresponding "
                    "--source_package_version/--target_package_version "
                    "explicitly. Note: pip versions carry no 'v' prefix "
                    "(dlt-meta==0.1.0, not ==v0.1.0)."
                )
            return match.group(1)

        source_package_version = self.args.get("source_package_version")
        if source_package_version is not None and not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9.!+_-]*", source_package_version
        ):
            raise ValueError(
                "--source_package_version must be a valid single-token "
                f"package version; got {source_package_version!r}."
            )
        if install_mode == "pypi" and source_package_version is None:
            source_package_version = _pypi_version_from_ref(
                source_ref, flag="--source_version"
            )

        build_target_from_worktree = bool(
            self.args.get("build_target_from_worktree")
        )
        if build_target_from_worktree and install_mode != "local":
            raise ValueError(
                "--build_target_from_worktree only makes sense with "
                "--install_mode=local; got "
                f"install_mode={install_mode!r}."
            )

        target_install_surface = (
            self.args.get("target_install_surface") or "primary_wheel"
        ).lower()
        if target_install_surface not in ("primary_wheel", "compat_wheelhouse"):
            raise ValueError(
                "--target_install_surface must be 'primary_wheel' or "
                f"'compat_wheelhouse'; got {target_install_surface!r}."
            )
        if target_install_surface == "compat_wheelhouse":
            if install_mode != "local":
                raise ValueError(
                    "--target_install_surface=compat_wheelhouse requires "
                    "--install_mode=local."
                )
            if not is_cross_namespace_upgrade(
                source=source_profile, target=target_profile
            ):
                raise ValueError(
                    "--target_install_surface=compat_wheelhouse is intended "
                    "for a legacy-to-current cross-namespace upgrade."
                )
            if self.args.get("phase2_append_onboarding"):
                raise ValueError(
                    "--phase2_append_onboarding requires "
                    "--target_install_surface=primary_wheel because the "
                    "offline wheelhouse is installed inside pipeline "
                    "notebooks, not Job Environments."
                )

        # ``None`` (the default) means "derive the pinned version from the
        # TARGET wheels at build time" -- see BCRunnerConf.target_package_version
        # and _resolve_target_package_version. Only validate the token shape
        # when the user pins an explicit version.
        target_package_version = self.args.get("target_package_version")
        if target_package_version is not None and not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9.!+_-]*", target_package_version
        ):
            raise ValueError(
                "--target_package_version must be a valid single-token "
                f"package version; got {target_package_version!r}."
            )
        if install_mode == "pypi" and target_package_version is None:
            target_package_version = _pypi_version_from_ref(
                target_ref, flag="--target_version"
            )

        compat_python_version = (
            self.args.get("compat_python_version")
            or BCRunnerConf.__dataclass_fields__["compat_python_version"].default
        )
        # Must be a bare CPython MAJOR.MINOR: it feeds pip download's
        # --python-version verbatim and derives the ``cpXY`` ABI tag
        # (``"cp" + value.replace(".", "")``). A patch-level or dotless
        # value would produce an invalid ABI (e.g. ``cp3121``) and only
        # surface as an opaque pip resolution error at build time.
        if not re.fullmatch(r"\d+\.\d+", compat_python_version):
            raise ValueError(
                "--compat_python_version must be a CPython MAJOR.MINOR "
                f"version (e.g. '3.12'); got {compat_python_version!r}."
            )

        pipeline_mode = (
            self.args.get("pipeline_mode") or "serverless_dpm"
        ).lower()
        if pipeline_mode not in ("serverless_dpm", "standard_legacy"):
            raise ValueError(
                "--pipeline_mode must be 'serverless_dpm' or "
                f"'standard_legacy'; got {pipeline_mode!r}."
            )

        pipeline_num_workers = self.args.get("pipeline_num_workers")
        if pipeline_mode == "standard_legacy":
            if pipeline_num_workers is None or int(pipeline_num_workers) < 1:
                raise ValueError(
                    "--pipeline_num_workers must be at least 1 when "
                    "--pipeline_mode=standard_legacy."
                )
            pipeline_num_workers = int(pipeline_num_workers)

        conf = BCRunnerConf(
            run_id=run_id,
            uc_catalog_name=self.args["uc_catalog_name"],
            source_ref=source_ref,
            target_ref=target_ref,
            source_profile=source_profile,
            target_profile=target_profile,
            profile=self.args.get("profile"),
            username=username,
            install_mode=install_mode,
            git_repo_url=self.args.get("git_repo_url")
            or BCRunnerConf.__dataclass_fields__["git_repo_url"].default,
            pipeline_mode=pipeline_mode,
            pipeline_num_workers=pipeline_num_workers,
            phase2_append_onboarding=bool(
                self.args.get("phase2_append_onboarding")
            ),
            build_target_from_worktree=build_target_from_worktree,
            target_install_surface=target_install_surface,
            source_package_version=source_package_version,
            target_package_version=target_package_version,
            compat_python_version=compat_python_version,
        )
        conf.runners_nb_path = (
            f"/Users/{username}/dlt_meta_int_tests/backward_compat/{run_id}"
        )
        conf.phase1_output_ws = (
            f"{conf.runners_nb_path}/integration-test-output_phase1.csv"
        )
        conf.phase2_output_ws = (
            f"{conf.runners_nb_path}/integration-test-output_phase2.csv"
        )

        if conf.is_cross_namespace_upgrade and not (
            target_ref.startswith("v0.0.")
            or target_ref.startswith("v0.1")
            or target_ref == "feature/sdp-meta"
        ):
            # Cross-namespace upgrades only work when the target wheel
            # bundles a legacy-namespace compat surface. v0.1.0 and later
            # CURRENT-profile builds do; arbitrary unrecognised refs
            # might not. Surface a warning so the user knows to verify
            # the target wheel actually ships the real ``src`` package
            # before running.
            warnings.warn(
                f"Cross-namespace upgrade requested ({source_profile.name!r} "
                f"-> {target_profile.name!r}) with target_ref={target_ref!r}. "
                "The target wheel MUST bundle a legacy-namespace compat "
                "surface (a real `src` package + `dlt_meta` package "
                "re-exporting `databricks.labs.sdp_meta.*`) for the source "
                "runner notebook's "
                "`from src.* import …` to keep resolving. Verify the "
                "wheel before running, or pin source/target profiles "
                "explicitly.",
                stacklevel=2,
            )

        return conf

    # ----- wheel build + upload ------------------------------------------

    def build_wheels(self, conf: BCRunnerConf) -> None:
        """Build SOURCE main and the requested TARGET install surface.

        Skipped entirely when ``install_mode == "git"`` -- in that case
        runners and wheel-tasks resolve git URLs at install time and
        nothing has to be uploaded.

        The default surface builds one wheel per side. The opt-in
        compatibility surface additionally builds the redirect wheel and
        downloads all target runtime dependency wheels.
        """
        if conf.install_mode == "git":
            print(
                f"=== install_mode=git: skipping local wheel build; "
                f"all installs will resolve from {conf.git_repo_url!r} ==="
            )
            return
        if conf.install_mode == "pypi":
            print(
                "=== install_mode=pypi: skipping local wheel build; "
                f"Phase 1 installs dlt-meta=={conf.source_package_version}, "
                f"Phase 2 installs dlt-meta=={conf.target_package_version} "
                "from the live PyPI index ==="
            )
            return

        builder = GitRefWheelBuilder()
        try:
            # SOURCE wheel always comes from the pinned ref -- it
            # represents the customer's already-released wheel, which
            # has no relationship to local working-tree edits.
            print(
                f"=== Building SOURCE wheel ({conf.source_profile.name}) from "
                f"ref={conf.source_ref!r} ==="
            )
            conf.source_main_whl_local = str(builder.build(conf.source_ref))

            # TARGET wheel: optionally bypass git for unreleased
            # target-side changes still living in the developer's
            # working tree. See ``BCRunnerConf.build_target_from_worktree``.
            if conf.build_target_from_worktree:
                print(
                    f"=== Building TARGET wheel ({conf.target_profile.name}) from "
                    "LOCAL WORKING TREE (--build_target_from_worktree) ==="
                )
                conf.target_main_whl_local = str(builder.build_from_worktree())
                if conf.target_install_surface == "compat_wheelhouse":
                    conf.target_compat_whl_local = str(
                        builder.build_from_worktree(subdir="compat")
                    )
            else:
                print(
                    f"=== Building TARGET wheel ({conf.target_profile.name}) from "
                    f"ref={conf.target_ref!r} ==="
                )
                conf.target_main_whl_local = str(builder.build(conf.target_ref))
                if conf.target_install_surface == "compat_wheelhouse":
                    conf.target_compat_whl_local = str(
                        builder.build(conf.target_ref, subdir="compat")
                    )
            if conf.target_install_surface == "compat_wheelhouse":
                self._resolve_target_package_version(conf)
                self._download_compat_runtime_wheels(conf)
        finally:
            builder.cleanup()

    @staticmethod
    def _wheel_version(wheel_path: str) -> str:
        """Return the version field of a wheel filename.

        Wheel names are ``{dist}-{version}(-{build})?-{py}-{abi}-{plat}.whl``
        with ``-`` as the field separator; neither the distribution name
        nor the (escaped) version contains a bare ``-``, so the second
        ``-``-delimited field is always the version.
        """
        name = os.path.basename(wheel_path)
        parts = name.split("-")
        if not name.endswith(".whl") or len(parts) < 5:
            raise ValueError(f"Unrecognized wheel filename: {name!r}")
        return parts[1]

    def _resolve_target_package_version(self, conf: BCRunnerConf) -> None:
        """Pin ``target_package_version`` to what the TARGET wheels build.

        The compat_wheelhouse Phase 2 install is ``dlt-meta==<version>``,
        resolved offline (``--no-index``) against the uploaded wheelhouse.
        For that to be satisfiable the pinned version MUST equal the
        version baked into BOTH built wheels:

          * the ``dlt-meta`` redirect wheel (provides the ``dlt-meta``
            distribution the install names), and
          * the ``databricks-labs-sdp-meta`` primary wheel (the redirect's
            pinned dependency, and what validate_phase2 asserts on).

        We derive the version from the built wheels (single source of
        truth = the artifacts themselves), fail if the two wheels disagree,
        and -- when the user pinned an explicit ``--target_package_version``
        -- fail if it doesn't match, rather than letting the mismatch
        surface much later as an unsatisfiable on-cluster install.
        """
        primary_version = self._wheel_version(conf.target_main_whl_local)
        compat_version = self._wheel_version(conf.target_compat_whl_local)
        if primary_version != compat_version:
            raise RuntimeError(
                "TARGET wheels disagree on version: "
                f"databricks-labs-sdp-meta=={primary_version} vs "
                f"dlt-meta=={compat_version}. The offline "
                "'dlt-meta==<version>' install would be unsatisfiable; "
                "rebuild both from the same ref/worktree."
            )
        if conf.target_package_version is None:
            conf.target_package_version = primary_version
            print(
                "  target_package_version resolved from built wheels: "
                f"{primary_version}"
            )
        elif conf.target_package_version != primary_version:
            raise RuntimeError(
                f"--target_package_version={conf.target_package_version!r} "
                f"does not match the built TARGET wheels (version "
                f"{primary_version!r}). Omit --target_package_version to "
                "derive it automatically, or pass the version the wheels "
                "actually build."
            )

    def _download_compat_runtime_wheels(self, conf: BCRunnerConf) -> None:
        """Download a complete Linux wheelhouse for Phase 2.

        Serverless pipelines cannot be assumed to have PyPI egress. Resolving
        the local ``dlt-meta`` redirect therefore requires not just the two
        project wheels, but every runtime dependency of the primary wheel.

        The target interpreter is ``conf.compat_python_version`` (default
        CPython 3.12). We accept the interpreter-specific ABI plus the
        stable (``abi3``) and pure-Python (``none``) ABIs, and both a
        conservative and a modern manylinux target plus pure-Python
        (``any``), so a dependency that only ships, say, an ``abi3`` or a
        ``manylinux_2_28`` wheel still resolves. ``--only-binary=:all:``
        makes pip FAIL (rather than fall back to an sdist) if any
        dependency in the closure has no wheel matching this target, so an
        incomplete wheelhouse surfaces here at build time, not later as an
        unsatisfiable on-cluster ``--no-index`` install.
        """
        wheelhouse_dir = os.path.join(
            os.path.dirname(conf.target_main_whl_local),
            f"compat-wheelhouse-{conf.run_id}",
        )
        shutil.rmtree(wheelhouse_dir, ignore_errors=True)
        os.makedirs(wheelhouse_dir, exist_ok=True)
        python_version = conf.compat_python_version
        abi = "cp" + python_version.replace(".", "")
        command = [
            sys.executable,
            "-m",
            "pip",
            "download",
            "--dest",
            wheelhouse_dir,
            "--only-binary=:all:",
            "--python-version",
            python_version,
            "--implementation",
            "cp",
            "--abi",
            abi,
            "--abi",
            "abi3",
            "--abi",
            "none",
            "--platform",
            "manylinux2014_x86_64",
            "--platform",
            "manylinux_2_28_x86_64",
            "--platform",
            "any",
            conf.target_main_whl_local,
            conf.target_compat_whl_local,
        ]
        print(
            "=== Downloading TARGET runtime dependency wheels "
            f"(python={python_version}, abi={abi}/abi3/none) ==="
        )
        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.COMPAT_DOWNLOAD_TIMEOUT_SEC,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                "Timed out after "
                f"{self.COMPAT_DOWNLOAD_TIMEOUT_SEC}s building the "
                "compatibility runtime wheelhouse.\n"
                f"Command: {' '.join(command)}"
            ) from exc
        if proc.returncode != 0:
            raise RuntimeError(
                "Failed to build the compatibility runtime wheelhouse.\n"
                f"Command: {' '.join(command)}\n"
                f"stdout:\n{proc.stdout}\n"
                f"stderr:\n{proc.stderr}"
            )

        project_wheels = {
            os.path.basename(conf.target_main_whl_local),
            os.path.basename(conf.target_compat_whl_local),
        }
        conf.target_dependency_whls_local = sorted(
            os.path.join(wheelhouse_dir, name)
            for name in os.listdir(wheelhouse_dir)
            if name.endswith(".whl") and name not in project_wheels
        )
        self._assert_wheelhouse_complete(conf, wheelhouse_dir)
        print(
            "  -> downloaded "
            f"{len(conf.target_dependency_whls_local)} runtime dependency wheels"
        )

    def _assert_wheelhouse_complete(
        self, conf: BCRunnerConf, wheelhouse_dir: str
    ) -> None:
        """Fail if a direct dependency of the primary wheel is missing.

        ``--only-binary=:all:`` already makes ``pip download`` fail on an
        unresolvable closure, but that is a coarse net. This is a targeted
        sanity check that every UNCONDITIONAL ``Requires-Dist`` the primary
        wheel declares (no environment marker gating it out) actually landed
        a wheel, so a gap is reported by name here rather than surfacing as
        an ``ImportError`` on-cluster. Reads the requirement list from the
        wheel's own metadata instead of hardcoding a single dependency name.
        """
        required = self._primary_wheel_required_dists(conf.target_main_whl_local)
        present = {
            self._canonical_dist(os.path.basename(path).split("-")[0])
            for path in conf.target_dependency_whls_local
        }
        missing = sorted(name for name in required if name not in present)
        if missing:
            raise RuntimeError(
                "Compatibility runtime wheelhouse is incomplete: no wheel "
                f"downloaded for required dependency/-ies {missing}. "
                f"Present dependency wheels: {sorted(present)}."
            )

    @staticmethod
    def _canonical_dist(name: str) -> str:
        """PEP 503-style normalized distribution name (``Foo.Bar`` -> ``foo_bar``).

        Wheel filenames escape the distribution name with ``_``; project
        metadata uses the original punctuation. Normalizing both sides to
        runs-of-[-_.] -> single ``_`` lets them be compared directly.
        """
        return re.sub(r"[-_.]+", "_", name).lower()

    @staticmethod
    def _primary_wheel_required_dists(wheel_path: str) -> set:
        """Unconditional ``Requires-Dist`` distribution names of a wheel.

        Parses ``*.dist-info/METADATA`` from the wheel zip and returns the
        canonicalized names of requirements that carry NO environment marker
        (the part after ``;``) -- i.e. deps that are always installed and
        therefore must be present in an offline wheelhouse. Requirements
        gated by an ``extra`` or a marker are skipped (they aren't pulled by
        a bare install). Returns an empty set if metadata can't be read, so
        the coarse ``--only-binary`` guard remains the backstop.
        """
        import zipfile

        try:
            with zipfile.ZipFile(wheel_path) as zf:
                metadata_name = next(
                    (
                        n
                        for n in zf.namelist()
                        if n.endswith(".dist-info/METADATA")
                    ),
                    None,
                )
                if metadata_name is None:
                    return set()
                metadata = zf.read(metadata_name).decode("utf-8", "replace")
        except (OSError, zipfile.BadZipFile):
            return set()

        required = set()
        for line in metadata.splitlines():
            if not line.startswith("Requires-Dist:"):
                continue
            spec = line.split(":", 1)[1].strip()
            if ";" in spec:  # has an environment marker / extra -> conditional
                continue
            # Name is the leading run of name chars before any version
            # specifier, whitespace, or extras bracket.
            match = re.match(r"[A-Za-z0-9._-]+", spec)
            if match:
                required.add(BackwardCompatRunner._canonical_dist(match.group(0)))
        return required

    def upload_wheel(self, conf: BCRunnerConf, local_path: str, remote_subdir: str) -> str:
        """Upload a single .whl to ``<volume>/wheels/<remote_subdir>/<name>``.

        Returns the absolute UC volume path of the uploaded wheel,
        suitable for ``%pip install`` from a Databricks notebook.
        """
        wheel_name = os.path.basename(local_path)
        remote = f"{conf.uc_volume_path}wheels/{remote_subdir}/{wheel_name}"
        with open(local_path, "rb") as fh:
            self.ws.files.upload(file_path=remote, contents=fh, overwrite=True)
        print(f"  uploaded -> {remote}")
        return remote

    # ----- install-spec resolver ----------------------------------------

    def _git_install_spec(
        self,
        conf: BCRunnerConf,
        ref: str,
        *,
        subdir: Optional[str] = None,
    ) -> str:
        """Build a pip-compatible ``git+<url>@<ref>[#subdirectory=...]`` URL.

        Both ``%pip install`` and ``JobEnvironment.dependencies`` accept
        this form (assuming the cluster has egress to the repo host).
        """
        spec = f"git+{conf.git_repo_url}@{ref}"
        if subdir:
            spec = f"{spec}#subdirectory={subdir}"
        return spec

    def install_spec_source_main(self, conf: BCRunnerConf) -> str:
        """Pip-installable spec for the SOURCE main wheel."""
        if conf.install_mode == "pypi":
            return f"dlt-meta=={conf.source_package_version}"
        if conf.install_mode == "git":
            return self._git_install_spec(conf, conf.source_ref)
        return conf.source_main_whl_remote

    def install_spec_target_main(self, conf: BCRunnerConf) -> str:
        """Pip-installable spec for the TARGET main wheel."""
        if conf.target_install_surface == "compat_wheelhouse":
            return f"dlt-meta=={conf.target_package_version}"
        if conf.install_mode == "pypi":
            # The published ``dlt-meta`` redirect resolves the primary
            # ``databricks-labs-sdp-meta`` distribution from live PyPI —
            # exactly what an existing customer's unpinned upgrade does.
            return f"dlt-meta=={conf.target_package_version}"
        if conf.install_mode == "git":
            return self._git_install_spec(conf, conf.target_ref)
        return conf.target_main_whl_remote

    @staticmethod
    def _notebook_source_for_upload(
        conf: BCRunnerConf,
        notebook_name: str,
        content: bytes,
    ) -> bytes:
        """Force Phase 2 pip install cells to use the local wheelhouse.

        Databricks notebook variable substitution passes each ``$var`` as
        one quoted argument, so a multi-token ``--find-links`` value cannot
        safely live inside the pipeline configuration. The wheelhouse path
        is therefore rendered as a literal in the uploaded copy only.
        ``--no-index`` proves that the uploaded wheelhouse is complete and
        prevents serverless behavior from depending on public-index egress.
        ``--force-reinstall`` is required because serverless environment
        reuse can expose base-runtime distribution metadata to pip without
        making the corresponding module importable from the isolated
        pipeline environment.
        """
        if (
            conf.target_install_surface != "compat_wheelhouse"
            and conf.install_mode != "pypi"
        ):
            return content

        runner_name = os.path.basename(conf.source_profile.runner_notebook_local_path)
        if notebook_name not in (runner_name, "validate_phase2.py"):
            return content

        text = content.decode("utf-8")
        if conf.install_mode == "pypi":
            # Live-PyPI installs keep index access but still need
            # ``--force-reinstall``: serverless environment reuse can
            # expose base-runtime (or prior-update) distribution
            # metadata to pip without making the corresponding module
            # importable from the isolated pipeline environment, so a
            # plain install may be skipped as "already satisfied" and
            # ``databricks.labs.sdp_meta`` then fails to import.
            replacements = {
                "%pip install $dlt_meta_whl": (
                    "%pip install --force-reinstall $dlt_meta_whl"
                ),
                "%pip install $target_main_whl": (
                    "%pip install --force-reinstall $target_main_whl"
                ),
            }
        else:
            replacements = {
                "%pip install $dlt_meta_whl": (
                    f"%pip install --force-reinstall --no-index --find-links "
                    f"{conf.target_wheelhouse_remote} "
                    "$dlt_meta_whl"
                ),
                "%pip install $target_main_whl": (
                    f"%pip install --force-reinstall --no-index --find-links "
                    f"{conf.target_wheelhouse_remote} "
                    "$target_main_whl"
                ),
            }
        old = next((line for line in replacements if line in text), None)
        if old is None:
            raise RuntimeError(
                f"Could not locate the expected %pip install line in {notebook_name!r}."
            )
        return text.replace(old, replacements[old], 1).encode("utf-8")

    # ----- UC + onboarding generation ------------------------------------

    def initialize_uc_resources(self, conf: BCRunnerConf) -> None:
        api = SchemasAPI(self.ws.api_client)
        for s, comment in (
            (conf.sdp_meta_schema, "sdp_meta dataflowspec schema (backward-compat run)"),
            (conf.bronze_schema, "bronze schema (backward-compat run)"),
            (conf.silver_schema, "silver schema (backward-compat run)"),
        ):
            api.create(catalog_name=conf.uc_catalog_name, name=s, comment=comment)
        vol = self.ws.volumes.create(
            catalog_name=conf.uc_catalog_name,
            schema_name=conf.sdp_meta_schema,
            name=conf.uc_volume_name,
            volume_type=VolumeType.MANAGED,
        )
        conf.volume_info = vol
        conf.uc_volume_path = (
            f"/Volumes/{vol.catalog_name}/{vol.schema_name}/{vol.name}/"
        )
        print(f"UC volume ready: {conf.uc_volume_path}")

    def generate_onboarding_files(self, conf: BCRunnerConf) -> None:
        """Render onboarding JSON from the SOURCE profile's templates.

        Each profile owns its onboarding shape (LEGACY templates strip
        v0.1.0-only fields like ``rowFilter``; CURRENT templates use
        the full v0.1.0+ shape). We render from the source profile's
        templates so Phase 1 onboards exactly what the customer would
        have onboarded on the source version.

        The shared current-version CloudFiles fixture also exercises
        append-flow ``source_metadata``, support added after v0.1.0 by
        issue #444. Remove only that nested field for older sources so
        v0.1.0 can establish the Phase 1 baseline; capable source refs
        retain it on their upgrade path. Top-level ``source_metadata``
        remains covered for every source.

        Row filters are intentionally removed for every version because
        this suite compares deterministic physical row counts. Their
        group-dependent visibility is covered by the regular CloudFiles
        integration suite instead.
        """
        subs = {
            "{uc_volume_path}": conf.uc_volume_path,
            "{uc_catalog_name}": conf.uc_catalog_name,
            "{bronze_schema}": conf.bronze_schema,
            "{silver_schema}": conf.silver_schema,
        }
        for tmpl, out in (
            (conf.source_profile.onboarding_a1_template, conf.a1_onboarding_file),
            (conf.source_profile.onboarding_a2_template, conf.a2_onboarding_file),
        ):
            with open(tmpl, "r") as fh:
                text = fh.read()
            for k, v in subs.items():
                text = text.replace(k, v or "")
            payload = json.loads(text)
            removed_metadata, removed_row_filters = (
                self._normalize_source_baseline_fixture(conf, payload)
            )
            if removed_metadata:
                print(
                    "  removed unsupported append-flow source_metadata from "
                    f"{removed_metadata} backward-compatibility flow(s)"
                )
            if removed_row_filters:
                print(
                    "  removed row-filter configuration from "
                    f"{removed_row_filters} backward-compatibility table(s)"
                )
            with open(out, "w") as fh:
                json.dump(payload, fh, indent=4)
            print(
                f"  rendered onboarding from {tmpl} ({conf.source_profile.name}) "
                f"-> {out}"
            )

        if conf.phase2_append_onboarding:
            with open(conf.a1_onboarding_file, "r") as fh:
                phase2_payload = json.load(fh)
            for row in phase2_payload:
                row["bronze_cluster_by_auto"] = True
                row["silver_cluster_by_auto"] = True
            inserted = copy.deepcopy(phase2_payload[0])
            inserted["data_flow_id"] = "schema-evolution-new"
            inserted["data_flow_group"] = "schema_evolution"
            phase2_payload.append(inserted)
            with open(conf.phase2_onboarding_file, "w") as fh:
                json.dump(phase2_payload, fh, indent=4)
            print(
                "  rendered target-only append onboarding -> "
                f"{conf.phase2_onboarding_file}"
            )

    @classmethod
    def _normalize_source_baseline_fixture(
        cls, conf: BCRunnerConf, payload
    ) -> tuple:
        """Apply only the compatibility harness's source-specific exclusions."""
        removed_metadata = 0
        if not conf.source_supports_append_flow_source_metadata:
            removed_metadata = cls._remove_append_flow_source_metadata(payload)
        removed_row_filters = cls._remove_row_filter_configuration(payload)
        return removed_metadata, removed_row_filters

    @staticmethod
    def _remove_append_flow_source_metadata(payload) -> int:
        """Keep source baselines within the pre-#444 append-flow contract."""
        removed = 0
        for row in payload:
            for layer in ("bronze", "silver"):
                for append_flow in row.get(f"{layer}_append_flows") or []:
                    source_details = append_flow.get("source_details") or {}
                    if "source_metadata" in source_details:
                        del source_details["source_metadata"]
                        removed += 1
        return removed

    @staticmethod
    def _remove_row_filter_configuration(payload) -> int:
        """Keep compatibility row counts independent of the caller's groups."""
        removed = 0
        for row in payload:
            for key in (
                "bronze_row_filter",
                "bronze_quarantine_row_filter",
                "silver_row_filter",
                "silver_quarantine_row_filter",
            ):
                if key in row:
                    del row[key]
                    removed += 1
        return removed

    # ----- workspace upload ---------------------------------------------

    def upload_files(self, conf: BCRunnerConf) -> None:
        # Resources (data + ddl) -- the same trees the existing
        # cloudfiles integration test consumes, plus our two
        # ``customers_phase2/`` and ``transactions_phase2/`` staging
        # dirs.
        for root, _dirs, files in os.walk(f"{conf.int_tests_dir}/resources"):
            for fname in files:
                with open(os.path.join(root, fname), "rb") as content:
                    self.ws.files.upload(
                        file_path=f"{conf.uc_volume_path}{root}/{fname}",
                        contents=content,
                        overwrite=True,
                    )

        # Conf (silver transformations + DQE rules + the rendered
        # onboarding files we just generated). Only .json -- the
        # backward-compat test is JSON-only by design (YAML support
        # is a v0.1.0-only addition).
        for root, _dirs, files in os.walk(f"{conf.int_tests_dir}/conf/json"):
            for fname in files:
                if not fname.endswith(".json"):
                    continue
                with open(os.path.join(root, fname), "rb") as content:
                    self.ws.files.upload(
                        file_path=f"{conf.uc_volume_path}{root}/{fname}",
                        contents=content,
                        overwrite=True,
                    )

        if conf.install_mode in ("git", "pypi"):
            print(
                f"  install_mode={conf.install_mode}: skipping wheel uploads "
                "(cluster resolves install specs at install time)."
            )
        else:
            # Wheels go under wheels/<source|target>/ so the same upload
            # routine works for arbitrary version pairs without
            # colliding if source and target build distinct wheels with
            # the same filename (e.g. main vs feature/sdp-meta both
            # producing
            # ``databricks_labs_sdp_meta-0.1.0-py3-none-any.whl``).
            conf.source_main_whl_remote = self.upload_wheel(
                conf, conf.source_main_whl_local, "source"
            )
            if conf.target_install_surface == "compat_wheelhouse":
                # compat_wheelhouse Phase 2 installs
                # ``dlt-meta==<version>`` via ``--find-links
                # <wheelhouse dir>`` (see install_spec_target_main and
                # _notebook_source_for_upload), so the driving path is the
                # wheelhouse DIRECTORY, not any individual wheel's remote
                # path. We upload the primary, redirect, and dependency
                # wheels into that one directory. Retain the two pure-Python
                # project-wheel paths for the Phase 2 legacy entry-point
                # JobEnvironment. Platform-specific transitive wheels remain
                # isolated to the pipeline notebook's offline install because
                # serverless Jobs and Pipelines may use different Python ABIs.
                conf.target_wheelhouse_remote = (
                    f"{conf.uc_volume_path}wheels/target/wheelhouse/"
                )
                conf.target_main_whl_remote = self.upload_wheel(
                    conf, conf.target_main_whl_local, "target/wheelhouse"
                )
                conf.target_compat_whl_remote = self.upload_wheel(
                    conf, conf.target_compat_whl_local, "target/wheelhouse"
                )
                for dependency_whl in conf.target_dependency_whls_local:
                    self.upload_wheel(
                        conf, dependency_whl, "target/wheelhouse"
                    )
            else:
                conf.target_main_whl_remote = self.upload_wheel(
                    conf, conf.target_main_whl_local, "target"
                )

        self.upload_runner_notebooks(conf)

    def upload_runner_notebooks(
        self, conf: BCRunnerConf, *, phase2: bool = False
    ) -> None:
        """Upload runner notebooks for the requested phase.

        Phase 1 must retain the source wheel's original install command.
        At the phase boundary, compatibility-wheelhouse mode replaces the
        uploaded copies with target-only offline install arguments.
        """
        self.ws.workspace.mkdirs(f"{conf.runners_nb_path}/runners")
        local_runners = f"{conf.int_tests_dir}/notebooks/backward_compat_runners"
        for nb in os.listdir(local_runners):
            local_path = os.path.join(local_runners, nb)
            if not os.path.isfile(local_path):
                continue
            with open(local_path, "rb") as fh:
                content = fh.read()
                if phase2:
                    content = self._notebook_source_for_upload(conf, nb, content)
                self.ws.workspace.upload(
                    path=f"{conf.runners_nb_path}/runners/{nb}",
                    format=ImportFormat.SOURCE,
                    language=Language.PYTHON,
                    content=content,
                    overwrite=True,
                )

    # ----- pipeline create / update --------------------------------------

    def _runner_notebook_filename(self, conf: BCRunnerConf) -> str:
        """File name (basename) of the SOURCE profile's runner notebook.

        Used by both ``create_pipeline`` (sets the library path) and
        ``upload_files`` (decides which file to upload from the local
        ``backward_compat_runners/`` directory).
        """
        return os.path.basename(conf.source_profile.runner_notebook_local_path)

    def _build_phase1_pipeline_config(
        self,
        conf: BCRunnerConf,
        layer: str,
        group: str,
    ) -> Dict[str, str]:
        """Pipeline configuration for Phase 1 (SOURCE wheel).

        One key, one wheel: the SOURCE profile's ``pipeline_config_whl_key``
        maps to the SOURCE main wheel path. The SOURCE runner notebook
        reads that key and runs a single ``%pip install $key`` line.

        ``pipelines.maxFlowRetryAttempts=0`` makes DLT fail an update
        on the FIRST flow failure rather than retrying the failing
        flow up to its default attempts. For an integration test
        we want fast, deterministic failure -- a true bug doesn't
        get less true with three more attempts, and waiting through
        the retry budget doubles or triples the diagnose-fix-rerun
        cycle on broken specs.
        """
        return {
            "layer": layer,
            conf.source_profile.pipeline_config_whl_key: (
                self.install_spec_source_main(conf)
            ),
            f"{layer}.group": group,
            f"{layer}.dataflowspecTable": (
                f"{conf.uc_catalog_name}.{conf.sdp_meta_schema}.{layer}_dataflowspec"
            ),
            "pipelines.externalSink.enabled": "true",
            "pipelines.maxFlowRetryAttempts": "0",
        }

    def _build_phase2_pipeline_config(
        self,
        conf: BCRunnerConf,
        layer: str,
        group: str,
    ) -> Dict[str, str]:
        """Pipeline configuration for Phase 2 (TARGET wheel).

        Same SOURCE-profile pipeline-config key as Phase 1 -- the
        runner notebook is the customer's source-version runner and
        only knows the key IT was wired with. We swap the VALUE behind
        that key from the SOURCE wheel path to the TARGET wheel path
        so the same notebook installs the new wheel via the same
        ``%pip install`` line. No second key, no second wheel.

        For LEGACY -> CURRENT cross-namespace upgrades, the TARGET
        wheel is responsible for bundling its own legacy-namespace
        compat surface (a real ``src`` package + ``dlt_meta`` package)
        so the SOURCE runner's ``from src.* import …`` keeps resolving
        through normal import machinery. The orchestrator does not
        deliver a separate companion wheel.
        """
        return {
            "layer": layer,
            conf.source_profile.pipeline_config_whl_key: (
                self.install_spec_target_main(conf)
            ),
            f"{layer}.group": group,
            f"{layer}.dataflowspecTable": (
                f"{conf.uc_catalog_name}.{conf.sdp_meta_schema}.{layer}_dataflowspec"
            ),
            "pipelines.externalSink.enabled": "true",
            # See _build_phase1_pipeline_config for why we disable
            # flow-level retries in this test.
            "pipelines.maxFlowRetryAttempts": "0",
        }

    @staticmethod
    def _pipeline_execution_kwargs(
        conf: BCRunnerConf,
        target_schema: str,
    ) -> Dict[str, object]:
        """Return mode-specific pipeline creation/update fields.

        ``standard_legacy`` deliberately uses ``target=`` directly, rather
        than the schema-first compatibility helper. A workspace that accepts
        ``schema=`` would otherwise create a DPM pipeline and fail to test
        the legacy publishing-mode upgrade contract.
        """
        if conf.is_standard_legacy_mode:
            return {
                "serverless": False,
                "target": target_schema,
                "clusters": [
                    PipelineCluster(
                        label="default",
                        num_workers=conf.pipeline_num_workers,
                    )
                ],
            }
        return {
            "serverless": True,
            "schema": target_schema,
        }

    def _verify_pipeline_mode(
        self,
        conf: BCRunnerConf,
        pipeline_id: str,
        target_schema: str,
        expected_configuration: Dict[str, str],
    ) -> None:
        """Fail fast if the API did not retain the requested pipeline mode.

        The wheel-config-key assertion runs in every mode -- it confirms
        the Phase 1 create (and the Phase 2 swap) actually landed the
        expected wheel path. The compute/publishing checks below are only
        meaningful for ``standard_legacy``.
        """
        spec = self.ws.pipelines.get(pipeline_id).spec
        actual_configuration = getattr(spec, "configuration", None) or {}

        errors = []
        wheel_key = conf.source_profile.pipeline_config_whl_key
        if actual_configuration.get(wheel_key) != expected_configuration.get(wheel_key):
            errors.append(
                f"configuration[{wheel_key!r}]="
                f"{actual_configuration.get(wheel_key)!r}, expected "
                f"{expected_configuration.get(wheel_key)!r}"
            )

        if conf.is_standard_legacy_mode:
            actual_target = getattr(spec, "target", None)
            actual_schema = getattr(spec, "schema", None)
            actual_serverless = getattr(spec, "serverless", None)
            clusters = getattr(spec, "clusters", None) or []
            default_cluster = next(
                (cluster for cluster in clusters if cluster.label == "default"),
                None,
            )
            if actual_serverless is not False:
                errors.append(f"serverless={actual_serverless!r}, expected False")
            if actual_target != target_schema:
                errors.append(f"target={actual_target!r}, expected {target_schema!r}")
            if actual_schema:
                errors.append(f"schema={actual_schema!r}, expected unset")
            if default_cluster is None:
                errors.append("default standard-compute cluster is missing")
            elif default_cluster.num_workers != conf.pipeline_num_workers:
                errors.append(
                    f"default cluster num_workers={default_cluster.num_workers!r}, "
                    f"expected {conf.pipeline_num_workers!r}"
                )

        if errors:
            raise RuntimeError(
                f"Pipeline {pipeline_id!r} did not retain the expected "
                f"{conf.pipeline_mode} configuration: {'; '.join(errors)}"
            )

    def create_pipeline(
        self,
        conf: BCRunnerConf,
        name: str,
        layer: str,
        group: str,
        target_schema: str,
    ) -> str:
        """Create a DLT pipeline pinned to the SOURCE wheel.

        Phase 2 swaps the wheel via ``swap_pipelines_to_target`` rather
        than recreating the pipeline; that preserves the pipeline ID
        AND the per-pipeline DLT checkpoints, which is the customer-
        upgrade scenario we're modelling.
        """
        configuration = self._build_phase1_pipeline_config(conf, layer, group)
        conf._phase1_pipeline_configs[name] = configuration
        runner_path = (
            f"{conf.runners_nb_path}/runners/{self._runner_notebook_filename(conf)}"
        )
        create_kwargs = {
            "catalog": conf.uc_catalog_name,
            "name": name,
            "configuration": configuration,
            "libraries": [
                PipelineLibrary(
                    notebook=NotebookLibrary(path=runner_path)
                )
            ],
            **self._pipeline_execution_kwargs(conf, target_schema),
        }
        if conf.is_standard_legacy_mode:
            created = self.ws.pipelines.create(**create_kwargs)
        else:
            created = sdk_create_pipeline(self.ws, **create_kwargs)
        if created is None or not created.pipeline_id:
            raise RuntimeError(f"Pipeline {name!r} creation failed")
        self._verify_pipeline_mode(
            conf, created.pipeline_id, target_schema, configuration
        )
        print(f"  created pipeline {name!r} -> {created.pipeline_id}")
        return created.pipeline_id

    def create_all_pipelines(self, conf: BCRunnerConf) -> None:
        conf.bronze_a1_pipeline_id = self.create_pipeline(
            conf,
            f"backward-compat-bronze-A1-{conf.run_id}",
            "bronze",
            "A1",
            conf.bronze_schema,
        )
        conf.bronze_a2_pipeline_id = self.create_pipeline(
            conf,
            f"backward-compat-bronze-A2-{conf.run_id}",
            "bronze",
            "A2",
            conf.bronze_schema,
        )
        conf.silver_pipeline_id = self.create_pipeline(
            conf,
            f"backward-compat-silver-{conf.run_id}",
            "silver",
            "A1",
            conf.silver_schema,
        )

    def swap_pipelines_to_target(self, conf: BCRunnerConf) -> None:
        """Update every pipeline's configuration to point at TARGET wheel.

        Pipeline IDs and DLT checkpoints are preserved; only the value
        behind the SOURCE-defined pipeline-config key changes (from
        SOURCE wheel path to TARGET wheel path). The runner notebook,
        the libraries list, the publishing destination, and every other
        pipeline attribute stay byte-identical.

        Note on whl-typed PipelineLibrary entries: serverless DLT
        rejects them with ``InvalidParameterValue: Whl libraries are
        not supported``, so the legacy-namespace compat surface cannot
        be delivered as a separate pipeline library. Bundling it
        directly into the main wheel sidesteps that: the wheel ships a
        real top-level ``src`` package, so after ``%pip install`` the
        runner's ``from src.* import …`` resolves through normal import
        machinery -- no separate library, and no reliance on a ``.pth``
        startup hook (which serverless ``%pip install`` would not fire
        anyway).
        """
        targets = (
            (conf.bronze_a1_pipeline_id, "bronze", "A1", conf.bronze_schema),
            (conf.bronze_a2_pipeline_id, "bronze", "A2", conf.bronze_schema),
            (conf.silver_pipeline_id, "silver", "A1", conf.silver_schema),
        )
        for pid, layer, group, target_schema in targets:
            config = self._build_phase2_pipeline_config(conf, layer, group)
            existing = self.ws.pipelines.get(pid)
            self.ws.pipelines.update(
                pipeline_id=pid,
                catalog=existing.spec.catalog,
                name=existing.spec.name,
                configuration=config,
                libraries=existing.spec.libraries,
                **self._pipeline_execution_kwargs(conf, target_schema),
            )
            self._verify_pipeline_mode(conf, pid, target_schema, config)
            print(
                f"  swapped pipeline {pid} -> "
                f"{conf.target_profile.name} wheel spec"
            )

    # ----- workflow building --------------------------------------------

    def build_phase1_job(self, conf: BCRunnerConf):
        """Phase 1 workflow: onboard A1 -> bronze A1 -> onboard A2 ->
        bronze A2 -> silver -> validate_phase1.

        Mirrors the existing cloudfiles A1+A2 happy path exactly
        (see ``IntegrationTestRunner.create_cloudfiles_workflow_spec``
        in ``run_integration_tests.py``), with EVERY pipeline pinned
        to the SOURCE wheel and the validation notebook set to the
        backward-compat ``validate_phase1.py`` (which persists
        per-table counts to a UC volume scratch file for Phase 2 to
        read back).

        Note: silver runs ONCE, AFTER bronze_A2. We do not run silver
        between bronze_A1 and bronze_A2 because the
        ``customers_silver_flow`` append flow reads from a
        ``customers_delta`` bronze table that is produced by the A2
        onboarding spec, not by A1. Inserting a silver step after A1
        would fail with ``TABLE_OR_VIEW_NOT_FOUND`` on
        ``customers_delta`` -- the canonical cloudfiles workflow has
        the same constraint.
        """
        # Onboarding wheel-tasks must resolve against the SOURCE wheel.
        # The wheel distribution name comes from the source profile
        # (e.g. LEGACY -> "dlt_meta", CURRENT -> "databricks_labs_sdp_meta").
        # The dependency itself is whatever ``install_spec_source_main``
        # returns -- a UC volume path in local mode, a git URL in git mode.
        env_key = "bc_phase1_env"
        environments = [
            jobs.JobEnvironment(
                environment_key=env_key,
                spec=compute.Environment(
                    client="1",
                    dependencies=[self.install_spec_source_main(conf)],
                ),
            )
        ]
        common_named = {
            "database": f"{conf.uc_catalog_name}.{conf.sdp_meta_schema}",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "silver_dataflowspec_table": "silver_dataflowspec",
            "import_author": "backward_compat",
            "version": "v1",
            "env": "it",
            "uc_enabled": "True",
        }
        tasks = [
            jobs.Task(
                task_key="phase1_onboard_A1",
                description=(
                    f"Phase 1 ({conf.source_profile.name} / {conf.source_ref}): "
                    "onboard A1 (initial)"
                ),
                environment_key=env_key,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name=conf.source_profile.distribution,
                    entry_point="run",
                    named_parameters={
                        **common_named,
                        "onboard_layer": "bronze_silver",
                        "onboarding_file_path": (
                            f"{conf.uc_volume_path}{conf.a1_onboarding_file}"
                        ),
                        "overwrite": "True",
                    },
                ),
            ),
            jobs.Task(
                task_key="phase1_bronze_A1",
                depends_on=[jobs.TaskDependency(task_key="phase1_onboard_A1")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.bronze_a1_pipeline_id),
            ),
            jobs.Task(
                task_key="phase1_onboard_A2",
                depends_on=[jobs.TaskDependency(task_key="phase1_bronze_A1")],
                description=(
                    f"Phase 1 ({conf.source_profile.name} / {conf.source_ref}): "
                    "onboard A2 (incremental)"
                ),
                environment_key=env_key,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name=conf.source_profile.distribution,
                    entry_point="run",
                    named_parameters={
                        **common_named,
                        "onboard_layer": "bronze",
                        "onboarding_file_path": (
                            f"{conf.uc_volume_path}{conf.a2_onboarding_file}"
                        ),
                        "overwrite": "False",
                    },
                ),
            ),
            jobs.Task(
                task_key="phase1_bronze_A2",
                depends_on=[jobs.TaskDependency(task_key="phase1_onboard_A2")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.bronze_a2_pipeline_id),
            ),
            jobs.Task(
                task_key="phase1_silver_after_A2",
                depends_on=[jobs.TaskDependency(task_key="phase1_bronze_A2")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.silver_pipeline_id),
            ),
            jobs.Task(
                task_key="phase1_validate",
                depends_on=[jobs.TaskDependency(task_key="phase1_silver_after_A2")],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{conf.runners_nb_path}/runners/validate_phase1.py",
                    base_parameters={
                        "uc_catalog_name": conf.uc_catalog_name,
                        "bronze_schema": conf.bronze_schema,
                        "silver_schema": conf.silver_schema,
                        "sdp_meta_schema": conf.sdp_meta_schema,
                        "uc_volume_path": conf.uc_volume_path,
                        "output_file_path": f"/Workspace{conf.phase1_output_ws}",
                        "run_id": conf.run_id,
                        "source_ref": conf.source_ref,
                    },
                ),
            ),
        ]
        return self.ws.jobs.create(
            name=f"backward-compat-phase1-{conf.run_id}",
            environments=environments,
            tasks=tasks,
        )

    def _phase2_legacy_entrypoint_dependencies(
        self, conf: BCRunnerConf
    ) -> List[str]:
        """Dependencies for exercising the target ``dlt_meta:run`` contract."""
        if not conf.is_cross_namespace_upgrade:
            return []
        if conf.target_install_surface == "compat_wheelhouse":
            required = {
                "target primary wheel": conf.target_main_whl_remote,
                "target compatibility wheel": conf.target_compat_whl_remote,
            }
            missing = [name for name, path in required.items() if not path]
            if missing:
                raise ValueError(
                    "Cannot test the target dlt_meta wheel entry point; "
                    f"missing uploaded {', '.join(missing)}"
                )
            return [
                conf.target_main_whl_remote,
                conf.target_compat_whl_remote,
            ]
        if conf.install_mode == "pypi":
            return [self.install_spec_target_main(conf)]
        return []

    def build_phase2_job(self, conf: BCRunnerConf):
        """Run optional onboarding checks, incremental pipelines, and validation.

        The dataflowspec persisted by Phase 1 remains the input to the target
        pipelines. For cross-namespace upgrades installed through the
        compatibility distribution, Phase 2 first re-runs A1 onboarding with
        ``overwrite=False`` using the legacy ``dlt_meta`` package name and
        ``run`` entry point. This proves an existing v0.0.10 wheel task remains
        executable after the target wheel swap without replacing Phase 1 state.

        ``phase2_append_onboarding`` additionally runs target-wheel onboarding
        twice against the physical Phase 1 tables, proving Issue #460 schema
        evolution and idempotency without changing the default test path.
        """
        legacy_dependencies = self._phase2_legacy_entrypoint_dependencies(conf)
        legacy_env_key = "bc_phase2_legacy_entrypoint_env"
        environments = []
        if legacy_dependencies:
            environments.append(
                jobs.JobEnvironment(
                    environment_key=legacy_env_key,
                    spec=compute.Environment(
                        # Client v2 isolates task dependencies from the
                        # serverless kernel's core packages. Client v1 lets
                        # the target wheel's databricks-sdk dependency shadow
                        # dbruntime dependencies and can prevent Python from
                        # restarting before the entry point is invoked.
                        client="2",
                        dependencies=legacy_dependencies,
                    ),
                )
            )
        bronze_dependency = "phase2_add_incremental"
        tasks = [
            jobs.Task(
                task_key="phase2_add_incremental",
                description=(
                    f"Phase 2 ({conf.target_profile.name} / {conf.target_ref}): "
                    "drop incremental seed"
                ),
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{conf.runners_nb_path}/runners/add_phase2_incremental.py",
                    base_parameters={
                        "uc_volume_path": conf.uc_volume_path,
                        "int_tests_dir": conf.int_tests_dir,
                    },
                ),
            ),
        ]
        if legacy_dependencies:
            tasks.append(
                jobs.Task(
                    task_key="phase2_legacy_entrypoint_onboard",
                    description=(
                        f"Phase 2 ({conf.target_profile.name} / "
                        f"{conf.target_ref}): exercise legacy dlt_meta:run"
                    ),
                    depends_on=[
                        jobs.TaskDependency(
                            task_key="phase2_add_incremental"
                        )
                    ],
                    environment_key=legacy_env_key,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "database": (
                                f"{conf.uc_catalog_name}."
                                f"{conf.sdp_meta_schema}"
                            ),
                            "bronze_dataflowspec_table": (
                                "bronze_dataflowspec"
                            ),
                            "silver_dataflowspec_table": (
                                "silver_dataflowspec"
                            ),
                            "import_author": "backward_compat",
                            "version": "v1",
                            "env": "it",
                            "uc_enabled": "True",
                            "onboard_layer": "bronze_silver",
                            "onboarding_file_path": (
                                f"{conf.uc_volume_path}"
                                f"{conf.a1_onboarding_file}"
                            ),
                            "overwrite": "False",
                        },
                    ),
                )
            )
            bronze_dependency = "phase2_legacy_entrypoint_onboard"

        if conf.phase2_append_onboarding:
            environment_key = "bc_phase2_onboarding_env"
            environments.append(
                jobs.JobEnvironment(
                    environment_key=environment_key,
                    spec=compute.Environment(
                        client="2",
                        dependencies=[self.install_spec_target_main(conf)],
                    ),
                )
            )
            tasks.append(
                jobs.Task(
                    task_key="phase2_append_onboarding",
                    depends_on=[
                        jobs.TaskDependency(task_key=bronze_dependency)
                    ],
                    environment_key=environment_key,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name=conf.target_profile.distribution,
                        entry_point="run",
                        named_parameters={
                            "database": (
                                f"{conf.uc_catalog_name}."
                                f"{conf.sdp_meta_schema}"
                            ),
                            "bronze_dataflowspec_table": (
                                "bronze_dataflowspec"
                            ),
                            "silver_dataflowspec_table": (
                                "silver_dataflowspec"
                            ),
                            "import_author": "backward_compat_phase2",
                            "version": "v2",
                            "env": "it",
                            "uc_enabled": "True",
                            "onboard_layer": "bronze_silver",
                            "onboarding_file_path": (
                                f"{conf.uc_volume_path}"
                                f"{conf.phase2_onboarding_file}"
                            ),
                            "overwrite": "False",
                        },
                    ),
                )
            )
            repeat_onboarding = copy.deepcopy(tasks[-1])
            repeat_onboarding.task_key = (
                "phase2_append_onboarding_idempotency"
            )
            repeat_onboarding.depends_on = [
                jobs.TaskDependency(
                    task_key="phase2_append_onboarding"
                )
            ]
            tasks.append(repeat_onboarding)
            bronze_dependency = (
                "phase2_append_onboarding_idempotency"
            )

        tasks.extend([
            jobs.Task(
                task_key="phase2_bronze",
                depends_on=[
                    jobs.TaskDependency(task_key=bronze_dependency)
                ],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.bronze_a1_pipeline_id),
            ),
            jobs.Task(
                task_key="phase2_silver",
                depends_on=[jobs.TaskDependency(task_key="phase2_bronze")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.silver_pipeline_id),
            ),
            jobs.Task(
                task_key="phase2_validate",
                depends_on=[jobs.TaskDependency(task_key="phase2_silver")],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{conf.runners_nb_path}/runners/validate_phase2.py",
                    base_parameters={
                        "uc_catalog_name": conf.uc_catalog_name,
                        "bronze_schema": conf.bronze_schema,
                        "silver_schema": conf.silver_schema,
                        "sdp_meta_schema": conf.sdp_meta_schema,
                        "uc_volume_path": conf.uc_volume_path,
                        "output_file_path": f"/Workspace{conf.phase2_output_ws}",
                        "run_id": conf.run_id,
                        "phase2_customer_delta": str(conf.phase2_customer_delta),
                        "phase2_transaction_delta": str(conf.phase2_transaction_delta),
                        # validate_phase2 is a regular notebook_task (not
                        # a DLT runner), so it must %pip install the
                        # TARGET wheel itself before doing
                        # ``from src.dataflow_spec import …``.
                        "target_main_whl": self.install_spec_target_main(conf),
                        "target_install_surface": conf.target_install_surface,
                        "source_profile": conf.source_profile.name,
                        "source_ref": conf.source_ref,
                        "target_ref": conf.target_ref,
                        "phase2_append_onboarding": str(
                            conf.phase2_append_onboarding
                        ),
                        # None in primary_wheel mode (validate_phase2 only
                        # reads this in compat_wheelhouse mode, where
                        # build_wheels has pinned it); coerce so the SDK
                        # never sees a null base-parameter value.
                        "target_package_version": conf.target_package_version or "",
                    },
                ),
            ),
        ])
        return self.ws.jobs.create(
            name=f"backward-compat-phase2-{conf.run_id}",
            environments=environments or None,
            tasks=tasks,
        )

    # ----- launch + result download --------------------------------------

    def launch_job(self, job_id: int, label: str):
        print(
            f"=== Launching {label} job_id={job_id} "
            f"(timeout={self.phase_timeout_min}min) ==="
        )
        webbrowser.open(
            f"{self.ws.config.host}/jobs/{job_id}?o={self.ws.get_workspace_id()}"
        )
        run = self.ws.jobs.run_now(job_id=job_id).result(
            timeout=timedelta(minutes=self.phase_timeout_min)
        )
        print(f"=== {label} run finished ===")
        return run

    def download_phase_output(self, ws_path: str, local_name: str) -> str:
        """Download a phase report and fail if it contains failed assertions."""
        content = None
        for attempt in range(1, self.PHASE_OUTPUT_DOWNLOAD_ATTEMPTS + 1):
            try:
                payload = self.ws.workspace.download(ws_path)
                content = payload.read()
                break
            except Exception as exc:
                if attempt == self.PHASE_OUTPUT_DOWNLOAD_ATTEMPTS:
                    raise RuntimeError(
                        f"Could not download backward-compat validation report "
                        f"{ws_path!r} after {attempt} attempts; validation status "
                        "is unavailable."
                    ) from exc
                print(
                    f"  validation report download attempt {attempt} failed "
                    f"for {ws_path}: {exc}; retrying..."
                )
                time.sleep(self.PHASE_OUTPUT_RETRY_DELAY_SEC * attempt)

        assert content is not None
        with open(local_name, "wb") as out:
            out.write(content)
        print(f"  downloaded -> {local_name}")

        with open(local_name, newline="", encoding="utf-8") as report:
            failures = [
                cell
                for row in csv.reader(report)
                for cell in row
                if self.VALIDATION_FAILURE_MARKER in cell
            ]
        if failures:
            details = "\n".join(f"  - {failure}" for failure in failures)
            raise AssertionError(
                f"Backward-compat validation failed in {local_name}:\n{details}"
            )
        return local_name

    # ----- cleanup -------------------------------------------------------

    def cleanup(self, conf: BCRunnerConf) -> None:
        print("=== Cleaning up backward-compat run ===")
        for pid in (
            conf.bronze_a1_pipeline_id,
            conf.bronze_a2_pipeline_id,
            conf.silver_pipeline_id,
        ):
            if pid:
                try:
                    self.ws.pipelines.delete(pid)
                    print(f"  deleted pipeline {pid}")
                except Exception as exc:
                    print(f"  warn: pipeline {pid} delete failed: {exc}")
        for jid in (conf.phase1_job_id, conf.phase2_job_id):
            if jid:
                try:
                    self.ws.jobs.delete(jid)
                    print(f"  deleted job {jid}")
                except Exception as exc:
                    print(f"  warn: job {jid} delete failed: {exc}")
        if conf.uc_catalog_name:
            test_schemas = {conf.sdp_meta_schema, conf.bronze_schema, conf.silver_schema}
            for schema in self.ws.schemas.list(catalog_name=conf.uc_catalog_name):
                if schema.name in test_schemas:
                    print(f"  deleting schema {schema.full_name}")
                    for vol in self.ws.volumes.list(
                        catalog_name=conf.uc_catalog_name, schema_name=schema.name
                    ):
                        try:
                            self.ws.volumes.delete(vol.full_name)
                        except Exception as exc:
                            print(f"    warn: volume delete failed: {exc}")
                    for table in self.ws.tables.list(
                        catalog_name=conf.uc_catalog_name, schema_name=schema.name
                    ):
                        try:
                            self.ws.tables.delete(table.full_name)
                        except Exception as exc:
                            print(f"    warn: table delete failed: {exc}")
                    try:
                        self.ws.schemas.delete(schema.full_name)
                    except Exception as exc:
                        print(f"  warn: schema delete failed: {exc}")
        print("=== Cleanup complete ===")

    # ----- top-level orchestration ---------------------------------------

    def run(self, *, cleanup: bool = False) -> int:
        """Drive the full two-phase test.

        ``cleanup=True`` runs ``cleanup()`` in a ``finally`` block; with
        ``cleanup=False`` (the default) we leave UC state in place so a
        failed run is debuggable. Re-run cleanup later with
        ``integration_tests/cleanup_script.py`` or rerun this script
        with ``--cleanup``.
        """
        conf = self._build_runner_conf()
        worktree_note = (
            " [TARGET wheels: local working tree]"
            if conf.build_target_from_worktree
            else ""
        )
        print(
            f"=== Backward-compat run_id={conf.run_id} "
            f"source={conf.source_ref!r} ({conf.source_profile.name}) -> "
            f"target={conf.target_ref!r} ({conf.target_profile.name}) "
            f"install_mode={conf.install_mode!r} "
            f"target_install_surface={conf.target_install_surface!r}"
            f"{worktree_note} ==="
        )
        if conf.is_cross_namespace_upgrade:
            install_note = (
                "Phase 2 runs one %pip command that resolves the dlt-meta "
                "redirect, primary, and runtime dependency wheels from the "
                "local wheelhouse without public-index access."
                if conf.target_install_surface == "compat_wheelhouse"
                else "Phase 2 installs ONE wheel and runs ONE %pip install."
            )
            print(
                f"  cross-namespace upgrade "
                f"({conf.source_profile.runner_imports_namespace!r} -> "
                f"{conf.target_profile.runner_imports_namespace!r}): "
                "the TARGET wheel is expected to bundle a legacy-namespace "
                "compat surface (a real `src` package + `dlt_meta` package "
                "re-exporting `databricks.labs.sdp_meta.*`) so the source "
                "runner notebook's `from src.* import …` keeps resolving "
                f"via normal import machinery. {install_note}"
            )
        rc = 0
        try:
            self.build_wheels(conf)
            self.initialize_uc_resources(conf)
            self.generate_onboarding_files(conf)
            self.upload_files(conf)
            self.create_all_pipelines(conf)

            phase1_label = (
                f"Phase 1 ({conf.source_profile.name} / {conf.source_ref})"
            )
            phase1_job = self.build_phase1_job(conf)
            conf.phase1_job_id = phase1_job.job_id
            self.launch_job(phase1_job.job_id, label=phase1_label)
            self.download_phase_output(
                conf.phase1_output_ws,
                f"backward_compat_phase1_{conf.run_id}.csv",
            )

            if (
                conf.target_install_surface == "compat_wheelhouse"
                or conf.install_mode == "pypi"
            ):
                print("=== Uploading Phase 2 target-install runner notebooks ===")
                self.upload_runner_notebooks(conf, phase2=True)

            print(
                f"=== Swapping pipelines: {conf.source_profile.name} "
                f"({conf.source_ref}) -> {conf.target_profile.name} "
                f"({conf.target_ref}) ==="
            )
            self.swap_pipelines_to_target(conf)

            phase2_label = (
                f"Phase 2 ({conf.target_profile.name} / {conf.target_ref})"
            )
            phase2_job = self.build_phase2_job(conf)
            conf.phase2_job_id = phase2_job.job_id
            self.launch_job(phase2_job.job_id, label=phase2_label)
            self.download_phase_output(
                conf.phase2_output_ws,
                f"backward_compat_phase2_{conf.run_id}.csv",
            )
        except Exception:
            traceback.print_exc()
            rc = 1
        finally:
            if cleanup:
                self.cleanup(conf)
        return rc


def get_workspace_client(profile: Optional[str]) -> WorkspaceClient:
    if os.environ.get("DATABRICKS_APP_PORT"):
        return WorkspaceClient()
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient(
        host=input("Databricks Workspace URL: "), token=input("Token: ")
    )


def parse_cli() -> dict:
    profile_choices = sorted(KNOWN_PROFILES)
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--uc_catalog_name",
        required=True,
        help="Unity Catalog name to create per-run schemas/volumes under.",
    )
    p.add_argument(
        "--profile",
        default=None,
        help="databricks-cli profile name (defaults to interactive prompt).",
    )
    p.add_argument(
        "--source_version",
        default=DEFAULT_SOURCE_REF,
        help=(
            "Git ref of the SOURCE version to upgrade FROM "
            f"(default: {DEFAULT_SOURCE_REF}). Tag, branch, or SHA."
        ),
    )
    p.add_argument(
        "--target_version",
        default=DEFAULT_TARGET_REF,
        help=(
            "Git ref of the TARGET version to upgrade TO "
            f"(default: {DEFAULT_TARGET_REF}). Tag, branch, or SHA."
        ),
    )
    p.add_argument(
        "--source_profile",
        default=None,
        choices=profile_choices,
        help=(
            "Override profile resolution for --source_version. Useful "
            "when --source_version is a custom branch the registry "
            "doesn't recognize."
        ),
    )
    p.add_argument(
        "--target_profile",
        default=None,
        choices=profile_choices,
        help=(
            "Override profile resolution for --target_version. Useful "
            "when --target_version is a custom branch the registry "
            "doesn't recognize."
        ),
    )
    p.add_argument(
        "--install_mode",
        default="local",
        choices=["local", "git", "pypi"],
        help=(
            "How wheels reach the cluster. "
            "'local' (default): build with GitRefWheelBuilder + upload "
            "to UC volume (matches what real customers do -- install a "
            "pre-built artifact, no GitHub egress required). "
            "'git': skip the local build; runners and wheel-tasks "
            "resolve via 'pip install git+<repo>@<ref>'. Faster local "
            "iteration; the cluster MUST have egress to --git_repo_url. "
            "'pypi': post-release verification -- no build, no upload; "
            "Phase 1 installs dlt-meta==<source_package_version> and "
            "Phase 2 installs dlt-meta==<target_package_version> from "
            "the LIVE PyPI index, proving the published packages "
            "upgrade a running deployment in place. Versions derive "
            "from --source_version/--target_version tags (v stripped) "
            "unless pinned explicitly."
        ),
    )
    p.add_argument(
        "--source_package_version",
        default=None,
        help=(
            "Exact dlt-meta version Phase 1 installs when "
            "--install_mode=pypi (e.g. 0.0.10 -- no 'v' prefix; pip "
            "versions never carry one). Omit to derive it from "
            "--source_version."
        ),
    )
    p.add_argument(
        "--git_repo_url",
        default=BCRunnerConf.__dataclass_fields__["git_repo_url"].default,
        help=(
            "Git repository URL used when --install_mode=git "
            "(default: %(default)s). Ignored when --install_mode=local."
        ),
    )
    p.add_argument(
        "--build_target_from_worktree",
        action="store_true",
        help=(
            "Build the TARGET main wheel from the developer's local "
            "working tree instead of from --target_version. The "
            "SOURCE wheel still comes from --source_version (as a "
            "released customer would have). Use ONLY for iterating on "
            "uncommitted target-side changes (bundled compat shim, "
            "post-rename CLI aliases, etc.) before they're pushed. "
            "Requires --install_mode=local; the produced wheel is "
            "uploaded to UC volume the same way ref-built wheels are."
        ),
    )
    p.add_argument(
        "--target_install_surface",
        default="primary_wheel",
        choices=("primary_wheel", "compat_wheelhouse"),
        help=(
            "How Phase 2 installs the target. 'primary_wheel' (default) "
            "installs the target wheel directly. 'compat_wheelhouse' "
            "builds both local target wheels, downloads their runtime "
            "dependencies, and installs dlt-meta through offline pip "
            "resolution from their UC Volume directory."
        ),
    )
    p.add_argument(
        "--target_package_version",
        default=None,
        help=(
            "Exact dlt-meta version installed by compat_wheelhouse. "
            "Omit (the default) to derive it from the version the TARGET "
            "wheels actually build -- the run then also verifies the "
            "primary and redirect wheels agree. Pass a value only to "
            "assert a specific version (a mismatch fails the run)."
        ),
    )
    p.add_argument(
        "--compat_python_version",
        default=BCRunnerConf.__dataclass_fields__["compat_python_version"].default,
        help=(
            "CPython minor version the compat_wheelhouse is downloaded "
            "for (default: %(default)s). Drives pip download's "
            "--python-version and the cpXY ABI tag. Set this to match the "
            "serverless/DBR interpreter when it differs, so ABI-specific "
            "dependency wheels resolve for the right interpreter."
        ),
    )
    p.add_argument(
        "--pipeline_mode",
        default="serverless_dpm",
        choices=("serverless_dpm", "standard_legacy"),
        help=(
            "Pipeline compute and publishing mode. "
            "'serverless_dpm' (default) creates serverless pipelines with "
            "schema=; 'standard_legacy' creates pipeline-managed standard "
            "compute with target= to test legacy-publishing upgrades."
        ),
    )
    p.add_argument(
        "--pipeline_num_workers",
        type=int,
        default=None,
        help=(
            "Worker count for pipeline-managed standard compute. Required "
            "when --pipeline_mode=standard_legacy."
        ),
    )
    p.add_argument(
        "--phase2_append_onboarding",
        action="store_true",
        help=(
            "After the wheel swap, run target-version onboarding with "
            "overwrite=False before the Phase 2 pipelines. This validates "
            "additive evolution of physical SOURCE dataflow-spec tables."
        ),
    )
    p.add_argument(
        "--phase_timeout_min",
        type=int,
        default=BackwardCompatRunner.DEFAULT_PHASE_TIMEOUT_MIN,
        help=(
            "Per-phase wall-clock timeout in minutes for "
            "jobs.run_now().result() (default: %(default)s). Phase 1 "
            "runs six DLT pipelines sequentially plus a validate "
            "notebook; bump this if your workspace's pipeline cold "
            "starts push the run past the default."
        ),
    )
    p.add_argument(
        "--cleanup",
        action="store_true",
        help=(
            "Delete pipelines/jobs/schemas/volumes after the run "
            "completes (success or fail)."
        ),
    )
    args = vars(p.parse_args())
    validate_uc_identifier(args["uc_catalog_name"], kind="--uc_catalog_name")
    return args


def main() -> int:
    args = parse_cli()
    ws = get_workspace_client(args.get("profile"))
    runner = BackwardCompatRunner(args, ws)
    return runner.run(cleanup=args["cleanup"])


if __name__ == "__main__":
    raise SystemExit(main())
