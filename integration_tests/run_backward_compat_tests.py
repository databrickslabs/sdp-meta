"""Backward-compatibility integration test: generic SOURCE -> TARGET upgrade.

Goal
----
A customer's existing pipeline running on SOURCE_VERSION must preserve its
data, pipeline IDs, and checkpoints while being migrated in place to
TARGET_VERSION. The v0.2.0 boundary requires replacing legacy runner
notebooks, imports, wheel keys, and task package metadata; onboarding and
checkpoint resets are not required.

Two profiles ship out of the box -- see ``version_profiles.py``:

  * ``LEGACY``   -- v0.0.1 through v0.0.10 (dlt_meta dist, ``from src.*``
                    runner imports, ``dlt_meta_whl`` config key).
  * ``CURRENT``  -- v0.1.0 and later (databricks_labs_sdp_meta dist,
                    ``from databricks.labs.sdp_meta.*`` runner imports,
                    ``sdp_meta_whl`` config key).

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
 ``pipelines.update()`` each pipeline to use the TARGET profile's canonical
 runner and configuration. Pipeline IDs DO NOT change, so DLT checkpoints
 (which are pipeline-scoped) are preserved.]

When the target profile or install spec changes, migrate the existing
onboarding wheel-task metadata before Phase 2. After Phase 2 validates every
SOURCE-persisted row, submit one cloned A1 onboarding task with
``overwrite=False`` to verify the target entry point and dependencies. The
optional append-onboarding path performs that verification itself.

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

Phase 1 preserves the historical source contract (including ``dlt-meta``,
``dlt_meta_whl``, and the legacy runner where applicable). Phase 2 explicitly
migrates the existing pipeline IDs to ``init_sdp_meta_pipeline.py``, the
``sdp_meta_whl`` key, canonical imports, and ``databricks-labs-sdp-meta`` task
metadata. This validates data, checkpoint, and persisted-spec continuity
without retaining the removed compatibility packaging.

Usage
-----

    # Default: v0.0.10 -> main (pre-release canonical target)
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog>

    # Canonical migration: v0.1.1 -> v0.2.0 (shim era -> post-shim)
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog> \\
        --source_version=v0.1.1 \\
        --target_version=v0.2.0

    # Custom branches with explicit profile pins
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog> \\
        --source_version=feature/legacy-bugfix --source_profile=legacy \\
        --target_version=feature/remove-shims --target_profile=current

"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import re
import sys
import time
import traceback
import uuid
import webbrowser
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, Optional

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
from packaging.utils import canonicalize_name  # noqa: E402

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
    template, pipeline-config key) lives on
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
    #                 uploaded; Phase 1 installs the SOURCE profile's
    #                 distribution and Phase 2 installs
    #                 ``databricks-labs-sdp-meta==<target_package_version>``
    #                 straight from the live PyPI index. Requires cluster
    #                 egress to pypi.org.
    install_mode: str = "local"
    git_repo_url: str = "https://github.com/databrickslabs/sdp-meta.git"
    # Exact SOURCE package version Phase 1 installs when
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
    # target-side changes before they're pushed to ``target_ref``. Source
    # ALWAYS comes from the pinned ref -- the source side is the
    # customer's already-released wheel and has nothing to do with
    # local edits.
    build_target_from_worktree: bool = False
    # Exact canonical target package version for ``install_mode == "pypi"``.
    target_package_version: Optional[str] = None

    # Schema names (one per layer + one for dataflowspec). Per-run
    # suffix avoids collisions when two devs hit the same UC catalog.
    sdp_meta_schema: str = ""
    bronze_schema: str = ""
    silver_schema: str = ""

    # Volume + workspace paths.
    uc_volume_name: str = "sdp_meta_files"
    volume_info: Optional[VolumeInfo] = None
    uc_volume_path: str = ""
    runners_nb_path: str = ""
    int_tests_dir: str = "integration_tests"

    # Local + remote wheel paths.
    #
    # ``source_main_whl`` -- main wheel built from --source_version.
    # ``target_main_whl`` -- main wheel built from --target_version.
    #
    source_main_whl_local: str = ""
    target_main_whl_local: str = ""
    source_main_whl_remote: str = ""
    target_main_whl_remote: str = ""

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
    def is_standard_legacy_mode(self) -> bool:
        return self.pipeline_mode == "standard_legacy"

    @property
    def source_supports_append_flow_source_metadata(self) -> bool:
        """Whether SOURCE includes the append-flow metadata fix from #444."""
        if self.source_profile.name == "legacy":
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
                package_flag = flag.replace(
                    "_version", "_package_version"
                )
                raise ValueError(
                    "--install_mode=pypi requires a released package "
                    f"version, but {flag}={ref!r} is not a version tag. "
                    f"Pass a release tag with {flag} (for example v0.2.0) "
                    f"or set {package_flag} explicitly. Pip versions carry "
                    "no 'v' prefix "
                    "(databricks-labs-sdp-meta==0.2.0, not ==v0.2.0)."
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
            source_package_version=source_package_version,
            target_package_version=target_package_version,
        )
        conf.runners_nb_path = (
            f"/Users/{username}/sdp_meta_int_tests/backward_compat/{run_id}"
        )
        conf.phase1_output_ws = (
            f"{conf.runners_nb_path}/integration-test-output_phase1.csv"
        )
        conf.phase2_output_ws = (
            f"{conf.runners_nb_path}/integration-test-output_phase2.csv"
        )

        return conf

    # ----- wheel build + upload ------------------------------------------

    def build_wheels(self, conf: BCRunnerConf) -> None:
        """Build SOURCE and TARGET main wheels.

        Skipped entirely when ``install_mode == "git"`` -- in that case
        runners and wheel-tasks resolve git URLs at install time and
        nothing has to be uploaded.

        Historical package coordinates are retained only for the SOURCE.
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
                f"Phase 1 installs {self.install_spec_source_main(conf)}, "
                "Phase 2 installs databricks-labs-sdp-meta=="
                f"{conf.target_package_version} "
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
            else:
                print(
                    f"=== Building TARGET wheel ({conf.target_profile.name}) from "
                    f"ref={conf.target_ref!r} ==="
                )
                conf.target_main_whl_local = str(builder.build(conf.target_ref))
        finally:
            builder.cleanup()

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
    ) -> str:
        """Build a pip-compatible ``git+<url>@<ref>`` URL.

        Both ``%pip install`` and ``JobEnvironment.dependencies`` accept
        this form (assuming the cluster has egress to the repo host).
        """
        return f"git+{conf.git_repo_url}@{ref}"

    @staticmethod
    def _pypi_install_spec(profile: VersionProfile, version: str) -> str:
        """Return a normalized PyPI requirement for a version profile."""
        return f"{canonicalize_name(profile.distribution)}=={version}"

    def install_spec_source_main(self, conf: BCRunnerConf) -> str:
        """Pip-installable spec for the SOURCE main wheel."""
        if conf.install_mode == "pypi":
            return self._pypi_install_spec(
                conf.source_profile, conf.source_package_version
            )
        if conf.install_mode == "git":
            return self._git_install_spec(conf, conf.source_ref)
        return conf.source_main_whl_remote

    def install_spec_target_main(self, conf: BCRunnerConf) -> str:
        """Pip-installable spec for the TARGET main wheel."""
        if conf.install_mode == "pypi":
            return self._pypi_install_spec(
                conf.target_profile, conf.target_package_version
            )
        if conf.install_mode == "git":
            return self._git_install_spec(conf, conf.target_ref)
        return conf.target_main_whl_remote

    def _requires_job_migration(self, conf: BCRunnerConf) -> bool:
        """Return whether Phase 2 changes the onboarding install surface."""
        return (
            conf.source_profile.name != conf.target_profile.name
            or self.install_spec_source_main(conf)
            != self.install_spec_target_main(conf)
        )

    @staticmethod
    def _notebook_source_for_upload(
        conf: BCRunnerConf,
        notebook_name: str,
        content: bytes,
        *,
        phase2: bool,
    ) -> bytes:
        """Force live-PyPI runner installs to reinstall the selected wheel.

        ``--force-reinstall`` is required because serverless environment
        reuse can expose base-runtime distribution metadata to pip without
        making the corresponding module importable from the isolated
        pipeline environment.
        """
        if conf.install_mode != "pypi":
            return content

        profile = conf.target_profile if phase2 else conf.source_profile
        runner_name = os.path.basename(profile.runner_notebook_local_path)
        validator_name = "validate_phase2.py" if phase2 else None
        if notebook_name not in (runner_name, validator_name):
            return content

        text = content.decode("utf-8")
        wheel_variable = (
            profile.pipeline_config_whl_key
            if notebook_name == runner_name
            else "target_main_whl"
        )
        install_line = f"%pip install ${wheel_variable}"
        if install_line not in text:
            raise RuntimeError(
                f"Could not locate the expected %pip install line in {notebook_name!r}."
            )
        force_reinstall_line = (
            f"%pip install --force-reinstall ${wheel_variable}"
        )
        return text.replace(
            install_line, force_reinstall_line, 1
        ).encode("utf-8")

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
            conf.target_main_whl_remote = self.upload_wheel(
                conf, conf.target_main_whl_local, "target"
            )

        self.upload_runner_notebooks(conf)

    def upload_runner_notebooks(
        self, conf: BCRunnerConf, *, phase2: bool = False
    ) -> None:
        """Upload runner notebooks for the requested phase.

        In PyPI mode, both the Phase 1 source runner and the Phase 2 target
        runner use force-reinstall to avoid stale serverless environments.
        """
        self.ws.workspace.mkdirs(f"{conf.runners_nb_path}/runners")
        local_runners = f"{conf.int_tests_dir}/notebooks/backward_compat_runners"
        for nb in os.listdir(local_runners):
            local_path = os.path.join(local_runners, nb)
            if not os.path.isfile(local_path):
                continue
            with open(local_path, "rb") as fh:
                content = fh.read()
                content = self._notebook_source_for_upload(
                    conf, nb, content, phase2=phase2
                )
                self.ws.workspace.upload(
                    path=f"{conf.runners_nb_path}/runners/{nb}",
                    format=ImportFormat.SOURCE,
                    language=Language.PYTHON,
                    content=content,
                    overwrite=True,
                )

    # ----- pipeline create / update --------------------------------------

    @staticmethod
    def _runner_path(conf: BCRunnerConf, profile: VersionProfile) -> str:
        """Workspace path of a profile's uploaded pipeline runner."""
        runner = os.path.basename(profile.runner_notebook_local_path)
        return f"{conf.runners_nb_path}/runners/{runner}"

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

        Phase 2 deliberately uses the TARGET profile's canonical wheel key.
        The pipeline ID remains unchanged, preserving its checkpoints and
        tables, while its runner/config contract moves off historical names.
        """
        return {
            "layer": layer,
            conf.target_profile.pipeline_config_whl_key: (
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
        expected_profile: VersionProfile,
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
        wheel_key = expected_profile.pipeline_config_whl_key
        if actual_configuration.get(wheel_key) != expected_configuration.get(wheel_key):
            errors.append(
                f"configuration[{wheel_key!r}]="
                f"{actual_configuration.get(wheel_key)!r}, expected "
                f"{expected_configuration.get(wheel_key)!r}"
            )
        unexpected_wheel_keys = {
            profile.pipeline_config_whl_key
            for profile in KNOWN_PROFILES.values()
            if profile.pipeline_config_whl_key != wheel_key
        }
        retained_wheel_keys = sorted(
            key for key in unexpected_wheel_keys
            if key in actual_configuration
        )
        if retained_wheel_keys:
            errors.append(
                "legacy/foreign wheel configuration keys remain: "
                f"{retained_wheel_keys}"
            )
        expected_runner = os.path.splitext(os.path.basename(
            expected_profile.runner_notebook_local_path
        ))[0]
        actual_notebooks = [
            os.path.splitext(os.path.basename(library.notebook.path))[0]
            for library in (getattr(spec, "libraries", None) or [])
            if getattr(library, "notebook", None)
            and getattr(library.notebook, "path", None)
        ]
        if actual_notebooks != [expected_runner]:
            errors.append(
                f"notebook libraries={actual_notebooks!r}, expected "
                f"[{expected_runner!r}]"
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
        runner_path = self._runner_path(conf, conf.source_profile)
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
            conf,
            created.pipeline_id,
            target_schema,
            configuration,
            conf.source_profile,
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
        """Move existing pipelines to the canonical TARGET runner/config.

        Pipeline IDs, publishing destinations, tables, and checkpoints are
        preserved. The notebook library and wheel key are intentionally
        replaced at the phase boundary.
        """
        targets = (
            (conf.bronze_a1_pipeline_id, "bronze", "A1", conf.bronze_schema),
            (conf.bronze_a2_pipeline_id, "bronze", "A2", conf.bronze_schema),
            (conf.silver_pipeline_id, "silver", "A1", conf.silver_schema),
        )
        target_runner_path = self._runner_path(conf, conf.target_profile)
        for pid, layer, group, target_schema in targets:
            config = self._build_phase2_pipeline_config(conf, layer, group)
            existing = self.ws.pipelines.get(pid)
            self.ws.pipelines.update(
                pipeline_id=pid,
                catalog=existing.spec.catalog,
                name=existing.spec.name,
                configuration=config,
                libraries=[
                    PipelineLibrary(
                        notebook=NotebookLibrary(path=target_runner_path)
                    )
                ],
                **self._pipeline_execution_kwargs(conf, target_schema),
            )
            self._verify_pipeline_mode(
                conf,
                pid,
                target_schema,
                config,
                conf.target_profile,
            )
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

    def migrate_phase1_job_to_target(
        self, conf: BCRunnerConf
    ) -> jobs.JobSettings:
        """Migrate and verify existing wheel-task metadata in place.

        The Phase 1 job models a customer's existing onboarding job. After
        Phase 1 completes, retain its job ID and task graph while replacing
        the source distribution, environment dependency, and environment
        runtime version with the target profile's canonical values.
        """
        if conf.phase1_job_id is None:
            raise RuntimeError("Phase 1 job ID is unavailable for migration")

        existing = self.ws.jobs.get(conf.phase1_job_id)
        settings = existing.settings
        tasks = copy.deepcopy(settings.tasks or [])
        environments = copy.deepcopy(settings.environments or [])
        wheel_tasks = [
            task for task in tasks
            if task.python_wheel_task is not None
        ]
        if not wheel_tasks:
            raise RuntimeError(
                f"Phase 1 job {conf.phase1_job_id} has no Python wheel tasks "
                "to migrate"
            )

        source_install_spec = self.install_spec_source_main(conf)
        target_install_spec = self.install_spec_target_main(conf)
        wheel_environment_keys = {
            task.environment_key for task in wheel_tasks
            if task.environment_key
        }
        for task in wheel_tasks:
            task.python_wheel_task.package_name = (
                conf.target_profile.distribution
            )
        for environment in environments:
            if environment.environment_key not in wheel_environment_keys:
                continue
            if environment.spec is None:
                raise RuntimeError(
                    f"Job environment {environment.environment_key!r} "
                    "has no environment spec"
                )
            dependencies = list(environment.spec.dependencies or [])
            if source_install_spec not in dependencies:
                raise RuntimeError(
                    f"Job environment {environment.environment_key!r} does "
                    f"not contain source dependency {source_install_spec!r}"
                )
            environment.spec.environment_version = "2"
            environment.spec.client = None
            environment.spec.dependencies = [
                target_install_spec
                if dependency == source_install_spec
                else dependency
                for dependency in dependencies
            ]

        self.ws.jobs.update(
            job_id=conf.phase1_job_id,
            new_settings=jobs.JobSettings(
                tasks=tasks,
                environments=environments,
            ),
        )

        migrated = self.ws.jobs.get(conf.phase1_job_id).settings
        migrated_wheel_tasks = [
            task for task in (migrated.tasks or [])
            if task.python_wheel_task is not None
        ]
        wrong_packages = {
            task.python_wheel_task.package_name
            for task in migrated_wheel_tasks
            if task.python_wheel_task.package_name
            != conf.target_profile.distribution
        }
        if len(migrated_wheel_tasks) != len(wheel_tasks) or wrong_packages:
            raise RuntimeError(
                f"Job {conf.phase1_job_id} did not retain canonical wheel "
                f"task package metadata; unexpected packages={wrong_packages}"
            )
        migrated_environments = {
            environment.environment_key: environment
            for environment in (migrated.environments or [])
        }
        expected_dependencies = {
            environment.environment_key: list(
                environment.spec.dependencies or []
            )
            for environment in environments
            if (
                environment.environment_key in wheel_environment_keys
                and environment.spec is not None
            )
        }
        for environment_key in wheel_environment_keys:
            environment = migrated_environments.get(environment_key)
            dependencies = (
                environment.spec.dependencies
                if environment is not None and environment.spec is not None
                else None
            )
            environment_version = (
                environment.spec.environment_version
                if environment is not None and environment.spec is not None
                else None
            )
            client = (
                environment.spec.client
                if environment is not None and environment.spec is not None
                else None
            )
            if environment_version != "2" and client != "2":
                raise RuntimeError(
                    f"Job environment {environment_key!r} has "
                    f"environment_version={environment_version!r} and "
                    f"client={client!r}; expected either field to be '2'"
                )
            expected = expected_dependencies[environment_key]
            if dependencies != expected:
                raise RuntimeError(
                    f"Job environment {environment_key!r} dependencies="
                    f"{dependencies!r}, expected {expected!r}"
                )
        print(
            f"  migrated job {conf.phase1_job_id} wheel tasks -> "
            f"{conf.target_profile.distribution}"
        )
        return migrated

    def run_migrated_onboarding_smoke(
        self,
        conf: BCRunnerConf,
        settings: jobs.JobSettings,
    ) -> None:
        """Run one migrated onboarding task without changing Phase 1 state."""
        if conf.phase1_job_id is None:
            raise RuntimeError("Phase 1 job ID is unavailable for smoke test")

        source_task = next(
            (
                task
                for task in (settings.tasks or [])
                if (
                    task.task_key == "phase1_onboard_A1"
                    and task.python_wheel_task is not None
                )
            ),
            None,
        )
        if source_task is None:
            raise RuntimeError(
                f"Migrated job {conf.phase1_job_id} has no "
                "phase1_onboard_A1 wheel task to test"
            )
        named_parameters = dict(
            source_task.python_wheel_task.named_parameters or {}
        )
        named_parameters["overwrite"] = "False"
        python_wheel_task = copy.deepcopy(source_task.python_wheel_task)
        python_wheel_task.named_parameters = named_parameters
        task = jobs.SubmitTask(
            task_key="migrated_onboarding_smoke",
            python_wheel_task=python_wheel_task,
            environment_key=source_task.environment_key,
            depends_on=None,
        )

        environment = next(
            (
                copy.deepcopy(candidate)
                for candidate in (settings.environments or [])
                if candidate.environment_key == task.environment_key
            ),
            None,
        )
        if environment is None:
            raise RuntimeError(
                f"Migrated task environment {task.environment_key!r} "
                "is unavailable"
            )

        print("=== Running migrated onboarding wheel-task smoke test ===")
        run = self.ws.jobs.submit(
            run_name=f"backward-compat-migrated-onboarding-{conf.run_id}",
            tasks=[task],
            environments=[environment],
        ).result(timeout=timedelta(minutes=self.phase_timeout_min))
        result_state = run.state.result_state if run.state else None
        if result_state != jobs.RunResultState.SUCCESS:
            raise RuntimeError(
                "Migrated onboarding wheel-task smoke test finished with "
                f"result_state={result_state!r}, expected SUCCESS"
            )
        print("=== Migrated onboarding wheel-task smoke test succeeded ===")

    def build_phase2_job(self, conf: BCRunnerConf):
        """Run optional onboarding checks, incremental pipelines, and validation.

        The dataflowspec persisted by Phase 1 remains the input to the target
        pipelines. ``phase2_append_onboarding`` runs canonical target-wheel
        onboarding twice against the physical Phase 1 tables, proving Issue
        #460 schema evolution and idempotency without replacing Phase 1 state.
        """
        environments = []
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
                        # canonical package imports.
                        "target_main_whl": self.install_spec_target_main(conf),
                        "source_profile": conf.source_profile.name,
                        "source_ref": conf.source_ref,
                        "target_ref": conf.target_ref,
                        "phase2_append_onboarding": str(
                            conf.phase2_append_onboarding
                        ),
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
                        self.ws.schemas.delete(schema.full_name, force=True)
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
        target_note = (
            " [TARGET: main pre-release]"
            if conf.target_ref == "main"
            else ""
        )
        print(
            f"=== Backward-compat run_id={conf.run_id} "
            f"source={conf.source_ref!r} ({conf.source_profile.name}) -> "
            f"target={conf.target_ref!r} ({conf.target_profile.name}) "
            f"install_mode={conf.install_mode!r} "
            f"{worktree_note}{target_note} ==="
        )
        rc = 0
        migrated_settings = None
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
            if self._requires_job_migration(conf):
                print("=== Migrating existing job wheel-task metadata ===")
                migrated_settings = self.migrate_phase1_job_to_target(conf)

            if conf.install_mode == "pypi":
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
            if (
                migrated_settings is not None
                and not conf.phase2_append_onboarding
            ):
                self.run_migrated_onboarding_smoke(
                    conf, migrated_settings
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
            "Phase 1 installs the SOURCE profile's distribution and "
            "Phase 2 installs databricks-labs-sdp-meta=="
            "<target_package_version> from "
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
            "Exact SOURCE package version Phase 1 installs when "
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
            "uncommitted target-side changes before they're pushed. "
            "Requires --install_mode=local; the produced wheel is "
            "uploaded to UC volume the same way ref-built wheels are."
        ),
    )
    p.add_argument(
        "--target_package_version",
        default=None,
        help=(
            "Exact databricks-labs-sdp-meta version Phase 2 installs when "
            "--install_mode=pypi. Omit to derive it from --target_version."
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
