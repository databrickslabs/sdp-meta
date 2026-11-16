"""Version profiles for the backward-compatibility integration test.

Background
----------

The backward-compat test simulates a customer pipeline upgrade by:

  1. Building a wheel from the SOURCE git ref (the version the customer is
     running today).
  2. Onboarding + running their pipeline on that wheel (Phase 1).
  3. Building a wheel from the TARGET git ref (the version they want to
     upgrade to).
  4. Migrating each existing pipeline in place to the TARGET wheel,
     canonical runner notebook, task package metadata, and configuration key
     without recreating the pipeline (Phase 2).
  5. Asserting data preservation, incremental growth, and dataclass
     compatibility.

Different version-lines of sdp-meta have different "shapes" -- distribution
name, runner-notebook import style, onboarding-spec fields, pipeline-config
keys, companion wheels, etc. A ``VersionProfile`` captures everything the
orchestrator needs to know about ONE version-line so the test stays
generic across upgrade pairs.

Two profiles ship out of the box:

  * ``LEGACY``   -- v0.0.1 through v0.0.10. Dist name ``dlt_meta``, runner
                    imports ``from src.*``, pipeline config key
                    ``dlt_meta_whl``. Phase 1 uses the v0.0.10 customer
                    notebook, adding only ``--force-reinstall`` in PyPI mode
                    to avoid stale serverless environments; Phase 2 migrates
                    it to the canonical target runner.
  * ``CURRENT``  -- v0.1.0 and later. Dist name
                    ``databricks_labs_sdp_meta``, runner imports
                    ``from databricks.labs.sdp_meta.*``, pipeline config
                    key ``sdp_meta_whl``. The v0.1.x wheels still shipped
                    legacy compatibility shims; v0.2.0 removes them. Both use
                    the same canonical integration surface represented here.

Unknown refs (e.g. arbitrary feature branches or SHAs)
fall back to ``CURRENT`` and emit a warning -- callers can override the resolver via the
``--source_profile`` / ``--target_profile`` CLI flags.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class VersionProfile:
    """Per-version-line knobs the orchestrator uses to build/install/run.

    Each field encodes ONE thing that varies across version-lines:

    Attributes:
      name: Short, stable identifier used in CLI flags and logs
        (e.g. ``"legacy"``, ``"current"``).
      description: Human-readable summary surfaced in ``--help`` and
        startup logs.
      ref_prefixes: Tuple of git-ref prefixes this profile owns
        (e.g. ``("v0.0.1", "v0.0.2", ..., "v0.0.10")`` for
        LEGACY). Matched as prefixes so a SHA on a release branch still
        resolves correctly.
      distribution: pip distribution name produced by this version's
        ``setup.py`` (e.g. ``"dlt_meta"`` or ``"databricks_labs_sdp_meta"``).
        Used by the Phase 1 ``PythonWheelTask`` to address the onboarding
        entry point.
      pipeline_config_whl_key: The DLT-pipeline ``configuration`` key the
        runner notebook reads to ``%pip install`` the wheel
        (e.g. ``"dlt_meta_whl"`` or ``"sdp_meta_whl"``). Phase 1 uses
        the SOURCE key; Phase 2 migrates the existing pipeline to the
        TARGET key and runner while preserving its ID and checkpoints.
      runner_notebook_local_path: Path under the repo root to the
        runner-notebook source file uploaded into the pipeline. Each
        profile ships its own runner so the import style + config
        key match.
      onboarding_a1_template: Path to the A1 (initial) onboarding
        template. Each profile uses its own shape so v0.1.0-only fields
        (``rowFilter`` etc.) only appear when the source is CURRENT.
      onboarding_a2_template: Path to the A2 (incremental) onboarding
        template. Same shape constraints as A1.
      exact_refs: Git refs that must match exactly rather than as a prefix.
    """

    name: str
    description: str
    ref_prefixes: Tuple[str, ...]
    distribution: str
    pipeline_config_whl_key: str
    runner_notebook_local_path: str
    onboarding_a1_template: str
    onboarding_a2_template: str
    exact_refs: Tuple[str, ...] = ()


LEGACY = VersionProfile(
    name="legacy",
    description="v0.0.1 through v0.0.10 (dlt_meta distribution, src.* imports)",
    ref_prefixes=(
        "v0.0.1",
        "v0.0.2",
        "v0.0.3",
        "v0.0.4",
        "v0.0.5",
        "v0.0.6",
        "v0.0.7",
        "v0.0.8",
        "v0.0.9",
        "v0.0.10",
    ),
    distribution="dlt_meta",
    pipeline_config_whl_key="dlt_meta_whl",
    runner_notebook_local_path=(
        "integration_tests/notebooks/backward_compat_runners/init_dlt_meta_pipeline.py"
    ),
    onboarding_a1_template=(
        "integration_tests/conf/json/backward_compat-cloudfiles-onboarding.template"
    ),
    onboarding_a2_template=(
        "integration_tests/conf/json/backward_compat-cloudfiles-onboarding_A2.template"
    ),
)


CURRENT = VersionProfile(
    name="current",
    description=(
        "v0.1.0 and later (databricks-labs-sdp-meta distribution, "
        "canonical databricks.labs.sdp_meta imports)"
    ),
    ref_prefixes=(
        "v0.1",     # matches v0.1.0, v0.1.1, ... v0.1.x
        "v0.2",
        "feature/sdp-meta",
    ),
    distribution="databricks_labs_sdp_meta",
    pipeline_config_whl_key="sdp_meta_whl",
    runner_notebook_local_path=(
        "integration_tests/notebooks/backward_compat_runners/init_sdp_meta_pipeline.py"
    ),
    onboarding_a1_template="integration_tests/conf/json/cloudfiles-onboarding.template",
    onboarding_a2_template="integration_tests/conf/json/cloudfiles-onboarding_A2.template",
    exact_refs=("main",),
)

KNOWN_PROFILES = {
    LEGACY.name: LEGACY,
    CURRENT.name: CURRENT,
}


# Public default refs. Surfaced separately so the orchestrator's
# argparse defaults stay readable.
DEFAULT_SOURCE_REF = "v0.0.10"
DEFAULT_TARGET_REF = "main"


def resolve_profile(ref: str, *, profile_override: Optional[str] = None) -> VersionProfile:
    """Resolve a git ref to a :class:`VersionProfile`.

    Resolution order:
      1. If ``profile_override`` is supplied, look it up by name in
         :data:`KNOWN_PROFILES` and return that profile (raises ValueError
         if unknown). This is the explicit-override path used by the
         CLI's ``--source_profile`` / ``--target_profile`` flags.
      2. Otherwise, match ``exact_refs`` first, then find the first profile
         whose ``ref_prefixes`` contains a prefix that ``ref`` starts with
         (longest first, so ``"v0.0.10"`` wins over ``"v0.0.1"``).
      3. If no profile matches, return :data:`CURRENT` and log a
         warning. Custom branches and SHAs typically reflect work on
         top of the latest contract, so CURRENT is the safer default
         than LEGACY.

    Args:
      ref: A git ref the wheel builder will check out (tag, branch,
        or SHA).
      profile_override: Optional explicit profile name to bypass
        prefix-based resolution.

    Returns:
      The matched :class:`VersionProfile`.

    Raises:
      ValueError: If ``profile_override`` is supplied but unknown.
    """
    if profile_override is not None:
        try:
            return KNOWN_PROFILES[profile_override]
        except KeyError as exc:
            valid = ", ".join(sorted(KNOWN_PROFILES))
            raise ValueError(
                f"Unknown profile name: {profile_override!r}. "
                f"Valid options: {valid}."
            ) from exc

    candidates = []
    for profile in KNOWN_PROFILES.values():
        if ref in profile.exact_refs:
            candidates.append((len(ref), profile))
            continue
        for prefix in profile.ref_prefixes:
            if ref == prefix or ref.startswith(f"{prefix}."):
                # Tag-style match: exact or dotted-suffix
                # (so ``v0.0.10`` matches itself and ``v0.0.10.post1``).
                candidates.append((len(prefix), profile))
                break
            if ref == prefix or ref.startswith(f"{prefix}/"):
                # Branch-style match (``feature/sdp-meta`` etc.).
                candidates.append((len(prefix), profile))
                break
    if candidates:
        # Longest prefix wins so ``v0.0.10`` resolves to LEGACY even
        # though both ``v0.0.1`` and ``v0.0.10`` are listed.
        candidates.sort(key=lambda c: c[0], reverse=True)
        return candidates[0][1]

    print(
        f"warn: ref={ref!r} did not match any registered version "
        f"profile; defaulting to {CURRENT.name!r}. Override with "
        "--source_profile / --target_profile if this is wrong."
    )
    return CURRENT
