"""Static quality-engine checks shared by App preview and submission."""

from __future__ import annotations

import os

from databricks.labs.sdp_meta.quality.onboarding_preflight import (
    collect_quality_configuration_errors,
)
from databricks.labs.sdp_meta.quality.registry import (
    deployable_quality_engines,
)

from .path_resolver import _OnboardingFileError


def _verify_quality_configuration(parsed, env, uc_enabled=True):
    """Reject quality configurations that onboarding cannot execute safely."""
    if not isinstance(parsed, list):
        return
    errors = collect_quality_configuration_errors(
        parsed,
        env=env,
        uc_enabled=uc_enabled,
        supported_engines=deployable_quality_engines(
            os.getenv("SDP_META_QUALITY_ENGINE_DEPENDENCY")
        ),
    )
    if errors:
        raise _OnboardingFileError(
            "Invalid quality-engine configuration:\n- "
            + "\n- ".join(errors)
        )
