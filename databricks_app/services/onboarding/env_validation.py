"""Detect + validate the per-row ``env`` suffix in an onboarding spec.

The SDP-META onboarding parser requires per-row fields suffixed with
the active environment, e.g. ``bronze_database_demo`` when ``env=demo``.
If the form's Environment value doesn't match the suffix present on
these fields, the parser silently ``continue``s past every row (see
``onboard_dataflowspec.py:1139`` \u2014 added for multi-source CDC support)
and the dataflowspec tables come out empty WITH THE JOB STILL REPORTING
SUCCESS \u2014 the worst possible failure mode for a demo onboarding.

We detect the suffix(es) present in the template and pre-flight-reject
any Environment value that doesn't match.
"""

from __future__ import annotations

from .path_resolver import _OnboardingFileError


# Ordering matters: longer prefixes are matched first so e.g.
# ``bronze_database_quarantine_demo`` resolves to the
# ``bronze_database_quarantine`` template (suffix ``demo``), not the
# ``bronze_database`` template (which would capture ``quarantine_demo``).
_ENV_REQUIRED_FIELD_PREFIXES = (
    "bronze_data_quality_expectations_json",
    "silver_data_quality_expectations_json",
    "bronze_quality_rules_path",
    "silver_quality_rules_path",
    "bronze_database_quarantine",
    "bronze_catalog_quarantine",
    "bronze_quarantine_table_path",
    "silver_transformation_json",
    "bronze_table_path",
    "silver_table_path",
    "bronze_database",
    "bronze_catalog",
    "silver_database",
    "silver_catalog",
    "source_catalog",
    "source_path",
)


def _detect_env_suffixes(parsed):
    """Scan a parsed onboarding spec (list of row-dicts) for the env
    suffix(es) actually used on env-aware field names. Returns a sorted
    list of detected suffixes \u2014 typically a single-element list like
    ``['demo']`` or ``['prod']``, but could be empty (template has no
    env-aware fields \u2014 usually a multi-source-CDC silver-only spec) or
    multi-element (mixed suffixes \u2014 almost always a user error).

    The detection rule: each key on each row that starts with one of
    ``_ENV_REQUIRED_FIELD_PREFIXES`` followed by ``_`` contributes the
    remainder (everything after the prefix's trailing underscore) as a
    candidate env. Single-word remainders only \u2014 env names in practice
    are bare identifiers (``demo``, ``prod``, ``dev``, ``uat``); any
    remainder containing ``_`` is a longer-prefix field we haven't
    listed and is silently ignored rather than mis-classified.
    """
    if not isinstance(parsed, list):
        return []
    sorted_prefixes = sorted(_ENV_REQUIRED_FIELD_PREFIXES, key=len, reverse=True)
    suffixes = set()
    for row in parsed:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            for prefix in sorted_prefixes:
                marker = prefix + "_"
                if key.startswith(marker):
                    candidate = key[len(marker):]
                    if candidate and "_" not in candidate:
                        suffixes.add(candidate)
                    break
    return sorted(suffixes)


def _verify_env_matches_template(parsed, env):
    """Compare the user's ``env`` form value against the env suffix(es)
    actually present in ``parsed``. Raise :class:`_OnboardingFileError`
    with a clear, actionable message when they don't match \u2014 never the
    silent ``continue`` that produces empty tables.

    Pass when ``parsed`` has no env-aware fields at all (e.g. a multi-
    source-CDC silver-only spec) \u2014 those templates legitimately have
    nothing to validate against, and the parser handles them fine.
    """
    detected = _detect_env_suffixes(parsed)
    if not detected:
        return
    if env in detected:
        return
    if len(detected) == 1:
        suggestion = (
            f"Set the Environment field to '{detected[0]}' (the only suffix "
            f"present in your onboarding template)."
        )
    else:
        suggestion = (
            f"Your template mixes multiple environment suffixes "
            f"({', '.join(detected)}); pick one and remove the others, "
            f"or set Environment to one of them and re-run for each."
        )
    raise _OnboardingFileError(
        f"Environment '{env}' does not match the suffix(es) in your "
        f"onboarding template. Detected: {detected}. "
        f"The onboarding parser would skip every row (it requires "
        f"'bronze_database_{env}', 'source_path_{env}', etc.) and the "
        f"dataflowspec tables would come out empty even though the "
        f"job reports SUCCESS. {suggestion}"
    )
