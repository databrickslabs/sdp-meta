"""Shared onboarding and persisted-field names for quality engines."""

QUALITY_CONFIG_FIELD = "qualityConfig"
QUALITY_ENGINE_SUFFIX = "quality_engine"
QUALITY_RULES_PATH_SUFFIX = "quality_rules_path"
QUALITY_ENGINE_MIGRATION_SUFFIX = "quality_engine_migration"

SUPPORTED_LAKEFLOW_GROUPS = (
    "expect",
    "expect_or_drop",
    "expect_or_fail",
    "expect_or_quarantine",
)

QUALITY_CONFIG_KEYS = (
    "engine",
    "spi_version",
    "plugin_package",
    "plugin_version",
    "engine_version",
    "diagnostic_schema_fingerprint",
    "rule_document",
    "normalized_rules",
    "rules_path",
    "content_hash",
    "validation_policy_version",
    "output_targets",
    "engine_options",
    "migration",
    "migration_fingerprint",
    "completed_migration_fingerprint",
)


def quality_engine_field(layer):
    return f"{layer}_{QUALITY_ENGINE_SUFFIX}"


def quality_rules_path_field(layer, env):
    return f"{layer}_{QUALITY_RULES_PATH_SUFFIX}_{env}"


def quality_migration_field(layer):
    return f"{layer}_{QUALITY_ENGINE_MIGRATION_SUFFIX}"
