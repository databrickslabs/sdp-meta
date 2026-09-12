"""Validation and immutable snapshots for the built-in Lakeflow engine."""

import hashlib
import json
import logging
import re

import pyspark
from pyspark.sql.types import BooleanType, StructType

from databricks.labs.sdp_meta.quality.fields import (
    QUALITY_CONFIG_KEYS,
    SUPPORTED_LAKEFLOW_GROUPS,
)
from databricks.labs.sdp_meta.quality.migration import migration_fingerprint
from databricks.labs.sdp_meta.quality.spi import SPI_API_VERSION


logger = logging.getLogger("databricks.labs.sdp_meta")

RULE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RANDOM_FUNCTION_PATTERN = re.compile(
    r"\b(rand|randn|uuid|shuffle|monotonically_increasing_id)\s*\(",
    re.IGNORECASE,
)
TIME_FUNCTION_PATTERN = re.compile(
    r"\b(current_timestamp|current_date|now)\s*\(",
    re.IGNORECASE,
)
VALIDATION_POLICY_VERSION = "1"
LAKEFLOW_SPI_VERSION = SPI_API_VERSION
LAKEFLOW_DIAGNOSTIC_SCHEMA = "array<struct<name:string,expression:string>>"
LAKEFLOW_DIAGNOSTIC_SCHEMA_FINGERPRINT = hashlib.sha256(
    LAKEFLOW_DIAGNOSTIC_SCHEMA.encode("utf-8")
).hexdigest()


class InvalidQualityConfiguration(ValueError):
    """Raised when onboarding or runtime quality configuration is invalid."""


def normalize_expression(expression):
    """Return the exact null-safe expression used by routing and diagnostics."""
    return f"COALESCE(({expression}), FALSE)"


def _analyze_expression(spark, schema, expression, rule_name):
    try:
        result_type = (
            spark.createDataFrame([], schema)
            .selectExpr(f"({expression}) AS __quality_result")
            .schema["__quality_result"]
            .dataType
        )
    except Exception as err:
        if (
            "PARSE_SYNTAX_ERROR" in str(err)
            or err.__class__.__name__ == "ParseException"
        ):
            raise InvalidQualityConfiguration(
                f"Quality rule '{rule_name}' has invalid Spark SQL: {err}"
            ) from err
        raise InvalidQualityConfiguration(
            f"Quality rule '{rule_name}' cannot be analyzed against the source schema: {err}"
        ) from err
    if not isinstance(result_type, BooleanType):
        raise InvalidQualityConfiguration(
            f"Quality rule '{rule_name}' must return BOOLEAN, got "
            f"{result_type.simpleString()}"
        )


def validate_lakeflow_rules(spark, document, schema=None):
    """Validate four-group Lakeflow rules and return normalized expressions."""
    if not isinstance(document, dict):
        raise InvalidQualityConfiguration("Lakeflow quality rules must be an object")
    unsupported = set(document) - set(SUPPORTED_LAKEFLOW_GROUPS)
    if unsupported:
        raise InvalidQualityConfiguration(
            f"Unsupported Lakeflow quality rule groups: {sorted(unsupported)}"
        )
    if not document:
        raise InvalidQualityConfiguration("Lakeflow quality rules cannot be empty")

    normalized = {}
    seen_names = set()
    for group in SUPPORTED_LAKEFLOW_GROUPS:
        rules = document.get(group, {})
        if rules is None:
            rules = {}
        if not isinstance(rules, dict):
            raise InvalidQualityConfiguration(
                f"Lakeflow quality group '{group}' must be an object"
            )
        normalized[group] = {}
        for rule_name, expression in rules.items():
            if not isinstance(rule_name, str) or not RULE_NAME_PATTERN.fullmatch(rule_name):
                raise InvalidQualityConfiguration(
                    f"Invalid quality rule name '{rule_name}'"
                )
            if rule_name in seen_names:
                raise InvalidQualityConfiguration(
                    f"Duplicate quality rule name '{rule_name}'"
                )
            seen_names.add(rule_name)
            if not isinstance(expression, str) or not expression.strip():
                raise InvalidQualityConfiguration(
                    f"Quality rule '{rule_name}' must contain a non-empty SQL expression"
                )
            if RANDOM_FUNCTION_PATTERN.search(expression):
                raise InvalidQualityConfiguration(
                    f"Quality rule '{rule_name}' uses a non-deterministic random function"
                )
            if TIME_FUNCTION_PATTERN.search(expression):
                logger.warning(
                    "Quality rule '%s' uses a time-dependent function and may "
                    "evaluate differently across independent flows",
                    rule_name,
                )
            if schema is not None:
                _analyze_expression(spark, schema, expression, rule_name)
            normalized[group][rule_name] = normalize_expression(expression)
    return normalized, schema is None


def analyze_lakeflow_normalized_rules(spark, rules, schema):
    """Analyze an accepted snapshot without reapplying onboarding policy."""
    for group, expressions in rules.items():
        if not isinstance(expressions, dict):
            raise InvalidQualityConfiguration(
                f"Normalized Lakeflow quality group '{group}' must be an object"
            )
        for rule_name, expression in expressions.items():
            _analyze_expression(spark, schema, expression, rule_name)
    return rules


def schema_from_json(schema_json):
    """Decode an optional persisted Spark schema."""
    if not schema_json:
        return None
    payload = json.loads(schema_json) if isinstance(schema_json, str) else schema_json
    return StructType.fromJson(payload)


def _validate_migration_value(migration):
    if migration not in (None, "full_refresh"):
        raise InvalidQualityConfiguration(
            "quality engine migration must be 'full_refresh' when provided"
        )


def build_lakeflow_quality_config(
    *,
    rules_path,
    raw_text,
    document,
    normalized_rules,
    analysis_required,
    target_details,
    quarantine_target_details,
    engine_options=None,
    migration=None,
):
    """Create the complete immutable qualityConfig snapshot."""
    _validate_migration_value(migration)
    config = {
        "engine": "lakeflow",
        "spi_version": LAKEFLOW_SPI_VERSION,
        "plugin_package": None,
        "plugin_version": None,
        "engine_version": pyspark.__version__,
        "diagnostic_schema_fingerprint": LAKEFLOW_DIAGNOSTIC_SCHEMA_FINGERPRINT,
        "rule_document": document,
        "normalized_rules": normalized_rules,
        "rules_path": rules_path,
        "content_hash": hashlib.sha256(raw_text.encode("utf-8")).hexdigest(),
        "validation_policy_version": VALIDATION_POLICY_VERSION,
        "output_targets": {
            "main": dict(target_details or {}),
            "quarantine": dict(quarantine_target_details or {}),
        },
        "engine_options": {
            **(engine_options or {}),
            "analysis_required": analysis_required,
        },
        "migration": migration,
        "migration_fingerprint": None,
        "completed_migration_fingerprint": None,
    }
    if migration == "full_refresh":
        config["migration_fingerprint"] = migration_fingerprint(config)
    if tuple(config) != QUALITY_CONFIG_KEYS:
        raise AssertionError("qualityConfig field order is out of sync")
    return config


def build_plugin_quality_config(
    *,
    engine,
    plugin,
    rules_path,
    raw_text,
    document,
    normalized_rules,
    analysis_required,
    target_details,
    quarantine_target_details,
    migration=None,
    engine_options=None,
):
    """Build an immutable snapshot for an externally installed engine."""
    _validate_migration_value(migration)
    diagnostic_fingerprint = getattr(
        plugin, "diagnostic_schema_fingerprint", None
    )
    if not diagnostic_fingerprint:
        diagnostic_fingerprint = hashlib.sha256(
            json.dumps(
                sorted(plugin.reserved_columns()),
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    config = {
        "engine": engine,
        "spi_version": SPI_API_VERSION,
        "plugin_package": getattr(plugin, "plugin_package", None),
        "plugin_version": getattr(plugin, "plugin_version", None),
        "engine_version": getattr(plugin, "engine_version", None),
        "diagnostic_schema_fingerprint": diagnostic_fingerprint,
        "rule_document": document,
        "normalized_rules": normalized_rules,
        "rules_path": rules_path,
        "content_hash": hashlib.sha256(
            raw_text.encode("utf-8")
        ).hexdigest(),
        "validation_policy_version": VALIDATION_POLICY_VERSION,
        "output_targets": {
            "main": dict(target_details or {}),
            "quarantine": dict(quarantine_target_details or {}),
        },
        "engine_options": {
            **(engine_options or {}),
            "analysis_required": bool(analysis_required),
        },
        "migration": migration,
        "migration_fingerprint": None,
        "completed_migration_fingerprint": None,
    }
    if migration == "full_refresh":
        config["migration_fingerprint"] = migration_fingerprint(config)
    if tuple(config) != QUALITY_CONFIG_KEYS:
        raise AssertionError("qualityConfig field order is out of sync")
    return config


def validate_runtime_quality_config(config):
    """Validate a persisted snapshot that may have been edited by hand."""
    if not isinstance(config, dict):
        raise InvalidQualityConfiguration("qualityConfig must be a JSON object")
    missing = [key for key in QUALITY_CONFIG_KEYS if key not in config]
    if missing:
        raise InvalidQualityConfiguration(
            f"qualityConfig is missing required fields: {', '.join(missing)}"
        )
    for key in ("output_targets", "engine_options"):
        if not isinstance(config[key], dict):
            raise InvalidQualityConfiguration(
                f"qualityConfig.{key} must be a JSON object"
            )
    rule_types = (dict,) if config["engine"] == "lakeflow" else (dict, list)
    for key in ("rule_document", "normalized_rules"):
        if not isinstance(config[key], rule_types):
            if config["engine"] == "lakeflow":
                raise InvalidQualityConfiguration(
                    f"qualityConfig.{key} must be a JSON object"
                )
            raise InvalidQualityConfiguration(
                f"qualityConfig.{key} has invalid type for "
                f"engine '{config['engine']}'"
            )
    missing_targets = {
        "main", "quarantine"
    } - set(config["output_targets"])
    if missing_targets:
        raise InvalidQualityConfiguration(
            "qualityConfig.output_targets is missing: "
            f"{', '.join(sorted(missing_targets))}"
        )
    if config["migration"] not in (None, "full_refresh"):
        raise InvalidQualityConfiguration(
            "qualityConfig.migration must be null or 'full_refresh'"
        )
    if (
        config["migration"] == "full_refresh"
        and config["migration_fingerprint"] != migration_fingerprint(config)
    ):
        raise InvalidQualityConfiguration(
            "qualityConfig.migration_fingerprint does not match its target"
        )
    for field in (
        "migration_fingerprint",
        "completed_migration_fingerprint",
    ):
        value = config[field]
        if value is not None and not re.fullmatch(r"[0-9a-f]{64}", value):
            raise InvalidQualityConfiguration(
                f"qualityConfig.{field} must be a SHA-256 hex digest or null"
            )
    if config["spi_version"] != SPI_API_VERSION:
        raise InvalidQualityConfiguration(
            f"Incompatible quality-engine SPI version '{config['spi_version']}'"
        )
    if config["engine"] != "lakeflow":
        for field in ("plugin_package", "plugin_version"):
            if not isinstance(config[field], str) or not config[field]:
                raise InvalidQualityConfiguration(
                    f"qualityConfig.{field} is required for external engines"
                )
    if (
        config["engine"] == "lakeflow"
        and config["diagnostic_schema_fingerprint"]
        != LAKEFLOW_DIAGNOSTIC_SCHEMA_FINGERPRINT
    ):
        raise InvalidQualityConfiguration(
            "Lakeflow diagnostic schema fingerprint does not match the runtime"
        )
    if not isinstance(config["validation_policy_version"], str) or not config[
        "validation_policy_version"
    ]:
        raise InvalidQualityConfiguration(
            "qualityConfig.validation_policy_version must identify the policy "
            "that accepted the snapshot"
        )
