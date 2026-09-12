"""Load immutable quality-rule documents during onboarding."""

import json

import yaml


MAX_RULE_DOCUMENT_BYTES = 1024 * 1024


def load_rule_document(spark, file_path):
    """Load a JSON or YAML rules document through Spark-backed storage."""
    if not file_path:
        raise ValueError("A quality rules path is required")
    lower_path = file_path.lower()
    if not lower_path.endswith((".json", ".yml", ".yaml")):
        raise ValueError(
            f"Unsupported quality rules format for '{file_path}'; "
            "expected .json, .yml, or .yaml"
        )
    try:
        rows = spark.read.text(file_path, wholetext=True).collect()
    except Exception as err:
        raise ValueError(f"Failed to read quality rules '{file_path}': {err}") from err
    if not rows or not rows[0]["value"]:
        raise ValueError(f"Quality rules '{file_path}' is empty or unreadable")
    text = rows[0]["value"]
    size = len(text.encode("utf-8"))
    if size > MAX_RULE_DOCUMENT_BYTES:
        raise ValueError(
            f"Quality rules '{file_path}' is {size} bytes; "
            f"maximum is {MAX_RULE_DOCUMENT_BYTES}"
        )
    try:
        document = (
            yaml.safe_load(text)
            if lower_path.endswith((".yml", ".yaml"))
            else json.loads(text)
        )
    except (yaml.YAMLError, json.JSONDecodeError) as err:
        raise ValueError(f"Invalid quality rules '{file_path}': {err}") from err
    return document, text
