"""Shared Spark-free quality output-target identity helpers."""


def _normalized(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def target_identity(target, *, include_path=False):
    """Return a normalized identity for UC or path-backed output targets."""
    fields = ["catalog", "database", "table"]
    if include_path:
        fields.append("path")
    details = target or {}
    return tuple(_normalized(details.get(field)) for field in fields)
