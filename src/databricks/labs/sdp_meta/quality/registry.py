"""Discovery and compatibility checks for installed quality engines."""

from __future__ import annotations

import importlib.metadata
import inspect
import hashlib
import json
import warnings
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from databricks.labs.sdp_meta.quality.spi import (
    QUALITY_ENGINE_ENTRY_POINT_GROUP,
    SPI_API_VERSION,
    QualityEnginePlugin,
)
from databricks.labs.sdp_meta.quality.validation import (
    InvalidQualityConfiguration,
)


class QualityEngineNotAvailable(InvalidQualityConfiguration):
    """Raised when a configured quality-engine plugin cannot be resolved."""


def _entry_points():
    discovered = importlib.metadata.entry_points()
    if hasattr(discovered, "select"):
        return discovered.select(group=QUALITY_ENGINE_ENTRY_POINT_GROUP)
    return discovered.get(QUALITY_ENGINE_ENTRY_POINT_GROUP, ())


def available_quality_engines():
    """Return built-in and installed plugin engine names."""
    return ("lakeflow", "dqx") + tuple(
        sorted(
            {
                ep.name
                for ep in _entry_points()
                if ep.name not in ("lakeflow", "dqx")
            }
        )
    )


def deployable_quality_engines(plugin_dependency=None):
    """Return engines that an App-launched remote job can install."""
    if not plugin_dependency:
        return ("lakeflow", "dqx")
    return available_quality_engines()


def _instantiate(target, spark):
    if inspect.isclass(target):
        return target(spark)
    if callable(target) and not isinstance(target, QualityEnginePlugin):
        return target(spark)
    return target


def resolve_quality_engine(name, spark):
    """Resolve a built-in or installed quality-engine implementation."""
    if name == "lakeflow":
        from databricks.labs.sdp_meta.quality.lakeflow_engine import (
            LakeflowQualityEngine,
        )

        return LakeflowQualityEngine(spark)
    if name == "dqx":
        from databricks.labs.sdp_meta.quality.dqx_engine import (
            DQXQualityEngine,
        )

        return DQXQualityEngine(spark)
    matches = [ep for ep in _entry_points() if ep.name == name]
    if not matches:
        raise QualityEngineNotAvailable(
            f"Quality engine '{name}' is not installed. Available engines: "
            f"{', '.join(available_quality_engines())}"
        )
    if len(matches) > 1:
        raise QualityEngineNotAvailable(
            f"Multiple installed plugins register quality engine '{name}'"
        )
    plugin = _instantiate(matches[0].load(), spark)
    if not isinstance(plugin, QualityEnginePlugin):
        raise QualityEngineNotAvailable(
            f"Quality engine '{name}' does not implement QualityEnginePlugin"
        )
    if plugin.api_version != SPI_API_VERSION:
        raise QualityEngineNotAvailable(
            f"Quality engine '{name}' requires SPI {plugin.api_version}; "
            f"SDP-META provides {SPI_API_VERSION}"
        )
    return plugin


def validate_plugin_snapshot(plugin, config):
    """Validate persisted plugin identity, warning on adapter patch drift."""
    if config.get("spi_version") != SPI_API_VERSION:
        raise QualityEngineNotAvailable(
            f"Persisted SPI {config.get('spi_version')!r} is incompatible "
            f"with {SPI_API_VERSION}"
        )
    installed = getattr(plugin, "plugin_version", None)
    persisted = config.get("plugin_version")
    strict = bool(config.get("engine_options", {}).get("strict_version"))
    if (
        not getattr(plugin, "is_builtin", False)
        and installed
        and persisted
        and installed != persisted
    ):
        message = (
            f"Quality engine adapter version drift: snapshot={persisted}, "
            f"installed={installed}"
        )
        if strict:
            raise QualityEngineNotAvailable(message)
        warnings.warn(message, RuntimeWarning, stacklevel=2)
    supported = getattr(plugin, "supported_engine_versions", None)
    engine_version = config.get("engine_version")
    if supported and engine_version:
        if Version(engine_version) not in SpecifierSet(supported):
            raise QualityEngineNotAvailable(
                f"Quality engine version {engine_version} is outside adapter "
                f"range {supported}"
            )
    current_fingerprint = getattr(
        plugin, "diagnostic_schema_fingerprint", None
    ) or hashlib.sha256(
        json.dumps(
            sorted(plugin.reserved_columns()), separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    if config.get("diagnostic_schema_fingerprint") != current_fingerprint:
        raise QualityEngineNotAvailable(
            "Installed quality engine diagnostic schema differs from the "
            "onboarding snapshot; re-onboard with an explicit migration"
        )
