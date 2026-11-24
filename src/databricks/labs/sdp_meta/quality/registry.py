"""Discovery and compatibility checks for installed quality engines."""

from __future__ import annotations

import importlib.metadata
import inspect
import warnings
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

from databricks.labs.sdp_meta.quality.spi import (
    QUALITY_ENGINE_ENTRY_POINT_GROUP,
    SPI_API_VERSION,
    QualityEnginePlugin,
    plugin_diagnostic_schema_fingerprint,
)
from databricks.labs.sdp_meta.quality.errors import InvalidQualityConfiguration


class QualityEngineNotAvailable(InvalidQualityConfiguration):
    """Raised when a configured quality-engine plugin cannot be resolved."""


def _entry_points():
    discovered = importlib.metadata.entry_points()
    if hasattr(discovered, "select"):
        return discovered.select(group=QUALITY_ENGINE_ENTRY_POINT_GROUP)
    return discovered.get(QUALITY_ENGINE_ENTRY_POINT_GROUP, ())


def available_quality_engines():
    """Return built-in and installed plugin engine names."""
    builtins = ("lakeflow", "dqx")
    discovered = {
        ep.name.casefold(): ep.name
        for ep in _entry_points()
        if ep.name.casefold() not in builtins
    }
    return builtins + tuple(
        discovered[name] for name in sorted(discovered)
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


def _validate_supported_engine_version(plugin, version, source):
    supported = getattr(plugin, "supported_engine_versions", None)
    if not supported:
        return
    if not version:
        raise QualityEngineNotAvailable(
            f"{source} quality engine version is missing; adapter requires "
            f"{supported}"
        )
    try:
        compatible = Version(str(version)) in SpecifierSet(str(supported))
    except (InvalidVersion, InvalidSpecifier) as err:
        raise QualityEngineNotAvailable(
            f"Cannot validate {source.lower()} quality engine version "
            f"{version!r} against adapter range {supported!r}"
        ) from err
    if not compatible:
        raise QualityEngineNotAvailable(
            f"{source} quality engine version {version} is outside adapter "
            f"range {supported}"
        )


def resolve_quality_engine(name, spark):
    """Resolve a built-in or installed quality-engine implementation."""
    canonical_name = str(name).casefold()
    if canonical_name == "lakeflow":
        from databricks.labs.sdp_meta.quality.lakeflow_engine import (
            LakeflowQualityEngine,
        )

        return LakeflowQualityEngine(spark)
    if canonical_name == "dqx":
        from databricks.labs.sdp_meta.quality.dqx_engine import (
            DQXQualityEngine,
        )

        plugin = DQXQualityEngine(spark)
        _validate_supported_engine_version(
            plugin, getattr(plugin, "engine_version", None), "Installed"
        )
        return plugin
    matches = [
        ep for ep in _entry_points()
        if ep.name.casefold() == canonical_name
    ]
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
    _validate_supported_engine_version(
        plugin, getattr(plugin, "engine_version", None), "Installed"
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
    _validate_supported_engine_version(
        plugin, config.get("engine_version"), "Persisted"
    )
    _validate_supported_engine_version(
        plugin, getattr(plugin, "engine_version", None), "Installed"
    )
    current_fingerprint = plugin_diagnostic_schema_fingerprint(plugin)
    if config.get("diagnostic_schema_fingerprint") != current_fingerprint:
        raise QualityEngineNotAvailable(
            "Installed quality engine diagnostic schema differs from the "
            "onboarding snapshot; re-onboard with an explicit migration"
        )
