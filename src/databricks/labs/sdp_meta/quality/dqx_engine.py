"""Built-in optional Databricks Labs DQX quality engine."""

import hashlib
import json
from importlib.metadata import PackageNotFoundError, requires, version

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from databricks.labs.sdp_meta.quality.spi import QualityEnginePlugin


CORE_DISTRIBUTION = "databricks-labs-sdp-meta"
DQX_DISTRIBUTION = "databricks-labs-dqx"


def _supported_dqx_versions():
    try:
        requirements = requires(CORE_DISTRIBUTION) or ()
    except PackageNotFoundError as err:
        raise ImportError(
            "DQX support requires an installed SDP-META distribution. Install "
            "'databricks-labs-sdp-meta[dqx]'."
        ) from err
    for requirement_text in requirements:
        requirement = Requirement(requirement_text)
        if canonicalize_name(requirement.name) == canonicalize_name(
            DQX_DISTRIBUTION
        ):
            return str(requirement.specifier)
    raise RuntimeError(
        "SDP-META package metadata does not declare its DQX optional "
        "dependency; reinstall the package with the 'dqx' extra."
    )


def _distribution_version(distribution):
    try:
        return version(distribution)
    except PackageNotFoundError as err:
        extra = "[dqx]" if distribution == DQX_DISTRIBUTION else ""
        raise ImportError(
            f"DQX support requires '{distribution}'. Install "
            f"'databricks-labs-sdp-meta{extra}'."
        ) from err


class DQXQualityEngine(QualityEnginePlugin):
    """Adapt DQX metadata checks to SDP-META's shared writer."""

    plugin_package = CORE_DISTRIBUTION
    is_builtin = True
    allows_overlap = True
    overlap_diagnostics = ("_warnings", "_errors")
    diagnostic_schema_fingerprint = hashlib.sha256(
        json.dumps(
            ["_dq_info", "_errors", "_warnings"],
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    def __init__(self, spark):
        self.spark = spark
        self._dq_engine = None

    @property
    def plugin_version(self):
        return _distribution_version(CORE_DISTRIBUTION)

    @property
    def engine_version(self):
        return _distribution_version(DQX_DISTRIBUTION)

    @property
    def supported_engine_versions(self):
        return _supported_dqx_versions()

    @property
    def dq_engine(self):
        if self._dq_engine is None:
            try:
                from databricks.labs.dqx.engine import DQEngine
            except ImportError as err:
                raise ImportError(
                    "DQX support requires the optional dependency. Install "
                    "'databricks-labs-sdp-meta[dqx]'."
                ) from err
            from databricks.sdk import WorkspaceClient

            self._dq_engine = DQEngine(
                WorkspaceClient(), spark=self.spark
            )
        return self._dq_engine

    def validate(self, document, options):
        if not isinstance(document, list) or not document:
            raise ValueError(
                "DQX quality rules must be a non-empty list of checks"
            )
        status = self.dq_engine.validate_checks(document)
        if status.errors:
            raise ValueError(
                "Invalid DQX quality checks: " + "; ".join(status.errors)
            )
        return document, False

    def apply(self, input_df, rules, options):
        checked = self.dq_engine.apply_checks_by_metadata(
            input_df, rules
        )
        return checked[0] if isinstance(checked, tuple) else checked

    def get_main_input(self, checked_df):
        return self.dq_engine.get_valid(checked_df)

    def get_invalid(self, checked_df):
        return self.dq_engine.get_invalid(checked_df)

    def native_expectations(self, rules, options):
        return []

    def reserved_columns(self):
        return ["_errors", "_warnings", "_dq_info"]
