"""Experimental public service-provider interface for quality engines."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Literal

SPI_API_VERSION = "1-beta"
QUALITY_ENGINE_ENTRY_POINT_GROUP = (
    "databricks.labs.sdp_meta.quality_engines"
)


@dataclass(frozen=True)
class ExpectationBinding:
    """Native Lakeflow expectations bound to one writer graph node."""

    target: Literal["checked", "main"]
    action: Literal["expect", "expect_or_drop", "expect_or_fail"]
    rules: Dict[str, str]


class QualityEnginePlugin(ABC):
    """Experimental writer-facing contract for external quality engines."""

    api_version = SPI_API_VERSION
    plugin_version = None
    allows_overlap = False
    overlap_diagnostics = None

    @abstractmethod
    def validate(self, document, options):
        """Validate and return rules, or ``(rules, True)`` to defer analysis."""

    def analyze(self, rules, schema, options):
        """Perform deferred schema analysis of already-normalized rules."""
        raise NotImplementedError(
            "Quality engine requested deferred schema analysis but does not "
            "implement analyze()"
        )

    @abstractmethod
    def apply(self, input_df, rules, options):
        """Return a checked DataFrame carrying engine diagnostics."""

    @abstractmethod
    def get_main_input(self, checked_df):
        """Return rows supplied to the main table's native bindings."""

    @abstractmethod
    def get_invalid(self, checked_df):
        """Return rows written to quarantine."""

    @abstractmethod
    def native_expectations(self, rules, options):
        """Return native expectation bindings for checked/main targets."""

    @abstractmethod
    def reserved_columns(self):
        """Return diagnostic columns owned by this engine."""


def unpack_validation_result(result):
    """Return normalized rules and an explicit deferred-analysis flag."""
    if not isinstance(result, tuple):
        return result, False
    if len(result) != 2 or not isinstance(result[1], bool):
        raise TypeError(
            "Quality engine validate() must return rules or "
            "(rules, analysis_required: bool)"
        )
    return result
