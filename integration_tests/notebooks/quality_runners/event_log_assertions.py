# Databricks notebook source
"""Helpers for asserting Lakeflow expectation metrics from a pipeline event log."""

import json


def _expectations_from_details(details):
    """Return expectation metric objects from one event-log details payload."""
    if not details:
        return []
    payload = json.loads(details) if isinstance(details, str) else details
    return (
        payload.get("flow_progress", {})
        .get("data_quality", {})
        .get("expectations", [])
    )


def read_max_failed_records(spark, pipeline_id):
    """Read the maximum failed-record count emitted for each expectation.

    Taking the maximum keeps the assertion stable across the scenario's second,
    no-op update: runtimes may emit a second metric object containing zeroes.
    """
    rows = spark.sql(
        f"""
        SELECT details
        FROM event_log('{pipeline_id}')
        WHERE event_type = 'flow_progress'
        """
    ).collect()
    failures = {}
    for row in rows:
        for metric in _expectations_from_details(row["details"]):
            name = metric.get("name")
            failed = metric.get("failed_records")
            if name is None or failed is None:
                continue
            failures[name] = max(failures.get(name, 0), int(failed))
    return failures


def assert_expected_failures(actual, expected):
    """Raise with a useful mismatch when event-log metrics are exposed."""
    missing = sorted(set(expected) - set(actual))
    mismatched = {
        name: {"expected": expected[name], "actual": actual.get(name)}
        for name in expected
        if name in actual and actual[name] != expected[name]
    }
    if missing or mismatched:
        raise AssertionError(
            f"expectation metric mismatch: missing={missing}, mismatched={mismatched}, "
            f"all_actual={actual}"
        )
