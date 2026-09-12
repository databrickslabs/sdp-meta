"""Wheel entry point for managed quality-aware pipeline updates."""

import argparse
import json


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_id", required=True)
    parser.add_argument(
        "--spec_tables",
        required=True,
        help="JSON object mapping bronze/silver to dataflow-spec tables",
    )
    parser.add_argument(
        "--groups",
        default="{}",
        help="JSON object mapping bronze/silver to dataflow groups",
    )
    parser.add_argument("--timeout_seconds", type=int, default=7200)
    return parser.parse_args(argv)


def main(argv=None):
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession

    from databricks.labs.sdp_meta.quality.migration import (
        run_managed_quality_update,
    )

    args = parse_args(argv)
    return run_managed_quality_update(
        WorkspaceClient(),
        SparkSession.getActiveSession() or SparkSession.builder.getOrCreate(),
        args.pipeline_id,
        json.loads(args.spec_tables),
        json.loads(args.groups),
        timeout_seconds=args.timeout_seconds,
    )


if __name__ == "__main__":
    main()
