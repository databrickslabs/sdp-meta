"""Curated registry of demo onboarding specs bundled with the App.

Surfaced via ``GET /onboarding/bundled-specs`` so the UI can render a
"pick a demo" dropdown instead of forcing the user to know the exact
relative path of each spec file.

Curation principle: **every entry in this registry must onboard
out-of-the-box** with the default form values (Environment=demo,
update_paths=Yes, local_directory=demo/). Specs that need external
infrastructure (Event Hubs, Kafka, custom UC source tables), multi-
step orchestration (Silver Fanout's two-pass onboarding), or
non-default ``{placeholder}`` substitutions are deliberately omitted
because users can't recover from those failures without leaving the
App. If a future demo needs one of those fixes, add it here as a
transparent override (see ``merge_with`` for the Cloud Files A2 row
merge, and ``env_override`` for the DAIS ``_prod`` suffix override).

Out-of-the-box requirements an entry must satisfy:

  * Every ``source_format`` row must have a ``source_details`` block.
  * Every ``{placeholder}`` in the template must be one of the four
    the App substitutes: ``{uc_volume_path}``, ``{uc_catalog_name}``,
    ``{bronze_schema}``, ``{silver_schema}``.
  * Any file referenced by ``source_path_*``, ``source_schema_path``,
    ``*_data_quality_expectations_json_*``, or
    ``silver_transformation_json_*`` must exist under ``demo/`` so it
    gets copied to the UC Volume when ``update_paths=Yes`` fires.
  * No reliance on tables created by other specs unless the entry
    declares them via ``merge_with`` (which causes the picker to
    transparently concatenate the dependent spec's rows into the same
    onboarding pass).
  * The template's env suffix (``_demo``, ``_prod``, ...) must match
    the Environment field's value at submit time, OR the entry must
    declare ``env_override`` to force the right value.

The current entries all satisfy this contract. Keep this registry in lockstep
with ``databricks_app/routes/demo.py::_DEMO_REGISTRY``.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Iterable

from .path_resolver import BUNDLED_SPEC_MERGED_DIR

logger = logging.getLogger(__name__)


# Each entry maps a *base name* (no extension) to:
#
#   - label: short, scannable title for the dropdown.
#   - description: 1-line "what does this demo show me" hint.
#   - tags: searchable keywords surfaced in the UI as small pills.
#   - merge_with: optional list of OTHER base names whose rows should
#     be CONCATENATED into this entry's rendered spec at request time.
#     Used to paper over runtime dependencies on tables produced by
#     companion specs (e.g. Cloud Files A1's silver append-flow reads
#     ``customers_delta`` which the A2 spec produces). The picker
#     materialises a single merged file so the user sees one entry
#     and one onboarding pass, but the DataflowSpec table ends up
#     with rows from BOTH templates.
#   - env_override: optional environment value the picker should
#     auto-set on the Environment form field when this entry is
#     selected. Used for templates that ship with a non-default env
#     suffix (DAIS uses ``_prod``).
#   - note: optional second paragraph displayed beneath the
#     description; surface non-trivial behaviour (like an env override)
#     here so first-time users understand why a field changed value
#     when they picked an entry.
#
# Order matches the recommended "complexity ladder" — start with
# Cars (1 table), then Multi-Source CDC (3 regional sources), then
# the two flagship demos.
_BUNDLED_DEMO_SPECS = {
    "onboarding_cars": {
        "label": "Cars (Simple bronze + silver)",
        "description": "Single Auto Loader CSV source \u2192 bronze "
                       "``cars`` + silver ``cars_usa``. The simplest "
                       "end-to-end SDP-META demo \u2014 best pick for "
                       "a first run.",
        "tags": ["cloudfiles", "simple", "first-run"],
    },
    "multi-source-cdc-onboarding": {
        "label": "Multi-Source CDC",
        "description": "Three regional Auto Loader JSON sources "
                       "(US / EU / APAC) \u2192 three bronze CDC tables. "
                       "Demonstrates the metadata-driven pattern at "
                       "scale without leaving cloudFiles.",
        "tags": ["cloudfiles", "multi-source", "cdc"],
    },
    "silver-fanout-onboarding": {
        "label": "Silver Fanout (one bronze \u2192 many silver)",
        "description": "One Auto Loader CSV source \u2192 bronze ``cars`` "
                       "\u2192 four region-specific silver tables "
                       "(``cars_usa`` / ``cars_germany`` / ``cars_uk`` "
                       "/ ``cars_japan``) sharing the same bronze. "
                       "Single onboarding pass, single pipeline.",
        # No ``merge_with`` or ``env_override`` needed \u2014 this works
        # in a single onboarding pass thanks to the fanout skip in
        # ``__get_bronze_dataflow_spec_dataframe`` (rows lacking
        # ``source_details`` are treated as silver-only consumers and
        # the silver pass picks them up from the same file).
        "tags": ["cloudfiles", "fanout", "one-to-many"],
    },
    "cloudfiles-onboarding": {
        "label": "Cloud Files (Auto Loader)",
        "description": "Streaming ingest of JSON files from a UC "
                       "Volume into bronze using Auto Loader, "
                       "including a row-filter UDF on the customers "
                       "flow. Demos tab: \u201cCloud Files\u201d.",
        # The A1 spec's silver append-flow reads ``customers_delta``
        # \u2014 a table the A2 spec creates. Merging A2's rows into
        # the same onboarding pass means a single picker click
        # produces a fully-runnable pipeline with no silent runtime
        # ``TABLE_OR_VIEW_NOT_FOUND`` failure on the silver layer.
        "merge_with": ["cloudfiles-onboarding_A2"],
        "tags": ["cloudfiles", "auto loader", "row filter"],
    },
    "onboarding": {
        "label": "DAIS Demo (end-to-end)",
        "description": "The Databricks AI Summit walkthrough: "
                       "customers + transactions across delta / "
                       "cloudFiles with CDC, DQE, and silver "
                       "transformations. Demos tab: \u201cDAIS Demo\u201d.",
        # The DAIS template ships with the ``_prod`` env suffix on
        # every env-aware field. The picker auto-sets the
        # Environment field to ``prod`` when this entry is selected,
        # so the preview's env-mismatch guard doesn't reject the
        # onboarding with the default ``demo`` value.
        "env_override": "prod",
        "note": ("This demo uses the ``_prod`` env suffix on env-aware "
                 "fields. The Environment field is set to ``prod`` "
                 "automatically when you pick this demo \u2014 leave it "
                 "alone unless you've edited the template."),
        "tags": ["dais", "end-to-end", "cdc", "dqe"],
    },
}


# ── Merge materialisation ────────────────────────────────────────────
# Specs with ``merge_with`` are rendered into a flat, single-file
# representation at request time so the rest of the onboarding pipeline
# (preview, submit, CLI subprocess) doesn't need to know about merging.
# The merged files live under ``<tempdir>/sdp_meta_app_bundled_merged/``
# \u2014 guaranteed writable inside the App container, and outside the
# repo tree so they don't pollute git.
#
# The output dir is sourced from ``path_resolver.BUNDLED_SPEC_MERGED_DIR``
# rather than defined locally, because the path-traversal guard in
# ``_resolve_local_onboarding_path`` needs to allow this exact directory
# through its repo-root boundary check. Sharing the constant guarantees
# the writer (this module) and the verifier (path_resolver) agree on
# the path \u2014 if we ever rename it, both ends move together and the
# allow-list never falls out of sync with the producer.

_MERGED_OUTPUT_DIR = BUNDLED_SPEC_MERGED_DIR


def _load_json_rows(path: str):
    """Parse ``path`` as JSON and return the top-level list of rows.

    Raises ValueError on non-list top-level structures \u2014 the merge
    only makes sense for list-of-rows templates, which every shipped
    SDP-META onboarding spec is.
    """
    with open(path, "r", encoding="utf-8") as fh:
        parsed = json.load(fh)
    if not isinstance(parsed, list):
        raise ValueError(
            f"Cannot merge {path}: top-level must be a JSON list, "
            f"got {type(parsed).__name__}"
        )
    return parsed


def _load_yaml_rows(path: str):
    """Parse ``path`` as YAML and return the top-level list of rows."""
    import yaml  # local import \u2014 YAML support is optional at install time

    with open(path, "r", encoding="utf-8") as fh:
        parsed = yaml.safe_load(fh)
    if not isinstance(parsed, list):
        raise ValueError(
            f"Cannot merge {path}: top-level must be a YAML list, "
            f"got {type(parsed).__name__}"
        )
    return parsed


def _materialise_merged_spec(
    base_name: str,
    merge_with_bases: Iterable[str],
    json_dir: str,
    yml_dir: str,
):
    """Write a flattened JSON and/or YAML file containing ``base_name``'s
    rows followed by every ``merge_with_bases`` entry's rows.

    Returns a ``{"json": abs_path_or_None, "yaml": abs_path_or_None}``
    dict reflecting which merged files were successfully materialised.
    Skips a format silently if any source file is missing or unparseable
    \u2014 the caller falls back to the non-merged file in that case.
    """
    os.makedirs(_MERGED_OUTPUT_DIR, exist_ok=True)
    out = {"json": None, "yaml": None}

    # ── JSON merge ───────────────────────────────────────────────
    json_primary = os.path.join(json_dir, f"{base_name}.template")
    json_companions = [
        os.path.join(json_dir, f"{c}.template") for c in merge_with_bases
    ]
    if os.path.isfile(json_primary) and all(
        os.path.isfile(p) for p in json_companions
    ):
        try:
            merged_rows = _load_json_rows(json_primary)
            for comp_path in json_companions:
                merged_rows.extend(_load_json_rows(comp_path))
            merged_path = os.path.join(
                _MERGED_OUTPUT_DIR, f"{base_name}.merged.template"
            )
            with open(merged_path, "w", encoding="utf-8") as fh:
                json.dump(merged_rows, fh, indent=2)
            out["json"] = merged_path
        except (ValueError, OSError, json.JSONDecodeError) as exc:
            logger.warning(
                "Failed to materialise merged JSON for %s + %s: %s",
                base_name, list(merge_with_bases), exc,
            )

    # ── YAML merge ───────────────────────────────────────────────
    yml_primary = os.path.join(yml_dir, f"{base_name}.template.yml")
    yml_companions = [
        os.path.join(yml_dir, f"{c}.template.yml") for c in merge_with_bases
    ]
    if os.path.isfile(yml_primary) and all(
        os.path.isfile(p) for p in yml_companions
    ):
        try:
            import yaml

            merged_rows = _load_yaml_rows(yml_primary)
            for comp_path in yml_companions:
                merged_rows.extend(_load_yaml_rows(comp_path))
            merged_path = os.path.join(
                _MERGED_OUTPUT_DIR, f"{base_name}.merged.template.yml"
            )
            with open(merged_path, "w", encoding="utf-8") as fh:
                yaml.safe_dump(merged_rows, fh, sort_keys=False)
            out["yaml"] = merged_path
        except (ValueError, OSError, ImportError) as exc:
            logger.warning(
                "Failed to materialise merged YAML for %s + %s: %s",
                base_name, list(merge_with_bases), exc,
            )
        except Exception as exc:  # noqa: BLE001 \u2014 yaml.YAMLError lives under yaml
            logger.warning(
                "Failed to materialise merged YAML for %s + %s: %s",
                base_name, list(merge_with_bases), exc,
            )

    return out


def _list_bundled_specs(repo_root: str):
    """Return a list of bundled-demo entries matching the curated
    ``_BUNDLED_DEMO_SPECS`` registry that ACTUALLY exist on disk in this
    deployment. Both ``demo/conf/json`` (``*.template``) and
    ``demo/conf/yml`` (``*.template.yml``) are scanned; the user picks
    the format via the UI radio.

    For entries with ``merge_with``, the JSON and YAML paths returned
    point at pre-materialised merged files (under a temp dir) rather
    than the original ``demo/conf/`` files, so the rest of the
    onboarding pipeline operates on a single, complete spec.

    Returns a JSON-ready list ordered by the registry's insertion
    order so the dropdown shows the simplest demo first.
    """
    json_dir = os.path.join(repo_root, "demo", "conf", "json")
    yml_dir = os.path.join(repo_root, "demo", "conf", "yml")

    entries = []
    for base_name, meta in _BUNDLED_DEMO_SPECS.items():
        json_primary = os.path.join(json_dir, f"{base_name}.template")
        yml_primary = os.path.join(yml_dir, f"{base_name}.template.yml")
        json_exists = os.path.isfile(json_primary)
        yml_exists = os.path.isfile(yml_primary)
        if not (json_exists or yml_exists):
            # Registry entry without backing files \u2014 skip silently
            # so a partial container layout doesn't break the picker.
            continue

        merge_with = list(meta.get("merge_with", []))
        merged_paths = {"json": None, "yaml": None}
        if merge_with:
            merged_paths = _materialise_merged_spec(
                base_name, merge_with, json_dir, yml_dir,
            )

        formats = {}
        # Prefer merged paths when ``merge_with`` is declared AND the
        # merge succeeded; otherwise fall back to the un-merged source.
        # Both branches yield strings the resolver can ingest \u2014
        # repo-relative for un-merged sources, absolute paths under
        # ``BUNDLED_SPEC_MERGED_DIR`` for merged outputs. The
        # path-traversal guard in ``_resolve_local_onboarding_path``
        # has an explicit allow-list entry for the merged directory
        # so these absolute paths bypass the repo-root boundary check
        # without weakening it for caller-supplied input.
        if json_exists:
            formats["json"] = (
                merged_paths["json"]
                if merged_paths["json"]
                else f"demo/conf/json/{base_name}.template"
            )
        if yml_exists:
            formats["yaml"] = (
                merged_paths["yaml"]
                if merged_paths["yaml"]
                else f"demo/conf/yml/{base_name}.template.yml"
            )

        entry = {
            "id": base_name,
            "label": meta["label"],
            "description": meta["description"],
            "tags": meta.get("tags", []),
            # Backward compat: existing callers (and tests) expect a
            # ``companion`` key. Keep emitting it as an empty list for
            # merge_with entries \u2014 the merge made the companion
            # transparent, so there's nothing for the UI to warn about.
            "companion": [],
            "merge_with": merge_with,
            "env_override": meta.get("env_override"),
            "note": meta.get("note"),
            "formats": formats,
            "default_local_directory": "demo/",
            "default_update_paths": True,
        }
        entries.append(entry)
    return entries
