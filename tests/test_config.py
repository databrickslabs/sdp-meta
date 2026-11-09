"""Unit tests for ``databricks.labs.sdp_meta.config``.

``config.py`` is pure dataclass plumbing (``ConnectConfig``,
``_Config``, ``WorkspaceConfig``) -- no Spark, no I/O beyond
filesystem reads. Historically it was at ~59% coverage because no
tests exercised it directly; the rest of the suite only imported the
classes by name. These tests cover every branch.

A note on mocking
-----------------
``databricks.sdk.core.Config(host=..., token=...)`` looks like a
plain dataclass-y constructor but actually probes credential sources
at construction time (env vars, ``~/.databrickscfg``, OAuth metadata
endpoints, etc.). Letting it run during unit tests blocks the suite
on network / filesystem / interactive auth -- exactly what we are
trying to keep out of unit tests.

Tests in this file therefore patch ``Config`` at the
``databricks.labs.sdp_meta.config.Config`` import site so the
construction is observable but inert. We assert the kwargs the code
*would* have passed to ``Config(...)`` rather than the resulting
real ``Config`` object.
"""
from __future__ import annotations

import copy
import json
import unittest
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from databricks.labs.sdp_meta.__about__ import __version__
from databricks.labs.sdp_meta.config import (
    ConnectConfig,
    WorkspaceConfig,
    _Config,
    _CONFIG_VERSION,
)


# A fully-populated ``WorkspaceConfig`` payload used as the canonical
# fixture for ``from_dict`` / ``from_bytes`` / ``from_file`` tests.
# Field set mirrors the dataclass exactly so adding a field there
# without updating the fixture fails loudly here.
_WORKSPACE_PAYLOAD = {
    "version": _CONFIG_VERSION,
    "dbr_version": "14.3.x-scala2.12",
    "cloud_provider_name": "aws",
    "dbfs_path": "/dbfs/sdp-meta",
    "sdp_meta_operation": "deploy_silver",
    "onboarding_file_path": "/Workspace/onboarding.json",
    "uc_enabled": True,
    "uc_catalog_name": "main",
    "sdp_meta_schema": "sdp_meta",
    "bronze_dataflow_spec_table": "bronze_dataflowspec",
    "bronze_dataflow_spec_path": "/Volumes/main/sdp_meta/bronze",
    "silver_dataflow_spec_table": "silver_dataflowspec",
    "silver_dataflow_spec_path": "/Volumes/main/sdp_meta/silver",
    "overwrite_dataflow_spec": False,
    "dataflow_spec_version": "v1",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "sdp_meta_layer": "silver",
    "sdp_meta_onboard_group": "A1",
    "serverless": True,
    "num_workers": 2,
    "connect": {
        "host": "https://example.cloud.databricks.com",
        "token": "fake-token",
        "profile": "DEFAULT",
        "cluster_id": "cid-123",
    },
}

_V0010_CONFIG_PATH = (
    Path(__file__).parent / "fixtures" / "config" / "v0.0.10-config.json"
)


def _fake_databricks_config(**kwargs) -> SimpleNamespace:
    """Return a SimpleNamespace mimicking ``databricks.sdk.core.Config``.

    Used as the input to ``ConnectConfig.from_databricks_config(cfg)``.
    Only the attributes that ``from_databricks_config`` actually reads
    need to be present; everything else is irrelevant.
    """
    defaults = dict(
        host=None,
        token=None,
        client_id=None,
        client_secret=None,
        azure_client_id=None,
        azure_tenant_id=None,
        azure_client_secret=None,
        azure_environment=None,
        cluster_id=None,
        profile=None,
        debug_headers=False,
        rate_limit=None,
        max_connection_pools=None,
        max_connections_per_pool=None,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


class TestConnectConfig(unittest.TestCase):
    """Cover ``ConnectConfig`` construction, conversion, and round-trip."""

    def test_from_databricks_config_copies_every_field(self):
        # Use a SimpleNamespace as a stand-in for the real Config so
        # we don't trigger SDK credential probing.
        cfg = _fake_databricks_config(
            host="https://example.cloud.databricks.com",
            token="fake-token",
            profile="DEFAULT",
            cluster_id="cid-123",
            debug_headers=True,
        )
        cc = ConnectConfig.from_databricks_config(cfg)
        self.assertEqual(cc.host, "https://example.cloud.databricks.com")
        self.assertEqual(cc.token, "fake-token")
        self.assertEqual(cc.profile, "DEFAULT")
        self.assertEqual(cc.cluster_id, "cid-123")
        self.assertTrue(cc.debug_headers)

    def test_to_databricks_config_brands_with_product_metadata(self):
        cc = ConnectConfig(host="https://x", token="t")
        # Patch the imported ``Config`` so construction is inert and
        # we can inspect the kwargs the code passed in.
        with patch("databricks.labs.sdp_meta.config.Config") as mock_cfg:
            cc.to_databricks_config()
        mock_cfg.assert_called_once()
        kwargs = mock_cfg.call_args.kwargs
        self.assertEqual(kwargs["host"], "https://x")
        self.assertEqual(kwargs["token"], "t")
        # The whole reason ``to_databricks_config`` exists rather
        # than callers building Config directly is the product
        # branding -- regress it loudly if it ever drifts.
        self.assertEqual(kwargs["product"], "sdp-meta")
        self.assertEqual(kwargs["product_version"], __version__)

    def test_from_dict_builds_dataclass_from_kwargs(self):
        cc = ConnectConfig.from_dict({"host": "https://x", "profile": "p"})
        self.assertEqual(cc.host, "https://x")
        self.assertEqual(cc.profile, "p")
        # Fields not in the dict default to None.
        self.assertIsNone(cc.token)


class TestUnderscoreConfigBaseClass(unittest.TestCase):
    """Cover the abstract base ``_Config`` via WorkspaceConfig."""

    def test_from_bytes_empty_payload_short_circuits_to_empty_dict(self):
        # ``json.loads("null")`` -> None -> ``not raw`` -> ``{}``. Use
        # a dataclass with no required fields so the empty-dict
        # construction succeeds. ``connect`` has to be re-declared
        # here for the same reason it's re-declared on
        # ``WorkspaceConfig`` -- ``_Config`` isn't @dataclass so its
        # annotation isn't picked up by the subclass's dataclass
        # field set.
        from typing import Optional as _Opt  # noqa: F401 (used in annotation below)

        @dataclass
        class _EmptyConfig(_Config["_EmptyConfig"]):
            connect: _Opt[ConnectConfig] = None

            @classmethod
            def from_dict(cls, raw):
                # Mirrors WorkspaceConfig.from_dict pattern but
                # without _verify_version (this fixture is for
                # exercising the empty branch).
                raw.pop("version", None)
                connect = ConnectConfig.from_dict(raw.pop("connect", {}))
                return cls(connect=connect)

        result = _EmptyConfig.from_bytes("null")
        self.assertIsInstance(result, _EmptyConfig)
        self.assertIsInstance(result.connect, ConnectConfig)

    def test_from_file_reads_json_and_round_trips_through_from_dict(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "config.json"
            path.write_text(json.dumps(_WORKSPACE_PAYLOAD))
            wc = WorkspaceConfig.from_file(path)
        self.assertEqual(wc.dbr_version, "14.3.x-scala2.12")
        self.assertEqual(wc.connect.host, "https://example.cloud.databricks.com")

    def test_verify_version_accepts_current_version(self):
        wc = WorkspaceConfig.from_dict(dict(_WORKSPACE_PAYLOAD))
        self.assertIsInstance(wc, WorkspaceConfig)

    def test_verify_version_rejects_unknown_version_with_value_error(self):
        bad = dict(_WORKSPACE_PAYLOAD, version=999)
        with self.assertRaises(ValueError) as ctx:
            WorkspaceConfig.from_dict(bad)
        msg = str(ctx.exception)
        self.assertIn("Unsupported config version", msg)
        self.assertIn("999", msg)

    def test_verify_version_rejects_missing_version_with_value_error(self):
        bad = {k: v for k, v in _WORKSPACE_PAYLOAD.items() if k != "version"}
        with self.assertRaises(ValueError) as ctx:
            WorkspaceConfig.from_dict(bad)
        self.assertIn("Unsupported config version", str(ctx.exception))

    def test_post_init_defaults_missing_connect_to_empty_connect(self):
        # Build with empty connect dict -> ConnectConfig() -> not None,
        # so the __post_init__ ``if connect is None`` branch needs the
        # connect attribute set to None explicitly. Do that by
        # overriding after construction.
        wc = WorkspaceConfig.from_dict(dict(_WORKSPACE_PAYLOAD))
        wc.connect = None
        wc.__post_init__()
        self.assertIsInstance(wc.connect, ConnectConfig)
        self.assertIsNone(wc.connect.host)

    def test_to_databricks_config_with_populated_connect(self):
        wc = WorkspaceConfig.from_dict(dict(_WORKSPACE_PAYLOAD))
        with patch("databricks.labs.sdp_meta.config.Config") as mock_cfg:
            wc.to_databricks_config()
        kwargs = mock_cfg.call_args.kwargs
        self.assertEqual(kwargs["host"], "https://example.cloud.databricks.com")
        self.assertEqual(kwargs["token"], "fake-token")
        self.assertEqual(kwargs["product"], "sdp-meta")

    def test_to_databricks_config_with_none_connect_uses_empty_defaults(self):
        # Exercise the ``if connect is None`` defensive branch.
        wc = WorkspaceConfig.from_dict(dict(_WORKSPACE_PAYLOAD))
        wc.connect = None
        with patch("databricks.labs.sdp_meta.config.Config") as mock_cfg:
            wc.to_databricks_config()
        kwargs = mock_cfg.call_args.kwargs
        # Empty ConnectConfig -> all None -> Config still gets the
        # product branding (which is the entire point of going
        # through to_databricks_config rather than Config()
        # directly).
        self.assertIsNone(kwargs["host"])
        self.assertEqual(kwargs["product"], "sdp-meta")

    def test_as_dict_round_trips_dataclass_fields_with_version_stamp(self):
        wc = WorkspaceConfig.from_dict(dict(_WORKSPACE_PAYLOAD))
        result = wc.as_dict()
        # Every populated field shows up.
        self.assertEqual(result["dbr_version"], "14.3.x-scala2.12")
        self.assertEqual(result["uc_catalog_name"], "main")
        # Version stamp wins regardless of input version.
        self.assertEqual(result["version"], _CONFIG_VERSION)
        # Nested dataclass got serialized recursively.
        self.assertEqual(
            result["connect"]["host"],
            "https://example.cloud.databricks.com",
        )
        # Falsy fields (False, None, 0, "") are SKIPPED -- that's
        # the documented contract of ``as_dict.inner``. ``num_workers=2``
        # is truthy so it survives; ``overwrite_dataflow_spec=False``
        # is falsy so it gets dropped.
        self.assertIn("num_workers", result)
        self.assertNotIn("overwrite_dataflow_spec", result)


class TestWorkspaceConfig(unittest.TestCase):
    """End-to-end ``WorkspaceConfig`` -> ``WorkspaceClient`` plumbing."""

    def test_v0010_config_file_migrates_all_legacy_keys(self):
        with self.assertLogs(
            "databricks.labs.sdp_meta.config", level="WARNING"
        ) as captured:
            config = WorkspaceConfig.from_file(_V0010_CONFIG_PATH)

        self.assertEqual(config.sdp_meta_operation, "onboard")
        self.assertEqual(config.sdp_meta_schema, "dlt_meta_dataflowspecs")
        self.assertEqual(config.sdp_meta_layer, "bronze_silver")
        self.assertEqual(config.sdp_meta_onboard_group, "A1")
        for legacy_key in (
            "dlt_meta_operation",
            "dlt_meta_schema",
            "dlt_meta_layer",
            "dlt_meta_onboard_group",
        ):
            self.assertTrue(
                any(legacy_key in message for message in captured.output),
                captured.output,
            )

    def test_current_config_load_does_not_mutate_input_or_warn(self):
        payload = copy.deepcopy(_WORKSPACE_PAYLOAD)
        original = copy.deepcopy(payload)

        with patch(
            "databricks.labs.sdp_meta.config.logger.warning"
        ) as warning:
            config = WorkspaceConfig.from_dict(payload)

        warning.assert_not_called()
        self.assertEqual(payload, original)
        self.assertEqual(config.sdp_meta_schema, "sdp_meta")

    def test_current_key_wins_when_legacy_and_current_keys_conflict(self):
        payload = copy.deepcopy(_WORKSPACE_PAYLOAD)
        payload["dlt_meta_schema"] = "legacy_schema"
        original = copy.deepcopy(payload)

        with self.assertLogs(
            "databricks.labs.sdp_meta.config", level="WARNING"
        ) as captured:
            config = WorkspaceConfig.from_dict(payload)

        self.assertEqual(config.sdp_meta_schema, "sdp_meta")
        self.assertEqual(payload, original)
        self.assertTrue(
            any(
                "current key takes precedence" in message
                for message in captured.output
            ),
            captured.output,
        )

    def test_to_workspace_client_returns_configured_workspace_client(self):
        wc = WorkspaceConfig.from_dict(dict(_WORKSPACE_PAYLOAD))
        # Patch BOTH the SDK's Config and the WorkspaceClient so
        # construction is inert and we can inspect the call shape.
        with patch("databricks.labs.sdp_meta.config.Config") as mock_cfg, patch(
            "databricks.labs.sdp_meta.config.WorkspaceClient"
        ) as mock_ws:
            mock_cfg.return_value = MagicMock(name="FakeConfigInstance")
            wc.to_workspace_client()
        mock_ws.assert_called_once()
        # The WorkspaceClient should be wired through the same Config
        # instance that to_databricks_config() returned.
        self.assertIs(
            mock_ws.call_args.kwargs["config"],
            mock_cfg.return_value,
        )
        # And that Config should have product branding.
        self.assertEqual(mock_cfg.call_args.kwargs["product"], "sdp-meta")


if __name__ == "__main__":
    unittest.main()
