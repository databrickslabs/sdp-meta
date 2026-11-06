"""Unit tests for the sdp-meta MCP server (issue #231).

These tests exercise the synchronous tool/resource dispatchers directly
(``call_tool`` / ``read_resource``) so we don't have to spin up the asyncio
stdio plumbing. Live workspace calls and the actual ``databricks bundle ...``
subprocesses are mocked at the bundle.py boundary.
"""

import asyncio
import importlib.util
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

try:
    from mcp import Client, StdioServerParameters, stdio_client
except ImportError as exc:  # pragma: no cover - environment-dependent path
    if importlib.util.find_spec("mcp") is not None:
        raise RuntimeError(
            "An incompatible MCP SDK is installed. Install the supported "
            "`mcp>=2.0.0,<3.0` extra instead of silently skipping MCP tests."
        ) from exc
    _MCP_AVAILABLE = False
else:
    from databricks.labs.sdp_meta import mcp_server  # noqa: F401
    _MCP_AVAILABLE = True


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class PathResolutionTests(unittest.TestCase):
    """Environment overrides resolve consistently across CI platforms."""

    def test_examples_dir_uses_valid_environment_override(self):
        with tempfile.TemporaryDirectory() as examples_dir:
            json_dir = Path(examples_dir) / "json"
            json_dir.mkdir()
            (json_dir / "sample.json").write_text("{}")
            with patch.dict(
                os.environ,
                {"SDP_META_EXAMPLES_DIR": examples_dir},
            ):
                self.assertEqual(
                    mcp_server._locate_examples_dir(),
                    Path(examples_dir).resolve(),
                )

    def test_invalid_mcp_root_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_root = str(Path(temp_dir) / "missing")
            with patch.dict(
                os.environ,
                {"SDP_META_MCP_ROOT": missing_root},
            ):
                with self.assertRaisesRegex(ValueError, "not an existing directory"):
                    mcp_server._mcp_root()

    def test_missing_mcp_root_is_rejected(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "SDP_META_MCP_ROOT must be set"):
                mcp_server._mcp_root()

    @unittest.skipUnless(Path("/tmp").is_symlink(), "macOS /tmp alias required")
    def test_absolute_tmp_alias_inside_root_is_allowed(self):
        with tempfile.TemporaryDirectory(dir="/tmp") as root:
            with patch.dict(os.environ, {"SDP_META_MCP_ROOT": root}):
                resolved = mcp_server._resolve_within_root(
                    str(Path(root) / "bundle"), kind="bundle_dir"
                )
        self.assertEqual(resolved, (Path(root) / "bundle").resolve())

    def test_symlink_target_outside_root_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            with tempfile.TemporaryDirectory() as outside:
                link = Path(root) / "escape"
                link.symlink_to(outside, target_is_directory=True)
                with patch.dict(os.environ, {"SDP_META_MCP_ROOT": root}):
                    with self.assertRaisesRegex(
                        ValueError, "outside the MCP filesystem root"
                    ):
                        mcp_server._resolve_within_root(
                            str(link / "file"), kind="bundle_dir"
                        )


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class ListToolsTests(unittest.TestCase):
    """Lock-in: the v0 tool surface."""

    def test_lists_v0_tools(self):
        async def get_tools():
            async with Client(mcp_server.build_server(), raise_exceptions=True) as client:
                return (await client.list_tools()).tools

        names = {tool.name for tool in asyncio.run(get_tools())}
        self.assertEqual(
            names,
            {
                "sdp_meta_bundle_init",
                "sdp_meta_bundle_validate",
                "sdp_meta_bundle_add_flow",
                "sdp_meta_bundle_add_pipeline",
                "sdp_meta_list_templates",
                "sdp_meta_get_onboarding_template",
            },
        )

    def test_every_tool_has_schema_and_description(self):
        async def get_tools():
            async with Client(mcp_server.build_server(), raise_exceptions=True) as client:
                return (await client.list_tools()).tools

        for tool in asyncio.run(get_tools()):
            self.assertTrue(tool.description, f"{tool.name} missing description")
            self.assertEqual(tool.input_schema.get("type"), "object")
            self.assertIsNotNone(tool.annotations)

    def test_every_tool_is_documented(self):
        async def get_tools():
            async with Client(mcp_server.build_server(), raise_exceptions=True) as client:
                return (await client.list_tools()).tools

        docs = (
            Path(__file__).parents[1] / "docs" / "docs" / "getting-started" / "mcp.md"
        ).read_text()
        for tool in asyncio.run(get_tools()):
            self.assertIn(f"`{tool.name}`", docs)

    def test_unknown_tool_raises(self):
        with self.assertRaisesRegex(ValueError, "Unknown tool"):
            mcp_server.call_tool("does_not_exist", {})


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class ProtocolTests(unittest.TestCase):
    """Exercise real MCP 2.x registration, validation, and result wrapping."""

    @patch(
        "databricks.labs.sdp_meta.mcp_server._locate_examples_dir",
        return_value=None,
    )
    def test_server_starts_without_template_resources(self, _locate):
        async def inspect_server():
            with self.assertLogs(
                "databricks.labs.sdp_meta.mcp", level="ERROR"
            ):
                server = mcp_server.build_server()
            async with Client(server, raise_exceptions=True) as client:
                tools = (await client.list_tools()).tools
                resources = (await client.list_resources()).resources
                return tools, resources

        tools, resources = asyncio.run(inspect_server())
        self.assertEqual(len(tools), 6)
        self.assertEqual(resources, [])

    def test_client_call_returns_structured_content(self):
        async def call_list_templates():
            async with Client(mcp_server.build_server(), raise_exceptions=True) as client:
                return await client.call_tool("sdp_meta_list_templates", {})

        result = asyncio.run(call_list_templates())
        self.assertFalse(result.is_error)
        self.assertIn("templates", result.structured_content["result"])

    def test_tool_failure_is_visible_as_error_result(self):
        async def call_without_root():
            with patch.dict(os.environ, {}, clear=True):
                async with Client(mcp_server.build_server()) as client:
                    return await client.call_tool(
                        "sdp_meta_bundle_validate", {"bundle_dir": "."}
                    )

        result = asyncio.run(call_without_root())
        self.assertTrue(result.is_error)
        self.assertIn("SDP_META_MCP_ROOT", result.content[0].text)

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate", return_value=2)
    def test_invalid_bundle_is_a_normal_negative_result(self, _validate):
        async def validate_bundle(root):
            with patch.dict(os.environ, {"SDP_META_MCP_ROOT": root}):
                async with Client(mcp_server.build_server()) as client:
                    return await client.call_tool(
                        "sdp_meta_bundle_validate", {"bundle_dir": "."}
                    )

        with tempfile.TemporaryDirectory() as root:
            result = asyncio.run(validate_bundle(root))
        self.assertFalse(result.is_error)
        self.assertEqual(result.structured_content["result"]["returncode"], 2)

    def test_client_rejects_invalid_tool_input(self):
        async def call_with_invalid_flows():
            async with Client(mcp_server.build_server()) as client:
                return await client.call_tool(
                    "sdp_meta_bundle_add_flow", {"flows": "not-a-list"}
                )

        result = asyncio.run(call_with_invalid_flows())
        self.assertTrue(result.is_error)

    def test_stdio_transport_lists_calls_and_reads(self):
        async def exercise_stdio():
            parameters = StdioServerParameters(
                command=sys.executable,
                args=[
                    "-c",
                    (
                        "from databricks.labs.sdp_meta.mcp_server import "
                        "run_stdio; run_stdio()"
                    ),
                ],
                env={
                    **os.environ,
                    "SDP_META_MCP_ROOT": str(Path.cwd()),
                },
                cwd=str(Path.cwd()),
            )
            async with Client(stdio_client(parameters)) as client:
                tools = await client.list_tools()
                result = await client.call_tool("sdp_meta_list_templates", {})
                resource = await client.read_resource(
                    "sdp-meta://templates/json/cloudfiles-onboarding.template"
                )
                return tools, result, resource

        tools, result, resource = asyncio.run(exercise_stdio())
        self.assertEqual(len(tools.tools), 6)
        self.assertFalse(result.is_error)
        self.assertIn("data_flow_id", resource.contents[0].text)


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class TemplateToolTests(unittest.TestCase):
    """list_templates + get_onboarding_template hit real packaged files."""

    def test_list_templates_returns_packaged_names(self):
        result = mcp_server.call_tool("sdp_meta_list_templates", {})
        names = result["templates"]
        self.assertIn("json/cloudfiles-onboarding.template", names)
        self.assertIn("yml/cloudfiles-onboarding.template.yml", names)
        # Both formats should be represented.
        self.assertTrue(any(n.startswith("json/") for n in names))
        self.assertTrue(any(n.startswith("yml/") for n in names))

    def test_get_onboarding_template_returns_content(self):
        result = mcp_server.call_tool(
            "sdp_meta_get_onboarding_template",
            {"name": "json/cloudfiles-onboarding.template"},
        )
        self.assertEqual(result["name"], "json/cloudfiles-onboarding.template")
        # Content is real JSON template text, not empty.
        self.assertGreater(len(result["content"]), 100)
        self.assertNotIn("path", result)

    def test_get_onboarding_template_unknown_name_raises(self):
        with self.assertRaisesRegex(FileNotFoundError, "Template 'no/such.json' not found"):
            mcp_server.call_tool(
                "sdp_meta_get_onboarding_template", {"name": "no/such.json"}
            )

    def test_get_onboarding_template_requires_name(self):
        with self.assertRaisesRegex(ValueError, "`name` is required"):
            mcp_server.call_tool("sdp_meta_get_onboarding_template", {})

    @patch(
        "databricks.labs.sdp_meta.mcp_server._locate_examples_dir",
        return_value=None,
    )
    def test_missing_packaged_templates_fail_actionably(self, _locate):
        with self.assertRaisesRegex(RuntimeError, "templates are unavailable"):
            mcp_server._list_template_files()


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BundleInitToolTests(unittest.TestCase):
    """bundle_init delegates to bundle.bundle_init with a quickstart config."""

    def setUp(self):
        # Sandbox root for this test; caller-supplied paths must live here.
        self.root = Path(tempfile.mkdtemp(prefix="sdp_mcp_root_")).resolve()
        self._env = patch.dict(
            os.environ, {"SDP_META_MCP_ROOT": str(self.root)}
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()
        shutil.rmtree(self.root, ignore_errors=True)

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    @patch("databricks.labs.sdp_meta.bundle.write_quickstart_config_file")
    def test_quickstart_writes_config_and_calls_bundle_init(
        self, mock_write_cfg, mock_bundle_init
    ):
        out_dir = self.root / "qs"
        cfg = out_dir / "config.json"
        mock_write_cfg.return_value = str(cfg)
        mock_bundle_init.return_value = 0

        result = mcp_server.call_tool(
            "sdp_meta_bundle_init",
            {"output_dir": str(out_dir), "quickstart": True},
        )
        self.assertEqual(result["returncode"], 0)
        # write_quickstart_config_file is called with the resolved Path.
        mock_write_cfg.assert_called_once()
        # bundle_init received a BundleInitCommand pointing at our config,
        # with the sandbox-resolved output_dir.
        cmd = mock_bundle_init.call_args[0][0]
        self.assertEqual(cmd.config_file, str(cfg))
        self.assertEqual(cmd.output_dir, str(out_dir))

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    @patch("databricks.labs.sdp_meta.bundle.write_quickstart_config_file")
    def test_quickstart_forwards_overrides(self, mock_write_cfg, mock_bundle_init):
        out_dir = self.root / "qs"
        mock_write_cfg.return_value = str(out_dir / "config.json")
        mock_bundle_init.return_value = 0

        mcp_server.call_tool(
            "sdp_meta_bundle_init",
            {
                "output_dir": str(out_dir),
                "quickstart": True,
                "overrides": {"uc_catalog_name": "acme_prod"},
            },
        )
        # The overrides dict is threaded into write_quickstart_config_file.
        _, kwargs = mock_write_cfg.call_args
        self.assertEqual(kwargs.get("overrides"), {"uc_catalog_name": "acme_prod"})

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_quickstart_invalid_override_is_rejected(self, mock_bundle_init):
        # A hyphenated catalog must fail before bundle_init runs. Here we do
        # NOT mock write_quickstart_config_file, so its real validation fires.
        out_dir = self.root / "qs2"
        with self.assertRaisesRegex(ValueError, "uc_catalog_name"):
            mcp_server.call_tool(
                "sdp_meta_bundle_init",
                {
                    "output_dir": str(out_dir),
                    "quickstart": True,
                    "overrides": {"uc_catalog_name": "bad-catalog"},
                },
            )
        mock_bundle_init.assert_not_called()

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_non_quickstart_requires_config_file(self, mock_bundle_init):
        with self.assertRaisesRegex(ValueError, "config_file is required"):
            mcp_server.call_tool(
                "sdp_meta_bundle_init",
                {"output_dir": ".", "quickstart": False},
            )
        mock_bundle_init.assert_not_called()

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_non_quickstart_with_config_file_calls_bundle_init(self, mock_bundle_init):
        mock_bundle_init.return_value = 0
        out_dir = self.root / "x"
        cfg = out_dir / "cfg.json"
        mcp_server.call_tool(
            "sdp_meta_bundle_init",
            {
                "output_dir": str(out_dir),
                "quickstart": False,
                "config_file": str(cfg),
                "profile": "myprofile",
            },
        )
        cmd = mock_bundle_init.call_args[0][0]
        self.assertEqual(cmd.config_file, str(cfg))
        self.assertEqual(cmd.profile, "myprofile")

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_output_dir_outside_root_is_rejected(self, mock_bundle_init):
        # A path that escapes the sandbox root (parent traversal) must be
        # rejected before bundle_init is ever called.
        with self.assertRaisesRegex(ValueError, "outside the MCP filesystem root"):
            mcp_server.call_tool(
                "sdp_meta_bundle_init",
                {"output_dir": str(self.root / ".." / "escape"), "quickstart": True},
            )
        mock_bundle_init.assert_not_called()

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_sibling_root_prefix_is_not_bypass(self, mock_bundle_init):
        # A sibling dir whose name merely starts with the root name must
        # not be treated as inside the root (trailing-separator boundary).
        sibling = str(self.root) + "_evil"
        with self.assertRaisesRegex(ValueError, "outside the MCP filesystem root"):
            mcp_server.call_tool(
                "sdp_meta_bundle_init",
                {"output_dir": sibling, "quickstart": True},
            )
        mock_bundle_init.assert_not_called()


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BundleValidateToolTests(unittest.TestCase):

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp_mcp_root_")).resolve()
        self._env = patch.dict(
            os.environ, {"SDP_META_MCP_ROOT": str(self.root)}
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()
        shutil.rmtree(self.root, ignore_errors=True)

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_validate_forwards_args_and_returns_rc(self, mock_validate):
        mock_validate.return_value = 0
        bundle_dir = self.root / "b"
        result = mcp_server.call_tool(
            "sdp_meta_bundle_validate",
            {"bundle_dir": str(bundle_dir), "target": "dev", "profile": "p"},
        )
        cmd = mock_validate.call_args[0][0]
        self.assertEqual(cmd.bundle_dir, str(bundle_dir))
        self.assertEqual(cmd.target, "dev")
        self.assertEqual(cmd.profile, "p")
        self.assertEqual(result["returncode"], 0)

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_validate_returns_non_zero_rc_as_normal_result(self, mock_validate):
        mock_validate.return_value = 2
        result = mcp_server.call_tool(
            "sdp_meta_bundle_validate", {"bundle_dir": str(self.root / "b")}
        )
        self.assertEqual(result["returncode"], 2)

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_validate_rejects_cli_flag_as_target(self, mock_validate):
        with self.assertRaisesRegex(ValueError, "target"):
            mcp_server.call_tool(
                "sdp_meta_bundle_validate",
                {"bundle_dir": str(self.root / "b"), "target": "--profile"},
            )
        mock_validate.assert_not_called()

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_bundle_dir_outside_root_is_rejected(self, mock_validate):
        with self.assertRaisesRegex(ValueError, "MCP filesystem root"):
            mcp_server.call_tool(
                "sdp_meta_bundle_validate",
                {"bundle_dir": "/etc"},
            )
        mock_validate.assert_not_called()


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BundleAddFlowToolTests(unittest.TestCase):

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp_mcp_root_")).resolve()
        self._env = patch.dict(
            os.environ, {"SDP_META_MCP_ROOT": str(self.root)}
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()
        shutil.rmtree(self.root, ignore_errors=True)

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_flow")
    def test_add_flow_builds_flow_specs_from_dicts(self, mock_add):
        mock_add.return_value = 0
        bundle_dir = self.root / "b"
        flows_in = [
            {
                "source_format": "cloudFiles",
                "source_path": "/Volumes/c/s/v/in",
                "bronze_table": "raw_orders",
                "data_flow_group": "demo",
            }
        ]
        result = mcp_server.call_tool(
            "sdp_meta_bundle_add_flow",
            {"bundle_dir": str(bundle_dir), "flows": flows_in, "dry_run": True},
        )
        cmd = mock_add.call_args[0][0]
        self.assertEqual(cmd.bundle_dir, str(bundle_dir))
        self.assertTrue(cmd.dry_run)
        self.assertEqual(len(cmd.flows), 1)
        # source_path is a flow data value (often a UC Volume path), NOT a
        # local filesystem path the server writes to — it is intentionally
        # NOT sandbox-constrained and passes through verbatim.
        self.assertEqual(cmd.flows[0].source_path, "/Volumes/c/s/v/in")
        self.assertEqual(cmd.flows[0].bronze_table, "raw_orders")
        self.assertEqual(result["returncode"], 0)
        self.assertEqual(result["flows_added"][0]["bronze_table"], "raw_orders")

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_flow")
    def test_add_flow_sandboxes_onboarding_file(self, mock_add):
        mock_add.return_value = 0
        bundle_dir = self.root / "b"
        onboarding = self.root / "b" / "conf" / "onboarding.json"
        mcp_server.call_tool(
            "sdp_meta_bundle_add_flow",
            {
                "bundle_dir": str(bundle_dir),
                "onboarding_file": str(onboarding),
                "flows": [{"source_format": "cloudFiles", "bronze_table": "t"}],
            },
        )
        cmd = mock_add.call_args[0][0]
        self.assertEqual(cmd.onboarding_file, str(onboarding))

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_flow")
    def test_add_flow_rejects_onboarding_file_outside_root(self, mock_add):
        # ``/etc/passwd`` traverses the ``/etc`` -> ``/private/etc`` symlink on
        # macOS, so rejection may come from the symlink-escape branch or the
        # plain out-of-root branch. Match the substring common to both.
        with self.assertRaisesRegex(ValueError, "MCP filesystem root"):
            mcp_server.call_tool(
                "sdp_meta_bundle_add_flow",
                {
                    "bundle_dir": str(self.root / "b"),
                    "onboarding_file": "/etc/passwd",
                    "flows": [{"source_format": "cloudFiles", "bronze_table": "t"}],
                },
            )
        mock_add.assert_not_called()

    def test_add_flow_rejects_empty_flows(self):
        with self.assertRaisesRegex(ValueError, "at least one entry"):
            mcp_server.call_tool(
                "sdp_meta_bundle_add_flow",
                {"bundle_dir": str(self.root / "b"), "flows": []},
            )

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_pipeline")
    def test_add_pipeline_builds_pipeline_spec(self, mock_add):
        mock_add.return_value = 0
        bundle_dir = self.root / "b"
        result = mcp_server.call_tool(
            "sdp_meta_bundle_add_pipeline",
            {
                "bundle_dir": str(bundle_dir),
                "pipeline": {
                    "name": "orders",
                    "layer": "bronze_silver",
                    "pipeline_mode": "split",
                    "dataflow_group": "orders",
                },
                "dry_run": True,
            },
        )
        cmd = mock_add.call_args[0][0]
        self.assertEqual(cmd.bundle_dir, str(bundle_dir))
        self.assertTrue(cmd.dry_run)
        self.assertEqual(cmd.pipeline.name, "orders")
        self.assertEqual(cmd.pipeline.layer, "bronze_silver")
        self.assertEqual(result["pipeline_added"]["dataflow_group"], "orders")

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_pipeline")
    def test_add_pipeline_surfaces_nonzero_returncode(self, mock_add):
        mock_add.return_value = 2
        with self.assertRaisesRegex(RuntimeError, '"returncode": 2'):
            mcp_server.call_tool(
                "sdp_meta_bundle_add_pipeline",
                {
                    "bundle_dir": str(self.root / "b"),
                    "pipeline": {
                        "name": "orders",
                        "layer": "bronze",
                        "dataflow_group": "orders",
                    },
                },
            )


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class ResourceTests(unittest.TestCase):

    def test_list_resources_includes_packaged_templates(self):
        async def get_resources():
            async with Client(mcp_server.build_server(), raise_exceptions=True) as client:
                return (await client.list_resources()).resources

        resources = asyncio.run(get_resources())
        uris = {str(r.uri) for r in resources}
        self.assertTrue(
            any(u.endswith("json/cloudfiles-onboarding.template") for u in uris),
            f"missing cloudfiles template in resources: {sorted(uris)[:5]}",
        )

    def test_read_resource_returns_template_text(self):
        async def read_template():
            async with Client(mcp_server.build_server(), raise_exceptions=True) as client:
                return await client.read_resource(
                    "sdp-meta://templates/json/cloudfiles-onboarding.template"
                )

        result = asyncio.run(read_template())
        self.assertIn("data_flow_id", result.contents[0].text)


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BuildServerTests(unittest.TestCase):
    """Smoke-test that build_server registers handlers without crashing."""

    def test_build_server_returns_named_server(self):
        srv = mcp_server.build_server(MagicMock())
        self.assertEqual(srv.name, "sdp-meta")


class CliWiringTests(unittest.TestCase):
    """The `mcp` command must be reachable from the CLI dispatcher."""

    def test_mcp_command_in_mapping(self):
        from databricks.labs.sdp_meta.cli import MAPPING

        self.assertIn("mcp", MAPPING)

    def test_mcp_command_in_labs_yml(self):
        import yaml

        with open("labs.yml") as f:
            doc = yaml.safe_load(f)
        commands = {c["name"] for c in doc["commands"]}
        self.assertIn("mcp", commands)

    def test_mcp_handler_raises_useful_error_when_extra_missing(self):
        """If `mcp` extra is missing, the CLI handler should ImportError clearly."""
        from databricks.labs.sdp_meta.cli import mcp as mcp_cmd

        with patch.dict("sys.modules", {"databricks.labs.sdp_meta.mcp_server": None}):
            with self.assertRaises(ImportError) as ctx:
                mcp_cmd(MagicMock())
            self.assertIn("mcp", str(ctx.exception).lower())


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class McpCliTests(unittest.TestCase):
    """The local server module must not shadow the external MCP SDK."""

    def setUp(self):
        from databricks.labs.sdp_meta import cli

        self.cli = cli
        self.script_dir = os.path.dirname(os.path.abspath(cli.__file__))
        self._orig_path = list(sys.path)

    def tearDown(self):
        sys.path[:] = self._orig_path

    def test_command_runs_without_mutating_sys_path(self):
        sys.path.insert(0, self.script_dir)
        with patch(
            "databricks.labs.sdp_meta.mcp_server.run_stdio"
        ) as mock_run:
            self.cli.mcp(MagicMock())
        mock_run.assert_called_once()
        self.assertEqual(os.path.abspath(sys.path[0]), self.script_dir)

    def test_missing_sdk_reports_extra_not_installed(self):
        # Genuine missing dependency: find_spec("mcp") is None AND the
        # server import fails -> the message must point at the extra.
        with patch.dict(
            "sys.modules", {"databricks.labs.sdp_meta.mcp_server": None}
        ):
            with patch("importlib.util.find_spec", return_value=None):
                with self.assertRaises(ImportError) as ctx:
                    self.cli.mcp(MagicMock())
        self.assertIn("extra is not installed", str(ctx.exception))

    def test_import_failure_with_sdk_present_reports_original_error(self):
        # SDK is present but the server import still fails -> this is NOT a
        # missing extra, so report it as an import failure.
        with patch.dict(
            "sys.modules", {"databricks.labs.sdp_meta.mcp_server": None}
        ):
            with patch("importlib.util.find_spec", return_value=object()):
                with self.assertRaises(ImportError) as ctx:
                    self.cli.mcp(MagicMock())
        self.assertIn("failed to import", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
