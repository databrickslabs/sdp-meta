"""Regression tests for the Databricks App deployment staging allow-lists."""

import ast
import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SETUP_PY = REPO_ROOT / "setup.py"
DEPLOY_SH = REPO_ROOT / "scripts" / "deploy_app.sh"
DEPLOY_PS1 = REPO_ROOT / "scripts" / "deploy_app.ps1"
DATABRICKSIGNORE = REPO_ROOT / ".databricksignore"

BASH_DIRS_RE = re.compile(r"^APP_RUNTIME_DIRS=\((.*?)^\)", re.MULTILINE | re.DOTALL)
PS1_DIRS_RE = re.compile(
    r"\$RuntimeDirs\s*=\s*@\((.*?)^\s*\)\s*$", re.MULTILINE | re.DOTALL
)


def _wheel_build_dirs() -> set[str]:
    """Return top-level directories read by setup.py while building the wheel."""
    tree = ast.parse(SETUP_PY.read_text(encoding="utf-8"), str(SETUP_PY))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "setup":
            for keyword in node.keywords:
                if keyword.arg == "package_dir":
                    package_dir = ast.literal_eval(keyword.value)
                    break
            else:
                continue
            break
    else:
        raise AssertionError("setup.py has no setup(package_dir=...) call")

    return {Path(path).parts[0] for path in package_dir.values()} | {"examples"}


def _bash_staged_dirs() -> set[str]:
    match = BASH_DIRS_RE.search(DEPLOY_SH.read_text(encoding="utf-8"))
    if not match:
        raise AssertionError("APP_RUNTIME_DIRS=(...) not found in deploy_app.sh")
    lines = (line.split("#", 1)[0].strip() for line in match.group(1).splitlines())
    return {line.split()[0] for line in lines if line}


def _powershell_staged_dirs() -> set[str]:
    match = PS1_DIRS_RE.search(DEPLOY_PS1.read_text(encoding="utf-8"))
    if not match:
        raise AssertionError("$RuntimeDirs = @(...) not found in deploy_app.ps1")
    body = "\n".join(line.split("#", 1)[0] for line in match.group(1).splitlines())
    return set(re.findall(r"'([^']+)'", body))


def _ignored_top_level_dirs() -> set[str]:
    """Return simple root directory exclusions from .databricksignore."""
    rules = set()
    for line in DATABRICKSIGNORE.read_text(encoding="utf-8").splitlines():
        rule = line.strip()
        if not rule or rule.startswith("#") or rule.startswith("!"):
            continue
        normalized = rule.strip("/")
        if "/" not in normalized and not any(char in normalized for char in "*?[]"):
            rules.add(normalized)
    return rules


class AppDeployStagingTests(unittest.TestCase):
    def test_both_scripts_stage_wheel_build_directories(self):
        required = _wheel_build_dirs()
        self.assertFalse(required - _bash_staged_dirs())
        self.assertFalse(required - _powershell_staged_dirs())

    def test_deploy_scripts_stage_the_same_directories(self):
        self.assertEqual(_bash_staged_dirs(), _powershell_staged_dirs())

    def test_wheel_build_directories_are_not_ignored_by_sync(self):
        ignored_required_dirs = _wheel_build_dirs() & _ignored_top_level_dirs()
        self.assertFalse(
            ignored_required_dirs,
            ".databricksignore excludes wheel-build directories: "
            f"{sorted(ignored_required_dirs)}",
        )
