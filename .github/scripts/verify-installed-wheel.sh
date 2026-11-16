#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <installed-venv-python>" >&2
  exit 2
fi

python_bin="$1"
if [[ "${python_bin}" != /* ]]; then
  if [[ "${python_bin}" == */* ]]; then
    python_dir="$(cd "$(dirname "${python_bin}")" && pwd -P)"
    python_bin="${python_dir}/$(basename "${python_bin}")"
  elif ! python_bin="$(command -v "${python_bin}")"; then
    echo "Python interpreter not found: $1" >&2
    exit 2
  fi
fi
if [[ ! -x "${python_bin}" ]]; then
  echo "Python interpreter is not executable: ${python_bin}" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
export SDP_META_REPO_ROOT
SDP_META_REPO_ROOT="$(cd "${script_dir}/../.." && pwd -P)"

startup_stderr="$(mktemp)"
trap 'rm -f "${startup_stderr}"' EXIT

if ! startup_stdout="$(
  cd /tmp
  "${python_bin}" -c 'print("ok")' 2>"${startup_stderr}"
)"; then
  echo "Python startup failed:" >&2
  cat "${startup_stderr}" >&2
  exit 1
fi
if [[ "${startup_stdout}" != "ok" ]]; then
  printf 'Unexpected Python startup output: %q\n' "${startup_stdout}" >&2
  exit 1
fi
if [[ -s "${startup_stderr}" ]]; then
  echo "Python startup wrote to stderr:" >&2
  cat "${startup_stderr}" >&2
  exit 1
fi

(
  cd /tmp
  "${python_bin}" - <<'PY'
from importlib.metadata import distribution
from importlib.resources import files
import os
from pathlib import Path

import databricks.labs.sdp_meta
import databricks.labs.sdp_meta._packaged_examples

entries = {
    entry.name: entry
    for entry in distribution("databricks-labs-sdp-meta").entry_points
    if entry.group == "group_1"
}
expected = {
    "run": "databricks.labs.sdp_meta.__main__:main",
    "stage_conf": "databricks.labs.sdp_meta.stage_conf:main",
}
for name, value in expected.items():
    assert entries[name].value == value

root = files("databricks.labs.sdp_meta._packaged_examples")
templates = [
    path
    for directory in ("json", "yml")
    for path in root.joinpath(directory).rglob("*")
    if path.is_file()
]
examples = Path(os.environ["SDP_META_REPO_ROOT"]) / "examples"
expected_count = sum(
    1
    for directory in ("json", "yml")
    for path in examples.joinpath(directory).rglob("*")
    if path.is_file()
)
assert len(templates) == expected_count, (
    "installed package exposes "
    f"{len(templates)} templates, expected {expected_count}"
)
print("clean Python startup, entry points, and packaged assets verified")
PY
)
