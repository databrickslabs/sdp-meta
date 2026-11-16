"""Shared pytest configuration for the SDP-META unit-test suite.

Three pieces of cross-cutting test hygiene live here:

1. Block ``webbrowser.open`` for the whole test session.

   ``src/databricks/labs/sdp_meta/cli.py`` calls ``webbrowser.open(...)``
   after creating jobs / pipelines so an interactive ``sdp-meta``
   session lands the user on the Databricks UI for the new artifact.
   The CLI tests in ``tests/test_cli.py`` mock :class:`WorkspaceClient`
   but historically did NOT mock the ``webbrowser`` module, so every
   test that exercised those code paths fired ``webbrowser.open`` with
   a URL built from MagicMock attributes — popping a real browser tab
   on every test run.

   ``cli.py`` now routes through ``_maybe_open_url`` which honours
   ``SDP_META_NO_BROWSER=1``. Setting the env var here, at conftest
   import time (which runs *before* any test module is collected),
   guarantees the suppression is in place even for tests that import
   ``cli`` at module-load time.

   Use ``os.environ.setdefault`` so a developer can still opt back in
   for a single run with ``SDP_META_NO_BROWSER=0 pytest tests/...``
   when they want to debug the actual browser-launch behaviour.

2. Belt-and-braces ``$BROWSER`` override.

   Anything we missed routing through ``_maybe_open_url`` (third-party
   libs, future regressions, the SDK's U2M OAuth flow if a test ever
   constructs a real ``WorkspaceClient``) still goes through Python's
   ``webbrowser`` module, which honours the ``$BROWSER`` env var when
   picking the launcher. Pointing it at ``true`` (the Unix no-op
   command) makes the OS layer a silent no-op too. Belt and braces.

3. Pin ``PYSPARK_PYTHON`` / ``PYSPARK_DRIVER_PYTHON`` to the running
   interpreter.

   When pyspark spawns Python workers it picks the executable from
   ``$PYSPARK_PYTHON`` (worker) and ``$PYSPARK_DRIVER_PYTHON``
   (driver), falling back to ``python3`` on ``PATH``. On a developer
   machine that has multiple Python interpreters installed (e.g. a
   system Python 3.14 alongside this venv's 3.10), the worker happily
   grabs the wrong one and the JVM blows up with
   ``[PYTHON_VERSION_MISMATCH] Python in worker has different
   version (3, 14) than that in driver 3.10``. Pinning both env vars
   to ``sys.executable`` at conftest import time makes the suite
   reproducible across environments without requiring developers to
   set the vars by hand.

   ``setdefault`` again -- a developer can still override with
   ``PYSPARK_PYTHON=/path/to/python pytest ...`` if they need to.
"""
from __future__ import annotations

import os
import sys

# 1. App-side opt-out flag — read by cli._maybe_open_url.
os.environ.setdefault("SDP_META_NO_BROWSER", "1")

# 2. OS-side fallback — Python's webbrowser module respects $BROWSER.
#    `true` exits 0 immediately and never paints anything to the screen.
os.environ.setdefault("BROWSER", "true")

# 3. Pin pyspark worker + driver Python to the current interpreter
#    so the JVM doesn't pick a stray python3 off $PATH and crash with
#    PYTHON_VERSION_MISMATCH on machines that have multiple Pythons.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# 4. Bump the JVM heap so the per-class shared SparkSession can absorb
#    the cumulative state from ~730 tests without ``OutOfMemoryError:
#    Java heap space``.
#
#    Why this is needed: ``tests/utils.py`` builds Spark in
#    ``setUpClass`` and intentionally never stops it (so subsequent
#    classes' ``getOrCreate()`` reuses the same JVM). Each test
#    leaves behind broadcast variables, query plans, codegen output,
#    etc.; the default 512MB local-mode heap can't hold all of it
#    by the time the suite finishes the readers / writers / pipeline
#    families.
#
#    ``PYSPARK_SUBMIT_ARGS`` is the only way to pass driver-memory
#    in local mode -- ``spark.driver.memory`` set after the JVM is
#    already up is ignored (the heap is fixed at JVM startup). The
#    trailing ``pyspark-shell`` token is required by the launcher.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--driver-memory 4g --conf spark.driver.maxResultSize=1g pyspark-shell",
)
