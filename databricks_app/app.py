"""SDP-META Databricks App \u2014 Flask entrypoint.

This module is intentionally slim. All business logic lives in
``routes/`` (route handlers grouped as Flask Blueprints) and
``services/`` (pure-Python helpers). ``app.py`` only:

  1. Configures logging (must run before any module that emits logs
     at import time \u2014 keep at the top).
  2. Constructs the Flask app instance.
  3. Attaches the global JSON error handler.
  4. Attaches the security-headers after-request hook.
  5. Registers every blueprint via ``routes.register_blueprints``.
  6. Re-exports the private helpers the existing test suite reaches
     into via ``app_mod.<name>`` (see ``tests/test_app_*.py``). These
     re-exports are not new public API \u2014 they exist purely to keep
     the existing tests working after the helpers moved out of this
     file. Future test code should import directly from the
     ``services/`` modules.

Gunicorn launches this app from ``start.sh`` as::

    gunicorn --chdir databricks_app/ "app:app"

which sets ``databricks_app/`` as the package root on sys.path so the
sibling top-level modules (``_config``, ``_jobs``, etc.) and sub-
packages (``routes/``, ``services/``) all import correctly.
"""

from __future__ import annotations

import logging
import os
import subprocess  # noqa: F401 \u2014 re-exported below so tests can mock subprocess.Popen

from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException

# ── Logging ──────────────────────────────────────────────────────────────────
# Always log to stdout/stderr (captured by the Apps runtime). Add a
# file handler only if the target path is writable \u2014 the App
# container's working directory can be read-only, and a FileHandler
# that can't open its file raises during ``basicConfig`` and takes
# down the whole app at import time.
_log_handlers: list[logging.Handler] = [logging.StreamHandler()]
try:
    _log_file = os.path.join(os.environ.get("TMPDIR", "/tmp"), "sdp-meta-app.log")
    _log_handlers.append(logging.FileHandler(_log_file))
except OSError:
    pass  # read-only FS \u2014 stdout/stderr capture is enough.

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=_log_handlers,
)
logger = logging.getLogger(__name__)


# ── Flask app instance ───────────────────────────────────────────────────────
app = Flask(__name__)


# ── Global JSON error handler ────────────────────────────────────────────────
# Flask returns HTML for unhandled exceptions by default. Override to
# return JSON so the frontend never receives an HTML page it can't
# parse. Standard HTTP errors (404, 405, etc.) are passed through
# normally so Flask can return the correct status code without
# logging them as application errors.
@app.errorhandler(Exception)
def handle_exception(exc):
    if isinstance(exc, HTTPException):
        return jsonify({'error': exc.description or str(exc)}), exc.code
    logger.exception("Unhandled exception in route: %s", exc)
    return jsonify({
        'error': str(exc),
        'stdout': '',
        'stderr': '',
        'returncode': -1,
        'modal_content': None,
    }), 500


@app.errorhandler(404)
def handle_404(exc):
    return jsonify({'error': 'Not found'}), 404


# ── App version injection ────────────────────────────────────────────────────
# Surface the installed ``databricks.labs.sdp_meta`` package version to
# every Jinja template via ``{{ app_version }}``. The landing page
# footer renders ``SDP-META v{{ app_version }}`` so the UI footer stays
# in lock-step with the wheel version automatically \u2014 no more drift
# between ``__about__.__version__`` and a hardcoded string in the HTML.
#
# Resolution order:
#   1. Installed package ``databricks.labs.sdp_meta.__about__.__version__``
#      \u2014 the canonical source. Both PYTHONPATH-based dev runs and
#      wheel-installed Apps containers hit this branch.
#   2. Fallback to ``"unknown"`` if the import fails. The App can still
#      boot for diagnostic purposes (e.g. wheel removed mid-debug)
#      without crashing on a missing version string.
try:
    from databricks.labs.sdp_meta.__about__ import (  # noqa: E402
        __version__ as _SDP_META_VERSION,
    )
except Exception:  # noqa: BLE001 \u2014 any import failure must not crash the App
    _SDP_META_VERSION = "unknown"
    logger.warning(
        "Could not resolve sdp-meta package version; landing page will "
        "show 'unknown'. Check that databricks.labs.sdp_meta is on "
        "PYTHONPATH or installed as a wheel."
    )


@app.context_processor
def _inject_app_version():
    """Make the package version available in every Jinja template as
    ``{{ app_version }}``. Single source of truth; safe to add more
    keys here if other globals need surfacing to the UI later."""
    return {"app_version": _SDP_META_VERSION}


# ── Security headers ─────────────────────────────────────────────────────────
@app.after_request
def add_security_headers(response):
    """Attach HTTP security headers to every response (fix M4)."""
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "frame-src 'self' *.cloud.databricks.com; "
        "object-src 'none';"
    )
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response


# ── Blueprint registration ───────────────────────────────────────────────────
# Routes live in ``routes/<cluster>.py``; each module exports a
# ``bp`` Blueprint. ``register_blueprints`` walks the registry and
# attaches every blueprint to this app instance. To add a new route
# cluster, drop a new module under ``routes/`` and add it to
# ``routes/__init__.py::_BLUEPRINTS``.
from routes import register_blueprints  # noqa: E402 \u2014 register after logging is set up

register_blueprints(app)


# ── Test backward-compat re-exports ──────────────────────────────────────────
# The existing test suite reaches into this module by name
# (``app_mod._BUNDLED_DEMO_SPECS``, ``app_mod._extract_required_files``,
# etc.). Re-export the symbols those tests touch so the refactor is
# a pure code reorganisation \u2014 zero test changes required. New
# tests should import the helpers directly from the ``services/``
# modules instead of through this surface.
from _command_output import (  # noqa: E402, F401
    _parse_command_result,
    extract_command_output,
)
from _config import (  # noqa: E402, F401
    _get_warehouse_id,
    _repo_root,
)
from _jobs import _jobs  # noqa: E402, F401
from services.onboarding.bundled_specs import (  # noqa: E402, F401
    _BUNDLED_DEMO_SPECS,
    _list_bundled_specs,
)
from services.onboarding.env_validation import (  # noqa: E402, F401
    _detect_env_suffixes,
    _verify_env_matches_template,
)
from services.onboarding.path_resolver import (  # noqa: E402, F401
    _OnboardingFileError,
    _preflight_parse_onboarding,
    _resolve_local_onboarding_path,
)
from services.onboarding.required_files import (  # noqa: E402, F401
    _check_required_files_existence,
    _extract_required_files,
)
from services.onboarding.quality_preflight import (  # noqa: E402, F401
    _verify_quality_configuration,
)


if __name__ == '__main__':
    # Fix C4: never run the Werkzeug interactive debugger in production.
    # To enable debug mode locally: ``export FLASK_DEBUG=true``.
    app.run(debug=os.getenv('FLASK_DEBUG', 'false').lower() == 'true')
