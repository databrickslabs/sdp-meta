"""Parse the stdout / stderr / returncode of a CLI demo subprocess into
the dict shape the frontend's ``renderApiResponse()`` expects.

Two layers:
  * ``_parse_command_result`` \u2014 pure-Python: takes (stdout, stderr,
    returncode) and returns a dict with ``modal_content`` (success-modal
    payload if a job/pipeline URL was extractable), plus the raw streams.
  * ``extract_command_output`` \u2014 thin Flask wrapper around it that
    ``jsonify``-es the result for direct return from a route handler.

The parser handles three URL shapes:

  1. Explicit ``url=https://...`` printed by ``SDPMETARunner.open_job_url``
     (most authoritative).
  2. Any URL containing ``/jobs/`` or ``/pipelines/``.
  3. Hash-routed legacy run URLs (``<host>/?o=ID#job/<JOB_ID>/run/<RUN_ID>``)
     from the interactive demo's serverless-stable workspace.

It explicitly REJECTS SDK-internal URLs (``/oidc/``, ``/api/``) that the
SDK logs at OAuth-token acquisition time \u2014 without this filter a
silent demo failure would surface the OIDC endpoint as the deploy
result and the success modal would light up incorrectly.
"""

from __future__ import annotations

import re

from flask import jsonify


SDK_INTERNAL_PATHS = ('/oidc/', '/api/')


def _strip_trailing_punct(u: str) -> str:
    return re.sub(r'[,;:.)+]+$', '', u)


def _parse_command_result(stdout: str, stderr: str, returncode: int) -> dict:
    """Return the dict that the frontend's ``renderApiResponse()`` expects."""

    # Pipeline IDs are UUIDs (e.g. a1b2c3d4-...); job IDs are numeric.
    # Try UUID-style pipeline_id first, then fall back to numeric ids.
    pipeline_id_match = re.search(
        r"pipeline_id[=:\s]+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        stdout, re.IGNORECASE,
    )
    # ``job_id=N`` / ``pipeline=N`` covers the bundle/CLI demos.
    # The interactive demo prints a hash-routed legacy run URL of the
    # shape ``<host>/?o=ID#job/<JOB_ID>/run/<RUN_ID>`` (workspace-side
    # ``Jobs.get_run().run_page_url`` on the serverless-stable shard),
    # so also recognise the numeric id inside ``#job/<N>/`` so the
    # success modal lights up for /demo_interactive launches.
    job_id_match = re.search(
        r"job_id=(\d+)|pipeline=(\d+)|#job/(\d+)/", stdout
    )

    if pipeline_id_match:
        pipeline_id = pipeline_id_match.group(1)
    elif job_id_match:
        pipeline_id = (
            job_id_match.group(1)
            or job_id_match.group(2)
            or job_id_match.group(3)
        )
    else:
        pipeline_id = None

    # ── URL extraction ───────────────────────────────────────────────
    # Resolve the job/pipeline URL the user should click on, in priority
    # order:
    #
    #   1. The explicit ``url=https://...`` printed by SDPMETARunner.open_job_url
    #      (and the demo helpers that mimic it). Most authoritative.
    #   2. Any URL containing ``/jobs/`` or ``/pipelines/`` — what the demos
    #      ultimately want to surface.
    #   3. Any URL in stdout, EXCLUDING SDK-internal endpoints (``/oidc/...``,
    #      ``/api/...``). Inside a Databricks Apps container the SDK logs the
    #      OAuth token endpoint (``{host}/oidc/v1/token``) when it acquires a
    #      service-principal token; without this filter the previous "last
    #      URL wins" heuristic would surface that endpoint as the deploy
    #      result.
    job_url = None
    explicit_match = re.search(
        r"(?:job created successfully|pipeline created successfully|launched|run page).*?(?:url=)?(https?://\S+)",
        stdout,
        re.IGNORECASE,
    )
    if explicit_match:
        job_url = _strip_trailing_punct(explicit_match.group(1))
    else:
        all_urls = [_strip_trailing_punct(u) for u in re.findall(r"https?://\S+", stdout)]
        # Only surface URLs that actually point at a job or pipeline.
        # ``#job/`` / ``#pipeline/`` covers the hash-routed legacy run URLs
        # emitted by the interactive demo (workspace-side ``run_page_url``
        # on the serverless-stable shard).
        job_pipeline_urls = [
            u for u in all_urls
            if (
                '/jobs/' in u or '/pipelines/' in u
                or '#job/' in u or '#pipeline/' in u
            )
            and not any(p in u for p in SDK_INTERNAL_PATHS)
        ]
        if job_pipeline_urls:
            job_url = job_pipeline_urls[-1]

    # If we extracted a pipeline UUID but the URL is missing/wrong,
    # build the direct pipeline URL from any workspace-host URL we did
    # find. Match AWS (*.cloud.databricks.com), Azure (*.azuredatabricks.net),
    # and GCP (*.gcp.databricks.com) workspace hosts \u2014 restricting to
    # AWS would silently drop the success modal on Azure/GCP workspaces.
    if pipeline_id and (
        not job_url
        or not any(marker in job_url for marker in ('/pipelines/', '/jobs/', '#pipeline/', '#job/'))
    ):
        all_hosts = re.findall(
            r"(https?://[a-zA-Z0-9.\-]+\."
            r"(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com))",
            stdout,
        )
        if all_hosts:
            job_url = f"{all_hosts[0]}/pipelines/{pipeline_id}"

    if job_url:
        modal_html = {
            'title': 'Pipeline Created Successfully',
            'job_id': pipeline_id,
            'job_url': job_url,
        }
    else:
        modal_html = None
    return {
        'modal_content': modal_html,
        'stdout': stdout,
        'stderr': stderr,
        'returncode': returncode,
    }


def extract_command_output(result):
    """Thin Flask wrapper \u2014 call ``_parse_command_result`` and ``jsonify``."""
    return jsonify(_parse_command_result(result.stdout, result.stderr, result.returncode))
