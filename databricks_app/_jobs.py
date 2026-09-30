"""Process-global background-job store.

Keyed by a hex token returned to the client when a long-running
subprocess (onboarding, deploy) starts in a background thread. The
``/api/job/<token>/logs`` endpoint polls each entry until ``done`` is
true.

Each entry has the shape:
  {
    'logs': [{'stream': 'stdout'|'stderr', 'line': str}, ...],
    'done': bool,
    'returncode': int | None,
    'stdout': str,           # full stdout joined when done
    'stderr': str,           # full stderr joined when done
    'modal_content': dict | None,
    'error': str | None,     # set when the background thread itself crashes
  }
"""

from __future__ import annotations

import os
import time
import threading
import uuid


_jobs: dict = {}
_jobs_lock = threading.RLock()
_MAX_LOG_LINES = 5000
_MAX_COMPLETED_JOBS = 50
_COMPLETED_JOB_TTL_SECONDS = 60 * 60


def _positive_int_env(name: str, default: int) -> int:
    """Read a positive integer environment setting with a safe fallback."""
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


_MAX_ACTIVE_DEMO_JOBS = _positive_int_env(
    'SDP_META_MAX_ACTIVE_DEMOS',
    8,
)


class JobCapacityError(RuntimeError):
    """Raised when a bounded job category has no free launch slots."""


def _prune_jobs(now: float | None = None) -> None:
    """Evict expired and excess completed jobs from the process store."""
    with _jobs_lock:
        now = time.time() if now is None else now
        stale = [
            token
            for token, job in _jobs.items()
            if job.get('done')
            and now - job.get('finished_at', now) > _COMPLETED_JOB_TTL_SECONDS
        ]
        for token in stale:
            _jobs.pop(token, None)

        completed = sorted(
            (
                (job.get('finished_at', 0), token)
                for token, job in _jobs.items()
                if job.get('done')
            ),
            reverse=True,
        )
        for _, token in completed[_MAX_COMPLETED_JOBS:]:
            _jobs.pop(token, None)


def _new_job_token(
    *,
    kind: str = 'generic',
    max_active_for_kind: int | None = None,
) -> str:
    """Allocate a fresh job-token entry in ``_jobs`` and return the token."""
    with _jobs_lock:
        _prune_jobs()
        if max_active_for_kind is not None:
            active_count = sum(
                1
                for job in _jobs.values()
                if not job.get('done') and job.get('kind') == kind
            )
            if active_count >= max_active_for_kind:
                raise JobCapacityError(
                    f"Too many active {kind} jobs "
                    f"({active_count}/{max_active_for_kind})"
                )
        token = uuid.uuid4().hex
        _jobs[token] = {
            'kind': kind,
            'logs': [],
            'log_base_offset': 0,
            'done': False,
            'returncode': None,
            'stdout': '',
            'stderr': '',
            'modal_content': None,
            'error': None,
            'created_at': time.time(),
            'finished_at': None,
        }
        return token


def _append_job_log(job: dict, entry: dict) -> None:
    """Append one line while bounding retained per-job output."""
    with _jobs_lock:
        if len(job['logs']) >= _MAX_LOG_LINES:
            job['logs'].pop(0)
            job['log_base_offset'] += 1
        job['logs'].append(entry)


def _update_job(token: str, **values) -> None:
    """Atomically update fields on an existing job."""
    with _jobs_lock:
        job = _jobs.get(token)
        if job is not None:
            job.update(values)


def _mark_job_done(token: str) -> None:
    """Finalize a job and enforce completed-job retention limits."""
    with _jobs_lock:
        job = _jobs.get(token)
        if job is None:
            return
        job['done'] = True
        job['finished_at'] = time.time()
        _prune_jobs()


def _get_job(token: str):
    """Look up a job entry by token, returning ``None`` if absent."""
    with _jobs_lock:
        _prune_jobs()
        return _jobs.get(token)


def _get_job_snapshot(token: str):
    """Return a stable copy suitable for request handlers."""
    with _jobs_lock:
        _prune_jobs()
        job = _jobs.get(token)
        if job is None:
            return None
        snapshot = dict(job)
        snapshot['logs'] = [dict(entry) for entry in job['logs']]
        return snapshot
