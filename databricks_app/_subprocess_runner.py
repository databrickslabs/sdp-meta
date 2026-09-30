"""Run a CLI subprocess in a background thread, streaming stdout / stderr
into a job entry in the ``_jobs`` registry.

Used by ``/onboarding`` and ``/deploy`` so the frontend can poll
``/api/job/<token>/logs`` for incremental output while the CLI runs.

Why a separate module: the launch + reader-thread + queue pattern was
duplicated verbatim in both routes (~60 lines each). Pulling it out
keeps the route bodies focused on payload-building and lets the
streaming machinery be unit-tested independently \u2014 plus it keeps the
behaviour identical across the two routes so a bug fix in one
automatically applies to the other.
"""

from __future__ import annotations

import logging
import os
import queue as _queue_module
import signal
import subprocess
import sys
import threading
import time
from collections import deque

import _jobs as _jobs_module  # noqa: E402 \u2014 absolute import; databricks_app/ is not a package

logger = logging.getLogger(__name__)


def _process_group_exists(process_group_id: int) -> bool:
    """Return whether a POSIX process group still has live members."""
    try:
        os.killpg(process_group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _terminate_process_tree(
    proc: subprocess.Popen,
    grace_seconds: float,
) -> None:
    """Terminate a child and every descendant in its isolated session."""
    if os.name == 'posix':
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except OSError:
            pass
        else:
            deadline = time.monotonic() + grace_seconds
            while (
                _process_group_exists(proc.pid)
                and time.monotonic() < deadline
            ):
                proc.poll()
                time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))

            # The leader can exit while descendants retain pipes or
            # ignore SIGTERM, so escalation depends on the process
            # group—not only on proc.wait().
            if _process_group_exists(proc.pid):
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

            try:
                proc.wait(timeout=max(1.0, grace_seconds))
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    proc.wait(timeout=max(1.0, grace_seconds))
                except subprocess.TimeoutExpired:
                    logger.error(
                        "Subprocess PID %s did not exit after SIGKILL; "
                        "continuing job finalization",
                        getattr(proc, 'pid', '?'),
                    )
            return

    proc.terminate()
    try:
        proc.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=max(1.0, grace_seconds))
        except subprocess.TimeoutExpired:
            logger.error(
                "Subprocess PID %s did not exit after kill; "
                "continuing job finalization",
                getattr(proc, 'pid', '?'),
            )


def _run_command_in_background(
    token: str,
    command: list[str],
    cwd: str,
    env: dict | None = None,
    cleanup_path: str | None = None,
    idle_timeout_seconds: int = 600,
) -> None:
    """Launch a command in a background thread and stream its output.

    Arguments:
        token: a job token previously allocated via ``_new_job_token``.
        command: subprocess argv, including the executable.
        cwd: working directory for the subprocess \u2014 typically
            ``_repo_root()`` so demo scripts can resolve relative paths.
        env: optional child environment. Defaults to the current process.
        cleanup_path: optional path to ``os.unlink`` after the
            subprocess completes \u2014 used to clean up the tempfile that
            ``_resolve_local_onboarding_path`` may have created when the
            user pointed at a UC Volume / DBFS spec.
        idle_timeout_seconds: terminate a child that produces no output for
            this long. Long-running demo waiters override this default.

    Returns immediately after spawning the background thread; the
    caller is expected to return the token to the client and poll.
    """
    job = _jobs_module._jobs[token]

    # Maximum seconds with no output before we treat the child as
    # hung. Pulled out so it shows up in stack traces and so a test
    # can monkey-patch it instead of waiting 10 minutes.
    _IDLE_TIMEOUT_S = idle_timeout_seconds
    # Graceful-shutdown grace period after ``terminate()`` before we
    # escalate to ``kill()``. CLI shells out to ``pip wheel`` which
    # can take a few seconds to unwind cleanly.
    _TERMINATE_GRACE_S = 10

    def _run():
        proc: subprocess.Popen | None = None
        timed_out = False
        stdout_parts = deque(maxlen=_jobs_module._MAX_LOG_LINES)
        stderr_parts = deque(maxlen=_jobs_module._MAX_LOG_LINES)
        try:
            _env = {**(env or os.environ), 'PYTHONUNBUFFERED': '1'}
            proc = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,          # line-buffered on our side
                env=_env,           # force child Python to flush every print()
                cwd=cwd,
                # The CLI launches Python, pip, and Databricks helper
                # processes. Isolate that tree so an idle timeout can
                # terminate every descendant, not only the direct child.
                start_new_session=(os.name == 'posix'),
            )
            q: _queue_module.Queue = _queue_module.Queue()

            def _reader(pipe, stream_name):
                for line in pipe:
                    q.put((stream_name, line.rstrip('\n')))
                q.put((stream_name, None))  # sentinel

            t1 = threading.Thread(target=_reader, args=(proc.stdout, 'stdout'), daemon=True)
            t2 = threading.Thread(target=_reader, args=(proc.stderr, 'stderr'), daemon=True)
            t1.start()
            t2.start()

            # Read-loop is wrapped so that a queue timeout (child has
            # gone silent for ``_IDLE_TIMEOUT_S``) flips us into the
            # reap path WITHOUT discarding the lines we already
            # collected. The previous code re-raised through the
            # outer ``except Exception`` which clobbered ``stdout`` /
            # ``stderr`` and never called ``proc.wait()`` \u2014 leaking
            # the child as a zombie until the worker process exited.
            done_count = 0
            try:
                while done_count < 2:
                    stream, line = q.get(timeout=_IDLE_TIMEOUT_S)
                    if line is None:
                        done_count += 1
                        continue
                    _jobs_module._append_job_log(
                        job,
                        {'stream': stream, 'line': line},
                    )
                    if stream == 'stdout':
                        stdout_parts.append(line)
                    else:
                        stderr_parts.append(line)
            except _queue_module.Empty:
                timed_out = True
                logger.warning(
                    "Subprocess silent for %ds; terminating PID %s",
                    _IDLE_TIMEOUT_S,
                    getattr(proc, 'pid', '?'),
                )
                _jobs_module._update_job(
                    token,
                    error=(
                        f"Subprocess produced no output for {_IDLE_TIMEOUT_S} "
                        f"seconds; terminated to avoid leaking a zombie process. "
                        f"Any output collected before the timeout is preserved "
                        f"below."
                    ),
                )

            # Either the readers drained both pipes (success path) or
            # we timed out and are about to escalate in ``finally``.
            # A child can close both pipes without exiting, so keep
            # ``wait()`` bounded as well; otherwise it can hold a demo
            # capacity slot forever while producing no more output.
            if not timed_out:
                try:
                    proc.wait(timeout=_IDLE_TIMEOUT_S)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    logger.warning(
                        "Subprocess closed its output streams but did not "
                        "exit within %ds; terminating PID %s",
                        _IDLE_TIMEOUT_S,
                        getattr(proc, 'pid', '?'),
                    )
                    _jobs_module._update_job(
                        token,
                        error=(
                            "Subprocess closed its output streams but did not "
                            f"exit within {_IDLE_TIMEOUT_S} seconds; terminated "
                            "to release its background-job slot."
                        ),
                    )
            _jobs_module._update_job(
                token,
                stdout='\n'.join(stdout_parts),
                stderr='\n'.join(stderr_parts),
                returncode=proc.returncode,
            )
        except Exception as exc:
            logger.exception("Background CLI subprocess thread failed")
            # Preserve the partial output we collected before the
            # exception so the UI can show whatever progress the CLI
            # made before it crashed. The job dict is pre-populated
            # with ``stdout=''`` / ``stderr=''`` / ``error=None`` by
            # ``_new_job_token``, so ``setdefault`` would be a no-op
            # \u2014 we only skip overwriting when a meaningful value
            # is already present (i.e. the success path got far
            # enough to set them).
            values = {'returncode': -1}
            if not job.get('stdout'):
                values['stdout'] = '\n'.join(stdout_parts)
            if not job.get('stderr'):
                values['stderr'] = '\n'.join(stderr_parts)
            if not job.get('error'):
                values['error'] = str(exc)
            _jobs_module._update_job(token, **values)
        finally:
            # Always reap the child, even when the read loop never
            # reached ``proc.wait()`` (timeout) or the Popen call
            # itself raised (proc is None). Without this the child
            # outlives the gunicorn worker as a zombie.
            if proc is not None and (timed_out or proc.poll() is None):
                try:
                    _terminate_process_tree(proc, _TERMINATE_GRACE_S)
                except OSError:
                    # ProcessLookupError etc. \u2014 child already gone.
                    pass
                # If we had to terminate, refresh returncode from the
                # signal that landed.
                if 'returncode' not in job or job['returncode'] is None:
                    _jobs_module._update_job(token, returncode=proc.returncode)
            _jobs_module._mark_job_done(token)
            if cleanup_path and os.path.exists(cleanup_path):
                try:
                    os.unlink(cleanup_path)
                except OSError:
                    pass

    threading.Thread(target=_run, daemon=True).start()


def _run_cli_json_payload(
    token: str,
    json_string: str,
    cwd: str,
    cleanup_path: str | None = None,
) -> None:
    """Run the SDP-META CLI JSON payload through the shared runner."""
    _run_command_in_background(
        token=token,
        command=[
            sys.executable,
            '-m',
            'databricks.labs.sdp_meta.cli',
            json_string,
        ],
        cwd=cwd,
        cleanup_path=cleanup_path,
    )
