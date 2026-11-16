"""Git-ref-based wheel builder for backward-compatibility integration tests.

By default, the backward-compatibility test runs Phase 1 against a v0.0.10
wheel and Phase 2 against the moving ``main`` pre-release target. Callers can
pin any tag, branch, or SHA; after v0.2.0 is published, post-release checks
should explicitly use the ``v0.2.0`` tag. Building both wheels from the SAME
local clone — without polluting the working tree — is what this module does.

Mechanism
---------

For each requested git ref:

1. Create a fresh ``git worktree`` under a sibling directory
   (``.bc_wheels/<ref-slug>/``). A worktree is a cheap second checkout
   of the same repo at the requested ref; the primary checkout the user
   is editing is left alone.
2. Run ``python setup.py bdist_wheel`` inside the worktree to produce the
   ``.whl`` artifact in ``<worktree>/dist/``.
3. Copy the wheel out to ``.bc_wheels/dist/`` (a stable per-run location)
   so callers don't have to keep the worktree alive.
4. Remove the worktree (idempotent on subsequent calls — we ``--force``).

The builder is intentionally minimal: no virtualenv, no
``pip wheel . --no-deps``, no ``python -m build``. Just ``setup.py
bdist_wheel`` because supported historical and current refs can run it
without extra project tooling. Build dependencies (``setuptools``, ``wheel``) are
expected on the developer's PATH already — the same ``pip install -e .``
they ran for unit tests covers it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional


_DEFAULT_BUILD_ROOT = ".bc_wheels"


class WheelBuilderError(RuntimeError):
    """Raised when a wheel build fails."""


def _slugify_ref(ref: str) -> str:
    """Make a filesystem-safe slug for a git ref.

    ``feature/sdp-meta`` -> ``feature_sdp-meta``. Tags like ``v0.0.10`` are
    already filesystem-safe but the slash-replacement still applies.
    """
    return ref.replace("/", "_").replace("\\", "_").replace(":", "_")


def _run(cmd: list[str], cwd: Optional[Path] = None) -> None:
    """Run a subprocess command, surfacing stderr/stdout on failure."""
    print(f"  $ {' '.join(cmd)}{' (in ' + str(cwd) + ')' if cwd else ''}")
    proc = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise WheelBuilderError(
            f"Command failed (exit {proc.returncode}): {' '.join(cmd)}\n"
            f"--- stdout ---\n{proc.stdout}\n"
            f"--- stderr ---\n{proc.stderr}"
        )


class GitRefWheelBuilder:
    """Builds Python wheels from arbitrary git refs of the local repo.

    Parameters
    ----------
    repo_root : Path | str | None
        Root of the git repo to build from. Defaults to the directory two
        levels up from this file (i.e. the project root).
    build_root : Path | str | None
        Directory where worktrees and the final ``dist/`` lives. Defaults
        to ``<repo_root>/.bc_wheels`` and is git-ignored via the existing
        ``.gitignore`` (we add a top-level entry in this PR).

    Usage
    -----

        builder = GitRefWheelBuilder()
        source_whl = builder.build("v0.0.10")
        target_whl = builder.build("v0.2.0")
        # Both paths are absolute, both wheels live under <repo>/.bc_wheels/dist/.
        builder.cleanup()
    """

    def __init__(
        self,
        repo_root: Optional[os.PathLike] = None,
        build_root: Optional[os.PathLike] = None,
    ) -> None:
        if repo_root is None:
            repo_root = Path(__file__).resolve().parent.parent
        self.repo_root: Path = Path(repo_root).resolve()
        if build_root is None:
            build_root = self.repo_root / _DEFAULT_BUILD_ROOT
        self.build_root: Path = Path(build_root).resolve()
        self.dist_dir: Path = self.build_root / "dist"
        self._worktrees: list[Path] = []

    # --- public API -------------------------------------------------------

    def build(
        self,
        ref: str,
        *,
        fetch: bool = True,
    ) -> Path:
        """Build a wheel from ``ref`` and return its absolute path.

        ``ref`` is anything ``git checkout`` understands: a tag
        (``v0.0.10``), a branch (``main``, ``feature/sdp-meta``), or a
        SHA. When ``fetch`` is True (the default), the builder runs
        ``git fetch origin <ref>`` first so freshly-pushed remote refs
        are visible locally.

        The wheel is always built from the repository root.
        """
        self._ensure_repo()
        if fetch:
            self._fetch(ref)

        slug = _slugify_ref(ref)
        worktree = self.build_root / "worktrees" / slug
        self._add_worktree(worktree, ref)
        try:
            build_cwd = worktree
            self._build_in_worktree(build_cwd)
            built = self._collect_wheel(build_cwd)
            self.dist_dir.mkdir(parents=True, exist_ok=True)
            target = self.dist_dir / built.name
            shutil.copy2(built, target)
            print(f"  -> built wheel for ref={ref!r}: {target}")
            return target.resolve()
        finally:
            self._remove_worktree(worktree)

    def build_from_worktree(self) -> Path:
        """Build a wheel directly from the developer's working tree.

        ``ref``-less companion to :meth:`build` for the case where the
        contributor wants to test changes that are NOT yet committed
        to any git ref. No fetch, no ``git worktree add``, no extra
        checkout: ``setup.py bdist_wheel`` runs in ``<repo_root>`` directly,
        so any uncommitted edits
        in the working tree are picked up.

        The produced wheel is still copied into ``<build_root>/dist/``
        (the same location as ref-based builds) so callers can use it
        interchangeably. The wheel's filename is whatever the local
        ``setup.py`` says (e.g. ``databricks_labs_sdp_meta-0.2.0-py3-none-any.whl``);
        if a same-named wheel from a previous ref-based build is
        already in ``dist/``, this build silently overwrites it.

        Use ONLY for development. Production / CI runs should pin a
        specific git ref via :meth:`build` so the artifact is
        reproducible from version control.
        """
        build_cwd = self.repo_root
        print("  building wheel from local working tree (no git checkout)")
        self._build_in_worktree(build_cwd)
        built = self._collect_wheel(build_cwd)
        self.dist_dir.mkdir(parents=True, exist_ok=True)
        target = self.dist_dir / built.name
        shutil.copy2(built, target)
        print(f"  -> built wheel from worktree: {target}")
        return target.resolve()

    def cleanup(self) -> None:
        """Remove leftover worktrees (no-op if all builds succeeded)."""
        for wt in list(self._worktrees):
            self._remove_worktree(wt)

    # --- internals --------------------------------------------------------

    def _ensure_repo(self) -> None:
        if not (self.repo_root / ".git").exists():
            raise WheelBuilderError(
                f"repo_root={self.repo_root} does not contain a .git directory; "
                "GitRefWheelBuilder requires a real git checkout."
            )

    def _fetch(self, ref: str) -> None:
        """Best-effort `git fetch origin <ref>` so remote refs resolve.

        Failures here are non-fatal: a developer working offline against a
        local-only ref shouldn't be blocked. The subsequent ``git worktree
        add`` will surface a clear error if the ref truly doesn't exist.
        """
        try:
            _run(["git", "fetch", "origin", ref], cwd=self.repo_root)
        except WheelBuilderError as exc:
            print(f"  warn: git fetch origin {ref} failed (continuing): {exc}")

    def _add_worktree(self, worktree: Path, ref: str) -> None:
        if worktree.exists():
            self._remove_worktree(worktree)
        worktree.parent.mkdir(parents=True, exist_ok=True)
        # ``--detach`` avoids creating a branch that pins the ref; we
        # just want a checkout. ``--force`` skips the safety check
        # that errors if a previous worktree path was abandoned.
        #
        # Branches that only exist as remote-tracking refs (e.g.
        # ``feature/sdp-meta`` on a fresh clone whose only local
        # branch is ``main``) are NOT resolvable as bare ``<ref>``
        # by ``git worktree add``. Try the bare ref first; on
        # ``invalid reference`` fall back to ``origin/<ref>`` once.
        # Tags + SHAs always resolve unprefixed.
        candidates = [ref]
        if "/" not in ref or not ref.startswith(("origin/", "refs/")):
            candidates.append(f"origin/{ref}")
        last_err: Optional[WheelBuilderError] = None
        for candidate in candidates:
            try:
                _run(
                    [
                        "git",
                        "worktree",
                        "add",
                        "--detach",
                        "--force",
                        str(worktree),
                        candidate,
                    ],
                    cwd=self.repo_root,
                )
                self._worktrees.append(worktree)
                if candidate != ref:
                    print(
                        f"  note: ref={ref!r} resolved as {candidate!r} "
                        "(local branch missing; using remote-tracking ref)."
                    )
                return
            except WheelBuilderError as exc:
                last_err = exc
                continue
        # Out of candidates -- surface the last error.
        assert last_err is not None
        raise last_err

    def _build_in_worktree(self, build_cwd: Path) -> None:
        # Clean up any prior dist/ inside the build dir so we know
        # exactly which wheel was just produced.
        dist = build_cwd / "dist"
        if dist.exists():
            shutil.rmtree(dist)
        _run(
            [sys.executable, "setup.py", "bdist_wheel"],
            cwd=build_cwd,
        )

    def _collect_wheel(self, build_cwd: Path) -> Path:
        dist = build_cwd / "dist"
        wheels = sorted(dist.glob("*.whl"))
        if not wheels:
            raise WheelBuilderError(
                f"setup.py bdist_wheel produced no .whl in {dist}"
            )
        if len(wheels) > 1:
            # Pick the newest (mtime). Multiple wheels are unexpected — log
            # the rest so a flaky build is debuggable.
            print(
                f"  warn: multiple wheels produced in {dist}, picking newest: "
                f"{[w.name for w in wheels]}"
            )
            wheels.sort(key=lambda p: p.stat().st_mtime)
        return wheels[-1]

    def _remove_worktree(self, worktree: Path) -> None:
        if not worktree.exists():
            if worktree in self._worktrees:
                self._worktrees.remove(worktree)
            return
        try:
            _run(
                ["git", "worktree", "remove", "--force", str(worktree)],
                cwd=self.repo_root,
            )
        except WheelBuilderError as exc:
            # Last-resort filesystem cleanup. ``git worktree remove``
            # occasionally fails on transient locks; the worktree
            # directory itself is just files, so removing it directly is
            # safe — git will reconcile via ``git worktree prune`` next
            # time.
            print(
                f"  warn: 'git worktree remove' failed ({exc}); "
                f"falling back to rmtree on {worktree}"
            )
            shutil.rmtree(worktree, ignore_errors=True)
        finally:
            if worktree in self._worktrees:
                self._worktrees.remove(worktree)


def main() -> int:
    """CLI entry point: build wheels for one or more refs.

    Usage::

        python integration_tests/wheel_builder.py v0.0.10 v0.2.0
    """
    if len(sys.argv) < 2:
        print(__doc__)
        print("Usage: python integration_tests/wheel_builder.py <ref> [<ref> ...]")
        return 2
    builder = GitRefWheelBuilder()
    try:
        for ref in sys.argv[1:]:
            print(f"Building wheel for ref={ref!r} ...")
            path = builder.build(ref)
            print(f"  -> {path}")
    finally:
        builder.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
