"""DLT-META Compatibility Package.

DEPRECATED: This package is a compatibility wrapper for
``databricks-labs-sdp-meta``. Please migrate to
``databricks.labs.sdp_meta`` directly.

This module wires three independent compatibility surfaces:

1. **Flat re-exports** under the ``dlt_meta.<name>`` namespace
   (``from dlt_meta import DataflowPipeline`` etc.). Names with v0.0.10
   spellings are aliased to their v0.1.0 counterparts (``DLTMeta`` →
   ``SDPMeta``, ``DLT_META_RUNNER_NOTEBOOK`` → ``SDP_META_RUNNER_NOTEBOOK``).
2. **``src.*`` aliasing** for v0.0.10 customer notebooks that still type
   ``from src.dataflow_pipeline import DataflowPipeline``. Module objects
   are registered in :data:`sys.modules` so the legacy import path
   resolves without a `ModuleNotFoundError`. See
   :func:`_register_src_aliases`.
3. **Actionable runtime errors** when the underlying import fails:
   instead of silently swallowing the failure (which leaves callers
   staring at confusing ``ImportError: cannot import name
   'DataflowPipeline'`` messages) or re-raising it (which the
   ``dlt_meta.pth`` startup hook would turn into a site.py traceback
   printed on every interpreter launch), each failing submodule is
   replaced by a stub that raises a clear error on attribute access —
   a ``Lakeflow SDP runtime`` message when
   :func:`_optional_runtime_import_error` recognizes the failure, a
   generic missing-dependency message otherwise. Import of
   ``dlt_meta`` itself therefore never raises.

Removal timeline: every alias here is scheduled for removal in v0.2.0.
"""
from __future__ import annotations

import importlib
import os
import sys
import types
import warnings


# ---------------------------------------------------------------------------
# Opt-out env var
#
# Customers with their own ``src/`` package on ``sys.path`` (rare but real
# for monorepo-style notebook layouts) can disable the ``src.*`` aliasing
# AND the package-level deprecation warning by exporting
# ``SDP_META_DISABLE_SRC_ALIAS=1`` before any import. The ``.pth`` shipped
# alongside this package also honours the same env var so opting out of
# the aliasing also opts out of the auto-load.
# ---------------------------------------------------------------------------
_OPT_OUT_ENV = "SDP_META_DISABLE_SRC_ALIAS"
_DISABLED = os.environ.get(_OPT_OUT_ENV) == "1"


# ---------------------------------------------------------------------------
# Submodule alias map
#
# Pinned to the v0.0.10 ``src/*.py`` set
# (``git ls-tree v0.0.10 -- 'src/*.py'``). Tests in
# ``tests/test_compat_src_aliases.py`` assert this list matches the
# v0.0.10 publication so future edits don't drift.
# ---------------------------------------------------------------------------
_SRC_SUBMODULES = (
    "dataflow_pipeline",
    "dataflow_spec",
    "onboard_dataflowspec",
    "pipeline_readers",
    "pipeline_writers",
    "install",
    "cli",
    "config",
    "metastore_ops",
    "uninstall",
    "__main__",
    "__about__",
)


# ---------------------------------------------------------------------------
# Optional-runtime stub: actionable error when pyspark.pipelines is missing
# ---------------------------------------------------------------------------
def _optional_runtime_import_error(exc: BaseException) -> bool:
    """Return ``True`` if *exc* is the SDP-runtime-missing failure mode.

    ``databricks.labs.sdp_meta.dataflow_pipeline`` does
    ``from pyspark import pipelines as dp`` at module load. On a runtime
    that doesn't ship Lakeflow SDP yet, that line surfaces in three
    distinct shapes depending on what's wrong with the platform's
    ``pyspark.pipelines`` package:

    - **No pyspark at all** → ``ModuleNotFoundError`` with
      ``exc.name == 'pyspark'``. CPython reports the *top-level* package
      it failed to find, so ``from pyspark import pipelines`` and
      ``from pyspark.sql import DataFrame`` both surface as
      ``name='pyspark'`` on a machine without pyspark (a laptop or CI
      box that ``pip install dlt-meta``-ed the shim). ``'pyspark.pipelines'``
      / ``'pipelines'`` are also accepted for runtimes where the parent
      package resolves but the submodule lookup fails.
    - **pyspark exists, no Lakeflow SDP** → bare ``ImportError`` with
      message ``cannot import name 'pipelines' from 'pyspark'`` and
      ``name`` unset.
    - **Buggy PySpark on a runtime that has the package but lacks
      ``dlt``** → ``TypeError`` from inside
      ``pyspark.errors.utils.get_error_message`` while PySpark is
      raising its own ``PIPELINES_NOT_SUPPORTED`` exception. The
      runtime's ``pyspark/pipelines/__init__.py`` first tries
      ``from dlt import *``; when that ``ModuleNotFoundError`` falls
      through, the next line is
      ``raise PySparkException(errorClass="PIPELINES_NOT_SUPPORTED")``,
      and the exception constructor crashes on
      ``set(messageParameters)`` because ``messageParameters`` is
      ``None``. This is technically a PySpark bug -- the
      ``PIPELINES_NOT_SUPPORTED`` errorClass entry on that runtime
      requires substitutions PySpark didn't supply. We can't fix
      that, but we can recognize the failure SHAPE: any exception
      whose traceback passes through ``pyspark/pipelines/`` is the
      Lakeflow-SDP-missing case from our point of view, regardless
      of which specific ``Exception`` subclass got raised.

    All three are treated as recoverable (register a stub that raises
    a clear error on access). Callers use the predicate only to pick
    the stub's *message*: SDP-runtime-missing when it returns ``True``,
    a generic compat-import-failure otherwise. Nothing is re-raised at
    import time — the ``.pth`` runs ``import dlt_meta`` at interpreter
    startup, and any exception escaping it is printed by ``site.py``
    on every ``python3`` launch.
    """
    if isinstance(exc, ModuleNotFoundError) and exc.name in {
        "pyspark",
        "pyspark.pipelines",
        "pipelines",
    }:
        return True
    if isinstance(exc, ImportError):
        msg = str(exc)
        if "pipelines" in msg and "pyspark" in msg:
            return True
    # Walk the traceback chain (current + __cause__ + __context__).
    # If any frame is inside ``pyspark/pipelines/``, this is the
    # buggy-error-formatter / PIPELINES_NOT_SUPPORTED case described
    # above. ``os.sep`` keeps the check working on Windows file
    # separators too, though serverless DLT is always Linux.
    needle = f"pyspark{os.sep}pipelines{os.sep}"
    seen: set[int] = set()
    cursor: BaseException | None = exc
    while cursor is not None and id(cursor) not in seen:
        seen.add(id(cursor))
        tb = cursor.__traceback__
        while tb is not None:
            filename = tb.tb_frame.f_code.co_filename
            if needle in filename or "pyspark/pipelines/" in filename:
                return True
            tb = tb.tb_next
        cursor = cursor.__cause__ or cursor.__context__
    return False


def _stub_error_message(qualified_name: str, original_error: str,
                        runtime_missing: bool) -> str:
    """Compose the actionable error a stub raises on attribute access.

    ``runtime_missing=True`` is the Lakeflow-SDP-missing case (the
    predicate matched); ``False`` covers every other import failure —
    e.g. a stripped image missing ``yaml`` or ``databricks-sdk``. Both
    get a stub rather than an import-time raise so the ``.pth`` startup
    hook can never print a traceback (site.py prints any exception a
    ``.pth`` line raises on EVERY interpreter launch); real problems
    still fail loudly, just at first use instead of at startup.
    """
    if runtime_missing:
        return (
            f"{qualified_name} requires the Lakeflow SDP runtime "
            f"(pyspark.pipelines) which is not installed. Run on a "
            f"Databricks Runtime that ships Lakeflow SDP, or use the "
            f"v0.0.10 dlt-meta wheel if you must stay on legacy DLT. "
            f"Original error: {original_error}"
        )
    return (
        f"{qualified_name} is unavailable because importing "
        f"databricks.labs.sdp_meta failed in this Python environment. "
        f"Install the missing dependency and retry. "
        f"Original error: {original_error}"
    )


def _make_stub_module(qualified_name: str, original_error: str,
                      runtime_missing: bool = True) -> types.ModuleType:
    """Build a sentinel module that raises a clear error on any access.

    The error message tells the customer exactly what's wrong (Lakeflow
    SDP runtime is missing) and includes the original exception message
    so they can self-diagnose runtime mismatches.

    Dunder-introspection contract
    -----------------------------
    Tools that walk ``sys.modules`` (IPython's autoreload reliability
    hook, ``inspect.getmodule``, importlib metadata scanners, etc.)
    routinely do ``getattr(mod, "__file__", "")`` and similar dunder
    probes on every loaded module. If our ``__getattr__`` raises
    ``ImportError`` for those probes, the introspecting tool's own
    error-formatting path may also probe a dunder, and the same
    ``ImportError`` re-fires inside the traceback formatter -- which
    is exactly what produced the multi-page "Unexpected exception
    formatting exception. Falling back to standard exception" cascade
    after every cell of ``validate_phase2.py`` on serverless DLT.

    Two-part fix:
    1. Set ``mod.__file__`` to a self-describing sentinel string so
       the most common probe (``getattr(mod, "__file__", default)``)
       returns the sentinel directly without ever calling
       ``__getattr__``.
    2. Make ``__getattr__`` raise ``AttributeError`` (PEP 562
       compliant) for any dunder name. Introspection tools handle
       ``AttributeError`` correctly and fall back to their defaults.
       Real attribute access like ``src.dataflow_pipeline.DataflowPipeline``
       still gets the actionable ``ImportError`` it needs.
    """
    mod = types.ModuleType(qualified_name)
    mod.__doc__ = (
        f"Stub for {qualified_name}: the Lakeflow SDP runtime "
        f"(pyspark.pipelines) is not available in this Python environment."
    )
    # Self-describing sentinel: shows up if anything logs the path.
    mod.__file__ = f"<sdp-meta compat stub for {qualified_name}>"

    _message = _stub_error_message(
        qualified_name, original_error, runtime_missing
    )

    def _raise() -> None:
        raise ImportError(_message)

    def __getattr__(name: str):  # noqa: N807 (module-level dunder)
        # PEP 562: raising ``AttributeError`` (not ``ImportError``)
        # for dunders lets ``getattr(mod, dunder, default)`` in
        # introspection paths return the default cleanly.
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        _raise()

    mod.__getattr__ = __getattr__  # type: ignore[attr-defined]
    return mod


# ---------------------------------------------------------------------------
# Lazy alias module: emits DeprecationWarning per-attribute, with the
# warning's stack pointing at the customer's code line (stacklevel=2),
# not at this module's registration walk.
# ---------------------------------------------------------------------------
_WARNED_ALIASES: set[str] = set()


def _warn_src_alias_once(alias: str) -> None:
    if alias in _WARNED_ALIASES:
        return
    _WARNED_ALIASES.add(alias)
    submodule = alias.split(".", 1)[1] if "." in alias else ""
    canonical = (
        f"databricks.labs.sdp_meta.{submodule}"
        if submodule
        else "databricks.labs.sdp_meta"
    )
    warnings.warn(
        f"'{alias}' is a v0.0.10 compatibility alias and will be removed "
        f"in v0.2.0. Migrate to 'from {canonical} import …' or "
        f"'from dlt_meta import …'. See "
        f"docs/content/getting_started/sdp_meta_renaming.md.",
        DeprecationWarning,
        stacklevel=4,
    )


class _LazyAliasModule(types.ModuleType):
    """Module proxy: forwards attribute access to *target*, warns once per alias.

    Why a proxy instead of plain ``sys.modules[alias] = target``? Two
    reasons:

    - **Correct ``stacklevel``** — emitting the deprecation warning lazily
      from ``__getattr__`` lets us set ``stacklevel=4`` so the warning
      surfaces at the customer's notebook line, not somewhere inside this
      package's eager-registration walk.
    - **No-op when not used** — a v0.0.10 customer who happened to upgrade
      and rewrite their imports doesn't get spammed by warnings just
      because the ``.pth`` ran ``import dlt_meta`` at interpreter startup.
    """

    def __init__(self, alias: str, target: types.ModuleType) -> None:
        super().__init__(alias)
        self._alias = alias
        self._target = target
        # Mirror the target's package metadata so isinstance / __path__ /
        # __file__ checks behave like the real module.
        self.__doc__ = getattr(target, "__doc__", None)
        self.__file__ = getattr(target, "__file__", None)
        self.__loader__ = getattr(target, "__loader__", None)
        self.__spec__ = getattr(target, "__spec__", None)
        if hasattr(target, "__path__"):
            self.__path__ = target.__path__  # type: ignore[attr-defined]

    def __getattr__(self, name: str):  # only called when not found on self
        _warn_src_alias_once(self._alias)
        return getattr(self._target, name)

    def __dir__(self):
        return dir(self._target)


# ---------------------------------------------------------------------------
# 1. Flat re-exports under dlt_meta.<name>
#
# Routed through the same _make_stub_module mechanism so that on a
# non-SDP runtime, ``from dlt_meta import DataflowPipeline`` raises a
# clear ``Lakeflow SDP runtime`` error instead of the silent-swallow
# behaviour the previous shim had (``except ImportError: pass`` left
# every symbol unbound and produced ``ImportError: cannot import name
# 'DataflowPipeline' from 'dlt_meta'``, which masks the real cause).
# ---------------------------------------------------------------------------
# Tuple of (target_qualname, [(source_attr, alias_or_none)...]).
# ``alias_or_none`` is the v0.0.10 name (``DLTMeta``); when ``None``,
# the source attr is bound under its own name. Driven by a data
# structure rather than a literal block of imports so the corresponding
# stub-binding loop in ``_flat_reexport_or_stub`` can re-use the same
# name set without duplication.
_FLAT_REEXPORTS: tuple[tuple[str, tuple[tuple[str, str | None], ...]], ...] = (
    ("databricks.labs.sdp_meta.cli", (
        ("SDPMeta", "DLTMeta"),
        ("OnboardCommand", None),
        ("DeployCommand", None),
        ("SDP_META_RUNNER_NOTEBOOK", "DLT_META_RUNNER_NOTEBOOK"),
        ("onboard", None),
        ("deploy", None),
        ("main", None),
    )),
    ("databricks.labs.sdp_meta.dataflow_pipeline", (
        ("DataflowPipeline", None),
    )),
    ("databricks.labs.sdp_meta.dataflow_spec", (
        ("BronzeDataflowSpec", None),
        ("SilverDataflowSpec", None),
        ("DataflowSpecUtils", None),
        ("DLTSink", None),
    )),
    ("databricks.labs.sdp_meta.onboard_dataflowspec", (
        ("OnboardDataflowspec", None),
    )),
    ("databricks.labs.sdp_meta.pipeline_readers", (
        ("PipelineReaders", None),
    )),
    ("databricks.labs.sdp_meta.pipeline_writers", (
        ("AppendFlowWriter", None),
        ("DLTSinkWriter", None),
    )),
    ("databricks.labs.sdp_meta.install", (
        ("WorkspaceInstaller", None),
    )),
    ("databricks.labs.sdp_meta.config", (
        ("WorkspaceConfig", None),
    )),
    ("databricks.labs.sdp_meta.metastore_ops", (
        ("DeltaPipelinesMetaStoreOps", None),
        ("DeltaPipelinesInternalTableOps", None),
    )),
)


def _flat_reexport_or_stub() -> None:
    """Bind v0.0.10 names on this package, with stub fallback."""
    module_globals = sys.modules[__name__].__dict__
    try:
        for target_qualname, names in _FLAT_REEXPORTS:
            target_module = importlib.import_module(target_qualname)
            for source_name, alias in names:
                bound_name = alias or source_name
                module_globals[bound_name] = getattr(target_module, source_name)
    except Exception as exc:  # noqa: BLE001 (broad-except is intentional)
        # Catching ``Exception`` rather than just ``ImportError`` is
        # deliberate. Some Databricks runtimes raise non-Import
        # exception types out of ``pyspark.pipelines/__init__.py`` --
        # most notably a ``TypeError`` from inside PySpark's own
        # error formatter when raising ``PIPELINES_NOT_SUPPORTED`` on
        # a runtime that lacks ``dlt`` (see
        # :func:`_optional_runtime_import_error` for the full
        # traceback shape).
        #
        # NOTHING is re-raised here. This module is imported by the
        # ``dlt_meta.pth`` startup hook, and ``site.py`` prints any
        # exception a ``.pth`` line raises on EVERY ``python3`` launch
        # ("Error processing line 1 of dlt_meta.pth ... Remainder of
        # file ignored"). Before this guard, a laptop without pyspark
        # (``ModuleNotFoundError: name='pyspark'`` — a shape the
        # predicate previously missed) got that traceback at every
        # interpreter start, and an explicit ``import dlt_meta``
        # failed outright. Instead, ALWAYS degrade to a module-level
        # ``__getattr__`` stub; the predicate only chooses the message
        # (SDP-runtime-missing vs. generic dependency failure), so
        # real problems still fail loudly — at first use.
        #
        # Dunders raise ``AttributeError`` instead of ``ImportError``
        # for the same reason ``_make_stub_module`` does: tools like
        # IPython's autoreload reliability hook walk ``sys.modules``
        # probing ``__file__``, ``__path__``, etc., and a raised
        # ``ImportError`` poisons their traceback formatter (which
        # itself probes dunders). See ``_make_stub_module`` for the
        # full rationale.
        _runtime_missing = _optional_runtime_import_error(exc)
        _err_msg = str(exc)

        def __getattr__(name: str):  # noqa: N807
            if name.startswith("__") and name.endswith("__"):
                raise AttributeError(name)
            raise ImportError(
                _stub_error_message(
                    f"dlt_meta.{name}", _err_msg, _runtime_missing
                )
            )

        sys.modules[__name__].__getattr__ = __getattr__  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# 2. src.* aliasing
# ---------------------------------------------------------------------------
def _register_src_aliases() -> None:
    """Register ``src`` and ``src.<sub>`` in :data:`sys.modules`.

    Idempotent and opt-out-aware. Callable from a ``.pth`` file at
    interpreter startup or directly via ``import dlt_meta``.
    """
    if _DISABLED:
        return

    # Top-level ``src`` package alias. Use ``setdefault`` so a customer
    # who happens to have their own ``src`` package on ``sys.path``
    # doesn't get clobbered (their import will have already populated
    # ``sys.modules['src']`` before this runs, and our ``setdefault``
    # is a no-op).
    parent = sys.modules.setdefault("src", sys.modules[__name__])
    if not _is_alias_parent(parent):
        # A customer-owned ``src`` package won: registering ``src.<sub>``
        # aliases now would shadow THEIR submodules in ``sys.modules``,
        # and binding our stubs/proxies onto their package (or onto
        # ``dlt_meta``, which ``import src.x`` never consults when the
        # parent isn't us) would be wrong either way. Skip aliasing
        # entirely — same effect as SDP_META_DISABLE_SRC_ALIAS=1 for
        # the ``src.*`` surface.
        return

    for sub in _SRC_SUBMODULES:
        alias = f"src.{sub}"
        if alias in sys.modules:
            continue

        target_qualname = f"databricks.labs.sdp_meta.{sub}"
        try:
            target = importlib.import_module(target_qualname)
        except Exception as exc:  # noqa: BLE001 (broad-except is intentional)
            # See ``_flat_reexport_or_stub`` for why we catch the
            # broad ``Exception`` rather than just ``ImportError``
            # (serverless DLT raises a TypeError out of
            # ``pyspark.pipelines/__init__.py`` on certain runtimes)
            # and why NOTHING is re-raised: this runs from the
            # ``.pth`` at interpreter startup, where a raise becomes
            # site.py noise on every ``python3`` launch. The predicate
            # only picks the stub's message.
            stub = _make_stub_module(
                alias, str(exc),
                runtime_missing=_optional_runtime_import_error(exc),
            )
            sys.modules[alias] = stub
            _bind_on_parent(sub, stub)
            continue

        # Wrap in a lazy proxy so DeprecationWarning fires at the
        # customer's import line (stacklevel resolved per attribute
        # access), not on the eager registration walk.
        proxy = _LazyAliasModule(alias, target)
        sys.modules[alias] = proxy
        _bind_on_parent(sub, proxy)


def _is_alias_parent(parent: types.ModuleType) -> bool:
    """Is *parent* a ``src`` object the shim is allowed to alias into?

    Two legitimate parents exist:

    - this package itself (``sys.modules.setdefault`` installed us as
      ``src`` — the ``.pth`` / plain ``import dlt_meta`` path), or
    - the **bundled** ``compat/src`` package shipped in the primary
      wheel, which is mid-import when it runs ``import dlt_meta`` for
      the registration side effects (the serverless ``%pip install``
      path, where ``.pth`` scanning never re-fires). It identifies
      itself with ``__sdp_meta_compat__ = True``, set before the
      import so it is visible here.

    Anything else is a customer-owned ``src`` package and must be left
    completely alone. ``parent.__dict__`` is probed directly (not
    ``getattr``) so a customer package with a module-level
    ``__getattr__`` is never tickled.
    """
    return parent is sys.modules[__name__] or bool(
        parent.__dict__.get("__sdp_meta_compat__", False)
    )


def _bind_on_parent(sub: str, module: types.ModuleType) -> None:
    """Mirror what ``import a.b`` does: set ``b`` on the ``src`` parent.

    Only ever called when :func:`_register_src_aliases` established via
    :func:`_is_alias_parent` that ``sys.modules['src']`` is a parent we
    own (this package, or the bundled compat ``src`` package — a
    customer-owned ``src`` short-circuits the registration walk before
    any binding). Without this, ``import src.dataflow_pipeline``
    succeeds (sys.modules hit) but the subsequent
    ``src.dataflow_pipeline`` attribute access falls through to the
    parent's ``__getattr__`` — which, when the flat re-exports failed,
    raises the flat-stub message instead of resolving to the registered
    submodule stub/proxy. Dunder submodules (``__about__``,
    ``__main__``) are skipped so they don't shadow real package
    dunders.
    """
    if sub.startswith("__"):
        return
    parent = sys.modules.get("src")
    if parent is None or not _is_alias_parent(parent):  # defensive
        return
    if sub not in parent.__dict__:
        parent.__dict__[sub] = module


# ---------------------------------------------------------------------------
# Package-level deprecation warning — suppressed under opt-out
# ---------------------------------------------------------------------------
if not _DISABLED:
    warnings.warn(
        "The 'dlt_meta' package is deprecated and will be removed in a "
        "future version. Please migrate to 'databricks.labs.sdp_meta' "
        "(pip install databricks-labs-sdp-meta). See "
        "https://databrickslabs.github.io/sdp-meta/ for migration guide.",
        DeprecationWarning,
        stacklevel=2,
    )

# Wire up the surfaces. Order matters: do flat re-exports first so the
# stub-module __getattr__ on this package (set when the runtime is
# missing) takes effect BEFORE any src.* alias resolves to a stub.
_flat_reexport_or_stub()
_register_src_aliases()


def _deprecated_wrapper(func, old_name, new_name):
    """Wrapper that adds a deprecation warning to *func* invocations."""
    def wrapper(*args, **kwargs):
        warnings.warn(
            f"'{old_name}' is deprecated, use '{new_name}' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return func(*args, **kwargs)
    return wrapper
