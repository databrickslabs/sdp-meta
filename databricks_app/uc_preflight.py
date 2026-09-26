"""Unity Catalog pre-flight checks for the SDP-META Databricks App.

The App runs as a Databricks Apps service principal (its identity inside
the workspace is whatever `WorkspaceClient().current_user.me()` returns
when the platform's OAuth env vars are present). The demo launchers all
end up issuing ``CREATE SCHEMA`` against a user-supplied UC catalog. The
App SP is a fresh principal with zero default privileges, so unless the
catalog owner has granted it ``USE CATALOG`` + ``CREATE SCHEMA`` first,
every demo fails the same way:

    PermissionDenied: User does not have CREATE SCHEMA on Catalog '<cat>'

The SP can NEITHER grant itself those privileges (only the catalog owner
or a metastore admin can) NOR work around them. The right ergonomic is:

    1. Detect the gap up-front.
    2. Surface the EXACT GRANT SQL the catalog owner should run (in their
       own SQL editor session, not via the App).
    3. Block the demo until the gap is closed.

This module is the detection half of that flow; ``app.py`` wires it into
``/rundemo`` (pre-flight blocker) and ``/check-uc-grants`` (manual probe
for the "Test App access" button on the form).

Outside a Databricks Apps container the App SP doesn't exist, so callers
that exercise this module from a local CLI / unit test pass an explicit
``WorkspaceClient`` and identity. The default constructor relies on the
SDK's env-var credentials provider — same path the demos use.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# Privileges every demo launcher transitively needs on the user-supplied
# UC catalog:
#
#   USE CATALOG    — required to even resolve ``<catalog>.<schema>``.
#   CREATE SCHEMA  — every demo creates a fresh per-run schema for its
#                    bronze / silver / dataflowspec tables.
#
# We deliberately keep this list short. Schema-level privileges
# (CREATE TABLE, MODIFY) get inherited automatically by the schema
# creator (the App SP, since CREATE SCHEMA was just granted), so we
# don't need to require them up-front. If a future demo needs additional
# catalog-level privileges (e.g. USE SHARE for Delta Sharing), append
# them here.
REQUIRED_CATALOG_PRIVILEGES: frozenset[str] = frozenset(
    {"USE_CATALOG", "CREATE_SCHEMA"}
)


@dataclass
class PreflightResult:
    """Structured outcome of a UC catalog pre-flight check.

    JSON-serialisable via ``dataclasses.asdict`` for the Flask routes.
    """

    ok: bool
    """True iff the App SP has every privilege in REQUIRED_CATALOG_PRIVILEGES."""

    uc_name: str
    """The catalog the check was run against (echoed for the UI)."""

    sp_principal: str
    """The grant-target identifier (App SP application_id or user_name)."""

    sp_display_name: str
    """Human-friendly SP name (e.g. ``app-XXXXXX <app-name>``, where the
    suffix is the App resource name and the prefix is the platform-assigned
    handle).

    Shown in the UI alongside the UUID so the operator can sanity-check
    *which* App they're being asked to grant access to.
    """

    have: list[str] = field(default_factory=list)
    """Catalog-level privileges the App SP currently has."""

    missing: list[str] = field(default_factory=list)
    """Subset of REQUIRED_CATALOG_PRIVILEGES the App SP is missing."""

    grant_sql: Optional[str] = None
    """Copy-pasteable SQL the catalog owner should run. None when ok=True."""

    error: Optional[str] = None
    """Human-readable error when the probe itself failed (catalog missing,
    SP can't see the catalog at all, SDK error, etc.). When set, ``ok`` is
    always False and ``missing`` is the full required set."""

    details: Optional[str] = None
    """SDK exception, shown only when the user expands technical details."""


def _build_grant_sql(uc_name: str, sp_principal: str) -> str:
    """Render the SQL the catalog owner should paste into a SQL editor.

    Both the catalog name and the SP principal are wrapped in backticks
    so SQL identifier quoting is unambiguous: catalog names that happen
    to be reserved words still parse, and SP application IDs (which are
    UUIDs containing hyphens) are not interpretable as bare identifiers.

    The SP principal is whatever the SDK reports as the grant target
    (typically the OAuth ``application_id`` UUID for service principals).
    """
    return (
        f"-- Run as the catalog owner, not as the App SP, in a SQL editor:\n"
        f"GRANT USE CATALOG  ON CATALOG `{uc_name}` TO `{sp_principal}`;\n"
        f"GRANT CREATE SCHEMA ON CATALOG `{uc_name}` TO `{sp_principal}`;"
    )


def _resolve_app_sp_identity(ws) -> tuple[str, str]:
    """Return ``(grant_principal, display_name)`` for the App's caller.

    ``grant_principal`` is what we pass to :meth:`grants.get_effective`
    AND splice into the GRANT SQL — for an Apps SP this is the OAuth
    ``application_id`` UUID. ``display_name`` is the friendlier label
    (``app-XXXXXX <app-name>``) shown in the UI alongside the UUID.

    Falls back to ``user_name`` for either field if the SDK doesn't
    populate the typical SP attributes (regular user accounts running
    this code locally hit that branch).
    """
    me = ws.current_user.me()
    application_id = getattr(me, "application_id", None)
    user_name = getattr(me, "user_name", None) or ""
    display_name = (
        getattr(me, "display_name", None)
        or user_name
        or application_id
        or "<unknown>"
    )
    grant_principal = application_id or user_name or display_name
    return grant_principal, display_name


def check_app_sp_grants_on_catalog(
    uc_name: str,
    *,
    ws=None,
) -> PreflightResult:
    """Probe whether the App SP has the required privileges on ``uc_name``.

    Returns a :class:`PreflightResult` describing the current state. The
    result is always populated — exceptions inside the probe are caught
    and surfaced via ``PreflightResult.error``, so callers can render
    a single consistent error path in the UI.

    ``ws`` is injectable for testing; production callers leave it as
    None and let this module construct ``WorkspaceClient()`` from the
    Apps-injected env vars.
    """
    # Lazy SDK import so importing this module from a local context
    # without the SDK installed (rare, but happens during static
    # analysis / CI) doesn't fail at import time.
    from databricks.sdk import WorkspaceClient

    if not isinstance(uc_name, str) or not uc_name.strip():
        return PreflightResult(
            ok=False,
            uc_name=uc_name or "",
            sp_principal="",
            sp_display_name="",
            missing=sorted(REQUIRED_CATALOG_PRIVILEGES),
            error="uc_name is required",
        )

    if ws is None:
        ws = WorkspaceClient()

    try:
        sp_principal, sp_display_name = _resolve_app_sp_identity(ws)
    except Exception as exc:  # pragma: no cover — defensive
        return PreflightResult(
            ok=False,
            uc_name=uc_name,
            sp_principal="",
            sp_display_name="",
            missing=sorted(REQUIRED_CATALOG_PRIVILEGES),
            error="Could not identify the App service principal. Check the App's workspace credentials and try again.",
            details=f'{type(exc).__name__}: {exc}',
        )

    grant_sql = _build_grant_sql(uc_name, sp_principal)

    # NOTE: pass ``securable_type`` as the literal string ``"CATALOG"``
    # rather than ``SecurableType.CATALOG``. Recent databricks-sdk releases
    # (>=0.40) serialise the enum via ``str(enum)``, producing
    # ``"SECURABLETYPE.CATALOG"`` on the wire — which the UC ``GetEffective``
    # RPC rejects with ``Invalid input: ... is not a valid securable type``.
    # The REST API has always accepted the bare canonical string, so we use
    # that to be SDK-version-agnostic.
    try:
        eff = ws.grants.get_effective(
            securable_type="CATALOG",
            full_name=uc_name,
            principal=sp_principal,
        )
    except Exception as exc:
        # Common failure modes:
        #   - Catalog doesn't exist -> SDK raises NotFound.
        #   - App SP can't see the catalog at all (no BROWSE privilege
        #     anywhere) -> PermissionDenied.
        #   - Network blip / SDK auth misconfiguration.
        #
        # Preserve SDK diagnostics in Details while keeping the action
        # and GRANT SQL visible to the catalog owner.
        return PreflightResult(
            ok=False,
            uc_name=uc_name,
            sp_principal=sp_principal,
            sp_display_name=sp_display_name,
            missing=sorted(REQUIRED_CATALOG_PRIVILEGES),
            grant_sql=grant_sql,
            error=(
                f"Could not check access to catalog '{uc_name}'. Confirm it exists, "
                "then have the catalog owner run the GRANT SQL below and retry."
            ),
            details=f'{type(exc).__name__}: {exc}',
        )

    have: set[str] = set()
    for assignment in (eff.privilege_assignments or []):
        for priv in (assignment.privileges or []):
            # ``priv.privilege`` is a Privilege enum on modern SDKs;
            # ``.value`` is the canonical string name. Older SDK builds
            # exposed it as a plain string; the str() fallback covers both.
            value = getattr(priv.privilege, "value", None) or str(priv.privilege)
            have.add(value)

    # A catalog owner (or any principal granted ``ALL PRIVILEGES``) surfaces
    # as the single ``ALL_PRIVILEGES`` token rather than the expanded set, so
    # check for it explicitly — otherwise the SP would be told to grant itself
    # privileges it already has and every demo would be blocked.
    if "ALL_PRIVILEGES" in have:
        missing: list[str] = []
    else:
        missing = sorted(REQUIRED_CATALOG_PRIVILEGES - have)
    return PreflightResult(
        ok=not missing,
        uc_name=uc_name,
        sp_principal=sp_principal,
        sp_display_name=sp_display_name,
        have=sorted(have),
        missing=missing,
        grant_sql=grant_sql if missing else None,
    )
