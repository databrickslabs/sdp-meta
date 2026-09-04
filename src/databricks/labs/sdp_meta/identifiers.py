"""Unity Catalog / SQL identifier and onboarding-enum validation.

`sdp-meta` accepts UC catalog / schema / table / volume names from many
input boundaries (CLI prompts, ``OnboardCommand`` / ``DeployCommand``
constructors, the DAB template, demo scripts, integration test runners).
All of those names eventually get spliced into SQL identifiers downstream
without backtick-quoting, so we reject anything that isn't a *regular*
SQL identifier as defined in
https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-identifiers --
i.e. ``[A-Za-z_][A-Za-z0-9_]*``.

This is the entire contract: strict validation at every input boundary,
and the rest of the codebase (``DataflowPipeline``, the rendered DAB
pipeline, the onboarding job) can splice these names into SQL strings
without further escaping (issue #261). Hyphenated catalog names that UC
itself permits are rejected here with a clear, actionable error message
instead of failing later as a cryptic Spark name-resolution error.

This module also pins the bounded enums onboarding accepts —
:data:`SUPPORTED_SOURCE_FORMATS` and :data:`SUPPORTED_SCD_TYPES` — so
typos like ``"cloudfiles"`` or ``scd_type="3"`` fail at the onboarding
boundary with a helpful "use one of …" message, instead of silently
flowing into the DLT pipeline and failing there.
"""
from __future__ import annotations

import ast
import json
import re
import sys
from typing import Optional

_REGULAR_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_MAX_IDENT_LEN = 255

# Source formats supported by the bronze readers in
# ``dataflow_pipeline.py`` and ``pipeline_readers.py`` (the
# ``if/elif source_format == ...`` chains). Pinned here so the bundle CLI,
# DAB template, and onboarding pre-flight all agree on the same set; if
# you add a reader, add the format here too.
SUPPORTED_SOURCE_FORMATS = frozenset(
    {"cloudFiles", "delta", "kafka", "eventhub", "snapshot"}
)

# SCD types DLT's apply_changes / apply_changes_from_snapshot accept.
# DLT requires the value as a string ("1" or "2"); see
# ``dlt.apply_changes(stored_as_scd_type=...)`` and the existing
# ``cdc_apply_changes.scd_type == "2"`` comparison in
# ``dataflow_pipeline.py``.
SUPPORTED_SCD_TYPES = frozenset({"1", "2"})


def is_regular_identifier(name) -> bool:
    """Return True iff ``name`` is a regular SQL identifier
    (``[A-Za-z_][A-Za-z0-9_]*``) and so safe to splice into SQL unquoted."""
    return isinstance(name, str) and bool(_REGULAR_IDENT_RE.match(name))


def validate_uc_identifier(name, *, kind: str = "identifier") -> str:
    """Validate ``name`` as a UC identifier; return it unchanged on success.

    Raises ``ValueError`` with an actionable message when ``name`` cannot be
    safely used as a UC identifier inside the sdp-meta toolchain. The rule
    is strict: regular SQL identifiers only (letters, digits, underscores;
    must start with a letter or underscore; max 255 chars).

    Hyphens, periods, spaces, leading digits and backticks are all
    rejected. UC itself permits hyphens in catalog / schema names, but the
    sdp-meta deployed pipeline reads ``bronze.dataflowspecTable`` /
    ``silver.dataflowspecTable`` directly via ``spark.read.table(...)``
    without backtick-quoting, so we reject hyphens at the input boundary
    rather than letting users hit a confusing Spark error at runtime
    (issue #261).
    """
    if not isinstance(name, str):
        raise ValueError(
            f"{kind} must be a non-empty string, got "
            f"{type(name).__name__}: {name!r}"
        )
    if not name:
        raise ValueError(f"{kind} must be a non-empty string")
    if len(name) > _MAX_IDENT_LEN:
        raise ValueError(
            f"{kind} {name!r} is {len(name)} characters; maximum allowed is "
            f"{_MAX_IDENT_LEN}"
        )
    if not _REGULAR_IDENT_RE.match(name):
        raise ValueError(
            f"{kind} {name!r} is not a valid Databricks SQL regular identifier. "
            f"Names must match {_REGULAR_IDENT_RE.pattern} (letters, digits "
            f"and underscores only; must start with a letter or underscore). "
            f"Hyphens, periods, spaces and leading digits are not supported. "
            f"See "
            f"https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-identifiers"
        )
    return name


def validate_uc_full_name(name, *, kind: str = "identifier", max_parts: int = 3) -> str:
    """Validate a dotted multi-part UC name, e.g. ``catalog.schema.table``.

    Each segment must independently pass :func:`validate_uc_identifier`,
    so the full name is safe to splice into SQL unquoted. Accepts 1, 2, or
    3 parts (``table`` / ``schema.table`` / ``catalog.schema.table``);
    callers can override ``max_parts`` if they want to be strict about a
    specific arity.

    Returns ``name`` unchanged on success. Raises ``ValueError`` with an
    actionable message on failure.
    """
    if not isinstance(name, str) or not name:
        raise ValueError(f"{kind} must be a non-empty string, got {name!r}")
    parts = name.split(".")
    if len(parts) < 1 or len(parts) > max_parts:
        raise ValueError(
            f"{kind} {name!r} has {len(parts)} dotted segments; expected "
            f"between 1 and {max_parts}."
        )
    for i, part in enumerate(parts):
        # Each segment gets a positional kind so the error message points
        # at the offending one (e.g. "schema in database 'main.bad-name'").
        validate_uc_identifier(part, kind=f"segment {i + 1} of {kind}")
    return name


def validate_uc_column_list(
    value, *, kind: str = "column list", allow_empty_entries: bool = False
) -> list:
    """Validate a column-name list/string and return the parsed column names.

    Onboarding accepts column-name fields in several shapes that the
    existing parser code in ``onboard_dataflowspec.py`` handles:

    * a single column name string (``"col1"``)
    * a comma-separated string (``"col1,col2"``) — used for
      ``*_partition_columns`` / ``*_quarantine_table_partitions``
    * a Python list (``["col1", "col2"]``) — used for ``*_cluster_by``
    * a string representation of a list (``"[col1, col2]"``) — also
      ``*_cluster_by``, parsed via ``ast.literal_eval``

    Each resulting column name must be a regular SQL identifier so it can
    be safely spliced into ``PARTITIONED BY`` / ``CLUSTER BY`` DDL or
    passed to DLT's ``partition_cols`` / ``cluster_by`` kwargs without
    surprising failures (issue #261).

    ``None`` and empty strings are treated as "no columns" and return an
    empty list rather than raising — matches the onboarding code's
    "if present and truthy" pattern so optional fields don't trip the
    pre-flight check.

    Returns the validated list of column names. Raises ``ValueError`` if
    any element fails :func:`validate_uc_identifier`.
    """
    if value is None or value == "":
        return []

    columns: list
    if isinstance(value, list):
        columns = list(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            # Stringified list, e.g. "[col1, col2]". Use literal_eval so we
            # accept the exact same shape the existing __parse_cluster_by_string
            # parser does — no surprises if it parsed there but failed here.
            try:
                parsed = ast.literal_eval(stripped)
            except (SyntaxError, ValueError) as exc:
                raise ValueError(
                    f"{kind} {value!r} looks like a list literal but could "
                    f"not be parsed: {exc}"
                ) from exc
            if not isinstance(parsed, list):
                raise ValueError(
                    f"{kind} {value!r} parsed to {type(parsed).__name__}, "
                    f"expected list"
                )
            columns = parsed
        else:
            columns = [c.strip() for c in stripped.split(",")]
    else:
        raise ValueError(
            f"{kind} must be a string or list of column names, got "
            f"{type(value).__name__}: {value!r}"
        )

    out = []
    for i, col in enumerate(columns):
        # v0.0.10 onboarding files used ``[""]`` (a list holding one
        # empty string) as the "no columns" idiom on the partition /
        # cluster-by fields, and the runtime ``get_partition_cols`` in
        # dataflow_spec.py still special-cases it (``[""] -> None``, and
        # ``filter(None, ...)`` for longer lists). Callers validating
        # THOSE fields pass ``allow_empty_entries=True`` so pre-flight
        # doesn't reject what the runtime deliberately tolerates. CDC
        # column lists (``keys``, ``column_list``, ...) are persisted
        # verbatim and go straight to DLT, where a blank entry fails at
        # runtime — so blanks stay errors there (the default).
        if isinstance(col, str) and not col.strip():
            if allow_empty_entries:
                continue
            raise ValueError(
                f"{kind} entry {i} is not a non-empty string: {col!r}"
            )
        if not isinstance(col, str):
            raise ValueError(
                f"{kind} entry {i} is not a non-empty string: {col!r}"
            )
        validate_uc_identifier(col, kind=f"{kind} entry {i}")
        out.append(col)
    return out


# lower-case → canonical spelling, e.g. "cloudfiles" → "cloudFiles".
# v0.0.10 onboarding checked ``source_format.lower()`` so any case
# variant onboarded successfully; we keep accepting those (issue #370
# class of upgrade break) but coerce to the canonical spelling the
# ``read_bronze`` dispatch in dataflow_pipeline.py compares against.
_CANONICAL_SOURCE_FORMATS = {s.lower(): s for s in SUPPORTED_SOURCE_FORMATS}


def validate_source_format(value, *, kind: str = "source_format") -> str:
    """Validate ``value`` is one of :data:`SUPPORTED_SOURCE_FORMATS`.

    The bronze readers branch on this string in
    ``dataflow_pipeline.py``; an unknown value silently falls through
    every ``elif`` and the pipeline starts with no input. Catching it
    at onboarding turns that "pipeline runs but does nothing" failure
    into a clear actionable error.

    Matching is case-INsensitive for backward compatibility — v0.0.10
    onboarding accepted ``"cloudfiles"`` / ``"CloudFiles"`` etc. via a
    ``.lower()`` check — but the *returned* value is always the
    canonical spelling (``"cloudFiles"``), because the runtime dispatch
    compares exactly. Callers that persist the format must use the
    return value so old case variants are healed rather than stored.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"{kind} must be a non-empty string, got {value!r}"
        )
    canonical = _CANONICAL_SOURCE_FORMATS.get(value.lower())
    if canonical is None:
        # Sort for deterministic, alphabetic error messages.
        allowed = ", ".join(sorted(SUPPORTED_SOURCE_FORMATS))
        raise ValueError(
            f"{kind}={value!r} is not supported. Use one of: {allowed}."
        )
    return canonical


def validate_scd_type(value, *, kind: str = "scd_type") -> str:
    """Validate ``value`` is one of :data:`SUPPORTED_SCD_TYPES` (``"1"``/``"2"``).

    DLT's ``apply_changes`` / ``apply_changes_from_snapshot`` accept the
    SCD type as a string. Catching a typo (``"3"``, ``"scd_2"``, etc.)
    here surfaces it during onboarding with the allowed values inlined,
    instead of bubbling up later as an opaque DLT runtime error.

    Integers ``1`` / ``2`` are accepted for backward compatibility —
    onboarding files written for v0.0.10 and earlier carried
    ``"scd_type": 1`` (issue #370) — and are coerced to the canonical
    string form. Callers must use the *returned* value so downstream
    dataclasses and the ``cdc_apply_changes.scd_type == "2"`` comparisons
    in dataflow_pipeline.py only ever see strings.
    """
    # bool is a subclass of int, and True/False were never valid SCD
    # types in any release, so reject it before the int coercion below.
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"{kind} must be a non-empty string, got {type(value).__name__}: {value!r}"
        )
    if value not in SUPPORTED_SCD_TYPES:
        allowed = ", ".join(sorted(SUPPORTED_SCD_TYPES))
        raise ValueError(
            f"{kind}={value!r} is not supported. Use one of: {allowed}."
        )
    return value


def validate_sequence_by(value, *, kind: str = "sequence_by") -> str:
    """Validate a CDC ``sequence_by`` value.

    ``sequence_by`` is NOT a plain identifier slot — the runtime
    (``cdc_apply_changes`` in dataflow_pipeline.py) supports a
    comma-separated list of columns (wrapped in ``struct(...)`` for
    multi-column ordering, documented in the CDC guide), and DLT itself
    accepts dotted column references such as ``_metadata.file_path``.
    v0.0.10 applied no validation at all here, so validating it with
    the strict regular-identifier rules would reject onboarding files
    that worked before (issue #370 class of upgrade break).

    Accepted shapes: ``"col"``, ``"col1,col2,col3"``, and dotted
    references like ``"_metadata.file_path"`` in any position. Each
    dot-segment of each comma-entry must be a regular identifier —
    that's exactly what the runtime's ``struct(...)`` split and DLT's
    column resolution can handle. Returns ``value`` unchanged.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"{kind} must be a non-empty string, got "
            f"{type(value).__name__}: {value!r}"
        )
    for entry in value.split(","):
        entry = entry.strip()
        if not entry:
            raise ValueError(
                f"{kind} {value!r} contains an empty entry; use a "
                f"comma-separated list of column names, e.g. "
                f"'event_ts,sequence_id'"
            )
        # No max_parts cap: nested struct fields can be arbitrarily deep.
        for i, part in enumerate(entry.split(".")):
            validate_uc_identifier(
                part, kind=f"segment {i + 1} of {kind} column {entry!r}"
            )
    return value


# SQL fragments (e.g. an optional WHERE clause on the App's Metadata
# Browse tab) cannot be parameterised by the Databricks Statement
# Execution API for structural positions, so anything user-supplied
# must be denylist-validated before being spliced into a query. We
# block the tokens that make second-statement / comment-out / DDL
# escapes possible. The set is intentionally narrow: legitimate
# WHERE clauses (column comparisons, AND / OR / IN / LIKE on a list
# of literal values) all pass; anything that smells like
# ``'; DROP TABLE x --`` or ``UNION SELECT … FROM system.…`` is
# rejected at the App boundary with an actionable 400.
#
# Why a denylist rather than a strict allowlist regex: even a
# minimal real-world WHERE clause exercises CASE / BETWEEN /
# subexpressions / quoted string literals containing arbitrary
# bytes, all of which are awkward to express as a single allow
# pattern without rejecting legitimate input. The denylist below
# covers every escape vector the Databricks SQL grammar exposes for
# stacking, commenting out the trailing LIMIT, or invoking DDL /
# DML — those are exactly the structural primitives the dialect
# requires for an injection to do damage. Everything else stays
# scoped to the SELECT we own.
_DANGEROUS_SQL_TOKENS = (
    ";",      # statement separator
    "--",     # line comment (would comment out the trailing LIMIT)
    "/*",     # block comment open
    "*/",     # block comment close
    "`",      # identifier delimiter — would let caller escape to a
              # different table reference
)

# Case-insensitive keyword denylist. A WHERE clause genuinely needs
# none of these — they only appear in injection payloads that try to
# escape the SELECT we built. Word-boundary matched (``\bUNION\b``)
# so column names like ``unionized_state`` aren't false-positives.
_DANGEROUS_SQL_KEYWORDS = (
    "UNION", "INTERSECT", "EXCEPT",      # set operations (data exfil)
    "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "COPY",
    "DROP", "CREATE", "ALTER", "RENAME", "REPLACE",
    "GRANT", "REVOKE",
    "EXEC", "EXECUTE", "CALL",
)

_DANGEROUS_KEYWORD_RE = re.compile(
    r"\b(" + "|".join(_DANGEROUS_SQL_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

_MAX_WHERE_CLAUSE_LEN = 2000


def validate_sql_where_clause(value, *, kind: str = "where_clause") -> str:
    """Reject SQL fragments that contain statement-separation,
    comment, or DDL/DML escape tokens.

    Returns ``value`` unchanged on success. Raises ``ValueError`` with
    an actionable message identifying the offending token so the user
    can adjust their input.

    Intended for the narrow case where a SELECT's WHERE clause is
    composed from user input (Metadata Browse table preview) and the
    Statement Execution API cannot bind it as a parameter. Callers
    that can use named parameters (``ws.statement_execution.execute_statement(..., parameters=[...])``)
    should prefer that path.
    """
    if value is None or value == "":
        return ""
    if not isinstance(value, str):
        raise ValueError(
            f"{kind} must be a string, got {type(value).__name__}: {value!r}"
        )
    if len(value) > _MAX_WHERE_CLAUSE_LEN:
        raise ValueError(
            f"{kind} is {len(value)} characters; maximum allowed is "
            f"{_MAX_WHERE_CLAUSE_LEN}"
        )
    for token in _DANGEROUS_SQL_TOKENS:
        if token in value:
            raise ValueError(
                f"{kind} contains disallowed token {token!r}. Statement "
                f"separators, comments, and identifier delimiters are not "
                f"permitted in user-supplied WHERE clauses."
            )
    match = _DANGEROUS_KEYWORD_RE.search(value)
    if match:
        raise ValueError(
            f"{kind} contains disallowed keyword "
            f"{match.group(1).upper()!r}. Only simple filter expressions "
            f"(column comparisons joined by AND / OR) are permitted; set "
            f"operations and DDL / DML are not."
        )
    return value


_MAX_COLUMN_COMMENT_LEN = 1000

# Matches the leading UC function name of a mask clause and, optionally, a
# trailing ``USING COLUMNS (col, ...)`` list (case-insensitive). The function
# name is everything up to the first whitespace or ``(``; the USING COLUMNS
# group is captured separately so the column list can be validated.
_MASK_CLAUSE_RE = re.compile(
    r"^\s*(?P<func>[^\s(]+)\s*(?:USING\s+COLUMNS\s*\((?P<cols>[^)]*)\))?\s*$",
    re.IGNORECASE,
)


def validate_column_comment(value, *, kind: str = "column_comment") -> str:
    """Validate a column comment string; return it unchanged on success.

    Column comments are emitted as SQL string literals (``COMMENT '...'``)
    with embedded backslashes and single quotes escaped by
    :meth:`DataflowSpecUtils.build_schema_ddl` (backslashes first, so ``\\'``
    cannot terminate the literal), so the comment is *data*, not
    code — the dangerous-keyword denylist that guards WHERE clauses does not
    apply (a comment may legitimately contain words like "select"). We only
    enforce type, a length bound, and the absence of control characters /
    newlines that would corrupt the rendered DDL.
    """
    if value is None or value == "":
        return ""
    if not isinstance(value, str):
        raise ValueError(
            f"{kind} must be a string, got {type(value).__name__}: {value!r}"
        )
    if len(value) > _MAX_COLUMN_COMMENT_LEN:
        raise ValueError(
            f"{kind} is {len(value)} characters; maximum allowed is "
            f"{_MAX_COLUMN_COMMENT_LEN}"
        )
    if any(ord(ch) < 0x20 for ch in value):
        raise ValueError(
            f"{kind} contains control characters (e.g. newlines/tabs), which "
            f"are not permitted in a column comment."
        )
    return value


def validate_column_mask_clause(value, *, kind: str = "column_mask") -> str:
    """Validate a column mask clause; return it unchanged on success.

    A mask clause is spliced verbatim after ``MASK`` into a DDL-string schema
    (an unparameterisable position), so it is validated strictly:

    1. the leading UC function name must pass :func:`validate_uc_full_name`
       (1–3 dotted regular identifiers);
    2. any ``USING COLUMNS (...)`` list must pass
       :func:`validate_uc_column_list`;
    3. the whole clause is run through the same dangerous-token / keyword
       denylist as :func:`validate_sql_where_clause` to block ``;``, comment
       markers, identifier delimiters and DDL/DML.

    Canonical form: ``cat.schema.mask_fn USING COLUMNS (region, tier)`` or a
    bare ``cat.schema.mask_fn``.
    """
    if value is None or value == "":
        return ""
    if not isinstance(value, str):
        raise ValueError(
            f"{kind} must be a string, got {type(value).__name__}: {value!r}"
        )
    # Reuse the WHERE-clause guard for statement-separation / comment / DDL
    # escape tokens and keywords (also enforces the length bound).
    validate_sql_where_clause(value, kind=kind)
    match = _MASK_CLAUSE_RE.match(value)
    if not match:
        raise ValueError(
            f"{kind} {value!r} is not a valid mask clause. Expected "
            f"'<catalog.schema.function> [USING COLUMNS (col, ...)]'."
        )
    validate_uc_full_name(match.group("func"), kind=f"{kind} function name")
    cols = match.group("cols")
    if cols is not None:
        validate_uc_column_list(cols, kind=f"{kind} USING COLUMNS list")
    return value


def _validate_column_policy_dict(
    value, per_value_validator, *, kind, drop_empty_values=False
):
    """Shared validator for the JSON-dict column-policy onboarding fields.

    Accepts a ``dict`` (already parsed from the onboarding row) or a JSON
    string, validates each key as a UC identifier and each value with
    ``per_value_validator``, and returns the parsed dict. ``None`` / empty
    returns ``{}`` so optional fields don't trip pre-flight.

    When ``drop_empty_values`` is set, entries whose value is empty (``""``
    or whitespace) are dropped after validation. Masks use this: an empty
    mask value would render an invalid bare ``MASK`` clause, so it is not a
    policy at all and must not survive to the renderer.
    """
    if value is None or value == "":
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{kind} must be a JSON object, could not parse: {exc}"
            ) from exc
    if not isinstance(value, dict):
        raise ValueError(
            f"{kind} must be a JSON object keyed by column name, got "
            f"{type(value).__name__}: {value!r}"
        )
    # ``spark.read.json`` unifies nested-object schemas across all onboarding
    # rows, so a row can surface phantom keys (contributed by other rows)
    # whose value is ``None``. Drop those — they are not policies for THIS
    # row — and validate only the entries the operator actually set.
    value = {column: entry for column, entry in value.items() if entry is not None}
    validated = {}
    for column, entry in value.items():
        validate_uc_identifier(column, kind=f"{kind} column name")
        per_value_validator(entry, kind=f"{kind} for column {column!r}")
        if drop_empty_values and isinstance(entry, str) and entry.strip() == "":
            continue
        validated[column] = entry
    return validated


def validate_column_comments(value, *, kind: str = "column_comments") -> dict:
    """Validate the ``*_column_comments`` onboarding field (a dict / JSON
    object of ``{column: comment}``). Returns the parsed dict."""
    return _validate_column_policy_dict(
        value, validate_column_comment, kind=kind
    )


def validate_column_masks(value, *, kind: str = "column_masks") -> dict:
    """Validate the ``*_column_masks`` onboarding field (a dict / JSON object
    of ``{column: mask_clause}``). Returns the parsed dict."""
    return _validate_column_policy_dict(
        value, validate_column_mask_clause, kind=kind, drop_empty_values=True
    )


def _format_prompt_error(message: str) -> str:
    """Format an error message for an interactive UC-identifier prompt.

    Uses ANSI red ONLY when stderr is a real TTY; piped/captured output
    (CI logs, file redirects) gets plain text so the escape codes don't
    end up rendered as garbage in build logs.
    """
    if sys.stderr.isatty():
        return f"\033[31m[ERROR] {message}\033[0m"
    return f"[ERROR] {message}"


def prompt_uc_identifier(
    wsi,
    text: str,
    *,
    kind: str,
    default: Optional[str] = None,
    max_attempts: int = 10,
) -> str:
    """Interactively prompt for a UC identifier with validation + retry.

    Wraps ``wsi._question`` (a
    ``databricks.labs.blueprint.installer.WorkspaceInstaller`` method):
    on each attempt, the user-supplied value is run through
    :func:`validate_uc_identifier`. Validation failures print a clear
    error message and re-prompt instead of crashing — matches the
    behavior every CLI / DAB prompt expects (issue #261).

    After ``max_attempts`` consecutive bad answers we raise so
    non-interactive runs (e.g. piped stdin returning the same junk every
    time) still terminate. The error stream is gated on
    ``sys.stderr.isatty()`` so CI logs don't get ANSI escape garbage.
    """
    for _ in range(max_attempts):
        value = wsi._question(text, default=default)
        try:
            return validate_uc_identifier(value, kind=kind)
        except ValueError as exc:
            print(_format_prompt_error(str(exc)) + "\n", file=sys.stderr)
    raise ValueError(
        f"Could not get a valid {kind} after {max_attempts} attempts"
    )
