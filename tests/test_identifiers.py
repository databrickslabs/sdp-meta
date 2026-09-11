"""Tests for `databricks.labs.sdp_meta.identifiers`.

The helper rejects anything that isn't a regular SQL identifier so the
rest of the codebase can splice these names into SQL strings without
having to think about quoting (issue #261). The tests pin both the
acceptance set (regular identifiers) and the rejection set (hyphens,
periods, spaces, leading digits, backticks, control chars, empty,
oversized, non-string).
"""
from __future__ import annotations

import unittest

from databricks.labs.sdp_meta.identifiers import (
    SUPPORTED_SCD_TYPES,
    SUPPORTED_SOURCE_FORMATS,
    is_regular_identifier,
    validate_column_comment,
    validate_column_comments,
    validate_column_mask_clause,
    validate_column_masks,
    parse_sequence_by_columns,
    validate_scd_type,
    validate_sequence_by,
    validate_source_format,
    validate_sql_where_clause,
    validate_uc_column_list,
    validate_uc_identifier,
)


class IsRegularIdentifierTests(unittest.TestCase):
    def test_simple_lowercase(self):
        self.assertTrue(is_regular_identifier("main"))

    def test_underscore_prefix(self):
        self.assertTrue(is_regular_identifier("_priv"))

    def test_alnum(self):
        self.assertTrue(is_regular_identifier("Schema123"))

    def test_hyphen_is_not_regular(self):
        self.assertFalse(is_regular_identifier("my-catalog"))

    def test_leading_digit_is_not_regular(self):
        self.assertFalse(is_regular_identifier("9lives"))

    def test_space_is_not_regular(self):
        self.assertFalse(is_regular_identifier("data lake"))

    def test_empty_is_not_regular(self):
        self.assertFalse(is_regular_identifier(""))

    def test_non_string_is_not_regular(self):
        self.assertFalse(is_regular_identifier(None))
        self.assertFalse(is_regular_identifier(123))


class ValidateUcIdentifierAcceptsTests(unittest.TestCase):
    def test_returns_input_on_valid(self):
        self.assertEqual(validate_uc_identifier("main"), "main")
        self.assertEqual(validate_uc_identifier("_x"), "_x")
        self.assertEqual(validate_uc_identifier("Schema123"), "Schema123")
        self.assertEqual(validate_uc_identifier("a_b_c"), "a_b_c")

    def test_max_length_accepted(self):
        validate_uc_identifier("a" * 255, kind="catalog")


class ValidateUcIdentifierRejectsTests(unittest.TestCase):
    def test_hyphen_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("my-catalog", kind="uc_catalog_name")

    def test_period_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("bad.name", kind="uc_catalog_name")

    def test_space_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("data lake", kind="uc_catalog_name")

    def test_leading_digit_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("9lives", kind="uc_catalog_name")

    def test_backtick_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("bad`name", kind="uc_catalog_name")

    def test_control_char_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("bad\x00name", kind="uc_catalog_name")

    def test_too_long_rejected(self):
        with self.assertRaisesRegex(ValueError, r"255"):
            validate_uc_identifier("a" * 256, kind="uc_catalog_name")

    def test_empty_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_identifier("", kind="uc_catalog_name")

    def test_none_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_identifier(None, kind="uc_catalog_name")

    def test_non_string_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_identifier(123, kind="uc_catalog_name")

    def test_kind_appears_in_message(self):
        with self.assertRaisesRegex(ValueError, r"my custom kind"):
            validate_uc_identifier("", kind="my custom kind")

    def test_error_links_to_docs(self):
        # Make the error message actionable by pointing to the upstream
        # SQL identifier rules so the user knows the *why*.
        with self.assertRaises(ValueError) as ctx:
            validate_uc_identifier("my-cat", kind="uc_catalog_name")
        self.assertIn("sql-ref-identifiers", str(ctx.exception))


class ValidateUcColumnListTests(unittest.TestCase):
    """Pinning the four input shapes the onboarding parser already accepts.

    The helper has to round-trip every shape ``__parse_cluster_by_string``
    and the ``*_partition_columns`` parser handle, otherwise we'd reject
    onboarding files that were valid before this validation existed.
    """

    def test_none_returns_empty(self):
        self.assertEqual(validate_uc_column_list(None), [])

    def test_empty_string_returns_empty(self):
        self.assertEqual(validate_uc_column_list(""), [])

    def test_single_string(self):
        self.assertEqual(validate_uc_column_list("col1"), ["col1"])

    def test_comma_separated_string(self):
        self.assertEqual(
            validate_uc_column_list("col1,col2,col3"),
            ["col1", "col2", "col3"],
        )

    def test_comma_separated_strips_whitespace(self):
        self.assertEqual(
            validate_uc_column_list(" col1 , col2 "),
            ["col1", "col2"],
        )

    def test_python_list(self):
        self.assertEqual(
            validate_uc_column_list(["col1", "col2"]),
            ["col1", "col2"],
        )

    def test_stringified_list(self):
        # __parse_cluster_by_string accepts this shape via ast.literal_eval,
        # so we have to as well.
        self.assertEqual(
            validate_uc_column_list("['col1', 'col2']"),
            ["col1", "col2"],
        )

    def test_hyphen_in_comma_string_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_column_list(
                "col1,bad-col", kind="bronze_partition_columns"
            )

    def test_hyphen_in_list_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_column_list(
                ["col1", "bad-col"], kind="bronze_cluster_by"
            )

    def test_leading_digit_in_stringified_list_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_column_list(
                "['1col', 'col2']", kind="bronze_cluster_by"
            )

    def test_empty_list_returns_empty(self):
        self.assertEqual(validate_uc_column_list([]), [])

    def test_list_with_empty_string_rejected_by_default(self):
        # CDC column lists (keys, column_list, ...) are persisted
        # verbatim into DLT calls, so a blank entry must stay an error
        # unless the caller explicitly opts into the legacy idiom.
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_column_list(["col1", ""], kind="bronze_cdc keys")

    def test_list_with_empty_string_entries_skipped_when_allowed(self):
        # v0.0.10 files used [""] as the "no columns" idiom on the
        # partition/cluster fields and the runtime get_partition_cols
        # still special-cases it (issue #370 class); those callers pass
        # allow_empty_entries=True so pre-flight matches the runtime.
        self.assertEqual(
            validate_uc_column_list(
                ["col1", ""],
                kind="bronze_cluster_by",
                allow_empty_entries=True,
            ),
            ["col1"],
        )

    def test_list_with_only_empty_string_allowed_returns_empty(self):
        self.assertEqual(
            validate_uc_column_list([""], allow_empty_entries=True), []
        )

    def test_list_with_only_empty_string_rejected_by_default(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_column_list([""])

    def test_list_with_whitespace_entry_skipped_when_allowed(self):
        self.assertEqual(
            validate_uc_column_list(["  ", "col1"], allow_empty_entries=True),
            ["col1"],
        )

    def test_list_with_non_string_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_column_list(["col1", 42], kind="bronze_cluster_by")

    def test_unparseable_list_literal_rejected(self):
        with self.assertRaisesRegex(ValueError, r"could not be parsed"):
            validate_uc_column_list("[col1, col2]", kind="bronze_cluster_by")

    def test_non_str_non_list_rejected(self):
        with self.assertRaisesRegex(
            ValueError, r"string or list of column names"
        ):
            validate_uc_column_list(42, kind="bronze_cluster_by")

    def test_kind_appears_in_error(self):
        with self.assertRaisesRegex(ValueError, r"silver_cluster_by"):
            validate_uc_column_list("bad-col", kind="silver_cluster_by")


class ValidateSourceFormatTests(unittest.TestCase):
    """Pin the bronze-reader format set so a typo (``parquet``) fails at
    onboarding instead of silently falling through every if/elif branch
    in ``DataflowPipeline`` and starting a pipeline with no input.

    Case variants are accepted for v0.0.10 backward compatibility
    (old onboarding checked ``.lower()``) and canonicalized, because
    the runtime dispatch compares exactly (issue #370 class)."""

    def test_supported_set_matches_expected(self):
        # Hard-coded so any drift between this module and dataflow_pipeline.py
        # / pipeline_readers.py shows up as a test failure rather than a
        # silently broken pipeline.
        self.assertEqual(
            SUPPORTED_SOURCE_FORMATS,
            frozenset({"cloudFiles", "delta", "kafka", "eventhub", "snapshot"}),
        )

    def test_each_supported_format_accepted(self):
        for fmt in SUPPORTED_SOURCE_FORMATS:
            self.assertEqual(validate_source_format(fmt), fmt)

    def test_lowercase_variant_canonicalized(self):
        # v0.0.10 onboarding accepted "cloudfiles" via .lower(); accept
        # it and return the canonical spelling the runtime dispatches on.
        self.assertEqual(validate_source_format("cloudfiles"), "cloudFiles")

    def test_mixed_case_variant_canonicalized(self):
        self.assertEqual(validate_source_format("CloudFiles"), "cloudFiles")
        self.assertEqual(validate_source_format("CLOUDFILES"), "cloudFiles")
        self.assertEqual(validate_source_format("Delta"), "delta")
        self.assertEqual(validate_source_format("KAFKA"), "kafka")

    def test_typo_rejected(self):
        with self.assertRaisesRegex(ValueError, r"is not supported"):
            validate_source_format("cloud_files")

    def test_empty_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_source_format("")

    def test_none_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_source_format(None)

    def test_error_lists_allowed_values(self):
        # Allowed values must appear in the error so the user can fix it
        # without going to docs.
        with self.assertRaises(ValueError) as ctx:
            validate_source_format("parquet")
        msg = str(ctx.exception)
        for fmt in SUPPORTED_SOURCE_FORMATS:
            self.assertIn(fmt, msg)

    def test_kind_appears_in_error(self):
        with self.assertRaisesRegex(ValueError, r"my_field"):
            validate_source_format("nope", kind="my_field")


class ValidateScdTypeTests(unittest.TestCase):
    """``stored_as_scd_type`` is a string in DLT's apply_changes API; reject
    ``"3"``, ``"scd_2"``, etc., to catch typos at onboarding. Integers
    ``1``/``2`` are accepted and coerced to strings for backward
    compatibility with v0.0.10 onboarding files (issue #370)."""

    def test_supported_set_matches_expected(self):
        self.assertEqual(SUPPORTED_SCD_TYPES, frozenset({"1", "2"}))

    def test_one_accepted(self):
        self.assertEqual(validate_scd_type("1"), "1")

    def test_two_accepted(self):
        self.assertEqual(validate_scd_type("2"), "2")

    def test_int_one_coerced_to_string(self):
        # v0.0.10 onboarding files carried ``"scd_type": 1`` (issue #370).
        self.assertEqual(validate_scd_type(1), "1")

    def test_int_two_coerced_to_string(self):
        self.assertEqual(validate_scd_type(2), "2")

    def test_int_three_rejected(self):
        with self.assertRaisesRegex(ValueError, r"is not supported"):
            validate_scd_type(3)

    def test_bool_rejected(self):
        # bool is an int subclass but was never a valid SCD type.
        with self.assertRaisesRegex(ValueError, r"non-empty string"):
            validate_scd_type(True)

    def test_three_rejected(self):
        with self.assertRaisesRegex(ValueError, r"is not supported"):
            validate_scd_type("3")

    def test_empty_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_scd_type("")

    def test_none_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_scd_type(None)

    def test_error_lists_allowed_values(self):
        with self.assertRaises(ValueError) as ctx:
            validate_scd_type("scd_2")
        msg = str(ctx.exception)
        self.assertIn("1", msg)
        self.assertIn("2", msg)

    def test_kind_appears_in_error(self):
        with self.assertRaisesRegex(ValueError, r"silver scd"):
            validate_scd_type("0", kind="silver scd")


class ValidateSequenceByTests(unittest.TestCase):
    """``sequence_by`` supports comma-separated multi-column ordering
    (wrapped in ``struct(...)`` by the runtime) and dotted column refs
    (``_metadata.file_path``), so it must NOT be validated with the
    strict regular-identifier rules (v0.0.10 compat, issue #370 class)."""

    def test_single_column_accepted(self):
        self.assertEqual(validate_sequence_by("event_ts"), "event_ts")

    def test_csv_multi_column_accepted(self):
        # Documented in docs/docs/guides/cdc.md.
        value = "dmsTimestamp,enqueueTimestamp,sequenceId"
        self.assertEqual(validate_sequence_by(value), value)

    def test_dotted_column_accepted(self):
        self.assertEqual(
            validate_sequence_by("_metadata.file_path"),
            "_metadata.file_path",
        )

    def test_csv_with_spaces_accepted(self):
        value = "event_ts, sequence_id"
        self.assertEqual(validate_sequence_by(value), value)

    def test_hyphenated_column_rejected(self):
        with self.assertRaisesRegex(ValueError, r"not a valid"):
            validate_sequence_by("bad-col")

    def test_trailing_comma_rejected(self):
        with self.assertRaisesRegex(ValueError, r"empty entry"):
            validate_sequence_by("event_ts,")

    def test_empty_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_sequence_by("")

    def test_none_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_sequence_by(None)

    def test_kind_appears_in_error(self):
        with self.assertRaisesRegex(ValueError, r"my_seq_field"):
            validate_sequence_by("bad-col", kind="my_seq_field")

    def test_expression_coalesce_rejected(self):
        # struct(*cols) treats each entry as a column NAME, so an expression
        # must be rejected with a clear message (bare-column-only contract).
        with self.assertRaisesRegex(ValueError, r"expression"):
            validate_sequence_by("coalesce(a,b)")

    def test_expression_cast_rejected(self):
        with self.assertRaisesRegex(ValueError, r"expression"):
            validate_sequence_by("cast(x as decimal(10,2))")

    def test_expression_embedded_space_rejected(self):
        with self.assertRaisesRegex(ValueError, r"expression"):
            validate_sequence_by("a b")


class ParseSequenceByColumnsTests(unittest.TestCase):
    """``parse_sequence_by_columns`` is the single source of truth turning a
    sequence_by spec into the bare column list both the apply-time struct and
    the SCD2 type derivation consume."""

    def test_single(self):
        self.assertEqual(parse_sequence_by_columns("event_ts"), ["event_ts"])

    def test_composite_strips_whitespace(self):
        self.assertEqual(parse_sequence_by_columns(" ts , id "), ["ts", "id"])

    def test_dotted_preserved(self):
        self.assertEqual(
            parse_sequence_by_columns("_metadata.file_path"),
            ["_metadata.file_path"],
        )

    def test_expression_rejected(self):
        with self.assertRaisesRegex(ValueError, r"expression"):
            parse_sequence_by_columns("coalesce(a, b)")


class ValidateSqlWhereClauseTests(unittest.TestCase):
    """``where_clause`` denylist for the Metadata Browse table preview.

    The Databricks Statement Execution API can't parameterise a
    structural WHERE expression, so any user-supplied fragment must be
    denylist-validated at the App boundary. These tests pin both the
    accept set (legitimate column comparisons) and the reject set
    (statement separators, comments, identifier delimiters, set ops,
    DDL / DML keywords) so a refactor can't quietly relax the contract.
    """

    def test_empty_returns_empty(self):
        self.assertEqual(validate_sql_where_clause(""), "")

    def test_none_returns_empty(self):
        self.assertEqual(validate_sql_where_clause(None), "")

    def test_non_string_rejected(self):
        with self.assertRaisesRegex(ValueError, r"must be a string"):
            validate_sql_where_clause(123)

    def test_oversized_rejected(self):
        with self.assertRaisesRegex(ValueError, r"maximum allowed"):
            validate_sql_where_clause("a" * 5000)

    def test_simple_comparison_passes(self):
        v = "col1 = 'CA' AND col2 > 10"
        self.assertEqual(validate_sql_where_clause(v), v)

    def test_like_passes(self):
        v = "name LIKE 'foo%'"
        self.assertEqual(validate_sql_where_clause(v), v)

    def test_in_list_passes(self):
        v = "state IN ('CA', 'NY', 'TX')"
        self.assertEqual(validate_sql_where_clause(v), v)

    def test_semicolon_rejected(self):
        with self.assertRaisesRegex(ValueError, r"';'"):
            validate_sql_where_clause("1=1; DROP TABLE x")

    def test_line_comment_rejected(self):
        with self.assertRaisesRegex(ValueError, r"'--'"):
            validate_sql_where_clause("1=1 --")

    def test_block_comment_open_rejected(self):
        with self.assertRaisesRegex(ValueError, r"'/\*'"):
            validate_sql_where_clause("1=1 /* hi")

    def test_block_comment_close_rejected(self):
        with self.assertRaisesRegex(ValueError, r"'\*/'"):
            validate_sql_where_clause("1=1 hi */")

    def test_backtick_rejected(self):
        with self.assertRaisesRegex(ValueError, r"'`'"):
            validate_sql_where_clause("1=1 AND `evil`.col=1")

    def test_union_rejected(self):
        with self.assertRaisesRegex(ValueError, r"'UNION'"):
            validate_sql_where_clause(
                "1=1 UNION SELECT * FROM system.information_schema.columns"
            )

    def test_union_case_insensitive(self):
        with self.assertRaisesRegex(ValueError, r"'UNION'"):
            validate_sql_where_clause("1=1 union select 1")

    def test_drop_rejected(self):
        with self.assertRaisesRegex(ValueError, r"'DROP'"):
            validate_sql_where_clause("DROP TABLE x")

    def test_insert_rejected(self):
        with self.assertRaisesRegex(ValueError, r"'INSERT'"):
            validate_sql_where_clause("INSERT INTO x VALUES (1)")

    def test_grant_rejected(self):
        with self.assertRaisesRegex(ValueError, r"'GRANT'"):
            validate_sql_where_clause("GRANT ALL ON x TO me")

    def test_unionized_state_is_not_union(self):
        """Word-boundary regex must not flag the substring ``UNION``
        inside legitimate column names like ``unionized_state``."""
        v = "unionized_state = 'CA'"
        self.assertEqual(validate_sql_where_clause(v), v)

    def test_intersection_column_is_not_intersect(self):
        v = "intersection_id = 42"
        self.assertEqual(validate_sql_where_clause(v), v)

    def test_kind_appears_in_error(self):
        with self.assertRaisesRegex(ValueError, r"row_filter"):
            validate_sql_where_clause("1=1;", kind="row_filter")


class ValidateColumnCommentTests(unittest.TestCase):
    def test_none_and_empty_ok(self):
        self.assertEqual(validate_column_comment(None), "")
        self.assertEqual(validate_column_comment(""), "")

    def test_free_text_including_keywords_ok(self):
        # No dangerous-keyword denylist — a comment is a quoted literal.
        v = "the customer's select preference (union of regions)"
        self.assertEqual(validate_column_comment(v), v)

    def test_non_string_raises(self):
        with self.assertRaises(ValueError):
            validate_column_comment(123)

    def test_too_long_raises(self):
        with self.assertRaises(ValueError):
            validate_column_comment("x" * 1001)

    def test_control_chars_raise(self):
        with self.assertRaisesRegex(ValueError, "control characters"):
            validate_column_comment("line1\nline2")


class ValidateColumnMaskClauseTests(unittest.TestCase):
    def test_none_and_empty_ok(self):
        self.assertEqual(validate_column_mask_clause(None), "")
        self.assertEqual(validate_column_mask_clause(""), "")

    def test_bare_function_name_ok(self):
        v = "cat.sec.mask_ssn"
        self.assertEqual(validate_column_mask_clause(v), v)

    def test_using_columns_ok(self):
        v = "cat.sec.mask_ssn USING COLUMNS (region, tier)"
        self.assertEqual(validate_column_mask_clause(v), v)

    def test_bad_function_name_raises(self):
        with self.assertRaises(ValueError):
            validate_column_mask_clause("bad-name USING COLUMNS (a)")

    def test_bad_using_column_raises(self):
        with self.assertRaises(ValueError):
            validate_column_mask_clause("cat.s.fn USING COLUMNS (b-ad)")

    def test_injection_token_raises(self):
        with self.assertRaisesRegex(ValueError, "disallowed token"):
            validate_column_mask_clause("cat.s.fn; DROP TABLE x")

    def test_too_many_name_parts_raises(self):
        with self.assertRaises(ValueError):
            validate_column_mask_clause("a.b.c.d USING COLUMNS (x)")

    def test_non_string_raises(self):
        with self.assertRaisesRegex(ValueError, "must be a string"):
            validate_column_mask_clause(123)

    def test_unparseable_clause_shape_raises(self):
        # Multiple bare tokens with no USING COLUMNS -> no regex match.
        with self.assertRaisesRegex(ValueError, "not a valid mask clause"):
            validate_column_mask_clause("cat.s.fn extra tokens")


class ValidateColumnPolicyDictTests(unittest.TestCase):
    def test_comments_dict_and_json_string(self):
        self.assertEqual(
            validate_column_comments({"ssn": "the ssn"}), {"ssn": "the ssn"}
        )
        self.assertEqual(
            validate_column_comments('{"ssn": "the ssn"}'), {"ssn": "the ssn"}
        )

    def test_masks_dict_ok(self):
        d = {"ssn": "cat.s.f USING COLUMNS (region)"}
        self.assertEqual(validate_column_masks(d), d)

    def test_none_returns_empty(self):
        self.assertEqual(validate_column_masks(None), {})
        self.assertEqual(validate_column_comments(""), {})

    def test_phantom_none_values_dropped(self):
        # spark.read.json schema unification can surface columns from OTHER
        # rows with a None value; those are not this row's policies.
        self.assertEqual(
            validate_column_masks({"ssn": "cat.s.f", "email": None}),
            {"ssn": "cat.s.f"},
        )

    def test_bad_column_key_raises(self):
        with self.assertRaises(ValueError):
            validate_column_comments({"bad-col": "x"})

    def test_non_dict_raises(self):
        with self.assertRaises(ValueError):
            validate_column_masks("not json at all {")

    def test_json_parses_to_non_dict_raises(self):
        # Valid JSON but not an object -> distinct error branch.
        with self.assertRaisesRegex(ValueError, "must be a JSON object"):
            validate_column_masks("[1, 2, 3]")

    def test_bad_mask_value_raises(self):
        with self.assertRaises(ValueError):
            validate_column_masks({"ssn": "cat.s.f; DROP TABLE x"})

    def test_empty_mask_value_dropped(self):
        # An empty mask value would render an invalid bare ``MASK`` clause, so
        # it is dropped before it can reach the renderer. Non-empty masks in
        # the same map survive.
        self.assertEqual(
            validate_column_masks({"ssn": "", "email": "cat.s.f"}),
            {"email": "cat.s.f"},
        )
        # A map of only empty masks collapses to an empty (but valid) policy.
        self.assertEqual(validate_column_masks({"ssn": ""}), {})

    def test_empty_comment_value_kept(self):
        # Empty comments render valid ``COMMENT ''`` DDL, so unlike masks they
        # are NOT dropped — the field is retained.
        self.assertEqual(
            validate_column_comments({"ssn": ""}), {"ssn": ""}
        )


if __name__ == "__main__":
    unittest.main()
