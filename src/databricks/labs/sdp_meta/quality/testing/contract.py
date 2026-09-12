"""Reusable behavioral contract for quality-engine plugin packages."""

from databricks.labs.sdp_meta.quality.spi import (
    SPI_API_VERSION,
    ExpectationBinding,
    QualityEnginePlugin,
    unpack_validation_result,
)


def assert_quality_engine_contract(
    plugin,
    input_df,
    valid_document,
    invalid_document,
    options=None,
    *,
    row_id_column=None,
    expected_main_ids=None,
    expected_invalid_ids=None,
    expected_overlap_ids=None,
):
    """Assert the public SPI behavior expected by SDP-META's writer."""
    options = options or {}
    assert isinstance(plugin, QualityEnginePlugin)
    assert plugin.api_version == SPI_API_VERSION
    reserved = plugin.reserved_columns()
    assert isinstance(reserved, (tuple, list))
    assert reserved and all(isinstance(name, str) for name in reserved)
    assert len(reserved) == len(set(reserved))
    if plugin.allows_overlap:
        diagnostics = plugin.overlap_diagnostics
        assert isinstance(diagnostics, (tuple, list))
        assert diagnostics
        assert len(diagnostics) == len(set(diagnostics))
        assert set(diagnostics).issubset(reserved)
    else:
        assert plugin.overlap_diagnostics is None

    validation = plugin.validate(valid_document, options)
    rules, _ = unpack_validation_result(validation)
    assert isinstance(rules, (dict, list))
    schema_less_validation = plugin.validate(
        valid_document, {**options, "schema": None}
    )
    schema_less_rules, analysis_required = unpack_validation_result(
        schema_less_validation
    )
    assert isinstance(schema_less_rules, (dict, list))
    if analysis_required:
        assert type(plugin).analyze is not QualityEnginePlugin.analyze

    try:
        plugin.validate(invalid_document, options)
    except (TypeError, ValueError):
        pass
    else:
        raise AssertionError("Plugin accepted an invalid rule document")

    checked = plugin.apply(input_df, rules, options)
    assert set(reserved).issubset(checked.columns)
    main = plugin.get_main_input(checked)
    invalid = plugin.get_invalid(checked)
    assert hasattr(main, "schema")
    assert hasattr(invalid, "schema")
    if row_id_column is not None:
        main_rows = main.select(row_id_column).collect()
        invalid_rows = invalid.select(
            row_id_column, *(plugin.overlap_diagnostics or ())
        ).collect()
        main_ids = {row[row_id_column] for row in main_rows}
        invalid_ids = {row[row_id_column] for row in invalid_rows}
        overlap_ids = main_ids.intersection(invalid_ids)
        if expected_main_ids is not None:
            assert main_ids == set(expected_main_ids)
        if expected_invalid_ids is not None:
            assert invalid_ids == set(expected_invalid_ids)
        if plugin.allows_overlap:
            assert expected_overlap_ids is not None
            assert overlap_ids == set(expected_overlap_ids)
            invalid_by_id = {
                row[row_id_column]: row for row in invalid_rows
            }
            for row_id in overlap_ids:
                assert any(
                    invalid_by_id[row_id][name]
                    for name in plugin.overlap_diagnostics
                )
        else:
            assert not overlap_ids

    bindings = plugin.native_expectations(rules, options)
    assert isinstance(bindings, (tuple, list))
    assert all(isinstance(item, ExpectationBinding) for item in bindings)
    assert all(item.target in ("checked", "main") for item in bindings)
    assert all(
        item.action in ("expect", "expect_or_drop", "expect_or_fail")
        for item in bindings
    )
    if not plugin.allows_overlap:
        assert plugin.overlap_diagnostics is None
