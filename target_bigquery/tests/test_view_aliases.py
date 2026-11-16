"""View column aliases should honor transforms without changing JSON lookup paths."""

import pytest

from target_bigquery.core import (
    BigQueryTable,
    IngestionStrategy,
    SchemaResolverVersion,
    SchemaTranslator,
)


@pytest.mark.parametrize("version", list(SchemaResolverVersion))
@pytest.mark.parametrize(
    "transforms,alias",
    [
        ({}, "ItemRows"),
        ({"snake_case": True}, "item_rows"),
        ({"lower": True}, "itemrows"),
        ({"snake_case": True, "quote": True}, "`item_rows`"),
    ],
)
@pytest.mark.parametrize(
    "items",
    [
        {"type": "string"},
        {"type": "object", "properties": {"InnerValue": {"type": "integer"}}},
    ],
)
def test_top_level_repeated_alias_and_source_path(version, transforms, alias, items):
    schema = {"type": "object", "properties": {"ItemRows": {"type": "array", "items": items}}}
    translator = SchemaTranslator(schema, transforms, version)
    table = BigQueryTable("orders", "dataset", "project", schema, IngestionStrategy.FIXED)
    sql = translator.generate_view_statement(table)
    assert f") AS {alias}," in sql
    assert "JSON_QUERY_ARRAY(data, '$.ItemRows')" in sql
    assert "AS ItemRows__rows" in sql
    assert translator.translated_schema[0].name == "ItemRows"
    assert schema["properties"]["ItemRows"]["items"] == items


@pytest.mark.parametrize("version", list(SchemaResolverVersion))
def test_nested_repeated_aliases_keep_original_lookup_paths(version):
    schema = {
        "type": "object",
        "properties": {
            "OuterRecord": {
                "type": "object",
                "properties": {
                    "ItemRows": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "ChildValues": {"type": "array", "items": {"type": "integer"}},
                            },
                        },
                    },
                },
            },
        },
    }
    translator = SchemaTranslator(schema, {"snake_case": True}, version)
    table = BigQueryTable("orders", "dataset", "project", schema, IngestionStrategy.FIXED)
    sql = translator.generate_view_statement(table)
    assert ") AS item_rows\n" in sql
    assert ") AS child_values\n" in sql
    assert "JSON_QUERY_ARRAY(data, '$.OuterRecord.ItemRows')" in sql
    assert "JSON_QUERY_ARRAY(ItemRows__rows, '$.ChildValues')" in sql
    assert ") as outer_record," in sql
