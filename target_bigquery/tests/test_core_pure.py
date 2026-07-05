"""Pure unit tests for core sink helpers."""

from __future__ import annotations

from typing import Any, cast

import pytest

from target_bigquery.batch_job import BigQueryBatchJobDenormalizedSink, BigQueryBatchJobSink
from target_bigquery.constants import DEFAULT_SCHEMA
from target_bigquery.core import BigQueryTable, IngestionStrategy, SchemaResolverVersion


def make_sink(config: dict[str, Any], *, stream_name: str = "orders") -> BigQueryBatchJobSink:
    sink = object.__new__(BigQueryBatchJobSink)
    sink._config = config
    sink.stream_name = stream_name
    return sink


@pytest.mark.parametrize(
    "config,expected",
    [
        ({}, False),
        ({"upsert": False}, False),
        ({"upsert": True}, True),
        ({"upsert": ["customers", "orders"]}, True),
        ({"upsert": ["orders", "!orders"]}, False),
        ({"upsert": ["!orders", "orders"]}, True),
    ],
)
def test_upsert_candidate_selection_uses_last_matching_pattern(config, expected):
    assert make_sink(config)._is_upsert_candidate() is expected


@pytest.mark.parametrize(
    "config,expected",
    [
        ({}, False),
        ({"overwrite": False}, False),
        ({"overwrite": True}, True),
        ({"overwrite": ["customers", "orders"]}, True),
        ({"overwrite": ["orders", "!orders"]}, False),
        ({"overwrite": ["!orders", "orders"]}, True),
    ],
)
def test_overwrite_candidate_selection_uses_last_matching_pattern(config, expected):
    assert make_sink(config)._is_overwrite_candidate() is expected


@pytest.mark.parametrize(
    "config,expected",
    [
        ({}, False),
        ({"dedupe_before_upsert": False}, False),
        ({"dedupe_before_upsert": True}, True),
        ({"dedupe_before_upsert": ["customers", "orders"]}, True),
        ({"dedupe_before_upsert": ["orders", "!orders"]}, False),
        ({"dedupe_before_upsert": ["!orders", "orders"]}, True),
    ],
)
def test_dedupe_before_upsert_candidate_selection_uses_last_matching_pattern(config, expected):
    assert make_sink(config)._is_dedupe_before_upsert_candidate() is expected


def test_table_name_normalizes_singer_stream_names_for_bigquery():
    sink = make_sink({}, stream_name="Salesforce.Account-Owner")

    assert sink.table_name == "salesforce_account_owner"


def test_fixed_schema_preprocess_record_moves_sdc_metadata_under_top_level_keys():
    sink = make_sink({})
    record = {
        "id": 1,
        "name": "Ada",
        "_sdc_extracted_at": "2026-07-05T00:00:00Z",
        "_sdc_sequence": 42,
    }

    transformed = sink.preprocess_record(record, {})

    assert transformed == {
        "data": {"id": 1, "name": "Ada"},
        "_sdc_extracted_at": "2026-07-05T00:00:00Z",
        "_sdc_received_at": None,
        "_sdc_batched_at": None,
        "_sdc_deleted_at": None,
        "_sdc_sequence": 42,
        "_sdc_table_version": None,
    }
    assert record == {"id": 1, "name": "Ada"}


def test_denormalized_preprocess_record_applies_table_column_transforms():
    sink = object.__new__(BigQueryBatchJobDenormalizedSink)
    sink.table = BigQueryTable(
        name="orders",
        dataset="analytics",
        project="project",
        jsonschema={"type": "object", "properties": {}},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        transforms={"snake_case": True, "replace_period_with_underscore": True},
    )

    transformed = sink.preprocess_record(
        {"OrderID": 1, "Nested.Value": {"InnerID": 2}},
        {},
    )

    assert transformed == {"order_id": 1, "nested__value": {"inner_id": 2}}


def test_fixed_strategy_resolves_to_default_schema():
    table = BigQueryTable(
        name="events",
        dataset="analytics",
        project="project",
        jsonschema={"type": "object", "properties": {"id": {"type": "integer"}}},
        ingestion_strategy=IngestionStrategy.FIXED,
    )

    assert table.get_resolved_schema() == DEFAULT_SCHEMA


def test_denormalized_strategy_resolves_translated_schema_with_transforms():
    table = BigQueryTable(
        name="events",
        dataset="analytics",
        project="project",
        jsonschema={"type": "object", "properties": {"EventID": {"type": "integer"}}},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        transforms={"snake_case": True},
    )

    schema = table.get_resolved_schema(apply_transforms=True)

    assert [field.name for field in schema] == ["event_id"]
    assert [field.field_type for field in schema] == ["INTEGER"]


def test_unknown_ingestion_strategy_is_rejected():
    table = BigQueryTable(
        name="events",
        dataset="analytics",
        project="project",
        jsonschema={},
        ingestion_strategy=cast(IngestionStrategy, "not-real"),
    )

    with pytest.raises(ValueError, match="Invalid ingestion strategy: not-real"):
        table.get_resolved_schema()


def test_schema_resolver_version_string_matches_wire_value():
    assert str(SchemaResolverVersion.V1) == "1"
    assert str(SchemaResolverVersion.V2) == "2"
