"""Pure unit tests for core sink helpers."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from singer_sdk.exceptions import MissingKeyPropertiesError

import target_bigquery.core as core
from target_bigquery.batch_job import BigQueryBatchJobDenormalizedSink, BigQueryBatchJobSink
from target_bigquery.constants import DEFAULT_SCHEMA
from target_bigquery.core import (
    BigQueryTable,
    IngestionStrategy,
    SchemaResolverVersion,
    make_json_compatible,
    selection_matches,
)
from target_bigquery.gcs_stage import BigQueryGcsStagingSink


class FakeBigQueryJob:
    def result(self) -> None:
        pass


class FakeBigQueryClient:
    def __init__(self, table: bigquery.Table) -> None:
        self.table = table
        self.queries: list[str] = []
        self.updated: list[tuple[bigquery.Table, list[str]]] = []

    def get_table(self, table_ref: bigquery.TableReference) -> bigquery.Table:
        assert table_ref == self.table.reference
        return self.table

    def update_table(
        self,
        table: bigquery.Table,
        fields: list[str],
        **kwargs: object,
    ) -> bigquery.Table:
        self.updated.append((table, fields))
        return table

    def query(self, sql: str) -> FakeBigQueryJob:
        self.queries.append(sql)
        return FakeBigQueryJob()


class FakeStorageClient:
    def __init__(self) -> None:
        self.created: list[tuple[SimpleNamespace, str]] = []

    def get_bucket(self, bucket_name: str) -> SimpleNamespace:
        raise NotFound(f"bucket {bucket_name} not found")

    def create_bucket(self, bucket: SimpleNamespace, location: str) -> SimpleNamespace:
        bucket.location = location
        self.created.append((bucket, location))
        return bucket


def make_sink(config: dict[str, Any], *, stream_name: str = "orders") -> BigQueryBatchJobSink:
    sink = object.__new__(BigQueryBatchJobSink)
    sink._config = config
    sink.stream_name = stream_name
    return sink


@pytest.mark.parametrize(
    "selection,expected",
    [
        ("true", True),
        ("FALSE", False),
        ("orders", True),
        ("customers", False),
        ("customers,orders", True),
        ("orders,!orders", False),
        ('["!orders", "orders"]', True),
    ],
)
def test_selection_matches_accepts_env_and_interactive_strings(selection, expected):
    assert selection_matches(selection, "orders") is expected


@pytest.mark.parametrize(
    "config,expected",
    [
        ({}, False),
        ({"upsert": False}, False),
        ({"upsert": True}, True),
        ({"upsert": "false"}, False),
        ({"upsert": "true"}, True),
        ({"upsert": "orders,!orders"}, False),
        ({"upsert": "customers,orders"}, True),
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
        ({"overwrite": "false"}, False),
        ({"overwrite": "true"}, True),
        ({"overwrite": "orders,!orders"}, False),
        ({"overwrite": "customers,orders"}, True),
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
        ({"dedupe_before_upsert": "false"}, False),
        ({"dedupe_before_upsert": "true"}, True),
        ({"dedupe_before_upsert": "orders,!orders"}, False),
        ({"dedupe_before_upsert": "customers,orders"}, True),
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


def test_table_name_replaces_bigquery_invalid_characters():
    sink = make_sink({}, stream_name="test:specialchars!in?attributes")

    assert sink.table_name == "test_specialchars_in_attributes"


def test_temporary_table_name_template_uses_safe_placeholders(monkeypatch):
    sink = make_sink(
        {"temporary_table_name_template": "tmp-{table_name}-{timestamp}-{uuid}"},
        stream_name="Orders.Stream",
    )
    monkeypatch.setattr(core.time, "strftime", lambda _: "20260705020530")
    monkeypatch.setattr(core.uuid, "uuid4", lambda: SimpleNamespace(hex="abc123"))

    assert sink._temporary_table_name() == "tmp_orders_stream_20260705020530_abc123"


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


def test_fixed_schema_key_properties_are_not_sdk_validation_keys():
    assert make_sink({})._target_key_properties(["id"]) == []


def test_fixed_schema_key_validation_checks_nested_data_dict():
    sink = make_sink({})
    sink._input_key_properties = ["id"]

    sink._singer_validate_message({"data": {"id": 1}})

    with pytest.raises(MissingKeyPropertiesError, match="missing one or more"):
        sink._singer_validate_message({"data": {"other": 1}})


def test_fixed_schema_key_validation_checks_nested_data_json_string():
    sink = make_sink({})
    sink._input_key_properties = ["id"]

    sink._singer_validate_message({"data": '{"id": 1}'})

    with pytest.raises(MissingKeyPropertiesError, match="missing one or more"):
        sink._singer_validate_message({"data": '{"other": 1}'})


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


def test_denormalized_key_properties_apply_column_transforms():
    sink = object.__new__(BigQueryBatchJobDenormalizedSink)
    sink.table = BigQueryTable(
        name="orders",
        dataset="analytics",
        project="project",
        jsonschema={"type": "object", "properties": {}},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        transforms={"snake_case": True, "replace_period_with_underscore": True},
    )

    keys = sink._transformed_key_properties(["OrderID", "Nested.Value"])

    assert keys == ["order_id", "nested__value"]
    assert sink._target_key_properties(["OrderID", "Nested.Value"]) == [
        "order_id",
        "nested__value",
    ]


def test_create_target_prefers_explicit_clustering_fields():
    calls: dict[str, Any] = {}
    sink = cast(
        Any,
        make_sink(
            {
                "cluster_on_key_properties": True,
                "clustering_fields": ["tenant_id", "updated_at", "id", "region", "ignored"],
            }
        ),
    )
    sink.client = object()
    sink.table = SimpleNamespace(
        create_table=lambda client, apply_transforms, **kwargs: (
            calls.update(
                client=client,
                apply_transforms=apply_transforms,
                kwargs=kwargs,
            )
            or True
        )
    )

    assert sink.create_target(key_properties=["tap_id"]) is True

    assert calls["kwargs"]["table"]["clustering_fields"] == (
        "tenant_id",
        "updated_at",
        "id",
        "region",
    )


def test_streaming_insert_records_are_normalized_to_json_compatible_values():
    record = {
        "id": Decimal("1"),
        "amount": Decimal("12.34"),
        "items": [{"quantity": Decimal("2")}],
    }

    assert make_json_compatible(record) == {
        "id": 1,
        "amount": 12.34,
        "items": [{"quantity": 2}],
    }


def test_denormalized_update_schema_uses_bigquery_client_for_gcs_stage_sinks():
    sink = object.__new__(BigQueryBatchJobDenormalizedSink)
    sink.table = BigQueryTable(
        name="orders",
        dataset="analytics",
        project="project",
        jsonschema={
            "type": "object",
            "properties": {
                "Existing": {"type": ["string", "null"]},
                "NewField": {"type": ["integer", "null"]},
            },
        },
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        transforms={"snake_case": True},
    )
    existing_table = bigquery.Table(
        sink.table.as_ref(),
        schema=[bigquery.SchemaField("existing", "STRING")],
    )
    client = FakeBigQueryClient(existing_table)
    sink._get_bigquery_client = lambda: client
    sink.client = SimpleNamespace()

    sink.update_schema()

    assert [field.name for field in existing_table.schema] == ["existing", "new_field"]
    assert client.updated == [(existing_table, ["schema"])]


def test_merge_table_uses_shared_columns_and_does_not_update_keys():
    sink = object.__new__(BigQueryBatchJobDenormalizedSink)
    sink._config = {"dedupe_before_upsert": False}
    sink.stream_name = "orders"
    sink._key_properties = ["id"]
    sink.table = BigQueryTable(
        name="orders__tmp",
        dataset="analytics",
        project="project",
        jsonschema={
            "type": "object",
            "properties": {
                "id": {"type": ["integer", "null"]},
                "value": {"type": ["string", "null"]},
                "source_only": {"type": ["string", "null"]},
            },
        },
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )
    sink.merge_target = BigQueryTable(
        name="orders",
        dataset="analytics",
        project="project",
        jsonschema={"type": "object", "properties": {}},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )
    target_table = bigquery.Table(
        sink.merge_target.as_ref(),
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("value", "STRING"),
            bigquery.SchemaField("target_only", "STRING"),
        ],
    )
    client = FakeBigQueryClient(target_table)

    sink.merge_table(client)

    sql = client.queries[0]
    assert "target.`id` = source.`id`" in sql
    assert "UPDATE SET target.`value` = source.`value`" in sql
    assert "UPDATE SET target.`id`" not in sql
    assert "INSERT (`id`, `value`)" in sql
    assert "source_only" not in sql
    assert "target_only" not in sql


def test_merge_table_omits_matched_clause_when_only_keys_are_shared():
    sink = object.__new__(BigQueryBatchJobDenormalizedSink)
    sink._config = {"dedupe_before_upsert": False}
    sink.stream_name = "orders"
    sink._key_properties = ["id"]
    sink.table = BigQueryTable(
        name="orders__tmp",
        dataset="analytics",
        project="project",
        jsonschema={
            "type": "object",
            "properties": {"id": {"type": ["integer", "null"]}},
        },
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )
    sink.merge_target = BigQueryTable(
        name="orders",
        dataset="analytics",
        project="project",
        jsonschema={"type": "object", "properties": {}},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )
    target_table = bigquery.Table(
        sink.merge_target.as_ref(),
        schema=[bigquery.SchemaField("id", "INTEGER")],
    )
    client = FakeBigQueryClient(target_table)

    sink.merge_table(client)

    assert "WHEN MATCHED" not in client.queries[0]


def test_gcs_bucket_creation_handles_missing_bucket():
    sink = object.__new__(BigQueryGcsStagingSink)
    sink._config = {"location": "US"}
    sink.bucket_name = "target-bigquery-test"
    sink.client = FakeStorageClient()
    bucket = SimpleNamespace(name=sink.bucket_name, location="US")
    sink.as_bucket = lambda: bucket

    created = sink.create_bucket_if_not_exists()

    assert created is bucket
    assert sink.client.created == [(bucket, "US")]


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
