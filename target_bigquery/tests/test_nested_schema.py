"""Offline nested-schema alignment and destination migration preflight."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from google.protobuf import json_format

from target_bigquery import core
from target_bigquery.batch_job import BigQueryBatchJobDenormalizedSink
from target_bigquery.core import (
    BaseBigQuerySink,
    SchemaResolverVersion,
    SchemaTranslator,
)
from target_bigquery.storage_write import proto_schema_factory_v2, serialize_json_fields
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_transform_collisions import SINKS, schema_for

SOURCE_SCHEMA = schema_for(
    {
        "PacketData": schema_for(
            {
                "ChildValue": {"type": "integer"},
                "RepeatedRows": {
                    "type": "array",
                    "items": schema_for({"NestedValue": {"type": "string"}}),
                },
                "JsonData": {"type": "object"},
            }
        )
    }
)
SOURCE = {
    "PacketData": {
        "ChildValue": 7,
        "RepeatedRows": [{"NestedValue": "value"}],
        "JsonData": {"DocumentKey": 8},
    }
}


@pytest.mark.parametrize("version", list(SchemaResolverVersion))
@pytest.mark.parametrize(
    "transforms",
    [
        {"snake_case": True},
        {"lower": True},
        {"quote": True, "snake_case": True},
        {"replace_period_with_underscore": True, "add_underscore_when_invalid": True},
        {},
    ],
)
@pytest.mark.parametrize("sink_class", SINKS)
def test_nested_schema_and_all_transport_preprocessing_match(version, transforms, sink_class):
    translator = SchemaTranslator(SOURCE_SCHEMA, transforms, version)
    original_schema = deepcopy([field.to_api_repr() for field in translator.translated_schema])
    sink = object.__new__(sink_class)
    sink.stream_name = "orders"
    sink.table = SimpleNamespace(schema_translator=translator)
    source = deepcopy(SOURCE)
    record = sink.preprocess_record(source, {})
    fields = translator.translated_schema_transformed
    message = json_format.ParseDict(
        serialize_json_fields(record, fields), proto_schema_factory_v2(fields)()
    )
    assert message.SerializeToString()
    root = fields[0].name
    assert record[root][fields[0].fields[0].name] == 7
    assert record[root][fields[0].fields[1].name][0][fields[0].fields[1].fields[0].name] == "value"
    assert source == SOURCE
    assert [field.to_api_repr() for field in translator.translated_schema] == original_schema
    assert translator.translated_schema_transformed is fields


def test_nested_period_digit_and_quote_names_use_physical_names():
    translator = SchemaTranslator(
        schema_for({"1root": schema_for({"a.b": {"type": "integer"}})}),
        {
            "quote": True,
            "replace_period_with_underscore": True,
            "add_underscore_when_invalid": True,
        },
    )
    fields = translator.translated_schema_transformed
    record = translator.translate_record({"1root": {"a.b": 3}})
    assert json_format.ParseDict(record, proto_schema_factory_v2(fields)())._1root.a_b == 3


def test_prequoted_physical_names_fail_instead_of_becoming_schema_only_quotes():
    translator = SchemaTranslator(schema_for({"`key`": {"type": "integer"}}), {"quote": True})
    with pytest.raises(ValueError, match="backticks"):
        _ = translator.translated_schema_transformed


def test_field_metadata_survives_and_json_key_policy_stays_legacy():
    translator = SchemaTranslator({}, {"snake_case": True})
    translator._translated_schema = [
        bigquery.SchemaField(
            "RootField",
            "RECORD",
            fields=[
                bigquery.SchemaField(
                    "ChildField", "NUMERIC", description="description", precision=20, scale=5
                ),
                bigquery.SchemaField("JsonData", "JSON"),
            ],
        )
    ]
    child = translator.translated_schema_transformed[0].fields[0]
    assert (child.name, child.description, child.precision, child.scale) == (
        "child_field",
        "description",
        20,
        5,
    )
    assert translator.translate_record({"RootField": {"JsonData": {"DocumentKey": 1}}}) == {
        "root_field": {"json_data": {"document_key": 1}}
    }


class Client:
    def __init__(self, fields=None, failure=None):
        self.tables = (
            {}
            if fields is None
            else {"orders": bigquery.Table("project.dataset.orders", schema=fields)}
        )
        self.failure = failure
        self.events = []

    def get_table(self, ref):
        self.events.append(("get", ref.table_id))
        if self.failure:
            raise self.failure
        if ref.table_id not in self.tables:
            raise NotFound("missing")
        return self.tables[ref.table_id]

    def get_dataset(self, ref):
        return bigquery.Dataset(ref)

    def create_table(self, table):
        self.events.append(("create", table.table_id))
        self.tables[table.table_id] = table
        return table

    def update_table(self, table, fields, **kwargs):
        self.events.append(("update", table.table_id))
        self.tables[table.table_id] = table
        return table


def construct(monkeypatch, client, sink_class=BigQueryBatchJobDenormalizedSink, mode="append"):
    monkeypatch.setattr(core, "bigquery_client_factory", lambda credentials: client)
    monkeypatch.setattr(core.time, "sleep", lambda seconds: None)
    target = TargetBigQuery(
        config={
            "project": "project",
            "dataset": "dataset",
            "credentials_json": "{}",
            "denormalized": True,
            "column_name_transforms": {"snake_case": True},
            "upsert": mode == "upsert",
            "overwrite": mode == "overwrite",
        }
    )
    sink = object.__new__(sink_class)
    BaseBigQuerySink.__init__(sink, target, "orders", deepcopy(SOURCE_SCHEMA), ["PacketData"])
    return sink


@pytest.mark.parametrize("sink_class", SINKS)
@pytest.mark.parametrize("mode", ["append", "upsert", "overwrite"])
def test_legacy_destination_rejected_before_any_mutation(monkeypatch, sink_class, mode):
    legacy = SchemaTranslator(SOURCE_SCHEMA, {}).translated_schema
    legacy[0] = bigquery.SchemaField("packet_data", "RECORD", fields=legacy[0].fields)
    client = Client(legacy)
    with pytest.raises(ValueError, match="Stream 'orders'.*incompatible nested schema"):
        construct(monkeypatch, client, sink_class, mode)
    assert client.events == [("get", "orders")]


@pytest.mark.parametrize("mode", ["append", "upsert", "overwrite"])
@pytest.mark.parametrize("existing", [False, True])
def test_new_or_compatible_destination_can_proceed(monkeypatch, mode, existing):
    fields = SchemaTranslator(SOURCE_SCHEMA, {"snake_case": True}).translated_schema_transformed
    client = Client(fields if existing else None)
    sink = construct(monkeypatch, client, mode=mode)
    assert sink.table.get_resolved_schema(True)[0].fields[0].name == "child_value"
    assert ("get", "orders") in client.events
    if mode != "append":
        assert sink.table.name != "orders"


@pytest.mark.parametrize("change", ["missing", "extra", "reordered", "type", "mode", "scalar"])
def test_incompatible_nested_structure_requires_explicit_migration(monkeypatch, change):
    fields = SchemaTranslator(SOURCE_SCHEMA, {"snake_case": True}).translated_schema_transformed
    definition = fields[0].to_api_repr()
    children = definition["fields"]
    if change == "missing":
        children.pop()
    elif change == "extra":
        children.append(bigquery.SchemaField("old_child", "STRING").to_api_repr())
    elif change == "reordered":
        children.reverse()
    elif change == "type":
        children[0]["type"] = "STRING"
    elif change == "mode":
        children[0]["mode"] = "REQUIRED"
    else:
        definition = bigquery.SchemaField("packet_data", "STRING").to_api_repr()
    client = Client([bigquery.SchemaField.from_api_repr(definition)])
    with pytest.raises(ValueError, match="No automatic nested rename or backfill"):
        construct(monkeypatch, client)
    assert client.events == [("get", "orders")]


def test_top_level_additions_and_historical_columns_remain_supported(monkeypatch):
    client = Client([bigquery.SchemaField("historical", "STRING")])
    construct(monkeypatch, client)
    assert {field.name for field in client.tables["orders"].schema} >= {"historical", "packet_data"}


def test_permission_failure_is_not_treated_as_missing_destination(monkeypatch):
    client = Client(failure=Forbidden("no metadata access"))
    with pytest.raises(Forbidden):
        construct(monkeypatch, client)
    assert client.events == [("get", "orders")]


def test_type_aliases_are_compatible(monkeypatch):
    definition = (
        SchemaTranslator(SOURCE_SCHEMA, {"snake_case": True})
        .translated_schema_transformed[0]
        .to_api_repr()
    )
    definition["type"] = "STRUCT"
    definition["fields"][0]["type"] = "INT64"
    client = Client([bigquery.SchemaField.from_api_repr(definition)])
    construct(monkeypatch, client)


def test_storage_schema_and_proto_cache_survive_real_staging_rotation(monkeypatch):
    from target_bigquery.storage_write import BigQueryStorageWriteDenormalizedSink

    fields = SchemaTranslator(SOURCE_SCHEMA, {"snake_case": True}).translated_schema_transformed
    client = Client(fields)
    sink = construct(monkeypatch, client, BigQueryStorageWriteDenormalizedSink, "upsert")
    schema, proto = sink.resolved_schema, sink.proto_schema
    first_table = sink.table.name
    sink.start_batch({})
    sink.process_record(sink.preprocess_record(deepcopy(SOURCE), {}), {})
    sink._create_overwrite_table()
    assert sink.table.name != first_table
    assert sink.resolved_schema is schema
    assert sink.proto_schema is proto
    sink.process_record(sink.preprocess_record(deepcopy(SOURCE), {}), {})
    assert len(sink.proto_rows.serialized_rows) == 2
    for row in sink.proto_rows.serialized_rows:
        assert proto.FromString(row).packet_data.repeated_rows[0].nested_value == "value"


def test_incompatible_schema_stops_singer_before_record_or_state(monkeypatch):
    import io
    import json

    legacy = SchemaTranslator(SOURCE_SCHEMA, {}).translated_schema
    client = Client([bigquery.SchemaField("packet_data", "RECORD", fields=legacy[0].fields)])
    monkeypatch.setattr(core, "bigquery_client_factory", lambda credentials: client)
    target = TargetBigQuery(
        config={
            "project": "project",
            "dataset": "dataset",
            "credentials_json": "{}",
            "method": "batch_job",
            "denormalized": True,
            "column_name_transforms": {"snake_case": True},
        }
    )
    monkeypatch.setattr(target, "_write_state_message", lambda *a: pytest.fail("state advanced"))
    messages = [
        {"type": "SCHEMA", "stream": "orders", "schema": deepcopy(SOURCE_SCHEMA)},
        {"type": "RECORD", "stream": "orders", "record": deepcopy(SOURCE)},
        {"type": "STATE", "value": {"pos": 1}},
    ]
    with pytest.raises(ValueError, match="incompatible nested schema"):
        target.process_lines(io.StringIO("\n".join(map(json.dumps, messages))))
    assert client.events == [("get", "orders")]


def test_update_rechecks_nested_schema_before_top_level_addition(monkeypatch):
    fields = SchemaTranslator(SOURCE_SCHEMA, {"snake_case": True}).translated_schema_transformed
    client = Client(fields)
    sink = construct(monkeypatch, client)
    client.tables["orders"].schema = [
        bigquery.SchemaField(
            "packet_data", "RECORD", fields=[bigquery.SchemaField("legacy", "STRING")]
        )
    ]
    client.events.clear()
    with pytest.raises(ValueError, match="incompatible nested schema"):
        sink.update_schema()
    assert client.events == [("get", "orders")]


@pytest.mark.parametrize("sink_class", [SINKS[0], SINKS[1]])
def test_batch_and_gcs_serialized_payload_matches_nested_schema(monkeypatch, sink_class):
    import gzip
    import json

    fields = SchemaTranslator(SOURCE_SCHEMA, {"snake_case": True}).translated_schema_transformed
    sink = construct(monkeypatch, Client(fields), sink_class)
    sink.buffer = core.Compressor()
    sink.process_record(sink.preprocess_record(deepcopy(SOURCE), {}), {})
    payload = json.loads(gzip.decompress(sink.buffer.getvalue()))
    assert payload["packet_data"]["repeated_rows"] == [{"nested_value": "value"}]
    configured = sink.job_config["schema"][0]
    assert configured.fields[1].fields[0].name == "nested_value"


def test_invalid_physical_name_rejected_before_new_dataset_even_without_transforms(monkeypatch):
    client = Client()
    monkeypatch.setattr(core, "bigquery_client_factory", lambda credentials: client)
    target = TargetBigQuery(
        config={
            "project": "project",
            "dataset": "dataset",
            "credentials_json": "{}",
            "denormalized": True,
        }
    )
    with pytest.raises(ValueError, match="backticks"):
        BigQueryBatchJobDenormalizedSink(
            target, "orders", schema_for({"`key`": {"type": "integer"}}), []
        )
    assert client.events == []
