"""Exercise JSON transport using actual generated protobuf messages."""

import copy
import json
from decimal import Decimal

import pytest
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery_storage_v1 import types
from google.protobuf import json_format

from target_bigquery.storage_write import (
    BigQueryStorageWriteDenormalizedSink,
    BigQueryStorageWriteSink,
    proto_schema_factory_v2,
    serialize_json_fields,
)


def parse(record, schema):
    return json_format.ParseDict(
        serialize_json_fields(record, schema), proto_schema_factory_v2(schema)()
    )


@pytest.mark.parametrize(
    "value",
    [
        {},
        [],
        "",
        "é🍉",
        '{"raw":true}',
        0,
        False,
        -(2**63),
        2**64 - 1,
        Decimal("42"),
        Decimal("1.25"),
    ],
)
def test_json_scalar_and_document_values_round_trip(value):
    parsed = parse({"payload": value}, [SchemaField("payload", "JSON")])
    assert json.loads(parsed.payload) == value


def test_nullable_json_and_repeated_json_null_are_distinct():
    schema = [SchemaField("optional", "JSON"), SchemaField("items", "JSON", mode="REPEATED")]
    parsed = parse({"optional": None, "items": [None, "null", {}, []]}, schema)
    assert not parsed.HasField("optional")
    assert list(parsed.items) == ["null", '"null"', "{}", "[]"]


def test_nested_repeated_record_serialization_does_not_mutate_input():
    schema = [
        SchemaField(
            "rows",
            "RECORD",
            mode="REPEATED",
            fields=[SchemaField("payload", "JSON"), SchemaField("plain", "STRING")],
        )
    ]
    record = {"rows": [{"payload": {"nested": [True, None]}, "plain": "plain"}]}
    original = copy.deepcopy(record)
    first = parse(record, schema)
    second = parse(record, schema)
    assert record == original
    assert first == second
    assert json.loads(first.rows[0].payload) == {"nested": [True, None]}
    assert first.rows[0].plain == "plain"


@pytest.mark.parametrize(
    "value",
    [
        float("nan"),
        float("inf"),
        -float("inf"),
        Decimal("NaN"),
        Decimal("Infinity"),
        2**64,
        -(2**63) - 1,
    ],
)
@pytest.mark.parametrize("repeated", [False, True])
def test_invalid_json_numbers_fail_before_buffering(value, repeated):
    schema = [SchemaField("payload", "JSON", mode="REPEATED" if repeated else "NULLABLE")]
    sink = object.__new__(BigQueryStorageWriteDenormalizedSink)
    sink._resolved_schema = schema
    sink._proto_schema = proto_schema_factory_v2(schema)
    sink.proto_rows = types.ProtoRows()
    payload = {"nested": [value]}
    with pytest.raises((ValueError, TypeError)):
        sink.process_record({"payload": [payload] if repeated else payload}, {})
    assert list(sink.proto_rows.serialized_rows) == []


def test_fixed_schema_transport_does_not_double_encode_json():
    schema = [SchemaField("data", "JSON")]
    sink = object.__new__(BigQueryStorageWriteSink)
    sink._proto_schema = proto_schema_factory_v2(schema)
    sink.proto_rows = types.ProtoRows()
    sink.process_record({"data": '{"already":"encoded"}'}, {})
    parsed = sink.proto_schema()
    parsed.ParseFromString(sink.proto_rows.serialized_rows[0])
    assert parsed.data == '{"already":"encoded"}'


def test_top_level_transformed_name_matches_cached_proto_schema_after_rotation():
    from target_bigquery.core import BigQueryTable, IngestionStrategy, SchemaResolverVersion

    table = BigQueryTable(
        name="stage0",
        dataset="d",
        project="p",
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        schema_resolver_version=SchemaResolverVersion.V2,
        transforms={"snake_case": True},
        jsonschema={
            "type": "object",
            "properties": {
                "PacketData": {
                    "type": "object",
                    "properties": {
                        "json_payload": {"type": "object", "additionalProperties": True}
                    },
                }
            },
        },
    )
    sink = object.__new__(BigQueryStorageWriteDenormalizedSink)
    sink.table = table
    sink.proto_rows = types.ProtoRows()
    record = {"PacketData": {"json_payload": {"value": 1}}}
    transformed = sink.preprocess_record(record, {})
    sink.process_record(transformed, {})
    cached = sink.resolved_schema
    table.name = "stage1"
    sink.process_record(transformed, {})
    assert sink.resolved_schema is cached
    for data in sink.proto_rows.serialized_rows:
        parsed = sink.proto_schema()
        parsed.ParseFromString(data)
        assert json.loads(parsed.packet_data.json_payload) == {"value": 1}


@pytest.mark.parametrize("value", ["invalid", {"key": 1}])
@pytest.mark.parametrize("field_type", ["JSON", "RECORD"])
def test_repeated_field_rejects_non_arrays_before_buffering(value, field_type):
    schema = [
        SchemaField(
            "payload",
            field_type,
            mode="REPEATED",
            fields=[SchemaField("key", "INTEGER")] if field_type == "RECORD" else [],
        )
    ]
    sink = object.__new__(BigQueryStorageWriteDenormalizedSink)
    sink._resolved_schema = schema
    sink._proto_schema = proto_schema_factory_v2(schema)
    sink.proto_rows = types.ProtoRows()
    with pytest.raises(TypeError, match="requires an array"):
        sink.process_record({"payload": value}, {})
    assert list(sink.proto_rows.serialized_rows) == []
