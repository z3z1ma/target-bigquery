"""Reject lossy transforms before destination mutation or record buffering."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from singer_sdk.sinks import BatchSink

from target_bigquery import core
from target_bigquery.batch_job import BigQueryBatchJobDenormalizedSink, BigQueryBatchJobSink
from target_bigquery.core import BaseBigQuerySink, SchemaResolverVersion, SchemaTranslator
from target_bigquery.gcs_stage import BigQueryGcsStagingDenormalizedSink
from target_bigquery.storage_write import BigQueryStorageWriteDenormalizedSink
from target_bigquery.streaming_insert import BigQueryStreamingInsertDenormalizedSink

SINKS = [
    BigQueryBatchJobDenormalizedSink,
    BigQueryGcsStagingDenormalizedSink,
    BigQueryStorageWriteDenormalizedSink,
    BigQueryStreamingInsertDenormalizedSink,
]


def schema_for(properties):
    return {"type": "object", "properties": properties}


@pytest.mark.parametrize("version", list(SchemaResolverVersion))
@pytest.mark.parametrize(
    "transforms,keys",
    [
        ({"snake_case": True}, ["CamelKey", "camel_key"]),
        ({"lower": True}, ["KEY", "key"]),
        ({"replace_period_with_underscore": True}, ["a.b", "a_b"]),
        ({"add_underscore_when_invalid": True}, ["1key", "_1key"]),
        ({"snake_case": True, "quote": True}, ["CamelKey", "camel_key", "camel_key_1"]),
    ],
)
def test_schema_and_record_collisions_rejected(version, transforms, keys):
    record = dict.fromkeys(keys, 1)
    schema = schema_for(dict.fromkeys(keys, {"type": "integer"}))
    translator = SchemaTranslator(schema, transforms, version)
    for operation in [
        lambda: translator.translated_schema_transformed,
        lambda: translator.translate_record(record),
    ]:
        with pytest.raises(ValueError, match="collision at") as exc:
            operation()
        assert repr(keys[0]) in str(exc.value)
        assert repr(keys[1]) in str(exc.value)
    assert record == dict.fromkeys(keys, 1)


@pytest.mark.parametrize("repeated", [False, True])
@pytest.mark.parametrize("version", list(SchemaResolverVersion))
def test_nested_declared_collision_preflight(version, repeated):
    child = schema_for({"CamelKey": {"type": "integer"}, "camel_key": {"type": "integer"}})
    if repeated:
        child = {"type": "array", "items": child}
    translator = SchemaTranslator(schema_for({"payload": child}), {"snake_case": True}, version)
    with pytest.raises(ValueError, match=r"\$\['payload'\]"):
        translator.validate_transform_collisions()


@pytest.mark.parametrize("sink_class", SINKS)
@pytest.mark.parametrize("version", list(SchemaResolverVersion))
def test_dynamic_json_collision_fails_before_transport_without_input_mutation(sink_class, version):
    sink = object.__new__(sink_class)
    sink.stream_name = "orders"
    sink.table = SimpleNamespace(
        schema_translator=SchemaTranslator(
            schema_for({"rows": {"type": "array"}}), {"snake_case": True}, version
        )
    )
    record = {"rows": [{"ValidKey": 0}, {"payload": {"CamelKey": 1, "camel_key": 2}}]}
    original = deepcopy(record)
    with pytest.raises(ValueError, match=r"Stream 'orders'.*\$\['rows'\]\[1\]\['payload'\]"):
        processed = sink.preprocess_record(record, {})
        sink.process_record(processed, {})
    assert record == original


@pytest.mark.parametrize("sink_class", SINKS)
def test_declared_collision_rejected_before_destination_calls(monkeypatch, sink_class):
    def init(self, target, stream_name, schema, key_properties):
        self._config = {
            "project": "project",
            "dataset": "dataset",
            "column_name_transforms": {"snake_case": True},
        }
        self.stream_name = stream_name
        self.schema = schema

    monkeypatch.setattr(BatchSink, "__init__", init)
    monkeypatch.setattr(core, "bigquery_client_factory", lambda credentials: None)
    monkeypatch.setattr(
        sink_class, "create_target", lambda *a, **kw: pytest.fail("destination mutation")
    )
    sink = object.__new__(sink_class)
    with pytest.raises(ValueError, match="Stream 'orders'.*collision"):
        BaseBigQuerySink.__init__(
            sink,
            None,
            "orders",
            schema_for({"CamelKey": {"type": "integer"}, "camel_key": {"type": "integer"}}),
            [],
        )


def test_valid_nested_records_keep_values_and_original_input():
    record = {"Rows": [{"FirstKey": 1}, {"SecondKey": 2}], "Value": {"ThirdKey": 3}}
    original = deepcopy(record)
    translator = SchemaTranslator({}, {"snake_case": True})
    assert translator.translate_record(record) == {
        "rows": [{"first_key": 1}, {"second_key": 2}],
        "value": {"third_key": 3},
    }
    assert record == original


def test_fixed_raw_record_keeps_colliding_document_keys():
    sink = object.__new__(BigQueryBatchJobSink)
    record = {"CamelKey": 1, "camel_key": 2}
    processed = sink.preprocess_record(record, {})
    assert processed["data"] == record


def test_disabled_transforms_leave_record_unchanged():
    record = {"CamelKey": 1, "camel_key": 2}
    assert SchemaTranslator({}, {}).translate_record(record) is record


@pytest.mark.parametrize("sink_class", SINKS)
def test_singer_stream_stops_before_buffer_or_state(monkeypatch, sink_class):
    import io
    import json

    from target_bigquery.tests.test_target import FakeRecordSink, StreamMapRecordingTarget

    target = StreamMapRecordingTarget(
        config={
            "credentials_json": "{}",
            "project": "project",
            "dataset": "dataset",
            "stream_maps": {"orders": {"__alias__": "table_name"}},
        }
    )
    target.calls = []
    target.fake_sink = FakeRecordSink()
    sink = object.__new__(sink_class)
    sink.stream_name = "orders"
    sink.table = SimpleNamespace(schema_translator=SchemaTranslator({}, {"snake_case": True}))
    monkeypatch.setattr(target.fake_sink, "preprocess_record", sink.preprocess_record)
    monkeypatch.setattr(target, "_write_state_message", lambda *a: pytest.fail("state advanced"))
    messages = [
        {
            "type": "SCHEMA",
            "stream": "orders",
            "schema": schema_for({"payload": {"type": "object"}}),
        },
        {
            "type": "RECORD",
            "stream": "orders",
            "record": {"payload": {"CamelKey": 1, "camel_key": 2}},
        },
        {"type": "STATE", "value": {"pos": 1}},
    ]
    with pytest.raises(ValueError, match="collision"):
        target.process_lines(io.StringIO("\n".join(map(json.dumps, messages))))
    assert target.fake_sink.records == []
