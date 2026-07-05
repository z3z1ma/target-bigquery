"""Pure unit tests for core sink helpers."""

from __future__ import annotations

import gzip
from decimal import Decimal
from multiprocessing import Process
from types import SimpleNamespace
from typing import Any, cast

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from singer_sdk.exceptions import MissingKeyPropertiesError

from target_bigquery.batch_job import (
    BatchJobWorker,
    BigQueryBatchJobDenormalizedSink,
    BigQueryBatchJobSink,
)
from target_bigquery.constants import DEFAULT_SCHEMA
from target_bigquery.core import (
    BigQueryTable,
    Compressor,
    IngestionStrategy,
    ParType,
    SchemaResolverVersion,
    make_json_compatible,
    selection_matches,
)
from target_bigquery.core import (
    time as core_time,
)
from target_bigquery.core import (
    uuid as core_uuid,
)
from target_bigquery.gcs_stage import (
    BigQueryGcsStagingDenormalizedSink,
    BigQueryGcsStagingSink,
)
from target_bigquery.streaming_insert import BigQueryStreamingInsertSink


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


class FakeBigQueryUriClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def load_table_from_uri(
        self,
        uris: list[str],
        table_ref: bigquery.TableReference,
        *,
        timeout: int,
        job_config: bigquery.LoadJobConfig,
    ) -> FakeBigQueryJob:
        self.calls.append(
            {
                "uris": uris,
                "table_ref": table_ref,
                "timeout": timeout,
                "job_config": job_config,
            }
        )
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


class FakeExistingBucketClient:
    def __init__(self, location: str) -> None:
        self.location = location

    def get_bucket(self, bucket_name: str) -> SimpleNamespace:
        return SimpleNamespace(name=bucket_name, location=self.location)


class FakeJobQueue:
    def __init__(self) -> None:
        self.items: list[Any] = []

    def put(self, item: Any) -> None:
        self.items.append(item)


class FakeNotification:
    def __init__(self, *messages: str) -> None:
        self.messages = list(messages)

    def poll(self) -> bool:
        return bool(self.messages)

    def recv(self) -> str:
        return self.messages.pop(0)


class FakeLogger:
    def __init__(self) -> None:
        self.messages: list[tuple[str, tuple[Any, ...]]] = []

    def info(self, message: str, *args: Any) -> None:
        self.messages.append((message, args))


def make_sink(config: dict[str, Any], *, stream_name: str = "orders") -> BigQueryBatchJobSink:
    sink = object.__new__(BigQueryBatchJobSink)
    sink._config = config
    sink.stream_name = stream_name
    return sink


def make_bigquery_table(
    *,
    name: str = "orders",
    jsonschema: dict[str, Any] | None = None,
    ingestion_strategy: IngestionStrategy = IngestionStrategy.FIXED,
    transforms: dict[str, bool] | None = None,
) -> BigQueryTable:
    return BigQueryTable(
        name=name,
        dataset="analytics",
        project="project",
        jsonschema=jsonschema or {"type": "object", "properties": {"id": {"type": "integer"}}},
        ingestion_strategy=ingestion_strategy,
        transforms=transforms or {},
    )


def make_denormalized_batch_sink(
    *,
    jsonschema: dict[str, Any] | None = None,
    transforms: dict[str, bool] | None = None,
) -> BigQueryBatchJobDenormalizedSink:
    sink = object.__new__(BigQueryBatchJobDenormalizedSink)
    sink.table = make_bigquery_table(
        jsonschema=jsonschema or {"type": "object", "properties": {}},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        transforms=transforms,
    )
    return sink


def make_merge_sink(
    source_properties: dict[str, dict[str, Any]],
) -> BigQueryBatchJobDenormalizedSink:
    sink = object.__new__(BigQueryBatchJobDenormalizedSink)
    sink._config = {"dedupe_before_upsert": False}
    sink.stream_name = "orders"
    sink._key_properties = ["id"]
    sink.table = make_bigquery_table(
        name="orders__tmp",
        jsonschema={"type": "object", "properties": source_properties},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )
    sink.merge_target = make_bigquery_table(
        jsonschema={"type": "object", "properties": {}},
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
    )
    return sink


def prepare_buffered_sink(
    sink_cls: type[BigQueryBatchJobSink] | type[BigQueryGcsStagingSink],
) -> tuple[Any, FakeJobQueue, list[bool]]:
    queue = FakeJobQueue()
    increments: list[bool] = []
    sink = object.__new__(sink_cls)
    sink.buffer = Compressor()
    sink.global_queue = queue
    sink.global_par_typ = ParType.THREAD
    sink.increment_jobs_enqueued = lambda: increments.append(True)
    sink.table = make_bigquery_table()
    return sink, queue, increments


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
    monkeypatch.setattr(core_time, "strftime", lambda _: "20260705020530")
    monkeypatch.setattr(core_uuid, "uuid4", lambda: SimpleNamespace(hex="abc123"))

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
    sink = make_denormalized_batch_sink(
        transforms={"snake_case": True, "replace_period_with_underscore": True}
    )

    transformed = sink.preprocess_record(
        {"OrderID": 1, "Nested.Value": {"InnerID": 2}},
        {},
    )

    assert transformed == {"order_id": 1, "nested__value": {"inner_id": 2}}


def test_denormalized_key_properties_apply_column_transforms():
    sink = make_denormalized_batch_sink(
        transforms={"snake_case": True, "replace_period_with_underscore": True}
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


def test_batch_job_process_batch_enqueues_compressed_json_job():
    sink, queue, increments = prepare_buffered_sink(BigQueryBatchJobSink)

    sink.process_record({"id": Decimal("1"), "amount": Decimal("12.5")}, {})
    sink.process_batch({})

    [job] = queue.items
    assert gzip.decompress(bytes(job.data)) == b'{"id":1,"amount":12.5}\n'
    assert job.table == sink.table.as_ref()
    assert job.config["write_disposition"] == bigquery.WriteDisposition.WRITE_APPEND
    assert increments == [True]


def test_denormalized_batch_job_config_allows_field_addition():
    sink = object.__new__(BigQueryBatchJobDenormalizedSink)
    sink.table = make_bigquery_table(ingestion_strategy=IngestionStrategy.DENORMALIZED)

    config = sink.job_config

    assert config["schema_update_options"] == [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    assert config["ignore_unknown_values"] is True
    assert sink.evolve_schema() is None


def test_batch_job_worker_factory_composes_executor_class():
    worker_cls = BigQueryBatchJobSink.worker_cls_factory(Process, {})

    assert issubclass(worker_cls, BatchJobWorker)
    assert issubclass(worker_cls, Process)


def test_streaming_insert_preprocess_serializes_fixed_schema_data_as_json():
    sink = object.__new__(BigQueryStreamingInsertSink)
    record = {"id": Decimal("1"), "amount": Decimal("12.5"), "_sdc_sequence": 7}

    transformed = sink.preprocess_record(record, {})

    assert transformed["data"] == '{"id":1,"amount":12.5}'
    assert transformed["_sdc_sequence"] == 7


def test_streaming_insert_process_batch_enqueues_copied_records():
    queue = FakeJobQueue()
    increments: list[bool] = []
    sink = object.__new__(BigQueryStreamingInsertSink)
    sink._config = {"batch_size": 10_000}
    sink.records_to_drain = []
    sink.global_queue = queue
    sink.increment_jobs_enqueued = lambda: increments.append(True)
    sink.table = make_bigquery_table()

    assert sink.max_size == 500
    sink.process_record({"id": Decimal("1")}, {})
    sink.process_batch({})

    [job] = queue.items
    assert job.table == sink.table.as_ref()
    assert job.records == [{"id": 1}]
    assert sink.records_to_drain == []
    assert increments == [True]


def test_gcs_staging_process_batch_enqueues_compressed_upload_job():
    sink, queue, increments = prepare_buffered_sink(BigQueryGcsStagingSink)
    notifier = SimpleNamespace()
    sink.bucket_name = "bucket"
    sink.gcs_notifier = notifier

    sink.process_record({"id": Decimal("1")}, {})
    sink.process_batch({"batch_id": "batch-1"})

    [job] = queue.items
    assert gzip.decompress(bytes(job.buffer)) == b'{"id":1}\n'
    assert job.batch_id == "batch-1"
    assert job.table == "orders"
    assert job.dataset == "analytics"
    assert job.bucket == "bucket"
    assert job.gcs_notifier is notifier
    assert increments == [True]


def test_gcs_staging_clean_up_loads_notified_uris(monkeypatch):
    client = FakeBigQueryUriClient()
    cleaned: list[bool] = []
    monkeypatch.setattr(
        "target_bigquery.gcs_stage.bigquery_client_factory",
        lambda credentials: client,
    )
    monkeypatch.setattr(
        "target_bigquery.gcs_stage.BaseBigQuerySink.clean_up",
        lambda self: cleaned.append(True),
    )
    sink = object.__new__(BigQueryGcsStagingSink)
    sink.gcs_notification = FakeNotification("gs://bucket/path/a.jsonl.gz")
    sink.uris = []
    sink.logger = FakeLogger()
    sink._credentials = object()
    sink._config = {"timeout": 123}
    sink.table = make_bigquery_table()

    sink.clean_up()

    [call] = client.calls
    assert call["uris"] == ["gs://bucket/path/a.jsonl.gz"]
    assert call["table_ref"] == sink.table.as_ref()
    assert call["timeout"] == 123
    assert call["job_config"].write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert cleaned == [True]
    assert ("Data loaded successfully", ()) in sink.logger.messages


def test_gcs_staging_bucket_helpers_protect_location(monkeypatch):
    sink = object.__new__(BigQueryGcsStagingSink)
    sink._config = {"location": "US", "storage_class": "NEARLINE"}
    sink.bucket_name = "bucket"
    sink.client = FakeExistingBucketClient("EU")

    with pytest.raises(Exception, match="Location of existing GCS bucket"):
        sink.create_bucket_if_not_exists()

    sink.client = object()
    bucket = sink.as_bucket(location="EU", storage_class="COLDLINE")
    assert bucket.name == "bucket"
    assert bucket.storage_class == "COLDLINE"

    slept: list[int] = []
    sink._gcs_bucket = SimpleNamespace(location="US")
    monkeypatch.setattr(
        "target_bigquery.gcs_stage.time.sleep", lambda seconds: slept.append(seconds)
    )

    assert sink.create_bucket_if_not_exists() is sink._gcs_bucket
    assert slept == [5]


def test_denormalized_gcs_staging_config_allows_field_addition():
    sink = object.__new__(BigQueryGcsStagingDenormalizedSink)
    sink.table = make_bigquery_table(ingestion_strategy=IngestionStrategy.DENORMALIZED)

    config = sink.job_config

    assert config["schema_update_options"] == [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    assert config["ignore_unknown_values"] is True
    assert sink.evolve_schema() is None


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
    sink = make_merge_sink(
        {
            "id": {"type": ["integer", "null"]},
            "value": {"type": ["string", "null"]},
            "source_only": {"type": ["string", "null"]},
        }
    )
    merge_target = cast(BigQueryTable, sink.merge_target)
    target_table = bigquery.Table(
        merge_target.as_ref(),
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("value", "STRING"),
            bigquery.SchemaField("target_only", "STRING"),
        ],
    )
    client = FakeBigQueryClient(target_table)

    sink.merge_table(cast(Any, client))

    sql = client.queries[0]
    assert "target.`id` = source.`id`" in sql
    assert "UPDATE SET target.`value` = source.`value`" in sql
    assert "UPDATE SET target.`id`" not in sql
    assert "INSERT (`id`, `value`)" in sql
    assert "source_only" not in sql
    assert "target_only" not in sql


def test_merge_table_omits_matched_clause_when_only_keys_are_shared():
    sink = make_merge_sink({"id": {"type": ["integer", "null"]}})
    merge_target = cast(BigQueryTable, sink.merge_target)
    target_table = bigquery.Table(
        merge_target.as_ref(),
        schema=[bigquery.SchemaField("id", "INTEGER")],
    )
    client = FakeBigQueryClient(target_table)

    sink.merge_table(cast(Any, client))

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
