"""BigQuery target sink class, which handles writing streams."""
import gzip
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from textwrap import dedent
from typing import Dict, List, Optional

import orjson
import smart_open
from google.cloud import _http, bigquery, bigquery_storage_v1, storage
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2
from memoization import cached
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
from tenacity import retry, retry_if_result
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt

from target_bigquery import record_pb2
from target_bigquery.utils import SchemaTranslator

DELAY = 1, 10, 1.5
DEFAULT_SCHEMA = [
    bigquery.SchemaField(
        "data",
        "JSON",
        description="Data ingested from Singer Tap",
    ),
    bigquery.SchemaField(
        "_sdc_extracted_at",
        bigquery.SqlTypeNames.TIMESTAMP,
        description="Timestamp indicating when the record was extracted the record from the source",
    ),
    bigquery.SchemaField(
        "_sdc_received_at",
        bigquery.SqlTypeNames.TIMESTAMP,
        description="Timestamp indicating when the record was received by the target for loading",
    ),
    bigquery.SchemaField(
        "_sdc_batched_at",
        bigquery.SqlTypeNames.TIMESTAMP,
        description="Timestamp indicating when the record's batch was initiated",
    ),
    bigquery.SchemaField(
        "_sdc_deleted_at",
        bigquery.SqlTypeNames.TIMESTAMP,
        description="Passed from a Singer tap if DELETE events are able to be tracked. In general, this is populated when the tap is synced LOG_BASED replication. If not sent from the tap, this field will be null.",
    ),
    bigquery.SchemaField(
        "_sdc_sequence",
        bigquery.SqlTypeNames.INT64,
        description="The epoch (milliseconds) that indicates the order in which the record was queued for loading",
    ),
    bigquery.SchemaField(
        "_sdc_table_version",
        bigquery.SqlTypeNames.INTEGER,
        description="Indicates the version of the table. This column is used to determine when to issue TRUNCATE commands during loading, where applicable",
    ),
]

GLOBAL_EXECUTOR = ThreadPoolExecutor(thread_name_prefix="target-bq-")


@cached
def get_bq_client(
    credentials_path: Optional[str] = None,
    credentials_json: Optional[str] = None,
) -> bigquery.Client:
    if not credentials_path and not credentials_json:
        raise KeyError(
            "Credentials not supplied. Required config of either credentials_path or credentials_json"
        )
    if credentials_path:
        return bigquery.Client.from_service_account_json(credentials_path)
    elif credentials_json:
        return bigquery.Client.from_service_account_info(orjson.loads(credentials_json))


@cached
def get_gcs_client(
    credentials_path: Optional[str] = None,
    credentials_json: Optional[str] = None,
) -> storage.Client:
    if not credentials_path and not credentials_json:
        raise KeyError(
            "Credentials not supplied. Required config of either credentials_path or credentials_json"
        )
    if credentials_path:
        return storage.Client.from_service_account_json(credentials_path)
    elif credentials_json:
        return storage.Client.from_service_account_info(orjson.loads(credentials_json))


@cached
def get_storage_client(
    credentials_path: Optional[str] = None,
    credentials_json: Optional[str] = None,
) -> bigquery_storage_v1.BigQueryWriteClient:
    if not credentials_path and not credentials_json:
        raise KeyError(
            "Credentials not supplied. Required config of either credentials_path or credentials_json"
        )
    if credentials_path:
        return bigquery_storage_v1.BigQueryWriteClient.from_service_account_file(
            credentials_path
        )
    elif credentials_json:
        return bigquery_storage_v1.BigQueryWriteClient.from_service_account_info(
            orjson.loads(credentials_json)
        )


def create_row_data(
    data: str,
    _sdc_extracted_at: str,
    _sdc_received_at: str,
    _sdc_batched_at: str,
    _sdc_deleted_at: str,
    _sdc_sequence: int,
    _sdc_table_version: int,
):
    row = record_pb2.Record()
    row.data = data
    row._sdc_received_at = _sdc_received_at
    row._sdc_batched_at = _sdc_batched_at
    row._sdc_sequence = _sdc_sequence
    # These are not guaranteed in sdc
    if _sdc_extracted_at:
        row._sdc_extracted_at = _sdc_extracted_at
    if _sdc_deleted_at:
        row._sdc_deleted_at = _sdc_deleted_at
    if _sdc_table_version:
        row._sdc_table_version = _sdc_table_version
    return row.SerializeToString()


# Base class for sinks which use a fixed schema, optionally
# including a view which is created on top of the table to unpack the data
class BaseBigQuerySink(BatchSink):

    include_sdc_metadata_properties = True

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._client = get_bq_client(
            self.config.get("credentials_path"), self.config.get("credentials_json")
        )
        self._table = f"{self.config['project']}.{self.config['dataset']}.{self.stream_name.lower()}"
        self.jobs_running = set()
        self._dataset_ref = None
        self._table_ref = None

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
        stop=stop_after_attempt(3),
    )
    def _make_target(self) -> None:
        if self._dataset_ref is None:
            self._dataset_ref = self._client.create_dataset(
                self.config["dataset"], exists_ok=True
            )
        if self._table_ref is None:
            table = bigquery.Table(
                self._table,
                schema=DEFAULT_SCHEMA,
            )
            table.clustering_fields = ["_sdc_batched_at"]
            table.description = dedent(
                f"""
                Singer based ingestion pattern.
                Target: target-bigquery (JSON)
                Stream: {self.stream_name}
            """
            ).lstrip()
            table.time_partitioning = TimePartitioning(
                type_=TimePartitioningType.DAY, field="_sdc_batched_at"
            )
            self._table_ref = self._client.create_table(table, exists_ok=True)
            if self.config.get("generate_view"):
                ddl = SchemaTranslator(self.schema).make_view_stmt(self._table)
                self._client.query(ddl).result()

    @property
    def max_size(self) -> int:
        return self.config.get("batch_size", 10000)

    def _parse_timestamps_in_record(self, *args, **kwargs) -> None:
        pass

    def start_batch(self, context: dict) -> None:
        self._make_target()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        metadata = {
            k: record.pop(k, None)
            for k in (
                "_sdc_extracted_at",
                "_sdc_received_at",
                "_sdc_batched_at",
                "_sdc_deleted_at",
                "_sdc_sequence",
                "_sdc_table_version",
            )
        }
        return {"data": record, **metadata}

    def clean_up(self) -> None:
        while len(self.jobs_running) > 0:
            time.sleep(0.1)

    def pre_state_hook(self) -> None:
        pass


# Base class for BigQuery sinks that use a denormalized approach
class BaseBigQuerySinkDenormalized(BaseBigQuerySink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._bq_schema = SchemaTranslator(schema).translated_schema

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
        stop=stop_after_attempt(3),
    )
    def _evolve_schema(self):
        if not self.config["append_columns"]:
            return

        # Create two copies, one which keeps current state and one open to mutation
        target_schema = self._bq_schema
        current_schema = self._table_ref.schema[:]
        mut_schema = self._table_ref.schema[:]

        for expected_field in target_schema:
            if not any(field.name == expected_field.name for field in current_schema):
                mut_schema.append(expected_field)

        if len(mut_schema) > len(current_schema):
            # Appends new columns to table SCHEMA
            if self.config["append_columns"]:
                self._table_ref.schema = mut_schema
                self._client.update_table(
                    self._table_ref,
                    ["schema"],
                    retry=bigquery.DEFAULT_RETRY.with_delay(*DELAY).with_deadline(
                        self.config["timeout"]
                    ),
                    timeout=self.config["timeout"],
                )

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
        stop=stop_after_attempt(3),
    )
    def _make_target(self) -> None:
        if self._dataset_ref is None:
            self._dataset_ref = self._client.create_dataset(
                self.config["dataset"], exists_ok=True
            )
        if self._table_ref is None:
            table = bigquery.Table(
                self._table,
                schema=self._bq_schema,
            )
            table.clustering_fields = ["_sdc_batched_at"]
            table.description = dedent(
                f"""
                Singer based ingestion pattern.
                Target: target-bigquery (denormalized)
                Stream: {self.stream_name}
            """
            ).lstrip()
            table.time_partitioning = TimePartitioning(
                type_=TimePartitioningType.DAY, field="_sdc_batched_at"
            )
            self._table_ref = self._client.create_table(table, exists_ok=True)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def start_batch(self, context: dict) -> None:
        self._make_target()
        self._evolve_schema()


# Lowest memory overhead since we eagerly flush to GCS using
# the multi-part upload API, compresses data in memory
class BigQueryGcsStagingImpl:
    executor = GLOBAL_EXECUTOR

    @property
    def is_full(self) -> bool:
        return self._fsize > self._max_size

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._gcs_client = get_gcs_client(
            self.config.get("credentials_path"), self.config.get("credentials_json")
        )
        self._blob_path = "gs://{}/{}/{}/{}".format(
            self.config["gcs_bucket"],
            self.config.get("prefix_override", "target_bigquery"),
            self.config["dataset"],
            self.stream_name,
        )
        self._fsize = 0
        self._max_size = self.config.get("gcs_max_file_size", 500) * 1e6
        self._gcs_files = []

    def _get_batch_id(self, context: dict):
        return context["batch_id"]

    @property
    def bufsize(self) -> int:
        _256kb: int = 256 * 1024
        return (
            int(_256kb * ((self.config.get("gcs_buffer_size", 0) * 1e6) // _256kb))
            or _256kb
        )

    @cached(custom_key_maker=_get_batch_id)
    def _get_gcs_write_handle(self, context: dict) -> BytesIO:
        fh = smart_open.open(
            uri=f"{self._blob_path}/{context['batch_id']}.jsonl.gzip",
            mode="wb",
            buffering=self.bufsize,
            transport_params=dict(
                client=self._gcs_client,
                buffer_size=self.bufsize,
                min_part_size=self.bufsize,
            ),
        )
        return gzip.GzipFile(fileobj=fh, mode="wb")

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
        stop=stop_after_attempt(3),
    )
    def _upload_records(self, target_files: str) -> bigquery.LoadJob:
        bq_job: bigquery.LoadJob = self._client.load_table_from_uri(
            target_files,
            self._table,
            timeout=self.config["timeout"],
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
            ),
        )
        bq_job._job_statistics
        self.jobs_running.add(bq_job.job_id)
        bq_job.add_done_callback(
            lambda bq: (
                self.jobs_running.remove(bq.job_id),
                self.logger.info("Job %s complete, (%s)", bq.job_id, bq.result()),
            )
        )

    def process_record(self, record: dict, context: dict) -> None:
        self._get_gcs_write_handle(context).write(
            orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        )
        self._fsize = self._get_gcs_write_handle(context).fileobj.tell()

    def process_batch(self, context: dict) -> None:
        gcs_file = f"{self._blob_path}/{context['batch_id']}.jsonl.gzip"
        inner_obj_prt = self._get_gcs_write_handle(context).fileobj
        # Flush the GzipFile object to ensure all data is written to the inner object
        self._get_gcs_write_handle(context).flush()
        self._get_gcs_write_handle(context).close()  # this detaches the inner object
        inner_obj_prt.close()  # this closes the inner object via captured ptr
        self._gcs_files.append(gcs_file)

    def clean_up(self) -> None:
        self._upload_records(self._gcs_files)
        return super().clean_up()


# Relatively fast and the data is instantly available for querying
class BigQueryLegacyStreamingImpl:
    executor = GLOBAL_EXECUTOR

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        _http.json = orjson  # patch bigquery json

    @property
    def max_size(self) -> int:
        return max(super().max_size, 500)

    @retry(
        retry=retry_if_exception_type(ConnectionError)
        | retry_if_result(lambda resp: bool(resp)),
        reraise=True,
        stop=stop_after_attempt(3),
    )
    def _upload_records(self, records_to_drain, insert_ids=None):
        errors = self._client.insert_rows_json(
            table=self._table,
            json_rows=records_to_drain,
            timeout=self.config["timeout"],
            row_ids=insert_ids if insert_ids else None,
            retry=bigquery.DEFAULT_RETRY.with_delay(*DELAY).with_deadline(
                self.config["timeout"]
            ),
        )
        if errors:
            raise ConnectionResetError
        return errors

    def preprocess_record(self, record: Dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        if not self.config.get("denormalized"):
            # Fixed schema on legacy streaming inserts require string data
            # for JSON columns.
            record["data"] = orjson.dumps(record["data"]).decode("utf-8")
        return record

    def process_record(self, record: dict, context: dict) -> None:
        self.records_to_drain.append(record)

    def process_batch(self, context: dict) -> None:
        job = self.executor.submit(
            self._upload_records,
            records_to_drain=self.records_to_drain.copy(),
        )
        self.jobs_running.add(id(job))
        job.add_done_callback(
            lambda fut: (
                self.logger.info("Job %s complete", id(fut)),
                self.jobs_running.discard(id(fut)),
            )
        )
        self.records_to_drain = []
        time.sleep(
            (len(self.jobs_running) // 25) * 0.01
        )  # mini-interrupt encourages buffer reduction via CPython select polls

    def clean_up(self) -> None:
        self.executor.shutdown(wait=True)
        return super().clean_up()


# Possibly the most performant Sink implementation, compresses data in memory
class BigQueryBatchImpl:
    executor = GLOBAL_EXECUTOR

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.buffer = gzip.GzipFile(fileobj=BytesIO(), mode="w")

    def process_record(self, record: dict, context: dict) -> None:
        self.buffer.write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))

    def process_batch(self, context: dict) -> None:
        fobj_ptr = self.buffer.fileobj
        self.buffer.close()
        fobj_ptr.flush()
        job = self.executor.submit(
            self._client.load_table_from_file,
            fobj_ptr,
            self._table,
            rewind=True,
            num_retries=3,
            timeout=self.config["timeout"],
            job_config=bigquery.LoadJobConfig(
                schema=getattr(self, "_bq_schema", DEFAULT_SCHEMA),
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
            ),
        )
        self.jobs_running.add(id(job))
        job.add_done_callback(
            lambda fut: (
                fut.result(),
                self.jobs_running.discard(id(fut)),
                self.logger.info(
                    "Job %s complete! exc(%s)", fut.result(), fut.exception()
                ),
            )
        )
        self.buffer = gzip.GzipFile(fileobj=BytesIO(), mode="w")

    def clean_up(self) -> None:
        self.executor.shutdown()
        return super().clean_up()


# This sink is tied to non-denormalized load due to a requirement
# for a static schema. All other sinks implement both.
class BigQueryStorageWriteSink(BaseBigQuerySink):
    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._make_target()
        self._tracked_streams = []
        self._offset = 0
        self._write_client = get_storage_client(
            self.config.get("credentials_path"), self.config.get("credentials_json")
        )
        self._parent = self._write_client.table_path(
            self.config["project"],
            self.config["dataset"],
            self.stream_name.lower(),
        )
        self.seed_new_append_stream()

    def preprocess_record(self, record: Dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        # For now, protobuf requires string data for JSON columns.
        record["data"] = orjson.dumps(record["data"]).decode("utf-8")
        return record

    def seed_new_append_stream(self):
        # Create write stream
        self.write_stream = types.WriteStream()
        self.write_stream.type_ = types.WriteStream.Type.PENDING
        self.write_stream = self._write_client.create_write_stream(
            parent=self._parent, write_stream=self.write_stream
        )
        self.logger.info("Created write stream: %s", self.write_stream.name)
        self._tracked_streams.append(self.write_stream.name)
        # Create request template to seed writers
        self._request_template = types.AppendRowsRequest()
        self._request_template.write_stream = (
            self.write_stream.name
        )  # <- updated when streams are created
        proto_schema = types.ProtoSchema()
        proto_descriptor = descriptor_pb2.DescriptorProto()
        record_pb2.Record.DESCRIPTOR.CopyToProto(proto_descriptor)
        proto_schema.proto_descriptor = proto_descriptor
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.writer_schema = proto_schema
        self._request_template.proto_rows = proto_data
        # Seed writer
        self.append_rows_stream = writer.AppendRowsStream(
            self._write_client, self._request_template
        )
        self._offset = self._total_records_written

    def start_batch(self, context: dict) -> None:
        self.proto_rows = types.ProtoRows()

    def process_record(self, record: dict, context: dict) -> None:
        self.proto_rows.serialized_rows.append(create_row_data(**record))

    def process_batch(self, context: dict) -> None:
        request = types.AppendRowsRequest()
        request.offset = self._total_records_written - self._offset
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = self.proto_rows
        request.proto_rows = proto_data
        if not self._tracked_streams:
            self.seed_new_append_stream()
            request.offset = self._total_records_written - self._offset
        try:
            f = self.append_rows_stream.send(request)
            self.jobs_running.add(id(f))
            f.add_done_callback(lambda fut: self.jobs_running.discard(id(fut)))
        except:
            # Simplified self-healing
            self._write_client.finalize_write_stream(name=self.write_stream.name)
            self.seed_new_append_stream()
            request.offset = self._total_records_written - self._offset
            f = self.append_rows_stream.send(request)
            self.jobs_running.add(id(f))
            f.add_done_callback(lambda fut: self.jobs_running.discard(id(fut)))

    def commit_streams(self):
        if self.jobs_running:
            self.append_rows_stream.close()
            self._write_client.finalize_write_stream(name=self.write_stream.name)
            batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest()
            batch_commit_write_streams_request.parent = self._parent
            batch_commit_write_streams_request.write_streams = self._tracked_streams
            self._write_client.batch_commit_write_streams(
                batch_commit_write_streams_request
            )
            self.logger.info(
                f"Writes to streams: '{self._tracked_streams}' have been committed."
            )
            self.jobs_running = set()
            self._tracked_streams = []

    def clean_up(self):
        self.commit_streams()

    def pre_state_hook(self):
        self.commit_streams()


BigQueryGcsStagingSink = type(
    "BigQueryGcsStagingSink",
    (BigQueryGcsStagingImpl, BaseBigQuerySink),
    {},
)
BigQueryGcsStagingDenormalizedSink = type(
    "BigQueryGcsStagingDenormalizedSink",
    (BigQueryGcsStagingImpl, BaseBigQuerySinkDenormalized),
    {},
)
BigQueryLegacyStreamingSink = type(
    "BigQueryLegacyStreamingSink",
    (BigQueryLegacyStreamingImpl, BaseBigQuerySink),
    {},
)
BigQueryLegacyStreamingDenormalizedSink = type(
    "BigQueryLegacyStreamingDenormalizedSink",
    (BigQueryLegacyStreamingImpl, BaseBigQuerySinkDenormalized),
    {},
)
BigQueryBatchSink = type(
    "BigQueryBatchSink",
    (BigQueryBatchImpl, BaseBigQuerySink),
    {},
)
BigQueryBatchDenormalizedSink = type(
    "BigQueryBatchDenormalizedSink",
    (BigQueryBatchImpl, BaseBigQuerySinkDenormalized),
    {},
)
