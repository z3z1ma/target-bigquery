"""BigQuery target sink class, which handles writing streams."""
import codecs
import csv
from concurrent.futures import Future, ThreadPoolExecutor, wait
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

DELAY = 1, 10, 1.5
SCHEMA = [
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


class CsvDictWriterWrapper(csv.DictWriter):
    def __init__(self, fh=None, *args, **kwargs) -> None:
        self.fh = fh
        super().__init__(self.fh, *args, **kwargs)


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
        self.jobs_running = []
        self.executor = ThreadPoolExecutor()
        self._dataset_ref = None
        self._table_ref = None

    def _pop_job_from_stack(self, completed: Future):
        for i, job in enumerate(self.jobs_running):
            if id(job) == id(completed):
                self.jobs_running.pop(i)

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
                schema=SCHEMA,
            )
            table.clustering_fields = [
                "_sdc_extracted_at",
                "_sdc_received_at",
                "_sdc_batched_at",
            ]
            table.description = dedent(
                f"""
                This table is loaded via target-bigquery which is a 
                Singer target that uses an unstructured load approach. 
                The originating stream name is `{self.stream_name}`. 
                This table is partitioned by _sdc_batched_at and 
                clustered by related _sdc timestamp fields.
            """
            )
            table.time_partitioning = TimePartitioning(
                type_=TimePartitioningType.DAY, field="_sdc_batched_at"
            )
            self._table_ref = self._client.create_table(table, exists_ok=True)

    @property
    def max_size(self) -> int:
        return self.config.get("batch_size", 10000)

    def _parse_timestamps_in_record(self, *args, **kwargs) -> None:
        pass

    def start_batch(self, context: dict) -> None:
        self._make_target()

    def preprocess_record(self, record: Dict, context: dict) -> dict:
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
        return {"data": orjson.dumps(record).decode("utf-8"), **metadata}

    def clean_up(self):
        wait(self.jobs_running)

    def pre_state_hook(self) -> None:
        pass


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
            self.jobs_running.append(self.append_rows_stream.send(request))
        except:
            # Simplified self-healing
            self._write_client.finalize_write_stream(name=self.write_stream.name)
            self.seed_new_append_stream()
            request.offset = self._total_records_written - self._offset
            self.jobs_running.append(self.append_rows_stream.send(request))

    def commit_streams(self):
        if self.jobs_running:
            self.jobs_running[-1].result()
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
            self.jobs_running = []
            self._tracked_streams = []

    def clean_up(self):
        self.commit_streams()

    def pre_state_hook(self):
        self.commit_streams()


class BigQueryGcsStagingSink(BaseBigQuerySink):
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
            self.config["bucket"],
            self.config.get("prefix_override", "target_bigquery"),
            self.config["dataset"],
            self.stream_name,
        )

    def _get_batch_id(self, context: dict):
        return context["batch_id"]

    @cached(custom_key_maker=_get_batch_id)
    def _get_gcs_write_handle(self, context: dict) -> csv.DictWriter:
        _256kb = int(256 * 1024)
        fh = smart_open.open(
            f"{self._blob_path}/{context['batch_id']}.csv",
            "w",
            transport_params=dict(
                client=self._gcs_client,
                buffer_size=int(
                    _256kb * ((self.config.get("gcs_buffer_size", 0) * 1e6) // _256kb)
                )
                or _256kb,
                min_part_size=int(
                    _256kb * ((self.config.get("gcs_buffer_size", 0) * 1e6) // _256kb)
                )
                or _256kb,
            ),
        )
        return CsvDictWriterWrapper(
            fh=fh,
            fieldnames=(
                "data",
                "_sdc_extracted_at",
                "_sdc_received_at",
                "_sdc_batched_at",
                "_sdc_deleted_at",
                "_sdc_sequence",
                "_sdc_table_version",
            ),
        )

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
        stop=stop_after_attempt(3),
    )
    def _upload_records(self, target_stage: str) -> bigquery.LoadJob:
        job: bigquery.LoadJob = self._client.load_table_from_uri(
            target_stage,
            self._table,
            timeout=self.config["timeout"],
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                ],
                ignore_unknown_values=True,
            ),
        )
        try:
            job.result(
                retry=bigquery.DEFAULT_RETRY.with_delay(*DELAY).with_deadline(
                    self.config["timeout"]
                ),
                timeout=self.config["timeout"],
            )
        except Exception as should_retry:
            self.logger.info("Error during upload: %s", job.errors)
            raise should_retry
        else:
            return job

    def process_record(self, record: dict, context: dict) -> None:
        self._get_gcs_write_handle(context).writerow(record)

    def process_batch(self, context: dict) -> None:
        self._get_gcs_write_handle(context).fh.close()
        job = self.executor.submit(
            self._upload_records,
            target_stage=f"{self._blob_path}/{context['batch_id']}.csv",
        )
        self.jobs_running.append(job)
        job.add_done_callback(self._pop_job_from_stack)


class BigQueryLegacyStreamingSink(BaseBigQuerySink):
    def _generate_batch_row_ids(self):
        return [
            "-".join(str(orjson.loads(row["data"])[k]) for k in self.key_properties)
            for row in self.records_to_drain
            if self.key_properties
        ]

    @retry(
        retry=retry_if_exception_type(ConnectionError)
        | retry_if_result(lambda resp: bool(resp)),
        reraise=True,
        stop=stop_after_attempt(3),
    )
    def _upload_records(self, records_to_drain, insert_ids):
        cached_ref = _http.json
        _http.json = orjson
        errors = self._client.insert_rows_json(
            table=self._table,
            json_rows=records_to_drain,
            timeout=self.config["timeout"],
            row_ids=insert_ids if insert_ids else None,
            retry=bigquery.DEFAULT_RETRY.with_delay(*DELAY).with_deadline(
                self.config["timeout"]
            ),
        )
        _http.json = cached_ref
        return errors

    def process_record(self, record: dict, context: dict) -> None:
        self.records_to_drain.append(record)

    def process_batch(self, context: dict) -> None:
        insert_ids = self._generate_batch_row_ids()
        job = self.executor.submit(
            self._upload_records,
            records_to_drain=self.records_to_drain.copy(),
            insert_ids=insert_ids,
        )
        self.jobs_running.append(job)
        job.add_done_callback(self._pop_job_from_stack)
        self.records_to_drain = []


class BigQueryBatchSink(BaseBigQuerySink):
    @retry(
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
        stop=stop_after_attempt(3),
    )
    def _upload_records(self, buf: BytesIO) -> bigquery.LoadJob:
        job: bigquery.LoadJob = self._client.load_table_from_file(
            buf,
            self._table,
            rewind=True,
            num_retries=3,
            timeout=self.config["timeout"],
            job_config=bigquery.LoadJobConfig(
                schema=SCHEMA,
                source_format=bigquery.SourceFormat.CSV,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                ],
                ignore_unknown_values=True,
            ),
        )
        try:
            job.result(
                retry=bigquery.DEFAULT_RETRY.with_delay(*DELAY).with_deadline(
                    self.config["timeout"]
                ),
                timeout=self.config["timeout"],
            )
        except Exception as should_retry:
            self.logger.info("Error during upload: %s", job.errors)
            raise should_retry
        else:
            return job

    def process_record(self, record: dict, context: dict) -> None:
        if not context.get("records"):
            context["records"] = BytesIO()
            wrapper = codecs.getwriter("utf-8")
            context["writer"] = csv.DictWriter(
                wrapper(context["records"]),
                fieldnames=(
                    "data",
                    "_sdc_extracted_at",
                    "_sdc_received_at",
                    "_sdc_batched_at",
                    "_sdc_deleted_at",
                    "_sdc_sequence",
                    "_sdc_table_version",
                ),
            )
        context["writer"].writerow(record)

    def process_batch(self, context: dict) -> None:
        buf: BytesIO = context["records"]
        job = self.executor.submit(self._upload_records, buf=BytesIO(buf.getvalue()))
        self.jobs_running.append(job)
        job.add_done_callback(self._pop_job_from_stack)
        buf.seek(0), buf.flush()
