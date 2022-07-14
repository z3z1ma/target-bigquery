"""BigQuery target sink class, which handles writing streams."""
import csv
import codecs
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from io import BytesIO
from typing import Dict, List, Optional

import orjson
import smart_open
from google.cloud import _http, bigquery, storage
from memoization import cached
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink
from tenacity import retry, retry_if_result
from tenacity.retry import retry_if_exception_type
from tenacity.stop import stop_after_attempt

DELAY = 1, 10, 1.5


@cached
def get_bq_client(credentials_path: str) -> bigquery.Client:
    """Returns a BigQuery client. Singleton."""
    return bigquery.Client.from_service_account_json(credentials_path)


@cached
def get_gcs_client(credentials_path: str) -> storage.Client:
    """Returns a BigQuery client. Singleton."""
    return storage.Client.from_service_account_json(credentials_path)


class CsvDictWriterWrapper(csv.DictWriter):
    def __init__(self, fh=None, *args, **kwargs) -> None:
        self.fh = fh
        super().__init__(self.fh, *args, **kwargs)


class BaseBigQuerySink(BatchSink):
    """Base BigQuery target sink class."""

    include_sdc_metadata_properties = True

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        # Client and Table ID
        self._client = get_bq_client(self.config["credentials_path"])
        self._table = f"{self.config['project']}.{self.config['dataset']}.{self.stream_name.lower()}"

        # Because we set the schema upfront, lets be opinionated on Singer Data Capture fields being added by default
        self.bigquery_schema = [
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

        # Track Jobs
        self.bq_jobs = set()
        self.jobs_running = []
        self.executor = ThreadPoolExecutor()

        # Refs & Build Destination Assets (Lazy)
        self._dataset_ref = None
        self._table_ref = None

    def _pop_job_from_stack(self, completed: Future):
        for i, job in enumerate(self.jobs_running):
            if id(job) == id(completed):
                self.jobs_running.pop(i)

    def _pop_bq_job_from_queue(self, fut: bigquery.LoadJob):
        self.bq_jobs.discard(fut.job_id)

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
        stop=stop_after_attempt(2),
    )
    def _make_target(self) -> None:
        if self._dataset_ref is None:
            self._dataset_ref = self._client.create_dataset(
                self.config["dataset"], exists_ok=True
            )
        if self._table_ref is None:
            self._table_ref = self._client.create_table(
                bigquery.Table(
                    self._table,
                    schema=self.bigquery_schema,
                ),
                exists_ok=True,
            )

    @property
    def max_size(self) -> int:
        return self.config.get("batch_size_limit", 15000)

    def start_batch(self, context: dict) -> None:
        """Ensure target exists, noop once verified. Throttle jobs"""
        self._make_target()
        while len(self.jobs_running) >= self.config["threads"]:
            self.logger.info("Throttling job queue...")
            time.sleep(2)

    def preprocess_record(self, record: Dict, context: dict) -> dict:
        """Wrap object in data key to standardize output."""
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
        """Ensure all jobs are completed"""
        self.logger.info(f"Awaiting jobs: {len(self.jobs_running)}")
        wait(self.jobs_running)
        if len(self.bq_jobs) > 0:
            raise RuntimeError(
                "BigQuery jobs not marked as completed after threaded execution end."
            )


class BigQueryStreamingSink(BaseBigQuerySink):
    """BigQuery Streaming target sink class.

    Traits: More expensive API, very fast for smaller syncs, recommend lower batch size (50-100)

    API -> IN MEM JSON STR -> BIGQUERY GCS STREAM JOB -> FLUSH"""

    def _generate_batch_row_ids(self):
        """Generate row ids if key properties is supplied"""
        return [
            "-".join(str(orjson.loads(row["data"])[k]) for k in self.key_properties)
            for row in self.records_to_drain
            if self.key_properties
        ]

    @retry(
        retry=retry_if_exception_type(ConnectionError)
        | retry_if_result(lambda r: bool(r)),
        reraise=True,
        stop=stop_after_attempt(5),
    )
    def _upload_records(self, records_to_drain, insert_ids):
        """Runs in thread blocking on job"""
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
        """Write record to buffer"""
        self.records_to_drain.append(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
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
    """BigQuery Batch target sink class.

    Traits: High footprint in memory dependent on batch_size, very fast since we dispatch byte buffers
    to bigquery in a separate thread

    API -> IN MEM JSON BYTES -> BIGQUERY -> FLUSH
    """

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
        stop=stop_after_attempt(5),
    )
    def _upload_records(self, buf: BytesIO) -> bigquery.LoadJob:
        """Runs in thread blocking on job"""
        job: bigquery.LoadJob = self._client.load_table_from_file(
            buf,
            self._table,
            rewind=True,
            num_retries=3,
            timeout=self.config["timeout"],
            job_config=bigquery.LoadJobConfig(
                schema=self.bigquery_schema,
                source_format=bigquery.SourceFormat.CSV,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                ],
                ignore_unknown_values=True,
            ),
        )
        self.bq_jobs.add(job.job_id)
        job.add_done_callback(self._pop_bq_job_from_queue)
        try:
            job.result(
                retry=bigquery.DEFAULT_RETRY.with_delay(*DELAY).with_deadline(
                    self.config["timeout"]
                ),
                timeout=self.config["timeout"],
            )
        except Exception as should_retry:
            self.logger.info("Error during upload: %s", job.errors)
            # On raise, tenacity will retry as defined in decorator
            raise should_retry
        else:
            return job

    def process_record(self, record: dict, context: dict) -> None:
        """Write record to buffer"""
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
        """Write out any prepped records and return once fully written."""
        buf: BytesIO = context["records"]
        job = self.executor.submit(self._upload_records, buf=BytesIO(buf.getvalue()))
        self.jobs_running.append(job)
        job.add_done_callback(self._pop_job_from_stack)
        buf.seek(0), buf.flush()


class BigQueryGcsStagingSink(BaseBigQuerySink):
    """BigQuery GCS target sink class.

    Traits: Lowest footprint in memory, rivals Batch method in speed, persistent
    intermediate storage in data lake

    API -> GCS JSONL -> GC FLUSH -> BIGQUERY GCS LOAD JOB
    """

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._gcs_client = get_gcs_client(self.config["credentials_path"])
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
        """Opens a stream for writing to the target cloud object,
        Singleton handle per batch"""
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
        stop=stop_after_attempt(5),
    )
    def _upload_records(self, target_stage: str) -> bigquery.LoadJob:
        """Runs in thread blocking on job"""
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
        self.bq_jobs.add(job.job_id)
        job.add_done_callback(self._pop_bq_job_from_queue)
        try:
            job.result(
                retry=bigquery.DEFAULT_RETRY.with_delay(*DELAY).with_deadline(
                    self.config["timeout"]
                ),
                timeout=self.config["timeout"],
            )
        except Exception as should_retry:
            self.logger.info("Error during upload: %s", job.errors)
            # On raise, tenacity will retry as defined in decorator
            raise should_retry
        else:
            return job

    def process_record(self, record: dict, context: dict) -> None:
        """Write record to buffer"""
        self._get_gcs_write_handle(context).writerow(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        self._get_gcs_write_handle(context).fh.close()
        job = self.executor.submit(
            self._upload_records,
            target_stage=f"{self._blob_path}/{context['batch_id']}.csv",
        )
        self.jobs_running.append(job)
        job.add_done_callback(self._pop_job_from_stack)
