"""BigQuery target sink class, which handles writing streams."""
import re
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from io import BytesIO, FileIO
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

# pylint: disable=no-else-return,too-many-branches,too-many-return-statements
def bigquery_type(property_type: List[str], property_format: str) -> str:
    """Translate jsonschema type to bigquery type."""
    if property_format == "date-time":
        return "timestamp"
    if property_format == "date":
        return "date"
    elif property_format == "time":
        return "time"
    elif "number" in property_type:
        return "float64"
    elif "integer" in property_type and "string" in property_type:
        return "string"
    elif "integer" in property_type:
        return "integer"
    elif "boolean" in property_type:
        return "boolean"
    elif "object" in property_type:
        return "record"
    else:
        return "string"


def safe_column_name(name: str, quotes: bool = False) -> str:
    """Returns a safe column name for BigQuery."""
    name = name.replace("`", "")
    pattern = "[^a-zA-Z0-9_]"
    name = re.sub(pattern, "_", name)
    if quotes:
        return "`{}`".format(name).lower()
    return "{}".format(name).lower()


@cached
def get_bq_client(credentials_path: str) -> bigquery.Client:
    """Returns a BigQuery client. Singleton."""
    return bigquery.Client.from_service_account_json(credentials_path)


@cached
def get_gcs_client(credentials_path: str) -> storage.Client:
    """Returns a BigQuery client. Singleton."""
    return storage.Client.from_service_account_json(credentials_path)


class BaseBigQuerySink(BatchSink):
    """Base BigQuery target sink class."""

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

        # Flag if field was coerced
        self._contains_coerced_field = False

        # BQ Schema resolution sets the _contains_coerced_field property to True if any of the fields are coerced
        self.bigquery_schema = [
            self.jsonschema_prop_to_bq_column(name, prop)
            for name, prop in self.schema["properties"].items()
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
            self._evolve_schema()

    @property
    def max_size(self) -> int:
        return self.config.get("batch_size_limit", 15000)

    def jsonschema_prop_to_bq_column(
        self, name: str, schema_property: dict
    ) -> bigquery.SchemaField:
        """Translates a json schema property to a BigQuery schema property."""
        safe_name = safe_column_name(name, quotes=False)
        property_type = schema_property.get("type", "string")
        property_format = schema_property.get("format", None)
        if "array" in property_type:
            try:
                items_schema = schema_property["items"]
                items_type = bigquery_type(
                    items_schema["type"], items_schema.get("format", None)
                )
            except KeyError:
                # Coerced to string because there was no `items` property, TODO: Add strict mode
                self._contains_coerced_field = True
                return bigquery.SchemaField(safe_name, "string", "NULLABLE")
            else:
                if items_type == "record":
                    return self._translate_record_to_bq_schema(
                        safe_name, items_schema, "REPEATED"
                    )
                return bigquery.SchemaField(safe_name, items_type, "REPEATED")
        elif "object" in property_type:
            return self._translate_record_to_bq_schema(safe_name, schema_property)
        else:
            result_type = bigquery_type(property_type, property_format)
            return bigquery.SchemaField(safe_name, result_type, "NULLABLE")

    def _translate_record_to_bq_schema(
        self, safe_name, schema_property, mode="NULLABLE"
    ) -> bigquery.SchemaField:
        """Recursive descent in record objects."""
        fields = [
            self.jsonschema_prop_to_bq_column(col, t)
            for col, t in schema_property.get("properties", {}).items()
        ]
        if fields:
            return bigquery.SchemaField(safe_name, "RECORD", mode, fields=fields)
        else:
            # Coerced to string because there was no populated `properties` property, TODO: Add strict mode
            self._contains_coerced_field = True
            return bigquery.SchemaField(safe_name, "string", mode)

    def _parse_json_with_props(self, record: dict, _prop_spec=None):
        """Recursively serialize a JSON object which has props that were forcibly coerced
        to string do to lack of prop definitons"""
        if _prop_spec is None:
            _prop_spec = self.schema["properties"]
        for name, contents in record.items():
            prop_spec = _prop_spec.get(name)
            if prop_spec:
                if (
                    "object" in prop_spec["type"]
                    and isinstance(contents, dict)
                    and not prop_spec.get("properties")
                ):
                    # If the prop_spec has no properties, it means it was coerced to a string
                    record[name] = orjson.dumps(record[name]).decode("utf-8")
                elif "object" in prop_spec["type"] and isinstance(contents, dict):
                    self._parse_json_with_props(contents, prop_spec["properties"])
                elif "array" in prop_spec["type"] and isinstance(contents, list):
                    if prop_spec.get("items"):
                        [
                            self._parse_json_with_props(item, prop_spec["items"])
                            for item in contents
                        ]
                    else:
                        record[name] = orjson.dumps(record[name]).decode("utf-8")
        return record

    def _evolve_schema(self):
        """Schema Evolution
        TODO: feature flag on casting"""
        original_schema = self._table_ref.schema[:]
        mutable_schema = self._table_ref.schema[:]
        for expected_field in self.bigquery_schema:
            if expected_field not in original_schema:
                for mut_field in mutable_schema:
                    if mut_field.name == expected_field.name:
                        # This can throw if uncastable change in schema
                        # It works for basic mutations
                        ddl = f"ALTER TABLE `{self._table_ref.dataset_id}`.`{self._table_ref.table_id}` \
                        ALTER COLUMN `{mut_field.name}` SET DATA TYPE {expected_field.field_type}"
                        self.logger.info("Schema change detected, dispatching: %s", ddl)
                        self._client.query(ddl).result()
                        break
                else:
                    # This is easy with PATCH
                    mutable_schema.append(expected_field)
        if len(mutable_schema) > len(original_schema):
            self._table_ref.schema = mutable_schema
            self._client.update_table(
                self._table_ref,
                ["schema"],
                retry=bigquery.DEFAULT_RETRY.with_delay(*DELAY).with_deadline(
                    self.config["timeout"]
                ),
                timeout=self.config["timeout"],
            )

    def start_batch(self, context: dict) -> None:
        """Ensure target exists, noop once verified. Throttle jobs"""
        self._make_target()
        while len(self.jobs_running) >= self.config["threads"]:
            self.logger.info("Throttling job queue...")
            time.sleep(2)

    def preprocess_record(self, record: Dict, context: dict) -> dict:
        """Inject metadata if not present and asked for, coerce json paths to strings if needed."""
        if self._contains_coerced_field:
            return self._parse_json_with_props(record)
        return record

    def clean_up(self):
        """Ensure all jobs are completed"""
        self.logger.info(f"Awaiting jobs: {len(self.jobs_running)}")
        for _ in as_completed(self.jobs_running):
            ...
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
            "-".join(str(row[k]) for k in self.key_properties)
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
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
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
        except Exception as exc:
            self.logger.info("Error during upload: %s", job.errors)
            raise exc  # Here, tenacity takes the wheel if needed
        else:
            return job

    def process_record(self, record: dict, context: dict) -> None:
        """Write record to buffer"""
        if not context.get("records"):
            context["records"] = BytesIO()
        context["records"].write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))

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
    def _get_gcs_write_handle(self, context: dict) -> FileIO:
        """Opens a stream for writing to the target cloud object,
        Singleton handle per batch"""
        _256kb = int(256 * 1024)
        return smart_open.open(
            f"{self._blob_path}/{context['batch_id']}.jsonl",
            "wb",
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
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
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
        except Exception as exc:
            self.logger.info("Error during upload: %s", job.errors)
            raise exc  # Here, tenacity takes the wheel if needed
        else:
            return job

    def process_record(self, record: dict, context: dict) -> None:
        """Write record to buffer"""
        self._get_gcs_write_handle(context).write(
            orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
        )

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""
        self._get_gcs_write_handle(context).close()
        job = self.executor.submit(
            self._upload_records,
            target_stage=f"{self._blob_path}/{context['batch_id']}.jsonl",
        )
        self.jobs_running.append(job)
        job.add_done_callback(self._pop_job_from_stack)
