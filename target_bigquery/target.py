"""BigQuery target class."""
import copy
from typing import List, Optional, Type

from singer_sdk import typing as th
from singer_sdk.target_base import Sink, Target

from target_bigquery.sinks import (
    BigQueryBatchDenormalizedSink,
    BigQueryBatchSink,
    BigQueryGcsStagingDenormalizedSink,
    BigQueryGcsStagingSink,
    BigQueryLegacyStreamingDenormalizedSink,
    BigQueryLegacyStreamingSink,
    BigQueryStorageWriteSink,
)


class TargetBigQuery(Target):
    """Target for BigQuery."""

    name = "target-bigquery"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "credentials_path",
            th.StringType,
            description="The path to a gcp credentials json file.",
        ),
        th.Property(
            "credentials_json",
            th.StringType,
            description="A JSON string of your service account JSON file.",
        ),
        th.Property(
            "project",
            th.StringType,
            description="The target GCP project to materialize data into.",
            required=True,
        ),
        th.Property(
            "dataset",
            th.StringType,
            description="The target dataset to materialize data into.",
            required=True,
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="The maximum number of rows to send in a single batch or commit.",
            default=250000,
        ),
        th.Property(
            "timeout",
            th.IntegerType,
            description="Default timeout for batch_job and gcs_stage derived LoadJobs.",
            default=600,
        ),
        th.Property(
            "denormalized",
            th.BooleanType,
            description="Determines whether to denormalize the data before writing to BigQuery. A false value "
            "will write data using a fixed JSON column based schema, while a true value will write data using a dynamic "
            "schema derived from the tap. Denormalization is only supported for the batch_job, streaming_insert, and gcs_stage methods.",
            default=False,
        ),
        th.Property(
            "method",
            th.CustomType(
                {
                    "type": "string",
                    "enum": [
                        "storage_write_api",
                        "batch_job",
                        "gcs_stage",
                        "streaming_insert",
                    ],
                }
            ),
            description="The method to use for writing to BigQuery.",
            default="batch_job",
            required=True,
        ),
        th.Property(
            "append_columns",
            th.BooleanType,
            description="In the case of a denormalize sync, whether to append new columns to existing schema",
            default=True,
            required=False,
        ),
        th.Property(
            "generate_view",
            th.BooleanType,
            description="Determines whether to generate a view based on the SCHEMA message parsed from the tap. "
            "Only valid if denormalized=false meaning you are using the fixed JSON column based schema.",
            default=False,
        ),
        th.Property(
            "gcs_bucket",
            th.StringType,
            description="The GCS bucket to use for staging data. Only used if method is gcs_stage.",
        ),
        th.Property(
            "gcs_buffer_size",
            th.NumberType,
            description="The size of the buffer for GCS stream before flushing a multipart upload chunk. Value in megabytes. "
            "Only used if method is gcs_stage. This eager flushing in conjunction with zlib results in very low memory usage.",
            default=15,
        ),
        th.Property(
            "gcs_max_file_size",
            th.NumberType,
            description="The maximum file size in megabytes for a bucket file. This is used as the batch indicator "
            "for GCS based ingestion. Only used if method is gcs_stage.",
            default=250,
        ),
        th.Property(
            "column_name_transforms",
            th.ObjectType(
                th.Property(
                    "lower",
                    th.BooleanType,
                    default=False,
                    description="Lowercase column names",
                ),
                th.Property(
                    "quote",
                    th.BooleanType,
                    default=False,
                    description="Quote columns during DDL generation",
                ),
                th.Property(
                    "add_underscore_when_invalid",
                    th.BooleanType,
                    default=False,
                    description="Add an underscore when a column starts with a digit",
                ),
                th.Property(
                    "snake_case",
                    th.BooleanType,
                    default=False,
                    description="Convert columns to snake case",
                ),
            ),
            description=(
                "Accepts a JSON object of options with boolean values to enable them. The available options are `quote` "
                "(quote columns in DDL), `lower` (lowercase column names), `add_underscore_when_invalid` (add underscore "
                "if column starts with digit), and `snake_case` (convert to snake case naming). For fixed schema, this "
                "transform only applies to the generated view if enabled."
            ),
            required=False,
        ),
    ).to_dict()

    @property
    def max_parallelism(self) -> int:
        method = self.config.get("method", "batch")
        if method in ("batch_job",):
            return 4
        elif method in ("streaming_insert",):
            return 8
        elif method in ("gcs_stage", "storage_write_api"):
            return 12
        raise ValueError(f"Unknown method: {method}")

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        method = self.config.get("method", "batch_job")
        denormalized = self.config.get("denormalized", False)
        if method == "storage_write_api" and denormalized:
            raise RuntimeError(
                f"Cannot use denormalized=true with method=storage_write_api"
            )
        if method == "batch_job":
            if denormalized:
                return BigQueryBatchDenormalizedSink
            return BigQueryBatchSink
        elif method == "streaming_insert":
            if denormalized:
                return BigQueryLegacyStreamingDenormalizedSink
            return BigQueryLegacyStreamingSink
        elif method == "gcs_stage":
            if denormalized:
                return BigQueryGcsStagingDenormalizedSink
            return BigQueryGcsStagingSink
        elif method == "storage_write_api":
            return BigQueryStorageWriteSink
        raise ValueError(f"Unknown method: {method}")

    def get_sink(
        self,
        stream_name: str,
        *,
        record: Optional[dict] = None,
        schema: Optional[dict] = None,
        key_properties: Optional[List[str]] = None,
    ) -> Sink:
        if schema is None:
            self._assert_sink_exists(stream_name)
            return self._sinks_active[stream_name]
        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sink(stream_name, schema, key_properties)
        return existing_sink

    def drain_all(self, is_endofpipe: bool = False) -> None:  # type: ignore
        state = copy.deepcopy(self._latest_state)
        self._drain_all(self._sinks_to_clear, 1)
        for sink in self._sinks_to_clear:
            if is_endofpipe:
                sink.clean_up()
            else:
                sink.pre_state_hook()
        self._sinks_to_clear = []
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        for sink in self._sinks_active.values():
            if is_endofpipe:
                sink.clean_up()
            else:
                sink.pre_state_hook()
        self._write_state_message(state)
        self._reset_max_record_age()
