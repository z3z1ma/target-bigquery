"""BigQuery target class."""
from typing import Type, Optional

from singer_sdk import typing as th
from singer_sdk.target_base import Sink, Target, List

from target_bigquery.sinks import (
    BigQueryBatchSink,
    BigQueryGcsStagingSink,
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
            required=True,
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
            "add_record_metadata",
            th.BooleanType,
            description="Inject record metadata into the schema.",
            default=True,
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="The maximum number of rows to send in a single batch or commit.",
            default=50,
        ),
        th.Property(
            "threads",
            th.IntegerType,
            description="The number of threads to use for writing to BigQuery.",
            default=8,
        ),
        th.Property(
            "method",
            th.StringType,
            description="The method to use for writing to BigQuery. Accepted values are: batch, stream, gcs",
            default="storage",
        ),
        th.Property(
            "bucket",
            th.StringType,
            description="The GCS bucket to use for staging data. Only used if method is gcs.",
        ),
        th.Property(
            "gcs_buffer_size",
            th.NumberType,
            description="The size of the buffer for GCS stream before flushing. Value in Megabytes.",
            default=2.5,
        ),
    ).to_dict()

    _MAX_RECORD_AGE_IN_MINUTES = 30.0

    @property
    def max_parallelism(self) -> int:
        method = self.config.get("method", "batch")
        if method == "batch":
            return 4
        elif method == "stream":
            return 8
        elif method in ("gcs", "storage"):
            return 16
        else:
            raise ValueError(f"Unknown method: {method}")

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        method = self.config.get("method", "batch")
        if method == "batch":
            return BigQueryBatchSink
        elif method == "stream":
            return BigQueryLegacyStreamingSink
        elif method == "gcs":
            return BigQueryGcsStagingSink
        elif method == "storage":
            return BigQueryStorageWriteSink
        else:
            raise ValueError(f"Unknown method: {method}")

    def _process_schema_message(self, message_dict: dict) -> None:
        self._assert_line_requires(message_dict, requires={"stream", "schema"})
        stream_name = message_dict["stream"]
        schema = message_dict["schema"]
        key_properties = message_dict.get("key_properties", None)
        self.mapper.register_raw_stream_schema(
            stream_name,
            schema,
            key_properties,
        )
        return

    def get_sink(
        self,
        stream_name: str,
        *,
        record: Optional[dict] = None,
        schema: Optional[dict] = None,
        key_properties: Optional[List[str]] = None,
    ) -> Sink:
        return self._sinks_active.get(stream_name, None)
