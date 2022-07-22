"""BigQuery target class."""
import copy
from typing import Optional, Type

from singer_sdk import typing as th
from singer_sdk.target_base import List, Sink, Target

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
            default=50,
        ),
        th.Property(
            "threads",
            th.IntegerType,
            description="The number of threads per sink to use for writing to BigQuery. Not used with \
                `storage` sink. Threads are lightwieght and typically just dispatch requests and poll \
                for completion retrying as needed.",
            default=8,
        ),
        th.Property(
            "method",
            th.StringType,
            description="The method to use for writing to BigQuery. Accepted values are: storage, batch, stream, gcs",
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
            description="The size of the buffer for GCS stream before flushing. Value in megabytes. Only used if method is gcs.",
            default=2.5,
        ),
    ).to_dict()

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
