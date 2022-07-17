"""BigQuery target class."""
import copy
from typing import Type

from singer_sdk import typing as th
from singer_sdk.target_base import Sink, Target

from target_bigquery.sinks import (
    BigQueryBatchSink,
    BigQueryGcsStagingSink,
    BigQueryStreamingSink,
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
            "batch_size_limit",
            th.IntegerType,
            description="The maximum number of rows to send in a single batch.",
            default=10000,
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
            default="batch",
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
        elif method == "gcs":
            return 16
        else:
            raise ValueError(f"Unknown method: {method}")

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        method = self.config.get("method", "batch")
        if method == "batch":
            return BigQueryBatchSink
        elif method == "stream":
            return BigQueryStreamingSink
        elif method == "gcs":
            return BigQueryGcsStagingSink
        else:
            raise ValueError(f"Unknown method: {method}")

    # Temporary method overrides until we merge the hook into the SDK

    def _process_endofpipe(self) -> None:
        """Called after all input lines have been read."""
        self.drain_all(is_endofpipe=True)

    def drain_all(self, is_endofpipe: bool = False) -> None:
        """Drains all sinks, starting with those cleared due to changed schema.

        This method is internal to the SDK and should not need to be overridden.
        """
        state = copy.deepcopy(self._latest_state)
        self._drain_all(self._sinks_to_clear, 1)
        if is_endofpipe:
            (sink.clean_up() for sink in self._sinks_active.values())
        self._sinks_to_clear = []
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        if is_endofpipe:
            (sink.clean_up() for sink in self._sinks_to_clear)
        self._write_state_message(state)
        self._reset_max_record_age()
