"""BigQuery target class."""
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
    ).to_dict()

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
