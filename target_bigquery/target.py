"""BigQuery target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_bigquery.sinks import BigQuerySink


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
            "project",
            th.StringType,
            description="The target GCP project to materialize data into.",
        ),
        th.Property(
            "dataset",
            th.StringType,
            description="The target dataset to materialize data into.",
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            description="Inject record metadata into the schema.",
            default=True,
        ),
    ).to_dict()
    default_sink_class = BigQuerySink
