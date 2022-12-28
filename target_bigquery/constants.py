from google.cloud import bigquery

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

SDC_FIELDS = [
    "_sdc_extracted_at",
    "_sdc_received_at",
    "_sdc_batched_at",
    "_sdc_deleted_at",
    "_sdc_sequence",
    "_sdc_table_version",
]

DEFAULT_BUCKET_PATH = "gs://{bucket}/target_bigquery/{dataset}/{table}/extracted_date={date}/{batch_id}.jsonl.gz"
