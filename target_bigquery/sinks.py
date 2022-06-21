"""BigQuery target sink class, which handles writing streams."""
import datetime
import json
import re
from io import BytesIO
from typing import Dict, List, Optional

import orjson
from google.cloud import bigquery
from memoization import cached
from singer_sdk import PluginBase
from singer_sdk.sinks import BatchSink


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
        return "numeric"
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
def get_client(credentials_path: str) -> bigquery.Client:
    """Returns a BigQuery client. Singleton."""
    return bigquery.Client.from_service_account_json(credentials_path)


class BigQuerySink(BatchSink):
    """BigQuery target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._client = get_client(self.config["credentials_path"])
        self._table = (
            f"{self.config['project']}.{self.config['dataset']}.{self.stream_name}"
        )
        self._contains_coerced_field = False
        # BQ Schema resolution sets the coerced_fields property to True if any of the fields are coerced
        self.bigquery_schema = [
            self.jsonschema_prop_to_bq_column(name, prop)
            for name, prop in self.schema["properties"].items()
        ]

    @property
    def max_size(self) -> int:
        return 500

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

    def start_batch(self, context: dict) -> None:
        """Start a batch of records by ensuring the target exists."""
        self._client.create_dataset(self.config["dataset"], exists_ok=True)
        table = self._client.create_table(
            bigquery.Table(
                self._table,
                schema=self.bigquery_schema,
            ),
            exists_ok=True,
        )
        # TODO: If needed, new_schema = original_schema[:]  --> Creates a copy of the schema.
        fields_to_add = []
        original_schema = table.schema
        for field in self.bigquery_schema:
            if not field in table.schema:
                fields_to_add.append(field)
        if fields_to_add:
            self.logger.info("Adding fields to table in stream: %s", self.stream_name)
            new_schema = original_schema[:]
            new_schema.extend(fields_to_add)
            table.schema = new_schema
            table = self._client.update_table(table, ["schema"])

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

    def preprocess_record(self, record: Dict, context: dict) -> dict:
        """Inject metadata if not present and asked for, coerce json paths to strings if needed."""
        if self.config["add_record_metadata"]:
            if "_sdc_extracted_at" not in record:
                record["_sdc_extracted_at"] = datetime.datetime.now().isoformat()
            if "_sdc_received_at" not in record:
                record["_sdc_received_at"] = datetime.datetime.now().isoformat()
        record["_sdc_batched_at"] = (
            context.get("batch_start_time", None) or datetime.datetime.now()
        ).isoformat()
        if self._contains_coerced_field:
            return self._parse_json_with_props(record)
        return record

    def process_record(self, record: dict, context: dict) -> None:
        """Write record to buffer"""
        self.records_to_drain.append(record)

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""

        if self.current_size == 0:
            return
        self.start_drain()

        cached_fn = json.dumps
        json.dumps = orjson.dumps
        errors = self._client.insert_rows_json(
            table=self._table,
            json_rows=self.records_to_drain,
            timeout=self.config["timeout"],
        )
        json.dumps = cached_fn

        if errors == []:
            self.logger.info("New rows have been added.")
        else:
            self.logger.info(
                "Encountered errors while inserting rows: {}".format(errors)
            )

        self.mark_drained()
        self.records_to_drain = []


class BigQueryBufferSink(BigQuerySink):
    """BigQuery target sink class."""

    def process_record(self, record: dict, context: dict) -> None:
        """Write record to buffer"""
        if not context.get("records"):
            context["records"] = BytesIO()
        context["records"].write(
            orjson.dumps(
                record,
                option=orjson.OPT_APPEND_NEWLINE,
            )
        )

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written."""

        if self.current_size == 0:
            return
        self.start_drain()

        # Get the reference to the buffer
        data: BytesIO = context["records"]

        # Load the data into BigQuery
        job = self._client.load_table_from_file(
            data,
            self._table,
            rewind=True,
            num_retries=3,
            timeout=self.config["timeout"],
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema_update_options=[
                    # bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                ],
                ignore_unknown_values=True,
            ),
        )

        # Wait for the load job to complete.
        self.logger.info(f"Job created for {self.stream_name}, awaiting results")
        r = job.result()
        self.logger.info(
            f"{r.job_id}: Loaded {self.current_size} rows into {self.stream_name}"
        )

        # Flush the buffer
        data.seek(0)
        data.flush()
        self.mark_drained()
