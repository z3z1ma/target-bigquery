import re
from typing import List, Optional

import orjson
from google.cloud import bigquery


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
        return "float"
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


class SchemaTranslator:
    def __init__(self, schema):
        self.schema = schema
        self._contains_coerced_field = False
        ...

    def jsonschema_prop_to_bq_column(
        self, name: str, schema_property: dict
    ) -> bigquery.SchemaField:
        """Translates a json schema property to a BigQuery schema property."""
        safe_name = safe_column_name(name, quotes=False)
        property_type = schema_property.get("type", "string")
        property_format = schema_property.get("format", None)
        if "array" in property_type:
            try:
                items_schema: dict = schema_property["items"]
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

    def _parse_json_with_props(self, record: dict, _prop_spec: Optional[dict] = None):
        """Recursively serialize a JSON object which has props that were forcibly coerced
        to string do to lack of prop definitons"""
        if _prop_spec is None:
            _prop_spec: dict = self.schema["properties"]
        for name, contents in record.items():
            prop_spec: dict = _prop_spec.get(name)
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
