import re
from textwrap import dedent, indent
from typing import List, Optional

from google.cloud.bigquery import SchemaField


# pylint: disable=no-else-return,too-many-branches,too-many-return-statements
def bigquery_type(
    property_type: List[str], property_format: Optional[str] = None
) -> str:
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


# TODO: We should try to avoid mutating the column name as it entails
# business logic employed outside user control which makes them reliant
# on other load methods performing the same transformation. With that said,
# it may be necessary when columns would otherwise break a load job.
def safe_column_name(
    name: str,
    quote: bool = False,
    lower: bool = False,
    add_underscore_when_invalid: bool = False,
    snake_case: bool = False,
) -> str:
    if snake_case and not lower:
        lower = True
    was_quoted = name.startswith("`") and name.endswith("`")
    name = name.strip("`")
    if snake_case:
        name = re.sub("((?!^)(?<!_)[A-Z][a-z]+|(?<=[a-z0-9])[A-Z])", r"_\1", name)
    if lower:
        name = "{}".format(name).lower()
    if add_underscore_when_invalid:
        if name[0].isdigit():
            name = "_{}".format(name)
    if quote or was_quoted:
        name = "`{}`".format(name)
    return name


# This class translates a JSON schema into a BigQuery schema.
# It also uses the translated schema to generate a CREATE VIEW statement.
class SchemaTranslator:
    def __init__(self, schema, transforms):
        self.schema = schema
        self.transforms = transforms
        # Used by fixed schema strategy where we defer transformation
        # to the view statement
        self.translated_schema = [
            self._jsonschema_prop_to_bq_column(name, contents)
            for name, contents in self.schema.get("properties", {}).items()
        ]
        # Used by the denormalized strategy where we eagerly transform
        # the target schema
        self.translated_schema_transformed = [
            self._jsonschema_prop_to_bq_column(
                safe_column_name(name, **transforms), contents
            )
            for name, contents in self.schema.get("properties", {}).items()
        ]

    def translate_record(self, record: dict) -> dict:
        if not self.transforms:
            return record
        output = dict(
            [
                (safe_column_name(k, **{**self.transforms, "quote": False}), v)
                for k, v in record.items()
            ]
        )
        for k, v in output.items():
            if isinstance(v, list):
                for i, inner in enumerate(v):
                    if isinstance(inner, dict):
                        output[k][i] = self.translate_record(inner)
            if isinstance(v, dict):
                output[k] = self.translate_record(v)
        return output

    def _jsonschema_prop_to_bq_column(
        self, name: str, schema_property: dict
    ) -> SchemaField:
        if "anyOf" in schema_property and len(schema_property["anyOf"]) > 0:
            # I have only seen this used in the wild with tap-salesforce, which
            # is incidentally an important one so lets handle the anyOf case
            # by giving the 0th index priority.
            property_type = schema_property["anyOf"][0].get("type", "string")
            property_format = schema_property["anyOf"][0].get("format", None)
        else:
            property_type = schema_property.get("type", "string")
            property_format = schema_property.get("format", None)

        if "array" in property_type:
            items_schema: dict = schema_property["items"]
            items_type = bigquery_type(
                items_schema["type"], items_schema.get("format", None)
            )
            if items_type == "record":
                return self._translate_record_to_bq_schema(
                    name, items_schema, "REPEATED"
                )
            return SchemaField(name, items_type, "REPEATED")
        elif "object" in property_type:
            return self._translate_record_to_bq_schema(name, schema_property)
        else:
            result_type = bigquery_type(property_type, property_format)
            return SchemaField(name, result_type, "NULLABLE")

    def _translate_record_to_bq_schema(
        self, name: str, schema_property: dict, mode: str = "NULLABLE"
    ) -> SchemaField:
        fields = [
            self._jsonschema_prop_to_bq_column(col, t)
            for col, t in schema_property.get("properties", {}).items()
        ]
        return SchemaField(name, "RECORD", mode, fields=fields)

    def make_view_stmt(self, table_name: str) -> str:
        """Generate a CREATE VIEW statement for the SchemaTranslator `schema`."""

        projection = ""
        for field in self.translated_schema[:]:
            if field.mode == "REPEATED":
                projection += indent(
                    self._wrap_repeat_json_extract(field, path="$", depth=1), " " * 4
                )
            else:
                projection += indent(self._field_to_sql(field), " " * 4)

        return f"CREATE OR REPLACE VIEW {table_name}_view AS \nSELECT \n{projection} FROM {table_name}"

    def _field_to_sql(
        self,
        field: SchemaField,
        path: str = "$",
        depth: int = 0,
        base: str = "data",
        rebase: bool = False,
    ) -> str:
        # Pass-through _sdc columns into the projection as-is
        if field.name.startswith("_sdc_"):
            return f"{field.name},\n"
        scalar = f"{base}.{field.name}" if not rebase else f"{base}"

        # Records are handled recursively
        if field.field_type.upper() == "RECORD":
            return (
                (" " * depth * 2)
                + "STRUCT(\n{}\n".format(
                    "".join(
                        [
                            self._field_to_sql(f, f"{path}.{field.name}", depth + 1)
                            if not f.mode == "REPEATED"
                            else self._wrap_repeat_json_extract(
                                f, f"{path}.{field.name}", depth, base
                            )
                            for f in field.fields
                        ]
                    ).rstrip(",\n"),
                )
                + (" " * depth * 2)
                + f") as {safe_column_name(field.name, **self.transforms)},\n"
            )
        # Nullable fields require a JSON_VALUE call which creates a 2-stage cast
        elif field.is_nullable:
            return (" " * depth * 2) + self._wrap_json_value(
                field, f"{path}.{field.name}", base
            )
        # These are not nullable so if the type is known, we can do a 1-stage extract & cast
        elif field.field_type.upper() == "STRING":
            return (
                (" " * depth * 2)
                + f"STRING({scalar}) as {safe_column_name(field.name, **self.transforms)},\n"
            )
        elif field.field_type.upper() == "INTEGER":
            return (
                (" " * depth * 2)
                + f"INT64({scalar}) as {safe_column_name(field.name, **self.transforms)},\n"
            )
        elif field.field_type.upper() == "FLOAT":
            return (
                (" " * depth * 2)
                + f"FLOAT64({scalar}) as {safe_column_name(field.name, **self.transforms)},\n"
            )
        elif field.field_type.upper() == "BOOLEAN":
            return (
                (" " * depth * 2)
                + f"BOOL({scalar}) as {safe_column_name(field.name, **self.transforms)},\n"
            )
        # Fallback to a 2-stage extract & cast
        else:
            return (" " * depth * 2) + self._wrap_json_value(
                field, f"{path}.{field.name}" if not rebase else "$", base
            )

    def _wrap_repeat_json_extract(
        self, field: SchemaField, path: str, depth: int = 0, base: str = "data"
    ) -> str:
        v = self._field_to_sql(
            field, "$", depth, f"{field.name}__rows", rebase=True
        ).rstrip(", \n")
        return (" " * depth * 2) + indent(
            dedent(
                f"""
        ARRAY(
            SELECT {v}
            FROM UNNEST(
                JSON_QUERY_ARRAY({base}, '{path}.{field.name}')
            ) AS {field.name}__rows
        """
                + (" " * depth * 2)
                + f") AS {field.name},\n"
            ).lstrip(),
            " " * depth * 2,
        )

    def _wrap_json_value(
        self, field: SchemaField, path: str = "$", base: str = "data"
    ) -> str:
        typ = field.field_type.upper()
        if typ == "STRING":
            return f"JSON_VALUE({base}, '{path}') as {safe_column_name(field.name, **self.transforms)},\n"
        if typ == "FLOAT":
            typ = "FLOAT64"
        if typ in ("INT", "INTEGER"):
            typ = "INT64"
        return f"CAST(JSON_VALUE({base}, '{path}') as {typ}) as {safe_column_name(field.name, **self.transforms)},\n"


# This can throw if uncastable change in schema
# It works for basic mutations. Left for posterity,
# I am dubious of the value of this feature
# hence why I chose not to include it; but it does work as-is.
def __mutate_column(self, mut_field, expected_field):  # type: ignore
    if (
        mut_field.field_type.upper() != expected_field.field_type.upper()
        and self.config["cast_columns"]
    ):
        self.logger.debug(
            f"Detected Diff {mut_field.name=} -> {expected_field.name=} \
            {mut_field.field_type=} -> {expected_field.field_type=}"
        )
        ddl = f"ALTER TABLE `{self._table_ref.dataset_id}`.`{self._table_ref.table_id}` \
        ALTER COLUMN `{mut_field.name}` SET DATA TYPE {expected_field.field_type};"
        self.logger.info("Schema change detected, dispatching: %s", ddl)
        self._client.query(ddl).result()
        mut_field._properties["type"] = expected_field.field_type


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="Convert a JSON schema to a BigQuery view statement based on the "
        "assumption all data is available in a single JSON column named `data`."
    )
    parser.add_argument(
        "--schema",
        type=str,
        help="Path to JSON Schema file",
        required=True,
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Table name to use in view statement",
        required=True,
    )
    args = parser.parse_args()

    with open(args.schema, "r") as f:
        schema = json.load(f)
    translator = SchemaTranslator(schema)
    ddl = translator.make_view_stmt(args.table)
    print(ddl)
