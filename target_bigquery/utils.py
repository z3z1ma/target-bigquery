import re
from textwrap import dedent
from typing import List

from google.cloud import bigquery


# pylint: disable=no-else-return,too-many-branches,too-many-return-statements
def bigquery_type(property_type: List[str], property_format: str) -> str:
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
    name = name.replace("`", "")
    pattern = "[^a-zA-Z0-9_]"
    name = re.sub(pattern, "_", name)
    if quotes:
        return "`{}`".format(name).lower()
    return "{}".format(name).lower()


class SchemaTranslator:
    def __init__(self, schema):
        self.schema = schema
        self.translated_schema = [
            self.jsonschema_prop_to_bq_column(name, contents)
            for name, contents in self.schema.get("properties", {}).items()
        ]

    def jsonschema_prop_to_bq_column(
        self, name: str, schema_property: dict
    ) -> bigquery.SchemaField:
        # Don't munge names, much more portable the less business logic we apply
        # safe_name = safe_column_name(name, quotes=False)

        safe_name = name

        property_type = schema_property.get("type", "string")
        property_format = schema_property.get("format", None)
        if "array" in property_type:
            items_schema: dict = schema_property["items"]
            items_type = bigquery_type(
                items_schema["type"], items_schema.get("format", None)
            )
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
        fields = [
            self.jsonschema_prop_to_bq_column(col, t)
            for col, t in schema_property.get("properties", {}).items()
        ]
        return bigquery.SchemaField(safe_name, "RECORD", mode, fields=fields)

    def make_view_sql(self, table_name: str) -> str:
        projection = ""
        for field in self.translated_schema[:]:
            if field.mode == "REPEATED":
                projection += self._wrap_repeat_json_extract(field)
            else:
                projection += self._field_to_sql(field)
        return f"CREATE OR REPLACE VIEW {table_name}_view AS SELECT {projection} FROM {table_name}"

    def _field_to_sql(
        self, field: bigquery.SchemaField, path: str = "data", array_depth: int = 0
    ) -> str:
        if field.name.startswith("_sdc"):
            return f"{field.name}, \n"
        if field.field_type.upper() == "RECORD":
            return "STRUCT(\n  {}\n) as {}, \n".format(
                "  ".join(
                    [
                        self._field_to_sql(f, f"{path}.{field.name}", array_depth)
                        if not f.mode == "REPEATED" and array_depth == 0
                        else self._wrap_repeat_json_extract(f, array_depth + 1)
                        for f in field.fields
                    ]
                ).rstrip(", \n"),
                field.name,
            )
        elif field.field_type.upper() == "STRING":
            return f"STRING({path}.{field.name}) as {field.name}, \n"
        elif field.field_type.upper() == "INTEGER":
            return f"INT64({path}.{field.name}) as {field.name}, \n"
        elif field.field_type.upper() == "FLOAT":
            return f"FLOAT64({path}.{field.name}) as {field.name}, \n"
        elif field.field_type.upper() == "BOOLEAN":
            return f"BOOL({path}.{field.name}) as {field.name}, \n"
        else:
            return self._wrap_json_extract(field, path)

    def _wrap_repeat_json_extract(self, field: bigquery.SchemaField) -> str:
        v = self._field_to_sql(field).rstrip(", \n")
        return dedent(
            f"""
        ARRAY(
        SELECT {v}
            FROM UNNEST(
                JSON_QUERY_ARRAY(data.{field.name},'$.{field.name}')
            ) AS f_typ_element
        ) AS {field.name}, 
        """
        )

    def _wrap_json_extract(
        self, field: bigquery.SchemaField, base: str = "data"
    ) -> str:
        return f"CAST(JSON_EXTRACT_SCALAR({base}, '$.{field.name}') as {field.field_type}) as {field.name}, \n"


def __mutate_column(self, mut_field, expected_field):  # type: ignore
    # This can throw if uncastable change in schema
    # It works for basic mutations. Left for posterity,
    # I am dubious of the value of this feature
    # hence why I chose not to include it; but it does work as-is.
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


def hacky_test():
    """A hacky test function that asserts DDL is generated correctly"""

    SAMPLE = {
        "properties": {
            "id": {"type": ["integer", "null"]},
            "rep_key": {"type": ["integer", "null"]},
            "fake_obj": {
                "type": "object",
                "properties": {
                    "inside_1": {"type": "string"},
                    "inside_2": {"type": "date"},
                },
            },
            "arr_obj": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "inside_1_arr": {"type": "string"},
                        "inside_2_arr": {"type": "date"},
                    },
                },
            },
            "Col_2_string": {"type": ["string", "null"]},
            "Col_3_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_4_int": {"type": ["integer", "null"]},
            "Col_5_float": {"type": ["number", "null"]},
            "Col_6_string": {"type": ["string", "null"]},
            "Col_7_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_8_int": {"type": ["integer", "null"]},
            "Col_9_float": {"type": ["number", "null"]},
            "Col_10_string": {"type": ["string", "null"]},
            "Col_11_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_12_int": {"type": ["integer", "null"]},
            "Col_13_float": {"type": ["number", "null"]},
            "Col_14_string": {"type": ["string", "null"]},
            "Col_15_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_16_int": {"type": ["integer", "null"]},
            "Col_17_float": {"type": ["number", "null"]},
            "Col_18_string": {"type": ["string", "null"]},
            "Col_19_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_20_int": {"type": ["integer", "null"]},
            "Col_21_float": {"type": ["number", "null"]},
            "Col_22_string": {"type": ["string", "null"]},
            "Col_23_date": {"format": "date", "type": ["string", "null"]},
            "Col_87_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_88_int": {"type": ["integer", "null"]},
            "Col_89_float": {"type": ["number", "null"]},
            "Col_90_string": {"type": ["string", "null"]},
            "Col_91_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_92_int": {"type": ["integer", "null"]},
            "Col_93_float": {"type": ["number", "null"]},
            "Col_94_string": {"type": ["string", "null"]},
            "Col_95_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_96_int": {"type": ["integer", "null"]},
            "Col_97_float": {"type": ["number", "null"]},
            "Col_98_string": {"type": ["string", "null"]},
            "Col_99_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_100_int": {"type": ["integer", "null"]},
            "Col_139_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_140_int": {"type": ["integer", "null"]},
            "Col_141_float": {"type": ["number", "null"]},
            "Col_142_string": {"type": ["string", "null"]},
            "Col_143_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_144_int": {"type": ["integer", "null"]},
            "Col_145_float": {"type": ["number", "null"]},
            "Col_146_string": {"type": ["string", "null"]},
            "Col_147_datetime": {"format": "date-time", "type": ["string", "null"]},
            "Col_148_int": {"type": ["integer", "null"]},
            "Col_149_float": {"type": ["number", "null"]},
        },
        "type": "object",
    }

    TARGET = dedent(
        """
    CREATE OR REPLACE VIEW ichigo.kurosaki.bleach_view AS SELECT INT64(data.id) as id, 
    INT64(data.rep_key) as rep_key, 
    STRUCT(
      STRING(data.fake_obj.inside_1) as inside_1, 
      STRING(data.fake_obj.inside_2) as inside_2
    ) as fake_obj, 
    
            ARRAY(
            SELECT STRUCT(
      STRING(data.arr_obj.inside_1_arr) as inside_1_arr, 
      STRING(data.arr_obj.inside_2_arr) as inside_2_arr
    ) as arr_obj
                FROM UNNEST(
                    JSON_QUERY_ARRAY(data.arr_obj,'$.arr_obj')
                ) AS f_typ_element
            ) AS arr_obj, 
    STRING(data.Col_2_string) as Col_2_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_3_datetime') as timestamp) as Col_3_datetime, 
    INT64(data.Col_4_int) as Col_4_int, 
    FLOAT64(data.Col_5_float) as Col_5_float, 
    STRING(data.Col_6_string) as Col_6_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_7_datetime') as timestamp) as Col_7_datetime, 
    INT64(data.Col_8_int) as Col_8_int, 
    FLOAT64(data.Col_9_float) as Col_9_float, 
    STRING(data.Col_10_string) as Col_10_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_11_datetime') as timestamp) as Col_11_datetime, 
    INT64(data.Col_12_int) as Col_12_int, 
    FLOAT64(data.Col_13_float) as Col_13_float, 
    STRING(data.Col_14_string) as Col_14_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_15_datetime') as timestamp) as Col_15_datetime, 
    INT64(data.Col_16_int) as Col_16_int, 
    FLOAT64(data.Col_17_float) as Col_17_float, 
    STRING(data.Col_18_string) as Col_18_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_19_datetime') as timestamp) as Col_19_datetime, 
    INT64(data.Col_20_int) as Col_20_int, 
    FLOAT64(data.Col_21_float) as Col_21_float, 
    STRING(data.Col_22_string) as Col_22_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_23_date') as date) as Col_23_date, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_87_datetime') as timestamp) as Col_87_datetime, 
    INT64(data.Col_88_int) as Col_88_int, 
    FLOAT64(data.Col_89_float) as Col_89_float, 
    STRING(data.Col_90_string) as Col_90_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_91_datetime') as timestamp) as Col_91_datetime, 
    INT64(data.Col_92_int) as Col_92_int, 
    FLOAT64(data.Col_93_float) as Col_93_float, 
    STRING(data.Col_94_string) as Col_94_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_95_datetime') as timestamp) as Col_95_datetime, 
    INT64(data.Col_96_int) as Col_96_int, 
    FLOAT64(data.Col_97_float) as Col_97_float, 
    STRING(data.Col_98_string) as Col_98_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_99_datetime') as timestamp) as Col_99_datetime, 
    INT64(data.Col_100_int) as Col_100_int, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_139_datetime') as timestamp) as Col_139_datetime, 
    INT64(data.Col_140_int) as Col_140_int, 
    FLOAT64(data.Col_141_float) as Col_141_float, 
    STRING(data.Col_142_string) as Col_142_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_143_datetime') as timestamp) as Col_143_datetime, 
    INT64(data.Col_144_int) as Col_144_int, 
    FLOAT64(data.Col_145_float) as Col_145_float, 
    STRING(data.Col_146_string) as Col_146_string, 
    CAST(JSON_EXTRACT_SCALAR(data, '$.Col_147_datetime') as timestamp) as Col_147_datetime, 
    INT64(data.Col_148_int) as Col_148_int, 
    FLOAT64(data.Col_149_float) as Col_149_float, 
     FROM ichigo.kurosaki.bleach"""
    ).lstrip()

    import difflib

    for i, s in enumerate(
        difflib.ndiff(
            SchemaTranslator(SAMPLE).make_view_sql("ichigo.kurosaki.bleach"), TARGET
        )
    ):
        if s[0] == " ":
            continue
        elif s[0] == "-":
            print('Delete "{}" from position {}'.format(s[-1], i))
        elif s[0] == "+":
            print('Add "{}" to position {}'.format(s[-1], i))

    assert SchemaTranslator(SAMPLE).make_view_sql("ichigo.kurosaki.bleach") == TARGET
    print("Success!")


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schema",
        type=str,
        help="Path to JSON Schema file",
        required=True,
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Table name to generate",
        required=True,
    )
    args = parser.parse_args()

    with open(args.schema, "r") as f:
        schema = json.load(f)
    translator = SchemaTranslator(schema)
    ddl = translator.make_view_sql(args.table)
    print(ddl)
