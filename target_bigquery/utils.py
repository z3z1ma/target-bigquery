import re
from textwrap import dedent, indent
from typing import List

from google.cloud.bigquery import SchemaField


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


# TODO: We should try to avoid mutating the column name as it entails
# business logic employed outside user control which makes them reliant
# on other load methods performing the same transformation. With that said,
# it may be necessary when columns would otherwise break a load job.
def safe_column_name(name: str, fix_columns: dict) -> str:
    quotes = fix_columns["quotes"]
    lower = fix_columns["lower"]
    add_underscore_when_invalid = fix_columns["add_underscore_when_invalid"]

    name = name.replace("`", "")
    pattern = "[^a-zA-Z0-9_]"
    name = re.sub(pattern, "_", name)

    if quotes:
        name = "`{}`".format(name)
    if lower:
        name = "{}".format(name).lower()
    if add_underscore_when_invalid:
        if name[0].isdigit():
            name = "_{}".format(name)
    return name


# This class translates a JSON schema into a BigQuery schema.
# It also uses the translated schema to generate a CREATE VIEW statement.
class SchemaTranslator:
    def __init__(self, schema, fix_columns):
        self.schema = schema
        self.translated_schema = [
            self._jsonschema_prop_to_bq_column(name, contents, fix_columns)
            for name, contents in self.schema.get("properties", {}).items()
        ]

    def _jsonschema_prop_to_bq_column(
        self, name: str, schema_property: dict, fix_columns: dict
    ) -> SchemaField:
        # Don't munge names, much more portable the less business logic we apply
        # safe_name = safe_column_name(name, quotes=False)  <- this mutates the name
        safe_name = safe_column_name(name, fix_columns=fix_columns)
        # safe_name = name

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
                    safe_name, items_schema, "REPEATED"
                )
            return SchemaField(safe_name, items_type, "REPEATED")
        elif "object" in property_type:
            return self._translate_record_to_bq_schema(safe_name, schema_property)
        else:
            result_type = bigquery_type(property_type, property_format)
            return SchemaField(safe_name, result_type, "NULLABLE")

    def _translate_record_to_bq_schema(
        self, safe_name, schema_property, mode="NULLABLE"
    ) -> SchemaField:
        fields = [
            self._jsonschema_prop_to_bq_column(col, t)
            for col, t in schema_property.get("properties", {}).items()
        ]
        return SchemaField(safe_name, "RECORD", mode, fields=fields)

    def make_view_stmt(self, table_name: str) -> str:
        """Generate a CREATE VIEW statement for the SchemaTranslator `schema`."""

        projection = ""
        for field in self.translated_schema[:]:
            if field.mode == "REPEATED":
                projection += indent(self._wrap_repeat_json_extract(field), " " * 4)
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
                + f") as {field.name},\n"
            )
        # Nullable fields require a JSON_VALUE call which creates a 2-stage cast
        elif field.is_nullable:
            return (" " * depth * 2) + self._wrap_json_value(
                field, f"{path}.{field.name}", base
            )
        # These are not nullable so if the type is known, we can do a 1-stage extract & cast
        elif field.field_type.upper() == "STRING":
            return (" " * depth * 2) + f"STRING({scalar}) as {field.name},\n"
        elif field.field_type.upper() == "INTEGER":
            return (" " * depth * 2) + f"INT64({scalar}) as {field.name},\n"
        elif field.field_type.upper() == "FLOAT":
            return (" " * depth * 2) + f"FLOAT64({scalar}) as {field.name},\n"
        elif field.field_type.upper() == "BOOLEAN":
            return (" " * depth * 2) + f"BOOL({scalar}) as {field.name},\n"
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
            return f"JSON_VALUE({base}, '{path}') as {field.name},\n"
        if typ == "FLOAT":
            typ = "FLOAT64"
        if typ in ("INT", "INTEGER"):
            typ = "INT64"
        return f"CAST(JSON_VALUE({base}, '{path}') as {typ}) as {field.name},\n"


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


# Just a quick assertion to make sure we're not breaking anything
# in the future. We should eventually add a proper test suite.
def test_convoluted_schema():
    # This schema is a bit of a mess, but it's a good test case
    # for the schema translator

    from singer_sdk import typing as th  # JSON Schema typing helpers

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("companyId", th.IntegerType),
        th.Property("email", th.StringType),
        th.Property("fullName", th.StringType),
        th.Property("firstName", th.StringType),
        th.Property("surname", th.StringType),
        th.Property("displayName", th.StringType),
        th.Property("creationDateTime", th.DateTimeType),
        th.Property(
            "internal",
            th.ObjectType(
                th.Property("yearsSinceTermination", th.NumberType),
                th.Property("terminationReason", th.StringType),
                th.Property("probationEndDate", th.StringType),
                th.Property("currentActiveStatusStartDate", th.StringType),
                th.Property("terminationDate", th.StringType),
                th.Property("status", th.StringType),
                th.Property("terminationType", th.StringType),
                th.Property("lifecycleStatus", th.StringType),
            ),
        ),
        th.Property(
            "work",
            th.ObjectType(
                th.Property(
                    "durationofemployment",
                    th.ObjectType(
                        th.Property("periodiso", th.StringType),
                        th.Property("sortfactor", th.IntegerType),
                        th.Property("humanize", th.StringType),
                    ),
                ),
                th.Property("startdate", th.StringType),
                th.Property("manager", th.StringType),
                th.Property("reportstoidincompany", th.IntegerType),
                th.Property("employeeIdInCompany", th.IntegerType),
                th.Property("shortstartdate", th.StringType),
                th.Property("daysofpreviousservice", th.IntegerType),
                th.Property(
                    "customColumns",
                    th.ObjectType(
                        th.Property("column_1655996461265", th.StringType),
                        th.Property(
                            "column_1644862416222", th.ArrayType(th.StringType)
                        ),
                        th.Property(
                            "column_1644861659664", th.ArrayType(th.StringType)
                        ),
                    ),
                ),
                th.Property(
                    "custom",
                    th.ObjectType(
                        th.Property("field_1651169416679", th.StringType),
                    ),
                ),
                th.Property("directreports", th.IntegerType),
                th.Property("indirectreports", th.IntegerType),
                th.Property("tenureyears", th.IntegerType),
                th.Property("yearsofservice__it", th.IntegerType),
                th.Property("tenuredurationyears", th.NumberType),
                th.Property("tenuredurationyears_it", th.IntegerType),
                th.Property(
                    "tenureduration",
                    th.ObjectType(
                        th.Property("periodiso", th.StringType),
                        th.Property("sortfactor", th.IntegerType),
                        th.Property("humanize", th.StringType),
                    ),
                ),
                th.Property(
                    "reportsTo",
                    th.ObjectType(
                        th.Property("id", th.StringType),
                        th.Property("email", th.StringType),
                        th.Property("firstName", th.StringType),
                        th.Property("surname", th.StringType),
                        th.Property("displayName", th.StringType),
                    ),
                ),
                th.Property("department", th.StringType),
                th.Property("siteId", th.IntegerType),
                th.Property("isManager", th.BooleanType),
                th.Property("title", th.StringType),
                th.Property("site", th.StringType),
                th.Property("activeEffectiveDate", th.StringType),
                th.Property("yearsofservice", th.NumberType),
                th.Property("secondlevelmanager", th.NumberType),
            ),
        ),
        th.Property(
            "humanReadable",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("companyId", th.StringType),
                th.Property("email", th.StringType),
                th.Property("fullName", th.StringType),
                th.Property("firstName", th.StringType),
                th.Property("surname", th.StringType),
                th.Property("displayName", th.StringType),
                th.Property("creationDateTime", th.StringType),
                th.Property("avatarurl", th.StringType),
                th.Property("secondname", th.StringType),
                th.Property(
                    "work",
                    th.ObjectType(
                        th.Property("startdate", th.StringType),
                        th.Property("shortstartdate", th.StringType),
                        th.Property("manager", th.StringType),
                        th.Property("reportsToIdInComany", th.IntegerType),
                        th.Property("employeeIdInCompany", th.StringType),
                        th.Property("reportsTo", th.StringType),
                        th.Property("department", th.StringType),
                        th.Property("siteId", th.StringType),
                        th.Property("isManager", th.StringType),
                        th.Property("title", th.StringType),
                        th.Property("site", th.StringType),
                        th.Property("durationofemployment", th.StringType),
                        th.Property("daysofpreviousservice", th.StringType),
                        th.Property("directreports", th.StringType),
                        th.Property("tenureduration", th.StringType),
                        th.Property("activeeffectivedate", th.StringType),
                        th.Property("tenuredurationyears", th.StringType),
                        th.Property("yearsofservice", th.StringType),
                        th.Property("secondlevelmanager", th.StringType),
                        th.Property("indirectreports", th.StringType),
                        th.Property("tenureyears", th.StringType),
                        th.Property(
                            "customColumns",
                            th.ObjectType(
                                th.Property("column_1664478354663", th.StringType),
                                th.Property("column_1655996461265", th.StringType),
                                th.Property("column_1644862416222", th.StringType),
                                th.Property("column_1644861659664", th.StringType),
                            ),
                        ),
                        th.Property(
                            "custom",
                            th.ObjectType(
                                th.Property("field_1651169416679", th.StringType),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "internal",
                    th.ObjectType(
                        th.Property("periodSinceTermination", th.StringType),
                        th.Property("yearsSinceTermination", th.StringType),
                        th.Property("terminationReason", th.StringType),
                        th.Property("probationEndDate", th.StringType),
                        th.Property("currentActiveStatusStartDate", th.StringType),
                        th.Property("terminationDate", th.StringType),
                        th.Property("status", th.StringType),
                        th.Property("terminationType", th.StringType),
                        th.Property("notice", th.StringType),
                        th.Property("lifecycleStatus", th.StringType),
                    ),
                ),
                th.Property(
                    "about",
                    th.ObjectType(
                        th.Property("superpowers", th.StringType),
                        th.Property("hobbies", th.StringType),
                        th.Property("avatar", th.StringType),
                        th.Property("about", th.StringType),
                        th.Property(
                            "socialdata",
                            th.ObjectType(
                                th.Property("linkedin", th.StringType),
                                th.Property("facebook", th.StringType),
                                th.Property("twitter", th.StringType),
                            ),
                        ),
                        th.Property(
                            "custom",
                            th.ObjectType(
                                th.Property("field_1645133202751", th.StringType),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "personal",
                    th.ObjectType(
                        th.Property("shortbirthdate", th.StringType),
                        th.Property("pronouns", th.StringType),
                        th.Property(
                            "custom",
                            th.ObjectType(
                                th.Property("field_1647463606890", th.StringType),
                                th.Property("field_1647619490812", th.StringType),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "lifecycle",
                    th.ObjectType(
                        th.Property(
                            "custom",
                            th.ObjectType(
                                th.Property("field_1651694080083", th.StringType),
                            ),
                        ),
                    ),
                ),
                th.Property(
                    "payroll",
                    th.ObjectType(
                        th.Property(
                            "employment",
                            th.ObjectType(
                                th.Property("siteWorkinPattern", th.StringType),
                                th.Property("salaryPayType", th.StringType),
                                th.Property("actualWorkingPattern", th.StringType),
                                th.Property("activeeffectivedate", th.StringType),
                                th.Property("workingPattern", th.StringType),
                                th.Property("fte", th.StringType),
                                th.Property("type", th.StringType),
                                th.Property("contract", th.StringType),
                                th.Property("calendarId", th.StringType),
                                th.Property("weeklyHours", th.StringType),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    ).to_dict()

    TARGET = dedent(
        """
    CREATE OR REPLACE VIEW my.neighbor.totoro_view AS
    SELECT
        JSON_VALUE(data, '$.id') as id,
        CAST(JSON_VALUE(data, '$.companyId') as INT64) as companyId,
        JSON_VALUE(data, '$.email') as email,
        JSON_VALUE(data, '$.fullName') as fullName,
        JSON_VALUE(data, '$.firstName') as firstName,
        JSON_VALUE(data, '$.surname') as surname,
        JSON_VALUE(data, '$.displayName') as displayName,
        CAST(JSON_VALUE(data, '$.creationDateTime') as TIMESTAMP) as creationDateTime,
        STRUCT(
          CAST(JSON_VALUE(data, '$.internal.yearsSinceTermination') as FLOAT64) as yearsSinceTermination,
          JSON_VALUE(data, '$.internal.terminationReason') as terminationReason,
          JSON_VALUE(data, '$.internal.probationEndDate') as probationEndDate,
          JSON_VALUE(data, '$.internal.currentActiveStatusStartDate') as currentActiveStatusStartDate,
          JSON_VALUE(data, '$.internal.terminationDate') as terminationDate,
          JSON_VALUE(data, '$.internal.status') as status,
          JSON_VALUE(data, '$.internal.terminationType') as terminationType,
          JSON_VALUE(data, '$.internal.lifecycleStatus') as lifecycleStatus
        ) as internal,
        STRUCT(
          STRUCT(
            JSON_VALUE(data, '$.work.durationofemployment.periodiso') as periodiso,
            CAST(JSON_VALUE(data, '$.work.durationofemployment.sortfactor') as INT64) as sortfactor,
            JSON_VALUE(data, '$.work.durationofemployment.humanize') as humanize
          ) as durationofemployment,
          JSON_VALUE(data, '$.work.startdate') as startdate,
          JSON_VALUE(data, '$.work.manager') as manager,
          CAST(JSON_VALUE(data, '$.work.reportstoidincompany') as INT64) as reportstoidincompany,
          CAST(JSON_VALUE(data, '$.work.employeeIdInCompany') as INT64) as employeeIdInCompany,
          JSON_VALUE(data, '$.work.shortstartdate') as shortstartdate,
          CAST(JSON_VALUE(data, '$.work.daysofpreviousservice') as INT64) as daysofpreviousservice,
          STRUCT(
            JSON_VALUE(data, '$.work.customColumns.column_1655996461265') as column_1655996461265,
            ARRAY(
              SELECT   STRING(column_1644862416222__rows) as column_1644862416222
              FROM UNNEST(
                  JSON_QUERY_ARRAY(data, '$.work.customColumns.column_1644862416222')
              ) AS column_1644862416222__rows
            ) AS column_1644862416222,
            ARRAY(
              SELECT   STRING(column_1644861659664__rows) as column_1644861659664
              FROM UNNEST(
                  JSON_QUERY_ARRAY(data, '$.work.customColumns.column_1644861659664')
              ) AS column_1644861659664__rows
            ) AS column_1644861659664
          ) as customColumns,
          STRUCT(
            JSON_VALUE(data, '$.work.custom.field_1651169416679') as field_1651169416679
          ) as custom,
          CAST(JSON_VALUE(data, '$.work.directreports') as INT64) as directreports,
          CAST(JSON_VALUE(data, '$.work.indirectreports') as INT64) as indirectreports,
          CAST(JSON_VALUE(data, '$.work.tenureyears') as INT64) as tenureyears,
          CAST(JSON_VALUE(data, '$.work.yearsofservice__it') as INT64) as yearsofservice__it,
          CAST(JSON_VALUE(data, '$.work.tenuredurationyears') as FLOAT64) as tenuredurationyears,
          CAST(JSON_VALUE(data, '$.work.tenuredurationyears_it') as INT64) as tenuredurationyears_it,
          STRUCT(
            JSON_VALUE(data, '$.work.tenureduration.periodiso') as periodiso,
            CAST(JSON_VALUE(data, '$.work.tenureduration.sortfactor') as INT64) as sortfactor,
            JSON_VALUE(data, '$.work.tenureduration.humanize') as humanize
          ) as tenureduration,
          STRUCT(
            JSON_VALUE(data, '$.work.reportsTo.id') as id,
            JSON_VALUE(data, '$.work.reportsTo.email') as email,
            JSON_VALUE(data, '$.work.reportsTo.firstName') as firstName,
            JSON_VALUE(data, '$.work.reportsTo.surname') as surname,
            JSON_VALUE(data, '$.work.reportsTo.displayName') as displayName
          ) as reportsTo,
          JSON_VALUE(data, '$.work.department') as department,
          CAST(JSON_VALUE(data, '$.work.siteId') as INT64) as siteId,
          CAST(JSON_VALUE(data, '$.work.isManager') as BOOLEAN) as isManager,
          JSON_VALUE(data, '$.work.title') as title,
          JSON_VALUE(data, '$.work.site') as site,
          JSON_VALUE(data, '$.work.activeEffectiveDate') as activeEffectiveDate,
          CAST(JSON_VALUE(data, '$.work.yearsofservice') as FLOAT64) as yearsofservice,
          CAST(JSON_VALUE(data, '$.work.secondlevelmanager') as FLOAT64) as secondlevelmanager
        ) as work,
        STRUCT(
          JSON_VALUE(data, '$.humanReadable.id') as id,
          JSON_VALUE(data, '$.humanReadable.companyId') as companyId,
          JSON_VALUE(data, '$.humanReadable.email') as email,
          JSON_VALUE(data, '$.humanReadable.fullName') as fullName,
          JSON_VALUE(data, '$.humanReadable.firstName') as firstName,
          JSON_VALUE(data, '$.humanReadable.surname') as surname,
          JSON_VALUE(data, '$.humanReadable.displayName') as displayName,
          JSON_VALUE(data, '$.humanReadable.creationDateTime') as creationDateTime,
          JSON_VALUE(data, '$.humanReadable.avatarurl') as avatarurl,
          JSON_VALUE(data, '$.humanReadable.secondname') as secondname,
          STRUCT(
            JSON_VALUE(data, '$.humanReadable.work.startdate') as startdate,
            JSON_VALUE(data, '$.humanReadable.work.shortstartdate') as shortstartdate,
            JSON_VALUE(data, '$.humanReadable.work.manager') as manager,
            CAST(JSON_VALUE(data, '$.humanReadable.work.reportsToIdInComany') as INT64) as reportsToIdInComany,
            JSON_VALUE(data, '$.humanReadable.work.employeeIdInCompany') as employeeIdInCompany,
            JSON_VALUE(data, '$.humanReadable.work.reportsTo') as reportsTo,
            JSON_VALUE(data, '$.humanReadable.work.department') as department,
            JSON_VALUE(data, '$.humanReadable.work.siteId') as siteId,
            JSON_VALUE(data, '$.humanReadable.work.isManager') as isManager,
            JSON_VALUE(data, '$.humanReadable.work.title') as title,
            JSON_VALUE(data, '$.humanReadable.work.site') as site,
            JSON_VALUE(data, '$.humanReadable.work.durationofemployment') as durationofemployment,
            JSON_VALUE(data, '$.humanReadable.work.daysofpreviousservice') as daysofpreviousservice,
            JSON_VALUE(data, '$.humanReadable.work.directreports') as directreports,
            JSON_VALUE(data, '$.humanReadable.work.tenureduration') as tenureduration,
            JSON_VALUE(data, '$.humanReadable.work.activeeffectivedate') as activeeffectivedate,
            JSON_VALUE(data, '$.humanReadable.work.tenuredurationyears') as tenuredurationyears,
            JSON_VALUE(data, '$.humanReadable.work.yearsofservice') as yearsofservice,
            JSON_VALUE(data, '$.humanReadable.work.secondlevelmanager') as secondlevelmanager,
            JSON_VALUE(data, '$.humanReadable.work.indirectreports') as indirectreports,
            JSON_VALUE(data, '$.humanReadable.work.tenureyears') as tenureyears,
            STRUCT(
              JSON_VALUE(data, '$.humanReadable.work.customColumns.column_1664478354663') as column_1664478354663,
              JSON_VALUE(data, '$.humanReadable.work.customColumns.column_1655996461265') as column_1655996461265,
              JSON_VALUE(data, '$.humanReadable.work.customColumns.column_1644862416222') as column_1644862416222,
              JSON_VALUE(data, '$.humanReadable.work.customColumns.column_1644861659664') as column_1644861659664
            ) as customColumns,
            STRUCT(
              JSON_VALUE(data, '$.humanReadable.work.custom.field_1651169416679') as field_1651169416679
            ) as custom
          ) as work,
          STRUCT(
            JSON_VALUE(data, '$.humanReadable.internal.periodSinceTermination') as periodSinceTermination,
            JSON_VALUE(data, '$.humanReadable.internal.yearsSinceTermination') as yearsSinceTermination,
            JSON_VALUE(data, '$.humanReadable.internal.terminationReason') as terminationReason,
            JSON_VALUE(data, '$.humanReadable.internal.probationEndDate') as probationEndDate,
            JSON_VALUE(data, '$.humanReadable.internal.currentActiveStatusStartDate') as currentActiveStatusStartDate,
            JSON_VALUE(data, '$.humanReadable.internal.terminationDate') as terminationDate,
            JSON_VALUE(data, '$.humanReadable.internal.status') as status,
            JSON_VALUE(data, '$.humanReadable.internal.terminationType') as terminationType,
            JSON_VALUE(data, '$.humanReadable.internal.notice') as notice,
            JSON_VALUE(data, '$.humanReadable.internal.lifecycleStatus') as lifecycleStatus
          ) as internal,
          STRUCT(
            JSON_VALUE(data, '$.humanReadable.about.superpowers') as superpowers,
            JSON_VALUE(data, '$.humanReadable.about.hobbies') as hobbies,
            JSON_VALUE(data, '$.humanReadable.about.avatar') as avatar,
            JSON_VALUE(data, '$.humanReadable.about.about') as about,
            STRUCT(
              JSON_VALUE(data, '$.humanReadable.about.socialdata.linkedin') as linkedin,
              JSON_VALUE(data, '$.humanReadable.about.socialdata.facebook') as facebook,
              JSON_VALUE(data, '$.humanReadable.about.socialdata.twitter') as twitter
            ) as socialdata,
            STRUCT(
              JSON_VALUE(data, '$.humanReadable.about.custom.field_1645133202751') as field_1645133202751
            ) as custom
          ) as about,
          STRUCT(
            JSON_VALUE(data, '$.humanReadable.personal.shortbirthdate') as shortbirthdate,
            JSON_VALUE(data, '$.humanReadable.personal.pronouns') as pronouns,
            STRUCT(
              JSON_VALUE(data, '$.humanReadable.personal.custom.field_1647463606890') as field_1647463606890,
              JSON_VALUE(data, '$.humanReadable.personal.custom.field_1647619490812') as field_1647619490812
            ) as custom
          ) as personal,
          STRUCT(
            STRUCT(
              JSON_VALUE(data, '$.humanReadable.lifecycle.custom.field_1651694080083') as field_1651694080083
            ) as custom
          ) as lifecycle,
          STRUCT(
            STRUCT(
              JSON_VALUE(data, '$.humanReadable.payroll.employment.siteWorkinPattern') as siteWorkinPattern,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.salaryPayType') as salaryPayType,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.actualWorkingPattern') as actualWorkingPattern,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.activeeffectivedate') as activeeffectivedate,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.workingPattern') as workingPattern,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.fte') as fte,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.type') as type,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.contract') as contract,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.calendarId') as calendarId,
              JSON_VALUE(data, '$.humanReadable.payroll.employment.weeklyHours') as weeklyHours
            ) as employment
          ) as payroll
        ) as humanReadable,
     FROM my.neighbor.totoro
    """
    ).strip()

    assert SchemaTranslator(schema).make_view_stmt("my.neighbor.totoro") == TARGET


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
