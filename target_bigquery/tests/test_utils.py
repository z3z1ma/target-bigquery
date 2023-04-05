from typing import List

import pytest
import singer_sdk.typing as th
from google.cloud.bigquery import SchemaField

from target_bigquery.core import SchemaTranslator, bigquery_type, transform_column_name, BigQueryTable, IngestionStrategy
from target_bigquery.proto_gen import proto_schema_factory_v2


@pytest.mark.parametrize(
    "name,rules,expected",
    [
        ("TestColumn", {"snake_case": True}, "test_column"),
        ("_TestColumn", {"snake_case": True}, "_test_column"),
        ("RANDOMCapsAREHere", {"snake_case": True}, "random_caps_are_here"),
        ("ALLCAPS", {"snake_case": True}, "allcaps"),
        ("ALL_CAPS", {"snake_case": True}, "all_caps"),
        ("SalesforceThing__c", {"snake_case": True}, "salesforce_thing__c"),
        ("TestColumn", {"lower": True}, "testcolumn"),
        ("TestColumn", {}, "TestColumn"),
        ("`TestColumn`", {}, "`TestColumn`"),
        ("ValidColumn", {"add_underscore_when_invalid": True}, "ValidColumn"),
        (
            "123InvalidColumn",
            {"add_underscore_when_invalid": True},
            "_123InvalidColumn",
        ),
        ("Shakespeare", {"quote": True}, "`Shakespeare`"),
        ("`Shakespeare`", {"quote": True}, "`Shakespeare`"),
        ("`ANewWorld`", {"snake_case": True}, "`a_new_world`"),
        (
            "123ANewWorld",
            {"snake_case": True, "add_underscore_when_invalid": True},
            "_123_a_new_world",
        ),
        (
            "`123ANewWorld`",
            {"snake_case": True, "add_underscore_when_invalid": True},
            "`_123_a_new_world`",
        ),
        (
            "123ANewWorld",
            {
                "snake_case": True,
                "lower": True,
                "add_underscore_when_invalid": True,
                "quote": True,
            },
            "`_123_a_new_world`",
        ),
    ],
    ids=[
        "snake_case",
        "snake_case_with_underscore_prefix",
        "snake_case_with_intermitten_caps",
        "snake_case_all_caps",
        "snake_case_all_caps_with_underscore",
        "snake_case_double_underscore",
        "lowercase",
        "no_rules_supplied",
        "no_rules_supplied_quoted_string",
        "add_underscore_rule_on_valid_column",
        "add_underscore_rule_on_invalid_column",
        "add_quotes",
        "add_quotes_on_quoted_string",
        "snake_case_quoted_string",
        "composite_rule_snake_case_underscore",
        "composite_rule_snake_case_underscore_on_quoted_column",
        "all_rules",
    ],
)
def test_transform_column_name(name: str, rules: dict, expected: str):
    assert transform_column_name(name, **rules) == expected


@pytest.mark.parametrize(
    "jsonschema_type,jsonschema_format,expected",
    [
        ("number", None, "float"),
        ("string", "date-time", "timestamp"),
        ("number", "time", "time"),
    ],
    ids=[
        "number_to_float",
        "datetime_format_in_jsonschema",
        "time_format_in_jsonschema",
    ],
)
def test_bigquery_type(jsonschema_type: str, jsonschema_format: str, expected: str):
    assert bigquery_type(jsonschema_type, jsonschema_format) == expected


@pytest.mark.parametrize(
    "schema,table,transforms,expected",
    [
        (
            {"type": "object", "properties": {"int_col_1": {"type": "integer"}}},
            BigQueryTable(name="table", dataset="some", project="project", jsonschema={}, ingestion_strategy=IngestionStrategy.FIXED),
            {},
            """CREATE OR REPLACE VIEW `project`.`some`.`table_view` AS 
SELECT 
    CAST(JSON_VALUE(data, '$.int_col_1') as INT64) as int_col_1,
 FROM `project`.`some`.`table`""",
        ),
        (
            {"type": "object", "properties": {"IntCol1": {"type": "integer"}}},
            BigQueryTable(name="table", dataset="some", project="project", jsonschema={}, ingestion_strategy=IngestionStrategy.FIXED),
            {"snake_case": True},
            """CREATE OR REPLACE VIEW `project`.`some`.`table_view` AS 
SELECT 
    CAST(JSON_VALUE(data, '$.IntCol1') as INT64) as int_col1,
 FROM `project`.`some`.`table`""",
        ),
        (
            th.PropertiesList(
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
                                th.Property("column_1644862416222", th.ArrayType(th.StringType)),
                                th.Property("column_1644861659664", th.ArrayType(th.StringType)),
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
            ).to_dict(),
            BigQueryTable(name="totoro", dataset="neighbor", project="my", jsonschema={}, ingestion_strategy=IngestionStrategy.FIXED),
            {},
            """CREATE OR REPLACE VIEW `my`.`neighbor`.`totoro_view` AS 
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
          SELECT   STRING(column_1644862416222__rows.column_1644862416222) as column_1644862416222
          FROM UNNEST(
              JSON_QUERY_ARRAY(data, '$.work.customColumns.column_1644862416222')
          ) AS column_1644862416222__rows
          WHERE   STRING(column_1644862416222__rows.column_1644862416222) IS NOT NULL
        ) AS column_1644862416222,
        ARRAY(
          SELECT   STRING(column_1644861659664__rows.column_1644861659664) as column_1644861659664
          FROM UNNEST(
              JSON_QUERY_ARRAY(data, '$.work.customColumns.column_1644861659664')
          ) AS column_1644861659664__rows
          WHERE   STRING(column_1644861659664__rows.column_1644861659664) IS NOT NULL
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
 FROM `my`.`neighbor`.`totoro`""",
        ),
    ],
    ids=[
        "generate_basic_view_stmt",
        "generate_basic_view_with_column_name_transforms",
        "generate_convoluted_view",
    ],
)
def test_schema_translator_views(schema: dict, table: BigQueryTable, transforms: dict, expected: str):
    assert (
        SchemaTranslator(
            schema,
            transforms,
        ).generate_view_statement(table)
        == expected
    )


@pytest.mark.parametrize(
    "schema,transforms,expected",
    [
        (
            {"type": "object", "properties": {"IntColumn": {"type": "integer"}}},
            {},
            [SchemaField("IntColumn", "integer")],
        ),
        (
            {"type": "object", "properties": {"IntColumn": {"type": "integer"}}},
            {"snake_case": True},
            [SchemaField("int_column", "integer")],
        ),
    ],
    ids=["basic_schema_translation", "schema_translation_with_transform"],
)
def test_schema_translator_tables(schema: dict, transforms: dict, expected: List[SchemaField]):
    assert (
        SchemaTranslator(
            schema,
            transforms,
        ).translated_schema_transformed
        == expected
    )


@pytest.mark.parametrize(
    "schema,transforms,records,expected",
    [
        (
            {"type": "object", "properties": {"IntColumn": {"type": "integer"}}},
            {},
            [{"IntColumn": 1}],
            [{"IntColumn": 1}],
        ),
        (
            {"type": "object", "properties": {"IntColumn": {"type": "integer"}}},
            {"snake_case": True},
            [{"IntColumn": 1}],
            [{"int_column": 1}],
        ),
        (
            {
                "type": "object",
                "properties": {
                    "NestedLevelOne": {
                        "type": "object",
                        "properties": {
                            "NestedLevelTwo": {
                                "type": "object",
                                "properties": {"IntColumn": {"type": "integer"}},
                            }
                        },
                    }
                },
            },
            {"snake_case": True},
            [{"NestedLevelOne": {"NestedLevelTwo": {"IntColumn": 1}}}],
            [{"nested_level_one": {"nested_level_two": {"int_column": 1}}}],
        ),
        (
            {
                "type": "object",
                "properties": {
                    "NestedLevelOne": {
                        "type": "object",
                        "properties": {
                            "NestedLevelTwo": {
                                "type": "object",
                                "properties": {
                                    "ArrayColumn": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {"IntColumn": {"type": "integer"}},
                                        },
                                    }
                                },
                            }
                        },
                    }
                },
            },
            {"snake_case": True},
            [
                {"NestedLevelOne": {"NestedLevelTwo": {"ArrayColumn": [{"IntColumn": 1}]}}},
                {
                    "NestedLevelOne": {
                        "NestedLevelTwo": {
                            "ArrayColumn": [
                                {"IntColumn": 1},
                                {"IntColumn": 2},
                                {"IntColumn": 3},
                            ]
                        }
                    }
                },
            ],
            [
                {"nested_level_one": {"nested_level_two": {"array_column": [{"int_column": 1}]}}},
                {
                    "nested_level_one": {
                        "nested_level_two": {
                            "array_column": [
                                {"int_column": 1},
                                {"int_column": 2},
                                {"int_column": 3},
                            ]
                        }
                    }
                },
            ],
        ),
    ],
    ids=[
        "record_translation_noop",
        "record_translation_with_transform",
        "record_translation_nested_with_transform",
        "record_translation_nested_list_with_transform",
    ],
)
def test_schema_translator_records(
    schema: dict, transforms: dict, records: List[dict], expected: List[dict]
):
    assert [
        SchemaTranslator(
            schema,
            transforms,
        ).translate_record(record)
        for record in records
    ] == expected


def test_jit_compile_proto():
    jit = proto_schema_factory_v2(
        [
            SchemaField("IntColumn", "integer"),
            SchemaField("StringColumn", "string"),
            SchemaField("FloatColumn", "float"),
            SchemaField("BooleanColumn", "boolean"),
            SchemaField("TimestampColumn", "timestamp"),
            SchemaField("DateColumn", "date"),
            SchemaField("TimeColumn", "time"),
        ],
    )
    payload = {
        "IntColumn": 1,
        "StringColumn": "test",
        "FloatColumn": 1.0,
        "BooleanColumn": True,
        "TimestampColumn": "2020-01-01",
        "DateColumn": "2020-01-01",
        "TimeColumn": "00:00:00",
    }
    data = jit()
    descript = jit.DESCRIPTOR
    for f in descript.fields:
        if f.name in payload:
            setattr(data, f.name, payload[f.name])
    assert (
        data.SerializeToString()
        == b"\x08\x01\x12\x04test\x19\x00\x00\x00\x00\x00\x00\xf0?"
        b" \x01*\n2020-01-012\n2020-01-01:\x0800:00:00"
    )
