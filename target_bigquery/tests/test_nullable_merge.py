"""Exercise generated MERGE predicates offline, without emulating BigQuery jobs."""

import re
import sqlite3

import pytest
from google.cloud import bigquery
from singer_sdk.exceptions import MissingKeyPropertiesError

from target_bigquery.tests.test_core_pure import FakeBigQueryClient, make_merge_sink, make_sink


def merge_sql(*, dedupe=False, keys=("id", "tenant")):
    sink = make_merge_sink(
        {
            "id": {"type": ["integer", "null"]},
            "tenant": {"type": ["string", "null"]},
            "value": {"type": ["string", "null"]},
            "_sdc_deleted_at": {"type": ["string", "null"]},
        }
    )
    sink._key_properties = list(keys)
    sink._config["dedupe_before_upsert"] = dedupe
    client = FakeBigQueryClient(
        bigquery.Table(
            sink.merge_target.as_ref(),
            schema=[
                bigquery.SchemaField("id", "INTEGER"),
                bigquery.SchemaField("tenant", "STRING"),
                bigquery.SchemaField("value", "STRING"),
                bigquery.SchemaField("_sdc_deleted_at", "TIMESTAMP"),
            ],
        )
    )
    sink.merge_table(client)
    return client.queries[0]


def predicate(sql):
    return sql.split(" AS source ON ", 1)[1].split(" WHEN ", 1)[0]


def test_exact_composite_predicate_retains_equality_without_coercion():
    assert predicate(merge_sql()) == (
        "(target.`id` = source.`id` OR (target.`id` IS NULL AND source.`id` IS NULL)) AND "
        "(target.`tenant` = source.`tenant` OR "
        "(target.`tenant` IS NULL AND source.`tenant` IS NULL))"
    )


@pytest.mark.parametrize(
    "left,right,match",
    [
        ((None, "a"), (None, "a"), True),
        ((None, None), (None, None), True),
        ((1, None), (1, None), True),
        ((1, "a"), (1, "a"), True),
        ((None, "a"), (1, "a"), False),
        ((1, "a"), (None, "a"), False),
        ((None, "a"), (None, "b"), False),
        ((1, None), (2, None), False),
        ((1, None), (1, "a"), False),
        ((1, "a"), (2, "a"), False),
    ],
)
def test_generated_predicate_null_and_composite_guards(left, right, match):
    # SQLite evaluates this shared SQL subset; this is not a BigQuery integration test.
    with sqlite3.connect(":memory:") as db:
        actual = db.execute(
            f"SELECT {predicate(merge_sql())} FROM (SELECT ? AS id, ? AS tenant) target "
            "CROSS JOIN (SELECT ? AS id, ? AS tenant) source",
            (*left, *right),
        ).fetchone()[0]
    assert (actual == 1) is match


def test_replay_does_not_insert_another_nullable_key():
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE destination (id INTEGER, tenant TEXT, value TEXT)")
        db.execute("CREATE TABLE staging (id INTEGER, tenant TEXT, value TEXT)")
        db.execute("INSERT INTO staging VALUES (NULL, 'a', 'first')")
        condition = predicate(merge_sql())
        # The NOT MATCHED part of a two-pass replay, using the actual generated ON.
        for _ in range(2):
            db.execute(
                "INSERT INTO destination SELECT * FROM staging source WHERE NOT EXISTS "
                f"(SELECT 1 FROM destination target WHERE {condition})"
            )
        assert db.execute("SELECT * FROM destination").fetchall() == [(None, "a", "first")]


def test_dedup_groups_nullable_composite_keys_before_merge():
    sql = merge_sql(dedupe=True)
    window = re.search(r"ROW_NUMBER\(\) OVER \((.*?)\) = 1", sql).group(1)
    assert (
        window
        == "PARTITION BY `id`, `tenant` ORDER BY _sdc_extracted_at DESC, _sdc_received_at DESC"
    )
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE staging (id, tenant, value, _sdc_extracted_at, _sdc_received_at)")
        db.executemany(
            "INSERT INTO staging VALUES (?, ?, ?, ?, ?)",
            [
                (None, "a", "old", 1, 1),
                (None, "a", "new", 2, 2),
                (None, "b", "other", 1, 1),
            ],
        )
        rows = db.execute(
            f"SELECT id, tenant, value FROM (SELECT *, ROW_NUMBER() OVER ({window}) AS n "
            "FROM staging) WHERE n = 1 ORDER BY tenant"
        ).fetchall()
    assert rows == [(None, "a", "new"), (None, "b", "other")]
    assert "ROW_NUMBER" not in merge_sql(dedupe=False)
    assert "USING `orders__tmp` AS source" in sql


@pytest.mark.parametrize("fixed", [False, True])
def test_explicit_nullable_key_is_accepted_but_missing_key_is_rejected(fixed):
    sink = make_sink({}) if fixed else make_merge_sink({"id": {"type": ["integer", "null"]}})
    sink._input_key_properties = ["id"]
    for record in [{"id": None}, {"id": 1}]:
        parsed = sink._validate_and_parse(record)
        prepared = sink.preprocess_record(parsed, {})
        sink._singer_validate_message(prepared)
    with pytest.raises(MissingKeyPropertiesError):
        sink._singer_validate_message(sink.preprocess_record({}, {}))
    if fixed:
        sink._singer_validate_message({"data": '{"id": null}'})
        assert sink._target_key_properties(["id"]) == []  # Fixed JSON mode does not upsert.


def test_tombstone_remains_an_updated_column_not_a_delete_clause():
    sql = merge_sql()
    assert "target.`_sdc_deleted_at` = source.`_sdc_deleted_at`" in sql
    assert "THEN DELETE" not in sql


def test_transformed_nullable_keys_use_physical_names():
    from target_bigquery.core import IngestionStrategy
    from target_bigquery.tests.test_core_pure import make_bigquery_table

    sink = make_merge_sink({"OrderID": {"type": ["integer", "null"]}})
    sink.table = make_bigquery_table(
        name="staging",
        jsonschema={
            "type": "object",
            "properties": {
                "OrderID": {"type": ["integer", "null"]},
            },
        },
        ingestion_strategy=IngestionStrategy.DENORMALIZED,
        transforms={"snake_case": True},
    )
    sink._key_properties = sink._target_key_properties(["OrderID"])
    record = sink.preprocess_record({"OrderID": None}, {})
    sink._singer_validate_message(record)
    assert record == {"order_id": None}
    client = FakeBigQueryClient(
        bigquery.Table(
            sink.merge_target.as_ref(),
            schema=[
                bigquery.SchemaField("order_id", "INTEGER"),
            ],
        )
    )
    sink.merge_table(client)
    assert predicate(client.queries[0]) == (
        "(target.`order_id` = source.`order_id` OR "
        "(target.`order_id` IS NULL AND source.`order_id` IS NULL))"
    )


def test_duplicate_nullable_source_rows_remain_multiple_matches_without_dedup():
    with sqlite3.connect(":memory:") as db:
        matches = db.execute(
            f"SELECT COUNT(*) FROM (SELECT NULL AS id, 'a' AS tenant) target "
            "CROSS JOIN (SELECT NULL AS id, 'a' AS tenant UNION ALL SELECT NULL, 'a') source "
            f"WHERE {predicate(merge_sql(dedupe=False))}"
        ).fetchone()[0]
    # BigQuery rejects multiple source matches for UPDATE; this test only counts matches.
    assert matches == 2
