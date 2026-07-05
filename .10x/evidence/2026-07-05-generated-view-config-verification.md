Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/2026-07-05-github-issue-pr-burn-down-plan.md

# Generated View and Configuration Verification

## What Was Observed

Generated fixed-schema views now handle JSON object columns, scalar array columns, custom timestamp parsing, and Singer SDK metadata fields that are not stored in the fixed target table.

This slice also adds explicit clustering-field configuration and a safe temporary-table name template.

## Procedure

- Ran `uv run --no-sync pytest -q target_bigquery/tests/test_core_pure.py target_bigquery/tests/test_utils.py --timeout=120`.
- Ran ruff fix/format/check on touched Python files.
- Ran a live BigQuery batch-load probe using the corrected service-account key path:
  - created throwaway dataset `regal-scholar-336206.target_bigquery_view_codex_20260705021409`
  - loaded a fixed-schema stream with `generate_view=true`
  - configured `timestamp_format=%Y-%m-%d %H:%M:%S %z`
  - included a JSON object field, repeated string field, timestamp string with a spaced timezone offset, and `_sdc_sync_started_at`
  - queried the generated view for typed timestamp, JSON object, and repeated string values
  - deleted the throwaway dataset in a `finally` block

## Results

- Focused local tests: exit 0, `92 passed in 1.43s`.
- Live generated-view probe: exit 0, `live_generated_view_json_timestamp=pass`.
- Cleanup confirmed the throwaway dataset was deleted.

## What This Supports

- Issues #34 and #39 are addressed for the reported fixed-schema generated-view JSON failures:
  - JSON columns use `JSON_QUERY(...)` instead of invalid `CAST(JSON_VALUE(...) AS JSON)`.
  - Scalar array elements use `JSON_VALUE(row, '$')` after `UNNEST(JSON_QUERY_ARRAY(...))`, not `JSON_VALUE(row, '$.<array_field>')`.
- Issue #40 is addressed with the `timestamp_format` config for generated timestamp view columns.
- Issue #104 is addressed with `clustering_fields`, an explicit BigQuery clustering override that does not mutate Singer primary-key or merge semantics.
- Issue #108 is further addressed with `temporary_table_name_template` in addition to configurable temporary-table expiration.

## Limits

- The live probe uses `batch_job` because GCS staging remains externally blocked by disabled project billing and legacy streaming remains blocked by the BigQuery free tier.
- BigQuery JSON extraction behavior was checked against the Google Cloud BigQuery JSON functions documentation: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions.
