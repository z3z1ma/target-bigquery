Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/2026-07-05-github-issue-pr-burn-down-plan.md

# Storage Write Nested Verification

## What Was Observed

Storage Write denormalized nested records failed before this slice by creating the table schema but leaving the table empty. The investigation found two issues:

- Worker errors emitted during final flush were not checked after `drain_all(is_endofpipe=True)` joined workers.
- BigQuery Storage Write `ProtoSchema` receives only a `DescriptorProto`, so nested RECORD field `type_name` values must be local nested message names such as `Nested_profile`, not fully-qualified descriptor-pool names.

## Procedure

- Reproduced the nested Storage Write empty-table behavior against live BigQuery.
- Isolated the writer schema by running a direct Storage Write append to a hand-created nested table.
- Confirmed direct append succeeds when nested `type_name` values are rewritten to local nested names.
- Added unit coverage for:
  - late worker errors after shutdown preventing cleanup/state advancement
  - append response errors becoming worker errors
  - already-closed cached streams during worker cleanup
  - local nested `type_name` values in generated Storage Write templates
- Ran live target probe:
  - created throwaway dataset `regal-scholar-336206.target_bigquery_storage_nested_20260705023226`
  - loaded a denormalized Storage Write stream with a nested record and repeated nested record
  - queried `profile.name`, `profile.score`, `ARRAY_LENGTH(items)`, and `items[OFFSET(1)].sku`
  - deleted the throwaway dataset in a `finally` block

## Results

- Target/worker/proto focused tests: exit 0.
- Live nested Storage Write target probe: exit 0, `live_storage_write_nested=pass`.
- Observed live row: `id=1`, `name=Ada`, `score=98.5`, `item_count=2`, `second_sku=b`.

## What This Supports

- Issue #71 is fixed for nested denormalized Storage Write records.
- Issue #76 has stronger evidence: Storage Write default-stream ingestion works live for flat and nested denormalized shapes.
- Worker failures discovered during final flush now fail fast before cleanup or state writes.

## Limits

- This probe uses the default Storage Write stream. Earlier live-compatible integration evidence covers pending-stream/batch mode for non-nested shapes.
