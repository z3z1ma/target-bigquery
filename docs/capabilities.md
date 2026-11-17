# Ingestion capabilities and validation boundaries

This guide describes current main. GitHub release0.8.0 includes the layout,
nested-name and view-alias fixes; #167 query recovery is subsequent work on main.
Most evidence is offline. The focused two-pass overwrite gate passed for
batch_job and storage_write_api; see [live evidence](live-overwrite-validation.md).
This is not full transport/schema cloud certification.

## Transport and schema choices

| Method | Fixed JSON schema | Denormalized schema | Offline evidence / constraints |
| --- | --- | --- | --- |
| `storage_write_api` (default) | JSON text in protobuf `data` | Generated protobuf; resolved JSON fields serialized recursively | Actual protobuf tests in `test_json_transport.py`; stream rotation, append errors and commit errors in `test_workers.py` and `test_target.py`. Default stream by default; optional pending-stream batch mode. |
| `batch_job` | Compressed JSON lines with `data` | Compressed JSON lines with resolved columns | Payload/job configuration tests in `test_core_pure.py`; load-job errors in `test_workers.py`. Denormalized jobs allow field addition and ignore unknown values, so schema/name agreement matters. |
| `gcs_stage` | Compressed JSON objects staged in a bucket | Same staging with resolved columns | Loads complete at each nonempty checkpoint before STATE/MERGE; successful URIs are not reloaded at EOF. Failure/order tests in `test_gcs_checkpoints.py`. Requires bucket access as well as BigQuery access. Nested unknown-value risk matches batch jobs. |
| `streaming_insert` | JSON text in the raw `data` field | JSON-compatible row dictionaries | Row-error and payload tests in `test_workers.py` and `test_core_pure.py`. Uses the legacy insert API; not claimed to provide exactly-once delivery. |

See [dispatch tests](../target_bigquery/tests/test_target.py),
[payload/job tests](../target_bigquery/tests/test_core_pure.py),
[worker tests](../target_bigquery/tests/test_workers.py), and
[JSON transport tests](../target_bigquery/tests/test_json_transport.py).
These are layered offline checks, not an end-to-end cloud certification matrix.

| Strategy (all four transports) | Fixed schema | Denormalized | Checkpoint / selection contract |
| --- | --- | --- | --- |
| Append | Supported | Supported | Default when no selected overwrite/upsert applies. Successful drain is required before STATE. Replays can duplicate rows. |
| Upsert | Not selected | Selected only with tap `key_properties` | Takes precedence over overwrite. Staging must merge before STATE; rotated staging is used by subsequent writes. Without keys, upsert is not selected and append or selected overwrite applies. |
| Overwrite | Supported; layout preflight | Supported; layout preflight | Configured layout is retained; incompatible existing layouts require migration. Staging publishes at EOF. Any pending overwrite defers global STATE, including mixed streams. Multiple table replacements are not one atomic transaction. |

An upsert flag on a fixed-schema stream does not turn raw JSON into a merge key.
Selection strings/patterns are tested in `test_core_pure.py`; checkpoint ordering
and overwrite failure/termination behavior are tested in `test_target.py` and
`test_overwrite_state.py`. Check the tap's exit status: closed stdin looks like EOF
and is not proof that an upstream full refresh succeeded.

## Schema and JSON contracts

- Fixed ingestion preserves document keys in `data`. Optional generated views
  expose typed projections; view SQL generation does not validate a schema against
  the server. Fixed ingestion can still fail on invalid JSON values, quotas,
  permissions or transport errors; it is not an unconditional success guarantee.
- Resolver v1 is the default. Resolver v2 falls back to JSON for more unsupported
  shapes. A fallback can change a column's physical type; it does not migrate an
  existing table automatically.
- Denormalized transforms recurse through record dictionaries, including JSON
  document keys. Collisions fail before buffering; declared collisions fail
  before destination mutations. Input values are not resolved by schema suffixes.
- Storage Write JSON strings represent JSON scalar strings, not pre-encoded
  documents. None means SQL NULL for a nullable field and JSON null in a repeated
  JSON element. Non-finite numeric values and integers outside the supported
  64-bit transport range fail before buffering. Other transports use their own
  serialization path; do not assume identical numeric edge behavior.
- Nested RECORD names align with record transforms after #164. Existing nested
  schemas require exact compatible names, order, types and modes; incompatible
  tables fail before mutations. See README for explicit migration guidance.
- BigQuery JSON server constraints remain authoritative; see
  [JSON ingestion](https://docs.cloud.google.com/bigquery/docs/json-data).

## Views and upgrades

Generated scalar and STRUCT view aliases use configured column transforms.
Repeated scalar/RECORD aliases now use the same rules, while their JSON lookup
paths keep original source keys. For example, `ItemRows` projects as `item_rows`
with snake_case. On regeneration this corrects an old inconsistent view column
name, so update downstream queries that used the previous raw repeated alias.
No raw-table data is rewritten. Regression coverage is in
[test_view_aliases.py](../target_bigquery/tests/test_view_aliases.py).
This is SQL-generation evidence; server execution and arbitrary flexible-name
escaping have not been validated in this pass.

## Durability, operations and remaining gates

Keep `fail_fast` enabled to surface worker failures. Query timeout bounds client
submission/result waits, not a strict whole-process deadline; a remote job may
complete after a local timeout. Known query IDs and actual job/client location support reconciliation, including
lost submission responses. Failed mutation jobs are not automatically resubmitted;
see [recovery procedure and retry tradeoffs](query-recovery.md).
Default-stream retries and source replay can duplicate rows. Pending-stream
offsets apply per stream, not to subsequent MERGE or multi-table publication.
Google's [Storage Write best practices](https://docs.cloud.google.com/bigquery/docs/write-api-best-practices)
support batching and bounded stream creation; this repository does not claim an
unmeasured throughput advantage or end-to-end exactly-once delivery.

Temporary BigQuery tables have configurable expiration. Failed-run staging and
GCS objects need an operator retention policy; this target does not promise that
all failed-run artifacts are deleted. Streaming upsert/overwrite templates require
an unmodified `{uuid}` placeholder; reusing deleted staging names is unsafe.
The live layout gate passed in #152. Nested migrations, legacy streaming inserts,
GCS and general failure/restart behavior still lack equivalent live validation.

The [overwrite compatibility decision](overwrite-compatibility.md) completes the
offline WRITE_TRUNCATE_DATA spike. The client can encode a future destination-query
publisher, but its schema/metadata/lifecycle acceptance matrix still needs separately
scoped server validation. Production continues to use CTAS. Follow [ROADMAP.md](../ROADMAP.md)
for sequencing and current status.
