# JSON and schema support documentation audit

Base: `c39b46b304ab0501c59cb3c342e33357db1f84ee`. Documentation-only pass.

## Confirmed discrepancies

- Public capabilities did not state that `TargetBigQuery.get_sink` reuses the
  first sink for a stream, ignoring later schema/key changes for that sink. The
  SDK default would create a replacement. This target intentionally overrides it.
- Resolver-v2 fallback and fixed JSON were described too broadly in the README
  (most/all schemas and essentially any tap). Neither bypasses validation or
  server constraints.
- The support matrix lacked the distinction between JSON document keys and
  physical RECORD children, and between retaining historical destination columns
  in append/upsert and replacing the schema from staging during overwrite.
- A denormalized JSON payload is supported alongside typed MERGE keys; JSON
  itself has no equality operator and cannot be used directly as a MERGE key.

## Verification

294 existing offline tests passed: `test_nested_schema.py`,
`test_json_transport.py`, `test_transform_collisions.py`, `test_core_pure.py`,
`test_nullable_merge.py`, and
`test_target.py::test_get_sink_adds_only_the_first_schema_for_a_stream`.
These include real Singer-input rejection of nested schema incompatibility and
transformed-key collisions before records or STATE advance, as well as generated
protobufs, job configuration, schema updates, MERGE columns and overwrite DDL.

A separate manual probe used the real `TargetBigQuery.process_lines` entry point
with the existing offline metadata Client from `test_nested_schema.py`; no API
worker was substituted or started. Input:

1. SCHEMA for `orders`, integer `id`, key `id`.
2. RECORD `{ "id": 1 }`.
3. SCHEMA adding string `new_value` and changing the key to `new_value`.
4. RECORD `{ "id": 2, "new_value": "new" }`.
5. STATE `{ "position": 2 }`.

With denormalized Storage Write, the second record raised protobuf ParseError
(`has no field named "new_value"`). The active sink still had key `id` and no
`new_value` in its schema. No worker started and no STATE was emitted. This is a
concrete unsupported mid-run change, not evidence that every transport rejects
unknown fields: batch/GCS job configurations explicitly ignore unknown values.
No runtime behavior or tests were changed by this documentation pass.

## Primary sources checked 2026-11-27

- [Meltano SDK sink setup](https://sdk.meltano.com/en/latest/classes/singer_sdk.Sink.html#singer_sdk.Sink.setup): normal schema-change lifecycle.
- [BigQuery JSON](https://docs.cloud.google.com/bigquery/docs/json-data): JSON equality/ordering limitations and server constraints.
- [BigQuery schema changes](https://docs.cloud.google.com/bigquery/docs/managing-table-schemas): available server/operator changes are broader than the target's automatic updates.

Independent documentation review found no blocker; its wording refinement for
configured JSON key transforms was applied. Live schema migrations, execution of
view/MERGE/overwrite SQL and server acceptance were not tested in this pass.
