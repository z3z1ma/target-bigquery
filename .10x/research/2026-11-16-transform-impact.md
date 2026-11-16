# Transform impact investigation for #159

Investigated 2026-11-16 against a108a53; no cloud calls or file changes in the
independent investigation. Root investigation independently reproduced the
collision using both resolver versions and real generated protobuf messages.

## Confirmed impact

All four denormalized methods call `Denormalized.preprocess_record`, which calls
`SchemaTranslator.translate_record`. Nested/repeated RECORD schema names remain
raw below the top level; record names transform recursively. This mismatch
predates the JSON serializer. Fixed-schema raw ingestion bypasses that path.

Collision example with snake_case, both resolvers:

```python
schema = {"type": "object", "properties": {
    "payload": {"type": "object", "properties": {
        "CamelKey": {"type": "integer"},
        "camel_key": {"type": "integer"},
    }}
}}
source = {"payload": {"CamelKey": 1, "camel_key": 2}}
# SchemaTranslator(...).translate_record(source):
# {"payload": {"camel_key": 2}}
# ParseDict(..., proto_schema_factory_v2(translated_schema_transformed)()) succeeds.
# proto.payload.HasField("CamelKey") is False; proto.payload.camel_key is 2.
```

The loss occurs before network submission. Top-level collisions also lose values:
schema-only suffixing produces foo_bar/foo_bar_1 while the record keeps only the
last foo_bar value. JSON document keys also transform recursively and can collapse.
Thus collision containment is P0; the ordinary nested mismatch/migration is P1.

Batch/GCS load configurations use ignore_unknown_values=True. The
[Job API](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job)
documents ignoring unmatched JSON names when enabled. This creates additional
risk for mismatched nested payloads, but no server outcome was tested here.

## Compatibility findings

- snake_case, lower, digit-prefix and period replacement affect applicable nested
  names. SQL quoting differs between record and schema handling and needs a
  separate rendering contract.
- Existing tables can contain raw nested names. `Denormalized.update_schema`
  adds missing top-level fields only, so a recursive-schema change cannot safely
  migrate existing nested fields by itself.
- Fixed-schema views retain original source paths. Scalar/STRUCT aliases
  transform, but repeated outer aliases remain raw; colliding aliases duplicate.
- Schema suffixing without a corresponding record mapping is not safe collision
  resolution, including collisions with already-present suffix names.

## Recommended sequence

1. Reject ambiguous transforms before dictionary construction/buffering, with
   stream/path/original-name diagnostics. Preflight schema collisions before
   destination mutations. Preserve inputs and fixed-schema raw ingestion.
2. Design one schema-aware naming contract for schemas, records, key properties
   and view aliases. Recurse through typed RECORDs; treat JSON document keys as
   data and choose their compatibility behavior explicitly.
3. Define existing-table compatibility and migration before recursive physical
   renaming. Fail early with migration guidance; never silently drop/recreate.

Acceptance criteria and priority are maintained in ROADMAP.md. This investigation
is not a claim that #159 is fixed or that live migration safety has been verified.
