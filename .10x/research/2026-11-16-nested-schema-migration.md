# Nested schema alignment and migration decision (#159)

Baseline: main `7e45c6e`, following P0 collision containment #162. No cloud
operations or live migration tests performed.

## Decision

Transform copies of resolved BigQuery SchemaFields recursively, preserving their
metadata and caching. Use the record translator's physical names (`quote=False`);
reject source names containing backticks. JSON fields remain JSON and their
record keys retain the existing recursive transform policy. Fixed raw ingestion
and generated views remain unchanged. Their alias consistency and any future
JSON-key policy changes require separate compatibility work.

Before destination creation/update or staging, resolve the expected schema and
read the final destination metadata. Catch only NotFound as a new table. For
existing RECORDs require identical ordered child names, canonicalized types and
modes recursively. Reject missing/extra/renamed/reordered children and scalar ↔
RECORD changes. Recheck before updating top-level schema. Preserve supported
whole top-level additions and historical extra top-level fields.

This conservative nested contract applies to append/upsert/overwrite: whole
STRUCT assignments during MERGE make permissive order/subset matching unsafe
without a separate SQL mapping design. No nested evolution or automatic backfill
is implemented. Metadata checks are not transactional against external changes.

## Evidence and limits

Offline coverage: real protobuf round-trips with both resolvers; all four
transport preprocessors; actual batch/GCS compressed payloads; real base
constructors across append/upsert/overwrite; new and compatible destinations;
legacy/changed structures; permission errors; immutable schema metadata;
staging rotation/cache reuse; Singer failure before record/state processing.
Full suite: 339 passed, 13 skipped; Ruff and mypy pass. Independent review passed 109 focused tests; its backtick-validation clarification
was addressed with three additional regressions. Exact final-head CI evidence
belongs to the PR.

[BigQuery schema guidance](https://docs.cloud.google.com/bigquery/docs/managing-table-schemas)
describes explicit nested schema changes; changing schema does not constitute
historical value backfill. The [Job API](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job)
allows unmatched JSON fields to be discarded when ignoreUnknownValues is true.
That motivates matching nested schema before transport; no remote discard or
migration outcome was tested in this task.

README's Nested schema migration section requires a retained old destination,
explicit new-destination reload or mapped backfill, value/metadata validation,
and deliberate reader/writer/checkpoint cutover. Prior silent loss requires
source reconciliation. Live validation remains separately authorized work.
