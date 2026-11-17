# 0.8.0 release notes

## Upgrade requirements

- Python3.10+ replaces the previous Python3.8–3.11 range. CI covers3.10–3.12.
- Dependencies move to modern Singer SDK, BigQuery, protobuf and storage clients;
  packaging uses Hatch/uv. Validate your deployment's dependency constraints.
- Transformed-name collisions now fail rather than silently discarding values.
  Rename conflicting source keys or adjust column transforms.
- Nested RECORD schema names now match record transforms. Existing nested schemas
  must have compatible names, order, types and modes; incompatible tables fail
  before mutation. Follow README's explicit migration guidance; no automatic
  rename/backfill is performed.
- Generated repeated-field view aliases now follow column transforms. Update
  downstream references that relied on the old raw aliases when views regenerate.
- Streaming upsert/overwrite staging templates must contain an unmodified
  `{uuid}` placeholder. Fixed or timestamp-only names now fail before table
  mutations because rapid reuse can silently lose streaming data. The default
  template already meets this requirement.
- Overwrite checkpoints wait until successful EOF publication. Coordinate source
  success checks and replay; multiple table replacements are not atomic together.

## Layout-safe overwrite

- Preserve configured partitioning, expiration and clustering during overwrite.
- Reject incompatible layouts before destination mutation; retain bounded query
  waits, deferred overwrite checkpoints and nested-schema compatibility checks.
- Per-stream layout overrides and unpartitioned mode are supported.

## Fixes and capabilities

- Storage Write commits and MERGE finish before STATE; staging rotation routes
  subsequent writes correctly and commit/append errors surface.
- Storage Write JSON fields serialize recursively, including nested/repeated
  fields, without mutating input. Invalid numeric/repeated values fail early.
- Query submission/result waits are bounded and failures report job diagnostics;
  timeouts do not cancel remote work or establish its outcome.
- Streaming-insert row errors and multiple schema/config/key edge cases are fixed.
- Period-name transforms, partition expiration, dedupe ordering and cleanup metrics
  include the previously unreleased contributor improvements.
- Capability, migration and operational guides now distinguish offline evidence
  from cloud validation. Original contributor attribution is retained in history.

## Validation

The offline suite passed 421 tests, with 15 credential-gated tests skipped;
Ruff and mypy passed. Both batch-job and Storage Write live two-pass overwrite
tests passed, checking exact rows, partitioning, expiration, and clustering after
each pass. All generated tables were verified deleted. See
[live evidence](live-overwrite-validation.md). Other transport/migration live
scenarios remain outside this verification scope.

Final release CI and artifact metadata/install checks are recorded in
[release readiness](release-readiness.md).
