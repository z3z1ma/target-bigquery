# 0.8.0 candidate release notes

Unpublished. Publication remains gated by live overwrite validation and final
artifact checks. These notes cover merged main9e7a99e; layout preservation from
#152 must be added only after its gate passes and it lands.

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
- Overwrite checkpoints wait until successful EOF publication. Coordinate source
  success checks and replay; multiple table replacements are not atomic together.

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

## Validation required before publication

Pass exact-release-head Python3.10–3.12 CI, rebuild wheel/sdist from that commit,
check metadata and install/CLI behavior from the artifacts, finalize the changelog,
and complete the separately authorized two-pass overwrite tests. Until then,
#150 remains open and no tag, GitHub release or package upload should be created.
