Status: done
Created: 2026-07-05
Updated: 2026-07-05
Parent: None
Depends-On: None

# CI Skip Legacy Streaming Insert

## Scope

Skip only the legacy `streaming_insert` BigQuery integration-test variants when running in GitHub Actions CI, because BigQuery free-tier projects reject legacy streaming inserts.

## Acceptance Criteria

- `streaming_insert` sync integration variants skip when `GITHUB_ACTIONS=true`.
- Non-legacy methods remain eligible in CI.
- The skip happens before constructing the target or BigQuery client for the legacy streaming variants.
- Local test runs outside GitHub Actions still keep the legacy streaming variants eligible when integration credentials are present.

## Explicit Exclusions

- No production sink behavior changes.
- No skip for `storage_write_api`, `batch_job`, or `gcs_stage`.

## Progress and Notes

- 2026-07-05: Added a test helper for the GitHub Actions legacy streaming skip and covered the skip/non-skip branches with local tests.
- 2026-07-05: Simulated GitHub Actions with fake BigQuery env vars; all three legacy streaming integration parametrizations skipped before credential use.

## Blockers

None.

## Evidence

- `.10x/evidence/2026-07-05-ci-skip-legacy-streaming-insert.md`
