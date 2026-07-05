Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Target: production hardening pushed through commit ceeafca
Verdict: pass

# Production Quality Burn-Down Review

## Target

Pushed `main` through commit `ceeafca`, covering production hardening for BigQuery correctness, selector semantics, JSON compatibility, worker fail-fast behavior, GCS client usage, Storage Write finalization, generated-view/config behavior, Storage Write nested proto handling, legacy streaming row-error handling, README/config documentation, and regression tests.

## Assumptions Tested

- Fixed-schema Singer validation must validate keys inside the wrapped `data` object rather than requiring top-level key fields.
- Upsert, overwrite, and dedupe selectors must treat env/Meltano strings such as `"false"` as false.
- Temporary overwrite/upsert table creation must use a BigQuery client even when the primary sink client is a GCS client.
- Denormalized merge and clustering keys must use transformed target column names.
- Merge SQL must tolerate schema drift by using only columns available on both source and target sides.
- JSON write paths must serialize `decimal.Decimal` values without crashing.
- Fail-fast worker errors must not advance state after a failed write.
- Storage Write stream close errors must not prevent finalization/commit handling for already-closed streams.

## Findings

- Minor: `merge_table` remains the highest-complexity block family; Radon/Complexipy exit zero, merge behavior has regression coverage, and further extraction should wait for the next functional change to avoid speculative churn.
- Minor: GCS staging live coverage remains externally blocked by the project billing account state. Source-backed GCS behavior is covered by unit tests, but live GCS staging should not be represented as verified.
- Minor: Legacy streaming insert live coverage remains externally blocked by the BigQuery free tier. CI now skips only that variant; returned row errors are unit-tested as worker failures, but live legacy streaming is not verified in this project.
- Minor: GitHub issue and PR burn-down is complete as of the final refresh: open issues `[]`, open pull requests `[]`.

## Verdict

Pass. No source-code blocker remains. The only residual limits are external live-service constraints recorded in `.10x/evidence/2026-07-05-production-quality-live-bigquery-verification.md`.

## Residual Risk

- Live GCS staging behavior must be rechecked under a project with enabled billing before making GCS-live claims.
- Legacy streaming insert must be rechecked under a project tier that permits streaming inserts before making live legacy-streaming claims.
