Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Target: production hardening working tree before commit
Verdict: concerns

# Production Quality Burn-Down Review

## Target

Uncommitted working tree covering production hardening for BigQuery correctness, selector semantics, JSON compatibility, worker fail-fast behavior, GCS client usage, Storage Write finalization, README/config documentation, and regression tests.

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

- Significant: `merge_table` remains the highest-complexity block at Radon complexity 23. The current refactor keeps behavior local and tested, and the tool gate exits zero, but future merge changes should consider extracting schema-intersection and SQL-clause construction helpers.
- Significant: GCS staging live coverage remains externally blocked by the project billing account state. Source-backed GCS fixes are covered by unit tests, but live GCS staging should not be represented as verified.
- Significant: Legacy streaming insert live coverage remains externally blocked by the BigQuery free tier. CI now skips only that variant; source normalization is unit-tested, but live legacy streaming is not verified in this project.
- Minor: GitHub issue and PR burn-down has source-backed fixes for many correctness issues, but GitHub closure actions must wait until this commit is pushed.

## Verdict

Concerns raised, with no source-code blocker for committing this slice. The remaining concerns are either external live-service constraints or GitHub write actions that depend on the pushed commit.

## Residual Risk

- Live GCS staging behavior must be rechecked under a project with enabled billing before closing GCS-only claims.
- Legacy streaming insert must be rechecked under a project tier that permits streaming inserts before closing live legacy-streaming claims.
- Open GitHub issues/PRs still require post-push mapping and closure/comment actions.
