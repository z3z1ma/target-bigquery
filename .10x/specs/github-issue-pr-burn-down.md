Status: active
Created: 2026-07-05
Updated: 2026-07-05

# GitHub Issue and PR Burn-Down

## Purpose and Scope

This specification governs the 2026-07-05 burn-down of current open GitHub issues and pull requests for `target-bigquery`.

It extends `.10x/specs/repository-quality-modernization.md`: open issues and PRs are no longer only diagnostic context for this workstream. The user explicitly expanded scope to internalize them, factor them into the record graph, and drive relevant items to zero when they are source-backed and verifiable.

## Behavior

- The implementation MUST preserve the uv/PEP 621 packaging conversion and must not reintroduce Poetry.
- Each open issue and PR observed in `.10x/research/2026-07-05-github-issue-pr-burn-down-context.md` MUST be classified as fixed, superseded, obsolete, documented, blocked, or intentionally not applicable before final burn-down closure.
- Source-backed correctness bugs MUST be fixed with focused tests where local or live BigQuery verification can exercise the behavior.
- Dependency-only PRs that are superseded by the uv lock and modern dependency constraints MAY be closed as obsolete only after dependency/security checks pass.
- Configuration values for stream-selection settings (`upsert`, `overwrite`, and `dedupe_before_upsert`) MUST treat boolean strings such as `"true"` and `"false"` as booleans, not as arbitrary truthy strings.
- Temporary-table creation for upsert/overwrite MUST use a BigQuery client even when the sink's primary client is a GCS client.
- Fail-fast worker errors MUST NOT write Singer state after a write failure.
- Denormalized key properties used in clustering, dedupe, and MERGE SQL MUST be transformed consistently with denormalized record/table column transforms.
- Overwrite replacement SHOULD avoid a separate destructive drop-before-create sequence when BigQuery supports a safer single replacement statement.
- MERGE behavior MUST avoid referencing columns unavailable on either side when schema drift is expected or configured.

## Acceptance Criteria

- Durable records exist for GitHub context, implementation tickets, evidence, and review.
- All currently open GitHub issues and PRs are mapped to closure status or an explicit blocker.
- Local procedure tools from the Production Python Quality Optimizer specification exit zero unless blocked by missing external state and recorded.
- Live BigQuery tests are run when credentials are available. If credentials are missing, the exact credential lookup failure is recorded and local/static evidence is not mislabeled as live verification.
- No service-account key, private key, token, or secret material is committed or printed.
- The final evidence maps each relevant command and each issue/PR closure claim to observed results.

## Explicit Exclusions

- No speculative feature broadening beyond the current open issues/PRs and source-backed correctness defects.
- No secret rotation or GCP IAM mutation.
- No issue/PR closure on GitHub until the corresponding fix/supersession evidence exists.
