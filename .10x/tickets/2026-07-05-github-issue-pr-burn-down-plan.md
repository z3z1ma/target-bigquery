Status: active
Created: 2026-07-05
Updated: 2026-07-05
Depends-On: .10x/specs/github-issue-pr-burn-down.md, .10x/research/2026-07-05-github-issue-pr-burn-down-context.md

# GitHub Issue and PR Burn-Down Plan

## Scope

Burn down the 18 open issues and 10 open PRs observed on 2026-07-05 by fixing, superseding, documenting, or blocking each item with evidence.

## Workstreams

1. Modernization/obsolete PR closure:
   - Issues: #109, #111, #113.
   - PRs: #116, #62, #58, #56, #29, #26, #23.
2. Configuration and documentation:
   - Issues: #93, #104, #110.
3. BigQuery write correctness:
   - Issues: #99, #101, #103, #105, #107.
   - PRs: #118, #125, #127.
4. Storage Write and proto behavior:
   - Issues: #71, #76, #124.
5. Generated-view and schema resolver behavior:
   - Issues: #34, #39, #40.
6. Optional/customization behavior:
   - Issue: #108.

## Acceptance Criteria

- Every issue and PR listed above has a final status in evidence: fixed, superseded by commit, documented, blocked with exact reason, or closed as obsolete.
- Source-backed bugs in workstreams 2 and 3 have regression tests.
- Storage Write and generated-view claims are verified by live BigQuery when credentials are available, or explicitly left blocked rather than misrepresented.
- GitHub write actions, if any, happen only after matching commits are pushed.

## Progress and Notes

- 2026-07-05: Raw issue/PR exports and per-item detail artifacts stored under `.10x/research/.storage/`.
- 2026-07-05: Synthesis recorded in `.10x/research/2026-07-05-github-issue-pr-burn-down-context.md`.

## Blockers

- Live BigQuery/GCS key not found at the path named by the user.

## Evidence Expectations

- Commit hashes for each fix slice.
- Tool matrix after each coherent slice.
- GitHub issue/PR closure commands and URLs when closure occurs.
