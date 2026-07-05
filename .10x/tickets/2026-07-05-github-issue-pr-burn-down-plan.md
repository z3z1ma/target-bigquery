Status: done
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
- 2026-07-05: Production hardening slice addresses source-backed portions of #93, #99, #101, #103, #107, #108, #110, #118, #125, and #127 with regression tests.
- 2026-07-05: Live-compatible BigQuery subset passed against project `regal-scholar-336206`; GCS staging and legacy streaming remain externally blocked as recorded in `.10x/evidence/2026-07-05-production-quality-live-bigquery-verification.md`.
- 2026-07-05: New dependabot GitHub Actions PRs #129, #130, and #131 were absorbed by updating `.github/workflows/ci.yml` action SHAs on `main`; evidence is recorded in `.10x/evidence/2026-07-05-github-actions-pin-refresh.md`.
- 2026-07-05: Generated-view/config slice addresses #34, #39, #40, #104, and the remaining table-name portion of #108 with local and live BigQuery evidence in `.10x/evidence/2026-07-05-generated-view-config-verification.md`.
- 2026-07-05: Install/protobuf/stream-map slice addresses #111, #113, and #124 with isolated pip install, protobuf 6 generation, console-script, and alias-routing evidence in `.10x/evidence/2026-07-05-install-protobuf-stream-map-verification.md`.
- 2026-07-05: Storage Write nested slice fixes #71 and strengthens #76 evidence with live nested default-stream verification in `.10x/evidence/2026-07-05-storage-write-nested-verification.md`.
- 2026-07-05: Closed all remaining open GitHub issues and PRs with pushed-evidence comments. Refresh commands returned zero open issues and zero open PRs; evidence is recorded in `.10x/evidence/2026-07-05-github-zero-open-verification.md`.
- 2026-07-05: Final pushed source/test slice `ceeafca` added legacy streaming row-error handling and hill-climbed coverage/duplication metrics without reopening GitHub issues or pull requests.

## Blockers

- None for GitHub burn-down closure. GCS staging and legacy streaming live-verification limits are recorded as external limits in `.10x/evidence/2026-07-05-production-quality-live-bigquery-verification.md`.

## Evidence Expectations

- Commit hashes for each fix slice.
- Tool matrix after each coherent slice.
- GitHub issue/PR closure commands and URLs when closure occurs.
