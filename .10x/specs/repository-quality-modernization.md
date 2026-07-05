Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Repository Quality Modernization

## Purpose and Scope

This specification governs the 2026-07-05 repository modernization pass for `target-bigquery`.
The user explicitly authorized implementation, dependency/configuration mutation, committing, and pushing on `main`.

The work MUST prioritize:

1. Correctness and intended behavior.
2. Security and supply-chain hygiene.
3. Packaging and installation correctness.
4. Type and lint health.
5. Maintainability and test quality.

## Required Behavior

- The project MUST be converted from Poetry metadata and lock management to standard PEP 621 project metadata managed by uv.
- `poetry.lock` MUST be deleted.
- `uv.lock` MUST be generated and committed.
- Project packaging MUST continue to expose the `target-bigquery` console script.
- CI and developer documentation MUST stop instructing users or automation to use Poetry for this project.
- Quality work MUST be driven by objective tool output from the uploaded Production Python Quality Optimizer procedure.
- Tool findings MAY drive source cleanup and bug fixes when the behavior is record-backed by source, tests, or documented project behavior.
- Live BigQuery verification is available after the corrected service-account key path was provided. BigQuery behavior MUST be verified live where the project tier allows it, and externally blocked variants MUST be recorded with exact service errors rather than represented as locally passing.

## Constraints

- Work happens on `main`, per user instruction.
- Changes SHOULD be committed and pushed as coherent slices.
- External open GitHub issues and PRs are diagnostic context only; they MUST NOT introduce unratified feature scope.
- The implementation MUST avoid speculative abstractions and broad rewrites.
- Tool skips MUST be explicit and evidence-backed.
- Secret scanner output MUST be redacted if findings appear.
- Service-account key material MUST NOT be committed or printed. Evidence may name the user-provided credential path and the non-secret project id only.

## Acceptance Criteria

- Packaging conversion is complete: no Poetry build backend, no Poetry dependency tables, no `poetry.lock`, and `uv.lock` exists.
- Runtime dependencies required by imports are declared in project metadata.
- Developer and CI workflows use uv-compatible commands.
- Relevant fast and deep quality tools have been run or explicitly skipped with reasons.
- Discovered regressions or obvious source-proven bugs are either fixed, recorded as blockers/follow-up tickets, or explicitly documented as out of scope with rationale.
- Final evidence records map commands to results, limits, and remaining risk.
