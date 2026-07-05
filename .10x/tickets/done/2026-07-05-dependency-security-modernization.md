Status: done
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/done/2026-07-05-repository-modernization-plan.md
Depends-On: .10x/tickets/done/2026-07-05-uv-packaging-conversion.md

# Dependency Security Modernization

## Scope

Update dependency constraints and metadata to reduce vulnerability findings and support modern Python versions without relying on live BigQuery access.

## Governing Records

- `.10x/specs/repository-quality-modernization.md`
- `.10x/decisions/raise-python-floor-for-2026-dependency-security.md`
- `.10x/evidence/2026-07-05-uv-packaging-conversion.md`

## Acceptance Criteria

- `requires-python` and classifiers reflect the new Python support floor.
- Runtime dependency constraints are updated to current secure lines where local tests and static checks allow.
- Unused direct runtime dependencies are removed unless source or packaging evidence shows they are needed.
- `uv.lock` is regenerated.
- `uv audit` and OSV findings are reduced as far as reasonably possible.
- Ruff and pure unit tests continue to pass.
- Python 3.12 compatibility is locally checked if uv can run with the installed Python 3.12.

## Explicit Exclusions

- No live BigQuery integration testing.
- No feature implementation from open GitHub issues/PRs.

## Progress and Notes

- 2026-07-05: Baseline `uv audit --frozen` found 60 known vulnerabilities across 157 packages.
- 2026-07-05: OSV scan found 17 affected packages and 43 vulnerabilities in `uv.lock`.
- 2026-07-05: Baseline pydoclint found only constructor-docstring style findings.
- 2026-07-05: Baseline gitleaks directory scan found no leaks.
- 2026-07-05: Final local verification passed; see `.10x/evidence/2026-07-05-dependency-security-modernization.md`.
- 2026-07-05: Closure review passed; see `.10x/reviews/2026-07-05-repository-modernization-review.md`.

## Blockers

None.

## Evidence Expectations

- Updated lockfile resolution.
- `uv audit --frozen` and OSV scan after update.
- Ruff, pure tests, import/CLI smoke, build, compileall.
- Python 3.12 smoke/test result when available.
