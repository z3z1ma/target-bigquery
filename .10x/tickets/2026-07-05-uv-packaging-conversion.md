Status: active
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-repository-modernization-plan.md
Depends-On: None

# uv Packaging Conversion

## Scope

Convert the repository from Poetry-managed packaging to uv-managed PEP 621 packaging.

## Acceptance Criteria

- `pyproject.toml` uses `[project]` metadata and a non-Poetry build backend.
- Runtime and development dependencies are represented in PEP 621-compatible metadata.
- `poetry.lock` is deleted.
- `uv.lock` is generated.
- CI and README developer commands use uv instead of Poetry.
- The `target-bigquery` console script remains present.
- Relevant packaging checks pass.

## Explicit Exclusions

- No BigQuery live integration verification.
- No broad feature work from open GitHub issues or PRs.

## Progress and Notes

- 2026-07-05: Initial metadata inspected. Current project uses `[tool.poetry]`, Poetry dependency tables, `poetry-core`, and a Poetry lockfile.
- 2026-07-05: Clean uv import/CLI checks exposed `ModuleNotFoundError: No module named 'pkg_resources'` through `singer-sdk -> fs`; added explicit `setuptools` runtime dependency.
- 2026-07-05: CLI `--version` initially reported `[could not be detected]` because the target name differs from the distribution name; added a package-name version lookup override and unit coverage.
- 2026-07-05: Ruff identified an overwrite cleanup bug where the overwrite branch reassigned `self.table` from `merge_target`; fixed to retain `overwrite_target`.

## Blockers

None.

## Evidence Expectations

- `uv lock`
- `uv run --frozen python -m pytest ...` where feasible
- `uv build` or equivalent packaging build check if build tooling is available
