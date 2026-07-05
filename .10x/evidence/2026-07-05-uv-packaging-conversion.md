Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/done/2026-07-05-uv-packaging-conversion.md, .10x/specs/repository-quality-modernization.md

# uv Packaging Conversion Evidence

## What Was Observed

The first modernization slice converted the project from Poetry metadata/lock management to PEP 621 metadata with uv, added `uv.lock`, updated CI/docs to uv, and fixed two packaging/runtime defects discovered by clean-environment checks.

## Procedure

- `uv lock`
  - Exit code: 0
  - Result: resolved 158 packages.
- `uv sync --frozen --extra dev`
  - Exit code: 0
  - Result: synchronized project and dev tools into `.venv`.
- `uv run --frozen ruff format .`
  - Exit code: 0
  - Result: 13 files already formatted after the canonicalization pass.
- `uv run --frozen ruff check .`
  - Exit code: 0
  - Result: all checks passed.
- `uv run --frozen pytest -q target_bigquery/tests/test_utils.py`
  - Exit code: 0
  - Result: 32 passed, 8 warnings. Warnings originate from third-party `singer-sdk`/`fs` deprecations around `jsonschema` and `pkg_resources`.
- `uv run --frozen target-bigquery --version`
  - Exit code: 0
  - Result: `target-bigquery v0.7.2, Meltano SDK v0.22.1`.
- `uv run --frozen python -c "import target_bigquery.target; print('import ok')"`
  - Exit code: 0
  - Result: import succeeded.
- `uv build`
  - Exit code: 0
  - Result: built `dist/z3_target_bigquery-0.7.2.tar.gz` and `dist/z3_target_bigquery-0.7.2-py3-none-any.whl`.
- `uv run --frozen python -m compileall -q target_bigquery`
  - Exit code: 0
  - Result: package compiled.
- `git diff --check`
  - Exit code: 0
  - Result: no whitespace errors.

## What This Supports

- Poetry metadata and lockfile removal is complete for the first slice.
- uv can resolve, sync, build, and run the CLI.
- The CLI version regression from distribution-name mismatch is fixed.
- The clean uv environment now includes direct dependencies for imported runtime packages: `proto-plus`, `protobuf`, and `setuptools<81` for the legacy `fs`/`pkg_resources` path.
- The overwrite cleanup branch no longer reassigns `self.table` from `merge_target` when `merge_target` is `None`.

## Limits

- Live BigQuery integration tests were not run because this workstream does not have live BigQuery access or credentials.
- Full test suite commands that require `BQ_CREDS`, `BQ_PROJECT`, `BQ_DATASET`, and `GCS_BUCKET` remain CI/live-environment checks.
- The later dependency-security slice removed the temporary direct `setuptools` runtime dependency after updating away from the vulnerable legacy dependency graph.
