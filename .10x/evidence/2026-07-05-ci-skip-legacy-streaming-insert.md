Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/done/2026-07-05-ci-skip-legacy-streaming-insert.md

# CI Skip Legacy Streaming Insert Evidence

## What Was Observed

GitHub Actions CI can hit `Access Denied: BigQuery: Streaming insert is not allowed in the free tier` for the legacy `streaming_insert` integration-test path. The repository now skips only that legacy method when `GITHUB_ACTIONS=true`.

## Procedure

- `uv run --frozen pytest -q target_bigquery/tests/test_ci_skips.py --timeout=300`
  - Exit code: 0
  - Result: 3 passed.
- `GITHUB_ACTIONS=true BQ_CREDS='{}' BQ_PROJECT='fake-project' BQ_DATASET='fake_dataset' GCS_BUCKET='fake-bucket' uv run --frozen pytest -q 'target_bigquery/tests/test_sync.py::test_basic_sync[no_batch_mode-streaming_insert]' --timeout=300`
  - Exit code: 0
  - Result: 1 skipped.
- `GITHUB_ACTIONS=true BQ_CREDS='{}' BQ_PROJECT='fake-project' BQ_DATASET='fake_dataset' GCS_BUCKET='fake-bucket' uv run --frozen pytest -q 'target_bigquery/tests/test_sync.py::test_basic_sync[batch_mode-streaming_insert]' --timeout=300`
  - Exit code: 0
  - Result: 1 skipped.
- `GITHUB_ACTIONS=true BQ_CREDS='{}' BQ_PROJECT='fake-project' BQ_DATASET='fake_dataset' GCS_BUCKET='fake-bucket' uv run --frozen pytest -q 'target_bigquery/tests/test_sync.py::test_basic_denorm_sync[streaming_insert]' --timeout=300`
  - Exit code: 0
  - Result: 1 skipped.
- `uv run --frozen pytest -q --timeout=300`
  - Exit code: 0
  - Result: 40 passed, 13 skipped.
- `uv run --frozen mypy .`
  - Exit code: 0
  - Result: no issues in 16 source files.
- `uv run --no-sync --with ty ty check`
  - Exit code: 0
  - Result: all checks passed.

## What This Supports

- The CI-only legacy streaming skip is narrow and does not hide non-legacy integration methods.
- The skip happens before target/client construction for the legacy streaming parametrizations.

## Limits

- Live BigQuery integration tests were not rerun here because this workspace does not have live BigQuery/GCS credentials.
