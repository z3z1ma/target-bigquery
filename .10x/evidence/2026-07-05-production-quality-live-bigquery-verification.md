Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/2026-07-05-quality-procedure-completion-audit.md, .10x/tickets/2026-07-05-github-issue-pr-burn-down-plan.md, .10x/specs/repository-quality-modernization.md, .10x/specs/github-issue-pr-burn-down.md

# Production Quality and Live BigQuery Verification

## What Was Observed

The corrected service-account key path at `~/Documents/regal-scholar-336206-47839b5c155a.json` is readable. The key contents were not printed or committed.

Local quality gates and the live-compatible BigQuery integration subset pass after the production hardening slice.

## Procedure

- `uv lock --check && git diff --check`
- `uv run --no-sync ruff format --check . && uv run --no-sync ruff check . && uv run --no-sync --with ty ty check && uv run --no-sync mypy . && uv run --no-sync --with tach tach check`
- `uv run --no-sync pytest -q -n auto --timeout=300`
- `uv run --no-sync coverage run --branch -m pytest -q --timeout=300 && uv run --no-sync coverage report --show-missing && uv run --no-sync coverage json -o /tmp/target-bigquery-ai-quality/coverage.json`
- `uv run --no-sync --with radon radon cc target_bigquery -s -a -j > /tmp/target-bigquery-ai-quality/radon-cc.json && uv run --no-sync --with complexipy complexipy . && uv run --no-sync vulture target_bigquery --min-confidence 80 && uv run --no-sync deptry . && uv run --no-sync pydoclint target_bigquery`
- `uv audit --frozen && semgrep p/default && semgrep p/security-audit`
- `osv-scanner scan source -r . --format json --output /tmp/target-bigquery-ai-quality/osv.json`
- `gitleaks git --report-format json --report-path /tmp/target-bigquery-ai-quality/gitleaks-git.json`
- `gitleaks dir . --report-format json --report-path /tmp/target-bigquery-ai-quality/gitleaks-dir.json`
- CodeQL database creation and analysis to `/tmp/target-bigquery-ai-quality/codeql.sarif`
- `uv run --no-sync pytest target_bigquery/tests/test_core_pure.py::test_schema_translation_benchmark --benchmark-json=/tmp/target-bigquery-ai-quality/benchmark.json`
- `uv run --no-sync --with scalene scalene run --cpu-only -o /tmp/target-bigquery-ai-quality/scalene.json .venv/bin/pytest --- -q target_bigquery/tests/test_benchmarks.py::test_schema_translation_benchmark --timeout=60 --benchmark-disable`
- `uv run --no-sync --with memray memray run -q -f -o /tmp/target-bigquery-ai-quality/memray.bin -m pytest -q target_bigquery/tests/test_benchmarks.py::test_schema_translation_benchmark --timeout=60 --benchmark-disable && uv run --no-sync --with memray memray summary /tmp/target-bigquery-ai-quality/memray.bin > /tmp/target-bigquery-ai-quality/memray-summary.txt`
- Live-compatible BigQuery wrapper:
  - creates a throwaway dataset in project `regal-scholar-336206`
  - exports `BQ_CREDS`, `BQ_PROJECT`, `BQ_DATASET`, and a throwaway `GCS_BUCKET` value only to the subprocess environment
  - runs `uv run --no-sync pytest -q target_bigquery/tests/test_core.py target_bigquery/tests/test_sync.py -k 'not gcs_stage and not streaming_insert' --timeout=900`
  - deletes the dataset in a `finally` block

## Results

- Lock/diff: exit 0.
- Ruff format/check, ty, mypy, Tach: exit 0.
- Local pytest: exit 0, `127 passed, 13 skipped in 5.17s`.
- Coverage: exit 0, 70% line coverage, `1468/2007` lines, `218/398` branches.
- Radon/Complexipy/Vulture/Deptry/pydoclint: exit 0.
  - Radon measured 317 blocks, average complexity 2.55, max complexity 23 at `target_bigquery/core.py::merge_table`.
- uv audit: exit 0, no vulnerabilities in the locked environment.
- OSV: exit 0, 0 vulnerabilities.
- Gitleaks git and directory scans: exit 0, 0 findings.
- Semgrep default and security-audit scans: exit 0, 0 findings.
- CodeQL: exit 0, 0 SARIF results.
- jscpd: exit 0, 1.7342987034854354% duplication.
- Benchmark: exit 0, `test_schema_translation_benchmark` mean 67.922 microseconds.
- Scalene: exit 0, one benchmark test passed, JSON profile saved to `/tmp/target-bigquery-ai-quality/scalene.json`.
- Memray: exit 0, one benchmark test passed, binary profile saved to `/tmp/target-bigquery-ai-quality/memray.bin`, summary saved to `/tmp/target-bigquery-ai-quality/memray-summary.txt`.
- Live-compatible BigQuery subset: exit 0, `22 passed, 6 deselected in 299.40s`.
  - Created and deleted dataset `regal-scholar-336206.target_bigquery_codex_20260705015722`.
  - Cleanup confirmed the throwaway GCS bucket name was not found, as expected for the excluded GCS variants.

## External Limits Observed

- Full GCS staging live verification is blocked by project billing state. Bucket creation returned a Google Cloud `403` indicating the owning project's billing account is disabled in closed state.
- Legacy streaming insert live verification is blocked by the project tier. BigQuery returned `Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier`.
- GitHub Actions CI now skips only the legacy streaming-insert variant for that free-tier restriction; local non-CI execution still attempts it when credentials and project tier allow it.

## What This Supports

- The uv packaging conversion remains intact.
- The production hardening slice is compatible with the locked dependency set and static tooling.
- Batch load, Storage Write API, upsert, overwrite, generated-view-independent fixed-schema paths, denormalized paths, and standard Singer SDK target behavior pass against live BigQuery when not blocked by GCS billing or legacy streaming tier restrictions.
- No secret material was printed into the command output summarized here or added to the repository.

## Limits

- This evidence does not prove the externally blocked GCS staging or legacy streaming variants.
- This evidence does not by itself close GitHub issues or pull requests; it supports source and live-verification claims used by `.10x/reviews/2026-07-05-production-quality-burn-down-review.md`.
