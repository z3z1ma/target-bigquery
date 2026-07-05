Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/done/2026-07-05-dependency-security-modernization.md, .10x/specs/repository-quality-modernization.md, .10x/decisions/raise-python-floor-for-2026-dependency-security.md

# Dependency Security Modernization Evidence

## What Was Observed

The second modernization slice raised the Python floor to 3.10, updated runtime dependencies to current secure lines, removed obsolete direct runtime dependencies, added uv development dependency groups for local quality tools, improved static-analysis findings, and added local regression coverage for worker lifecycle behavior.

## Procedure

- `uv lock --check`
  - Exit code: 0
  - Result: resolved 88 packages.
- `uv sync --frozen`
  - Exit code: 0
  - Result: synchronized the default environment.
- `uv run --frozen pytest -q --timeout=300`
  - Exit code: 0
  - Result: 37 passed, 13 skipped. Skips are live BigQuery/GCS integration tests without `BQ_CREDS`, `BQ_PROJECT`, `BQ_DATASET`, or `GCS_BUCKET`.
- `uv run --python 3.10 --frozen pytest -q --timeout=300`
  - Exit code: 0
  - Result: 37 passed, 13 skipped, 2 warnings. Warnings are Google library notices that Python 3.10 support ends after 2026-10-04.
- `uv run --python 3.12 --frozen pytest -q --timeout=300`
  - Exit code: 0
  - Result: 37 passed, 13 skipped.
- `uv run --frozen ruff format --check .`
  - Exit code: 0
  - Result: 15 files already formatted.
- `uv run --frozen ruff check .`
  - Exit code: 0
  - Result: all checks passed.
- `uv run --no-sync --with ty ty check`
  - Exit code: 0
  - Result: all checks passed.
- `uv run --frozen mypy .`
  - Exit code: 0
  - Result: no issues in 15 source files.
- `uv run --frozen deptry .`
  - Exit code: 0
  - Result: no dependency issues found.
- `uv run --frozen pydoclint target_bigquery`
  - Exit code: 0
  - Result: no violations.
- `uv run --frozen vulture`
  - Exit code: 0
  - Result: no high-confidence unused-code findings with project threshold `min_confidence = 80`.
- `uv audit --frozen`
  - Exit code: 0
  - Result: no known vulnerabilities or adverse project statuses in 87 packages.
- `osv-scanner scan source -r .`
  - Exit code: 0
  - Result: no issues found; scanned 88 packages from `uv.lock`.
- `gitleaks dir --no-banner --redact .`
  - Exit code: 0
  - Result: no leaks found.
- `uv run --no-sync --with semgrep semgrep scan --config p/default --error .`
  - Exit code: 0
  - Result: 0 findings across 329 rules.
- `codeql database create ...` and `codeql database analyze ... python-security-and-quality.qls`
  - Exit code: 0
  - Result: 0 SARIF results after reducing the initial 43 CodeQL findings.
- `uv run --no-sync --with complexipy complexipy target_bigquery`
  - Exit code: 0
  - Result: all functions within the allowed complexity threshold.
- `uv run --no-sync --with radon radon cc target_bigquery -s`
  - Exit code: 0
  - Result: highest remaining function-level ratings are C for `transform_column_name` and `_bigquery_field_to_projection`; no D/E/F functions remain from the pre-refactor hotspot.
- `uv run --no-sync --with radon radon mi target_bigquery -s`
  - Exit code: 0
  - Result: most files rate A; `target_bigquery/core.py` remains C due to legacy module size.
- `npx --yes jscpd target_bigquery --reporters console --silent`
  - Exit code: 0
  - Result: 7 exact clones, 55 duplicated lines, 1.39% duplicated lines.
- `uv run --frozen coverage run --branch -m pytest -q --timeout=300` plus coverage JSON summary
  - Exit code: 0
  - Result: 44% line coverage, 96/374 covered branches. Coverage is limited by skipped live BigQuery/GCS integration tests.
- `uv build`
  - Exit code: 0
  - Result: built `dist/z3_target_bigquery-0.7.2.tar.gz` and `dist/z3_target_bigquery-0.7.2-py3-none-any.whl`.
- `uv run --frozen python -m compileall -q target_bigquery`
  - Exit code: 0
  - Result: package compiled.
- `uv run --frozen target-bigquery --version`
  - Exit code: 0
  - Result: `target-bigquery v0.7.2, Meltano SDK v0.54.5`.
- `git diff --check`
  - Exit code: 0
  - Result: no whitespace errors.

## What This Supports

- Dependency vulnerability findings were reduced from `uv audit` 60 vulnerabilities and OSV 43 vulnerabilities to zero reported findings.
- Python support metadata, classifiers, and CI now target Python 3.10, 3.11, and 3.12.
- Local unit/static/security tooling passes after dependency updates.
- CodeQL findings were reduced from 43 to zero by removing type-only cycles, fixing hash/equality mismatches, removing a no-op statement, and simplifying destructor/import patterns.
- Complexity hotspots were reduced: `SchemaTranslator._jsonschema_property_to_bigquery_column` no longer fails complexity checks, `StorageWriteBatchWorker.run` dropped to passing complexity, and `TargetBigQuery.drain_all` dropped to passing complexity.
- The `TargetBigQuery.drain_all` end-of-pipe worker shutdown bug is covered by a local regression test.
- Full local pytest no longer fails on absent integration secrets; live tests skip with explicit credential requirements.

## Limits

- No live BigQuery or GCS integration test was executed in this workstream.
- Radon maintainability for `target_bigquery/core.py` remains C because the file is a large legacy multipurpose module. A module split would be a separate behavior-risking refactor and was not attempted without live integration coverage.
- Coverage remains modest because the uncredentialed local suite cannot exercise live BigQuery write paths.
