Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/2026-07-05-github-issue-pr-burn-down-plan.md

# GitHub Actions Pin Refresh

## What Was Observed

Dependabot opened PRs #129, #130, and #131 after the initial GitHub context snapshot. Each PR changes only `.github/workflows/ci.yml` to update an existing SHA-pinned action.

## Procedure

- Inspected the open PR metadata with `gh pr view`.
- Updated `.github/workflows/ci.yml` to the exact new action commit SHAs from the PR compare targets:
  - `actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0`
  - `actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1`
  - `astral-sh/setup-uv@fac544c07dec837d0ccb6301d7b5580bf5edae39`
- Ran YAML parsing and pin assertions with PyYAML.
- Ran `uv lock --check && git diff --check && uv run --no-sync pytest -q target_bigquery/tests/test_ci_skips.py --timeout=60`.

## Results

- YAML parsing and pin assertions: exit 0.
- Lock/diff/test command: exit 0, `3 passed in 0.78s`.

## What This Supports

PRs #129, #130, and #131 are superseded by the direct `main` update once committed and pushed.

## Limits

`actionlint` is not installed locally, so this evidence uses YAML parsing and repository tests rather than actionlint.
