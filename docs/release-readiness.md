# 0.8.0 release readiness

Publication to PyPI (`z3-target-bigquery`) and GitHub is authorized after validation.
This file records preparation; a version change does not establish publication.

## Scope and validation

The candidate includes the merged durability, timeout, JSON, collision, nested
schema, view-alias and CI improvements, plus reconciled #152 layout preservation
and streaming staging-name containment. Contributor history remains preserved.
See [release notes](release-notes-0.8.0.md) for Python 3.10+, dependency, naming,
staging-template and migration requirements.

- Full local offline suite: 421 passed, 15 credential-gated skips. Ruff/mypy pass.
- Independent review: reconciliation, layout checks and UUID staging containment;
  38 additional offline cases cover early rejection and unaffected paths.
- Live gate passed: batch_job and storage_write_api, two overwrites each, exact
  rows/layout checked after each pass; all six final-run tables verified absent.
  [Detailed live evidence](live-overwrite-validation.md) includes earlier findings,
  query billing, cleanup and limits. No broader cloud verification is claimed.
- Final-head Python 3.10–3.12 CI, merged-main verification and final wheel/sdist
  build/install smoke checks are required before publication.

## Publication destinations and authentication

GitHub releases: `z3z1ma/target-bigquery`, version/tag `0.8.0`.
PyPI distribution: `z3-target-bigquery`, version `0.8.0`.
No release/tag/upload has been created by this preparation.

A read-only publishing preflight found no PyPI publishing secret or environment
in this repository, no local `.pypirc`, no TWINE/PYPI/UV_PUBLISH environment
credential, and no stored uv credential for the PyPI upload endpoint. An authorized
publishing credential or configured trusted publisher is needed to upload.
Do not close #150 until publication is verified. Do not publish superseded local
artifacts; build from the final release commit.
