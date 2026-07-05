Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Raise Python Floor for 2026 Dependency Security

## Context

The uv migration initially preserved the old `>=3.8,<3.12` Python range. Security scanning then found high-volume transitive vulnerabilities from the old dependency set, especially through `singer-sdk==0.22.x` and its older dependency graph.

Current PyPI metadata observed on 2026-07-05 shows:

- `singer-sdk` latest stable `0.54.5` requires Python `>=3.10`.
- `google-cloud-bigquery` latest stable `3.42.1` requires Python `>=3.10`.
- `google-cloud-storage` latest stable `3.12.0` requires Python `>=3.10`.
- `orjson` latest stable `3.11.9` requires Python `>=3.10`.
- `grpcio-tools`, `grpcio-status`, `proto-plus`, and `protobuf` latest releases require Python `>=3.10`.

Keeping Python 3.8/3.9 would force the project to retain older dependency lines with known vulnerabilities or maintain a split dependency policy. The user explicitly authorized dependency updates and repository modernization.

## Decision

Raise the project Python floor to Python 3.10 for the modernization pass.

The project should verify local compatibility on Python 3.11 and Python 3.12 when possible. CI should cover Python 3.10, 3.11, and 3.12 after the dependency update.

## Alternatives Considered

- Preserve Python 3.8/3.9 and accept old dependency vulnerabilities.
  - Rejected because security and supply-chain hygiene outrank legacy interpreter support in the authorized modernization scope.
- Maintain conditional old/new dependency sets by Python version.
  - Rejected for this pass because it increases resolver complexity and still leaves vulnerable environments for older interpreters.
- Raise to Python 3.12 only.
  - Rejected because current dependency metadata supports 3.10+ and there is no record-backed reason to drop Python 3.10/3.11.

## Consequences

- Python 3.8 and 3.9 are no longer supported after this modernization pass.
- The lockfile can select modern dependency versions with security fixes.
- This is a breaking packaging compatibility change and must be visible in `pyproject.toml`, classifiers, CI, and evidence.
