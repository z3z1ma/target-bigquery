Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/2026-07-05-github-issue-pr-burn-down-plan.md

# Install, Protobuf, and Stream-Map Verification

## What Was Observed

Modernized packaging installs in a vanilla pip virtualenv, exposes the `target-bigquery` console script, imports the package version, and generates protobuf message classes with protobuf 6 for nested and repeated record schemas.

The upgraded Singer SDK stream-map alias path routes both SCHEMA and RECORD messages to the alias stream.

## Procedure

- Created `/tmp/target-bigquery-pip-smoke` with `python3 -m venv`.
- Ran pip upgrade and `pip install .` from the repository.
- In that isolated venv:
  - imported `TargetBigQuery`
  - imported and ran `proto_schema_factory_v2`
  - generated a protobuf class from a schema containing scalar, nested record, and repeated record fields
  - instantiated the generated message class and asserted nested/repeated values
  - asserted `TargetBigQuery.plugin_version` is detected
  - ran `/tmp/target-bigquery-pip-smoke/bin/target-bigquery --help`
- Added and ran `test_stream_maps_alias_schema_and_records_route_to_alias`.
- Ran `uv run --no-sync pytest -q target_bigquery/tests/test_target.py --timeout=120`.

## Results

- Isolated pip/protobuf/console-script smoke: exit 0, `pip_install_proto_smoke=pass`.
- Target tests: exit 0, `20 passed in 0.45s`.

## What This Supports

- Issue #113 is addressed for the current packaging surface: vanilla pip installation from the repository succeeds.
- Issue #124 is addressed for the current protobuf surface: protobuf 6 works with `message_factory.GetMessageClass` and nested/repeated Storage Write schema generation.
- Issue #111 is addressed by the Singer SDK upgrade and regression coverage for alias routing.

## Limits

- This is an install smoke from the repository checkout, not a published PyPI artifact install.
- The protobuf smoke validates class generation and message construction; full live Storage Write ingestion is covered separately by the live-compatible BigQuery subset.
