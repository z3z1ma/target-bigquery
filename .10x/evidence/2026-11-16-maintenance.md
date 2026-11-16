# Maintenance audit and first delivery evidence

Date: 2026-11-16. Baseline main: c0acf83595d816bc8c8f4961a8984b7efb7ee120.
Current queue and status: [ROADMAP.md](../../ROADMAP.md).

## Audit

GitHub API search returned 5 open issues (#134, #141, #143, #148, #150) and
7 open PRs (#136, #137, #139, #140, #147, #149, #152). Read their bodies,
issue comments and PR diffs; compare the corresponding core, storage_write,
proto generation, target lifecycle, tests and workflow code. Independent read-only
research checked current official BigQuery/Singer documentation and dlt/Airbyte
behavior. The roadmap records exact audited PR heads and source URLs.

No repository AGENTS.md or .agents instructions were present. Existing `.10x`
specs require evidence before closure; the July zero-open record is historical.
The current task explicitly excludes live cloud work, overriding older specs'
credential-dependent live-test guidance. No credentials were read for cloud use.

## #143 / #153

Implementation head: cca0eeb8c325807ff3b80623b19387a2537960dd.
Merge: 8038818ef3acc2c54f765b5c1aee3c7aad4f02bb.

- Four initial checkpoint regression cases failed before the change.
- Final full offline suite: 163 passed, 13 skipped on Python 3.12.
- Ruff and mypy passed (20 source files).
- Independent review found no production defect; requested tests for an actual
  queued batch after rotation and response errors through drain_all were added.
- [CI run 43932834199](https://github.com/z3z1ma/target-bigquery/actions/runs/43932834199)
  passed at the exact implementation head with Python 3.10, 3.11 and 3.12.
- Main was refreshed before merge and remained c0acf83.
- Attribution to akalautaro's blueprint-data/target-bigquery#5 appears in commit
  and PR. The staging-parent refresh and response-error handling extend that fix.

## #155 / #156

Implementation head: fd3be44ab3788ed98920a94e41a910d06da44ad9.
Final head a6939136f0f01796875cafd6ae2f83e384cf9519 adds the reviewer-requested
documentation caveat about already-submitted remote publication.
Base: 8038818. See roadmap for subsequent CI/merge status.

- An offline probe invoking real TargetBigQuery.drain_all and the base sink hook
  recorded STATE while overwrite publication had not executed.
- Three new regression cases failed before the checkpoint change.
- Independent lifecycle review found SDK `_handle_termination` invokes EOF cleanup.
  Probe: SIGTERM produced exit 0, publication true, and emitted state. The patch
  overrides termination only when an overwrite is pending.
- Final full offline suite: 171 passed, 13 skipped; Ruff and mypy pass (21 files).
- New tests cover mixed streams, repeated checkpoints, partial cleanup failure,
  latest-state publication, append-only behavior, SIGINT and SIGTERM.
- Tests used the locked Python 3.12 environment with PYTHONPATH explicitly selecting
  the isolated overwrite-state checkout; imported module path was verified.

## Scope and limitations

No BigQuery queries, cloud writes, IAM changes, setup, spending or release.
#139/#152 branches were neither modified nor merged. Their two-pass live overwrite
gate remains with its separate owner. Storage Write live checkpoint/restart
validation also remains unperformed. Mocked control-flow tests do not prove remote
atomicity, query cancellation or global exactly-once delivery.

## Replacement design spike: offline client compatibility

Locked google-cloud-bigquery 3.42.1 exports `WriteDisposition.WRITE_TRUNCATE_DATA`.
QueryJobConfig and LoadJobConfig both serialize `write_disposition="WRITE_TRUNCATE_DATA"`
and `schema_update_options=["ALLOW_FIELD_ADDITION"]` to the expected REST keys.
This probe constructed configs only; no client, query or cloud request was made.
It establishes client-side representation, not server acceptance or metadata
preservation. The remaining design acceptance criteria stay in ROADMAP.md.
