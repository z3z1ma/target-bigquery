# Maintained roadmap

Updated: 2026-11-16. Owner: repository maintainer (execution delegated to Codex).
Baseline: `c0acf83` on main. This replaces the July zero-open snapshot as the
current work queue; the historical `.10x` records remain evidence for that date.

## Operating rules

Update this file in every roadmap implementation PR: status, exact tested commit,
acceptance results, next action, and any new issue discovered. Reconcile the open
issue/PR inventory at the start of each maintenance pass and after merges. Keep
completed work in the delivery log. A proposal becomes done only after reviewed
code and the required checks land on main; published releases are tracked
separately. Preserve original contributor commits or explicit attribution when
adapting their work. Refresh main and rerun affected checks before merging.

Priority follows data durability, blocked ingestion, operability, then features.
Offline failures must be reproducible without credentials. Live verification is
separate evidence and requires authorization. This maintenance pass authorizes
no BigQuery queries, writes, setup, spending, or release publication. The separate
live-overwrite task owns #139/#152; their branches remain untouched here.

## Current inventory and disposition

The initial 2026-11-16 API snapshot contained **5 open issues and 7 open PRs**.
These counts describe the audit baseline, not a promise about future counts.
Issues, comments, PR diffs, source and tests were inspected; the table records all
12 items. Main's CI baseline is #151 (`c0acf83`).

| Item | Classification and evidence | Decision / next action |
| --- | --- | --- |
| [#143](https://github.com/z3z1ma/target-bigquery/issues/143) Storage Write state | Confirmed durability bug. `storage_write.py:pre_state_hook` only commits streams; `core.py:pre_state_hook` performs MERGE. `target.py:drain_all` emits state after the sink hook. | P0, implemented in [#153](https://github.com/z3z1ma/target-bigquery/pull/153), CI pending. Calling the parent alone is insufficient: MERGE rotates staging, so refresh the Storage Write parent before the next batch. Also reject response-level commit errors before clearing streams. |
| [#148](https://github.com/z3z1ma/target-bigquery/issues/148) cleanup hang | Confirmed unbounded client waits in `core.py:merge_table/clean_up`; upstream reproduction supports the report. The report uses 0.7.2 but the unbounded calls remain on main. | P1, adapt #149 with failure tests and diagnostics. |
| [#141](https://github.com/z3z1ma/target-bigquery/issues/141) JSON | Confirmed compatibility bug, not a configuration misunderstanding. Resolver v2 emits JSON; protobuf expects string; denormalized `process_record` passes dicts to `ParseDict`. | P1, preserve #140 contribution and expand its coverage on current main. |
| [#134](https://github.com/z3z1ma/target-bigquery/issues/134) overwrite layout | Confirmed regression: initial table creation uses partitioning, replacement CTAS omits it. Separately, deliberately changing a pre-existing physical layout requires migration. | P1, blocked only on the separate #152 live gate. |
| [#150](https://github.com/z3z1ma/target-bigquery/issues/150) release | Valid maintenance request, not a runtime defect. Package version remains 0.7.2 although main includes later correctness changes. | Prepare evidence/changelog after correctness work; release publication requires separate authorization. No release date promised. |
| [#149](https://github.com/z3z1ma/target-bigquery/pull/149) `ee32bff2` | Correct direction: submission/result timeout for MERGE/overwrite. Missing timeout-failure/state tests, job-ID diagnostics and config/docs scope. Generated-view query still unbounded. CI run 39289842637 is `action_required`. | Adapt onto current main; do not merge from stale CI. |
| [#140](https://github.com/z3z1ma/target-bigquery/pull/140) `1841adf9` | Schema-guided recursive serialization appears sound. Existing tests cover objects, repeated JSON, nested JSON and protobuf parsing. Helper mutates its input. CI run 31814907445 failed. | Add scalar/null/repeated RECORD/transformed-name/fixed-mode tests; establish fresh CI. |
| [#139](https://github.com/z3z1ma/target-bigquery/pull/139) `b4a5c595` | Valid layout implementation. Validation runs after `create_target` and schema mutation; range partitioning can be mistaken for unpartitioned. Original repeated mock assertion does not execute two cleanup cycles. | Keep open until replacement #152 is validated and merged; preserve contributor attribution. |
| [#152](https://github.com/z3z1ma/target-bigquery/pull/152) `219efca5` | Follow-up moves compatibility checks before mutations, rejects range partitioning, documents migration and adds two real cleanup calls plus opt-in live tests. | Draft, separate owner. Require two live overwrites for batch_job and Storage Write, verifying rows and layout each time. Offline success does not satisfy this gate. |
| [#136](https://github.com/z3z1ma/target-bigquery/pull/136) `865a01be` | Pinned checkout patch update; escaping/whitespace fixes. No PR workflow run returned during audit. | P2, refresh on main and run Python 3.10–3.12 CI. |
| [#137](https://github.com/z3z1ma/target-bigquery/pull/137) `d829210c` | Pinned setup-python major update; removed `pip-install` input is unused here. No PR workflow run returned. | P2, preserve offline workflow and verify supported runtime matrix. |
| [#147](https://github.com/z3z1ma/target-bigquery/pull/147) `2ef432c9` | Pinned setup-uv major update; v9 changes cache pruning default to false. v10 sensitive-event cache rules do not affect current triggers. No PR workflow run returned. | P2, preserve prior cache behavior with `prune-cache: true`, then matrix CI. |

PR mergeability snapshots reported false on the five stale-base proposals; refresh
Git refs before classifying that as a real conflict. No issue above is closed
merely because a patch exists.

## Sequenced delivery and acceptance tests

### 1. P0 — durable Storage Write checkpoints (#143)

Status: [#153](https://github.com/z3z1ma/target-bigquery/pull/153), head `cca0eeb`.
163 offline tests passed, 13 skipped; Ruff and mypy pass. Independent review
completed and requested test additions addressed. Exact-head CI pending.

Required ordering: completed appends → successful finalize/commit → successful
MERGE → usable next staging destination → emitted STATE. Cover default and
pending stream semantics without implying end-to-end exactly-once delivery.

Acceptance:
- Exercise real `TargetBigQuery.drain_all` and real sink/base hooks; assert order.
- Finalize exceptions, commit exceptions, response `stream_errors`, MERGE failure
  and staging-creation failure prevent state. Retain failed stream tracking.
- Two intermediate checkpoints rotate distinct staging names; enqueue a batch
  between them and assert its `Job.parent` uses the new staging table.
- Fixed-schema sinks do not merge. EOF commit failure also suppresses state.
- Full offline suite, Ruff, mypy, independent review and exact-head matrix CI.
- Record live two-checkpoint verification as outstanding until separately
  authorized; never infer it from mocks.

### 2. P0 investigation — overwrite checkpoint durability

Status: source-backed follow-up, design not yet selected.

`BaseBigQuerySink.pre_state_hook` handles merge targets only; overwrite publication
occurs at EOF. Assess what a resumable tap does after an intermediate STATE and
process failure while data exists only in staging. This is distinct from physical
layout preservation. Acceptance: offline failure/restart trace demonstrating the
bookmark and destination behavior; choose deferred state or a recoverable staged
checkpoint protocol with explicit full-refresh semantics. Add a test proving no
unrecoverable rows are skipped. Investigate before declaring all write strategies
durable or preparing a release; do not silently introduce partial overwrites.

### 3. P1 — bounded query waits (#148/#149)

Status: audited; next implementation after the state patch and P0 investigation.

Use the existing numeric timeout for submission and result waiting, including
MERGE, overwrite and generated-view DDL. Record job ID when available and
re-raise failures. A timeout does not cancel a server-side job or prove it failed.

Acceptance: default and custom timeout reach both client calls; inject submission
and polling failures; no state at checkpoint or EOF; pointers/staging are not
cleared on failure; diagnostic contains job ID without record payloads. Document
RPC/retry/wait scope rather than promising a strict wall-clock process deadline.
Capture the original contribution when adapting #149.

### 4. P1 — JSON transport compatibility (#141/#140)

Status: audited; follows checkpoint correctness, can proceed independently of
live-overwrite approval.

Serialize only fields resolved as BigQuery JSON, recursively within RECORD and
repeated RECORD. Specify that Python strings are JSON scalar strings, not
pre-encoded documents. A nullable field's Python None remains SQL NULL; repeated
JSON null needs its own explicit test. Preserve ordinary STRING/RECORD behavior.

Acceptance: actual protobuf parse round-trips objects, arrays, Unicode, scalar
strings/numbers/booleans, null and empty values; repeated JSON and nested repeated
RECORD; transformed column names; unchanged fixed-schema path; document/test
input mutation. Test non-finite numbers and integer boundaries with an explicit
reject-or-preserve policy: a valid protobuf string does not establish BigQuery
JSON acceptance. Full offline suite and current-head matrix CI before merge.

### 5. P1 — layout-safe overwrite (#134/#139/#152)

Status: blocked on the separate owner's bounded live-test authorization.

Acceptance: existing compatible layout and new destination succeed; incompatible
partition field/type/expiration, ordered clustering and range partitions fail
before destination mutation; two overwrite runs each replace rows and retain
layout. Test batch_job and Storage Write. Reconcile #152 with then-current main
only through its owner. Migration instructions must cover legacy unpartitioned
tables and dataset expiration defaults. Close #139 as superseded only after the
replacement lands.

### 6. P2 — maintenance and capability documentation

Status: queued. Dependencies #136/#137/#147 can proceed without cloud access.

Keep the Python 3.10–3.12 offline matrix and locked installs green. Add a tested
method × fixed/denormalized × append/upsert/overwrite × JSON support matrix,
including schema-change limits, replay behavior and staging cleanup. Record
which features are cloud-verified versus offline-only. Prepare #150 release notes
from merged commits only, with migration and known-limit sections. Publishing
is outside this roadmap execution's authorization.

### 7. P3 — scoped replacement design spike

Status: research only; no implementation selected.

Evaluate `WRITE_TRUNCATE_DATA` against current CTAS replacement. The REST API
preserves destination schema/constraints and supports field addition/relaxation
options. This is not proof of layout, permission, metadata or client compatibility.
Acceptance: locked-client serialization check, documented job-type compatibility,
schema drift matrix (added/removed/relaxed fields), destination metadata/constraints
and partition/clustering preservation, failure atomicity and replay plan. Any live
experiment needs separate approval. Do not replace #152 with this speculative work.

## Research informing decisions

Sources reviewed 2026-11-16; conclusions are specific to documented behavior.

- [Singer SDK state](https://sdk.meltano.com/en/v0.54.9/implementation/state.html):
  checkpoints occur during drains, not just at EOF; test the emitted-state boundary.
- [BigQuery pending streams](https://docs.cloud.google.com/bigquery/docs/write-api-batch-load):
  commit responses can carry stream errors. Batch commit atomicity does not include
  a subsequent MERGE.
- [Storage Write best practices](https://docs.cloud.google.com/bigquery/docs/write-api-best-practices):
  retain default-stream/batching benefits; per-stream offsets are not a global
  exactly-once guarantee. Measure stream creation before adding tuning knobs.
- [Type mapping](https://docs.cloud.google.com/bigquery/docs/supported-data-types)
  and [JSON ingestion](https://docs.cloud.google.com/bigquery/docs/json-data):
  JSON uses protobuf string, including serialized scalar values.
- [QueryJob](https://docs.cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.QueryJob)
  and [upstream hang #18857](https://github.com/googleapis/google-cloud-python/issues/18857):
  explicit result timeout addresses unbounded waits; retries and transport behavior
  mean it is not a strict whole-operation deadline.
- [Partition migration](https://docs.cloud.google.com/bigquery/docs/manage-partition-cluster-recommendations):
  distinguish a software layout regression from an intentional migration.
- [dlt BigQuery](https://dlthub.com/docs/dlt-ecosystem/destinations/bigquery)
  separates timeout controls and documents destination capabilities;
  [replace strategies](https://dlthub.com/docs/general-usage/full-loading) expose
  atomicity/table-identity tradeoffs. Adopt clear contracts before new strategies.
- [Airbyte migration](https://docs.airbyte.com/integrations/destinations/bigquery-migrations)
  and [direct loading](https://docs.airbyte.com/platform/using-airbyte/core-concepts/direct-load-tables):
  current direct loading and temporary overwrite tables reinforce transport and
  schema correctness. Historical GCS staging is a useful comparison, not a reason
  to copy every feature or remove this target's GCS mode.
- [BigQuery Job API](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job):
  `WRITE_TRUNCATE_DATA` merits a bounded design spike, not an untested switch.
- [setup-uv v9](https://github.com/astral-sh/setup-uv/releases/tag/v9.0.0),
  [v10](https://github.com/astral-sh/setup-uv/releases/tag/v10.0.0),
  [setup-python v7](https://github.com/actions/setup-python/releases/tag/v7.0.0),
  [checkout v7.0.1](https://github.com/actions/checkout/releases/tag/v7.0.1):
  review action behavior changes as well as SHA updates.

## Delivery log

- 2026-11-16: audited all 5 issues and 7 PRs against `c0acf83`; read-only independent
  research and PR review completed. No cloud operations or releases performed.
- #151 remains the merged offline CI repair baseline. #152 remains a separate
  draft with live validation pending; this roadmap does not claim it is complete.

- #153 (`cca0eeb`): implemented state ordering, rotated staging routing and commit
  response rejection; 163 passed, 13 skipped offline, Ruff/mypy clean. No live test.
