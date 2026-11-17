# Maintained roadmap

Updated: 2026-11-17. Owner: repository maintainer (execution delegated to Codex).
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
separate evidence. The owner authorized two generated-data overwrite tests in the
existing CI dataset, cleanup of their temporary tables, and up to $1 usage.
Version 0.8.0 publication to PyPI and GitHub is authorized after validation.
Other cloud experiments remain outside this scope.

## Current inventory and disposition

The initial 2026-11-16 API snapshot contained **5 open issues and 7 open PRs**.
These counts describe the audit baseline, not a promise about future counts.
Issues, comments, PR diffs, source and tests were inspected; the table records all
12 items. Main's CI baseline is #151 (`c0acf83`).

| Item | Classification and evidence | Decision / next action |
| --- | --- | --- |
| [#143](https://github.com/z3z1ma/target-bigquery/issues/143) Storage Write state | Confirmed durability bug. `storage_write.py:pre_state_hook` only commits streams; `core.py:pre_state_hook` performs MERGE. `target.py:drain_all` emits state after the sink hook. | P0, implemented in [#153](https://github.com/z3z1ma/target-bigquery/pull/153), merged. Calling the parent alone is insufficient: MERGE rotates staging, so refresh the Storage Write parent before the next batch. Also reject response-level commit errors before clearing streams. |
| [#148](https://github.com/z3z1ma/target-bigquery/issues/148) cleanup hang | Confirmed unbounded client waits in `core.py:merge_table/clean_up`; upstream reproduction supports the report. The report uses 0.7.2 but the unbounded calls remain on main. | Done: #157 merged (`9864f87`), including failure tests and diagnostics. |
| [#141](https://github.com/z3z1ma/target-bigquery/issues/141) JSON | Confirmed compatibility bug, not a configuration misunderstanding. Resolver v2 emits JSON; protobuf expects string; denormalized `process_record` passes dicts to `ParseDict`. | Done: #160 merged (`a12cf40`), retaining #140 contribution and expanded coverage. |
| [#134](https://github.com/z3z1ma/target-bigquery/issues/134) overwrite layout | Confirmed regression: initial table creation uses partitioning, replacement CTAS omits it. Separately, deliberately changing a pre-existing physical layout requires migration. | Done: #152 merged as e9709ec after live gate and exact-head CI passed. |
| [#150](https://github.com/z3z1ma/target-bigquery/issues/150) release | Valid maintenance request, not a runtime defect. Package version remains 0.7.2 although main includes later correctness changes. | 0.8.0 prepared in #166; publication authorized after validation. PyPI authentication remains unresolved; keep open until published. |
| [#149](https://github.com/z3z1ma/target-bigquery/pull/149) `ee32bff2` | Correct direction: submission/result timeout for MERGE/overwrite. Missing timeout-failure/state tests, job-ID diagnostics and config/docs scope. Generated-view query still unbounded. CI run 39289842637 is `action_required`. | Incorporated with original authorship into merged #157; closed. |
| [#140](https://github.com/z3z1ma/target-bigquery/pull/140) `1841adf9` | Schema-guided recursive serialization appears sound. Existing tests cover objects, repeated JSON, nested JSON and protobuf parsing. Helper mutates its input. CI run 31814907445 failed. | Incorporated with original authorship into merged #160; closed. Separate nested-name/collision work is #159. |
| [#139](https://github.com/z3z1ma/target-bigquery/pull/139) `b4a5c595` | Valid layout implementation. Validation runs after `create_target` and schema mutation; range partitioning can be mistaken for unpartitioned. Original repeated mock assertion does not execute two cleanup cycles. | Closed as incorporated through merged #152; contributor commits retained. |
| [#152](https://github.com/z3z1ma/target-bigquery/pull/152) `219efca5` | Follow-up moves compatibility checks before mutations, rejects range partitioning, documents migration and adds two real cleanup calls plus opt-in live tests. | Merged as e9709ec after independent review, both live tests, and exact-head CI43993914278. |
| [#136](https://github.com/z3z1ma/target-bigquery/pull/136) `865a01be` | Pinned checkout patch update; escaping/whitespace fixes. No PR workflow run returned during audit. | Done: incorporated into #158 (`f4f9739`); current-head matrix passed. |
| [#137](https://github.com/z3z1ma/target-bigquery/pull/137) `d829210c` | Pinned setup-python major update; removed `pip-install` input is unused here. No PR workflow run returned. | Done: incorporated into #158 (`f4f9739`); current-head matrix passed. |
| [#147](https://github.com/z3z1ma/target-bigquery/pull/147) `2ef432c9` | Pinned setup-uv major update; v9 changes cache pruning default to false. v10 sensitive-event cache rules do not affect current triggers. No PR workflow run returned. | Done: #158 (`f4f9739`) retains `prune-cache: true`; matrix passed. |

PR mergeability snapshots reported false on the five stale-base proposals; refresh
Git refs before classifying that as a real conflict. No issue above is closed
merely because a patch exists.

## Sequenced delivery and acceptance tests

### 1. P0 — durable Storage Write checkpoints (#143)

Status: [#153](https://github.com/z3z1ma/target-bigquery/pull/153), head `cca0eeb`.
163 offline tests passed, 13 skipped; Ruff and mypy pass. Independent review
completed and requested test additions addressed. Python 3.10–3.12 CI passed
at the exact head (run 43932834199); merged as `8038818`.

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

### 2. P0 — overwrite checkpoint durability (#155)

Status: [#156](https://github.com/z3z1ma/target-bigquery/pull/156), head `a693913`,
fixes [#155](https://github.com/z3z1ma/target-bigquery/issues/155); merged as `cac1981`
after exact-head Python 3.10–3.12 CI run 43934050631 passed.
171 offline tests passed, 13 skipped; Ruff and mypy pass. Independent review's
termination finding and signal-test requests are addressed.

The real target drain reproduced intermediate STATE before overwrite publication.
The chosen contract defers global state while any overwrite is pending, including
mixed streams. EOF publishes all replacements before emitting the latest state.
SIGINT/SIGTERM stop workers without initiating publication or state and exit
nonzero. An already-submitted remote query cannot be undone by signal handling.

Acceptance: two intermediate checkpoints emit nothing; successful EOF emits the
latest state; failure after one of several tables publishes emits nothing;
append/upsert-only checkpoints remain unchanged; SIGINT/SIGTERM with mixed sinks
skip cleanup/state and preserve normal termination behavior without overwrites.
Documentation must explain replay, lack of cross-table atomicity, temporary-table
expiration and upstream success checks (a failed tap closing stdin looks like EOF).
These offline criteria pass. Live failure/restart validation remains outstanding.

### 3. P0 — transform collision containment (#159)

Status: implemented in [#162](https://github.com/z3z1ma/target-bigquery/pull/162),
merged as `8a6c0ba`. Independently reviewed code head `d8c4605`; 99 focused
tests passed independently. Final head `e66cea5` passed Python 3.10–3.12 CI
run 43942188040. Local: 260 passed, 13 skipped; Ruff/mypy pass. P0 containment
is complete; #159 stays open for P1 naming and migration work. Both resolvers
silently collapse colliding keys before transport. With snake_case, nested
`CamelKey: 1` and `camel_key: 2` become only `camel_key: 2`; actual protobuf
parsing succeeds with the first field absent. Top-level fields and transformed
JSON keys are also affected. All four denormalized methods share this path.
This data-integrity risk takes priority over further feature work.

Implemented fail-fast collision detection before dictionary construction and
buffering/state, with path/source-name diagnostics. Cover top-level, nested and
repeated RECORDs, legacy-transformed JSON objects, case/punctuation collisions,
and already-existing suffix names. Validate schema ambiguity before destination
mutations; the existing schema-only suffixing does not preserve record values.
Keep original inputs unchanged and protect fixed-schema raw ingestion.

P1 schema/record alignment merged in #164 as `3ebd3ad` after independent
review (109 focused tests) and final-head CI run 43947355286 passed. Local: 339 passed, 13 skipped;
Ruff/mypy pass. Resolved physical RECORD fields
transform recursively using the record naming rules; SQL quotes are excluded and
prequoted source names rejected. Preflight requires exact ordered nested
structures before destination/staging mutations in append/upsert/overwrite, and
rechecks before top-level schema updates. New top-level fields remain supported;
nested changes require explicit migration. README documents new-destination
reload or backfill/cutover, including historical loss and metadata risks. JSON-key
transforms and fixed-mode view aliases deliberately retain existing behavior.

Acceptance: both resolvers, all denormalized preprocessing paths, every transform,
actual protobuf and captured other-method payloads; stable schema cache; collision
rejection with no data/state advancement; explicit legacy-table compatibility;
consistent scalar/STRUCT/repeated view aliases while preserving source JSON paths.
JSON document-key behavior remains legacy-compatible in this delivery; any
future change requires a separate compatibility decision. Fixed-mode repeated
view aliases remain a separate documentation/capability follow-up. Keep SQL
quoting separate from logical physical names. Migration/server validation remains
subject to separate cloud authorization. See [#159](https://github.com/z3z1ma/target-bigquery/issues/159)
and [.10x/research/2026-11-16-transform-impact.md](.10x/research/2026-11-16-transform-impact.md).

### 4. P1 — bounded query waits (#148/#149)

Status: merged through [#157](https://github.com/z3z1ma/target-bigquery/pull/157)
as `9864f87`, preserving #149 contributor commit. 192 passed / 13 skipped;
Ruff/mypy and independent review passed; exact-head Python 3.10–3.12 CI
run 43935751192 passed. #148 is fixed and #149 closed as incorporated. Views, MERGE and overwrite share bounded waits; invalid timeout
values fail before submission and failure diagnostics record job ID/location.

Use the existing numeric timeout for submission and result waiting, including
MERGE, overwrite and generated-view DDL. Record job ID when available and
re-raise failures. A timeout does not cancel a server-side job or prove it failed.

Acceptance: default and custom timeout reach both client calls; inject submission
and polling failures; no state at checkpoint or EOF; pointers/staging are not
cleared on failure; diagnostic contains job ID without record payloads. Document
RPC/retry/wait scope rather than promising a strict wall-clock process deadline.
Capture the original contribution when adapting #149.

### 5. P1 — JSON transport compatibility (#141/#140)

Status: merged through [#160](https://github.com/z3z1ma/target-bigquery/pull/160)
as `a12cf40`, preserving #140 author/commit. Final head `a108a53` passed
Python 3.10–3.12 CI run 43938944542 after reconciliation with current main.
227 local tests passed, 13 skipped; Ruff/mypy and independent review passed.
#141 is fixed and #140 closed as incorporated. No live server test claimed.

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

### 6. P1 — layout-safe overwrite (#134/#139/#152)

Status: #152 reconciled with main9e7a99e locally; 383 tests passed, 15 skipped,
Ruff/mypy passed. Layout-preserving overwrite DDL now uses the bounded query
helper. Current checkpoint and nested-schema guarantees are retained.
Independent review approved the reconciliation. Exact-head CI aabc5de passed
(run43982654420). Live validation exposed two fixture/configuration hazards:
monthly partitions require retention beyond the current month boundary, and
reusing a deleted staging name produced an empty second Storage Write overwrite.
Distinct staging names passed both transports. Require UUID-bearing templates
for streaming upsert/overwrite before destination mutation; append paths and
job-based custom templates retain their behavior. Final UUID-template live validation passed both methods (2 tests); all six tables
were verified absent. Offline: 421 passed, 15 skipped; Ruff/mypy pass. Exact-head CI43993914278 passed; merged as e9709ec. See [live evidence](docs/live-overwrite-validation.md).

Acceptance: existing compatible layout and new destination succeed; incompatible
partition field/type/expiration, ordered clustering and range partitions fail
before destination mutation; two overwrite runs each replace rows and retain
layout. Test batch_job and Storage Write. Reconcile #152 with then-current main
with current main. Migration instructions must cover legacy unpartitioned
tables and dataset expiration defaults. Close #139 as superseded only after the
replacement lands.

### 7. P2 — maintenance and capability documentation

Status: dependency PRs #136/#137/#147 incorporated into merged #158 (`f4f9739`),
with all original commits preserved and current-head Python 3.10–3.12 CI green.
Capability documentation and release preparation are implemented in the
`fix/view-array-aliases` follow-up: `docs/capabilities.md` maps configured paths
to offline tests and cloud gates; `docs/release-readiness.md` prepares #150 from
merged history and records an offline wheel/sdist build plus CLI smoke check.
Repeated view aliases are corrected with scalar/RECORD/nested/quote regressions.
Independent review passed 84 focused tests; local suite 278 passed, 13 skipped,
Ruff/mypy clean. Exact-head CI is required before merging this follow-up.

Keep the Python 3.10–3.12 offline matrix and locked installs green. Add a tested
method × fixed/denormalized × append/upsert/overwrite × JSON support matrix,
including schema-change limits, replay behavior and staging cleanup. Record
which features are cloud-verified versus offline-only. Prepare #150 release notes
from merged commits only, with migration and known-limit sections. Publishing is authorized after required validation succeeds.

### 8. P3 — scoped replacement design spike

Status: offline spike completed in #168 (`11da304`). Eight maintained tests
verify query/load configuration and the actual client request boundary, including
a counterexample showing serialization is not server validation. Retain CTAS;
see [compatibility decision](docs/overwrite-compatibility.md) for the proposed
publisher lifecycle and schema/metadata server-acceptance matrix.

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

- #153 merged as `8038818` after exact-head CI run 43932834199 succeeded.
- #155: offline reproduction confirms intermediate state precedes overwrite
  publication; #156 (`a693913`) merged as `cac1981` after CI run 43934050631
  passed. Local results: 171 passed / 13 skipped, Ruff/mypy pass.

- Inventory after #153/#156: 4 open issues (#134, #141, #148, #150) and the
  original 7 open PRs (#136, #137, #139, #140, #147, #149, #152), excluding
  roadmap publication #154. #143 and #155 are closed by their merged fixes.

- JSON regression work reproduced a pre-existing nested RECORD transform mismatch:
  schema transformation changes top-level names only, while record preprocessing
  transforms recursively. Tracked as [#159](https://github.com/z3z1ma/target-bigquery/issues/159), P1 with existing-table migration
  requirements; the JSON fix supports transformed top-level names and canonical
  nested names and explicitly documents this limit.

- Action refresh implementation: contributor commits from #136/#137/#147 are
  preserved in `ci/refresh-action-pins`; explicit `prune-cache: true` retains
  prior cache behavior. Merged as #158 (`f4f9739`) after exact-head
  Python 3.10–3.12 CI run 43936552874 passed. Original PRs closed as incorporated.

- Follow-up delivery: #157 timeout (`9864f87`), #158 actions (`f4f9739`), and
  #160 JSON (`a12cf40`) merged after independent review and exact-head matrix CI.
  Original #149/#136/#137/#147/#140 closed as incorporated with attribution.
- Current backlog after those merges: 3 issues (#134, #150, #159) and 2 original
  PRs (#139/#152), excluding this documentation PR. #159 collision containment
  landed in #162; broader schema/migration work remains P1. No release performed.

- #159 P0 containment: shared translator rejects collisions with stream/path/source
  diagnostics; schema preflight runs before destination creation/update. Removed
  schema-only suffixing and preserve input containers on failure. Added 33 offline
  cases across both resolvers, all four denormalized transports and real Singer
  message processing, including no buffering/state after collision. Fixed raw
  ingestion is unchanged. Nested schema naming/migration remains open as P1.

- #162 merged as `8a6c0ba` after final-head Python 3.10–3.12 CI run
  43942188040 passed. Post-merge main run 43944790674 verifies the same runtime;
  its terminal result is linked from #162. Remaining inventory: issues #134,
  #150, #159 and protected PRs #139/#152. Next unblocked correctness work is
  #159 P1 schema/record alignment with an explicit existing-table migration plan.

- #159 P1 implementation: recursive physical RECORD names now align with records;
  existing nested schemas are checked before mutations. Missing/renamed/extra,
  reordered, type/mode-changed nested fields require explicit migration. No
  automatic nested evolution, JSON-key policy change or fixed-view change.
  Offline tests include actual protobuf round-trips, all transport preprocessing,
  real constructor preflight for append/upsert/overwrite, staging-cache reuse,
  metadata preservation and Singer failure before data/state. Live migration
  validation remains separate and unauthorized.

- #164 merged after explicit maintainer authorization; #159 implementation is complete.
  #165 adds repeated-view alias consistency, capability documentation and release
  preparation. #139/#152 still await live-test authorization; no release performed.

- 2026-11-17: authorized live overwrite gate passed for both methods. Reused
  streaming staging names reproduced silent loss; UUID-template containment and
  38 offline regressions added. Final local suite421 passed/15 skipped. See live
  evidence and release-readiness docs. Release0.8.0 preparation merged; publication status is recorded below.

- #152 merged as e9709ec; #134 resolved and #139 closed as incorporated.
  Remaining inventory: release request #150; no open PRs. #166 merged as964fe9a.

- 2026-11-17: [GitHub0.8.0](https://github.com/z3z1ma/target-bigquery/releases/tag/0.8.0)
  published from964fe9a after main Tests run43994530095 passed Python3.10–3.12.
  Wheel/sdist match the fresh-install-tested artifacts and their published SHA256
  digests. PyPI0.8.0 returns404; upload authentication is unavailable. #150 remains
  open for this concrete distribution blocker. Inventory:1 issue,0 PRs.

### Next executable roadmap work

1. Complete PyPI publication of the already validated0.8.0 artifacts once an
   authorized upload credential or trusted publisher is available; verify hashes
   and installation from PyPI, then close#150. Do not rebuild from later main.
2. Completed in [#167](https://github.com/z3z1ma/target-bigquery/pull/167):
   deterministic failure/restart harness, known query IDs across submission errors,
   and explicit reconciliation guidance. See the delivery entry below.
3. Offline compatibility spike completed in #168. Adoption of a destination-query
   publisher requires the documented server-acceptance matrix; experiments beyond
   the completed overwrite gate remain separately scoped.


### Delivered — query outcome reconciliation and restart coverage (#167)

Merged as `b136c04` from reviewed head `41ace9c`: assign query IDs before submission,
log them before the RPC and on failure, report actual client/job location, and
disable automatic failed-query resubmission. Explicit IDs also propagate Conflict
instead of automatic lookup; transport retries remain. This availability tradeoff
is documented in [query recovery](docs/query-recovery.md).

The deterministic post-staging harness runs real drain/cleanup paths across lost
submission/poll responses, partial multi-table publication, remote completion on
either side of timeout, and full-snapshot restart with fresh staging. A separate
case proves why unresolved old jobs must finish before replay: a late old overwrite
can replace a newer snapshot. Installed-client Conflict behavior is tested offline.

Local acceptance:428 passed,15 cloud skips;Ruff/mypy passed. Independent review
approved with35 focused tests passed. Exact-head Python3.10–3.12
CI[44016740919](https://github.com/z3z1ma/target-bigquery/actions/runs/44016740919) passed. No new cloud operations or package publication.
Existing0.8.0 tag/assets remain unchanged. #150 still waits on authentication.

Merged-main Python3.10–3.12 CI[44016846704](https://github.com/z3z1ma/target-bigquery/actions/runs/44016846704)
passed at `b136c04`. Remaining inventory:1 open issue (#150),0 open PRs.
The subsequent offline WRITE_TRUNCATE_DATA spike is recorded below.


### Delivered — overwrite publication compatibility decision (#168)

The offline WRITE_TRUNCATE_DATA spike is implemented in
[the compatibility decision](docs/overwrite-compatibility.md) and eight API-contract
probes. Query and load configuration preserve the intended fields; a real client
query reaches an intercepted jobs.insert boundary. A copy-job counterexample
proves SDK serialization alone does not validate server support. Production CTAS
is retained; no experimental runtime flag or speculative publisher is introduced.

The decision defines schema evolution, metadata, cleanup/STATE and unknown-outcome
acceptance cases for a later destination-SELECT publisher. Updated capabilities
now reflect merged layout/nested/view work and the completed focused live gate.
Local validation:436 passed,15 cloud skips;Ruff/mypy passed. Independent review
approved with8 focused tests passed. Exact-head Python3.10–3.12
CI[44017370018](https://github.com/z3z1ma/target-bigquery/actions/runs/44017370018) passed;
merged as `11da304`. No cloud
operations, credentials changes, release or publishing attempts are included.

Merged-main Python3.10–3.12 CI[44017467345](https://github.com/z3z1ma/target-bigquery/actions/runs/44017467345)
passed at `11da304`. No open PRs; #150 remains the publishing-authentication issue.
