# Maintained roadmap

Updated: 2026-11-23. Owner: repository maintainer (execution delegated to Codex).
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


### Completed — P0 GCS checkpoint durability (#169, #170)

Offline reproduction against18dcbee showed append STATE without a BigQuery load,
and upsert MERGE/rotation before loading uploaded GCS objects. GCS previously
loaded only in EOF cleanup. Seven initial regression cases failed on old main.

The fix loads and waits before the base pre-state hook and EOF cleanup. It clears
only successfully loaded URI batches, preserves upsert rotation and overwrite EOF
publication, bounds load result waits, and retains an explicit job ID after failure.
Further checkpoints create new load IDs; empty EOF does not reload earlier files.
Mixed-stream and load/merge failure tests protect global STATE. GCS objects remain
subject to the user's retention policy; more frequent load jobs may affect quotas
and latency. No live cloud validation or expenditure is included.

Validation: 461 passed, 15 cloud skips; Ruff/mypy passed. Independent review
approved with 33 focused tests passed. #170 merged as `b8ef5d6`; exact-head
CI44018227854 and merged-main CI44018868355 passed on Python 3.10–3.12.

### Completed — batch append retry identity and bounded waits (#171, #172)

Lost submission/poll responses previously caused three successful appends under
fresh server IDs in the offline reproduction. Each queued batch now carries one
UUID through submission retries and process serialization. Conflict or failure
after a returned job stops without acknowledgement; operators reconcile the
logged ID before replay. The configured timeout reaches submission and result
waits; invalid values cannot enqueue a batch. This does not make source replay
exactly-once, cancel remote jobs, or impose a whole-process deadline.

Acceptance covers accepted-but-response-lost submission, polling failure,
pre-acceptance retry success, configuration wiring, process serialization, and
real target drain withholding STATE/publication on worker error. 474 offline tests
passed, 15 cloud tests skipped; Ruff/mypy clean. Independent review passed with
172 focused tests. Tested head `3621719` passed Python 3.10–3.12
[CI44019395931](https://github.com/z3z1ma/target-bigquery/actions/runs/44019395931).
Merged as `52a81a1`; merged-main
[CI44019569618](https://github.com/z3z1ma/target-bigquery/actions/runs/44019569618)
also passed all three versions. No cloud operations.

Post-merge inventory: zero open PRs; #150 is the sole open issue, blocked on
PyPI authentication and the existing publication approval-review denial.
Next: audit Storage Write retry/stream lifecycle against ambiguous completion;
keep new write modes and metadata-preserving overwrite gated on scoped evidence.


### Completed — Storage Write completion deadlines (#173, #174)

The configured timeout now travels with each queued job through serialization to
append future waits, replacing the locked SDK’s inherited 900-second polling limit. Pending finalize/commit use the same timeout and `retry=None`
to avoid the locked client's independent 600-second retry window. Errors retain
stream diagnostics, pending stream identities and default fail-fast STATE gates.
No append retry/reconnection changes or end-to-end exactly-once guarantees.

Seventeen regression cases failed against `6839381` (old main), covering timeout
wiring, validation and finalize/commit bounds. New tests also exercise a real
unfinished installed-client future, successful/rejected append responses, default
stream exclusion, checkpoint/EOF failure and process serialization. Per-operation
limits, total-drain limitations, and transient-error recovery tradeoffs are in
`docs/query-recovery.md`. No cloud operations. Independent review and exact-head
Python 3.10–3.12 exact-head CI44020496027 passed at `9b18de7`; #174 merged
as `d329809` and merged-main CI44021808296 passed all three versions.
Full offline suite: 495 passed,
15 cloud skips; Ruff/mypy clean. Initial CI exposed a Python 3.10 test-only
exception-class mismatch; use concurrent.futures.TimeoutError consistently.

Next priority: isolate ambiguous synchronous append dispatch/reconnect failures
with an offline accepted-but-response-lost server model. Distinguish safe retries
on the same pending stream and offset from default-stream replay and fresh-stream
creation; do not change retry policy without reproductions and compatibility tests.


### Completed — ambiguous append dispatch containment (#175, #176)

Real installed SDK connection setup plus simulated transport acceptance reproduces
lost-response duplication on the default stream. Including the connector's nested
send/worker retries produces 13 dispatch attempts; pending same-stream offsets
can deduplicate, but fresh streams lose that identity. Independent reproduction
confirmed the default-stream risk without mocking SDK send/open methods.

The containment removes send-level retries and marks errors after entering dispatch
as potentially accepted. Those errors stop the worker without queue replay; stream
setup failures before dispatch retain bounded retries. Tests use real SDK setup
with fake transport and deterministic connection-renewal scheduling, model pending
stream deduplication, and cover checkpoint/EOF failure, both stream types and safe
setup recovery. Four regression cases fail against `d329809`.

No cloud writes or end-to-end exactly-once claim. Full offline suite: 500 passed,
15 cloud skips; Ruff/mypy clean. Independent review passed 89 focused tests.
Exact head `c07ecf6` passed Python 3.10–3.12
[CI44022270096](https://github.com/z3z1ma/target-bigquery/actions/runs/44022270096).
Merged as `acf6273`; merged-main
[CI44029355711](https://github.com/z3z1ma/target-bigquery/actions/runs/44029355711)
also passed all three versions. The GitHub review bot exhausted its quota;
the separate independent review and tests above completed successfully.

Post-merge inventory: zero open PRs; #150 is the sole open issue, still blocked
on PyPI authentication and the existing publication approval-review denial.
No publishing action was attempted.

Next: design explicit pending-stream recovery by stream/offset identity, with
durable diagnostics and fault-injection acceptance criteria before introducing
automatic replay. Require identical payload/offset reuse on the same stream,
explicit handling of ALREADY_EXISTS and OUT_OF_RANGE, unknown commit outcomes,
and a prohibition on fresh-stream replay of an unresolved append. Default-stream
recovery remains separately scoped because it cannot use per-stream offsets to
deduplicate. Keep fail-closed behavior until these cases are proven.


### Completed — pending append acknowledgement identity (#177, #178)

Before adding replay, retain stream/requested offset/row count per append and
verify successful pending responses against the captured offset. Log the identity
before dispatch and on completion failure. A returned stream is checked when
supplied. Missing/mismatched offsets trigger the existing fail-fast STATE gate;
zero is valid and default streams remain offset-free. This is defensive validation
of the official contract, not an observed server offset bug.

Acceptance: multiple outstanding batches after counters advance; zero/nonzero
success; absent/wrong offset or returned stream; default success without offsets;
ALREADY_EXISTS/OUT_OF_RANGE remain failures; checkpoint/EOF suppress publication;
pre-dispatch identity logging excludes payloads. Installed-client futures are used
offline. Full suite: 515 passed, 15 cloud skips; Ruff/mypy clean. Eleven initial
regression cases failed against `ebc8d92`. Independent review passed 104 focused
tests. Exact head `e4e657e` passed Python 3.10–3.12
[CI44030226420](https://github.com/z3z1ma/target-bigquery/actions/runs/44030226420).
Merged as `5235560`; merged-main
[CI44030360891](https://github.com/z3z1ma/target-bigquery/actions/runs/44030360891)
also passed all three versions. No automatic retry, cloud operations, or persistent
payload journal. Post-merge inventory: zero open PRs; only publishing issue #150
remains, with its existing authentication/approval-review block unchanged.

Next: design retained payload identity and ordered pending-stream replay. A bounded
same-stream/offset recovery policy must distinguish already accepted data, gaps,
unknown commit outcomes and lost process state before any implementation is enabled.


### Completed — streaming insert partial/unknown outcome containment (#181, #182)

Daily inventory found new dependency PRs #179/#180 and existing publishing block
#150. A higher-priority runtime gap emerged in `streaming_insert`: returned row
errors and uncertain RPC errors caused whole-batch replay with fresh auto-generated
insert IDs. Installed-client/fake-server tests accepted one row three times;
four regression cases fail on `214f686`.

The fix stops on returned row errors or ambiguous RPC exceptions, disables SDK
retries, and retains limited request-level NotFound recovery (three attempts, one
second apart). Queued batch-derived row IDs survive process serialization; the
configured positive finite timeout reaches each request. Stable IDs do not make
best-effort BigQuery deduplication a guarantee. Default fail-fast suppresses STATE
and publication; source replay can still duplicate previously accepted rows.

Acceptance covers partial success, lost response, actual client request IDs/retry
settings, safe NotFound recovery/exhaustion, configuration validation and process
serialization, plus checkpoint/EOF suppression. 531 offline tests passed, 15 cloud
skips; Ruff/mypy clean. Independent review passed 199 focused tests. Exact head
`b51f05a` passed Python 3.10–3.12
[CI44114651079](https://github.com/z3z1ma/target-bigquery/actions/runs/44114651079).
Merged as `709c25f`; merged-main
[CI44114941624](https://github.com/z3z1ma/target-bigquery/actions/runs/44114941624)
also passed all three versions. No cloud
operations. This confirmed duplicate-write defect takes priority over speculative
pending-stream replay; the latter's retained-payload/ordering requirements remain.

### Completed maintenance — action pins (#179, #180)

Independent research verified official setup-uv 10.3.3 and setup-python 7.1.0 pins,
with no relevant input migration. Existing prune-cache remains true. #179 passed
exact-head matrix CI44104425569 and merged as `483e0bc`; #180 is refreshed on that
base; combined matrix CI44114311813 passed at `26dca3c` before merge. Original
bot commits are preserved. #179 merged-main CI44114224407 passed. #180 merged as
`22f6900`; merged-main CI44114501841 passed. Subsequent runtime fix #182 passed
with both pins on the integrated matrix.

Daily post-merge inventory: zero open PRs; #150 remains the sole open issue,
blocked on PyPI authentication and the existing publication approval-review
restriction. No cloud operations or publication were attempted. Pending-stream
recovery remains gated on retained payload identity and ordered fault-injection
coverage; no automatic replay was added.


### Completed — process worker startup and Storage Write IPC (#183 / #184)

Current main and CI were green; only blocked release issue #150 remained. Offline
reproduction identified two concrete process-mode failures: Storage Write sent
SDK objects containing RLocks through a multiprocessing pipe; all four ingestion
factories dynamically created unpicklable worker classes. Existing named classes
already provided the required behavior.

Stream notifications now contain names only. Workers own and close SDK clients;
the coordinator deduplicates/filter names and finalizes/commits after join and
worker-error checks. Standard Process/Thread factories return the existing named
classes; custom executor fallback remains unchanged. Acceptance includes real
spawn start/exit for every standard worker variant, real SDK construction with
real process pipes, worker-close failure blocking checkpoint/EOF hooks and STATE,
name deduplication, commit-failure retention, and staging-parent rotation.

No cloud ingestion or expenditure. These tests do not establish live process-mode
validation or resolve unexpected-worker-death and pipe-backpressure behavior.
Full offline suite: 540 passed, 15 cloud skips; Ruff/mypy clean. Four initial
regressions failed against `548fe46`. Independent review passed 110 focused tests
for the core change and 38 for the factory extension, with no remaining blockers.
PR [#184](https://github.com/z3z1ma/target-bigquery/pull/184), exact head `b7b62bc`,
passed Python 3.10–3.12 CI run 44330648722; merged as `3ffc4e8`.
Post-merge main matrix run 44331206196 passed all three Python versions.
Issue #183 is closed. Reconciled inventory: zero open PRs; only release issue
#150 remains, blocked on PyPI authentication and the existing publication-review
restriction. No publishing retry was attempted.
This startup blocker takes precedence over automatic pending-stream replay; its
retained-payload and ordered-recovery acceptance gates remain unchanged.


### Completed — abnormal process exit containment (#185 / #186)

Main `0c5b80d` had zero open PRs and only blocked release issue #150. The prior
runtime main matrix (44331206196, `3ffc4e8`) passed; subsequent main edits were
roadmap-only. Read the local `.10x` quality specification and completion audit;
this pass prioritizes a reproducible checkpoint defect over speculative recovery.

Independent reproduction showed that a process exiting with status 17 and an
empty error pipe still allowed checkpoint hooks and EOF overwrite publication.
Check exit status after culling and checkpoint/EOF joins. Default fail-fast stops
replacement, hooks and STATE; explicit fail-fast-disabled behavior warns and
remains permissive. All siblings are joined before shutdown reports failure.
Acceptance includes positive/negative exits, overwrite/append checkpoint and EOF,
clean process/thread compatibility, culling, original error preservation and a real
spawned `os._exit(17)` process. No cloud calls, release changes or permission changes.

Independent review found no data-integrity blockers; its diagnostic suggestion
was incorporated to preserve an existing error notification during shutdown.
Automated review additionally identified pending notifications at checkpoint/EOF;
the fallback exit-status check now consumes their detailed error first after all
siblings join. Regressions cover checkpoint, EOF and culling. Final offline suite:
559 passed, 15 cloud skips; Ruff/mypy clean.
PR [#186](https://github.com/z3z1ma/target-bigquery/pull/186), final head `b84a98c`,
passed Python 3.10–3.12 matrix run 44755899850. Merged as `dadb548`;
post-merge main run 44756064562 passed all three Python versions. Eight checkpoint
regressions fail against old main `0c5b80d`. Issue #185 is closed.
Final inventory: zero open PRs; only #150 remains blocked on PyPI sign-in and the
existing publication-review restriction. No publication retry or cloud spending.
Silent zero exits, thread death and join/pipe backpressure remain separate work.


### Completed — explicit normal worker completion (#187 / #188)

Reconciled main `1685c2e`: zero open PRs, only blocked #150. Runtime main CI
44756064562 passed; subsequent edits were documentation-only. Four deterministic
real-spawn regressions confirmed that `os._exit(0)` before an unconsumed batch and
sentinel allowed append/overwrite checkpoint and EOF hooks on old main.

Standard workers now set a shared event at natural loop end, after Storage Write
finish. Default fail-fast checks it after join and before culling replacements or
publication/STATE. Nonzero status and detailed pending errors retain priority.
Idle-timeout and sentinel exits remain normal; fail-fast-disabled mode warns.
Acceptance covers all five standard process variants under spawn, early thread
return, culling, finish ordering, and checkpoint/EOF gates. Normal loop completion
is not queue emptiness or durable-delivery proof. Late-enqueued work, feeder loss
and backpressure remain separate investigations; retries and replay are unchanged.

PR [#188](https://github.com/z3z1ma/target-bigquery/pull/188) opened as draft, then
passed exact-head Python 3.10–3.12 CI 44915442948 at `1321f3c`. Offline suite:
574 passed, 15 cloud skips; Ruff/mypy clean. Independent review passed 106 focused
tests; automated review completed with no findings. Merged as `103a5b5`; main
matrix 44915674127 passed all three versions. Issue #187 is closed.

Final inventory: zero open PRs, only #150 remains. No cloud operations, release/tag,
permission or credential changes; the existing publication block is untouched.
Next gate: reproduce late-enqueued work or queue-feeder loss independently before
introducing any queue-exhaustion protocol. This completion marker does not claim
to solve either condition.

### Completed — nullable upsert matching (#189 / #190)

Main `53fb2c7`: zero open PRs, only blocked release #150. Reviewed current Google
MERGE documentation and local `.10x` guidance. SDK/target checks key presence and
accepts explicit null; denormalized upsert uses MERGE, while fixed JSON does not.
Five offline regressions failed against old main: generated composite predicate,
nullable matches and two-pass replay. A transformed-key test separately exposed
source-schema intersection using original names despite transformed ingestion.

Use parenthesized equality-or-both-null per key component and physical transformed
source columns. No casting, alternate NaN equality, validation, append/overwrite,
physical tombstone deletion or historical duplicate cleanup changes. Test optional
staging dedup and multiple source matches; document the possible cardinality error
with dedup disabled. Existing record-schema validation bypass and unsupported key
types remain separate gaps. Offline predicate/window/replay tests are not live
BigQuery evidence; planner performance remains unmeasured.

PR [#190](https://github.com/z3z1ma/target-bigquery/pull/190) opened as draft.
592 offline tests passed, 15 cloud skips; Ruff/mypy clean. Independent review:
174 focused tests passed; automated review completed with no findings. Exact head
`fa79436` passed Python 3.10–3.12 CI 45020338895; merged as `1e2c3df`.
Post-merge main matrix 45020597045 passed all three versions. Issue #189 closed;
final inventory is zero open PRs and only blocked #150. No cloud, publishing,
tag, credential or permission changes. Next validation gate for this area is
explicit key-type/schema-validation contract work; live planner evidence remains
separate and requires authorized cloud scope.

### Completed — EOF retry ownership (#191 / #192)

Reconciled main `c65f271`: no open PRs, only #150. A gated fake submission on a real
standard batch thread confirmed EOF queued a sentinel before a retryable failure;
the retry went behind the sentinel and normal completion permitted publication
and STATE. This deterministic test failed before the fix.

All four loops now retry eligible jobs locally before another shared-queue read,
using existing caps, identities, delays and classifications. Storage Write returns
an explicit retry decision; terminal/ambiguous paths retain their error behavior.
An outer finally accounts each original data dequeue once. No empty/qsize checks,
queue joins or retry/replay expansion. Nine gated EOF tests cover all transports,
recovery and exhaustion, publication/STATE gates and identity/accounting.

601 offline tests passed, 15 cloud skips; Ruff/mypy clean. Independent review:
72 focused tests passed, no blockers. Draft-first PR #192 passed exact-head Python
3.10–3.12 CI 45053506544 at `62cd069`. Automated review completed without findings;
merged as `1bd90dc`. Main verification run 45054583396 passed Python 3.10–3.12
on the exact merge commit. Issue #191 closed; current
inventory is zero open PRs and only blocked #150. Separate source-confirmed finding: process mode configures
multiprocessing.Queue, which lacks task_done used by all data-job loops. This
predates the fix; next gate is a real process data-job regression, since sentinel
spawn tests cannot cover it. Feeder loss remains independently unproven.
No cloud, publishing, tag, credential or permission changes.

### Completed — process data-job accounting (#193 / #194)

Main `0db06d1` records #192's successful exact post-merge matrix. The separate
queue mismatch was reproduced with actual standard spawned workers, the target's
configured multiprocessing.Queue, two data jobs and a sentinel. Ten baseline
cases failed: the first fake API call occurred, task_done raised AttributeError,
and the child exited with status 1. Parent checkpoint/EOF correctly failed closed.

A shared worker helper calls task_done only when available, propagating errors
from existing accounting methods. All four loops use it once per data dequeue.
No queue replacement, join/wait, FIFO, retry or state-contract changes. Real-spawn
coverage includes five worker variants, FIFO completion at checkpoint/EOF, terminal
API failure containment and thread accounting regressions. APIs remain offline
fakes; feeder loss and live ingestion are outside these claims.

Full offline suite: 622 passed, 15 cloud skips; Ruff/mypy clean. Independent review:
102 focused tests passed; automated review found no issues. Draft-first PR #194
passed exact-head Python 3.10–3.12 matrix 45058186230 at `13a45f4`; merged as
`f502c81`. Exact post-merge run 45058609177 passed Python 3.10–3.12,
completing at 19:15 UTC on 2026-11-22. Main verification is complete. Issue #193
closed; zero open PRs, only blocked #150 remains. No cloud, publishing, tag,
permission or credential changes.

Next bounded investigation: test whether a multiprocessing queue feeder serialization
failure can discard a data job while a later sentinel permits normal completion.
The completion marker proves the worker loop ended, not that every queued payload
arrived; this is a documented coverage gap, not a confirmed transport defect.
Use an offline real-spawn test with a deliberately unpicklable payload, capture the
feeder failure and parent checkpoint/EOF outcome, then assess whether production
job construction can reach that failure. Require deterministic evidence before any
production change; preserve FIFO, bounded waits and fail-closed publication/STATE.
No cloud experiment, retry-policy expansion or release work is part of this gate.

### Completed — completed process feeder failure containment (#195 / #196)

Baseline `3df9332`: zero open PRs, only blocked #150. The deliberate custom-object
probe confirmed the mechanism but was not sufficient to justify a fix. Actual
producer-created GCS/Storage jobs then failed during Connection reduction in the
restricted executor (resource_sharer local socket PermissionError). Outside that
restriction, all eight fixed/denormalized producer round trips passed, with working
received notifier connections. No normal-input unpicklable job or Arrow path found.
Evidence and runnable probe: `.10x/evidence/2026-11-22-feeder-findings.md`.

Four gated actual-job/standard-spawn regressions failed before the change: the
feeder dropped the job, the later sentinel allowed normal completion, and parent
checkpoint/EOF advanced. ProcessJobQueue now latches the first completed feeder
failure and preserves its traceback; existing fail-fast gates reject hooks/STATE.
Spawn reconstruction initializes child-local bookkeeping. Explicit permissive mode
warns. No retry, wait, FIFO or remote semantics changes. In-flight serialization
at an idle checkpoint is unresolved and is not claimed by these sentinel-gated tests.

627 offline tests passed, 15 skips; Ruff/mypy pass. Independent review: 69 focused
passes, no blockers. Nine unrestricted-local-IPC probes passed. Draft-first PR #196
passed exact-head Python 3.10–3.12 CI 45062104324 at `74b0a99`; automated review
completed with no findings. Merged as `a6bd41d`; exact post-merge main matrix
45062337329 passed all three Python versions. Issue #195 closed; zero open PRs,
only blocked #150 remains. No cloud, publishing, release/tag, credential or
permission configuration changes. Next gate: independently reproduce serialization
still in progress at an idle checkpoint before considering a delivery barrier;
this fix does not establish one.


### Completed — idle process checkpoint completion (#197 / #198)

Baseline `93ebc41`: zero open PRs, only blocked #150. A real spawned batch worker
and actual producer Job reproduce checkpoint advancement while serialization is
held in the parent feeder. No sentinel is sent. The test accelerates the existing
idle timeout; after normal worker exit, old code ran hooks/STATE before release.
Eventual success leaves an unprocessed job; eventual failure arrives after STATE.
Both containment tests failed before the correction. Production incidence and live
cloud impact are unmeasured.

Process checkpoints and EOF now consume remaining completion notifications after
existing worker/error gates and reject nonzero submitted-minus-completed counts
before hooks/STATE. Counts persist across checkpoints. Explicit permissive mode
warns; thread mode and retry behavior are unchanged. No new wait/join or queue
empty/qsize proof. Transport commit/load hooks remain necessary after this gate.
Evidence: `.10x/evidence/2026-11-23-idle-checkpoint.md`.

637 offline tests passed, 15 cloud skips; Ruff/mypy pass. Independent review:
94 focused passes, no blockers. Draft-first PR #198 passed exact-head Python
3.10–3.12 CI [45210936773](https://github.com/z3z1ma/target-bigquery/actions/runs/45210936773)
at `ff027ba30575d17f1329fda401f960534f357b08`. Automated review completed without
findings. Merged as `f6021da3bf4e2c631fbc8315b87312fe2bf56d0f`; exact post-merge
main CI [45214584357](https://github.com/z3z1ma/target-bigquery/actions/runs/45214584357)
passed all three Python versions. Issue #197 closed; zero open PRs and only blocked
#150 remain. Verification is complete.
Next bounded gate: characterize completion-pipe backpressure during existing worker
joins with an offline bounded harness before proposing any wait-loop change.
This correction does not resolve that separate potential hang. #150's publishing
block remains untouched; no cloud, release/tag, credential or permission changes.

### Completed — process completion/log backpressure (#199 / #200)

Baseline `ef025f6`: zero open PRs, only blocked #150. Actual streaming-insert
producer jobs and standard spawned workers reproduce checkpoint/EOF joins blocked
by ordinary notification sends with unchanged OS pipe capacity. Completion-only
cases stalled at 8,193/10,000 API calls locally; real logs stalled at 820. The
supported batch_size=1 and a gated slow first API model a plausible backlog, not
an oversized-message fault. These local bounds are not universal or live evidence.

Process joins now poll while draining completion/log messages, with a final drain
after exit. Thread mode, retries, FIFO and existing failure/publication gates are
unchanged. Eight cases cover single-worker order, sibling completion, logs,
checkpoint/EOF and terminal API failure after backlog. Full offline suite:
645 passed, 15 cloud skips; Ruff/mypy pass. Independent review: 84 focused passes,
no blockers. Draft-first PR #200 passed exact-head Python 3.10–3.12 CI
[45216548232](https://github.com/z3z1ma/target-bigquery/actions/runs/45216548232)
at `6f0c581c54eacfd1d84d57291d5682dd240d4652`. Automated review completed without
findings. Merged as `2bb9901464eee416114aa70581e6db563767e293`; exact post-merge
main CI [45219721627](https://github.com/z3z1ma/target-bigquery/actions/runs/45219721627)
passed all three Python versions. Issue #199 closed; zero open PRs and only blocked
#150 remain. Verification is complete.
Evidence: `.10x/evidence/2026-11-23-process-backpressure.md`.

Next bounded gate: reproduce pressure on the separate error or sink-owned
GCS URI/Storage Write stream-name pipes and characterize their normal workload
bounds before changing lifecycle ordering. This fix is not an overall shutdown
deadline and does not resolve those channels. #150 remains blocked; no cloud,
spend, publishing, tag, credential or permission changes.

### In review — process error-message backpressure (#201)

Baseline `a1398b3`: zero open PRs, only blocked #150. Actual streaming-insert
producer jobs and a standard spawned worker reproduce checkpoint/EOF stalls on
ordinary per-row error details from one supported 500-row batch, after a successful
one-row batch. Both bounded baseline tests failed; no pipe resizing or custom
unpicklable payload. This is a liveness defect, not evidence that STATE advanced.

Process notification draining now buffers complete error tuples, then the existing
handler preserves their order, diagnostic content and exception causes before
hooks/STATE. No recursive shutdown from collection, retry-policy change, thread
collection change or overall deadline. Ten cases cover siblings, partial work,
checkpoint/EOF, full row-detail endpoints, permissive mode and buffered ordering.
Full offline suite: 655 passed, 15 cloud skips; Ruff/mypy pass. Independent review
and exact-head CI verification pending.
Evidence and channel lifecycle map: `.10x/evidence/2026-11-23-error-pipe.md`.

Next bounded gate: actual-producer GCS URI backpressure and collecting paths without
premature loading; assess Storage stream-name pressure against cache/worker bounds
separately. Interrupted frames, concurrent large unsynchronized writers and buffer
memory bounds remain separate risks. #150 remains blocked. No cloud, spend,
publishing/tag, credential or permission changes.
