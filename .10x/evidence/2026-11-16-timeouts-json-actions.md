# Timeout, JSON and workflow maintenance evidence

Date: 2026-11-16. Starting main: 76099b0. Starting open inventory: 4 issues,
7 PRs. #139/#152 were excluded from branch changes and merging throughout.

## Timeout (#148/#149 → #157)

Original author Dana Arsovska's commit ee32bff2 was cherry-picked with attribution.
Follow-up head 9c093abe84398e72dfa5d53e4d605df46975586a adds the shared query helper,
positive-integer validation, available job ID/location diagnostics and failure
regressions at the real target drain boundary. It covers views, MERGE and
replacement queries, preserving exceptions and staging pointers after failure.
Timeout limits client waits; SDK retries can exceed it and remote work is not
cancelled. Independent review verified installed BigQuery 3.42.1 behavior.

Local: 192 passed, 13 skipped; Ruff/mypy pass. Independent focused check: 20 passed.
Exact-head Python 3.10–3.12 CI run 43935751192 passed. Merged as 9864f87; #148 closed,
#149 closed as incorporated with an attribution/evidence comment.

## JSON (#141/#140 → #160)

Original author Lautaro Celli's commit 1841adf9 was preserved. Follow-up 89ffc22
adds recursive non-finite number rejection, input-container copying and repeated
JSON/RECORD array validation. Actual generated protobuf tests cover scalar JSON,
SQL NULL versus JSON null, repeated/nested values, 64-bit integer boundaries,
invalid inputs before buffering, fixed mode, top-level column transforms and
schema-cache reuse. Supported integers remain numeric; out-of-range values fail.
Fractional Decimal values retain the existing float-conversion policy.

Independent review found two issues during development: a pre-existing nested
schema transform mismatch (tracked separately as #159), and repeated inputs being
silently iterated as characters/keys (fixed with type validation). The final review
approved 89ffc22; 62 targeted tests passed independently. After incorporating #157,
local suite 227 passed/13 skipped and Ruff passed at c7a58d2. Mypy passed before
integration; runtime code changes during integration were the independently
verified timeout implementation.

## Actions (#136/#137/#147 → #158)

All three Dependabot commits were preserved. Exact upstream action manifests
were reviewed: Node 24 runtime, current workflow inputs and events remain compatible.
Explicit prune-cache:true retains the old setup-uv cache behavior. Python 3.10–3.12,
frozen installs, read-only workflow permissions and the complete offline suite
remain intact. Original reviewed head 8f3b166 was independently approved; refreshed head 7571cfd
incorporates #157 and passed 192 local tests/13 skips plus Ruff.

## Limits

No BigQuery client queries, writes, setup, IAM changes, or releases. Tests requiring
cloud credentials were skipped. Offline protobuf acceptance does not prove remote
BigQuery acceptance. #159 needs a schema/migration design before nested field
transforms can be declared supported; JSON document key transforms also predate
this work and are explicitly documented.


## Verified delivery updates

- Timeout main CI run 43936374065 passed at merge 9864f87.
- Actions final reviewed/refreshed head 7571cfd passed all Python 3.10–3.12 jobs
  in run 43936552874. Merged #158 as f4f9739; originals #136/#137/#147 closed
  as incorporated with their commits preserved.
- JSON head a108a53 merges current main f4f9739 (workflow pins only changed
  since its prior code review). The roadmap-only conflict was resolved by
  preserving both delivery records. Combined suite remains 227 passed/13 skipped,
  Ruff passed; final-head CI run 43938944542 passed on Python 3.10–3.12.
  #160 merged as a12cf402a76418dbf592be43e915fba693cabb24; #141 closed
  and #140 closed as incorporated.

- Main at f4f9739 passed run 43938570012. JSON post-merge main verification
  is recorded in the PR/final delivery report once terminal.
