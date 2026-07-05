Status: active
Created: 2026-07-05
Updated: 2026-07-05

# GitHub Issue and PR Burn-Down Context

## Question

What current open GitHub issues and pull requests must be considered in the repository quality and correctness burn-down?

## Sources and Methods

- `gh issue list --state open --limit 200 --json number,title,labels,createdAt,updatedAt,url,author,body`
- `gh pr list --state open --limit 200 --json number,title,headRefName,baseRefName,isDraft,mergeable,reviewDecision,createdAt,updatedAt,url,author,body`
- Per-issue comment exports stored in `.10x/research/.storage/github-open-issues/`.
- Per-PR metadata/comment/file exports stored in `.10x/research/.storage/github-open-prs/`.
- Raw list exports:
  - `.10x/research/.storage/2026-07-05-open-issues.json`
  - `.10x/research/.storage/2026-07-05-open-prs.json`

Observed on 2026-07-05 against `z3z1ma/target-bigquery`.

## Findings

There are 18 open issues and 10 open PRs.

Dependency and packaging context:

- #109 asks for Python 3.12 support. This is already materially addressed by the uv migration and Python 3.10/3.11/3.12 metadata and CI matrix.
- #111 reports stream map aliasing broken by the old Singer SDK. This is materially addressed by the SDK upgrade to `singer-sdk>=0.54.5,<0.55`, but should be verified with a focused stream-map regression test if possible.
- #113 reports Meltano install failure with sparse logs. The uv/PEP 621 packaging conversion likely changes the installation surface; remaining action is packaging/install smoke evidence.
- PR #116 updates the SDK and is superseded by the modernization dependency update.
- Dependabot PRs #62, #58, #56, #29, #26, and #23 are largely superseded by the uv dependency refresh and pinned GitHub Actions modernization.
- #124 reports Storage Write API protobuf generation failure with vanilla `pip install` and recommends `protobuf<6`. Current code uses the modern `message_factory.GetMessageClass` path and metadata currently allows `protobuf>=6.33.6,<7`; this requires direct Storage Write API verification or a focused proto-generation compatibility test before closing.

Config and user-facing semantics:

- #93 and #110 report `upsert` and `overwrite` values saved as strings by Meltano interactive/env configuration. Source currently treats arbitrary non-empty strings as truthy in candidacy checks, so `"false"` can become enabled. This is a real source-backed bug.
- #104 asks how to customize `key_properties` for clustering. The current product receives key properties from Singer SCHEMA messages, not target config. This is primarily documentation unless a config override is explicitly designed.
- #108 asks for overwrite temporary-table name and expiration customization. This is related to #99 and has source-backed operational value for long-running jobs.
- #40 asks for custom timestamp parsing in generated views. This is a feature request requiring a careful interface; current source uses `CAST(JSON_VALUE(...) as TIMESTAMP)` for timestamp projections.

Correctness bugs and PRs:

- #107 reports `Client object has no attribute get_dataset` after PR #96. Source confirms `GcsStagingSink` replaces `self.client` with a GCS client after initialization, while later BaseBigQuerySink temporary-table recreation can call `BigQueryTable.create_table(self.client, ...)`. `_create_overwrite_table` should use `_get_bigquery_client()`.
- #101 reports state advancing when a batch worker fails. Source confirms `TargetBigQuery.drain_one` calls `drain_all(is_endofpipe=True)` in the fail-fast error path, and `drain_all` can write state. This is a real source-backed bug.
- #99 reports temporary-table expiry during runs longer than 24 hours. Source confirms `_create_overwrite_table` hard-codes expiration to one day. This is a real operational bug and overlaps #108.
- #103 reports upsert MERGE failing when new columns appear. PR #125 proposes `merge_column_strategy`; source currently builds MERGE from target schema only, which can reference mismatched source/target columns. This is a real schema-drift risk.
- #105 reports batch denormalized inserts using original schema rather than transformed schema. Current source appears to use `self.table.get_resolved_schema(self.apply_transforms)` for batch load jobs, so this may already be fixed; keep a regression test.
- #118 proposes transforming key properties before validation/merge. Source currently uses raw `self.key_properties` for clustering, dedupe, and MERGE equality while denormalized records can be transformed. This is a real source-backed bug.
- #127 proposes `CREATE OR REPLACE` for overwrite instead of `DROP TABLE; CREATE TABLE`. Current overwrite cleanup still uses separate `DROP TABLE IF EXISTS; CREATE TABLE ... AS SELECT ...; DROP TABLE ...`. This is a real atomicity/metadata-risk issue.
- #76 reports intermittent Storage Write API default stream 404. This requires live BigQuery Storage Write verification; local source review suggests table creation/eventual consistency and stream path construction are the likely areas.
- #71 reports nested records failing with Storage Write denormalized mode. Source has pure proto-generation paths for nested records, but live Storage Write verification is required.
- #39 and #34 report JSON/nested fixed-schema generated-view failures. Current schema resolver v2 and JSON fallback may address part of this, but generated view SQL still needs regression coverage.

## Conclusions

The GitHub burn-down should be executed as several coherent workstreams:

1. Configuration normalization and documentation: #93, #110, #104.
2. Temp-table, overwrite, merge, and state-safety correctness: #99, #101, #103, #107, #108, #118, #125, #127.
3. Storage Write and proto compatibility: #71, #76, #124.
4. Generated-view/schema resolver behavior: #34, #39, #40.
5. Modernization closure and obsolete PR/issue retirement: #109, #111, #113, #116, #62, #58, #56, #29, #26, #23.

The first two workstreams have source-backed defects that can be fixed without inventing product scope. The Storage Write/generated-view workstreams should use live BigQuery when credentials are available; the later corrected credential path at `~/Documents/regal-scholar-336206-47839b5c155a.json` allowed live-compatible BigQuery verification, while GCS staging and legacy streaming remained externally blocked by project billing/tier constraints.

## Limits

- This record summarizes current issue/PR bodies, comments, PR metadata, and PR changed-file lists. It does not claim every issue is fixed.
- The original service-account key path was incorrect. The corrected path later enabled live BigQuery tests, but not the GCS staging or legacy streaming variants blocked by external project state.
