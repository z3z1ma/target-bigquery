# Overwrite publication design decision

Status: offline spike completed; retain the current CTAS publisher.
Reviewed: 2026-11-17. Runtime baseline: #152 layout preservation and #167 recovery.

## Evidence and job-type decision

The REST API documents `WRITE_TRUNCATE_DATA` for query and load jobs: it replaces
rows while retaining destination schema and constraints. Both permit nullable
field addition and required-field relaxation through schema update options.
Copy jobs do not list this disposition. Destination writes are atomic on successful
job completion; that does not make a multi-table run atomic.
[Job API](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job)

The audit metadata reference additionally describes preservation of row-access
policies, column policies and descriptions. This is a documented intent, not our
verification of every metadata property or interaction.
[Write disposition](https://docs.cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata.WriteDisposition)

| Path | Client evidence | Decision |
| --- | --- | --- |
| Query with explicit destination and a SELECT | Installed client submits the expected jobs.insert body, UUID-compatible job identity, CREATE_NEVER and disposition; intercepted before network | Candidate for a future common publication step |
| Load job | Config round-trip retains disposition and schema options | Do not replace every batch load with truncate: multiple batches would erase earlier batches |
| Copy job | Client accepts the string despite absent documented API support | Excluded; serialization is not server validation |
| Existing CTAS + DROP script | Current implementation explicitly restates layout and uses bounded, identified queries | Retain; adding a disposition does not turn this script into destination-query publication |

The reproducible probe is [test_overwrite_job_compatibility.py](../target_bigquery/tests/test_overwrite_job_compatibility.py).
It uses anonymous credentials and intercepts the SDK request boundary; it cannot
create tables, submit jobs or incur charges. The locked baseline is
`google-cloud-bigquery` 3.42.1. CI checks the same locked contract on Python 3.10–3.12.

## Schema compatibility matrix for a future publisher

These are proposed acceptance decisions, not additional supported schema changes.

| Incoming schema | Required behavior before adoption |
| --- | --- |
| Exact match, including ordered nested fields | Two replacements retain metadata and contain exactly the latest snapshot |
| Added nullable top-level field | Decide whether existing preflight schema update or explicit ALLOW_FIELD_ADDITION owns the change; prove failure semantics |
| Missing top-level field | Choose explicit nullable projection or reject; SELECT * is insufficient evidence that destination schema can be retained |
| REQUIRED to NULLABLE | Reject initially; relaxation needs a separate user-facing compatibility decision |
| Changed scalar type or mode | Reject before publication; require explicit migration |
| Nested addition/removal/rename/order/type change | Keep the existing strict nested preflight; this spike does not authorize automatic nested migration |
| Transformed-name collision | Keep fail-fast schema/record rejection before staging and STATE |

## Proposed architecture and acceptance gate

Keep all transports appending to fresh UUID staging. At EOF, use one destination
SELECT job per table with explicit destination and CREATE_NEVER. Confirm successful
publication before deleting staging; treat cleanup failure separately from unknown
publication, with an explicit decision about STATE. Preserve known query IDs,
bounded waits, reconciliation-before-replay and no cross-table atomicity claims.
This requires changing the publisher interface and lifecycle, not changing a
single constant. No experimental user configuration flag is introduced here.

A separately scoped live test must compare before/after metadata and exact rows
across two replacements for batch and Storage Write staging. Record ordered schema,
field descriptions/policy tags, constraints, labels, table description, table and
partition expiration, partition field/granularity, clustering, encryption settings,
row-access policies and table permissions where present. Properties requiring new
IAM roles or security configuration need their own approval; do not invent them in
the existing test dataset. Test compatible and rejected schema cases, job failure,
lost-response reconciliation, and staging deletion failure. Capture cost and cleanup.

Until those checks pass, no claim is made that this alternative preserves all
metadata or is safer for every supported schema. The earlier $1 approval covered
specific overwrite regressions, not this new experimental matrix. No live calls
were made for this spike. PyPI authentication is unrelated and remains blocked.
