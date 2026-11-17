# Recovering an interrupted publication

A timeout or lost response is a local failure, not proof that BigQuery rejected or
cancelled a job. A mutation can finish after the target exits. Overwrites of
multiple destination tables are not one transaction.

The target logs a generated job ID, project and known client location before
submitting each view/MERGE/overwrite query. Failure logs retain that ID even when
submission never returns a job object. If location is `unavailable`, look up the job
using the destination dataset's location. Configured dataset-creation location is
not assumed to be the query location. Logs contain identifiers, not SQL or records.
Retain INFO submission logs so termination during submission can be investigated.

## Recovery procedure

1. Stop overlapping writers. Look up uncertain jobs using their logged project,
   ID and dataset location. Submission timeout or HTTP409 does not prove absence.
2. Establish terminal remote outcomes before replay. Wait for completion, or request
   cancellation and verify the terminal result. Cancellation acknowledgement alone
   does not prove no mutation occurred. If the outcome remains unknown, keep replay
   paused and inspect job history and destinations. Retain staging while in use.
3. Inspect every affected destination: a multi-statement query or multi-table run
   may partially publish. Withholding STATE does not roll back completed mutations.
4. Restart from the last persisted successful state with fresh staging. Overwrite
   must replay a complete snapshot, not an incremental suffix. Mixed append streams
   can duplicate rows; use stable upsert keys where replay must be idempotent.
   Persist STATE only after successful EOF publication.

UUID staging prevents name reuse but does not lock destinations. An unresolved
old overwrite can replace a newer snapshot after the newer run emits STATE.
The regression harness demonstrates this unsafe interleaving; automatic
cross-process recovery and distributed locking are not provided.

## Retry policy

Queries use explicit UUID job IDs and `job_retry=None`. Transport RPC retries
remain under the Google client's policy; failed queries are not resubmitted as
new jobs. Explicit IDs also bypass its outer insertion retry and automatic
Conflict lookup. A lost response followed by HTTP409 stops the target with the
known ID for reconciliation. Some transient failures that formerly recovered
automatically now require replay. This favors traceable outcomes over automatic
mutation resubmission. Submission/result waits remain individually bounded;
there is no whole-process deadline, automatic cancellation, or extra billable query.

Offline tests exercise real drain/cleanup code against simulated lost submission
and polling responses, partial publication, reconciled full replay, and late old
jobs. An installed-client Conflict test runs without network access. This is not
new live-cloud validation.

References: [Client.query](https://docs.cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client),
[QueryJob](https://docs.cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.QueryJob).
