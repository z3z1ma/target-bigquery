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


## Batch file loads

Batch loads assign a UUID before uploading and retain it through submission
retries, including process workers. Logs identify the job and destination; use
the destination dataset location when reconciling. A conflict or error after the
client returns a load job stops the worker without acknowledging the batch.
With the default `fail_fast: true`, the target withholds STATE and publication.
Do not disable fail-fast when durable checkpoints are required.

`timeout` (default 600 seconds) applies separately to submission and result waits
and must be positive and finite. Submission retries reuse the same ID; result
failures do not re-upload. Timeouts do not cancel server jobs. Reconcile the remote
outcome before restarting: a fresh run creates fresh IDs and replayed append rows
can still duplicate. These guarantees cover worker retries, not end-to-end
exactly-once delivery. Validation uses simulated servers without cloud writes.


## Storage Write completion waits

`timeout` also bounds each append future completion wait and each pending-stream
finalize/commit RPC. It must be positive and finite. Finalize/commit use no automatic
RPC retry, so transient errors now require operator reconciliation and replay.
The client's former retry window was independent of the configured timeout.
Append completion failures add no retries. See the dispatch policy below.

Append errors identify the stream; finalize/commit log stream identities before
calling the server. Failed commit attempts retain the pending stream list. With
`fail_fast: true` (the default), worker failures prevent checkpoint hooks, MERGE,
overwrite publication and STATE. Worker completion notifications are local work
accounting and do not assert successful delivery. Disabling fail-fast can permit
STATE despite an error and is unsuitable for durable checkpoints.

A timeout does not cancel or roll back a remote append/commit. Reconcile the
logged streams and destination before replay. Default-stream rows can already be
visible, and an uncertain pending commit may have succeeded. Replaying can duplicate
rows; per-stream offsets do not provide end-to-end exactly-once delivery. These
are per-wait/RPC limits: queueing, multiple outstanding futures, stream creation,
connection management and shutdown can make total elapsed time longer.

The locked client's unfinished `AppendRowsFuture`, completed/rejected responses,
and real target drain paths are tested offline. No live cloud validation is
claimed. See the official [Storage Write client reference](https://docs.cloud.google.com/python/docs/reference/bigquerystorage/latest/google.cloud.bigquery_storage_v1.client.BigQueryWriteClient)
for pending/default visibility and per-stream offsets.


## Ambiguous Storage Write dispatch

An exception from `AppendRowsStream.send` can occur after the server accepts its
initial request. The target therefore stops that worker, logs the stream identity,
and does not automatically retry or requeue that payload. This applies to both
default and pending streams. Already submitted work can still finish remotely;
closing client streams is not rollback. Default fail-fast handling prevents hooks
and STATE after this error.

Stream setup failures before dispatch retain the existing bounded queue retries.
This distinction avoids replaying an uncertain append while allowing recovery when
no payload has been sent. Some dispatch failures that previously recovered now
require reconciliation. The SDK's internal connection management is unchanged.
A future design may recover pending writes on the same stream and offset, but the
target does not claim that guarantee across worker retries or fresh streams.


## Pending append acknowledgement identity

Each dispatch now logs its stream, requested offset (or `None` for the default
stream), and row count before sending. Keep these INFO logs when investigating
uncertain writes. The same captured identity accompanies completion errors, even
after later batches have advanced the worker's offset counter. Payloads are not
included in these identity logs; the identity is not a durable payload journal.

A successful pending-stream response must contain the requested append offset.
Zero is a valid offset. A missing or different offset, or a different returned
stream when supplied, triggers the existing failure/state gate. Default-stream
responses do not require an offset. Server errors are checked first;
`ALREADY_EXISTS` and `OUT_OF_RANGE` still stop the target because it has not
established an identical-payload replay protocol. An offset alone cannot establish
which payload occupied it. No automatic replay or remote rollback is introduced.

This validates the [documented append response contract](https://docs.cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsresponse).
It is an invariant check and recovery prerequisite, not a claim that the service
has returned incorrect offsets in a live run. Automatic recovery remains gated on
retained payload identity, stream ownership, and ordering across in-flight writes.


## Streaming insert partial outcomes

`streaming_insert` now gives each queued batch an ID and explicit row IDs of the
form `<batch-id>-<zero-based-index>`. Logs identify the batch and destination
before submission. IDs survive worker retries/process serialization but are new
on source replay. BigQuery insert-ID deduplication is best effort, not a guarantee.

A returned row error can accompany successfully inserted rows. The target stops
without retrying the entire batch on any row error or uncertain RPC exception.
SDK and connector transient-error retries are disabled. Only a request-level
`NotFound` is retried, at most three attempts one second apart, with identical IDs
and payloads. A row-level `notFound` is still a partial-response failure. This
shorter retry policy may require operator recovery for table propagation delays
or transient failures that previously recovered automatically.

The positive finite `timeout` applies to each insert request (default 600 seconds).
With default `fail_fast: true`, a terminal error prevents STATE, MERGE and overwrite
publication. It does not undo rows already accepted. Reconcile partial or unknown
outcomes before replay; fresh runs can duplicate append rows. No automatic
failed-row retry, remote rollback or whole-process deadline is provided.

See Google's [partial-success and deduplication guidance](https://docs.cloud.google.com/bigquery/docs/troubleshoot-storage-api).
Offline tests use the installed client request builder and simulated acceptance;
no live cloud test is claimed.


## Storage Write worker ownership

With `options.process_pool: true`, notifications now carry only stream names.
SDK stream objects and their locks stay in the worker process. Standard process workers for all four ingestion methods use importable classes
and are tested under Python's spawn start method. Thread mode uses the same stream-name notification contract.

Workers finish append waits and close their own clients before normal exit.
The coordinator joins them and checks errors before finalizing/committing pending
stream names. It deduplicates names, excludes the default stream and retains pending
names on finalize/commit failure. A worker close error blocks hooks and STATE under
default fail-fast behavior; closing a client is not remote rollback.

Validation covers real multiprocessing pipes with installed SDK stream objects,
spawned sentinel-only workers, and offline lifecycle failures. It does not claim
live process-mode ingestion validation or solve pipe backpressure or spawn
compatibility for custom executor classes.


## Abnormal process exits

The coordinator checks process exit status after joins, including idle-worker
removal. A nonzero status (including a negative signal status) blocks replacement,
checkpoint hooks, publication and STATE with default `fail_fast: true`, even when
the worker could not send an error notification. Shutdown joins all workers before
reporting the failure. Diagnostics identify the worker and exit status; an existing
error notification remains the primary exception if shutdown also detects a crash.

`fail_fast: false` logs a warning and retains permissive behavior, which can advance
STATE with incomplete data. This check does not undo remote writes or impose a deadline on joins.


## Normal worker completion

Standard workers now share an explicit normal-completion marker with the
coordinator. They set it only at the end of their ingestion loop, after Storage
Write finishes its append waits and client cleanup. Both idle timeouts and queue
sentinels are normal exits. After joining, the coordinator rejects a missing
marker under default fail-fast behavior, even for exit status zero or an early
thread return. Culling checks this before starting replacements. Detailed pending
worker errors remain primary; `fail_fast: false` warns and remains permissive.

The marker establishes that the loop reached its normal end, not that every
queued job was delivered or every remote write succeeded. Error notifications,
stream commits and publication hooks still determine checkpoint safety. Work
arriving after an idle worker exits and queue feeder/backpressure failures need
separate handling. No automatic replay or live-cloud validation is added.

## Nullable upsert keys

Denormalized upsert matches each key component using equality or both values
being SQL NULL, with each component grouped before AND. This prevents a replay
from inserting another row solely because an accepted key component is null.
Ordinary equality is retained for non-null values: no casts, COALESCE sentinels,
or IS NOT DISTINCT FROM substitution changes NaN or coercion behavior. MERGE
schema intersection uses the same transformed physical names as ingestion.

Missing Singer keys remain errors. Fixed JSON mode validates original key presence
inside `data` but does not upsert or extract keys for MERGE. Append and overwrite
are unaffected. `_sdc_deleted_at` remains an updated metadata column, not a physical
DELETE operation. General record validation is already bypassed by this target;
this change does not add validation, relax schemas or support new key types.

For repeated staged keys, enable `dedupe_before_upsert` when appropriate: it groups
by the same keys and keeps the latest extraction/receipt ordering. Without dedup,
multiple nullable source rows can now match one destination row and cause a
BigQuery UPDATE cardinality error instead of silently inserting duplicates. An
empty destination can still receive repeated source keys. Existing destination
duplicates are not repaired, and tied dedup timestamps do not gain a new tie-breaker.

The predicate follows [Google's MERGE guidance](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement).
Offline tests execute the generated predicate/window SQL subset and a minimal
replay model in SQLite; they are not BigQuery integration tests. Live correctness,
query-plan cost and partition-pruning equivalence have not been measured.

## Retries during EOF drain

Eligible retries stay with the worker that owns the job until success or exhaustion.
The worker reads the next shared-queue item only afterward, so EOF sentinels cannot
pass a retry queued during shutdown. Existing attempt limits, delays, identities
and retry eligibility remain unchanged; ambiguous Storage Write dispatch and
submitted batch failures still stop without retry. Queue accounting occurs once
per original data-job dequeue.

This may reduce work sharing while a worker retries. It adds no replay, queue-size
checks or queue-join waits. Gated offline thread tests cover every ingestion method
at EOF, including exhaustion and STATE/publication suppression. Existing spawn
tests cover startup and clean shutdown, not live ingestion or queue feeder failure.

## Process queue accounting

Workers account for each dequeued data job only when the queue provides
`task_done()`. Thread queues retain their accounting; the configured plain
multiprocessing queue does not require it. Errors from an existing accounting
method still propagate. This avoids a child crash after API acceptance without
changing FIFO consumption, retries, queue type or wait behavior.

Offline spawn tests now send two data jobs and a sentinel through the actual
configured process queue for every standard worker variant, checking FIFO API
observations, successful completion and parent checkpoint/EOF hooks. Separate
terminal API cases verify that STATE and publication remain blocked. The APIs are
fakes: these tests do not establish live ingestion, remote durability, queue feeder
reliability or end-to-end exactly-once semantics.
