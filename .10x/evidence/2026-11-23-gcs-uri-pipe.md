# GCS URI notification pressure

Baseline: `b426db9e7f1481dd9bad6321a08535c8b0af9af5`. Issue #203.

## Lifecycle and reproduction

The actual GCS producer places a Job containing compressed bytes, a UUID batch ID,
table/dataset/bucket names and the sink's Connection on the process queue. After
uploading, the worker sends the resulting URI before its completion notification.
The parent previously joined workers before `_load_pending_uris` read the sink
pipe in checkpoint/cleanup hooks. A full URI pipe therefore prevents completion
and exit while the parent waits for them.

Two bounded baseline tests stalled after 456 of 1,000 fake uploads, at both
checkpoint and EOF (2 failed in 20.69 seconds). `test_gcs_backpressure.py` uses the
actual process_record/process_batch path, supported batch_size=1, ordinary UUIDs,
standard GcsStagingProcessWorker, real queue and unmodified pipes. It holds the
first fake upload until submission finishes, representing a fast producer ahead
of a slow service. The supported Python gzip fallback avoids unrelated external
gzip startup cost. Only the idle timeout is accelerated for checkpoint testing.

The count is a local macOS observation, not a universal pipe limit or live workload
measurement. At 500 rows per job the same job count represents roughly 228,000
rows; the test itself uses one row per job. No custom oversized notification,
resized pipe, cloud upload or real BigQuery load was used. Local handle-sharing
sockets must be available for the real producer Connection reduction.

## Small correction

Extract URI receipt into `_collect_uploaded_uris`, retaining received order in
`self.uris`. During process notification draining, collect from every active GCS
sink (including denormalized subclasses). `_load_pending_uris` also collects any
remaining messages before its existing load logic. Collection does not load,
publish, deduplicate, commit, rotate or acknowledge STATE.

Existing worker/error/completion gates still precede hooks. Successful loads alone
clear URIs and load-job identity; failures retain them. Thread notification draining,
upload retry behavior and EOF sentinel ordering are unchanged. Receiving an upload
path is not evidence of a successful BigQuery load.

## Acceptance and limits

Ten added cases cover real pressure at checkpoint/EOF, multiple active sinks,
exact URI membership/order/count, no second load from an empty hook, worker upload
failure preventing all loads/STATE, load-result failure retaining URIs/job ID, and
process-only collection for both GCS sink variants. Fake load entry asserts that
the worker has completed normally and outstanding jobs are zero. Harness waits
and owned-child cleanup are bounded.

Full offline suite: 665 passed, 15 cloud skips; Ruff/mypy pass. Independent review:
61 focused passes with local IPC, no blockers. Exact CI/integration evidence is
recorded in ROADMAP.md. No cloud, spending, publication, tag or permission changes.

The parent retains paths in memory; this patch does not establish a memory bound,
overall shutdown deadline, interrupted-frame recovery or concurrent large-writer
framing protection. Ordinary short paths from one worker establish this defect;
those other risks were not implicated. Storage Write stream-name pressure remains
a separate question tied to cache churn and worker/stream creation, not row count.
