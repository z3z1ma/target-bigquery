# Storage Write process queue portability

2026-11-25, baseline `a0e66d29dad965a0668466ff24f153dd153291ad`.
Issue: [#209](https://github.com/z3z1ma/target-bigquery/issues/209).

## Observed entry-point behavior

Run `PYTHONPATH=. python .10x/evidence/2026-11-25-storage-queue-probe.py`
with the project environment. The probe constructs TargetBigQuery, feeds Singer
SCHEMA/RECORD/STATE messages and calls normal EOF handling. It uses the actual
spawn queue, producer envelope, sink initialization and worker loop. Replacements
are limited to BigQuery metadata and AppendRows API boundaries. Worker bootstrap
installs those fakes in the spawned interpreter, then delegates to its real loop.
Only generated offline data is used; no credentials, queries or cloud writes.

On macOS, process mode fails on its first batch with `NotImplementedError` at
`storage_write.py:505`, before enqueueing data. No STATE is emitted. The same
input in thread mode returns and emits `{"position":1}`. Independent review
reproduced both outcomes. Normalized results are in the adjacent JSON file.
The probe adds no injected transport faults. Its local worker cleanup is bounded.

This is a native-platform ingestion availability defect, not observed silent
loss or a false checkpoint. Linux does not reproduce the missing qsize operation.
The default thread configuration is the current workaround.

## Safe correction gate

Python documents that [Queue.qsize can raise NotImplementedError on macOS](https://docs.python.org/3.12/library/multiprocessing.html#multiprocessing.Queue.qsize).
Simply ignoring that exception removes existing backpressure. Using outstanding
jobs instead changes the meaning of the bound: Storage Write workers collect
more than 15 in-flight requests before waiting for completion. A lower admission
limit can therefore strand work. A bounded queue alone is also insufficient:
shutdown currently puts sentinels before draining notifications, so a full queue
can block shutdown while workers cannot report progress.

Independent review recommends Storage-specific bounded admission and coordinated
notification/error draining, including sentinel insertion. Acceptance must cover
real spawn producers; default and pending streams; capacity below 15; delayed
acknowledgments; notification pressure; worker and feeder failures during full
admission; checkpoint and EOF shutdown; and unchanged no-STATE-on-failure behavior.
Retain native macOS qualification alongside Linux CI. Runtime remains unchanged
until that producer/shutdown invariant is proven together. No speculative partial
fix is included in this evidence pass.

## Refreshed ingestion guidance

Google recommends [stream reuse, batching and default streams where appropriate](https://docs.cloud.google.com/bigquery/docs/write-api-best-practices).
Offsets protect writes within a stream; they do not remove upstream replay.
Singer documents [at-least-once delivery and replay after interruption](https://sdk.meltano.com/en/latest/implementation/at_least_once.html).
These support retaining bounded admission and checkpoint ordering rather than
trading them away for portability. Live API persistence, pending-stream behavior
for this new producer path, and Linux capacity saturation were not verified here.
