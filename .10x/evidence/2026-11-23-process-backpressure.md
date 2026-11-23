# Completion/log pipe backpressure

Baseline: `ef025f62bbb62d2b7fc4e7c2e9d9025f76654aa1`. Issue #199.

## Evidence and realistic workload

The SDK's `_drain_all` calls target `drain_one`, which consumes completion/log
notifications before enqueueing the next batch. A slow first API request can hold
workers while many subsequent jobs accumulate. At checkpoint/EOF the parent then
joins before consuming more notifications. A worker can block sending an ordinary
completion or log message while the parent waits for its exit.

`test_process_backpressure.py` uses the real target queue and pipes, standard
spawned StreamingInsertProcessWorker and actual process_record/process_batch jobs.
The supported batch_size=1 produces 10,000 one-record jobs. A bounded fake API gate
holds initial processing until the backlog is submitted. No pipe resizing or
oversized synthetic messages are used. Only the normal worker idle timeout is
accelerated for checkpoint tests; the EOF path sends normal target sentinels.
The main test harness waits at most 15 seconds then terminates only its own children.

Four baseline cases failed (32.94 seconds total, eight-second observation per
case): completion-only checkpoint and EOF stalled after 8,193 API calls; enabling
the real log pipe stalled each after 820 calls. These are local macOS measurements,
not universal pipe capacities. No STATE or publication occurred during the stall.
The later test version uses the normal preprocessed JSON-string field shape.

The backlog is plausible, not an injected transport fault: a slow service followed
by fast producer submissions can accumulate pending jobs, and batch_size is
configurable. The test models 10,000 records at batch_size=1. At the default 500
rows per batch, the same job counts would represent larger workloads (roughly
410,000 rows at the observed log stall). No production frequency or cloud behavior
was measured, and API results here are immediate offline fakes after gate release.

Python documents [bounded Process.join calls and pipes](https://docs.python.org/3.12/library/multiprocessing.html#pipes-and-queues).
Joining alone does not consume these child-to-parent messages. The correction
reuses the current notification consumer while process joins poll at 0.1 seconds,
then drains once more after exit. The interval is a polling cadence, not a job or
shutdown deadline. Thread joins, API timeouts, retries and job FIFO are unchanged.

## Validation and limits

Eight new cases cover completion-only pressure, real logs, one/two workers,
single-worker FIFO, checkpoint/EOF and a terminal API error after the backlog.
Successful drains must consume all jobs, reach normal worker completion and emit
STATE only after hooks. Failure must retain its diagnostic and withhold hooks/STATE.
All waits and cleanup in the harness are bounded. The full offline suite passed
645 tests with 15 cloud skips; Ruff and mypy pass. Review/CI results are recorded in
ROADMAP.md when available.

This is a targeted liveness correction, not a general IPC redesign. Large error
payloads or accumulated errors may still block the error pipe; GCS URI and Storage
Write stream-name pipes are consumed by separate sink hooks and can also back up.
Those channels and shared-writer framing need separate bounded reproductions before
changing their lifecycle. The existing total join duration remains unbounded for
unrelated hangs. No cloud experiments, publication, tags or permissions changes.
