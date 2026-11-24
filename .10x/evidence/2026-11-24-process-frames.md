# Process error-frame audit

Baseline: `08160bf9755987ea612beb550bfefabe50c0dd4f`. Issue #205.
Inventory at start: zero PRs, only blocked #150. No release or cloud work.

## Current primary guidance

Google's [Storage Write best practices](https://docs.cloud.google.com/bigquery/docs/write-api-best-practices)
recommend reusing connections, limiting application-created streams, batching and
using default streams where their delivery contract suffices. Offsets apply to a
stream; they are not an end-to-end Singer exactly-once guarantee. This audit adds
no remote retry or stream-creation behavior.

The [Singer SDK at-least-once contract](https://sdk.meltano.com/en/latest/implementation/at_least_once.html)
allows replay after interrupted runs, which may duplicate already accepted records.
This makes withholding STATE on incomplete work essential; preserving diagnostics
also matters for reconciling accepted work before restart.

Python [documents](https://docs.python.org/3.12/library/multiprocessing.html#pipes-and-queues)
that simultaneous writes to the same pipe end can corrupt data, and killing a
writer can leave message boundaries unusable. A shared send lock alone does not
recover a partial frame or automatically release a dead owner's lock.

## Real entry point and controlled boundaries

Run from the repository root with its environment:

```sh
PYTHONPATH=. python .10x/evidence/2026-11-24-process-frame-probe.py
```

This POSIX characterization script constructs TargetBigQuery normally and runs
actual SCHEMA/RECORD/STATE parsing followed by process_endofpipe. No sink or target
constructor, enqueue, receive, checkpoint or worker loop is stubbed. The metadata
client returns generated existing table metadata; each child's insert client
returns generated ordinary per-row errors. Those are the only external API
substitutions. A worker bootstrap installs that fake client then calls its normal
production run method. Stdout is observed at the caller boundary.

Concurrent mode sends 11,500 rows with supported batch_size=500/max_workers=2.
A 1.1-second producer pause lets the existing worker-creation interval expire;
the normal backlog predicate starts the second worker. A barrier at the fake API
response synchronizes two failures, each containing 500 row-error details. The
queue, error pipe, serialization and worker scheduling remain real and unmodified.

Interrupted mode sends 500 rows, waits for the error pipe to become readable, then
SIGKILLs only its owned worker. This models abrupt process death during an error
send; it is not presented as a routine service error or a measured production rate.
The outer observer imposes a 12-second observation deadline. Only after that
expires does cleanup send SIGUSR1 to the harness; its handler closes the parent's
retained error writer. A fallback kills only recorded probe-created PIDs if cleanup
does not finish. This intervention is test cleanup and causal evidence, not a
production correction. Expected hard-death cleanup may invalidate a frame.

## Observed outcomes

Captured local results: `2026-11-24-process-frame-results.json`.

| Experiment | Observed caller outcome | STATE |
| --- | --- | --- |
| Six concurrent local trials | Five RuntimeErrors preserved both complete diagnostics; one UnicodeDecodeError preserved neither | None |
| Interrupted writer, initial campaign | Parent exceeded deadline; outer cleanup killed harness | No stdout write observed |
| Interrupted writer, cleanup-assisted campaign | Parent exceeded deadline; closing retained writer then released it with OSError | None |
| Independent repeat: three concurrent trials | Two preserved both diagnostics; one UnicodeDecodeError preserved neither | None |
| Independent interrupted repeat | Same deadline stall, then OSError after cleanup-only close | None |

The initial hard-kill campaign reported resource-tracker cleanup warnings. The
cleanup-assisted script and independent repeat exited their harnesses normally,
without that warning. Concurrent corruption is intermittent: the probe must not
become a flaky CI test that requires corruption on every run. Tests demonstrate
local macOS spawn behavior; Windows, fork contexts, remote APIs, production rates
and broader delivery behavior remain unverified.

These findings differ from #202 (intact error backpressure) and #204 (GCS URI
collection). Those fixes remain useful, but do not frame shared writes or establish
EOF after interrupted writers. No premature checkpoint was observed here.

## Invariant and next gate

A correction must preserve normal full diagnostics while returning explicit failure
for incomplete/corrupt frames, with no STATE/publication or automatic replay. A
lock-only patch would leave the dead-owner and incomplete-frame cases unresolved;
a queue substitution would need independent feeder-loss and lock-death proof.
Neither is a defensible correction on this evidence alone.

Issue #205 requires ownership-aware channel design and bounded death/reception tests:
concurrent healthy errors; death before, during and after sends; surviving siblings;
spawn and supported fork contexts; repeated checkpoints and new worker creation;
normal diagnostics, permissive behavior, memory/cleanup bounds and fail-closed
publication. Complete diagnostics cannot be promised for a killed sender that never
finished delivery. No production change is included in this evidence PR.

Separate source-level note: Storage Write process_batch still calls Queue.qsize,
which Python documents as unavailable on some platforms. Earlier probe wrappers
substituting qsize=0 do not establish real producer portability. Validate that entry
point separately instead of folding a queue/backpressure redesign into this audit.
