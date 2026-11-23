# Error and sink-owned pipe lifecycle

Baseline: `a1398b33534edf537852187ca73d1e5ca8818d02`. Issue #201.

| Channel | Writer and timing | Parent consumer | Consequence before this change |
| --- | --- | --- | --- |
| Error | All transport workers send exception/diagnostic tuples. Batch, insert and GCS fatal paths send before raising. Storage Write future completion errors can send before completion notification and normal exit. | `_handle_worker_error`, after joins at checkpoint/EOF; also during ordinary `drain_one` | A large complete error can block worker exit while the parent joins. |
| GCS URI | Upload worker sends object path after upload, before completion count. | `_load_pending_uris` in pre-state/cleanup hooks, after worker checks | A full per-sink pipe can prevent workers reaching hooks; receiving a URI alone is not a BigQuery load. |
| Storage stream name | Worker sends name when creating a cached stream component, before append dispatch. | `commit_streams` in pre-state/cleanup hooks, after worker checks | Pressure depends on stream creation/cache churn, not one name per row. Receiving a name does not finalize or commit it. |

Checkpoint drains producers, joins workers, checks errors/completions, runs hooks,
then emits STATE. EOF first queues normal sentinels and joins, then checks and
cleans up before STATE. Hard nonzero worker exits and missing normal completion
remain separate failure checks. Earlier remote work may have succeeded before a
later task fails; no notification collector implies delivery or authorizes replay.

## Reproduced highest-impact path

`test_process_error_pressure.py` uses actual streaming-insert process_record and
process_batch jobs, a standard spawned worker, unchanged real pipes and fake APIs.
A one-row batch succeeds, then a supported 500-row batch returns ordinary per-row
validation details (~100 characters of explanatory text per row). The target's
existing RuntimeError and traceback serialization makes the error tuple exceed
the local pipe capacity. There is no custom unpicklable object, resized pipe or
single synthetic megabyte exception.

Both baseline checkpoint/EOF cases stalled beyond their eight-second bounded
observation; 2 failures in 17.84 seconds. No hooks or STATE advanced. Test cleanup
terminates only owned children and disables the old handler before termination to
avoid reading a deliberately interrupted frame during baseline teardown.

Google's [Python client reference](https://docs.cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client)
defines per-row index/error mappings. Its [streaming troubleshooting guidance](https://docs.cloud.google.com/bigquery/docs/troubleshoot-storage-api)
explains that schema errors can produce an entry for each row. The response shape
is therefore supported; wording here is generated test data, not a recorded cloud
response. Production incidence and exact remote error sizes remain unmeasured.

## Correction and acceptance

While process notifications are drained, receive complete error tuples into a
parent-owned deque. The existing handler consumes buffered errors before newer
pipe messages, preserving exception chaining, complete diagnostic text and order.
The collector does not raise or invoke shutdown. Existing gates still run before
hooks/STATE, and explicit permissive mode still logs and proceeds. Thread draining,
API retry policy and checkpoint semantics remain unchanged.

Ten tests cover checkpoint/EOF, one/two workers, partial earlier work, full first
and last row details, original exception cause, fail-fast/permissive behavior,
buffered ordering and unchanged thread collection. Full-suite/review/CI results
are recorded in ROADMAP.md. Existing hard-exit and transport persistence tests
remain applicable; no live cloud operations were used.

## Separate risks and next gate

Python's [Connection contract](https://docs.python.org/3.12/library/multiprocessing.html#pipes-and-queues)
does not make poll() a complete-frame guarantee. A writer dying mid-frame while
another write handle stays open can still block recv; unsynchronized concurrent
large writers can corrupt framing. This patch does not resolve either, impose an
overall join deadline, bound buffered-error memory, or drain sink-owned pipes.
Only intact error sends were tested. Next: bounded actual-producer GCS URI pressure
reproduction and retaining collected paths without loading before error checks.
Storage stream-name pressure should be measured against cache/worker creation
bounds separately. No speculative sink-lifecycle or retry changes in this patch.
