# Idle checkpoint and parent serialization

Baseline: `93ebc4145db8fe092b281d1327ef792550239042`. Issue #197.

Python documents that multiprocessing.Queue serializes in a background feeder;
put returning does not establish delivery. Joining an idle consumer does not join
the parent's feeder. See [Python multiprocessing queues](https://docs.python.org/3.12/library/multiprocessing.html#pipes-and-queues).
Singer's [at-least-once contract](https://sdk.meltano.com/en/latest/implementation/at_least_once.html)
requires processed records before confirmed state advances; replay after interruption
can duplicate previously accepted records. Neither contract promises exactly once.

## Reproduction

`test_idle_checkpoint.py::test_idle_checkpoint_refuses_unresolved_serialization`
uses the target's real process queue, standard BatchJobProcessWorker with spawn,
and a Job produced by actual process_record/process_batch with a compressed buffer
and TableReference. A reducer registered for that real Job class pauses parent
serialization on bounded threading Events. The child runs the normal worker loop
with fake APIs, accelerating only Queue.get's existing idle timeout to 0.05 seconds.
There is no EOF sentinel, custom unpicklable payload or cloud operation.

While held: the worker exits 0 with normal_completion set, no API is observed,
one job remains submitted, and no feeder error exists. On the baseline, drain_all
runs checkpoint hooks and emits STATE before the gate is released. Both regression
cases failed before the production change (2 failed in 3.15 seconds).

After release, the success case receives the valid Job from the real queue after
its consumer exited. The failure case deliberately raises TypeError and confirms
the completed feeder error only then. These are controlled scheduling/failure
injections into a supported job shape, not evidence of frequency in production.

## Correction and acceptance

Reuse the existing submitted/completed counter. After joined process workers and
existing error checks, drain completion notifications and reject any nonzero count
before hooks/STATE. Positive and negative mismatches reject; counts persist and
notifications consumed earlier by drain_one are not consumed twice. No new wait,
join, Queue.empty/qsize proof, transport retry or thread-mode change. Permissive
mode warns and proceeds as before.

Ten new cases cover both serialization outcomes, checkpoint/EOF mismatches,
permissive/thread behavior, mixed streams and consecutive checkpoints with early
notification consumption. Existing real-spawn two-job tests cover all five worker
variants with the new process gate, normal completion and terminal API failures.
A zero count does not supersede transport persistence checks and commit/load hooks.

Full offline suite: 637 passed, 15 cloud skips (35 local resource_tracker cleanup
warnings). Ruff and mypy pass. Independent review: 94 focused passes, no blockers.
Exact-head CI and post-merge verification are recorded in ROADMAP.md when complete.
Existing completion-pipe backpressure and unbounded joins are a separate next
investigation; this change does not solve them. No live BigQuery claims.
