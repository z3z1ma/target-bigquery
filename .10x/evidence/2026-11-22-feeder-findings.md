# Process queue feeder investigation

Baseline: main `3df9332`. No cloud APIs or credentials used.

1. Gated custom `__reduce__` failure: real Queue feeder drops an injected object,
   sends a later sentinel, standard spawned worker exits normally, parent EOF
   publishes/emits STATE. This alone is not a supported-input trigger.
2. Actual process_record/process_batch producers: batch and insert round trips
   pass. GCS and Storage Write fail in the restricted executor while reducing
   their real notifier Connection: resource_sharer Listener.bind raises
   PermissionError. Four failures, five passes including the injected probe.
3. The same nine probes outside the local socket restriction pass, including
   protobuf template/row preservation and use of received notifier connections.
   No normal JSON input was found that creates an inherently unpicklable payload.
   Process buffers are bytes; Arrow is not a target-produced payload.
4. Deterministic Connection-reduction fault injection on actual GCS/Storage jobs
   reproduces checkpoint/EOF advancement with standard workers. Four regressions
   fail before the containment change and pass afterward.

The bounded change records the first completed feeder error in the parent queue,
retains the standard traceback, and uses existing fail-fast gates. It does not
prove queue delivery, detect serialization still in progress, or fix idle-exit
ordering. Checkpoint fault tests consume a sentinel to establish completed feeder
processing. No synchronization wait or replay is added.

Validation: 627 offline tests passed, 15 cloud skips. Five local resource-tracker
reentrancy warnings occurred in existing spawn tests; no test failed. Ruff/mypy
pass. Independent focused review: 69 passed. The companion probe, run with local
IPC allowed, passes all nine cases after the fix:

```
PYTHONPATH=. python -m pytest .10x/evidence/2026-11-22-feeder-probe.py --benchmark-disable -q
```

Its gates and process waits are bounded; cleanup cancels queue feeder joining.
The observed restriction is an environment-triggered serialization failure, not
live BigQuery evidence or a claim that unrestricted ingestion normally fails.
