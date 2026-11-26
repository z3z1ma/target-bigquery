# Pending-stream creation timeout

On main `3d69a8d16d724d4c1a9fe119ad02e994c66b78af`, Storage Write pending-stream
creation did not pass the job timeout to `create_write_stream`. The installed
BigQuery Storage client's generated transport wrapper supplied a 1,200-second
RPC timeout and retry deadline, with an initial retry delay of 10 seconds.
This was a gap in timeout coverage; previous documentation only listed append,
finalize and commit completion, not stream creation.

## Caller-visible reproduction and correction

`target_bigquery/tests/storage_creation_probe.py` feeds SCHEMA, one RECORD and
STATE through TargetBigQuery.process_lines and process_endofpipe. The generated
BigQueryWriteClient and installed retry/timeout wrappers run unchanged. Only
metadata and RPC/append boundaries are fake; workers, queues, Singer parsing,
error propagation and commit scheduling are real. Each run uses an isolated,
bounded subprocess harness. No credentials or network channels are created.

Before the correction, both native macOS thread and spawn process modes reported
`create_timeout: 1200.0`, one RPC attempt, and exceeded the four-second observation
window when the fake RPC raised DeadlineExceeded. Neither emitted STATE. This
proves the configured 0.05-second creation bound was absent, not that a live
BigQuery call hung indefinitely or lost records.

The correction passes `timeout=job.timeout, retry=None`. Both modes now report
three creation attempts with timeout 0.05 and a caller-visible RuntimeError
caused by DeadlineExceeded. There are no append, finalize, commit or STATE events.
The existing target setup retry limit is unchanged. A first-attempt deadline
followed by success yields one append, one finalize/commit and STATE. Default
streams complete without calling create_write_stream. Six entry regressions
cover these outcomes with the real installed SDK wrappers.

This is a per-RPC deadline, not a whole-job/process deadline: three setup attempts
can consume roughly three configured timeouts plus overhead. Disabling nested
SDK retries also removes their backoff/transient tolerance. An ambiguous create
response can leave an unused pending stream; no records have been appended at
that point. No change is made to append retry/replay, default-stream delivery,
checkpoint policy or permissive errors. Offline tests do not prove live server
cancellation, persistence or cloud timing.

## Guidance checked 2026-11-26 and remaining gate

[Google's Storage Write guidance](https://docs.cloud.google.com/bigquery/docs/write-api-best-practices)
recommends default streams where appropriate, connection reuse and controlled
stream creation. Per-stream offsets are not an end-to-end replay guarantee.
The [generated client API](https://docs.cloud.google.com/python/docs/reference/bigquerystorage/latest/google.cloud.bigquery_storage_v1.services.big_query_write.BigQueryWriteClient)
accepts timeout and retry controls. [Meltano's at-least-once contract](https://sdk.meltano.com/en/latest/implementation/at_least_once.html)
allows replay of accepted records after interruption; checkpoint progress still
must follow successful target work. These are consistent with retaining the
existing fail-closed state handling rather than adding replay behavior.

The separate notification-pressure hypothesis is unproven: each sink owns its
pipe and each worker caches its stream, so records or distinct sinks cannot simply
be counted as repeated writes to one pipe. A supported high-worker/long-name
workload remains the next reproduction gate. No speculative IPC patch is included.
