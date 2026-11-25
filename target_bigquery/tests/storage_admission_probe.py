"""Real Singer entry-point probe with bounded fake API delays."""

import contextlib
import io
import json
import multiprocessing as mp
import os
import signal
import threading
import time
from queue import Full
from types import SimpleNamespace

from google.cloud import bigquery
from google.cloud.bigquery_storage_v1 import types, writer

from target_bigquery import core, storage_write
from target_bigquery.constants import DEFAULT_SCHEMA
from target_bigquery.proto_gen import proto_schema_factory_v2
from target_bigquery.target import TargetBigQuery


class Metadata:
    def get_dataset(self, ref):
        return bigquery.Dataset(ref)

    def get_table(self, ref):
        return bigquery.Table(ref, schema=DEFAULT_SCHEMA)


def install_api_fakes(entered, release, accepted, failure, commits):
    class Client:
        def create_write_stream(self, *, parent, **kwargs):
            return types.WriteStream(name=parent + f"/streams/offline-pending-{os.getpid()}")

        def finalize_write_stream(self, **kwargs):
            commits.append("finalize")

        def batch_commit_write_streams(self, request, **kwargs):
            commits.append("commit")
            if failure == "commit":
                raise RuntimeError("offline commit failure")
            return SimpleNamespace(stream_errors=[], commit_time="offline")

    class Append:
        def __init__(self, client, template):
            self._closed = False
            self.path = template.write_stream

        def send(self, request):
            entered.set()
            assert release.wait(10), "offline API gate timed out"
            if failure == "exit":
                raise SystemExit(7)
            message_class = proto_schema_factory_v2(DEFAULT_SCHEMA)
            for row in request.proto_rows.rows.serialized_rows:
                record = message_class.FromString(row)
                index = json.loads(record.data)["id"]
                with accepted.get_lock():
                    accepted[index] += 1
            future = writer.AppendRowsFuture(None)
            if failure == "append":
                future.set_exception(RuntimeError("offline append failure"))
            else:
                response = types.AppendRowsResponse()
                if not self.path.endswith("_default"):
                    response.append_result.offset = request.offset
                future.set_result(response)
            return future

        def close(self):
            self._closed = True

    storage_write.storage_client_factory = lambda _: Client()
    storage_write.writer.AppendRowsStream = Append


def offline_worker():
    worker = mp.current_process()
    install_api_fakes(*worker.api_args)
    type(worker).run(worker)


def harness(output, process_pool, pending, failure, rows, checkpoint):
    os.setsid()
    mp.set_start_method("spawn", force=True)
    # Four queued jobs is the existing limit on a one-CPU host, below the
    # fifteen-appends worker window. This is not an outstanding-job limit.
    storage_write.BigQueryStorageWriteSink.MAX_JOBS_QUEUED = 4
    entered, release = mp.Event(), mp.Event()
    accepted = mp.Array("i", rows)
    commits = []
    core.bigquery_client_factory = lambda _: Metadata()
    api_args = (entered, release, accepted, failure, commits)
    install_api_fakes(*api_args)
    target = TargetBigQuery(
        config={
            "project": "offline",
            "dataset": "offline",
            "method": "storage_write_api",
            "batch_size": 1,
            "options": {
                "process_pool": process_pool,
                "max_workers": 1,
                "storage_write_batch_mode": pending,
            },
        }
    )
    sentinel_full = threading.Event()
    actual_put = target.queue.put

    def observed_put(job, *args, **kwargs):
        # Observe the real queue result without substituting its behavior.
        try:
            return actual_put(job, *args, **kwargs)
        except Full:
            if job is None:
                sentinel_full.set()
            raise

    target.queue.put = observed_put
    created = []
    factory = target.worker_factory

    def create():
        worker = factory()
        if process_pool:
            worker.api_args = api_args
            worker.run = offline_worker
        created.append(worker)
        return worker

    target.worker_factory = create
    if failure == "feeder":
        from multiprocessing import reduction

        original_dup = reduction.DupFd

        def unavailable(fd):
            if threading.current_thread().name != "QueueFeederThread":
                return original_dup(fd)
            entered.set()
            assert release.wait(10)
            raise PermissionError("offline IPC handle sharing unavailable")

        reduction.DupFd = unavailable
    states = []

    class Output(io.StringIO):
        def write(self, value):
            if value.strip():
                states.append(
                    {
                        "value": json.loads(value),
                        "accepted": sum(accepted),
                        "commits": list(commits),
                    }
                )
            return super().write(value)

    observation = {}

    def release_when_full():
        try:
            assert entered.wait(8), "worker did not reach fake API"
            deadline = time.monotonic() + 8
            while not target.queue.full() and time.monotonic() < deadline:
                time.sleep(0.01)
            observation["full"] = target.queue.full()
            observation["state_before_release"] = bool(states)
            if rows == 5:
                observation["sentinel_full"] = sentinel_full.wait(8)
        finally:
            release.set()

    observer = threading.Thread(target=release_when_full, daemon=True)
    observer.start()
    messages = [
        {
            "type": "SCHEMA",
            "stream": "rows",
            "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
            "key_properties": [],
        }
    ]
    messages += [{"type": "RECORD", "stream": "rows", "record": {"id": i}} for i in range(rows)]
    messages += [{"type": "STATE", "value": {"position": rows}}]
    result = {}
    try:
        with contextlib.redirect_stdout(Output()):
            target.process_lines(map(json.dumps, messages))
            if checkpoint:
                target.drain_all()
                # A new real batch after the idle checkpoint must replace the
                # exited worker and reuse bounded admission safely.
                messages[-1] = {"type": "STATE", "value": {"position": rows * 2}}
                target.process_lines(map(json.dumps, messages))
            target.process_endofpipe()
        result["outcome"] = "returned"
    except Exception as exc:
        result.update(outcome=type(exc).__name__, detail=str(exc))
    finally:
        release.set()
        observer.join(2)
        # The target's actual shutdown path, including bounded sentinel admission.
        with contextlib.suppress(RuntimeError):
            target._shutdown_workers()
        for worker in created:
            worker.join(2)
            if worker.is_alive() and process_pool:
                worker.terminate()
                worker.join(2)
        if process_pool:
            target.queue.cancel_join_thread()
            target.queue.close()
    output.send(
        {
            **result,
            **observation,
            "states": states,
            "accepted": list(accepted),
            "commits": commits,
            "outstanding": target._jobs_enqueued,
            "worker_count": len(created),
        }
    )


def probe(process_pool, pending, failure=None, rows=40, checkpoint=False):
    context = mp.get_context("spawn")
    received, sent = context.Pipe(False)
    process = context.Process(
        target=harness, args=(sent, process_pool, pending, failure, rows, checkpoint)
    )
    process.start()
    try:
        assert received.poll(45 if checkpoint else 20), "Singer entry point stalled"
        result = received.recv()
        process.join(3)
        assert process.exitcode == 0
        return result
    finally:
        if process.is_alive():
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.join(3)
        process.close()
        received.close()
        sent.close()
