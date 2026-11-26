"""Real Singer and installed SDK paths with only RPC/append boundaries replaced."""

import contextlib
import io
import json
import multiprocessing as mp
import os
import signal
import threading
import time
from unittest import mock

from google.api_core import exceptions, gapic_v1
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types, writer
from google.cloud.bigquery_storage_v1.services.big_query_write.transports.base import (
    BigQueryWriteTransport,
)

from target_bigquery import core, storage_write
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.storage_admission_probe import Metadata


def install_boundaries(events, mode):
    attempts = 0

    def create(request, **kwargs):
        nonlocal attempts
        attempts += 1
        events.send({"create_timeout": kwargs.get("timeout"), "attempt": attempts})
        if mode == "exhaust" or (mode == "transient" and attempts == 1):
            threading.Event().wait(min(kwargs.get("timeout", 1), 0.5))
            raise exceptions.DeadlineExceeded("offline creation deadline")
        return types.WriteStream(name=request.parent + "/streams/offline-pending")

    def finalize(request, **kwargs):
        events.send({"finalize": request.name})
        return types.FinalizeWriteStreamResponse(row_count=1)

    def commit(request, **kwargs):
        events.send({"commit": list(request.write_streams)})
        return types.BatchCommitWriteStreamsResponse()

    transport = mock.create_autospec(BigQueryWriteTransport, instance=True)
    transport._host = "offline.invalid"
    transport.create_write_stream.side_effect = create
    transport.finalize_write_stream.side_effect = finalize
    transport.batch_commit_write_streams.side_effect = commit
    # Use the installed SDK's actual retry/timeout wrappers and generated client.
    # No credentials, channel or network connection is constructed.
    BigQueryWriteTransport._prep_wrapped_messages(transport, gapic_v1.client_info.ClientInfo())
    client = BigQueryWriteClient(transport=transport)
    storage_write.storage_client_factory = lambda _: client

    class Append:
        def __init__(self, client, template):
            self._closed = False
            self.path = template.write_stream

        def send(self, request):
            events.send({"append": len(request.proto_rows.rows.serialized_rows)})
            future = writer.AppendRowsFuture(None)
            response = types.AppendRowsResponse()
            if not self.path.endswith("_default"):
                response.append_result.offset = request.offset
            future.set_result(response)
            return future

        def close(self):
            self._closed = True

    storage_write.writer.AppendRowsStream = Append


def offline_worker():
    worker = mp.current_process()
    install_boundaries(worker.events, worker.mode)
    type(worker).run(worker)


def harness(events, process_pool, pending, mode):
    os.setsid()
    mp.set_start_method("spawn", force=True)
    core.bigquery_client_factory = lambda _: Metadata()
    install_boundaries(events, mode)
    target = TargetBigQuery(
        config={
            "project": "offline",
            "dataset": "offline",
            "method": "storage_write_api",
            "batch_size": 1,
            "timeout": 0.05,
            "options": {
                "process_pool": process_pool,
                "max_workers": 1,
                "storage_write_batch_mode": pending,
            },
        }
    )
    factory = target.worker_factory

    def create_worker():
        worker = factory()
        if process_pool:
            worker.events, worker.mode = events, mode
            worker.run = offline_worker
        return worker

    target.worker_factory = create_worker
    messages = [
        {
            "type": "SCHEMA",
            "stream": "rows",
            "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
            "key_properties": [],
        },
        {"type": "RECORD", "stream": "rows", "record": {"id": 1}},
        {"type": "STATE", "value": {"position": 1}},
    ]
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            target.process_lines(map(json.dumps, messages))
            target.process_endofpipe()
    except Exception as exc:
        events.send(
            {
                "outcome": type(exc).__name__,
                "cause": type(exc.__cause__).__name__,
                "state": output.getvalue(),
            }
        )
    else:
        events.send({"outcome": "returned", "state": output.getvalue()})
    finally:
        with contextlib.suppress(RuntimeError):
            target._shutdown_workers()
        if process_pool:
            target.queue.cancel_join_thread()
            target.queue.close()


def probe(process_pool, pending=True, mode="exhaust", deadline=8):
    context = mp.get_context("spawn")
    received, sent = context.Pipe(False)
    process = context.Process(target=harness, args=(sent, process_pool, pending, mode))
    process.start()
    events = []
    stop = time.monotonic() + deadline
    try:
        while process.is_alive() and time.monotonic() < stop:
            if received.poll(0.1):
                events.append(received.recv())
        timed_out = process.is_alive()
        if timed_out:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        process.join(3)
        while received.poll():
            events.append(received.recv())
        return {"timed_out": timed_out, "exitcode": process.exitcode, "events": events}
    finally:
        if process.is_alive():
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.join(3)
        process.close()
        received.close()
        sent.close()


if __name__ == "__main__":
    for process_pool in (False, True):
        print(json.dumps(probe(process_pool, deadline=4)), flush=True)
