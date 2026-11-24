"""Bounded offline probe of complete Singer input and shared error frames.

Run from the repository root with PYTHONPATH=. and its project interpreter.
Only BigQuery clients are replaced; production target/SDK and worker loops run.
"""

import contextlib
import io
import json
import logging
import multiprocessing as mp
import os
import select
import signal
import time
from types import SimpleNamespace

from google.cloud import bigquery

from target_bigquery import core, streaming_insert
from target_bigquery.constants import DEFAULT_SCHEMA
from target_bigquery.target import TargetBigQuery


class OfflineClient:
    def get_dataset(self, ref):
        return bigquery.Dataset(ref)

    def get_table(self, ref):
        return bigquery.Table(ref, schema=DEFAULT_SCHEMA)


def run_api_worker():
    worker = mp.current_process()

    def insert(**kwargs):
        if worker.interrupted:
            assert worker.response_release.wait(6)
        if worker.api_barrier is not None:
            worker.api_barrier.wait(timeout=6)
            assert worker.response_release.wait(6)
            worker.api_barrier = None
        if worker.crash:
            os.kill(os.getpid(), signal.SIGKILL)
        if worker.healthy:
            with worker.accepted.get_lock():
                worker.accepted.value += len(kwargs["json_rows"])
            return []
        return [
            {
                "index": index,
                "errors": [
                    {
                        "reason": "invalid",
                        "location": "data",
                        "message": f"{worker.ext_id}:row-{index}: The supplied value does not match the configured field type; check the upstream schema and record value.",
                    }
                ],
            }
            for index in range(len(kwargs["json_rows"]))
        ]

    streaming_insert.bigquery_client_factory = lambda _: SimpleNamespace(insert_rows_json=insert)
    type(worker).run(worker)


def harness(mode, output, start_method):
    os.setsid()
    mp.set_start_method(start_method, force=True)
    logging.disable(logging.CRITICAL)
    with open(os.devnull, "w") as devnull:
        os.dup2(devnull.fileno(), 2)
    core.bigquery_client_factory = lambda _: OfflineClient()
    target = TargetBigQuery(
        config={
            "project": "offline",
            "dataset": "offline",
            "method": "streaming_insert",
            "fail_fast": mode != "permissive",
            "batch_size": 500 if mode in ("concurrent", "permissive", "survivor") else 5000,
            "options": {"process_pool": True, "max_workers": 2},
        }
    )

    # Test cleanup only: let the outer observer unblock an already measured stall.
    def rescue(signum, frame):
        target.error_notifier.close()
        output.send({"cleanup_closed_parent_error_writer": True})

    signal.signal(signal.SIGUSR1, rescue)
    original_factory = target.worker_factory
    created = []
    barrier = mp.Barrier(2) if mode in ("concurrent", "permissive", "survivor") else None
    response_release = mp.Event()
    accepted = mp.Value("i", 0)

    def factory():
        worker = original_factory()
        worker.healthy = mode == "healthy" or (mode == "survivor" and bool(created))
        worker.crash = mode == "survivor" and not created
        worker.interrupted = mode == "interrupted"
        worker.accepted = accepted
        worker.api_barrier = barrier
        worker.response_release = response_release
        worker.run = run_api_worker
        created.append(worker)
        return worker

    target.worker_factory = factory

    class ObservedOutput(io.StringIO):
        def write(self, value):
            if value.strip():
                output.send({"state_write": value})
            return super().write(value)

    stdout = ObservedOutput()
    reported = 0

    def lines():
        nonlocal reported
        yield json.dumps(
            {
                "type": "SCHEMA",
                "stream": "rows",
                "schema": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                },
                "key_properties": [],
            }
        )
        rows = 11500 if mode in ("concurrent", "permissive", "survivor") else 5000
        for index in range(rows):
            if index == 10500:
                # Allow the normal one-second worker creation interval to expire.
                time.sleep(1.1)
            yield json.dumps({"type": "RECORD", "stream": "rows", "record": {"id": index}})
            pids = [w.pid for w in created if w.pid is not None]
            if len(pids) != reported:
                output.send({"workers": pids})
                reported = len(pids)
        yield json.dumps({"type": "STATE", "value": {"position": rows}})
        output.send({"state_received": bool(target._latest_state)})
        response_release.set()

        if mode == "interrupted":
            receiver = next(iter(target._process_error_channels.values()))
            assert select.select([receiver.endpoint], [], [], 6)[0], (
                "worker never began its error send"
            )
            worker = created[0]
            assert worker.is_alive()
            os.kill(worker.pid, signal.SIGKILL)
            worker.join(2)
            output.send({"interrupted_exit": worker.exitcode})

    try:
        with contextlib.redirect_stdout(stdout):
            target.process_lines(lines())
            if mode == "healthy":
                target.drain_all()
                target.process_lines(lines())
            target.process_endofpipe()
    except Exception as exc:
        pending = list(target._pending_worker_errors)
        diagnostics = [str(exc.__cause__ or exc)] + [message for _, message in pending]
        output.send(
            {
                "outcome": "raised",
                "accepted_rows": accepted.value,
                "exception": type(exc).__name__,
                "cause": type(exc.__cause__).__name__ if exc.__cause__ else None,
                "incomplete_diagnostic": any("incomplete diagnostic" in d for d in diagnostics),
                "complete_diagnostics": sum("row-0:" in d and "row-499:" in d for d in diagnostics),
                "state_output": stdout.getvalue(),
                "worker_count": len(created),
            }
        )
    else:
        output.send(
            {
                "outcome": "returned",
                "state_output": stdout.getvalue(),
                "worker_count": len(created),
                "channels": len(target._process_error_channels),
            }
        )
    finally:
        for worker in created:
            if worker.is_alive():
                worker.terminate()
            worker.join(2)
        target.queue.cancel_join_thread()
        target.queue.close()


def probe(mode, start_method="spawn"):
    context = mp.get_context("spawn")
    received, sent = context.Pipe(False)
    process = context.Process(target=harness, args=(mode, sent, start_method))
    process.start()
    messages = []
    deadline = time.monotonic() + (40 if mode == "healthy" else 12)
    while time.monotonic() < deadline and process.is_alive():
        if received.poll(0.1):
            message = received.recv()
            messages.append(message)
    timed_out = process.is_alive()
    if timed_out:
        os.kill(process.pid, signal.SIGUSR1)
        process.join(3)
    if process.is_alive():
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    process.join(3)
    while received.poll():
        messages.append(received.recv())
    result = {
        "mode": mode,
        "timed_out": timed_out,
        "harness_exit": process.exitcode,
        "events": messages,
    }
    process.close()
    received.close()
    sent.close()
    return result


if __name__ == "__main__":
    for kind in ("concurrent", "concurrent", "concurrent", "interrupted"):
        print(json.dumps(probe(kind)), flush=True)
