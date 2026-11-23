"""Large ordinary API error details must survive process joins."""

import multiprocessing as mp
from threading import Thread
from types import SimpleNamespace

import pytest

from target_bigquery import streaming_insert
from target_bigquery.core import ParType
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_core_pure import make_bigquery_table
from target_bigquery.tests.test_worker_exit import target_with_workers

DETAIL = "The supplied value does not match the configured field type; check the upstream schema and record value."


def run_row_error_worker():
    worker = mp.current_process()

    def insert(**kwargs):
        with worker.calls.get_lock():
            worker.calls.value += 1
        if len(kwargs["json_rows"]) == 1:
            return []
        return [
            {
                "index": index,
                "errors": [
                    {"reason": "invalid", "location": "data", "message": f"row-{index}: {DETAIL}"}
                ],
            }
            for index in range(len(kwargs["json_rows"]))
        ]

    streaming_insert.bigquery_client_factory = lambda _: SimpleNamespace(insert_rows_json=insert)
    original_get = worker.queue.get
    worker.queue.get = lambda timeout: original_get(timeout=0.1)
    type(worker).run(worker)


@pytest.mark.parametrize("eof", [False, True])
@pytest.mark.parametrize("worker_count", [1, 2])
@pytest.mark.parametrize("fail_fast", [False, True])
def test_large_row_error_survives_process_join(eof, worker_count, fail_fast, caplog):
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    owner = object.__new__(TargetBigQuery)
    owner._config = {"options": {"process_pool": True}}
    executor, pipe, factory, _ = owner.get_parallelization_components()
    queue = factory()
    completed, done = pipe(False)
    logged, log = pipe(False)
    received, sent = pipe(False)
    calls = mp.Value("i", 0)
    workers = []
    for index in range(worker_count):
        worker = streaming_insert.BigQueryStreamingInsertSink.worker_cls_factory(executor, {})(
            ext_id=f"row-errors-{index}",
            queue=queue,
            credentials=None,
            job_notifier=done,
            error_notifier=sent,
            log_notifier=log,
        )
        worker.calls = calls
        worker.run = run_row_error_worker
        workers.append(worker)
    target, sink, emitted = target_with_workers(workers.copy(), fail_fast=fail_fast)
    target.par_typ = ParType.PROCESS
    target.queue = queue
    target.job_notification, target.log_notification = completed, logged
    target.error_notification = received
    target.worker_pings = {}
    target._jobs_enqueued = 0
    producer = object.__new__(streaming_insert.BigQueryStreamingInsertSink)
    producer._config = {}
    producer.table = make_bigquery_table()
    producer.records_to_drain = []
    producer.global_queue = queue
    producer.increment_jobs_enqueued = target.increment_jobs_enqueued
    for size in (1, 500):
        for _ in range(size):
            producer.process_record({"data": "{}"}, {})
        producer.process_batch({})
    errors = []

    def checkpoint():
        try:
            target.drain_all(is_endofpipe=eof)
        except Exception as exc:
            errors.append(exc)

    draining = Thread(target=checkpoint, daemon=True)
    for worker in workers:
        worker.start()
    try:
        draining.start()
        draining.join(8)
        assert not draining.is_alive(), "join blocked on ordinary 500-row error response"
        assert calls.value == 2
        if fail_fast:
            assert len(errors) == 1
            assert "row-0:" in str(errors[0]) and "row-499:" in str(errors[0])
            assert isinstance(errors[0].__cause__, RuntimeError)
            assert DETAIL in str(errors[0].__cause__)
            assert emitted == [] and not sink.published and sink.checkpoints == 0
        else:
            assert errors == []
            assert "row-0:" in caplog.text and "row-499:" in caplog.text
            assert emitted == [{"pos": 1}]
            assert sink.published if eof else sink.checkpoints == 1
        assert not target._pending_worker_errors
    finally:
        # On baseline timeout, avoid attempting to read the killed writer's partial frame.
        target._handle_worker_error = lambda **kwargs: None
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
        draining.join(5)
        for worker in workers:
            worker.close()
        for conn in (completed, done, logged, log, received, sent):
            conn.close()
        queue.cancel_join_thread()
        queue.close()
        mp.set_start_method(previous, force=True)


@pytest.mark.parametrize("process_mode", [False, True])
def test_buffered_error_order_and_thread_behavior(process_mode):
    received, sent = mp.Pipe(False)
    target, _, _ = target_with_workers([])
    target.par_typ = ParType.PROCESS if process_mode else ParType.THREAD
    target.error_notification = received
    target.job_notification = target.log_notification = SimpleNamespace(poll=lambda: False)
    try:
        sent.send((ValueError("first cause"), "first diagnostic"))
        target._drain_worker_notifications()
        assert len(target._pending_worker_errors) == int(process_mode)
        sent.send((RuntimeError("second cause"), "second diagnostic"))
        for message in ("first", "second"):
            with pytest.raises(RuntimeError, match=f"{message} diagnostic") as error:
                target._handle_worker_error(shutdown_workers=False)
            assert str(error.value.__cause__) == f"{message} cause"
        assert not target._pending_worker_errors and not received.poll()
    finally:
        received.close()
        sent.close()
