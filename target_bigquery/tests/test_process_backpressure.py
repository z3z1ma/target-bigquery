"""Process joins must continue consuming worker notifications."""

import json
import multiprocessing as mp
from threading import Thread
from types import SimpleNamespace

import pytest

from target_bigquery import streaming_insert
from target_bigquery.core import ParType
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_core_pure import make_bigquery_table
from target_bigquery.tests.test_worker_exit import target_with_workers
from target_bigquery.tests.test_workers import FakeNotifier


def run_backlogged_worker():
    worker = mp.current_process()

    def insert(**kwargs):
        assert worker.release.wait(15)
        record_id = json.loads(kwargs["json_rows"][0]["data"])["id"]
        with worker.accepted.get_lock():
            if worker.single_worker:
                assert record_id == worker.accepted.value
            worker.accepted.value += 1
        if worker.fail_last and record_id == 9999:
            raise RuntimeError("offline API failure after backlog")
        return []

    streaming_insert.bigquery_client_factory = lambda _: SimpleNamespace(insert_rows_json=insert)
    original_get = worker.queue.get
    worker.queue.get = lambda timeout: original_get(timeout=0.1)
    type(worker).run(worker)


@pytest.mark.parametrize("eof", [False, True])
@pytest.mark.parametrize(
    "worker_count,real_logs,fail_last",
    [
        (1, False, False),
        (1, True, False),
        (2, True, False),
        (2, True, True),
    ],
)
def test_process_join_drains_backlogged_notifications(eof, worker_count, real_logs, fail_last):
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    owner = object.__new__(TargetBigQuery)
    owner._config = {"options": {"process_pool": True}}
    executor, pipe, factory, _ = owner.get_parallelization_components()
    queue = factory()
    completed, done = pipe(False)
    logged, log = pipe(False)
    # 80 KB of eight-byte True notifications; leave OS pipe capacity unchanged.
    count = 10000
    errors_received, errors_sent = pipe(False)
    release, accepted = mp.Event(), mp.Value("i", 0)
    workers = []
    for index in range(worker_count):
        worker = streaming_insert.BigQueryStreamingInsertSink.worker_cls_factory(executor, {})(
            ext_id=f"backlogged-{index}",
            queue=queue,
            credentials=None,
            job_notifier=done,
            error_notifier=errors_sent,
            log_notifier=log if real_logs else FakeNotifier(),
        )
        worker.release, worker.accepted = release, accepted
        worker.single_worker, worker.fail_last = worker_count == 1, fail_last
        worker.run = run_backlogged_worker
        workers.append(worker)
    target, sink, emitted = target_with_workers(workers.copy())
    target.error_notification = errors_received
    target.par_typ = ParType.PROCESS
    target.queue = queue
    target.job_notification = completed
    target.log_notification = logged
    target.worker_pings = {}
    target._jobs_enqueued = 0
    producer = object.__new__(streaming_insert.BigQueryStreamingInsertSink)
    producer._config = {"batch_size": 1}
    producer.table = make_bigquery_table()
    producer.records_to_drain = []
    producer.global_par_typ = ParType.PROCESS
    producer.global_queue = queue
    producer.increment_jobs_enqueued = target.increment_jobs_enqueued
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
        for i in range(count):
            producer.process_record({"data": json.dumps({"id": i})}, {})
            producer.process_batch({})
        release.set()
        draining.start()
        draining.join(15)
        assert not draining.is_alive(), f"join stalled after {accepted.value}/{count} APIs"
        assert accepted.value == count
        if fail_last:
            assert len(errors) == 1 and "offline API failure" in str(errors[0])
            assert emitted == [] and not sink.published and sink.checkpoints == 0
        else:
            assert errors == []
            assert all(w.exitcode == 0 and w.normal_completion.is_set() for w in workers)
            assert target._jobs_enqueued == 0
            assert emitted == [{"pos": 1}]
            assert sink.published if eof else sink.checkpoints == 1
    finally:
        release.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
        if draining.ident is not None:
            draining.join(5)
        for worker in workers:
            worker.close()
        for conn in (completed, done, logged, log, errors_received, errors_sent):
            conn.close()
        queue.cancel_join_thread()
        queue.close()
        mp.set_start_method(previous, force=True)
