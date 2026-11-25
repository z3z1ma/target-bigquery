"""A completed feeder failure must not disappear behind a normal worker exit."""

import multiprocessing as mp
from multiprocessing import resource_sharer
from threading import Event
from types import SimpleNamespace

import pytest

from target_bigquery import gcs_stage, storage_write
from target_bigquery.core import Compressor, ParType
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_core_pure import make_bigquery_table
from target_bigquery.tests.test_process_data_jobs import run_offline_data_worker
from target_bigquery.tests.test_worker_exit import target_with_workers
from target_bigquery.tests.test_workers import FakeNotifier


@pytest.mark.parametrize("method", ["gcs", "storage"])
@pytest.mark.parametrize("eof", [False, True])
def test_producer_connection_reduction_failure_blocks_state(monkeypatch, method, eof):
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    owner = object.__new__(TargetBigQuery)
    owner._config = {"options": {"process_pool": True}}
    executor, pipe, factory, _ = owner.get_parallelization_components()
    queue = factory()
    notification, notifier = pipe(False)
    observed, sent = pipe(False)
    entered, release = Event(), Event()
    cls = (
        gcs_stage.BigQueryGcsStagingSink
        if method == "gcs"
        else storage_write.BigQueryStorageWriteSink
    )
    sink = object.__new__(cls)
    sink._config = {}
    sink.table = make_bigquery_table()
    sink.global_par_typ = ParType.PROCESS
    sink.increment_jobs_enqueued = lambda: None
    jobs = []
    sink.global_queue = SimpleNamespace(put=jobs.append)
    sink.enqueue_job = jobs.append
    if method == "gcs":
        sink.buffer = Compressor()
        sink.bucket_name = "offline"
        sink.gcs_notifier = notifier
    else:
        sink.parent = "projects/p/datasets/d/tables/t"
        sink.stream_notifier = notifier
        sink.template = storage_write.generate_template(sink.proto_schema)
        sink.start_batch({})
    sink.process_record(sink.preprocess_record({"id": 1}, {}), {})
    sink.process_batch({"batch_id": "offline"})
    worker = cls.worker_cls_factory(executor, {})(
        ext_id="feeder-test",
        queue=queue,
        credentials=None,
        job_notifier=FakeNotifier(),
        error_notifier=FakeNotifier(),
        log_notifier=FakeNotifier(),
    )
    worker.observed, worker.fail_transport = sent, False
    worker.run = run_offline_data_worker
    worker.start()

    def unavailable(fd):
        entered.set()
        assert release.wait(5)
        raise PermissionError("offline IPC handle sharing unavailable")

    monkeypatch.setattr(resource_sharer, "DupFd", unavailable)
    try:
        queue.put(jobs[0])
        assert entered.wait(5)
        queue.put(None)
        release.set()
        worker.join(10)
        assert worker.exitcode == 0
        assert worker.normal_completion.is_set()
        assert not observed.poll()
        target, publication, emitted = target_with_workers([worker])
        target.queue = queue
        with pytest.raises(RuntimeError, match="IPC handle sharing unavailable"):
            target.drain_all(is_endofpipe=eof)
        assert emitted == []
        assert not publication.published
        assert publication.checkpoints == 0
        # Failure evidence remains latched across later checks.
        with pytest.raises(RuntimeError, match="IPC handle sharing unavailable"):
            target._handle_worker_error(shutdown_workers=False)
    finally:
        release.set()
        if worker.is_alive():
            worker.terminate()
            worker.join(5)
        worker.close()
        for conn in (notification, notifier, observed, sent):
            conn.close()
        queue.cancel_join_thread()
        queue.close()
        mp.set_start_method(previous, force=True)


def test_feeder_error_remains_first_failure_and_warns_when_permissive(monkeypatch, caplog):
    from multiprocessing.queues import Queue

    from target_bigquery.process_queue import ProcessJobQueue

    monkeypatch.setattr(Queue, "_on_queue_feeder_error", staticmethod(lambda *args: None))
    queue = ProcessJobQueue()
    first = ValueError("first failure")
    try:
        queue._on_queue_feeder_error(first, None)
        queue._on_queue_feeder_error(ValueError("later failure"), None)
        assert queue.feeder_error is first
        target, _, emitted = target_with_workers([], fail_fast=False)
        target.queue = queue
        target.drain_all()
        assert emitted == [{"pos": 1}]
        assert "first failure" in caplog.text
    finally:
        queue.cancel_join_thread()
        queue.close()
