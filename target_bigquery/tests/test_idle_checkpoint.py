"""Worker idle exit is not a barrier for the parent queue feeder."""

import multiprocessing as mp
from multiprocessing.reduction import ForkingPickler
from threading import Event

import pytest

from target_bigquery import batch_job
from target_bigquery.core import Compressor, ParType
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_core_pure import make_bigquery_table
from target_bigquery.tests.test_process_data_jobs import run_offline_data_worker
from target_bigquery.tests.test_worker_exit import target_with_workers
from target_bigquery.tests.test_workers import FakeNotifier


def run_short_idle_worker():
    # Accelerate the existing idle timeout; still use the real Queue.get/Empty path.
    worker = mp.current_process()
    original_get = worker.queue.get
    worker.queue.get = lambda timeout: original_get(timeout=0.05)
    run_offline_data_worker()


@pytest.mark.parametrize("eventual_failure", [False, True])
def test_idle_checkpoint_refuses_unresolved_serialization(monkeypatch, eventual_failure):
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    owner = object.__new__(TargetBigQuery)
    owner._config = {"options": {"process_pool": True}}
    executor, pipe, factory, _ = owner.get_parallelization_components()
    queue = factory()
    completed, done = pipe(False)
    observed, sent = pipe(False)
    entered, release, failed = Event(), Event(), Event()
    worker = batch_job.BigQueryBatchJobSink.worker_cls_factory(executor, {})(
        ext_id="idle-serializer",
        queue=queue,
        credentials=None,
        job_notifier=done,
        error_notifier=FakeNotifier(),
        log_notifier=FakeNotifier(),
    )
    worker.observed, worker.fail_transport = sent, False
    worker.run = run_short_idle_worker
    target, sink, emitted = target_with_workers([worker])
    target.par_typ = ParType.PROCESS
    target.queue = queue
    target.job_notification = completed
    target.log_notification = type("NoLogs", (), {"poll": lambda self: False})()
    target.worker_pings = {}
    target._jobs_enqueued = 0
    producer = object.__new__(batch_job.BigQueryBatchJobSink)
    producer._config = {}
    producer.table = make_bigquery_table()
    producer.buffer = Compressor()
    producer.global_par_typ = ParType.PROCESS
    producer.global_queue = queue
    producer.increment_jobs_enqueued = target.increment_jobs_enqueued
    producer.process_record({"data": {"id": 1}}, {})

    def reduce_job(job):
        entered.set()
        assert release.wait(10)
        if eventual_failure:
            raise TypeError("late serialization failure")
        return job.__reduce_ex__(4)

    original_error = queue._on_queue_feeder_error

    def on_error(error, obj):
        original_error(error, obj)
        failed.set()

    queue._on_queue_feeder_error = on_error
    monkeypatch.setitem(ForkingPickler._extra_reducers, batch_job.Job, reduce_job)
    worker.start()
    try:
        producer.process_batch({})
        assert entered.wait(5)
        worker.join(5)
        assert worker.exitcode == 0 and worker.normal_completion.is_set()
        assert queue.feeder_error is None
        assert target._jobs_enqueued == 1
        assert not observed.poll()
        rejection = None
        try:
            target.drain_all()
        except RuntimeError as error:
            rejection = error
        before_release = list(emitted), sink.checkpoints
        release.set()
        if eventual_failure:
            assert failed.wait(5)
            assert isinstance(queue.feeder_error, TypeError)
        else:
            # Serialization succeeded only after the consumer had already exited.
            assert isinstance(queue.get(timeout=5), batch_job.Job)
            assert queue.feeder_error is None
        assert rejection is not None and "completion" in str(rejection)
        assert before_release == ([], 0)
        assert not sink.published
    finally:
        producer.buffer.close()
        release.set()
        if worker.is_alive():
            worker.terminate()
            worker.join(5)
        worker.close()
        for conn in (completed, done, observed, sent):
            conn.close()
        queue.cancel_join_thread()
        queue.close()
        mp.set_start_method(previous, force=True)


@pytest.mark.parametrize("eof", [False, True])
@pytest.mark.parametrize("outstanding", [1, -1])
def test_process_accounting_mismatch_refuses_hooks(eof, outstanding):
    received, sent = mp.Pipe(False)
    target, sink, emitted = target_with_workers([])
    target.par_typ = ParType.PROCESS
    target._jobs_enqueued = outstanding
    target.job_notification = received
    target.worker_pings = {}
    try:
        with pytest.raises(RuntimeError, match="completion mismatch"):
            target.drain_all(is_endofpipe=eof)
        assert target._jobs_enqueued == outstanding
        assert emitted == [] and sink.checkpoints == 0 and not sink.published
    finally:
        received.close()
        sent.close()


@pytest.mark.parametrize("process_mode", [False, True])
def test_permissive_process_or_unchanged_thread_mode(process_mode, caplog):
    received, sent = mp.Pipe(False)
    target, sink, emitted = target_with_workers([], fail_fast=not process_mode)
    target.par_typ = ParType.PROCESS if process_mode else ParType.THREAD
    target._jobs_enqueued = 1
    target.job_notification = received
    target.worker_pings = {}
    try:
        target.drain_all()
        assert emitted == [{"pos": 1}] and sink.checkpoints == 1
        assert target._jobs_enqueued == 1
        assert ("completion mismatch" in caplog.text) is process_mode
    finally:
        received.close()
        sent.close()


@pytest.mark.parametrize("consume_early", [False, True])
def test_multiple_stream_completions_and_consecutive_checkpoints(monkeypatch, consume_early):
    from singer_sdk.target_base import Target

    from target_bigquery.tests.test_overwrite_state import StagedSink

    received, sent = mp.Pipe(False)
    target, first, emitted = target_with_workers([])
    second = StagedSink(False)
    target._sinks_active["second"] = second
    target.par_typ = ParType.PROCESS
    target._jobs_enqueued = 2
    target.job_notification = received
    target.worker_pings = {}
    target.resize_worker_pool = lambda: None
    target.log_notification = type("NoLogs", (), {"poll": lambda self: False})()
    monkeypatch.setattr(Target, "drain_one", lambda *args: None)
    try:
        sent.send(True)
        if consume_early:
            target.drain_one(first)
            assert target._jobs_enqueued == 1
        sent.send(True)
        target.drain_all()
        assert target._jobs_enqueued == 0
        assert first.checkpoints == second.checkpoints == 1
        target.increment_jobs_enqueued()
        sent.send(True)
        target._latest_state = {"pos": 2}
        target.drain_all()
        assert target._jobs_enqueued == 0
        assert emitted == [{"pos": 1}, {"pos": 2}]
    finally:
        received.close()
        sent.close()
