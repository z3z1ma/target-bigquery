"""A zero exit code is not evidence that a worker completed its loop."""

import multiprocessing as mp
import os
from threading import Event

import pytest

from target_bigquery.batch_job import BatchJobProcessWorker, BatchJobThreadWorker
from target_bigquery.tests.test_worker_exit import ExitedWorker, target_with_workers
from target_bigquery.tests.test_workers import FakeNotifier


def exit_cleanly_before_receiving_work():
    os._exit(0)


@pytest.mark.parametrize("eof", [False, True])
@pytest.mark.parametrize("overwrite", [False, True])
def test_clean_early_process_exit_with_queued_work_blocks_state(eof, overwrite):
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    queue = mp.Queue()
    worker = BatchJobProcessWorker(
        ext_id="early-exit",
        queue=queue,
        credentials=None,
        job_notifier=FakeNotifier(),
        error_notifier=FakeNotifier(),
        log_notifier=FakeNotifier(),
    )
    worker.run = exit_cleanly_before_receiving_work
    queue.put("unprocessed batch")
    queue.put(None)
    worker.start()
    try:
        worker.join(20)
        assert not worker.is_alive()
        assert worker.exitcode == 0
        target, sink, emitted = target_with_workers([worker])
        target.queue = queue
        sink.overwrite_target = object() if overwrite else None
        with pytest.raises(RuntimeError, match="normal completion"):
            target.drain_all(is_endofpipe=eof)
        assert emitted == []
        assert not sink.published
        assert sink.checkpoints == 0
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(5)
        assert queue.get(timeout=5) == "unprocessed batch"
        assert queue.get(timeout=5) is None
        worker.close()
        queue.close()
        queue.join_thread()
        mp.set_start_method(previous, force=True)


def test_early_thread_return_blocks_checkpoint():
    worker = BatchJobThreadWorker(
        ext_id="early-thread",
        queue=None,
        credentials=None,
        job_notifier=FakeNotifier(),
        error_notifier=FakeNotifier(),
        log_notifier=FakeNotifier(),
    )
    worker.run = lambda: None
    worker.start()
    worker.join(5)
    target, sink, emitted = target_with_workers([worker])
    with pytest.raises(RuntimeError, match="normal completion"):
        target.drain_all()
    assert emitted == []
    assert sink.checkpoints == 0


def test_culling_incomplete_worker_stops_before_replacement():
    worker = ExitedWorker(0)
    worker.normal_completion = Event()
    target, _, emitted = target_with_workers([worker])
    replacements = []
    target.worker_factory = lambda: replacements.append(True)
    with pytest.raises(RuntimeError, match="normal completion"):
        target.resize_worker_pool()
    assert replacements == []
    assert target.workers == []
    assert emitted == []


def test_culling_completed_worker_keeps_pool_resize_behavior():
    worker = ExitedWorker(0)
    worker.normal_completion = Event()
    worker.normal_completion.set()
    target, _, _ = target_with_workers([worker])
    target._jobs_enqueued = 0
    replacement = ExitedWorker(0)
    replacement.start = lambda: None
    target.worker_factory = lambda: replacement
    target.resize_worker_pool()
    assert target.workers == [replacement]
    assert worker.joins == 1


def test_fail_fast_disabled_warns_on_missing_completion(caplog):
    worker = ExitedWorker(0)
    worker.normal_completion = Event()
    target, _, emitted = target_with_workers([worker], fail_fast=False)
    target.drain_all()
    assert emitted == [{"pos": 1}]
    assert "without normal completion" in caplog.text


@pytest.mark.parametrize("fail_finish", [False, True])
def test_storage_completion_is_marked_only_after_finish(monkeypatch, fail_finish):
    from target_bigquery import storage_write
    from target_bigquery.tests.test_storage_deadlines import make_worker

    worker = make_worker()
    monkeypatch.setattr(storage_write, "storage_client_factory", lambda _: None)

    def finish():
        assert not worker.normal_completion.is_set()
        if fail_finish:
            raise RuntimeError("finish failed")

    worker._finish = finish
    if fail_finish:
        with pytest.raises(RuntimeError, match="finish failed"):
            worker.run()
    else:
        worker.run()
    assert worker.normal_completion.is_set() is not fail_finish
