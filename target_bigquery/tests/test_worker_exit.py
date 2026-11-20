"""Abnormal process exits must not silently advance Singer checkpoints."""

import multiprocessing as mp
import os
from types import SimpleNamespace

import pytest

from target_bigquery.tests.test_overwrite_state import StagedSink, make_target


def exit_without_notification():
    os._exit(17)


class ExitedWorker:
    ext_id = "failed-worker"

    def __init__(self, exitcode):
        self.exitcode = exitcode
        self.joins = 0

    def is_alive(self):
        return False

    def join(self):
        self.joins += 1


def target_with_workers(workers, *, fail_fast=True):
    sink = StagedSink(False)
    target, emitted = make_target({"orders": sink})
    del target._join_workers, target._shutdown_workers
    target.workers = workers
    target._config = {"fail_fast": fail_fast}
    target.queue = SimpleNamespace(put=lambda value: None)
    return target, sink, emitted


@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("exitcode", [17, -9])
@pytest.mark.parametrize("eof", [False, True])
def test_abnormal_exit_blocks_hooks_and_state(exitcode, eof, overwrite):
    failed, healthy = ExitedWorker(exitcode), ExitedWorker(0)
    target, sink, emitted = target_with_workers([healthy, failed])
    sink.overwrite_target = object() if overwrite else None
    with pytest.raises(RuntimeError, match=f"failed-worker exited with status {exitcode}"):
        target.drain_all(is_endofpipe=eof)
    assert emitted == []
    assert not sink.published
    assert sink.checkpoints == 0
    assert failed.joins == healthy.joins == 1


def test_real_spawned_failure_without_error_notification_blocks_state():
    worker = mp.get_context("spawn").Process(target=exit_without_notification)
    worker.ext_id = "crashed-process"
    worker.start()
    try:
        worker.join(20)
        assert not worker.is_alive()
        assert worker.exitcode == 17
        target, sink, emitted = target_with_workers([worker])
        with pytest.raises(RuntimeError, match="crashed-process exited with status 17"):
            target.drain_all()
        assert emitted == []
        assert sink.checkpoints == 0
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(5)
        worker.close()


def test_culling_failed_process_stops_before_replacement():
    failed, sibling = ExitedWorker(17), ExitedWorker(0)
    target, _, _ = target_with_workers([sibling, failed])
    replacements = []
    target.worker_factory = lambda: replacements.append(True)
    with pytest.raises(RuntimeError, match="status 17"):
        target.resize_worker_pool()
    assert replacements == []
    assert target.workers == []
    assert failed.joins == sibling.joins == 1


@pytest.mark.parametrize("eof", [False, True])
def test_fail_fast_disabled_warns_and_keeps_permissive_behavior(eof, caplog):
    target, sink, emitted = target_with_workers([ExitedWorker(17)], fail_fast=False)
    target.drain_all(is_endofpipe=eof)
    assert emitted == [{"pos": 1}]
    assert "queued data may be incomplete" in caplog.text
    assert sink.published if eof else sink.checkpoints == 1


@pytest.mark.parametrize("exitcode", [0, None, "absent"])
def test_clean_process_and_thread_workers_preserve_checkpoint(exitcode):
    worker = ExitedWorker(exitcode)
    if exitcode == "absent":
        del worker.exitcode
    target, sink, emitted = target_with_workers([worker])
    target.drain_all()
    assert emitted == [{"pos": 1}]
    assert sink.checkpoints == 1


def test_error_notification_remains_primary_when_shutdown_finds_failed_process():
    target, _, emitted = target_with_workers([ExitedWorker(17)])
    original = ValueError("specific ingestion failure")
    target.error_notification = SimpleNamespace(
        poll=lambda: True, recv=lambda: (original, "specific ingestion failure")
    )
    with pytest.raises(RuntimeError, match="specific ingestion failure") as exc:
        target._handle_worker_error()
    assert exc.value.__cause__ is original
    assert target.workers == []
    assert emitted == []
