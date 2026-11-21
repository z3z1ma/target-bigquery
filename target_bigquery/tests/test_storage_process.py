"""Storage Write IPC carries identities, never live SDK clients or locks."""

import multiprocessing as mp
import pickle
from queue import Empty
from types import SimpleNamespace

import pytest
from google.cloud.bigquery_storage_v1 import types, writer

from target_bigquery import batch_job, gcs_stage, storage_write, streaming_insert
from target_bigquery.tests.test_overwrite_state import StagedSink, make_target
from target_bigquery.tests.test_storage_deadlines import PARENT, make_worker
from target_bigquery.tests.test_worker_exit import target_with_workers
from target_bigquery.tests.test_workers import FakeNotifier, FakeStreamNotification


class IdleQueue:
    def get(self, timeout):
        raise Empty


def _run_offline_worker():
    # Runs in the spawned interpreter; a sentinel-only queue needs no API call.
    storage_write.storage_client_factory = lambda _: None
    batch_job.bigquery_client_factory = lambda _: None
    gcs_stage.gcs_client_factory = lambda _: None
    streaming_insert.bigquery_client_factory = lambda _: None
    worker = mp.current_process()
    type(worker).run(worker)


@pytest.mark.parametrize("idle", [False, True])
@pytest.mark.parametrize("method", ["storage_default", "storage_pending", "batch", "gcs", "insert"])
def test_standard_process_worker_starts_with_spawn(method, idle):
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    queue = mp.Queue()
    worker = None
    try:
        sink_cls = {
            "storage_default": storage_write.BigQueryStorageWriteSink,
            "storage_pending": storage_write.BigQueryStorageWriteSink,
            "batch": batch_job.BigQueryBatchJobSink,
            "gcs": gcs_stage.BigQueryGcsStagingSink,
            "insert": streaming_insert.BigQueryStreamingInsertSink,
        }[method]
        cls = sink_cls.worker_cls_factory(
            mp.Process, {"options": {"storage_write_batch_mode": method == "storage_pending"}}
        )
        assert pickle.loads(pickle.dumps(cls)) is cls
        worker = cls(
            ext_id="spawn-test",
            queue=IdleQueue() if idle else queue,
            credentials=None,
            job_notifier=FakeNotifier(),
            error_notifier=FakeNotifier(),
            log_notifier=FakeNotifier(),
        )
        worker.run = _run_offline_worker
        if not idle:
            queue.put(None)
        worker.start()
        worker.join(timeout=20)
        assert not worker.is_alive()
        assert worker.exitcode == 0
        assert worker.normal_completion.is_set()
        target, sink, emitted = target_with_workers([worker])
        target.drain_all(is_endofpipe=idle)
        assert emitted == [{"pos": 1}]
        assert sink.published if idle else sink.checkpoints == 1
    finally:
        if worker is not None and worker.is_alive():
            worker.terminate()
            worker.join(timeout=5)
        queue.close()
        queue.join_thread()
        mp.set_start_method(previous, force=True)


@pytest.mark.parametrize("batch", [False, True])
def test_real_sdk_stream_notification_crosses_process_pipe(batch):
    receiver, sender = mp.Pipe(duplex=False)
    expected = f"{PARENT}/streams/" + ("pending-a" if batch else "_default")
    client = SimpleNamespace(
        append_rows=None,
        create_write_stream=lambda **kwargs: types.WriteStream(name=expected),
    )
    job = storage_write.Job(PARENT, types.AppendRowsRequest(), sender, types.ProtoRows())
    try:
        factory = (
            storage_write.get_application_stream if batch else storage_write.get_default_stream
        )
        name, stream, send = factory(client, job)
        assert isinstance(stream, writer.AppendRowsStream)
        assert receiver.poll(1)
        assert receiver.recv() == expected == name
        assert send.__self__ is stream
    finally:
        receiver.close()
        sender.close()


@pytest.mark.parametrize("endofpipe", [False, True])
def test_worker_close_error_prevents_coordinator_hooks_and_state(endofpipe):
    worker = make_worker()

    def close():
        raise RuntimeError("worker stream close failed")

    worker.cache = {PARENT: (f"{PARENT}/streams/pending-a", SimpleNamespace(close=close), None)}
    worker._finish()
    sink = StagedSink(False)
    target, emitted = make_target({"orders": sink})
    target.error_notification = FakeStreamNotification(*worker.error_notifier.messages)
    with pytest.raises(RuntimeError, match="worker stream close failed"):
        target.drain_all(is_endofpipe=endofpipe)
    assert emitted == []
    assert not sink.published
    assert sink.checkpoints == 0
