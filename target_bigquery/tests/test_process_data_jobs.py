"""Spawned production workers must drain data from the configured process queue."""

import io
import multiprocessing as mp
from types import SimpleNamespace

import pytest
from google.cloud.bigquery_storage_v1 import types, writer

from target_bigquery import batch_job, gcs_stage, storage_write, streaming_insert
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_worker_exit import target_with_workers
from target_bigquery.tests.test_workers import FakeNotifier


def run_offline_data_worker():
    worker = mp.current_process()

    def accepted(value):
        worker.observed.send(str(value))
        if worker.fail_transport:
            raise RuntimeError("offline API failure")

    def load(data, table, **kwargs):
        accepted(table)
        return SimpleNamespace(result=lambda **kwargs: None)

    def insert(**kwargs):
        accepted(kwargs["table"])
        return []

    def blob(path, **kwargs):
        accepted(path)
        return SimpleNamespace(open=lambda *args, **kwargs: io.BytesIO())

    def components(client, job):
        accepted(job.parent)
        future = writer.AppendRowsFuture(None)
        future.set_result(types.AppendRowsResponse(append_result={"offset": 0}))
        suffix = (
            "_default" if isinstance(worker, storage_write.StorageWriteStreamWorker) else "pending"
        )
        return (
            job.parent + "/streams/" + suffix,
            SimpleNamespace(close=lambda: None),
            lambda request: future,
        )

    batch_job.bigquery_client_factory = lambda _: SimpleNamespace(load_table_from_file=load)
    streaming_insert.bigquery_client_factory = lambda _: SimpleNamespace(insert_rows_json=insert)
    gcs_stage.gcs_client_factory = lambda _: None
    gcs_stage.storage.Blob.from_string = blob
    storage_write.storage_client_factory = lambda _: None
    if isinstance(worker, storage_write.StorageWriteBatchWorker):
        worker.get_stream_components = components
    type(worker).run(worker)


@pytest.mark.parametrize("fail_transport", [False, True])
@pytest.mark.parametrize("method", ["batch", "gcs", "insert", "storage_pending", "storage_default"])
@pytest.mark.parametrize("eof", [False, True])
def test_spawned_data_jobs_complete_before_parent_publication(method, eof, fail_transport):
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    owner = object.__new__(TargetBigQuery)
    owner._config = {"options": {"process_pool": True}}
    executor, pipe, queue_factory, _ = owner.get_parallelization_components()
    queue = queue_factory()
    received, sent = pipe(False)
    completed, done = pipe(False)
    error_received, error_sent = pipe(False)
    cls = {
        "batch": batch_job.BigQueryBatchJobSink,
        "gcs": gcs_stage.BigQueryGcsStagingSink,
        "insert": streaming_insert.BigQueryStreamingInsertSink,
        "storage_pending": storage_write.BigQueryStorageWriteSink,
        "storage_default": storage_write.BigQueryStorageWriteSink,
    }[method].worker_cls_factory(
        executor, {"options": {"storage_write_batch_mode": method == "storage_pending"}}
    )
    worker = cls(
        ext_id="process-data",
        queue=queue,
        credentials=None,
        job_notifier=done,
        error_notifier=error_sent,
        log_notifier=FakeNotifier(),
    )
    worker.fail_transport = fail_transport
    worker.observed = sent
    worker.run = run_offline_data_worker
    for table in ["first", "second"]:
        if method == "batch":
            job = batch_job.Job(b"{}", f"p.d.{table}", {})
        elif method == "insert":
            job = streaming_insert.Job(f"p.d.{table}", [{}])
        elif method == "gcs":
            job = gcs_stage.Job(b"{}", "batch", table, "d", "bucket", FakeNotifier())
        else:
            job = storage_write.Job(
                f"projects/p/datasets/d/tables/{table}",
                types.AppendRowsRequest(),
                FakeNotifier(),
                types.ProtoRows(),
            )
        queue.put(job)
    queue.put(None)
    worker.start()
    try:
        worker.join(20)
        assert not worker.is_alive()
        target, sink, emitted = target_with_workers([worker])
        target.queue = queue
        target.error_notification = error_received
        if fail_transport:
            with pytest.raises(RuntimeError, match="offline API failure"):
                target.drain_all(is_endofpipe=eof)
            assert emitted == []
            assert not sink.published
            assert sink.checkpoints == 0
            return
        target.drain_all(is_endofpipe=eof)
        assert worker.exitcode == 0
        assert worker.normal_completion.is_set()
        assert received.poll(1)
        assert "first" in received.recv()
        assert received.poll(1)
        assert "second" in received.recv()
        assert completed.poll(1) and completed.recv() is True
        assert completed.poll(1) and completed.recv() is True
        assert not error_received.poll()
        assert emitted == [{"pos": 1}]
        assert sink.published if eof else sink.checkpoints == 1
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(5)
        worker.close()
        for conn in [received, sent, completed, done, error_received, error_sent]:
            conn.close()
        queue.close()
        queue.join_thread()
        mp.set_start_method(previous, force=True)


def test_existing_task_accounting_error_is_not_suppressed():
    from target_bigquery.tests.test_storage_deadlines import make_worker

    worker = make_worker()

    def invalid_accounting():
        raise ValueError("too many task_done calls")

    worker.queue = SimpleNamespace(task_done=invalid_accounting)
    with pytest.raises(ValueError, match="too many task_done"):
        worker.mark_job_done()
