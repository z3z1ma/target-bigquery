"""EOF sentinels must not overtake retryable work already owned by a worker."""

import io
from queue import Queue
from threading import Event
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.bigquery_storage_v1 import types, writer

from target_bigquery import batch_job, gcs_stage, storage_write, streaming_insert
from target_bigquery.tests.test_worker_exit import target_with_workers
from target_bigquery.tests.test_workers import FakeNotifier, FakeStreamNotification


class ShutdownQueue(Queue):
    def __init__(self):
        super().__init__()
        self.sentinel_queued = Event()

    def put(self, item, *args, **kwargs):
        super().put(item, *args, **kwargs)
        if item is None:
            self.sentinel_queued.set()


def test_eof_does_not_leave_retry_behind_sentinel(monkeypatch):
    queue = ShutdownQueue()
    started = Event()
    calls = []

    def load(*args, **kwargs):
        calls.append(kwargs["job_id"])
        if len(calls) == 1:
            started.set()
            assert queue.sentinel_queued.wait(5)
            raise ConnectionError("retryable submission failure")
        return SimpleNamespace(result=lambda **kwargs: None)

    monkeypatch.setattr(
        batch_job, "bigquery_client_factory", lambda _: SimpleNamespace(load_table_from_file=load)
    )
    worker = batch_job.BatchJobThreadWorker(
        ext_id="retry-at-eof",
        queue=queue,
        credentials=None,
        job_notifier=FakeNotifier(),
        error_notifier=FakeNotifier(),
        log_notifier=FakeNotifier(),
    )
    job = batch_job.Job(b"{}", "p.d.t", {})
    queue.put(job)
    worker.start()
    try:
        assert started.wait(5)
        target, sink, emitted = target_with_workers([worker])
        target.queue = queue
        target.error_notification = FakeStreamNotification()
        target.drain_all(is_endofpipe=True)
        assert calls == [job.job_id, job.job_id]
        assert worker.job_notifier.messages == [True]
        assert sink.published
        assert emitted == [{"pos": 1}]
        assert worker.normal_completion.is_set()
    finally:
        queue.sentinel_queued.set()
        worker.join(5)
        assert not worker.is_alive()




@pytest.mark.parametrize("method", ["batch", "gcs", "insert", "storage"])
@pytest.mark.parametrize("exhausted", [False, True])
def test_eof_waits_for_local_retries_or_reports_failure(monkeypatch, method, exhausted):
    queue = ShutdownQueue()
    started = Event()
    calls = []
    identity = []

    def attempt(value):
        calls.append(True)
        identity.append(value)
        if len(calls) == 1:
            started.set()
            assert queue.sentinel_queued.wait(5)
        if exhausted or len(calls) == 1:
            raise NotFound("retryable setup failure")

    if method == "batch":

        def load(*args, **kwargs):
            attempt(kwargs["job_id"])
            return SimpleNamespace(result=lambda **kwargs: None)

        monkeypatch.setattr(
            batch_job,
            "bigquery_client_factory",
            lambda _: SimpleNamespace(load_table_from_file=load),
        )
        cls = batch_job.BatchJobThreadWorker
        job = batch_job.Job(b"{}", "p.d.t", {})
    elif method == "insert":

        def insert(**kwargs):
            attempt(kwargs["row_ids"])
            return []

        monkeypatch.setattr(
            streaming_insert,
            "bigquery_client_factory",
            lambda _: SimpleNamespace(insert_rows_json=insert),
        )
        monkeypatch.setattr(streaming_insert, "sleep", lambda _: None)
        cls = streaming_insert.StreamingInsertThreadWorker
        job = streaming_insert.Job("p.d.t", [{}])
    elif method == "gcs":

        def blob(path, **kwargs):
            attempt(path)
            return SimpleNamespace(open=lambda *args, **kwargs: io.BytesIO())

        monkeypatch.setattr(gcs_stage, "gcs_client_factory", lambda _: None)
        monkeypatch.setattr(gcs_stage.storage.Blob, "from_string", blob)
        cls = gcs_stage.GcsStagingThreadWorker
        job = gcs_stage.Job(b"{}", "batch", "t", "d", "bucket", FakeNotifier())
    else:
        monkeypatch.setattr(storage_write, "storage_client_factory", lambda _: None)
        cls = storage_write.StorageWriteThreadBatchWorker
        job = storage_write.Job(
            "projects/p/datasets/d/tables/t",
            types.AppendRowsRequest(),
            FakeNotifier(),
            types.ProtoRows(),
        )

    worker = cls(
        ext_id="retry",
        queue=queue,
        credentials=None,
        job_notifier=FakeNotifier(),
        error_notifier=FakeNotifier(),
        log_notifier=FakeNotifier(),
    )
    if method == "storage":

        def components(client, item):
            attempt(item.parent)
            future = writer.AppendRowsFuture(None)
            future.set_result(types.AppendRowsResponse(append_result={"offset": 0}))
            return (
                item.parent + "/streams/pending",
                SimpleNamespace(close=lambda: None),
                lambda request: future,
            )

        worker.get_stream_components = components
    original_run = worker.run
    failures = []

    def run():
        try:
            original_run()
        except Exception as exc:
            failures.append(exc)

    worker.run = run
    queue.put(job)
    worker.start()
    try:
        assert started.wait(5)
        target, sink, emitted = target_with_workers([worker])
        target.queue = queue
        target.error_notification = SimpleNamespace(
            poll=lambda: bool(worker.error_notifier.messages),
            recv=lambda: worker.error_notifier.messages.pop(0),
        )
        if exhausted:
            with pytest.raises(RuntimeError, match="retryable setup failure"):
                target.drain_all(is_endofpipe=True)
            assert emitted == []
            assert not sink.published
        else:
            target.drain_all(is_endofpipe=True)
            assert not failures
            assert emitted == [{"pos": 1}]
            assert sink.published
            assert worker.job_notifier.messages == [True]
        assert len(calls) == (3 if exhausted else 2)
        assert all(value == identity[0] for value in identity)
        # The sentinel is not a task; the sole data dequeue must be accounted once.
        assert queue.unfinished_tasks == 1
    finally:
        queue.sentinel_queued.set()
        worker.join(5)
        assert not worker.is_alive()
