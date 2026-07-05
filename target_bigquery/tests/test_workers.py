"""Tests for worker lifecycle helpers that do not require BigQuery."""

from multiprocessing import Process
from queue import Empty
from types import SimpleNamespace
from typing import cast

import pytest
from google.cloud.bigquery_storage_v1 import exceptions, types

import target_bigquery.storage_write as storage_write
from target_bigquery.target import TargetBigQuery


class FakeQueue:
    def __init__(self) -> None:
        self.items: list[object] = []

    def put(self, item: object) -> None:
        self.items.append(item)


class FakeWorker:
    def __init__(self, *, alive: bool) -> None:
        self.alive = alive
        self.joins = 0

    def is_alive(self) -> bool:
        return self.alive

    def join(self) -> None:
        self.joins += 1


class EmptyQueue:
    def get(self, timeout: float) -> object:
        raise Empty


class FakeNotifier:
    def __init__(self) -> None:
        self.messages: list[tuple[Exception, str] | bool | str] = []

    def send(self, message: tuple[Exception, str] | bool | str) -> None:
        self.messages.append(message)


class FakeStreamNotification:
    def __init__(self, *messages: object) -> None:
        self.messages = list(messages)

    def poll(self) -> bool:
        return bool(self.messages)

    def recv(self) -> object:
        return self.messages.pop(0)


class AlreadyClosedStream:
    def close(self) -> None:
        raise exceptions.StreamClosedError("already closed")


class FakeStorageCommitter:
    def __init__(self) -> None:
        self.finalized: list[str] = []
        self.committed: list[list[str]] = []

    def finalize_write_stream(self, name: str) -> None:
        self.finalized.append(name)

    def batch_commit_write_streams(
        self,
        request: types.BatchCommitWriteStreamsRequest,
    ) -> SimpleNamespace:
        self.committed.append(list(request.write_streams))
        return SimpleNamespace(commit_time="now", stream_errors=[])


def first_error_message(notifier: FakeNotifier) -> tuple[Exception, str]:
    return cast(tuple[Exception, str], notifier.messages[0])


class FakeFuture:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc
        self.results = 0

    def result(self) -> None:
        self.results += 1
        if self.exc is not None:
            raise self.exc


class FakeStream:
    def __init__(self, exc: Exception | None = None) -> None:
        self.exc = exc
        self.closed = False

    def close(self) -> None:
        self.closed = True
        if self.exc is not None:
            raise self.exc


def test_shutdown_workers_joins_each_worker_once():
    target = object.__new__(TargetBigQuery)
    target.queue = FakeQueue()
    workers = [
        FakeWorker(alive=False),
        FakeWorker(alive=True),
        FakeWorker(alive=True),
    ]
    target.workers = workers.copy()

    target._shutdown_workers()

    assert target.queue.items == [None, None]
    assert target.workers == []
    assert [worker.joins for worker in workers] == [1, 1, 1]


def test_receive_job_returns_none_when_queue_is_empty():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    worker.queue = EmptyQueue()

    assert worker._receive_job() is None


def test_generate_request_for_default_stream_uses_stream_path():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    payload = types.ProtoRows(serialized_rows=[b"a", b"b"])
    job = SimpleNamespace(parent="parent", data=payload)

    request = worker._generate_request_for_job(job, "projects/p/datasets/d/tables/t/_default")

    assert request.write_stream == "projects/p/datasets/d/tables/t/_default"
    assert request.proto_rows.rows == payload


def test_generate_request_for_application_stream_uses_cached_offset():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    worker.offsets = {"parent": 7}
    payload = types.ProtoRows(serialized_rows=[b"a"])
    job = SimpleNamespace(parent="parent", data=payload)

    request = worker._generate_request_for_job(job, "projects/p/datasets/d/tables/t/streams/s")

    assert request.offset == 7
    assert request.proto_rows.rows == payload


def test_get_stream_components_for_job_refreshes_missing_or_closed_streams():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    worker.cache = {}
    worker.offsets = {}
    job = SimpleNamespace(parent="parent")
    stream = SimpleNamespace(_closed=False)
    components = ("stream-a", stream, lambda request: request)
    worker.get_stream_components = lambda client, job: components

    assert worker._get_stream_components_for_job(object(), job) == components
    assert worker.offsets == {"parent": 0}

    replacement = ("stream-b", SimpleNamespace(_closed=False), lambda request: request)
    worker.cache["parent"] = ("stream-a", SimpleNamespace(_closed=True), lambda request: request)
    worker.get_stream_components = lambda client, job: replacement

    assert worker._get_stream_components_for_job(object(), job) == replacement
    assert worker.cache["parent"] == replacement


def test_storage_write_worker_factory_selects_stream_or_batch_worker():
    stream_worker_cls = storage_write.BigQueryStorageWriteSink.worker_cls_factory(
        Process,
        {"options": {"storage_write_batch_mode": False}},
    )
    batch_worker_cls = storage_write.BigQueryStorageWriteSink.worker_cls_factory(
        Process,
        {"options": {"storage_write_batch_mode": True}},
    )

    assert issubclass(stream_worker_cls, storage_write.StorageWriteStreamWorker)
    assert issubclass(batch_worker_cls, storage_write.StorageWriteBatchWorker)


def test_handle_dispatch_error_requeues_retryable_job():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    worker.queue = FakeQueue()
    worker.error_notifier = FakeNotifier()
    worker.logger = SimpleNamespace(info=lambda message: None)
    worker.max_errors_before_recycle = 5
    worker.ext_id = "worker-a"
    job = SimpleNamespace(attempts=1)

    worker._handle_dispatch_error(job, RuntimeError("transient"))

    assert job.attempts == 2
    assert worker.queue.items == [job]
    assert worker.error_notifier.messages == []
    assert worker.max_errors_before_recycle == 4


def test_handle_dispatch_error_sends_terminal_error_after_three_attempts():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    worker.queue = FakeQueue()
    worker.error_notifier = FakeNotifier()
    worker.logger = SimpleNamespace(info=lambda message: None)
    worker.max_errors_before_recycle = 5
    worker.ext_id = "worker-a"
    exc = RuntimeError("terminal")
    job = SimpleNamespace(attempts=3)

    worker._handle_dispatch_error(job, exc)

    assert worker.queue.items == []
    error, message = first_error_message(worker.error_notifier)
    assert error is exc
    assert "Worker ID: worker-a" in message


def test_handle_dispatch_error_recycles_streams_when_error_threshold_is_hit():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    worker.queue = FakeQueue()
    worker.error_notifier = FakeNotifier()
    worker.logger = SimpleNamespace(info=lambda message: None)
    worker.max_errors_before_recycle = 1
    waited: list[bool] = []
    closed: list[bool] = []
    worker.wait = lambda drain=False: waited.append(drain)
    worker.close_cached_streams = lambda: closed.append(True)
    job = SimpleNamespace(attempts=1)
    exc = RuntimeError("recycle")

    with pytest.raises(RuntimeError, match="recycle"):
        worker._handle_dispatch_error(job, exc)

    assert worker.queue.items == [job]
    assert waited == [True]
    assert closed == [True]


def test_record_dispatch_success_advances_offsets_and_applies_backpressure():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    worker.ext_id = "worker-a"
    worker.log_notifier = FakeNotifier()
    worker.offsets = {"parent": 7}
    waited: list[bool] = []
    worker.wait = lambda drain=False: waited.append(drain)
    worker.awaiting = [object()] * (storage_write.MAX_IN_FLIGHT + 1)
    job = SimpleNamespace(parent="parent", data=SimpleNamespace(serialized_rows=[b"a", b"b"]))

    worker._record_dispatch_success(job, "stream-a")

    assert worker.offsets == {"parent": 9}
    assert worker.log_notifier.messages == ["[worker-a] Sent 2 rows to stream-a with offset 7."]
    assert waited == [False]


def test_wait_reports_success_and_serializes_future_errors():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    exc = RuntimeError("append failed")
    worker.awaiting = [FakeFuture(), FakeFuture(exc)]
    worker.error_notifier = FakeNotifier()
    worker.job_notifier = FakeNotifier()
    worker.ext_id = "worker-a"

    worker.wait(drain=True)

    assert worker.awaiting == []
    assert worker.job_notifier.messages == [True, True]
    error, message = first_error_message(worker.error_notifier)
    assert error is exc
    assert "Worker ID: worker-a" in message


def test_close_cached_streams_reports_close_errors():
    worker = object.__new__(storage_write.StorageWriteBatchWorker)
    exc = RuntimeError("close failed")
    good_stream = FakeStream()
    bad_stream = FakeStream(exc)
    worker.cache = {
        "good": ("good-stream", good_stream, lambda request: request),
        "bad": ("bad-stream", bad_stream, lambda request: request),
    }
    worker.error_notifier = FakeNotifier()
    worker.ext_id = "worker-a"

    worker.close_cached_streams()

    assert good_stream.closed is True
    assert bad_stream.closed is True
    error, message = first_error_message(worker.error_notifier)
    assert error is exc
    assert "Worker ID: worker-a" in message


def test_storage_write_commit_streams_tolerates_already_closed_stream(monkeypatch):
    sink = object.__new__(storage_write.BigQueryStorageWriteSink)
    stream_name = "projects/p/datasets/d/tables/t/streams/s"
    sink.open_streams = set()
    sink.stream_notification = FakeStreamNotification((stream_name, AlreadyClosedStream()))
    sink._credentials = object()
    sink.parent = "projects/p/datasets/d/tables/t"
    sink.logger = SimpleNamespace(
        debug=lambda *args: None,
        info=lambda *args: None,
    )
    committer = FakeStorageCommitter()
    monkeypatch.setattr(storage_write, "storage_client_factory", lambda credentials: committer)

    sink.commit_streams()

    assert committer.finalized == [stream_name]
    assert committer.committed == [[stream_name]]
    assert sink.open_streams == set()
