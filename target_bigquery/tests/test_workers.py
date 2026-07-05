"""Tests for worker lifecycle helpers that do not require BigQuery."""

from queue import Empty
from types import SimpleNamespace

from google.cloud.bigquery_storage_v1 import types

from target_bigquery.storage_write import StorageWriteBatchWorker
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
    worker = object.__new__(StorageWriteBatchWorker)
    worker.queue = EmptyQueue()

    assert worker._receive_job() is None


def test_generate_request_for_default_stream_uses_stream_path():
    worker = object.__new__(StorageWriteBatchWorker)
    payload = types.ProtoRows(serialized_rows=[b"a", b"b"])
    job = SimpleNamespace(parent="parent", data=payload)

    request = worker._generate_request_for_job(job, "projects/p/datasets/d/tables/t/_default")

    assert request.write_stream == "projects/p/datasets/d/tables/t/_default"
    assert request.proto_rows.rows == payload


def test_generate_request_for_application_stream_uses_cached_offset():
    worker = object.__new__(StorageWriteBatchWorker)
    worker.offsets = {"parent": 7}
    payload = types.ProtoRows(serialized_rows=[b"a"])
    job = SimpleNamespace(parent="parent", data=payload)

    request = worker._generate_request_for_job(job, "projects/p/datasets/d/tables/t/streams/s")

    assert request.offset == 7
    assert request.proto_rows.rows == payload


def test_get_stream_components_for_job_refreshes_missing_or_closed_streams():
    worker = object.__new__(StorageWriteBatchWorker)
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
