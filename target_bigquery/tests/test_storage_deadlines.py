"""Completion timeouts must return control without advancing durable state."""

import logging
import pickle
from concurrent import futures
from types import SimpleNamespace

import pytest
from google.cloud.bigquery_storage_v1 import types, writer

from target_bigquery import storage_write
from target_bigquery.tests.test_overwrite_state import StagedSink, make_target
from target_bigquery.tests.test_workers import FakeNotifier, FakeStreamNotification, SequencedQueue

PARENT = "projects/p/datasets/d/tables/t"


def make_sink(timeout=7):
    sink = object.__new__(storage_write.BigQueryStorageWriteSink)
    sink._config = {"timeout": timeout}
    sink.parent = PARENT
    sink.template = types.AppendRowsRequest()
    sink.proto_rows = types.ProtoRows(serialized_rows=[b"row"])
    sink.stream_notifier = None
    sink.global_queue = SimpleNamespace(qsize=lambda: 0, put=lambda job: jobs.append(job))
    jobs = []
    sink.increment_jobs_enqueued = lambda: None
    return sink, jobs


def make_worker():
    worker = storage_write.StorageWriteBatchWorker(
        ext_id="deadline-test",
        queue=SequencedQueue(),
        credentials=None,
        job_notifier=FakeNotifier(),
        error_notifier=FakeNotifier(),
        log_notifier=FakeNotifier(),
    )
    return worker


@pytest.mark.parametrize("default", [False, True])
@pytest.mark.parametrize("endofpipe", [False, True])
def test_append_timeout_keeps_stream_identity_and_blocks_state(default, endofpipe):
    sink, jobs = make_sink(7)
    sink.process_batch({})
    job = pickle.loads(pickle.dumps(jobs[0]))
    assert job.timeout == 7
    stream = f"{PARENT}/streams/" + ("_default" if default else "pending-a")
    waits = []
    requests = []

    def result(*, timeout=None):
        waits.append(timeout)
        raise TimeoutError("completion unknown")

    def dispatch(request):
        requests.append(request)
        return SimpleNamespace(result=result)

    worker = make_worker()
    worker.get_stream_components = lambda *args: (stream, SimpleNamespace(_closed=False), dispatch)
    worker._send_job(None, job)
    worker.wait(drain=True)
    assert waits == [7]
    assert len(requests) == 1
    assert worker.queue.requeued == []
    assert len(worker.error_notifier.messages) == 1
    assert stream in worker.error_notifier.messages[0][1]
    staged = StagedSink(False)
    target, emitted = make_target({"orders": staged})
    target.error_notification = FakeStreamNotification(*worker.error_notifier.messages)
    with pytest.raises(RuntimeError, match="completion unknown"):
        target.drain_all(is_endofpipe=endofpipe)
    assert emitted == []
    assert not staged.published
    assert staged.checkpoints == 0


def test_locked_sdk_unfinished_append_future_times_out_without_cancelling():
    worker = make_worker()
    future = writer.AppendRowsFuture(SimpleNamespace(close=lambda: pytest.fail("cancelled")))
    worker.awaiting = [storage_write.PendingAppend(future, "stream-a", 0.001, None, 1)]
    worker.wait(drain=True)
    assert isinstance(worker.error_notifier.messages[0][0], futures.TimeoutError)
    assert not future.cancelled()
    assert not future.done()


@pytest.mark.parametrize("timeout", [None, True, 0, -1, float("inf"), float("nan"), "7"])
def test_invalid_timeout_rejects_job_before_enqueue(timeout):
    sink, jobs = make_sink(timeout)
    with pytest.raises(ValueError, match="positive finite"):
        sink.process_batch({})
    assert jobs == []


@pytest.mark.parametrize("failure", [None, "finalize", "commit"])
@pytest.mark.parametrize("endofpipe", [False, True])
def test_pending_commit_deadlines_preserve_recovery_streams(monkeypatch, failure, endofpipe):
    sink, _ = make_sink(13)
    stream_name = f"{PARENT}/streams/pending-a"
    # Stream objects in production are hashable.
    stream = type("Stream", (), {"close": lambda self: None})()
    sink.open_streams = {(stream_name, stream)}
    sink.stream_notification = FakeStreamNotification()
    sink._credentials = None
    sink.logger = logging.getLogger(__name__)
    calls = []

    def finalize(*, name, timeout=None, retry="default"):
        calls.append(("finalize", timeout, retry))
        assert name == stream_name
        if failure == "finalize":
            raise TimeoutError("finalize uncertain")

    def commit(request, *, timeout=None, retry="default"):
        calls.append(("commit", timeout, retry))
        assert list(request.write_streams) == [stream_name]
        if failure == "commit":
            raise TimeoutError("commit uncertain")
        return types.BatchCommitWriteStreamsResponse()

    monkeypatch.setattr(
        storage_write,
        "storage_client_factory",
        lambda _: SimpleNamespace(
            finalize_write_stream=finalize,
            batch_commit_write_streams=commit,
        ),
    )
    if failure:
        target, emitted = make_target({"orders": sink})
        with pytest.raises(TimeoutError, match="uncertain"):
            target.drain_all(is_endofpipe=endofpipe)
        assert emitted == []
        assert sink.open_streams == {(stream_name, stream)}
    else:
        sink.commit_streams()
        assert sink.open_streams == set()
    assert calls == (
        [("finalize", 13, None)]
        if failure == "finalize"
        else [
            ("finalize", 13, None),
            ("commit", 13, None),
        ]
    )


@pytest.mark.parametrize("rejected", [False, True])
def test_completed_append_response_uses_client_future(rejected):
    worker = make_worker()
    future = writer.AppendRowsFuture(None)
    future.set_result(
        types.AppendRowsResponse(
            row_errors=[types.RowError(index=0, message="invalid row")] if rejected else [],
        )
    )
    worker.awaiting = [storage_write.PendingAppend(future, "stream-a", 5, None, 1)]
    worker.wait(drain=True)
    assert worker.job_notifier.messages == [True]
    assert bool(worker.error_notifier.messages) is rejected


def test_default_stream_is_not_finalized_or_committed(monkeypatch):
    sink, _ = make_sink()
    sink.open_streams = {(f"{PARENT}/streams/_default", object())}
    sink.stream_notification = FakeStreamNotification()
    monkeypatch.setattr(
        storage_write, "storage_client_factory", lambda _: pytest.fail("commit called")
    )
    sink.commit_streams()
    assert sink.open_streams == set()
