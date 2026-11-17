"""Real SDK dispatch errors may follow remote acceptance of an initial append."""

from types import SimpleNamespace

import pytest
from google.cloud.bigquery_storage_v1 import types, writer

from target_bigquery import storage_write
from target_bigquery.tests.test_overwrite_state import StagedSink, make_target
from target_bigquery.tests.test_storage_deadlines import PARENT, make_worker
from target_bigquery.tests.test_workers import FakeNotifier, FakeStreamNotification, SequencedQueue


@pytest.mark.parametrize("default", [False, True])
@pytest.mark.parametrize("endofpipe", [False, True])
def test_accepted_initial_append_is_not_retried_after_sdk_open_error(
    monkeypatch, default, endofpipe
):
    accepted = []
    requests = []
    offsets = set()
    created = []
    streams = []

    class Rpc:
        def __init__(self, method, *, initial_request, **kwargs):
            self.request = initial_request
            self.is_active = False

        def add_done_callback(self, callback):
            pass

        def close(self):
            pass

    class Consumer:
        def __init__(self, rpc, callback):
            self.rpc = rpc
            self.is_active = False

        def start(self):
            request = self.rpc.request
            requests.append(request)
            identity = (request.write_stream, request.offset)
            if default or identity not in offsets:
                accepted.append(request)
                offsets.add(identity)
            # Server accepted the initial request, but the response/connection
            # disappeared before the SDK finished its synchronous open check.

        def stop(self):
            pass

    def create_stream(**kwargs):
        name = f"{PARENT}/streams/pending-{len(created)}"
        created.append(name)
        return types.WriteStream(name=name)

    client = SimpleNamespace(append_rows=lambda: None, create_write_stream=create_stream)
    monkeypatch.setattr(storage_write, "storage_client_factory", lambda _: client)
    monkeypatch.setattr(writer.bidi, "BidiRpc", Rpc)
    monkeypatch.setattr(writer.bidi, "BackgroundConsumer", Consumer)
    worker = make_worker()
    factory = storage_write.get_default_stream if default else storage_write.get_application_stream

    def components(*args):
        path, stream, dispatch = factory(*args)
        streams.append(path)
        # The old tenacity wrapper retried after SDK connection renewal. Model
        # that callback scheduling deterministically, without network or sleeps.
        if hasattr(dispatch, "retry"):
            dispatch.retry.sleep = lambda _: stream._renew_connection()
        return path, stream, dispatch

    worker.get_stream_components = components
    job = storage_write.Job(
        PARENT,
        types.AppendRowsRequest(),
        FakeNotifier(),
        types.ProtoRows(serialized_rows=[b"row"]),
        timeout=7,
    )
    worker.queue = SequencedQueue(job)
    error = None
    try:
        worker.run()
    except RuntimeError as exc:
        error = exc
    assert len(requests) == 1
    assert len(accepted) == 1
    assert error is not None
    assert worker.queue.requeued == []
    assert worker.job_notifier.messages == []
    assert len(worker.error_notifier.messages) == 1
    assert streams[0] in worker.error_notifier.messages[0][1]
    assert len(created) == (0 if default else 1)
    assert bytes(accepted[0].proto_rows.rows.serialized_rows[0]) == b"row"
    staged = StagedSink(False)
    target, emitted = make_target({"orders": staged})
    target.error_notification = FakeStreamNotification(*worker.error_notifier.messages)
    with pytest.raises(RuntimeError, match="Remote completion may be unknown"):
        target.drain_all(is_endofpipe=endofpipe)
    assert emitted == []
    assert staged.checkpoints == 0
    assert not staged.published


def test_setup_failure_before_dispatch_can_retry(monkeypatch):
    worker = make_worker()
    setup = []
    sent = []
    closed = []
    future = writer.AppendRowsFuture(None)
    future.set_result(types.AppendRowsResponse(append_result={"offset": 0}))

    def components(*args):
        setup.append(True)
        if len(setup) == 1:
            raise ConnectionError("stream creation unavailable")
        stream = SimpleNamespace(_closed=False, close=lambda: closed.append(True))
        return f"{PARENT}/streams/pending-a", stream, lambda request: sent.append(request) or future

    monkeypatch.setattr(storage_write, "storage_client_factory", lambda _: None)
    worker.get_stream_components = components
    job = storage_write.Job(PARENT, types.AppendRowsRequest(), FakeNotifier(), types.ProtoRows())
    worker.queue = SequencedQueue(job)
    worker.run()
    assert len(setup) == 2
    assert len(sent) == 1
    assert worker.queue.requeued == [job]
    assert worker.job_notifier.messages == [True]
    assert worker.error_notifier.messages == []
    assert closed == [True]
