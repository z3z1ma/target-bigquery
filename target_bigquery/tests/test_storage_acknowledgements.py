"""Verify acknowledgement identity before trusting pending-stream completion."""

import logging
from types import SimpleNamespace

import pytest
from google.cloud.bigquery_storage_v1 import types, writer

from target_bigquery import storage_write
from target_bigquery.tests.test_overwrite_state import StagedSink, make_target
from target_bigquery.tests.test_storage_deadlines import PARENT, make_worker
from target_bigquery.tests.test_workers import FakeNotifier, FakeStreamNotification

STREAM = f"{PARENT}/streams/pending-a"


def dispatch(worker, responses, *, default=False):
    path = f"{PARENT}/streams/_default" if default else STREAM
    requests = []

    def send(request):
        requests.append(request)
        future = writer.AppendRowsFuture(None)
        future.set_result(responses[len(requests) - 1])
        return future

    worker.get_stream_components = lambda *args: (path, SimpleNamespace(_closed=False), send)
    for _ in responses:
        job = storage_write.Job(
            PARENT,
            types.AppendRowsRequest(),
            FakeNotifier(),
            types.ProtoRows(serialized_rows=[b"secret-row", b"another-row"]),
        )
        actual = worker._send_job(None, job)
        worker._record_dispatch_success(job, actual)
    return requests


def test_pending_acknowledgements_use_captured_offsets_after_counter_advances(caplog):
    caplog.set_level(logging.INFO)
    worker = make_worker()
    requests = dispatch(
        worker,
        [
            types.AppendRowsResponse(append_result={"offset": 0}, write_stream=STREAM),
            types.AppendRowsResponse(append_result={"offset": 2}, write_stream=STREAM),
        ],
    )
    assert [request.offset for request in requests] == [0, 2]
    assert worker.offsets[PARENT] == 4
    assert [(p.offset, p.row_count) for p in worker.awaiting] == [(0, 2), (2, 2)]
    worker.wait(drain=True)
    assert worker.error_notifier.messages == []
    assert worker.job_notifier.messages == [True, True]
    assert f"stream={STREAM}, offset=0, row_count=2" in caplog.text
    assert "secret-row" not in caplog.text


@pytest.mark.parametrize(
    "response",
    [
        None,
        types.AppendRowsResponse(),
        types.AppendRowsResponse(append_result={}),
        types.AppendRowsResponse(append_result={"offset": 9}),
        types.AppendRowsResponse(
            append_result={"offset": 0}, write_stream=f"{PARENT}/streams/wrong"
        ),
    ],
)
@pytest.mark.parametrize("endofpipe", [False, True])
def test_invalid_pending_acknowledgement_blocks_state_and_publication(response, endofpipe):
    worker = make_worker()
    dispatch(worker, [response])
    worker.wait(drain=True)
    assert len(worker.error_notifier.messages) == 1
    message = worker.error_notifier.messages[0][1]
    assert f"stream={STREAM}, offset=0, row_count=2" in message
    assert "mismatch" in message
    assert "secret-row" not in message
    staged = StagedSink(False)
    target, emitted = make_target({"orders": staged})
    target.error_notification = FakeStreamNotification(*worker.error_notifier.messages)
    with pytest.raises(RuntimeError, match="mismatch"):
        target.drain_all(is_endofpipe=endofpipe)
    assert emitted == []
    assert staged.checkpoints == 0
    assert not staged.published


def test_default_stream_acknowledgement_does_not_require_offset():
    worker = make_worker()
    requests = dispatch(worker, [types.AppendRowsResponse(append_result={})], default=True)
    assert requests[0].offset is None
    worker.wait(drain=True)
    assert worker.error_notifier.messages == []
    assert worker.job_notifier.messages == [True]


@pytest.mark.parametrize("code", [6, 11])
def test_offset_errors_are_not_treated_as_replay_success(code):
    worker = make_worker()
    dispatch(worker, [types.AppendRowsResponse(error={"code": code, "message": "offset rejected"})])
    worker.wait(drain=True)
    assert len(worker.error_notifier.messages) == 1
    assert "offset rejected" in worker.error_notifier.messages[0][1]
    assert "mismatch" not in worker.error_notifier.messages[0][1]


def test_dispatch_failure_logs_identity_before_send(caplog):
    caplog.set_level(logging.INFO)
    worker = make_worker()

    logged_before_send = []

    def send(request):
        logged_before_send.append(f"stream={STREAM}, offset=0, row_count=1" in caplog.text)
        raise ConnectionError("response lost")

    worker.get_stream_components = lambda *args: (STREAM, SimpleNamespace(_closed=False), send)
    job = storage_write.Job(
        PARENT,
        types.AppendRowsRequest(),
        FakeNotifier(),
        types.ProtoRows(serialized_rows=[b"secret-row"]),
    )
    with pytest.raises(storage_write.AppendDispatchError, match="offset=0, row_count=1"):
        worker._send_job(None, job)
    assert logged_before_send == [True]
    assert "secret-row" not in caplog.text
