"""Partial and ambiguous insertAll outcomes must not replay accepted rows."""

import pickle
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from target_bigquery import streaming_insert
from target_bigquery.tests.test_overwrite_state import StagedSink, make_target
from target_bigquery.tests.test_workers import FakeStreamNotification, make_worker

TABLE = bigquery.TableReference.from_string("p.d.t")


@pytest.mark.parametrize("outcome", ["partial", "row-not-found", "lost"])
@pytest.mark.parametrize("endofpipe", [False, True])
def test_real_client_partial_or_unknown_insert_does_not_replay(monkeypatch, outcome, endofpipe):
    client = bigquery.Client(project="p", credentials=AnonymousCredentials())
    calls = []
    retries = []
    accepted = []
    original = client._call_api

    def call_api(retry, **kwargs):
        retries.append(retry)
        # Keep the real request builder, but avoid old SDK backoff in the
        # pre-fix regression run. Retry eligibility is asserted separately.
        return original(None, **kwargs)

    def api_request(**kwargs):
        calls.append(kwargs)
        accepted.append(kwargs["data"]["rows"][0])
        if outcome == "lost":
            raise ServiceUnavailable("response lost after acceptance")
        return {
            "insertErrors": [
                {
                    "index": 1,
                    "errors": [
                        {"reason": "notFound" if outcome == "row-not-found" else "backendError"}
                    ],
                }
            ]
        }

    monkeypatch.setattr(client, "_call_api", call_api)
    monkeypatch.setattr(client._connection, "api_request", api_request)
    monkeypatch.setattr(streaming_insert, "bigquery_client_factory", lambda _: client)
    job = streaming_insert.Job(TABLE, [{"id": 1}, {"id": 2}])
    worker = make_worker(streaming_insert.StreamingInsertWorker, job)
    with pytest.raises((RuntimeError, ServiceUnavailable)):
        worker.run()
    assert len(accepted) == 1
    assert len(calls) == 1
    assert retries == [None]
    assert calls[0]["timeout"] == 600
    assert [r["insertId"] for r in calls[0]["data"]["rows"]] == job.row_ids
    assert worker.queue.requeued == []
    assert worker.job_notifier.messages == []
    assert job.batch_id in worker.error_notifier.messages[0][1]
    sink = StagedSink(False)
    target, emitted = make_target({"orders": sink})
    target.error_notification = FakeStreamNotification(*worker.error_notifier.messages)
    with pytest.raises(RuntimeError):
        target.drain_all(is_endofpipe=endofpipe)
    assert emitted == []
    assert not sink.published
    assert sink.checkpoints == 0


@pytest.mark.parametrize("recover", [False, True])
def test_only_request_not_found_retries_with_stable_ids(monkeypatch, recover):
    calls = []

    def insert_rows_json(**kwargs):
        calls.append(kwargs)
        if recover and len(calls) == 2:
            return []
        raise NotFound("table metadata not available")

    monkeypatch.setattr(
        streaming_insert,
        "bigquery_client_factory",
        lambda _: SimpleNamespace(
            insert_rows_json=insert_rows_json,
        ),
    )
    monkeypatch.setattr(streaming_insert, "sleep", lambda _: None)
    job = streaming_insert.Job(TABLE, [{"id": 1}], timeout=7)
    job = pickle.loads(pickle.dumps(job))
    worker = make_worker(streaming_insert.StreamingInsertWorker, job)
    if recover:
        worker.run()
        assert worker.job_notifier.messages == [True]
    else:
        with pytest.raises(NotFound):
            worker.run()
        assert worker.job_notifier.messages == []
    assert len(calls) == (2 if recover else 3)
    assert all(
        c["row_ids"] == job.row_ids and c["timeout"] == 7 and c["retry"] is None for c in calls
    )


@pytest.mark.parametrize("timeout", [None, True, 0, -1, float("inf"), float("nan"), "7"])
def test_invalid_timeout_cannot_enqueue(timeout):
    sink, queued = make_sink(timeout)
    with pytest.raises(ValueError, match="positive finite"):
        sink.process_batch({})
    assert queued == []
    assert sink.records_to_drain == [{"id": 1}]


def make_sink(timeout):
    queued = []
    sink = object.__new__(streaming_insert.BigQueryStreamingInsertSink)
    sink._config = {"timeout": timeout}
    sink.records_to_drain = [{"id": 1}]
    sink.table = SimpleNamespace(as_ref=lambda: TABLE)
    sink.global_queue = SimpleNamespace(put=queued.append)
    sink.increment_jobs_enqueued = lambda: None
    return sink, queued


def test_configured_timeout_and_identity_survive_process_serialization():
    sink, queued = make_sink(19)
    sink.process_batch({})
    original = queued[0]
    restored = pickle.loads(pickle.dumps(original))
    assert restored.timeout == 19
    assert restored.batch_id == original.batch_id
    assert restored.row_ids == original.row_ids
    assert restored.records == [{"id": 1}]
    assert sink.records_to_drain == []
    assert streaming_insert.Job(TABLE, restored.records).row_ids != restored.row_ids
