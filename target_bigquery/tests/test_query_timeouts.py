"""Query wait failures must never acknowledge uncommitted progress."""

import logging
from collections import deque
from types import SimpleNamespace

import pytest
from google.cloud import bigquery
from singer_sdk.sinks import BatchSink

from target_bigquery.core import ParType
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_core_pure import make_bigquery_table, make_merge_sink, make_sink


class Client:
    def __init__(self, failure=None):
        self.failure = failure
        self.calls = []
        self.job_id = "job-123"
        self.location = "US"

    def get_table(self, ref):
        return bigquery.Table(ref, schema=[bigquery.SchemaField("id", "INTEGER")])

    def query(self, sql, *, timeout, job_id, job_retry):
        self.job_id = job_id
        assert job_retry is None
        self.calls.append(("submit", timeout))
        if self.failure == "submit":
            raise TimeoutError("submit timeout")
        return self

    def result(self, *, timeout):
        self.calls.append(("result", timeout))
        if self.failure == "result":
            raise TimeoutError("result timeout")


@pytest.mark.parametrize("timeout", [600, 123])
@pytest.mark.parametrize("operation", ["merge", "overwrite", "view"])
def test_queries_bound_submission_and_wait(monkeypatch, operation, timeout):
    sink = make_merge_sink({"id": {"type": "integer"}})
    sink._config["timeout"] = timeout
    client = Client()
    sink.client = client
    monkeypatch.setattr(sink, "_get_bigquery_client", lambda: client)
    if operation == "merge":
        sink.merge_table(client)
    elif operation == "overwrite":
        sink.merge_target = None
        sink.overwrite_target = make_bigquery_table()
        monkeypatch.setattr(BatchSink, "clean_up", lambda self: None)
        sink.clean_up()
    else:
        sink = make_sink({"generate_view": True, "timeout": timeout})
        sink.client = client
        sink.table = make_bigquery_table()
        monkeypatch.setattr(sink.table, "create_table", lambda *args, **kwargs: True)
        sink.create_target()
    assert client.calls == [("submit", timeout), ("result", timeout)]


@pytest.mark.parametrize("failure", ["submit", "result"])
@pytest.mark.parametrize(
    "operation,endofpipe", [("merge", False), ("merge", True), ("overwrite", True)]
)
def test_query_failure_does_not_emit_state_or_clear_staging(
    monkeypatch, caplog, failure, operation, endofpipe
):
    sink = make_merge_sink({"id": {"type": "integer"}})
    sink.overwrite_target = None
    if operation == "overwrite":
        sink.overwrite_target, sink.merge_target = sink.merge_target, None
    staging, merge, overwrite = sink.table, sink.merge_target, sink.overwrite_target
    client = Client(failure)
    sink.logger = logging.getLogger("query-test")
    monkeypatch.setattr(sink, "_get_bigquery_client", lambda: client)
    target = object.__new__(TargetBigQuery)
    target._pending_worker_errors = deque()
    target.par_typ = ParType.THREAD
    target._config = {}
    target._latest_state = {"pos": 7}
    target._sinks_active = {"orders": sink}
    target.max_parallelism = 1
    target.error_notification = SimpleNamespace(poll=lambda: False)
    monkeypatch.setattr(target, "_drain_all", lambda *args: None)
    monkeypatch.setattr(target, "_join_workers", lambda: None)
    monkeypatch.setattr(target, "_shutdown_workers", lambda: None)
    monkeypatch.setattr(target, "_write_state_message", lambda state: pytest.fail("state advanced"))
    with pytest.raises(TimeoutError, match=failure):
        target.drain_all(is_endofpipe=endofpipe)
    assert (sink.table, sink.merge_target, sink.overwrite_target) == (staging, merge, overwrite)
    assert client.job_id.startswith("target_bigquery_")
    assert client.job_id in caplog.text
    assert "reconcile this job before restarting" in caplog.text


@pytest.mark.parametrize("timeout", [None, True, 0, -1, float("inf"), float("nan"), 1.5, "600"])
def test_invalid_query_timeout_fails_before_submission(timeout):
    client = Client()
    with pytest.raises(ValueError, match="positive integer"):
        make_sink({"timeout": timeout})._run_query(client, "SELECT 1")
    assert client.calls == []
