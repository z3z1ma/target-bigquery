"""Deterministic server outcomes across failed target runs and full-snapshot replay."""

import logging
import re

import pytest
from singer_sdk.sinks import BatchSink

from target_bigquery.tests.test_core_pure import make_bigquery_table, make_merge_sink
from target_bigquery.tests.test_overwrite_state import make_target


class RemoteJob:
    location = "EU"

    def __init__(self, server, job_id, source, destination, failure):
        self.server = server
        self.job_id = job_id
        self.source = source
        self.destination = destination
        self.failure = failure
        self.done = False

    def complete(self):
        if not self.done:
            self.server.tables[self.destination] = list(self.server.tables[self.source])
            del self.server.tables[self.source]
            self.done = True

    def result(self, *, timeout):
        assert timeout == 17
        if self.failure == "wait_pending":
            raise TimeoutError("response unavailable")
        self.complete()
        if self.failure == "wait_completed":
            raise TimeoutError("response unavailable")


class RemoteServer:
    """Model table publication independently of whether the client gets a response."""

    project = "project"
    location = "EU"

    def __init__(self, failure):
        self.tables = {"first": [0], "second": [0]}
        self.jobs = {}
        self.failure = failure

    def query(self, sql, *, timeout, job_id, job_retry):
        assert timeout == 17
        assert job_retry is None
        assert job_id not in self.jobs
        names = re.findall(r"`project`\.`analytics`\.`([^`]+)`", sql)
        destination, source, dropped = names
        assert dropped == source
        failure = self.failure if destination == "second" else None
        job = RemoteJob(self, job_id, source, destination, failure)
        self.jobs[job_id] = job
        if failure == "submit_completed":
            job.complete()
        if failure in {"submit_completed", "submit_pending"}:
            raise TimeoutError("response unavailable")
        return job


def new_run(server, rows, monkeypatch):
    sinks = {}
    stages = []
    for name in ("first", "second"):
        sink = make_merge_sink({"id": {"type": "integer"}})
        sink.stream_name = name
        sink._config.update(timeout=17, project="project", location="EU")
        sink.overwrite_target = make_bigquery_table(name=name)
        sink.merge_target = None
        staging = sink._temporary_table_name()
        sink.table = make_bigquery_table(name=staging)
        sink.logger = logging.getLogger(f"recovery.{name}")
        monkeypatch.setattr(sink, "_get_bigquery_client", lambda: server)
        server.tables[staging] = list(rows)
        sinks[name] = sink
        stages.append(staging)
    # Keep real target drain ordering and real sink publication/query handling.
    # Transport and SDK bookkeeping are outside this post-staging fault boundary.
    monkeypatch.setattr(BatchSink, "clean_up", lambda self: None)
    target, emitted = make_target(sinks)
    target._latest_state = {"snapshot": rows[-1]}
    return target, emitted, stages


@pytest.mark.parametrize(
    "failure", ["submit_pending", "submit_completed", "wait_pending", "wait_completed"]
)
def test_reconciled_partial_publication_replays_to_fresh_staging(monkeypatch, caplog, failure):
    server = RemoteServer(failure)
    first_run, first_states, old_stages = new_run(server, [1, 2], monkeypatch)
    first_run.drain_all()
    assert first_states == []
    with pytest.raises(TimeoutError, match="response unavailable"):
        first_run.drain_all(is_endofpipe=True)
    assert first_states == []
    assert server.tables["first"] == [1, 2]
    failed_job = list(server.jobs.values())[-1]
    assert failed_job.job_id in caplog.text
    assert "project=project, location=EU" in caplog.text
    assert "reconcile this job before restarting" in caplog.text
    assert first_run._sinks_active["second"].overwrite_target is not None
    assert server.tables["second"] == ([1, 2] if failed_job.done else [0])

    # Operator reconciliation: let the old remote job reach a known terminal state
    # before a fresh process replays a COMPLETE snapshot from persisted state.
    failed_job.complete()
    server.failure = None
    replay, replay_states, new_stages = new_run(server, [1, 2, 3], monkeypatch)
    assert set(old_stages).isdisjoint(new_stages)
    replay.drain_all()
    assert replay_states == []
    replay.drain_all(is_endofpipe=True)
    assert replay_states == [{"snapshot": 3}]
    assert server.tables == {"first": [1, 2, 3], "second": [1, 2, 3]}
    assert len(server.jobs) == 4


def test_unreconciled_remote_job_can_replace_a_newer_snapshot(monkeypatch):
    """Document why a fresh UUID alone cannot make overlapping replay safe."""
    server = RemoteServer("wait_pending")
    first_run, first_states, _ = new_run(server, [1, 2], monkeypatch)
    with pytest.raises(TimeoutError):
        first_run.drain_all(is_endofpipe=True)
    old_job = list(server.jobs.values())[-1]
    server.failure = None
    replay, replay_states, _ = new_run(server, [1, 2, 3], monkeypatch)
    replay.drain_all(is_endofpipe=True)
    assert first_states == []
    assert replay_states == [{"snapshot": 3}]
    assert server.tables["second"] == [1, 2, 3]
    old_job.complete()
    assert server.tables["second"] == [1, 2]


def test_explicit_job_id_survives_conflict_in_real_client(monkeypatch, caplog):
    """Exercise the installed client's explicit-ID branch without cloud access."""
    from google.api_core.exceptions import Conflict
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    client = bigquery.Client(project="project", location="EU", credentials=AnonymousCredentials())
    submitted = []

    def begin(job, **kwargs):
        submitted.append(job.job_id)
        raise Conflict("previous submission may already exist")

    monkeypatch.setattr(bigquery.QueryJob, "_begin", begin)
    monkeypatch.setattr(
        client, "get_job", lambda *a, **k: pytest.fail("unexpected automatic job recovery")
    )
    target, emitted, _ = new_run(RemoteServer(None), [1, 2], monkeypatch)
    sink = target._sinks_active["first"]
    # Configuration is not the client's actual query location.
    sink._config["location"] = "US"
    monkeypatch.setattr(sink, "_get_bigquery_client", lambda: client)
    with caplog.at_level(logging.INFO), pytest.raises(Conflict):
        target.drain_all(is_endofpipe=True)
    assert emitted == []
    assert len(submitted) == 1
    assert submitted[0] in caplog.text
    assert "location=EU" in caplog.text
    assert "location=US" not in caplog.text
    assert "Submitting BigQuery query" in caplog.text


def test_submission_without_client_location_reports_unknown(monkeypatch, caplog):
    server = RemoteServer("submit_pending")
    server.location = None
    target, emitted, _ = new_run(server, [1], monkeypatch)
    with pytest.raises(TimeoutError):
        target.drain_all(is_endofpipe=True)
    assert emitted == []
    assert "location=unavailable" in caplog.text
