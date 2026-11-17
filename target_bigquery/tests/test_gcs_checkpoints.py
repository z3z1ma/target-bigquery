"""Uploaded GCS objects are not a durable BigQuery checkpoint until loaded."""

import logging
from types import SimpleNamespace

import pytest
from singer_sdk.sinks import BatchSink

from target_bigquery.gcs_stage import BigQueryGcsStagingSink
from target_bigquery.tests.test_core_pure import FakeNotification, make_bigquery_table
from target_bigquery.tests.test_overwrite_state import make_target


def setup_sink(monkeypatch, *, failure=None, overwrite=False, upsert=False):
    events = []
    loads = []

    class Client:
        project = "project"
        location = "EU"

        def load_table_from_uri(self, uris, table, *, timeout, job_config, **kwargs):
            loads.append((list(uris), table.table_id, timeout, kwargs))
            events.append("submit")
            if failure == "submit":
                raise TimeoutError("submission lost")

            def result(*, timeout=None):
                events.append(("loaded", timeout))
                if failure == "result":
                    raise TimeoutError("polling lost")

            return SimpleNamespace(result=result, location="EU")

    monkeypatch.setattr("target_bigquery.gcs_stage.bigquery_client_factory", lambda _: Client())
    monkeypatch.setattr(BatchSink, "clean_up", lambda self: None)
    sink = object.__new__(BigQueryGcsStagingSink)
    sink._config = {"timeout": 17}
    sink._credentials = object()
    sink.logger = logging.getLogger("gcs-checkpoint-test")
    sink.stream_name = "orders"
    sink._key_properties = []
    sink.table = make_bigquery_table(name="stage0" if overwrite or upsert else "orders")
    sink.merge_target = make_bigquery_table() if upsert else None
    sink.overwrite_target = make_bigquery_table() if overwrite else None
    sink.uris = []
    sink._load_job_id = None
    sink.gcs_notification = FakeNotification("gs://bucket/first.jsonl.gz")
    sink._get_bigquery_client = lambda: Client()
    target, emitted = make_target({"orders": sink})
    target._write_state_message = lambda state: (events.append("state"), emitted.append(state))
    return sink, target, emitted, events, loads


def test_append_checkpoint_loads_before_state_and_never_reloads_at_eof(monkeypatch):
    sink, target, emitted, events, loads = setup_sink(monkeypatch)
    target.drain_all()
    assert events == ["submit", ("loaded", 17), "state"]
    assert sink.uris == []
    sink.gcs_notification.messages.append("gs://bucket/second.jsonl.gz")
    target._latest_state = {"pos": 2}
    target.drain_all()
    target.drain_all(is_endofpipe=True)
    assert [load[0] for load in loads] == [
        ["gs://bucket/first.jsonl.gz"],
        ["gs://bucket/second.jsonl.gz"],
    ]
    assert all(load[1] == "orders" for load in loads)
    assert loads[0][3]["job_id"] != loads[1][3]["job_id"]
    assert emitted[-1] == {"pos": 2}


@pytest.mark.parametrize("endofpipe", [False, True])
@pytest.mark.parametrize("failure", ["submit", "result"])
@pytest.mark.parametrize("strategy", ["append", "upsert", "overwrite"])
def test_load_failure_retains_uris_and_withholds_state(
    monkeypatch, caplog, failure, endofpipe, strategy
):
    sink, target, emitted, events, loads = setup_sink(
        monkeypatch, failure=failure, upsert=strategy == "upsert", overwrite=strategy == "overwrite"
    )

    def forbidden(*args, **kwargs):
        pytest.fail("publication/rotation occurred after failed load")

    monkeypatch.setattr(sink, "merge_table", forbidden)
    monkeypatch.setattr(sink, "_create_overwrite_table", forbidden)
    monkeypatch.setattr(sink, "_run_query", forbidden)
    with pytest.raises(TimeoutError):
        target.drain_all(is_endofpipe=endofpipe)
    assert emitted == []
    assert sink.uris == ["gs://bucket/first.jsonl.gz"]
    assert len(loads) == 1
    assert loads[0][3]["job_id"] in caplog.text
    assert "Remote completion may be unknown" in caplog.text


def test_upsert_loads_current_staging_before_merge_and_rotation(monkeypatch):
    sink, target, emitted, events, loads = setup_sink(monkeypatch, upsert=True)
    monkeypatch.setattr(sink, "merge_table", lambda **kwargs: events.append("merge"))

    def rotate():
        events.append("rotate")
        sink.table = make_bigquery_table(name="stage1")

    monkeypatch.setattr(sink, "_create_overwrite_table", rotate)
    target.drain_all()
    assert events == ["submit", ("loaded", 17), "merge", "rotate", "state"]
    sink.gcs_notification.messages.append("gs://bucket/second.jsonl.gz")
    target.drain_all()
    assert [load[1] for load in loads] == ["stage0", "stage1"]
    assert len(emitted) == 2


def test_overwrite_loads_at_checkpoint_but_defers_state_until_publication(monkeypatch):
    sink, target, emitted, events, loads = setup_sink(monkeypatch, overwrite=True)
    monkeypatch.setattr(sink, "_run_query", lambda *args: events.append("publish"))
    target.drain_all()
    assert events == ["submit", ("loaded", 17)]
    assert emitted == []
    target.drain_all(is_endofpipe=True)
    assert events == ["submit", ("loaded", 17), "publish", "state"]
    assert len(loads) == 1
    assert emitted == [{"pos": 1}]


@pytest.mark.parametrize("failure", ["submit", "result"])
def test_failed_load_keeps_identity_on_reentry(monkeypatch, failure):
    sink, target, emitted, events, loads = setup_sink(monkeypatch, failure=failure)
    for _ in range(2):
        with pytest.raises(TimeoutError):
            target.drain_all()
    assert loads[0][3]["job_id"] == loads[1][3]["job_id"]
    assert emitted == []


@pytest.mark.parametrize("timeout", [0, -1, True, None, 1.5, "17"])
def test_invalid_timeout_rejects_before_load(monkeypatch, timeout):
    sink, target, emitted, events, loads = setup_sink(monkeypatch)
    sink._config["timeout"] = timeout
    with pytest.raises(ValueError, match="positive integer"):
        target.drain_all()
    assert loads == []
    assert emitted == []
    assert sink.uris == ["gs://bucket/first.jsonl.gz"]


def test_merge_failure_after_load_withholds_state_without_reloading_objects(monkeypatch):
    sink, target, emitted, events, loads = setup_sink(monkeypatch, upsert=True)

    def fail_merge(**kwargs):
        raise RuntimeError("merge failed")

    monkeypatch.setattr(sink, "merge_table", fail_merge)
    for _ in range(2):
        with pytest.raises(RuntimeError, match="merge failed"):
            target.drain_all()
    assert emitted == []
    assert len(loads) == 1
    assert sink.uris == []
    assert sink.table.name == "stage0"
    assert sink.merge_target is not None


def test_gcs_failure_suppresses_global_state_after_other_sink_succeeds(monkeypatch):
    from target_bigquery.tests.test_overwrite_state import StagedSink

    sink, target, emitted, events, loads = setup_sink(monkeypatch, failure="result")
    other = StagedSink(False)
    target._sinks_active = {"other": other, "orders": sink}
    with pytest.raises(TimeoutError):
        target.drain_all()
    assert other.checkpoints == 1
    assert emitted == []
