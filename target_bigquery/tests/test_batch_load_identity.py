"""A lost append response must not become a second load with a different identity."""

import pickle
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from target_bigquery import batch_job
from target_bigquery.tests.test_workers import make_worker


@pytest.mark.parametrize("failure", ["submit", "poll"])
def test_completed_remote_append_is_not_repeated_after_lost_response(monkeypatch, failure):
    accepted = set()
    calls = []
    result_timeouts = []

    class Client:
        def load_table_from_file(self, file_obj, table, **kwargs):
            identity = kwargs.get("job_id") or f"automatic-{len(calls)}"
            calls.append(identity)
            if identity in accepted:
                raise Conflict("job already exists")
            accepted.add(identity)
            if failure == "submit":
                raise TimeoutError("response lost after append")

            def result(*, timeout=None):
                result_timeouts.append(timeout)
                raise TimeoutError("polling lost after append")

            return SimpleNamespace(result=result)

    monkeypatch.setattr(batch_job, "bigquery_client_factory", lambda _: Client())
    job = batch_job.Job(b'{"id":1}\n', bigquery.TableReference.from_string("p.d.t"), {}, timeout=17)
    worker = make_worker(batch_job.BatchJobWorker, job)
    with pytest.raises((TimeoutError, Conflict)):
        worker.run()
    assert len(accepted) == 1
    assert len(set(calls)) == 1
    assert worker.job_notifier.messages == []
    assert len(worker.error_notifier.messages) == 1
    assert calls[0] in worker.error_notifier.messages[0][1]
    if failure == "poll":
        assert len(calls) == 1
        assert result_timeouts == [17]


def test_retry_before_acceptance_uses_same_id_and_awaits_success(monkeypatch):
    calls = []
    waits = []

    class Client:
        def load_table_from_file(self, file_obj, table, **kwargs):
            calls.append(kwargs.get("job_id"))
            if len(calls) == 1:
                raise ConnectionError("not accepted")
            return SimpleNamespace(result=lambda *, timeout: waits.append(timeout))

    monkeypatch.setattr(batch_job, "bigquery_client_factory", lambda _: Client())
    job = batch_job.Job(b"{}", bigquery.TableReference.from_string("p.d.t"), {}, timeout=9)
    worker = make_worker(batch_job.BatchJobWorker, job)
    worker.run()
    assert calls[0] and calls[0] == calls[1]
    assert waits == [9]
    assert worker.job_notifier.messages == [True]
    assert worker.error_notifier.messages == []


@pytest.mark.parametrize("timeout", [None, True, 0, -1, float("inf"), float("nan"), "9"])
def test_invalid_configured_timeout_cannot_enqueue(timeout):
    sink, queued = make_sink(timeout)
    with pytest.raises(ValueError, match="positive finite"):
        batch_job.BigQueryBatchJobSink.process_batch(sink, {})
    assert queued == []
    assert sink.enqueued == 0


def make_sink(timeout):
    queued = []
    sink = SimpleNamespace(
        buffer=batch_job.Compressor(),
        global_par_typ=batch_job.ParType.PROCESS,
        global_queue=SimpleNamespace(put=queued.append),
        table=SimpleNamespace(as_ref=lambda: bigquery.TableReference.from_string("p.d.t")),
        job_config={},
        config={"timeout": timeout},
        enqueued=0,
    )
    sink.increment_jobs_enqueued = lambda: setattr(sink, "enqueued", sink.enqueued + 1)
    sink.buffer.write(b"{}\n")
    return sink, queued


def test_process_batch_preserves_configured_timeout_and_identity_across_pickle():
    sink, queued = make_sink(23)
    batch_job.BigQueryBatchJobSink.process_batch(sink, {})
    original = queued[0]
    restored = pickle.loads(pickle.dumps(original))
    assert restored.job_id == original.job_id
    assert restored.timeout == 23
    assert restored.data == original.data
    assert sink.enqueued == 1
    batch_job.BigQueryBatchJobSink.process_batch(sink, {})
    assert queued[1].job_id != original.job_id


@pytest.mark.parametrize("endofpipe", [False, True])
def test_uncertain_load_error_blocks_state_and_publication(monkeypatch, endofpipe):
    from target_bigquery.tests.test_overwrite_state import StagedSink, make_target
    from target_bigquery.tests.test_workers import FakeStreamNotification

    class Client:
        def load_table_from_file(self, *args, **kwargs):
            def result(*, timeout):
                raise TimeoutError("unknown remote outcome")

            return SimpleNamespace(result=result)

    monkeypatch.setattr(batch_job, "bigquery_client_factory", lambda _: Client())
    job = batch_job.Job(b"{}", bigquery.TableReference.from_string("p.d.t"), {})
    worker = make_worker(batch_job.BatchJobWorker, job)
    with pytest.raises(TimeoutError):
        worker.run()
    sink = StagedSink(False)
    target, emitted = make_target({"orders": sink})
    target.error_notification = FakeStreamNotification(*worker.error_notifier.messages)
    with pytest.raises(RuntimeError, match=job.job_id):
        target.drain_all(is_endofpipe=endofpipe)
    assert emitted == []
    assert sink.checkpoints == 0
    assert not sink.published
