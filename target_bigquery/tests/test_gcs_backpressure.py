"""Collect upload paths without loading while process workers are still running."""

import copy
import io
import multiprocessing as mp
import uuid
from threading import Thread
from types import SimpleNamespace

import pytest

from target_bigquery import gcs_stage
from target_bigquery.core import Compressor, ParType
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_core_pure import make_bigquery_table
from target_bigquery.tests.test_gcs_checkpoints import setup_sink


def run_gcs_upload_worker():
    worker = mp.current_process()

    def blob(path, **kwargs):
        assert worker.release.wait(15)
        if worker.fail_id and worker.fail_id in path:
            raise RuntimeError("offline upload failure")
        with worker.uploads.get_lock():
            worker.uploads.value += 1
        return SimpleNamespace(open=lambda *args, **kwargs: io.BytesIO())

    gcs_stage.gcs_client_factory = lambda _: None
    gcs_stage.storage.Blob.from_string = blob
    original_get = worker.queue.get
    worker.queue.get = lambda timeout: original_get(timeout=0.5)
    type(worker).run(worker)


@pytest.mark.parametrize("eof", [False, True])
@pytest.mark.parametrize("sink_count,failure", [(1, None), (2, None), (1, "upload"), (1, "result")])
def test_gcs_paths_do_not_block_process_join(monkeypatch, eof, sink_count, failure):
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    owner = object.__new__(TargetBigQuery)
    owner._config = {"options": {"process_pool": True}}
    executor, pipe, factory, _ = owner.get_parallelization_components()
    queue = factory()
    completed, done = pipe(False)
    logged, log = pipe(False)
    errors_received, errors_sent = pipe(False)
    # Exercise the supported in-process gzip implementation, not external gzip startup.
    monkeypatch.setattr("target_bigquery.core.shutil.which", lambda _: None)
    sink, target, emitted, events, loads = setup_sink(monkeypatch, failure=failure)
    sink._config["batch_size"] = 1
    sinks, path_connections = [], []
    for index in range(sink_count):
        current = sink if index == 0 else copy.copy(sink)
        current.table = make_bigquery_table(name=f"orders{index}")
        current.stream_name = f"orders{index}"
        current.gcs_notification, current.gcs_notifier = pipe(False)
        path_connections.extend((current.gcs_notification, current.gcs_notifier))
        current.uris = []
        current._load_job_id = None
        current.bucket_name = "offline-staging"
        current.buffer = Compressor()
        current.global_par_typ = ParType.PROCESS
        current.global_queue = queue
        current.increment_jobs_enqueued = target.increment_jobs_enqueued
        sinks.append(current)
    target._sinks_active = {current.stream_name: current for current in sinks}
    target.par_typ = ParType.PROCESS
    target.queue = queue
    target.job_notification, target.log_notification = completed, logged
    target.error_notification = errors_received
    target.worker_pings = {}
    target._jobs_enqueued = 0
    del target._join_workers, target._shutdown_workers
    worker = gcs_stage.BigQueryGcsStagingSink.worker_cls_factory(executor, {})(
        ext_id="gcs-upload",
        queue=queue,
        credentials=None,
        job_notifier=done,
        error_notifier=errors_sent,
        log_notifier=log,
    )
    worker.release, worker.uploads = mp.Event(), mp.Value("i", 0)
    worker.run = run_gcs_upload_worker
    target.workers = [worker]
    identifiers = [str(uuid.uuid4()) for _ in range(1000)]
    worker.fail_id = identifiers[-1] if failure == "upload" else None
    original_factory = gcs_stage.bigquery_client_factory

    def load_client(credentials):
        client = original_factory(credentials)
        original_load = client.load_table_from_uri

        def load(*args, **kwargs):
            assert not worker.is_alive() and worker.normal_completion.is_set()
            assert target._jobs_enqueued == 0
            return original_load(*args, **kwargs)

        client.load_table_from_uri = load
        return client

    monkeypatch.setattr(gcs_stage, "bigquery_client_factory", load_client)
    errors = []

    def checkpoint():
        try:
            target.drain_all(is_endofpipe=eof)
        except Exception as exc:
            errors.append(exc)

    draining = Thread(target=checkpoint, daemon=True)
    worker.start()
    try:
        for identifier in identifiers:
            for current in sinks:
                current.process_record({"data": {"id": identifier}}, {})
                current.process_batch({"batch_id": identifier})
        worker.release.set()
        draining.start()
        draining.join(10)
        assert not draining.is_alive(), f"join stalled after {worker.uploads.value}/1000 uploads"
        if failure:
            assert len(errors) == 1 and emitted == []
            if failure == "upload":
                assert "offline upload failure" in str(errors[0])
                assert loads == [] and events == []
                assert len(sink.uris) == len(identifiers) - 1
                assert sink._load_job_id is None
            else:
                assert "polling lost" in str(errors[0])
                assert len(loads) == 1 and len(sink.uris) == len(identifiers)
                assert sink._load_job_id == loads[0][3]["job_id"]
            return
        assert errors == []
        assert worker.exitcode == 0 and worker.normal_completion.is_set()
        assert worker.uploads.value == len(identifiers) * sink_count
        assert target._jobs_enqueued == 0
        assert len(loads) == sink_count
        for current, loaded in zip(sinks, loads, strict=True):
            assert loaded[1] == current.table.name
            assert len(loaded[0]) == len(identifiers)
            assert all(
                identifier in path for identifier, path in zip(identifiers, loaded[0], strict=True)
            )
            assert len(set(loaded[0])) == len(identifiers)
            assert current.uris == []
            current._load_pending_uris()
        assert len(loads) == sink_count
        assert events == ["submit", ("loaded", 17)] * sink_count + ["state"]
        assert emitted
    finally:
        worker.release.set()
        target._handle_worker_error = lambda **kwargs: None
        if worker.is_alive():
            worker.terminate()
            worker.join(5)
        draining.join(5)
        worker.close()
        for current in sinks:
            current.buffer.close()
        for conn in (
            *path_connections,
            completed,
            done,
            logged,
            log,
            errors_received,
            errors_sent,
        ):
            conn.close()
        queue.cancel_join_thread()
        queue.close()
        mp.set_start_method(previous, force=True)


@pytest.mark.parametrize("process_mode", [False, True])
def test_collects_gcs_variants_only_in_process_mode(process_mode):
    from target_bigquery.tests.test_core_pure import FakeNotification
    from target_bigquery.tests.test_overwrite_state import make_target

    sinks = {}
    for index, cls in enumerate(
        (gcs_stage.BigQueryGcsStagingSink, gcs_stage.BigQueryGcsStagingDenormalizedSink)
    ):
        sink = object.__new__(cls)
        sink.uris = []
        sink.gcs_notification = FakeNotification(f"gs://offline/{index}")
        sinks[str(index)] = sink
    target, _ = make_target(sinks)
    target.par_typ = ParType.PROCESS if process_mode else ParType.THREAD
    target.job_notification = target.log_notification = SimpleNamespace(poll=lambda: False)
    target._drain_worker_notifications()
    target._drain_worker_notifications()
    for index, sink in enumerate(sinks.values()):
        assert sink.uris == ([f"gs://offline/{index}"] if process_mode else [])
