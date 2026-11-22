"""Offline characterization; run with PYTHONPATH=. python -m pytest <this file>."""

import multiprocessing as mp
from threading import Event

import pytest
from google.cloud.bigquery_storage_v1 import types

from target_bigquery import batch_job, gcs_stage, storage_write, streaming_insert
from target_bigquery.core import Compressor, IngestionStrategy, ParType
from target_bigquery.target import TargetBigQuery
from target_bigquery.tests.test_core_pure import make_bigquery_table
from target_bigquery.tests.test_process_data_jobs import run_offline_data_worker
from target_bigquery.tests.test_worker_exit import target_with_workers
from target_bigquery.tests.test_workers import FakeNotifier


class InjectedBadPayload:
    def __init__(self, entered, release):
        self.entered, self.release = entered, release

    def __reduce__(self):
        self.entered.set()
        if not self.release.wait(5):
            raise RuntimeError("probe gate timed out")
        raise TypeError("deliberately unpicklable custom payload")


def fingerprint(job):
    values = []
    for name, value in vars(job).items():
        if name in ("stream_notifier", "gcs_notifier"):
            continue
        if isinstance(value, (types.AppendRowsRequest, types.ProtoRows)):
            value = type(value).serialize(value)
        elif name == "table":
            value = str(value)
        elif name == "config":
            value = repr(value)
        values.append((name, value))
    return values


def receive_jobs(queue, results):
    while True:
        job = queue.get(timeout=5)
        if job is None:
            return
        for name in ("stream_notifier", "gcs_notifier"):
            notifier = getattr(job, name, None)
            if notifier is not None:
                notifier.send("received notifier works")
        results.send(fingerprint(job))


@pytest.fixture
def spawn():
    previous = mp.get_start_method(allow_none=True)
    mp.set_start_method("spawn", force=True)
    yield
    mp.set_start_method(previous, force=True)


def configured_queue():
    owner = object.__new__(TargetBigQuery)
    owner._config = {"options": {"process_pool": True}}
    return owner.get_parallelization_components()[2]()


def test_injected_feeder_loss_is_reported(spawn):
    queue = configured_queue()
    entered, release, failed = Event(), Event(), Event()
    errors = []
    default_hook = queue._on_queue_feeder_error

    def on_error(error, obj):
        default_hook(error, obj)
        errors.append(str(error))
        failed.set()

    queue._on_queue_feeder_error = on_error
    observed, sent = mp.Pipe(False)
    worker = batch_job.BatchJobProcessWorker(
        ext_id="injected-loss",
        queue=queue,
        credentials=None,
        job_notifier=FakeNotifier(),
        error_notifier=FakeNotifier(),
        log_notifier=FakeNotifier(),
    )
    worker.fail_transport = False
    worker.observed = sent
    worker.run = run_offline_data_worker
    worker.start()
    try:
        queue.put(InjectedBadPayload(entered, release))
        assert entered.wait(5)
        queue.put(None)
        release.set()
        assert failed.wait(5)
        worker.join(10)
        assert worker.exitcode == 0
        assert worker.normal_completion.is_set()
        assert not observed.poll()
        target, sink, emitted = target_with_workers([worker])
        target.queue = queue
        with pytest.raises(RuntimeError, match="deliberately unpicklable"):
            target.drain_all(is_endofpipe=True)
        assert not sink.published and emitted == []
        assert errors == ["deliberately unpicklable custom payload"]
    finally:
        release.set()
        if worker.is_alive():
            worker.terminate()
            worker.join(5)
        worker.close()
        observed.close()
        sent.close()
        queue.cancel_join_thread()
        queue.close()


@pytest.mark.parametrize("method", ["batch", "gcs", "insert", "storage"])
@pytest.mark.parametrize("denormalized", [False, True])
def test_actual_producer_jobs_cross_spawn_queue(spawn, method, denormalized):
    queue = configured_queue()
    results, output = mp.Pipe(False)
    notifications, notifier = mp.Pipe(False)
    classes = {
        "batch": (batch_job.BigQueryBatchJobSink, batch_job.BigQueryBatchJobDenormalizedSink),
        "gcs": (gcs_stage.BigQueryGcsStagingSink, gcs_stage.BigQueryGcsStagingDenormalizedSink),
        "insert": (
            streaming_insert.BigQueryStreamingInsertSink,
            streaming_insert.BigQueryStreamingInsertDenormalizedSink,
        ),
        "storage": (
            storage_write.BigQueryStorageWriteSink,
            storage_write.BigQueryStorageWriteDenormalizedSink,
        ),
    }
    sink = object.__new__(classes[method][denormalized])
    sink._config = {}
    sink.stream_name = "orders"
    sink.table = make_bigquery_table(
        ingestion_strategy=IngestionStrategy.DENORMALIZED
        if denormalized
        else IngestionStrategy.FIXED,
        jsonschema={
            "type": "object",
            "properties": {
                "id": {"type": ["integer", "null"]},
                "detail": {"type": "object", "properties": {"label": {"type": "string"}}},
                "items": {"type": "array", "items": {"type": "integer"}},
            },
        },
    )
    sink.global_par_typ = ParType.PROCESS
    sink.increment_jobs_enqueued = lambda: None
    captured = []

    # Capture exactly what process_batch constructs, then submit it to the real queue.
    class Capture:
        def put(self, job):
            captured.append(job)

        def qsize(self):
            return 0

    sink.global_queue = Capture()
    if method in ("batch", "gcs"):
        sink.buffer = Compressor()
    if method == "gcs":
        sink.bucket_name = "offline-bucket"
        sink.gcs_notifier = notifier
    if method == "insert":
        sink.records_to_drain = []
    if method == "storage":
        sink.parent = "projects/p/datasets/d/tables/t"
        sink.stream_notifier = notifier
        sink.template = storage_write.generate_template(sink.proto_schema)
        sink.start_batch({})
    record = {"id": None, "detail": {"label": "test"}, "items": [1, 2]}
    sink.process_record(sink.preprocess_record(record, {}), {})
    sink.process_batch({"batch_id": "offline-batch"})
    assert len(captured) == 1
    expected = fingerprint(captured[0])
    process = mp.Process(target=receive_jobs, args=(queue, output))
    process.start()
    try:
        queue.put(captured[0])
        queue.put(None)
        assert results.poll(10)
        assert results.recv() == expected
        if method in ("gcs", "storage"):
            assert notifications.poll(5)
            assert notifications.recv() == "received notifier works"
        process.join(10)
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)
        process.close()
        for conn in (results, output, notifications, notifier):
            conn.close()
        queue.cancel_join_thread()
        queue.close()
