"""BigQuery GCS Staging Sink.
Throughput test: 6m 30s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import concurrent.futures
import shutil
import time
from io import BytesIO
from multiprocessing import Pipe, Process, Queue
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import orjson
from google.cloud import bigquery, storage

from target_bigquery.constants import DEFAULT_BUCKET_PATH
from target_bigquery.core import (
    BaseBigQuerySink,
    BigQueryCredentials,
    Compressor,
    DenormalizedSink,
    ParType,
    bigquery_client_factory,
    gcs_client_factory,
)

# Stream specific constants
MAX_CONCURRENCY_PER_WORKER = 1
"""Maximum number of concurrent LoadJobs per worker before awaiting."""
MAX_WORKERS_PER_STREAM = 5
"""Maximum number of workers per stream. We must balance network upload speed with timeouts."""
WORKER_CREATION_MIN_INTERVAL = 10
"""Minimum interval between worker creation attempts, in seconds."""


class GcsStagingWorker:
    def __init__(
        self,
        table: str,
        dataset: str,
        bucket: str,
        queue: Queue,
        credentials: BigQueryCredentials,
        job_notifier: Connection,
        gcs_notifier: Connection,
    ):
        super().__init__()
        self.table: str = table
        self.dataset: str = dataset
        self.bucket: str = bucket
        self.queue: Queue = queue
        self.credentials: BigQueryCredentials = credentials
        self.awaiting: List[concurrent.futures.Future] = []
        self.job_notifier: Connection = job_notifier
        self.gcs_notifier: Connection = gcs_notifier

    def run(self):
        client: storage.Client = gcs_client_factory(self.credentials)
        while True:
            payload: Optional[Tuple[Union[memoryview, bytes], str]] = self.queue.get()
            if payload is None:
                break
            buf, batch_id = payload
            try:
                # TODO: consider configurability?
                path = DEFAULT_BUCKET_PATH.format(
                    bucket=self.bucket,
                    dataset=self.dataset,
                    table=self.table,
                    date=time.strftime("%Y-%m-%d"),
                    batch_id=batch_id,
                )
                blob = storage.Blob.from_string(path, client=client)
                # TODO: pass in timeout?
                # TODO: composite uploads
                with blob.open(
                    "wb",
                    if_generation_match=0,
                    chunk_size=1024 * 1024 * 10,
                    timeout=300,
                ) as f:
                    shutil.copyfileobj(BytesIO(buf), f)
                self.gcs_notifier.send(path)
            except Exception as exc:
                self.queue.put(payload)
                raise exc
            self.job_notifier.send(True)


GcsStagingThreadWorker = type("GcsStagingThreadWorker", (GcsStagingWorker, _Thread), {})
GcsStagingProcessWorker = type(
    "GcsStagingProcessWorker", (GcsStagingWorker, Process), {}
)


if TYPE_CHECKING:
    _GcsStagingWorker = Union[
        GcsStagingThreadWorker,
        GcsStagingProcessWorker,
    ]


class BigQueryGcsStagingSink(BaseBigQuerySink):
    worker_factory: Callable[[], "_GcsStagingWorker"]

    @property
    def job_config(self) -> Dict[str, Any]:
        return {
            "schema": self.table.get_resolved_schema(),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
        }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # Configure parallelism
        pipe, queue, self.par_typ = self.set_par_typ()

        self.buffer = Compressor()
        self.queue = queue()
        self.workers: List["_GcsStagingWorker"] = []
        self.job_notification, self.job_notifier = pipe(False)
        self.gcs_notification, self.gcs_notifier = pipe(False)
        self.uris: List[str] = []

        worker_cls = f"GcsStaging{self.par_typ}Worker"
        self.logger.info(f"Configured Sink Class: {worker_cls}")
        self.worker_cls: Type["_GcsStagingWorker"] = globals()[worker_cls]

        def worker_factory() -> "_GcsStagingWorker":
            return self.worker_cls(
                table=self.table.table,
                dataset=self.table.dataset,
                bucket=self.config["bucket"],
                queue=self.queue,
                credentials=self._credentials,
                job_notifier=self.job_notifier,
                gcs_notifier=self.gcs_notifier,
            )

        self.worker_factory = worker_factory
        worker = worker_factory()
        worker.start()
        self.workers.append(worker)

        self._last_worker_creation = time.time()
        self._jobs_enqueued = 0

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.buffer.write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))

    def process_batch(self, context: Dict[str, Any]) -> None:
        self.ensure_workers()
        while self.job_notification.poll():
            self.job_notification.recv()
            self._jobs_enqueued -= 1
        while self.gcs_notification.poll():
            self.uris.append(self.gcs_notification.recv())
        self.buffer.close()
        self.queue.put(
            (
                self.buffer.getvalue()
                if self.par_typ == ParType.PROCESS
                else self.buffer.getbuffer(),
                context["batch_id"],
            )
        )
        self._jobs_enqueued += 1
        self.buffer = Compressor()

    @property
    def add_worker_predicate(self) -> bool:
        return (
            self._jobs_enqueued
            and len(self.workers)
            < self.config.get("options", {}).get(
                "max_workers_per_stream", MAX_WORKERS_PER_STREAM
            )
            and time.time() - self._last_worker_creation > WORKER_CREATION_MIN_INTERVAL
        )

    def clean_up(self) -> None:
        for worker in self.workers:
            if worker.is_alive():
                self.queue.put(None)
        for worker in self.workers:
            worker.join()
        while self.gcs_notification.poll():
            self.uris.append(self.gcs_notification.recv())
        # Anything in the queue at this point can be considered now a DLQ
        if self.uris:
            self.logger.info("Loading data into BigQuery from GCS stage...")
            self.logger.info("URIs: %s", ", ".join(self.uris))
            client = bigquery_client_factory(self._credentials)
            client.load_table_from_uri(
                self.uris,
                self.table.as_ref(),
                timeout=self.config.get("timeout", 600),
                job_config=bigquery.LoadJobConfig(**self.job_config),
            ).result()
            self.logger.info("Data loaded successfully")
        else:
            self.logger.info("No data to load")


class BigQueryGcsStagingDenormalizedSink(DenormalizedSink, BigQueryGcsStagingSink):
    @property
    def job_config(self) -> Dict[str, Any]:
        return {
            "schema": self.table.get_resolved_schema(),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
            "schema_update_options": [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
        }

    # Defer schema evolution the the write disposition
    def evolve_schema(self: BaseBigQuerySink) -> None:
        pass
