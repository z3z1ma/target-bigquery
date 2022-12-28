"""BigQuery Batch Job Sink.
Throughput test: 6m 25s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import concurrent.futures
import time
from io import BytesIO
from multiprocessing import Pipe, Process, Queue
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union

import orjson
from google.cloud import bigquery

from target_bigquery.core import (
    BaseBigQuerySink,
    BigQueryCredentials,
    Compressor,
    DenormalizedSink,
    ParType,
    bigquery_client_factory,
)

# Stream specific constants
MAX_CONCURRENCY_PER_WORKER = 1
"""Maximum number of concurrent LoadJobs per worker before awaiting."""
MAX_WORKERS_PER_STREAM = 5
"""Maximum number of workers per stream."""
WORKER_CREATION_MIN_INTERVAL = 10
"""Minimum interval between worker creation attempts, in seconds."""


class BatchJobWorker:
    def __init__(
        self,
        table: str,
        job_config: Dict[str, Any],
        queue: Queue,
        credentials: BigQueryCredentials,
        job_notifier: Connection,
    ):
        super().__init__()
        self.table: str = table
        self.job_config: Dict[str, Any] = job_config
        self.queue: Queue = queue
        self.credentials: BigQueryCredentials = credentials
        self.awaiting: List[concurrent.futures.Future] = []
        self.job_notifier: Connection = job_notifier

    def run(self):
        client: bigquery.Client = bigquery_client_factory(self.credentials)
        while True:
            payload: Optional[Union[memoryview, bytes]] = self.queue.get()
            if payload is None:
                break
            try:
                client.load_table_from_file(
                    BytesIO(payload),
                    self.table,
                    num_retries=3,
                    job_config=bigquery.LoadJobConfig(**self.job_config),
                ).result()
            except Exception as e:
                self.queue.put(payload)
                raise e
            self.job_notifier.send(True)


BatchJobThreadWorker = type("BatchJobThreadWorker", (BatchJobWorker, _Thread), {})
BatchJobProcessWorker = type("BatchJobProcessWorker", (BatchJobWorker, Process), {})


if TYPE_CHECKING:
    _BatchJobWorker = Union[
        BatchJobThreadWorker,
        BatchJobProcessWorker,
    ]


class BigQueryBatchJobSink(BaseBigQuerySink):
    worker_factory: Callable[[], "_BatchJobWorker"]

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
        self.workers: List["_BatchJobWorker"] = []
        self.job_notification, self.job_notifier = pipe(False)

        worker_cls = f"BatchJob{self.par_typ}Worker"
        self.logger.info(f"Configured Sink Class: {worker_cls}")
        self.worker_cls: Type["_BatchJobWorker"] = globals()[worker_cls]

        def worker_factory() -> "_BatchJobWorker":
            return self.worker_cls(
                table=self.table.as_ref(),
                queue=self.queue,
                credentials=self._credentials,
                job_notifier=self.job_notifier,
                job_config=self.job_config,
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
        self.buffer.close()
        self.queue.put(
            self.buffer.getvalue()
            if self.par_typ == ParType.PROCESS
            else self.buffer.getbuffer()
        )
        self._jobs_enqueued += 1
        del self.buffer
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
        # Anything in the queue at this point can be considered now a DLQ


class BigQueryBatchJobDenormalizedSink(DenormalizedSink, BigQueryBatchJobSink):
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
