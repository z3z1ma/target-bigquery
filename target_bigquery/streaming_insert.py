"""BigQuery Streaming Insert Sink.
Throughput test: ...slower than all other methods, no test results available.
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import time
from multiprocessing import Pipe, Process, Queue
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Type, Union

import orjson
from google.api_core.exceptions import GatewayTimeout, NotFound
from google.cloud import _http, bigquery
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

if TYPE_CHECKING:
    from singer_sdk import PluginBase

from target_bigquery.core import (
    BaseBigQuerySink,
    BigQueryCredentials,
    DenormalizedSink,
    ParType,
    bigquery_client_factory,
)

# Stream specific constants
MAX_CONCURRENCY_PER_WORKER = 10
"""Forms part of the formula for provisioning workers based on queued requests exceeding this number * workers."""
MAX_WORKERS_PER_STREAM = 5
"""Maximum number of workers per stream."""
WORKER_CREATION_MIN_INTERVAL = 0.1
"""Minimum interval between worker creation attempts, in seconds."""


class StreamingInsertWorker:
    def __init__(
        self,
        table: str,
        queue: Queue,
        credentials: BigQueryCredentials,
        job_notifier: Connection,
    ):
        super().__init__()
        self.table: str = table
        self.queue: Queue = queue
        self.credentials: BigQueryCredentials = credentials
        self.job_notifier = job_notifier

    def run(self):
        client: bigquery.Client = bigquery_client_factory(self.credentials)
        while True:
            payload: Optional[List[Dict[str, Any]]] = self.queue.get()
            if payload is None:
                break
            try:
                _ = retry(
                    client.insert_rows_json,
                    retry=retry_if_exception_type(
                        (ConnectionError, TimeoutError, NotFound, GatewayTimeout)
                    ),
                    wait=wait_fixed(1),
                    stop=stop_after_delay(10),
                    reraise=True,
                )(table=self.table, json_rows=payload)
            except Exception as exc:
                self.queue.put(payload)
                raise exc


StreamingInsertThreadWorker = type(
    "StreamingInsertThreadWorker", (StreamingInsertWorker, _Thread), {}
)
StreamingInsertProcessWorker = type(
    "StreamingInsertProcessWorker", (StreamingInsertWorker, Process), {}
)


if TYPE_CHECKING:
    _StreamingInsertWorker = Union[
        StreamingInsertThreadWorker,
        StreamingInsertProcessWorker,
    ]


class BigQueryStreamingInsertSink(BaseBigQuerySink):
    worker_factory: Callable[[], "_StreamingInsertWorker"]

    def __init__(
        self,
        target: "PluginBase",
        stream_name: str,
        schema: Dict[str, Any],
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        # Configure parallelism
        pipe, queue, self.par_typ = self.set_par_typ()

        self.queue = queue()
        self.workers: List["_StreamingInsertWorker"] = []
        self.job_notification, self.job_notifier = pipe(False)

        worker_cls = f"StreamingInsert{self.par_typ}Worker"
        self.logger.info(f"Configured Sink Class: {worker_cls}")
        self.worker_cls: Type["_StreamingInsertWorker"] = globals()[worker_cls]

        def worker_factory() -> "_StreamingInsertWorker":
            return self.worker_cls(
                self.table.as_ref(),
                self.queue,
                self._credentials,
                self.job_notifier,
            )

        self.worker_factory = worker_factory
        worker = self.worker_factory()
        worker.start()
        self.workers.append(worker)

        self._last_worker_creation = time.time()
        self._jobs_enqueued = 0

        # A hack since we can't override the default json encoder...
        _http.json = orjson
        self.logger.warning(
            "Using the legacy streaming API. It is highly recommended to use the new storage write streaming API."
        )

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(record["data"]).decode("utf-8")
        return record

    @property
    def max_size(self) -> int:
        return min(super().max_size, 500)

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.records_to_drain.append(record)

    def process_batch(self, context: Dict[str, Any]) -> None:
        self.ensure_workers()
        while self.job_notification.poll():
            self.job_notification.recv()
            self._jobs_enqueued -= 1
        self.queue.put(self.records_to_drain.copy())
        self._jobs_enqueued += 1
        self.records_to_drain = []

    @property
    def add_worker_predicate(self) -> bool:
        return (
            self._jobs_enqueued > MAX_CONCURRENCY_PER_WORKER * (len(self.workers) + 1)
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


class BigQueryStreamingInsertDenormalizedSink(
    DenormalizedSink, BigQueryStreamingInsertSink
):
    pass
