"""BigQuery Storage Write Sink.
Throughput test: 11m 0s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import concurrent.futures
import time
from enum import Enum
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
    Set,
    Tuple,
    Type,
    Union,
)

import orjson
from google.api_core.exceptions import NotFound, Unknown
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types, writer
from google.protobuf import json_format
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

if TYPE_CHECKING:
    from singer_sdk import PluginBase

from target_bigquery.core import (
    BaseBigQuerySink,
    BigQueryCredentials,
    DenormalizedSink,
    ParType,
    ProtoJIT,
    storage_client_factory,
)

# Stream specific constants
MAX_CONCURRENCY_PER_WORKER = 100
"""Maximum number of concurrent requests per worker be processed by grpc before awaiting."""
MAX_WORKERS_PER_STREAM = 10
"""Maximum number of workers per stream."""
WORKER_CREATION_MIN_INTERVAL = 5
"""Minimum interval between worker creation attempts, in seconds."""


def make_append_rows_stream(
    client: BigQueryWriteClient, parent: str, template: types.AppendRowsRequest
) -> Tuple[int, types.WriteStream, writer.AppendRowsStream]:
    write_stream = types.WriteStream()
    write_stream.type_ = types.WriteStream.Type.PENDING

    write_stream = client.create_write_stream(parent=parent, write_stream=write_stream)
    template.write_stream = write_stream.name
    append_rows_stream = writer.AppendRowsStream(client, template)

    return 0, write_stream, append_rows_stream


def get_default_stream(
    client: BigQueryWriteClient, parent: str, template: types.AppendRowsRequest
) -> Tuple[int, types.WriteStream, writer.AppendRowsStream]:
    write_stream = client.get_write_stream(name=parent + "/streams/_default")

    template.write_stream = write_stream.name
    append_rows_stream = writer.AppendRowsStream(client, template)

    return 0, write_stream, append_rows_stream


def make_request(
    payload: types.ProtoRows, offset: Optional[int] = None
) -> types.AppendRowsRequest:
    request = types.AppendRowsRequest()
    if offset is not None:
        request.offset = int(offset)

    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = payload
    request.proto_rows = proto_data

    return request


class StreamType(str, Enum):
    STREAM = "Stream"
    BATCH = "Batch"


class StorageWriteBatchWorker:
    def __init__(
        self,
        parent: str,
        template: types.AppendRowsRequest,
        queue: Queue,
        credentials: BigQueryCredentials,
        job_notifier: Connection,
        stream_notifier: Connection,
    ):
        super().__init__()
        self.parent: str = parent
        self.template: types.AppendRowsRequest = template
        self.queue: Queue = queue
        self.credentials: BigQueryCredentials = credentials
        self.awaiting: List[concurrent.futures.Future] = []
        self.job_notifier = job_notifier
        self.stream_notifier = stream_notifier

    def run(self):
        client: BigQueryWriteClient = storage_client_factory(self.credentials)
        offset, write_stream, append_rows_stream = make_append_rows_stream(
            client,
            self.parent,
            self.template,
        )
        self.stream_notifier.send(write_stream.name)
        while True:
            payload: Optional[types.ProtoRows] = self.queue.get()
            if payload is None:
                self.wait(drain=True)
                append_rows_stream.close()
                break
            try:
                self.awaiting.append(
                    retry(
                        append_rows_stream.send,
                        retry=retry_if_exception_type((Unknown, NotFound)),
                        wait=wait_fixed(1),
                        stop=stop_after_delay(5),
                        reraise=True,
                    )(make_request(payload, offset))
                )
            except Exception as exc:
                self.wait(drain=True)
                append_rows_stream.close()
                self.queue.put(payload)
                raise exc
            offset += len(payload.serialized_rows)
            if len(self.awaiting) > MAX_CONCURRENCY_PER_WORKER:
                self.wait()

    def wait(self, drain: bool = False) -> None:
        while self.awaiting and (
            (len(self.awaiting) > MAX_CONCURRENCY_PER_WORKER // 2) or drain
        ):
            for i in reversed(
                range(
                    (MAX_CONCURRENCY_PER_WORKER // 2 + 1)
                    if not drain
                    else len(self.awaiting)
                )
            ):
                self.awaiting.pop(i).result()
                self.job_notifier.send(True)


class StorageWriteStreamWorker:
    def __init__(
        self,
        parent: str,
        template: types.AppendRowsRequest,
        queue: Queue,
        credentials: BigQueryCredentials,
        job_notifier: Connection,
        stream_notifier: Connection,
    ):
        super().__init__()
        self.parent: str = parent
        self.template: types.AppendRowsRequest = template
        self.queue: Queue = queue
        self.credentials: BigQueryCredentials = credentials
        self.awaiting: List[concurrent.futures.Future] = []
        self.job_notifier = job_notifier
        self.stream_notifier = stream_notifier

    def run(self):
        client: BigQueryWriteClient = storage_client_factory(self.credentials)
        _, write_stream, append_rows_stream = get_default_stream(
            client,
            self.parent,
            self.template,
        )
        self.stream_notifier.send(write_stream.name)
        while True:
            payload: Optional[types.ProtoRows] = self.queue.get()
            if payload is None:
                self.wait(drain=True)
                append_rows_stream.close()
                break
            try:
                retry(
                    append_rows_stream.send,
                    retry=retry_if_exception_type((Unknown, NotFound)),
                    wait=wait_fixed(1),
                    stop=stop_after_delay(5),
                    reraise=True,
                )(make_request(payload))
            except Exception as exc:
                self.wait(drain=True)
                append_rows_stream.close()
                self.queue.put(payload)
                raise exc
            if len(self.awaiting) > MAX_CONCURRENCY_PER_WORKER:
                self.wait()

    def wait(self, drain: bool = False) -> None:
        while self.awaiting and (
            (len(self.awaiting) > MAX_CONCURRENCY_PER_WORKER // 2) or drain
        ):
            for i in reversed(
                range(
                    (MAX_CONCURRENCY_PER_WORKER // 2 + 1)
                    if not drain
                    else len(self.awaiting)
                )
            ):
                self.awaiting.pop(i).result()
                self.job_notifier.send(True)


StorageWriteThreadStreamWorker = type(
    "StorageWriteThreadStreamWorker", (StorageWriteStreamWorker, _Thread), {}
)
StorageWriteProcessStreamWorker = type(
    "StorageWriteProcessStreamWorker", (StorageWriteStreamWorker, Process), {}
)
StorageWriteThreadBatchWorker = type(
    "StorageWriteThreadBatchWorker", (StorageWriteBatchWorker, _Thread), {}
)
StorageWriteProcessBatchWorker = type(
    "StorageWriteProcessBatchWorker", (StorageWriteBatchWorker, Process), {}
)

if TYPE_CHECKING:
    _StorageWriteWorker = Union[
        StorageWriteThreadStreamWorker,
        StorageWriteProcessStreamWorker,
        StorageWriteThreadBatchWorker,
        StorageWriteProcessBatchWorker,
    ]


class BigQueryStorageWriteSink(BaseBigQuerySink):
    worker_factory: Callable[[], "_StorageWriteWorker"]

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
        self.workers: List["_StorageWriteWorker"] = []
        self.open_streams: Set[str] = set()
        self.parent = BigQueryWriteClient.table_path(
            self.table.project,
            self.table.dataset,
            self.table.table,
        )
        self.job_notification, self.job_notifier = pipe(False)
        self.stream_notification, self.stream_notifier = pipe(False)

        if self.config.get("options", {}).get("storage_write_batch_mode", False):
            self.logger.info(
                "Using storage write application created streams (batching)"
            )
            self.stream_typ = StreamType.BATCH
        else:
            self.logger.info("Using storage write _default stream (streaming)")
            self.stream_typ = StreamType.STREAM

        worker_cls = f"StorageWrite{self.par_typ}{self.stream_typ}Worker"
        self.logger.info(f"Configured Sink Class: {worker_cls}")
        self.worker_cls: Type["_StorageWriteWorker"] = globals()[worker_cls]

        def worker_factory() -> "_StorageWriteWorker":
            return self.worker_cls(
                self.parent,
                self.jit.generate_template(),
                self.queue,
                self._credentials,
                self.job_notifier,
                self.stream_notifier,
            )

        self.worker_factory = worker_factory
        worker = self.worker_factory()
        worker.start()
        self.workers.append(worker)

        self._last_worker_creation = time.time()
        self._jobs_enqueued = 0

    @property
    def jit(self) -> ProtoJIT:
        if not hasattr(self, "_jit"):
            self._jit = ProtoJIT.fixed_schema(self.stream_name)
        return self._jit

    def start_batch(self, context: Dict[str, Any]) -> None:
        self.proto_rows = types.ProtoRows()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(record["data"]).decode("utf-8")
        return record

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.proto_rows.serialized_rows.append(
            json_format.ParseDict(record, self.jit.message()).SerializeToString()
        )

    def process_batch(self, context: Dict[str, Any]) -> None:
        self.ensure_workers()
        while self.job_notification.poll():
            self.job_notification.recv()
            self._jobs_enqueued -= 1
        while self.stream_notification.poll():
            stream_name = self.stream_notification.recv()
            self.logger.info("New stream %s", stream_name)
            self.open_streams.add(stream_name)
        self.queue.put(self.proto_rows)
        self._jobs_enqueued += 1

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

    def commit_streams(self) -> None:
        for worker in self.workers:
            if worker.is_alive():
                self.queue.put(None)
        for worker in self.workers:
            worker.join()
        # Anything in the queue at this point can be considered now a DLQ
        if self.stream_typ == "Stream":
            self.logger.info(
                f"Writes to table '{self.table.table}' have been streamed."
            )
        else:
            committer = storage_client_factory(self._credentials)
            for stream in self.open_streams:
                committer.finalize_write_stream(name=stream)
            write = committer.batch_commit_write_streams(
                types.BatchCommitWriteStreamsRequest(
                    parent=self.parent, write_streams=list(self.open_streams)
                )
            )
            self.logger.info(f"Batch commit time: {write.commit_time}")
            self.logger.info(f"Batch commit errors: {write.stream_errors}")
            self.logger.info(
                f"Writes to streams: '{self.open_streams}' have been committed."
            )
        self.open_streams = set()

    def clean_up(self) -> None:
        self.commit_streams()

    def pre_state_hook(self) -> None:
        self.commit_streams()


class BigQueryStorageWriteDenormalizedSink(DenormalizedSink, BigQueryStorageWriteSink):
    @property
    def jit(self) -> ProtoJIT:
        if not hasattr(self, "_jit"):
            self._jit = ProtoJIT(
                self.table.get_schema(self.apply_transforms), self.stream_name
            )
        return self._jit
