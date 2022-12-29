"""BigQuery Storage Write Sink.
Throughput test: 11m 0s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import concurrent.futures
from enum import Enum
from multiprocessing import Process
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    NamedTuple,
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
    from target_bigquery.target import TargetBigQuery

from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    Denormalized,
    ProtoJIT,
    storage_client_factory,
)

# Stream specific constant
MAX_IN_FLIGHT = 50
"""Maximum number of concurrent requests per worker be processed by grpc before awaiting."""


def make_append_rows_stream(
    client: BigQueryWriteClient, parent: str, template: types.AppendRowsRequest
) -> Tuple[types.WriteStream, writer.AppendRowsStream]:
    write_stream = types.WriteStream()
    write_stream.type_ = types.WriteStream.Type.PENDING

    write_stream = client.create_write_stream(parent=parent, write_stream=write_stream)
    template.write_stream = write_stream.name
    append_rows_stream = writer.AppendRowsStream(client, template)

    return write_stream, append_rows_stream


def get_default_stream(
    client: BigQueryWriteClient, parent: str, template: types.AppendRowsRequest
) -> Tuple[types.WriteStream, writer.AppendRowsStream]:
    write_stream = client.get_write_stream(name=parent + "/streams/_default")

    template.write_stream = write_stream.name
    append_rows_stream = writer.AppendRowsStream(client, template)

    return write_stream, append_rows_stream


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


class Job(NamedTuple):
    parent: str
    template: types.AppendRowsRequest
    stream_notifier: Connection
    data: types.ProtoRows


class StorageWriteBatchWorker(BaseWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_stream = make_append_rows_stream
        self.awaiting: List[concurrent.futures.Future] = []
        self._offset = 0

    @property
    def offset(self) -> int:
        return self._offset

    @offset.setter
    def offset(self, value: int) -> None:
        self._offset = value

    def run(self):
        client: BigQueryWriteClient = storage_client_factory(self.credentials)
        stream = {}
        while True:
            try:
                job: Optional[Job] = self.queue.get(timeout=60.0)
            except Empty:
                break
            if job is None:
                break
            if not stream:
                (
                    stream["write_stream"],
                    stream["append_rows_stream"],
                ) = self.get_stream(
                    client,
                    job.parent,
                    job.template,
                )
                job.stream_notifier.send(
                    (stream["write_stream"].name, stream["append_rows_stream"])
                )
            try:
                self.awaiting.append(
                    retry(
                        stream["append_rows_stream"].send,
                        retry=retry_if_exception_type((Unknown, NotFound)),
                        wait=wait_fixed(1),
                        stop=stop_after_delay(5),
                        reraise=True,
                    )(make_request(job.data, self.offset))
                )
            except Exception as exc:
                self.wait(drain=True)
                stream["append_rows_stream"].close()
                self.queue.put(job)
                raise exc
            self.offset += len(job.data.serialized_rows)
            if len(self.awaiting) > MAX_IN_FLIGHT:
                self.wait()
        if stream:
            self.wait(drain=True)
            stream["append_rows_stream"].close()

    def wait(self, drain: bool = False) -> None:
        while self.awaiting and ((len(self.awaiting) > MAX_IN_FLIGHT // 2) or drain):
            for i in reversed(
                range((MAX_IN_FLIGHT // 2 + 1) if not drain else len(self.awaiting))
            ):
                self.awaiting.pop(i).result()
                self.job_notifier.send(True)


class StorageWriteStreamWorker(StorageWriteBatchWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_stream = get_default_stream

    @property
    def offset(self) -> None:
        return None

    @offset.setter
    def offset(self, value: int) -> None:
        _ = value


class StorageWriteThreadStreamWorker(StorageWriteStreamWorker, _Thread):
    pass


class StorageWriteProcessStreamWorker(StorageWriteStreamWorker, Process):
    pass


class StorageWriteThreadBatchWorker(StorageWriteBatchWorker, _Thread):
    pass


class StorageWriteProcessBatchWorker(StorageWriteBatchWorker, Process):
    pass


class BigQueryStorageWriteSink(BaseBigQuerySink):

    MAX_WORKERS = 50
    WORKER_CAPACITY_FACTOR = 2
    WORKER_CREATION_MIN_INTERVAL = 1.0

    @staticmethod
    def worker_cls_factory(
        worker_executor_cls: Type[Process], config: Dict[str, Any]
    ) -> Type[
        Union[
            StorageWriteThreadStreamWorker,
            StorageWriteProcessStreamWorker,
            StorageWriteThreadBatchWorker,
            StorageWriteProcessBatchWorker,
        ]
    ]:
        if config.get("options", {}).get("storage_write_batch_mode", False):
            Worker = type("Worker", (StorageWriteStreamWorker, worker_executor_cls), {})
        else:
            Worker = type("Worker", (StorageWriteBatchWorker, worker_executor_cls), {})
        return Worker

    def __init__(
        self,
        target: "TargetBigQuery",
        stream_name: str,
        schema: Dict[str, Any],
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.open_streams: Set[Tuple[str, writer.AppendRowsStream]] = set()
        self.parent = BigQueryWriteClient.table_path(
            self.table.project,
            self.table.dataset,
            self.table.table,
        )
        self.stream_notification, self.stream_notifier = target.pipe_cls(False)

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
        self.global_queue.put(
            Job(
                parent=self.parent,
                template=self.jit.generate_template(),
                data=self.proto_rows,
                stream_notifier=self.stream_notifier,
            )
        )
        self.increment_jobs_enqueued()

    def commit_streams(self) -> None:
        while self.stream_notification.poll():
            stream_name = self.stream_notification.recv()
            self.logger.info("New stream %s", stream_name)
            self.open_streams.add(stream_name)
        if not self.open_streams:
            return
        if all(name == "_default" for name, _ in self.open_streams):
            self.logger.info(
                f"Writes to table '{self.table.table}' have been streamed."
            )
        else:
            committer = storage_client_factory(self._credentials)
            for name, stream in self.open_streams:
                stream.close()
                committer.finalize_write_stream(name=name)
            write = committer.batch_commit_write_streams(
                types.BatchCommitWriteStreamsRequest(
                    parent=self.parent,
                    write_streams=[name for name, _ in self.open_streams],
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


class BigQueryStorageWriteDenormalizedSink(Denormalized, BigQueryStorageWriteSink):
    @property
    def jit(self) -> ProtoJIT:
        if not hasattr(self, "_jit"):
            self._jit = ProtoJIT(
                self.table.get_resolved_schema(self.apply_transforms), self.stream_name
            )
        return self._jit
