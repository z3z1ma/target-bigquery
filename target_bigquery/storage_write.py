# Copyright (c) 2023 Alex Butler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
"""BigQuery Storage Write Sink.
Throughput test: 11m 0s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import os
from multiprocessing import Process
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import orjson
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types, writer
from google.protobuf import json_format
from proto import Message
from tenacity import retry, stop_after_attempt, wait_fixed

if TYPE_CHECKING:
    from target_bigquery.target import TargetBigQuery

import logging

from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    Denormalized,
    storage_client_factory,
)
from target_bigquery.proto_gen import proto_schema_factory_v2

# Stream specific constant
MAX_IN_FLIGHT = 15
"""Maximum number of concurrent requests per worker be processed by grpc before awaiting."""


Dispatcher = Callable[[types.AppendRowsRequest], writer.AppendRowsFuture]
StreamComponents = Tuple[str, writer.AppendRowsStream, Dispatcher]


def get_application_stream(client: BigQueryWriteClient, job: "Job") -> StreamComponents:
    """Get an application created stream for the parent. This stream must be finalized and committed."""
    write_stream = types.WriteStream()
    write_stream.type_ = types.WriteStream.Type.PENDING
    write_stream = client.create_write_stream(
        parent=job.parent, write_stream=write_stream
    )
    job.template.write_stream = write_stream.name
    append_rows_stream = writer.AppendRowsStream(client, job.template)
    rv = (write_stream.name, append_rows_stream)
    job.stream_notifier.send(rv)
    return *rv, retry(
        append_rows_stream.send,
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
        reraise=True,
    )


def get_default_stream(client: BigQueryWriteClient, job: "Job") -> StreamComponents:
    """Get the default storage write API stream for the parent."""
    job.template.write_stream = BigQueryWriteClient.write_stream_path(
        **BigQueryWriteClient.parse_table_path(job.parent), stream="_default"
    )
    append_rows_stream = writer.AppendRowsStream(client, job.template)
    rv = (job.template.write_stream, append_rows_stream)
    job.stream_notifier.send(rv)
    return *rv, retry(
        append_rows_stream.send,
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
        reraise=True,
    )


def generate_request(
    payload: types.ProtoRows,
    offset: Optional[int] = None,
    path: Optional[str] = None,
) -> types.AppendRowsRequest:
    """Generate a request for the storage write API from a payload."""
    request = types.AppendRowsRequest()
    if offset is not None:
        request.offset = int(offset)
    if path is not None:
        request.write_stream = path
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = payload
    request.proto_rows = proto_data
    return request


def generate_template(message: Type[Message]):
    """Generate a template for the storage write API from a proto message class."""
    from google.protobuf import descriptor_pb2

    template, proto_schema, proto_descriptor, proto_data = (
        types.AppendRowsRequest(),
        types.ProtoSchema(),
        descriptor_pb2.DescriptorProto(),
        types.AppendRowsRequest.ProtoData(),
    )
    message.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor
    proto_data.writer_schema = proto_schema
    template.proto_rows = proto_data
    return template


class Job(NamedTuple):
    parent: str
    template: types.AppendRowsRequest
    stream_notifier: Connection
    data: types.ProtoRows


class StorageWriteBatchWorker(BaseWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_stream_components = get_application_stream
        self.awaiting: List[writer.AppendRowsFuture] = []

    def run(self):
        client: BigQueryWriteClient = storage_client_factory(self.credentials)
        cache: Dict[str, StreamComponents] = {}
        offset = 0
        if os.getenv("TARGET_BIGQUERY_DEBUG", "false").lower() == "true":
            bidi_logger = logging.getLogger("google.api_core.bidi")
            bidi_logger.setLevel(logging.DEBUG)
        while True:
            try:
                job: Optional[Job] = self.queue.get(timeout=60.0)
            except Empty:
                break
            if job is None:
                break
            if job.parent not in cache:
                cache[job.parent] = self.get_stream_components(client, job)
            write_stream, append_rows_stream, dispatch = cast(
                StreamComponents, cache[job.parent]
            )
            try:
                kwargs = {}
                if write_stream.endswith("_default"):
                    kwargs["offset"] = None
                    kwargs["path"] = write_stream
                else:
                    kwargs["offset"] = offset
                self.awaiting.append(dispatch(generate_request(job.data, **kwargs)))
            except Exception as exc:
                self.wait(drain=True)
                append_rows_stream.close()
                self.queue.put(job)
                raise exc
            offset += len(job.data.serialized_rows)
            if len(self.awaiting) > MAX_IN_FLIGHT:
                self.wait()
        self.wait(drain=True)
        for _, stream, _ in cache.values():
            stream.close()

    def wait(self, drain: bool = False) -> None:
        while self.awaiting and ((len(self.awaiting) > MAX_IN_FLIGHT // 2) or drain):
            self.awaiting.pop(0).result()
            self.job_notifier.send(True)


class StorageWriteStreamWorker(StorageWriteBatchWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_stream_components = get_default_stream


class StorageWriteThreadStreamWorker(StorageWriteStreamWorker, _Thread):
    pass


class StorageWriteProcessStreamWorker(StorageWriteStreamWorker, Process):
    pass


class StorageWriteThreadBatchWorker(StorageWriteBatchWorker, _Thread):
    pass


class StorageWriteProcessBatchWorker(StorageWriteBatchWorker, Process):
    pass


class BigQueryStorageWriteSink(BaseBigQuerySink):

    MAX_WORKERS = os.cpu_count() * 2
    WORKER_CAPACITY_FACTOR = 10
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
            Worker = type("Worker", (StorageWriteBatchWorker, worker_executor_cls), {})
        else:
            Worker = type("Worker", (StorageWriteStreamWorker, worker_executor_cls), {})
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
            self.table.name,
        )
        self.stream_notification, self.stream_notifier = target.pipe_cls(False)
        self.template = generate_template(self.proto_schema)

    @property
    def proto_schema(self) -> Type[Message]:
        if not hasattr(self, "_proto_schema"):
            self._proto_schema = proto_schema_factory_v2(
                self.table.get_resolved_schema(self.apply_transforms)
            )
        return self._proto_schema

    def start_batch(self, context: Dict[str, Any]) -> None:
        self.proto_rows = types.ProtoRows()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(record["data"]).decode("utf-8")
        return record

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.proto_rows.serialized_rows.append(
            json_format.ParseDict(record, self.proto_schema()).SerializeToString()
        )

    def process_batch(self, context: Dict[str, Any]) -> None:
        self.global_queue.put(
            Job(
                parent=self.parent,
                template=self.template,
                data=self.proto_rows,
                stream_notifier=self.stream_notifier,
            )
        )
        self.increment_jobs_enqueued()

    def commit_streams(self) -> None:
        while self.stream_notification.poll():
            stream_payload = self.stream_notification.recv()
            self.logger.debug("Stream enqueued %s", stream_payload)
            self.open_streams.add(stream_payload)
        if not self.open_streams:
            return
        self.open_streams = [
            (name, stream)
            for name, stream in self.open_streams
            if not name.endswith("_default")
        ]
        if self.open_streams:
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
    pass
