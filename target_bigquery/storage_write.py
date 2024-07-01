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
"""BigQuery Storage Write Sink."""

import os
from multiprocessing import Process
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from time import sleep
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
    cast,
)

import orjson
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, types, writer
from google.protobuf import json_format, message
from proto import Field
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

logger = logging.getLogger(__name__)

# Stream specific constant
MAX_IN_FLIGHT = 15
"""Maximum number of concurrent requests per worker be processed by grpc before awaiting."""

Dispatcher = Callable[[types.AppendRowsRequest], writer.AppendRowsFuture]
StreamComponents = Tuple[Field, writer.AppendRowsStream, Dispatcher]


def get_application_stream(client: BigQueryWriteClient, job: "Job") -> StreamComponents:
    """Get an application created stream for the parent. This stream must be finalized and committed."""
    write_stream = types.WriteStream()
    write_stream.type_ = types.WriteStream.Type.PENDING  # type: ignore
    write_stream = client.create_write_stream(
        parent=job.parent, write_stream=write_stream
    )
    job.template.write_stream = write_stream.name
    append_rows_stream = writer.AppendRowsStream(client, job.template)
    rv = (write_stream.name, append_rows_stream)
    job.stream_notifier.send(rv)
    return *rv, retry(
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
        reraise=True,
    )(append_rows_stream.send)


def get_default_stream(client: BigQueryWriteClient, job: "Job") -> StreamComponents:
    """Get the default storage write API stream for the parent."""
    job.template.write_stream = BigQueryWriteClient.write_stream_path(  # type: ignore
        **BigQueryWriteClient.parse_table_path(job.parent), stream="_default"
    )
    append_rows_stream = writer.AppendRowsStream(client, job.template)
    rv = (job.template.write_stream, append_rows_stream)
    job.stream_notifier.send(rv)
    return *rv, retry(
        wait=wait_fixed(2),
        stop=stop_after_attempt(5),
        reraise=True,
    )(append_rows_stream.send)


def generate_request(
    payload: types.ProtoRows,
    offset: Optional[int] = None,
    path: Optional[str] = None,
) -> types.AppendRowsRequest:
    """Generate a request for the storage write API from a payload."""
    request = types.AppendRowsRequest()
    if offset is not None:
        request.offset = int(offset)  # type: ignore
    if path is not None:
        request.write_stream = path  # type: ignore
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = payload  # type: ignore
    request.proto_rows = proto_data  # type: ignore
    return request


def generate_template(message: Type[message.Message]):
    """Generate a template for the storage write API from a proto message class."""
    from google.protobuf import descriptor_pb2

    template, proto_schema, proto_descriptor, proto_data = (
        types.AppendRowsRequest(),
        types.ProtoSchema(),
        descriptor_pb2.DescriptorProto(),
        types.AppendRowsRequest.ProtoData(),  # type: ignore
    )
    message.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor  # type: ignore
    proto_data.writer_schema = proto_schema  # type: ignore
    template.proto_rows = proto_data  # type: ignore
    return template


class Job:
    """Encapsulate information required to execute a job

    This is safe and sendable meaning it can cross process boundaries.
    """

    def __init__(
        self,
        parent: str,
        template: types.AppendRowsRequest,
        stream_notifier: Connection,
        data: types.ProtoRows,
    ) -> None:
        self.parent = parent
        self.template = template
        self.stream_notifier = stream_notifier
        self.data = data
        self.attempts = 1


class StorageWriteBatchWorker(BaseWorker):
    """Worker process for the storage write API."""

    def __init__(self, *args, **kwargs):
        """Initialize the worker process."""
        super().__init__(*args, **kwargs)
        self.get_stream_components = get_application_stream
        self.awaiting: List[writer.AppendRowsFuture] = []
        self.cache: Dict[str, StreamComponents] = {}
        self.max_errors_before_recycle = 5
        self.offsets: Dict[str, int] = {}
        self.logger = logger

    def run(self):
        """Run the worker process."""
        client: BigQueryWriteClient = storage_client_factory(self.credentials)
        bidi_logger = logging.getLogger("google.api_core.bidi")
        if os.getenv("TARGET_BIGQUERY_DEBUG", "false").lower() == "true":
            bidi_logger.setLevel(logging.DEBUG)
        else:
            bidi_logger.setLevel(logging.INFO)
        while True:
            try:
                job: Optional[Job] = self.queue.get(timeout=30.0)
            except Empty:
                break
            if job is None:
                break
            if job.parent not in self.cache or self.cache[job.parent][1]._closed:
                self.cache[job.parent] = self.get_stream_components(client, job)
                self.offsets[job.parent] = 0
            write_stream, _, dispatch = cast(StreamComponents, self.cache[job.parent])

            try:
                kwargs = {}
                if write_stream.endswith("_default"):  # type: ignore
                    kwargs["offset"] = None
                    kwargs["path"] = write_stream
                else:
                    kwargs["offset"] = self.offsets[job.parent]
                self.awaiting.append(dispatch(generate_request(job.data, **kwargs)))

            except Exception as exc:
                job.attempts += 1
                self.logger.info(f"job.attempts : {job.attempts}")
                self.max_errors_before_recycle -= 1
                if job.attempts > 3:
                    # TODO: add a metric for this + a DLQ & wrap exception type
                    self.error_notifier.send((exc, self.serialize_exception(exc)))
                else:
                    self.queue.put(job)
                # Track errors and recycle the stream if we hit a threshold
                # 1 bad payload 👆 is not indicative of a bad bidi stream as it _could_
                # be a transient error or luck of the draw with the first payload.
                # 5 worker-specific errors is a good threshold to recycle the stream
                # and start fresh. This is an arbitrary number and can be adjusted.
                if self.max_errors_before_recycle == 0:
                    self.wait(drain=True)
                    self.close_cached_streams()
                    raise
            else:
                self.log_notifier.send(
                    f"[{self.ext_id}] Sent {len(job.data.serialized_rows)} rows to {write_stream}"  # type: ignore
                    f" with offset {self.offsets[job.parent]}."
                )
                self.offsets[job.parent] += len(job.data.serialized_rows)  # type: ignore
                if len(self.awaiting) > MAX_IN_FLIGHT:
                    self.wait()
            finally:
                self.queue.task_done()  # type: ignore
        # Wait for all in-flight requests to complete after poison pill
        self.logger.info(f"[{self.ext_id}] : {self.offsets}")
        self.wait(drain=True)
        self.close_cached_streams()
        self.logger.info("Worker process exiting.")
        self.log_notifier.send("Worker process exiting.")

    def close_cached_streams(self) -> None:
        """Close all cached streams."""
        for _, stream, _ in self.cache.values():
            try:
                stream.close()
            except Exception as exc:
                self.error_notifier.send((exc, self.serialize_exception(exc)))

    def wait(self, drain: bool = False) -> None:
        """Wait for in-flight requests to complete."""
        while self.awaiting and ((len(self.awaiting) > MAX_IN_FLIGHT // 2) or drain):
            try:
                self.awaiting.pop(0).result()
            except Exception as exc:
                self.error_notifier.send((exc, self.serialize_exception(exc)))
            finally:
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
    MAX_WORKERS = (os.cpu_count() or 1) * 2
    MAX_JOBS_QUEUED = MAX_WORKERS * 2
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
        return cast(Type[StorageWriteThreadStreamWorker], Worker)

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
    def proto_schema(self) -> Type[message.Message]:
        if not hasattr(self, "_proto_schema"):
            self._proto_schema = proto_schema_factory_v2(
                self.table.get_resolved_schema(self.apply_transforms)
            )
        return cast(Type[message.Message], self._proto_schema)

    def start_batch(self, context: Dict[str, Any]) -> None:
        self.proto_rows = types.ProtoRows()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(record["data"]).decode("utf-8")
        return record

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.proto_rows.serialized_rows.append(  # type: ignore
            json_format.ParseDict(record, self.proto_schema()).SerializeToString()
        )

    def process_batch(self, context: Dict[str, Any]) -> None:
        while self.global_queue.qsize() >= self.MAX_JOBS_QUEUED:
            self.logger.warn(f"Max jobs enqueued reached ({self.MAX_JOBS_QUEUED})")
            sleep(1)

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
            if not name.endswith("_default")  # type: ignore
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
        super().clean_up()

    def pre_state_hook(self) -> None:
        self.commit_streams()


class BigQueryStorageWriteDenormalizedSink(Denormalized, BigQueryStorageWriteSink):
    pass
