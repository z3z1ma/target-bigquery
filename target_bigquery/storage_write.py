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

import logging
import math
import os
from collections.abc import Callable, Iterable
from decimal import Decimal
from multiprocessing import Process
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from time import sleep
from typing import (
    Any,
    NamedTuple,
    cast,
)

import orjson
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery_storage_v1 import BigQueryWriteClient, exceptions, types, writer
from google.protobuf import json_format, message

from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    Denormalized,
    IngestionStrategy,
    make_json_compatible,
    storage_client_factory,
)
from target_bigquery.proto_gen import proto_schema_factory_v2

logger = logging.getLogger(__name__)

# Stream specific constant
MAX_IN_FLIGHT = 15
"""Maximum number of concurrent requests per worker be processed by grpc before awaiting."""

Dispatcher = Callable[[types.AppendRowsRequest], writer.AppendRowsFuture]
StreamComponents = tuple[str, writer.AppendRowsStream, Dispatcher]


class AppendDispatchError(RuntimeError):
    """An append may have reached the server before dispatch raised."""


class PendingAppend(NamedTuple):
    future: writer.AppendRowsFuture
    stream: str
    timeout: float
    offset: int | None
    row_count: int


def _completion_timeout(value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError("timeout must be a positive finite number of seconds")
    return value


def get_application_stream(client: BigQueryWriteClient, job: "Job") -> StreamComponents:
    """Get an application created stream for the parent. This stream must be finalized and committed."""
    write_stream = types.WriteStream()
    cast(Any, write_stream).type_ = types.WriteStream.Type.PENDING
    write_stream = client.create_write_stream(parent=job.parent, write_stream=write_stream)
    job.template.write_stream = write_stream.name
    append_rows_stream = writer.AppendRowsStream(client, job.template)
    rv = (write_stream.name, append_rows_stream)
    job.stream_notifier.send(rv[0])
    return *rv, append_rows_stream.send


def get_default_stream(client: BigQueryWriteClient, job: "Job") -> StreamComponents:
    """Get the default storage write API stream for the parent."""
    job.template.write_stream = BigQueryWriteClient.write_stream_path(
        **BigQueryWriteClient.parse_table_path(job.parent), stream="_default"
    )
    append_rows_stream = writer.AppendRowsStream(client, job.template)
    rv = (job.template.write_stream, append_rows_stream)
    job.stream_notifier.send(rv[0])
    return *rv, append_rows_stream.send


def generate_request(
    payload: types.ProtoRows,
    offset: int | None = None,
    path: str | None = None,
) -> types.AppendRowsRequest:
    """Generate a request for the storage write API from a payload."""
    request = types.AppendRowsRequest()
    if offset is not None:
        request.offset = int(offset)  # type: ignore
    if path is not None:
        request.write_stream = path
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = payload
    request.proto_rows = proto_data
    return request


def generate_template(proto_message: type[Any]) -> types.AppendRowsRequest:
    """Generate a template for the storage write API from a proto message class."""
    from google.protobuf import descriptor_pb2

    template, proto_schema, proto_descriptor, proto_data = (
        types.AppendRowsRequest(),
        types.ProtoSchema(),
        descriptor_pb2.DescriptorProto(),
        types.AppendRowsRequest.ProtoData(),
    )
    proto_message.DESCRIPTOR.CopyToProto(proto_descriptor)
    _localize_nested_type_names(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor
    proto_data.writer_schema = proto_schema
    template.proto_rows = proto_data
    return template


def _localize_nested_type_names(descriptor: Any) -> None:
    """Rewrite nested message type names for BigQuery's DescriptorProto-only schema."""
    nested_names = {nested.name for nested in descriptor.nested_type}
    for field in descriptor.field:
        if not field.type_name:
            continue
        for nested_name in nested_names:
            if field.type_name == nested_name or field.type_name.endswith(f".{nested_name}"):
                field.type_name = nested_name
                break
    for nested in descriptor.nested_type:
        _localize_nested_type_names(nested)


def _normalize_json_value(value: Any) -> Any:
    """Normalize Decimal values without silently replacing non-finite numbers."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("BigQuery JSON does not support non-finite numbers")
        value = make_json_compatible(value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("BigQuery JSON does not support non-finite numbers")
    if isinstance(value, dict):
        return {key: _normalize_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item) for item in value]
    return value


def serialize_json_fields(record: dict[str, Any], schema: Iterable[SchemaField]) -> dict[str, Any]:
    """Copy record containers and encode JSON fields as protobuf strings."""
    record = record.copy()
    for field in schema:
        value = record.get(field.name)
        if value is None:
            continue
        if (
            field.mode.upper() == "REPEATED"
            and field.field_type.upper() in {"JSON", "RECORD"}
            and not isinstance(value, (list, tuple))
        ):
            raise TypeError(f"Repeated field {field.name!r} requires an array")
        if field.field_type.upper() == "JSON":
            if field.mode.upper() == "REPEATED":
                record[field.name] = [
                    orjson.dumps(_normalize_json_value(item)).decode("utf-8") for item in value
                ]
            else:
                record[field.name] = orjson.dumps(_normalize_json_value(value)).decode("utf-8")
        elif field.field_type.upper() == "RECORD":
            if field.mode.upper() == "REPEATED":
                record[field.name] = [
                    serialize_json_fields(item, field.fields) if isinstance(item, dict) else item
                    for item in value
                ]
            elif isinstance(value, dict):
                record[field.name] = serialize_json_fields(value, field.fields)
    return record


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
        timeout: float = 600,
    ) -> None:
        self.timeout = _completion_timeout(timeout)
        self.parent = parent
        self.template = template
        self.stream_notifier = stream_notifier
        self.data = data
        self.attempts = 1


class StorageWriteBatchWorker(BaseWorker):
    """Worker process for the storage write API."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.get_stream_components = get_application_stream
        self.awaiting: list[PendingAppend] = []
        self.cache: dict[str, StreamComponents] = {}
        self.max_errors_before_recycle = 5
        self.offsets: dict[str, int] = {}
        self.logger = logger

    def run(self) -> None:
        """Run the worker process."""
        client: BigQueryWriteClient = storage_client_factory(self.credentials)
        self._configure_bidi_logging()
        while True:
            job = self._receive_job()
            if job is None:
                break
            try:
                while True:
                    try:
                        write_stream = self._send_job(client, job)
                    except Exception as exc:
                        if self._handle_dispatch_error(job, exc):
                            continue
                    else:
                        self._record_dispatch_success(job, write_stream)
                    break
            finally:
                self.mark_job_done()
        self._finish()
        self.normal_completion.set()

    @staticmethod
    def _configure_bidi_logging() -> None:
        """Configure bidi logging from the target debug environment variable."""
        bidi_logger = logging.getLogger("google.api_core.bidi")
        if os.getenv("TARGET_BIGQUERY_DEBUG", "false").lower() == "true":
            bidi_logger.setLevel(logging.DEBUG)
        else:
            bidi_logger.setLevel(logging.INFO)

    def _receive_job(self) -> Job | None:
        """Return the next queued job, or None when the worker should exit."""
        try:
            return self.queue.get(timeout=30.0)
        except Empty:
            return None

    def _send_job(self, client: BigQueryWriteClient, job: Job) -> str:
        """Send a job to the storage write API and return the stream path."""
        write_stream, _, dispatch = self._get_stream_components_for_job(client, job)
        request = self._generate_request_for_job(job, write_stream)
        offset = None if write_stream.endswith("_default") else self.offsets[job.parent]
        row_count = len(job.data.serialized_rows)
        identity = f"stream={write_stream}, offset={offset}, row_count={row_count}"
        self.logger.info("Submitting Storage Write append (%s).", identity)
        try:
            future = dispatch(request)
        except Exception as exc:
            raise AppendDispatchError(
                f"Storage Write dispatch failed for {identity}. "
                f"Remote completion may be unknown; reconcile before replay. {exc}"
            ) from exc
        self.awaiting.append(PendingAppend(future, write_stream, job.timeout, offset, row_count))
        return write_stream

    def _get_stream_components_for_job(
        self, client: BigQueryWriteClient, job: Job
    ) -> StreamComponents:
        """Return cached stream components for the job, refreshing closed streams."""
        components = self.cache.get(job.parent)
        if components is None or components[1]._closed:
            components = self.get_stream_components(client, job)
            self.cache[job.parent] = components
            self.offsets[job.parent] = 0
        return components

    def _generate_request_for_job(self, job: Job, write_stream: str) -> types.AppendRowsRequest:
        """Generate the append request for the target stream."""
        if write_stream.endswith("_default"):
            return generate_request(job.data, path=write_stream)
        return generate_request(job.data, offset=self.offsets[job.parent])

    def _handle_dispatch_error(self, job: Job, exc: Exception) -> bool:
        """Retry setup failures; stop after any potentially accepted dispatch."""
        if isinstance(exc, AppendDispatchError):
            self.error_notifier.send((exc, self.serialize_exception(exc)))
            self.close_cached_streams()
            raise exc
        job.attempts += 1
        self.logger.info(f"job.attempts : {job.attempts}")
        self.max_errors_before_recycle -= 1
        if job.attempts > 3:
            # TODO: add a metric for this + a DLQ & wrap exception type
            self.error_notifier.send((exc, self.serialize_exception(exc)))
        # Track errors and recycle the stream if we hit a threshold
        # 1 bad payload is not indicative of a bad bidi stream as it could
        # be a transient error or luck of the draw with the first payload.
        if self.max_errors_before_recycle == 0:
            self.wait(drain=True)
            self.close_cached_streams()
            raise exc
        return job.attempts <= 3

    def _record_dispatch_success(self, job: Job, write_stream: str) -> None:
        """Record a successful append dispatch and apply backpressure."""
        self.log_notifier.send(
            f"[{self.ext_id}] Sent {len(job.data.serialized_rows)} rows to {write_stream}"
            f" with offset {self.offsets[job.parent]}."
        )
        self.offsets[job.parent] += len(job.data.serialized_rows)
        if len(self.awaiting) > MAX_IN_FLIGHT:
            self.wait()

    def _finish(self) -> None:
        """Drain in-flight writes and close cached streams before exit."""
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
            except exceptions.StreamClosedError:
                # The Storage Write client may already close streams during shutdown.
                pass
            except Exception as exc:
                self.error_notifier.send((exc, self.serialize_exception(exc)))

    def wait(self, drain: bool = False) -> None:
        """Wait for in-flight requests to complete."""
        while self.awaiting and ((len(self.awaiting) > MAX_IN_FLIGHT // 2) or drain):
            pending = self.awaiting.pop(0)
            try:
                response = pending.future.result(timeout=pending.timeout)
                self._raise_for_append_response(response)
                self._validate_append_identity(pending, response)
            except Exception as exc:
                diagnostic = (
                    f"Storage Write append failed for stream={pending.stream}, "
                    f"offset={pending.offset}, row_count={pending.row_count}. "
                    "Remote completion may be unknown; reconcile before replay.\n"
                    + self.serialize_exception(exc)
                )
                self.error_notifier.send((exc, diagnostic))
            finally:
                # This counts completed local work, not durable delivery.
                self.job_notifier.send(True)

    @staticmethod
    def _validate_append_identity(
        pending: PendingAppend, response: types.AppendRowsResponse | None
    ) -> None:
        if (
            response is not None
            and response.write_stream
            and response.write_stream != pending.stream
        ):
            raise RuntimeError(
                f"Storage Write response stream mismatch: expected {pending.stream}, "
                f"received {response.write_stream}"
            )
        if pending.offset is not None:
            actual = response.append_result.offset if response is not None else None
            if actual != pending.offset:
                raise RuntimeError(
                    f"Storage Write response offset mismatch: expected {pending.offset}, "
                    f"received {actual}"
                )

    @staticmethod
    def _raise_for_append_response(response: types.AppendRowsResponse | None) -> None:
        """Raise when Storage Write accepts the RPC but rejects rows."""
        if response is None:
            return
        if response.error.code:
            raise RuntimeError(f"Storage Write append failed: {response.error.message}")
        if response.row_errors:
            errors = "; ".join(
                f"row {error.index}: {error.message}" for error in response.row_errors
            )
            raise RuntimeError(f"Storage Write append row errors: {errors}")


class StorageWriteStreamWorker(StorageWriteBatchWorker):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
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
        worker_executor_cls: type[Process], config: dict[str, Any]
    ) -> type[
        StorageWriteThreadStreamWorker
        | StorageWriteProcessStreamWorker
        | StorageWriteThreadBatchWorker
        | StorageWriteProcessBatchWorker
    ]:
        batch_mode = config.get("options", {}).get("storage_write_batch_mode", False)
        if worker_executor_cls is Process:
            return StorageWriteProcessBatchWorker if batch_mode else StorageWriteProcessStreamWorker
        if worker_executor_cls is _Thread:
            return StorageWriteThreadBatchWorker if batch_mode else StorageWriteThreadStreamWorker
        base = StorageWriteBatchWorker if batch_mode else StorageWriteStreamWorker
        Worker = type("Worker", (base, worker_executor_cls), {})
        return cast(type[StorageWriteThreadStreamWorker], Worker)

    def __init__(
        self,
        target: Any,
        stream_name: str,
        schema: dict[str, Any],
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.open_streams: set[str] = set()
        self.parent = BigQueryWriteClient.table_path(
            self.table.project,
            self.table.dataset,
            self.table.name,
        )
        self.stream_notification, self.stream_notifier = target.pipe_cls(False)
        self._resolved_schema = self.table.get_resolved_schema(self.apply_transforms)
        self.template = generate_template(self.proto_schema)

    @property
    def resolved_schema(self) -> list[SchemaField]:
        """Return the schema used for this sink's lifetime."""
        if not hasattr(self, "_resolved_schema"):
            self._resolved_schema = self.table.get_resolved_schema(self.apply_transforms)
        return self._resolved_schema

    @property
    def proto_schema(self) -> type[message.Message]:
        if not hasattr(self, "_proto_schema"):
            self._proto_schema = proto_schema_factory_v2(self.resolved_schema)
        return cast(type[message.Message], self._proto_schema)

    def start_batch(self, context: dict[str, Any]) -> None:
        self.proto_rows = types.ProtoRows()

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(make_json_compatible(record["data"])).decode("utf-8")
        return record

    def process_record(self, record: dict[str, Any], context: dict[str, Any]) -> None:
        if self.ingestion_strategy is IngestionStrategy.DENORMALIZED:
            record = serialize_json_fields(
                record,
                self.resolved_schema,
            )
        self.proto_rows.serialized_rows.append(
            json_format.ParseDict(record, self.proto_schema()).SerializeToString()
        )

    def process_batch(self, context: dict[str, Any]) -> None:
        while self.global_queue.qsize() >= self.MAX_JOBS_QUEUED:
            self.logger.warning(f"Max jobs enqueued reached ({self.MAX_JOBS_QUEUED})")
            sleep(1)

        self.global_queue.put(
            Job(
                parent=self.parent,
                template=self.template,
                data=self.proto_rows,
                stream_notifier=self.stream_notifier,
                timeout=self.config.get("timeout", 600),
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
        self.open_streams = {name for name in self.open_streams if not name.endswith("_default")}
        if self.open_streams:
            timeout = _completion_timeout(self.config.get("timeout", 600))
            committer = storage_client_factory(self._credentials)
            for name in sorted(self.open_streams):
                self.logger.info("Finalizing Storage Write stream=%s", name)
                committer.finalize_write_stream(name=name, timeout=timeout, retry=None)
            self.logger.info(
                "Committing Storage Write parent=%s streams=%s",
                self.parent,
                sorted(self.open_streams),
            )
            write = committer.batch_commit_write_streams(
                types.BatchCommitWriteStreamsRequest(
                    parent=self.parent,
                    write_streams=sorted(self.open_streams),
                ),
                timeout=timeout,
                retry=None,
            )
            if write.stream_errors:
                raise RuntimeError(f"Storage Write batch commit failed: {write.stream_errors}")
            self.logger.info(f"Batch commit time: {write.commit_time}")
            self.logger.info(f"Batch commit errors: {write.stream_errors}")
            self.logger.info(f"Writes to streams: '{self.open_streams}' have been committed.")
        self.open_streams = set()

    def clean_up(self) -> None:
        self.commit_streams()
        super().clean_up()

    def pre_state_hook(self) -> None:
        self.commit_streams()
        super().pre_state_hook()
        # A successful merge rotates the staging table. New jobs must use it,
        # rather than the old table which merge_table has dropped.
        self.parent = BigQueryWriteClient.table_path(
            self.table.project, self.table.dataset, self.table.name
        )


class BigQueryStorageWriteDenormalizedSink(Denormalized, BigQueryStorageWriteSink):
    pass
