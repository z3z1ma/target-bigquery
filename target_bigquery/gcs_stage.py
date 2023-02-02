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
"""BigQuery GCS Staging Sink.
Throughput test: 6m 30s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import os
import shutil
import time
from io import BytesIO
from multiprocessing import Process
from multiprocessing.connection import Connection
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from typing import TYPE_CHECKING, Any, Dict, List, NamedTuple, Optional, Type, Union

import orjson
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage

from target_bigquery.constants import DEFAULT_BUCKET_PATH
from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    Compressor,
    Denormalized,
    ParType,
    bigquery_client_factory,
    gcs_client_factory, BigQueryCredentials,
)

if TYPE_CHECKING:
    from target_bigquery.target import TargetBigQuery


class Job(NamedTuple):
    """Job to be processed by a worker."""

    buffer: Union[memoryview, bytes]
    batch_id: str
    table: str
    dataset: str
    bucket: str
    gcs_notifier: Connection


class GcsStagingWorker(BaseWorker):
    def run(self):
        client: storage.Client = gcs_client_factory(self.credentials)
        # client.
        while True:
            try:
                job: Optional[Job] = self.queue.get(timeout=30.0)
            except Empty:
                break
            if job is None:
                break
            try:
                # TODO: consider configurability?
                path = DEFAULT_BUCKET_PATH.format(
                    bucket=job.bucket,
                    dataset=job.dataset,
                    table=job.table,
                    date=time.strftime("%Y-%m-%d"),
                    batch_id=job.batch_id,
                )
                blob = storage.Blob.from_string(path, client=client)
                # TODO: pass in timeout?
                # TODO: composite uploads
                with blob.open(
                    "wb",
                    if_generation_match=0,
                    chunk_size=1024 * 1024 * 10,
                    timeout=300,
                ) as fh:
                    shutil.copyfileobj(BytesIO(job.buffer), fh)
                job.gcs_notifier.send(path)
            except Exception as exc:
                self.queue.put(job)
                raise exc
            self.job_notifier.send(True)


class GcsStagingThreadWorker(GcsStagingWorker, _Thread):
    pass


class GcsStagingProcessWorker(GcsStagingWorker, Process):
    pass


class BigQueryGcsStagingSink(BaseBigQuerySink):
    MAX_WORKERS = os.cpu_count() * 2
    WORKER_CAPACITY_FACTOR = 1
    WORKER_CREATION_MIN_INTERVAL = 10.0

    def __init__(
        self,
        target: "TargetBigQuery",
        stream_name: str,
        schema: Dict[str, Any],
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self.bucket_name = self.config["bucket"]
        self._credentials = BigQueryCredentials(
            self.config.get("credentials_path"),
            self.config.get("credentials_json"),
            self.config["project"],
        )
        self.client = gcs_client_factory(self._credentials)
        self.create_bucket_if_not_exists()
        self.buffer = Compressor()
        self.buffer = Compressor()
        self.gcs_notification, self.gcs_notifier = target.pipe_cls(False)
        self.uris: List[str] = []
        self.increment_jobs_enqueued = target.increment_jobs_enqueued

    @staticmethod
    def worker_cls_factory(
        worker_executor_cls: Type[Process], config: Dict[str, Any]
    ) -> Type[Union[GcsStagingThreadWorker, GcsStagingProcessWorker,]]:
        Worker = type("Worker", (GcsStagingWorker, worker_executor_cls), {})
        return Worker

    @property
    def job_config(self) -> Dict[str, Any]:
        return {
            "schema": self.table.get_resolved_schema(),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
        }

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.buffer.write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))

    def process_batch(self, context: Dict[str, Any]) -> None:
        self.buffer.close()
        self.global_queue.put(
            Job(
                buffer=self.buffer.getvalue()
                if self.global_par_typ is ParType.PROCESS
                else self.buffer.getbuffer(),
                batch_id=context["batch_id"],
                table=self.table.name,
                dataset=self.table.dataset,
                bucket=self.bucket_name,
                gcs_notifier=self.gcs_notifier,
            ),
        )
        self.increment_jobs_enqueued()
        self.buffer = Compressor()

    def clean_up(self) -> None:
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
        super().clean_up()

    def as_bucket(self, **kwargs) -> storage.Bucket:
        """Returns a Bucket instance for this GCS specification."""
        bucket = storage.Bucket(client=self.client, name=self.bucket_name)
        config = {**self.default_bucket_options(), **kwargs}
        for option, value in config.items():
            if option != "location":
                setattr(bucket, option, value)
        return bucket

    def create_bucket_if_not_exists(self) -> storage.Bucket:
        """Creates a cloud storage bucket.

        This is idempotent and will not create
        a new GCS bucket if one already exists."""
        kwargs = {}
        storage_class: str = self.config.get("storage_class")
        if storage_class:
            kwargs["storage_class"] = storage_class
        location: str = self.config.get("location", self.default_bucket_options()["location"])
        kwargs["location"] = location

        if not hasattr(self, "_gcs_bucket"):
            try:
                self._gcs_bucket = self.client.create_bucket(
                    self.as_bucket(),
                    location=location
                )
            except Conflict:
                gcs_bucket = self.client.get_bucket(self.as_bucket())
                if gcs_bucket.location.lower() != location:
                    raise Exception(f"Location of existing GCS bucket {self.bucket_name} "
                                    f"({gcs_bucket.location.lower()}) does not match specified location: {location}")
                else:
                    self._gcs_bucket = gcs_bucket
        else:
            # Wait for eventual consistency
            time.sleep(5)
        return self._gcs_bucket

    @staticmethod
    def default_bucket_options() -> Dict[str, str]:
        return {
            "storage_class": "STANDARD",
            "location": "US"
        }


class BigQueryGcsStagingDenormalizedSink(Denormalized, BigQueryGcsStagingSink):
    @property
    def job_config(self) -> Dict[str, Any]:
        return {
            "schema": self.table.get_resolved_schema(),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
            "schema_update_options": [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
            "ignore_unknown_values": True,
        }

    # Defer schema evolution to the write disposition
    def evolve_schema(self: BaseBigQuerySink) -> None:
        pass
