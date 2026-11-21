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
"""BigQuery Batch Job Sink."""

import logging
import math
import os
import uuid
from io import BytesIO
from mmap import mmap
from multiprocessing import Process
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from typing import Any, cast

import orjson
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    Compressor,
    Denormalized,
    ParType,
    bigquery_client_factory,
    make_json_compatible,
)


class Job:
    def __init__(
        self,
        data: memoryview | bytes | mmap,
        table: bigquery.TableReference,
        config: dict[str, Any],
        timeout: float = 600.0,
    ) -> None:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("timeout must be a positive finite number of seconds")
        self.job_id = f"target_bigquery_load_{uuid.uuid4().hex}"
        self.data = data
        self.table = table
        self.config = config
        self.timeout = timeout
        self.attempt = 1


class BatchJobWorker(BaseWorker):
    """Worker that loads data into BigQuery via LoadJobs."""

    def run(self) -> None:
        """Run the worker."""
        client: bigquery.Client = bigquery_client_factory(self.credentials)
        while True:
            try:
                job: Job | None = self.queue.get(timeout=30.0)
            except Empty:
                break
            if job is None:
                break
            submitted = False
            logging.getLogger(__name__).info(
                "Submitting batch load (job_id=%s, destination=%s).", job.job_id, job.table
            )
            try:
                load_job = client.load_table_from_file(
                    BytesIO(job.data),
                    job.table,
                    job_id=job.job_id,
                    num_retries=3,
                    timeout=job.timeout,
                    job_config=bigquery.LoadJobConfig(**job.config),
                )
                submitted = True
                load_job.result(timeout=job.timeout)
            except Exception as exc:
                job.attempt += 1
                diagnostic = (
                    f"Load job_id={job.job_id}, destination={job.table}. "
                    "Remote completion may be unknown; reconcile before restarting.\n"
                    + self.serialize_exception(exc)
                )
                if submitted or isinstance(exc, Conflict) or job.attempt > 3:
                    self.error_notifier.send((exc, diagnostic))
                    raise
                else:
                    self.queue.put(job)
                    self.log_notifier.send(diagnostic)
            else:
                self.job_notifier.send(True)
                self.log_notifier.send(
                    f"[{self.ext_id}] Loaded {len(job.data)} bytes into {job.table}."
                )
            finally:
                self.queue.task_done()  # type: ignore
        self.normal_completion.set()


class BatchJobThreadWorker(BatchJobWorker, _Thread):
    pass


class BatchJobProcessWorker(BatchJobWorker, Process):
    pass


class BigQueryBatchJobSink(BaseBigQuerySink):
    MAX_WORKERS = (os.cpu_count() or 1) * 2
    WORKER_CAPACITY_FACTOR = 1
    WORKER_CREATION_MIN_INTERVAL = 10.0

    @property
    def job_config(self) -> dict[str, Any]:
        return {
            "schema": self.table.get_resolved_schema(self.apply_transforms),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
        }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.buffer = Compressor()

    @staticmethod
    def worker_cls_factory(
        worker_executor_cls: type[Process], config: dict[str, Any]
    ) -> type[BatchJobThreadWorker | BatchJobProcessWorker]:
        if worker_executor_cls is Process:
            return BatchJobProcessWorker
        if worker_executor_cls is _Thread:
            return BatchJobThreadWorker
        Worker = type("Worker", (BatchJobWorker, worker_executor_cls), {})
        return cast(type[BatchJobThreadWorker], Worker)

    def process_record(self, record: dict[str, Any], context: dict[str, Any]) -> None:
        self.buffer.write(
            orjson.dumps(make_json_compatible(record), option=orjson.OPT_APPEND_NEWLINE)
        )

    def process_batch(self, context: dict[str, Any]) -> None:
        self.buffer.close()
        self.global_queue.put(
            Job(
                data=(
                    self.buffer.getvalue()
                    if self.global_par_typ is ParType.PROCESS
                    else self.buffer.getbuffer()
                ),
                table=self.table.as_ref(),
                config=self.job_config,
                timeout=self.config.get("timeout", 600),
            ),
        )
        self.increment_jobs_enqueued()
        self.buffer = Compressor()


class BigQueryBatchJobDenormalizedSink(Denormalized, BigQueryBatchJobSink):
    @property
    def job_config(self) -> dict[str, Any]:
        return {
            "schema": self.table.get_resolved_schema(self.apply_transforms),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
            "schema_update_options": [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
            "ignore_unknown_values": True,
        }

    # Defer schema evolution the the write disposition
    def evolve_schema(self: BaseBigQuerySink) -> None:
        pass
