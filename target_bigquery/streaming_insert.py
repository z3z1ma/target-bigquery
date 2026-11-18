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
"""BigQuery Streaming Insert Sink."""

import logging
import math
import os
import uuid
from multiprocessing import Process
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from time import sleep
from typing import Any, cast

import orjson
from google.api_core.exceptions import NotFound
from google.cloud import _http, bigquery

from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    Denormalized,
    bigquery_client_factory,
    make_json_compatible,
)


class Job:
    """Job to be processed by a worker."""

    def __init__(
        self,
        table: bigquery.TableReference,
        records: list[dict[str, Any]],
        timeout: float = 600,
    ) -> None:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError("timeout must be a positive finite number of seconds")
        self.timeout = timeout
        self.batch_id = uuid.uuid4().hex
        self.row_ids = [f"{self.batch_id}-{index}" for index in range(len(records))]
        self.table = table
        self.records = records
        self.attempt = 1


class StreamingInsertWorker(BaseWorker):
    """Worker that streams data into BigQuery."""

    def run(self) -> None:
        """Run the worker."""
        # A monkey patch since we can't override the default json encoder...
        cast(Any, _http).json = orjson
        client: bigquery.Client = bigquery_client_factory(self.credentials)
        while True:
            try:
                job: Job | None = self.queue.get(timeout=20.0)
            except Empty:
                break
            if job is None:
                break
            identity = f"batch_id={job.batch_id}, destination={job.table}"
            logging.getLogger(__name__).info("Submitting streaming insert (%s).", identity)
            try:
                errors = client.insert_rows_json(
                    table=job.table,
                    json_rows=job.records,
                    row_ids=job.row_ids,
                    timeout=job.timeout,
                    retry=None,  # type: ignore[arg-type]  # Supported by SDK runtime.
                )
                if errors:
                    raise RuntimeError(f"BigQuery streaming insert returned row errors: {errors}")
            except Exception as exc:
                job.attempt += 1
                if not isinstance(exc, NotFound) or job.attempt > 3:
                    diagnostic = (
                        f"Streaming insert failed for {identity}. "
                        "Rows may already be present; reconcile before replay.\n"
                        + self.serialize_exception(exc)
                    )
                    self.error_notifier.send((exc, diagnostic))
                    raise
                else:
                    sleep(1)
                    self.queue.put(job)
            else:
                self.job_notifier.send(True)
                self.log_notifier.send(
                    f"[{self.ext_id}] Inserted {len(job.records)} records into {job.table}"
                )
            finally:
                self.queue.task_done()  # type: ignore


class StreamingInsertThreadWorker(StreamingInsertWorker, _Thread):
    pass


class StreamingInsertProcessWorker(StreamingInsertWorker, Process):
    pass


class BigQueryStreamingInsertSink(BaseBigQuerySink):
    MAX_WORKERS = (os.cpu_count() or 1) * 2
    WORKER_CAPACITY_FACTOR = 10
    WORKER_CREATION_MIN_INTERVAL = 1.0

    @staticmethod
    def worker_cls_factory(
        worker_executor_cls: type[Process], config: dict[str, Any]
    ) -> type[StreamingInsertThreadWorker | StreamingInsertProcessWorker]:
        if worker_executor_cls is Process:
            return StreamingInsertProcessWorker
        if worker_executor_cls is _Thread:
            return StreamingInsertThreadWorker
        Worker = type("Worker", (StreamingInsertWorker, worker_executor_cls), {})
        return cast(type[StreamingInsertThreadWorker], Worker)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = super().preprocess_record(record, context)
        record["data"] = orjson.dumps(make_json_compatible(record["data"])).decode("utf-8")
        return record

    @property
    def max_size(self) -> int:
        return min(super().max_size, 500)

    def process_record(self, record: dict[str, Any], context: dict[str, Any]) -> None:
        self.records_to_drain.append(make_json_compatible(record))

    def process_batch(self, context: dict[str, Any]) -> None:
        self.global_queue.put(
            Job(
                table=self.table.as_ref(),
                records=self.records_to_drain.copy(),
                timeout=self.config.get("timeout", 600),
            )
        )
        self.increment_jobs_enqueued()
        self.records_to_drain = []


class BigQueryStreamingInsertDenormalizedSink(Denormalized, BigQueryStreamingInsertSink):
    pass
