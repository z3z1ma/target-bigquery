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
"""BigQuery Batch Job Sink.
Throughput test: 6m 25s @ 1M rows / 150 keys / 1.5GB
NOTE: This is naive and will vary drastically based on network speed, for example on a GCP VM.
"""
import os
from io import BytesIO
from multiprocessing import Process
from multiprocessing.dummy import Process as _Thread
from queue import Empty
from typing import Any, Dict, NamedTuple, Optional, Type, Union

import orjson
from google.cloud import bigquery

from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    Compressor,
    Denormalized,
    ParType,
    bigquery_client_factory,
)


class Job(NamedTuple):
    table: str
    data: Union[bytes, memoryview]
    config: Dict[str, Any]


class BatchJobWorker(BaseWorker):
    def run(self):
        client: bigquery.Client = bigquery_client_factory(self.credentials)
        while True:
            try:
                job: Optional[Job] = self.queue.get(timeout=30.0)
            except Empty:
                break
            if job is None:
                break
            try:
                client.load_table_from_file(
                    BytesIO(job.data),
                    job.table,
                    num_retries=3,
                    job_config=bigquery.LoadJobConfig(**job.config),
                ).result()
            except Exception as exc:
                self.queue.put(job)
                raise exc
            self.job_notifier.send(True)


class BatchJobThreadWorker(BatchJobWorker, _Thread):
    pass


class BatchJobProcessWorker(BatchJobWorker, Process):
    pass


class BigQueryBatchJobSink(BaseBigQuerySink):

    MAX_WORKERS = os.cpu_count() * 2
    WORKER_CAPACITY_FACTOR = 1
    WORKER_CREATION_MIN_INTERVAL = 10.0

    @property
    def job_config(self) -> Dict[str, Any]:
        return {
            "schema": self.table.get_resolved_schema(),
            "source_format": bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            "write_disposition": bigquery.WriteDisposition.WRITE_APPEND,
        }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.buffer = Compressor()

    @staticmethod
    def worker_cls_factory(
        worker_executor_cls: Type[Process], config: Dict[str, Any]
    ) -> Type[Union[BatchJobThreadWorker, BatchJobProcessWorker]]:
        Worker = type("Worker", (BatchJobWorker, worker_executor_cls), {})
        return Worker

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        self.buffer.write(orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))

    def process_batch(self, context: Dict[str, Any]) -> None:
        self.buffer.close()
        self.global_queue.put(
            Job(
                data=self.buffer.getvalue()
                if self.global_par_typ is ParType.PROCESS
                else self.buffer.getbuffer(),
                table=self.table.as_ref(),
                config=self.job_config,
            ),
        )
        self.increment_jobs_enqueued()
        self.buffer = Compressor()


class BigQueryBatchJobDenormalizedSink(Denormalized, BigQueryBatchJobSink):
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
