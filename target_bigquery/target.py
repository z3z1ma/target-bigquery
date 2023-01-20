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
"""BigQuery target class."""
import copy
import time
import uuid
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Type, Union

from singer_sdk import typing as th
from singer_sdk.target_base import Sink, Target

from target_bigquery.batch_job import (
    BigQueryBatchJobDenormalizedSink,
    BigQueryBatchJobSink,
)
from target_bigquery.core import (
    BaseBigQuerySink,
    BaseWorker,
    BigQueryCredentials,
    ParType,
)
from target_bigquery.gcs_stage import (
    BigQueryGcsStagingDenormalizedSink,
    BigQueryGcsStagingSink,
)
from target_bigquery.storage_write import (
    BigQueryStorageWriteDenormalizedSink,
    BigQueryStorageWriteSink,
)
from target_bigquery.streaming_insert import (
    BigQueryStreamingInsertDenormalizedSink,
    BigQueryStreamingInsertSink,
)

if TYPE_CHECKING:
    from multiprocessing import Process, Queue
    from multiprocessing.connection import Connection

# Defaults for target worker pool parameters
MAX_WORKERS = 15
"""Maximum number of workers to spawn."""
WORKER_CAPACITY_FACTOR = 5
"""Jobs enqueued must exceed the number of active workers times this number."""
WORKER_CREATION_MIN_INTERVAL = 5
"""Minimum time between worker creation attempts."""


class TargetBigQuery(Target):
    """Target for BigQuery."""

    name = "target-bigquery"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "credentials_path",
            th.StringType,
            description="The path to a gcp credentials json file.",
        ),
        th.Property(
            "credentials_json",
            th.StringType,
            description="A JSON string of your service account JSON file.",
        ),
        th.Property(
            "project",
            th.StringType,
            description="The target GCP project to materialize data into.",
            required=True,
        ),
        th.Property(
            "dataset",
            th.StringType,
            description="The target dataset to materialize data into.",
            required=True,
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="The maximum number of rows to send in a single batch or commit.",
            default=500,
        ),
        th.Property(
            "timeout",
            th.IntegerType,
            description="Default timeout for batch_job and gcs_stage derived LoadJobs.",
            default=600,
        ),
        th.Property(
            "denormalized",
            th.BooleanType,
            description="Determines whether to denormalize the data before writing to BigQuery. A false value "
            "will write data using a fixed JSON column based schema, while a true value will write data using a dynamic "
            "schema derived from the tap. Denormalization is only supported for the batch_job, streaming_insert, and gcs_stage methods.",
            default=False,
        ),
        th.Property(
            "method",
            th.CustomType(
                {
                    "type": "string",
                    "enum": [
                        "storage_write_api",
                        "batch_job",
                        "gcs_stage",
                        "streaming_insert",
                    ],
                }
            ),
            description="The method to use for writing to BigQuery.",
            default="storage_write_api",
            required=True,
        ),
        th.Property(
            "generate_view",
            th.BooleanType,
            description="Determines whether to generate a view based on the SCHEMA message parsed from the tap. "
            "Only valid if denormalized=false meaning you are using the fixed JSON column based schema.",
            default=False,
        ),
        th.Property(
            "bucket",
            th.StringType,
            description="The GCS bucket to use for staging data. Only used if method is gcs_stage.",
        ),
        th.Property(
            "partition_granularity",
            th.CustomType(
                {
                    "type": "string",
                    "enum": [
                        "year",
                        "month",
                        "day",
                        "hour",
                    ],
                }
            ),
            default="month",
            description="The granularity of the partitioning strategy. Defaults to month.",
        ),
        th.Property(
            "cluster_on_key_properties",
            th.BooleanType,
            default=False,
            description="Determines whether to cluster on the key properties from the tap. Defaults to false.",
        ),
        th.Property(
            "column_name_transforms",
            th.ObjectType(
                th.Property(
                    "lower",
                    th.BooleanType,
                    default=False,
                    description="Lowercase column names",
                ),
                th.Property(
                    "quote",
                    th.BooleanType,
                    default=False,
                    description="Quote columns during DDL generation",
                ),
                th.Property(
                    "add_underscore_when_invalid",
                    th.BooleanType,
                    default=False,
                    description="Add an underscore when a column starts with a digit",
                ),
                th.Property(
                    "snake_case",
                    th.BooleanType,
                    default=False,
                    description="Convert columns to snake case",
                ),
            ),
            description=(
                "Accepts a JSON object of options with boolean values to enable them. The available options are `quote` "
                "(quote columns in DDL), `lower` (lowercase column names), `add_underscore_when_invalid` (add underscore "
                "if column starts with digit), and `snake_case` (convert to snake case naming). For fixed schema, this "
                "transform only applies to the generated view if enabled."
            ),
            required=False,
        ),
        th.Property(
            "options",
            th.ObjectType(
                th.Property(
                    "storage_write_batch_mode",
                    th.BooleanType,
                    default=False,
                    description=(
                        "By default, we use the default stream (Committed mode) in the storage_write_api load method "
                        "which results in streaming records which are immediately available and is generally fastest. If this is set to true, we will "
                        "use the application created streams (Committed mode) to transactionally batch data on STATE messages and at end of pipe."
                    ),
                ),
                th.Property(
                    "process_pool",
                    th.BooleanType,
                    default=False,
                    description="By default we use an autoscaling threadpool to write to BigQuery. If set to true, we will use a process pool.",
                ),
                th.Property(
                    "max_workers",
                    th.IntegerType,
                    required=False,
                    description="By default, each sink type has a preconfigured max worker limit. This sets an override for maximum number of workers per stream.",
                ),
            ),
        ),
    ).to_dict()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.max_parallelism = 1
        (
            self.proc_cls,
            self.pipe_cls,
            self.queue_cls,
            self.par_typ,
        ) = self.get_parallelization_components()
        self.queue = self.queue_cls()
        self.job_notification, self.job_notifier = self.pipe_cls(False)
        self._credentials = BigQueryCredentials(
            self.config.get("credentials_path"),
            self.config.get("credentials_json"),
            self.config["project"],
        )

        def worker_factory():
            return self.get_sink_class().worker_cls_factory(
                self.proc_cls,
                self.config,
            )(
                ext_id=uuid.uuid4().hex,
                queue=self.queue,
                credentials=self._credentials,
                job_notifier=self.job_notifier,
            )

        self.worker_factory = worker_factory
        self.workers: List[Union[BaseWorker, "Process"]] = []
        self.worker_pings: Dict[str, float] = {}
        self._jobs_enqueued = 0
        self._last_worker_creation = 0.0

    def increment_jobs_enqueued(self) -> None:
        """Increment the number of jobs enqueued."""
        self._jobs_enqueued += 1

    # We can expand this to support other parallelization methods in the future.
    # We woulod approach this by adding a new ParType enum and interpreting the
    # the Process, Pipe, and Queue classes as protocols which can be duck-typed.

    def get_parallelization_components(
        self, default=ParType.THREAD
    ) -> Tuple[
        Type["Process"],
        Callable[[bool], Tuple["Connection", "Connection"]],
        Callable[[], "Queue"],
        ParType,
    ]:
        """Get the appropriate Process, Pipe, and Queue classes and the assoc ParTyp enum."""
        use_procs: Optional[bool] = self.config.get("options", {}).get("process_pool")

        if use_procs is None:
            use_procs = default == ParType.PROCESS

        if not use_procs:
            from multiprocessing.dummy import Pipe, Process, Queue

            self.logger.info("Using thread-based parallelism")
            return Process, Pipe, Queue, ParType.THREAD
        else:
            from multiprocessing import Pipe, Process, Queue

            self.logger.info("Using process-based parallelism")
            return Process, Pipe, Queue, ParType.PROCESS

    # Worker management methods, which are used to manage the number of
    # workers in the pool. The ensure_workers method should be called
    # periodically to ensure that the pool is at the correct size, ideally
    # once per batch.

    @property
    def add_worker_predicate(self) -> bool:
        """Predicate determining when it is valid to add a worker to the pool."""
        return (
            self._jobs_enqueued
            > getattr(
                self.get_sink_class(), "WORKER_CAPACITY_FACTOR", WORKER_CAPACITY_FACTOR
            )
            * (len(self.workers) + 1)
            and len(self.workers)
            < self.config.get("options", {}).get(
                "max_workers",
                getattr(self.get_sink_class(), "MAX_WORKERS", MAX_WORKERS),
            )
            and time.time() - self._last_worker_creation
            > getattr(
                self.get_sink_class(),
                "WORKER_CREATION_MIN_INTERVAL",
                WORKER_CREATION_MIN_INTERVAL,
            )
        )

    def resize_worker_pool(self) -> None:
        """Right-sizes the worker pool.

        Workers self terminate when they have been idle for a while.
        This method will remove terminated workers and add new workers
        if the add_worker_predicate evaluates to True. It will always
        ensure that there is at least one worker in the pool."""
        workers_to_cull = []
        worker_spawned = False
        for i, worker in enumerate(self.workers):
            if not worker.is_alive():
                workers_to_cull.append(i)
        for i in reversed(workers_to_cull):
            worker = self.workers.pop(i)
            worker.join()  # Wait for the worker to terminate. This should be a no-op.
            self.logger.info("Culling terminated worker %s", worker.ext_id)
        while self.add_worker_predicate or not self.workers:
            worker = self.worker_factory()
            worker.start()
            self.workers.append(worker)
            worker_spawned = True
            self.logger.info("Adding worker %s", worker.ext_id)
            self._last_worker_creation = time.time()
        if worker_spawned:
            ...

    # SDK overrides to inject our worker management logic and sink selection.

    def get_sink_class(
        self, stream_name: Optional[str] = None
    ) -> Type[BaseBigQuerySink]:
        """Returns the sink class to use for a given stream based on user config."""
        _ = stream_name
        method, denormalized = self.config.get(
            "method", "storage_write_api"
        ), self.config.get("denormalized", False)
        if method == "batch_job":
            if denormalized:
                return BigQueryBatchJobDenormalizedSink
            return BigQueryBatchJobSink
        elif method == "streaming_insert":
            if denormalized:
                return BigQueryStreamingInsertDenormalizedSink
            return BigQueryStreamingInsertSink
        elif method == "gcs_stage":
            if denormalized:
                return BigQueryGcsStagingDenormalizedSink
            return BigQueryGcsStagingSink
        elif method == "storage_write_api":
            if denormalized:
                return BigQueryStorageWriteDenormalizedSink
            return BigQueryStorageWriteSink
        raise ValueError(f"Unknown method: {method}")

    def get_sink(
        self,
        stream_name: str,
        *,
        record: Optional[dict] = None,
        schema: Optional[dict] = None,
        key_properties: Optional[List[str]] = None,
    ) -> Sink:
        """Get a sink for a stream. If the sink does not exist, create it. This override skips sink recreation
        on schema change. Meaningful mid stream schema changes are not supported and extremely rare to begin
        with. Most taps provide a static schema at stream init. We handle +90% of cases with this override without
        the undue complexity or overhead of mid-stream schema evolution. If you need to support mid-stream schema
        evolution on a regular basis, you should be using the fixed schema load pattern."""
        _ = record
        if schema is None:
            self._assert_sink_exists(stream_name)
            return self._sinks_active[stream_name]
        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sink(stream_name, schema, key_properties)
        return existing_sink

    def drain_one(self, sink: Sink) -> None:  # type: ignore
        """Drain a sink. Includes a hook to manage the worker pool."""
        self.resize_worker_pool()
        while self.job_notification.poll():
            ext_id = self.job_notification.recv()
            self.worker_pings[ext_id] = time.time()
            self._jobs_enqueued -= 1
        super().drain_one(sink)

    def drain_all(self, is_endofpipe: bool = False) -> None:  # type: ignore
        """Drain all sinks and write state message. If is_endofpipe, execute clean_up() on all sinks.
        Includes an additional hook to allow sinks to do any pre-state message processing."""
        state = copy.deepcopy(self._latest_state)
        sink: BaseBigQuerySink
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        if is_endofpipe:
            for worker in self.workers:
                if worker.is_alive():
                    self.queue.put(None)
            for worker in self.workers:
                worker.join()
            for sink in self._sinks_active.values():
                sink.clean_up()
        else:
            for sink in self._sinks_active.values():
                sink.pre_state_hook()
        self._write_state_message(state)
        self._reset_max_record_age()
