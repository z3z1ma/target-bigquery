"""Pure unit tests for target dispatch and worker-pool behavior."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from target_bigquery.batch_job import BigQueryBatchJobDenormalizedSink, BigQueryBatchJobSink
from target_bigquery.core import ParType
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
from target_bigquery.target import TargetBigQuery


class FakeWorker:
    """Worker double with the subset used by TargetBigQuery."""

    def __init__(self, ext_id: str, *, alive: bool) -> None:
        self.ext_id = ext_id
        self.alive = alive
        self.joins = 0
        self.starts = 0

    def is_alive(self) -> bool:
        return self.alive

    def join(self) -> None:
        self.joins += 1

    def start(self) -> None:
        self.starts += 1
        self.alive = True


def make_target(config: dict[str, Any]) -> TargetBigQuery:
    target = object.__new__(TargetBigQuery)
    target._config = config
    target.workers = []
    target._jobs_enqueued = 0
    target._last_worker_creation = 0.0
    return target


@pytest.mark.parametrize(
    "method,denormalized,expected",
    [
        ("batch_job", False, BigQueryBatchJobSink),
        ("batch_job", True, BigQueryBatchJobDenormalizedSink),
        ("streaming_insert", False, BigQueryStreamingInsertSink),
        ("streaming_insert", True, BigQueryStreamingInsertDenormalizedSink),
        ("gcs_stage", False, BigQueryGcsStagingSink),
        ("gcs_stage", True, BigQueryGcsStagingDenormalizedSink),
        ("storage_write_api", False, BigQueryStorageWriteSink),
        ("storage_write_api", True, BigQueryStorageWriteDenormalizedSink),
    ],
)
def test_get_sink_class_selects_method_and_schema_strategy(method, denormalized, expected):
    target = make_target({"method": method, "denormalized": denormalized})

    assert target.get_sink_class("ignored-stream-name") is expected


def test_get_sink_class_defaults_to_storage_write_fixed_schema():
    target = make_target({})

    assert target.get_sink_class() is BigQueryStorageWriteSink


def test_get_sink_class_rejects_unknown_method():
    target = make_target({"method": "not-real"})

    with pytest.raises(ValueError, match="Unknown method: not-real"):
        target.get_sink_class()


def test_get_parallelization_components_use_threads_by_default(caplog):
    caplog.set_level(logging.INFO, logger=TargetBigQuery.name)
    target = make_target({"options": {}})

    _, _, _, par_typ = target.get_parallelization_components()

    assert par_typ is ParType.THREAD
    assert "Using thread-based parallelism" in caplog.messages


def test_get_parallelization_components_can_default_to_processes(caplog):
    caplog.set_level(logging.INFO, logger=TargetBigQuery.name)
    target = make_target({"options": {}})

    _, _, _, par_typ = target.get_parallelization_components(default=ParType.PROCESS)

    assert par_typ is ParType.PROCESS
    assert "Using process-based parallelism" in caplog.messages


@pytest.mark.parametrize(
    "process_pool,expected", [(False, ParType.THREAD), (True, ParType.PROCESS)]
)
def test_get_parallelization_components_config_overrides_default(process_pool, expected):
    target = make_target({"options": {"process_pool": process_pool}})

    _, _, _, par_typ = target.get_parallelization_components(default=ParType.PROCESS)

    assert par_typ is expected


def test_add_worker_predicate_requires_capacity_room_and_cooldown():
    target = make_target({"method": "batch_job", "options": {"max_workers": 2}})
    target._last_worker_creation = 0.0
    target._jobs_enqueued = 3

    assert target.add_worker_predicate is True

    target.workers = [object(), object()]
    assert target.add_worker_predicate is False

    target.workers = []
    target._last_worker_creation = 9_999_999_999.0
    assert target.add_worker_predicate is False

    target._last_worker_creation = 0.0
    target._jobs_enqueued = 1
    assert target.add_worker_predicate is False


def test_resize_worker_pool_culls_dead_workers_and_starts_at_least_one_worker(caplog):
    caplog.set_level(logging.INFO, logger=TargetBigQuery.name)
    target = make_target({"method": "batch_job", "options": {}})
    dead_worker = FakeWorker("dead", alive=False)
    new_worker = FakeWorker("new", alive=False)
    target.workers = [dead_worker]
    target.worker_factory = lambda: new_worker

    target.resize_worker_pool()

    assert target.workers == [new_worker]
    assert dead_worker.joins == 1
    assert new_worker.starts == 1
    assert "Culling terminated worker dead" in caplog.messages
    assert "Adding worker new" in caplog.messages


def test_get_sink_returns_active_sink_when_schema_is_absent():
    target = make_target({})
    active_sink = object()
    target._sinks_active = {"orders": active_sink}
    checked_streams: list[str] = []
    target._assert_sink_exists = checked_streams.append

    assert target.get_sink("orders") is active_sink
    assert checked_streams == ["orders"]


def test_get_sink_adds_only_the_first_schema_for_a_stream():
    target = make_target({})
    first_sink = object()
    existing_sink = object()
    target._sinks_active = {}
    added: list[tuple[str, dict[str, Any], list[str] | None]] = []

    def add_sink(
        stream_name: str,
        schema: dict[str, Any],
        key_properties: list[str] | None,
    ) -> object:
        added.append((stream_name, schema, key_properties))
        return first_sink

    target.add_sink = add_sink

    schema = {"type": "object"}
    assert target.get_sink("orders", schema=schema, key_properties=["id"]) is first_sink
    assert added == [("orders", schema, ["id"])]

    target._sinks_active["orders"] = existing_sink
    assert target.get_sink("orders", schema={"type": "object"}, key_properties=["other"]) is (
        existing_sink
    )
