"""State must describe published data, not temporary overwrite staging."""

from collections import deque
from types import SimpleNamespace

import pytest

from target_bigquery.core import ParType
from target_bigquery.target import TargetBigQuery


class StagedSink:
    def __init__(self, overwrite, *, fail_cleanup=False):
        self.overwrite_target = object() if overwrite else None
        self.fail_cleanup = fail_cleanup
        self.checkpoints = 0
        self.published = False

    def pre_state_hook(self):
        self.checkpoints += 1

    def clean_up(self):
        if self.fail_cleanup:
            raise RuntimeError("publication failed")
        self.published = True
        self.overwrite_target = None


def make_target(sinks):
    target = object.__new__(TargetBigQuery)
    target._pending_worker_errors = deque()
    target._process_error_channels = {}
    target._config = {}
    target.par_typ = ParType.THREAD
    target._latest_state = {"pos": 1}
    target._sinks_active = sinks
    target.max_parallelism = 1
    target.error_notification = SimpleNamespace(poll=lambda: False)
    target._drain_all = lambda *args: None
    target._join_workers = lambda: None
    target._shutdown_workers = lambda: None
    target._reset_max_record_age = lambda: None
    emitted = []
    target._write_state_message = emitted.append
    return target, emitted


@pytest.mark.parametrize("mixed", [False, True])
def test_overwrite_defers_global_state_until_all_data_is_published(mixed):
    overwrite = StagedSink(True)
    sinks = {"snapshot": overwrite}
    if mixed:
        sinks["append"] = StagedSink(False)
    target, emitted = make_target(sinks)

    target.drain_all()
    target._latest_state = {"pos": 2}
    target.drain_all()
    assert emitted == []  # A crash here leaves the upstream persisted bookmark unchanged.
    assert overwrite.checkpoints == 2
    assert not overwrite.published

    target.drain_all(is_endofpipe=True)
    assert all(sink.published for sink in sinks.values())
    assert emitted == [{"pos": 2}]


def test_overwrite_failure_after_another_sink_publishes_emits_no_state():
    first = StagedSink(True)
    second = StagedSink(True, fail_cleanup=True)
    target, emitted = make_target({"first": first, "second": second})
    target.drain_all()
    with pytest.raises(RuntimeError, match="publication failed"):
        target.drain_all(is_endofpipe=True)
    assert first.published
    assert not second.published
    assert emitted == []


def test_append_or_upsert_without_overwrite_keeps_intermediate_checkpoints():
    target, emitted = make_target({"orders": StagedSink(False)})
    target.drain_all()
    assert emitted == [{"pos": 1}]


@pytest.mark.parametrize("overwrite", [False, True])
@pytest.mark.parametrize("signum", [2, 15])
def test_termination_does_not_publish_an_incomplete_overwrite(overwrite, signum):
    sink = StagedSink(overwrite)
    append = StagedSink(False)
    target, emitted = make_target({"snapshot": sink, "append": append})
    shutdowns = []
    target._shutdown_workers = lambda: shutdowns.append(True)
    with pytest.raises(SystemExit) as exc:
        target._handle_termination(signum, None)
    assert exc.value.code == (128 + signum if overwrite else 0)
    assert shutdowns == [True]
    assert sink.published is not overwrite
    assert append.published is not overwrite
    assert sink.checkpoints == append.checkpoints == 0
    assert emitted == ([] if overwrite else [{"pos": 1}])
