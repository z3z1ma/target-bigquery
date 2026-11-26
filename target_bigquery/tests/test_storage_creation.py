"""Stream creation deadlines must fail closed at the Singer entry point."""

import json
import os

import pytest

from target_bigquery.tests.storage_creation_probe import probe

pytestmark = pytest.mark.skipif(os.name != "posix", reason="bounded harness uses process groups")


@pytest.mark.parametrize("process_pool", [False, True])
def test_creation_deadline_exhausts_setup_attempts_without_state(process_pool):
    result = probe(process_pool)
    assert not result["timed_out"], result
    assert result["exitcode"] == 0, result
    events = result["events"]
    creations = [event for event in events if "create_timeout" in event]
    assert [event["attempt"] for event in creations] == [1, 2, 3], result
    assert all(event["create_timeout"] == 0.05 for event in creations), result
    assert not any(set(event) & {"append", "finalize", "commit"} for event in events), result
    assert events[-1] == {"outcome": "RuntimeError", "cause": "DeadlineExceeded", "state": ""}, (
        result
    )


@pytest.mark.parametrize("process_pool", [False, True])
@pytest.mark.parametrize("pending", [False, True])
def test_creation_retry_recovers_and_default_stream_bypasses_creation(process_pool, pending):
    result = probe(process_pool, pending=pending, mode="transient")
    assert not result["timed_out"], result
    assert result["exitcode"] == 0, result
    events = result["events"]
    creations = [event for event in events if "create_timeout" in event]
    assert [event["attempt"] for event in creations] == ([1, 2] if pending else []), result
    assert all(event["create_timeout"] == 0.05 for event in creations), result
    assert [event["append"] for event in events if "append" in event] == [1], result
    assert sum("finalize" in event for event in events) == int(pending), result
    assert sum("commit" in event for event in events) == int(pending), result
    assert events[-1]["outcome"] == "returned", result
    assert json.loads(events[-1]["state"]) == {"position": 1}, result
