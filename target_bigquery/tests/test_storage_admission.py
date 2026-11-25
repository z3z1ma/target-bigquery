"""Portable bounded Storage Write admission must retain completion ordering."""

import os

import pytest

from target_bigquery.tests.storage_admission_probe import probe

pytestmark = pytest.mark.skipif(os.name != "posix", reason="bounded harness uses process groups")


@pytest.mark.parametrize("process_pool", [False, True])
@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("rows", [5, 40])
def test_full_queue_and_eof_complete_real_singer_input(process_pool, pending, rows):
    result = probe(process_pool, pending, rows=rows)
    assert result["full"], result
    assert not result["state_before_release"], result
    if rows == 5:
        assert result["sentinel_full"], result
    assert result["outcome"] == "returned", result
    assert result["accepted"] == [1] * rows, result
    if process_pool:
        assert result["outstanding"] == 0, result
    assert len(result["states"]) == 1, result
    state = result["states"][0]
    assert state["value"] == {"position": rows}, result
    assert state["accepted"] == rows, result
    assert state["commits"] == (["finalize", "commit"] if pending else []), result


@pytest.mark.parametrize("process_pool", [False, True])
@pytest.mark.parametrize("failure", ["append", "exit", "commit"])
def test_failed_work_with_full_admission_withholds_state(process_pool, failure):
    result = probe(process_pool, True, failure=failure)
    assert result["full"], result
    assert result["outcome"] == "RuntimeError", result
    assert result["states"] == [], result
    if failure != "commit":
        assert result["commits"] == [], result


@pytest.mark.parametrize("process_pool", [False, True])
def test_pending_checkpoint_commits_before_state(process_pool):
    result = probe(process_pool, True, checkpoint=True)
    assert result["outcome"] == "returned", result
    assert result["accepted"] == [2] * 40, result
    assert result["worker_count"] == 2, result
    assert len(result["states"]) == 2, result
    assert result["states"][0]["commits"] == ["finalize", "commit"], result
    assert result["states"][1]["commits"] == ["finalize", "commit"] * 2, result
    assert result["states"][1]["value"] == {"position": 80}, result


@pytest.mark.parametrize("process_pool", [False, True])
def test_worker_exit_during_full_eof_admission_withholds_state(process_pool):
    result = probe(process_pool, True, failure="exit", rows=5)
    assert result["full"], result
    assert result["sentinel_full"], result
    assert result["outcome"] == "RuntimeError", result
    assert result["states"] == [], result
    assert result["commits"] == [], result


def test_full_process_queue_feeder_failure_withholds_state():
    result = probe(True, True, failure="feeder")
    assert result["full"], result
    assert result["outcome"] == "RuntimeError", result
    assert "feeder failed" in result["detail"], result
    assert result["states"] == [], result
    assert result["commits"] == [], result
