"""Caller-visible diagnostics through real Singer input and spawned workers."""

import multiprocessing as mp
import os

import pytest

from target_bigquery.tests.process_error_probe import probe


@pytest.mark.skipif(os.name != "posix", reason="bounded process-group harness requires POSIX")
@pytest.mark.parametrize(
    "start_method", [m for m in ("spawn", "fork") if m in mp.get_all_start_methods()]
)
@pytest.mark.parametrize("mode", ["concurrent", "interrupted", "survivor"])
def test_real_target_error_delivery(mode, start_method):
    result = probe(mode, start_method)
    assert not result["timed_out"], result
    assert result["harness_exit"] == 0, result
    events = result["events"]
    assert any(e.get("state_received") for e in events), result
    assert not any("state_write" in e for e in events), result
    outcome = next(e for e in events if "outcome" in e)
    assert outcome["outcome"] == "raised", result
    assert outcome["exception"] == "RuntimeError", result
    assert outcome["state_output"] == "", result
    if mode == "concurrent":
        assert outcome["complete_diagnostics"] == 2, result
    elif mode == "interrupted":
        assert outcome["incomplete_diagnostic"], result
    else:
        assert outcome["accepted_rows"] > 0, result


def test_fragmented_frames_and_clean_eof():
    import pickle
    import struct

    from target_bigquery.process_errors import ErrorReceiver

    receiver, sender = ErrorReceiver.create()
    expected = (ValueError("failure"), "complete diagnostic")
    payload = pickle.dumps(expected)
    frame = struct.pack("!Q", len(payload)) + payload
    try:
        for byte in frame[:-1]:
            sender.endpoint.sendall(bytes([byte]))
            assert receiver.drain() == []
        sender.endpoint.sendall(frame[-1:])
        messages = receiver.drain()
        assert [(type(exc), str(exc), detail) for exc, detail in messages] == [
            (ValueError, "failure", "complete diagnostic")
        ]
        sender.close()
        assert receiver.drain(finished=True) == []
        assert receiver.closed
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("prefix", [b"\0", b"\0" * 7 + b"\x05ab"])
def test_partial_frame_after_death_without_eof(prefix):
    from target_bigquery.process_errors import ErrorReceiver

    receiver, sender = ErrorReceiver.create()
    try:
        sender.endpoint.sendall(prefix)
        assert receiver.drain() == []
        # Retain the writer deliberately: completion must not depend on EOF.
        errors = receiver.drain(finished=True)
        assert len(errors) == 1
        assert "incomplete diagnostic" in errors[0][1]
        assert receiver.closed
        assert receiver.drain(finished=True) == []
    finally:
        sender.close()
        receiver.close()


def test_corrupt_frame_does_not_hide_following_complete_diagnostic():
    import struct

    from target_bigquery.process_errors import ErrorReceiver

    receiver, sender = ErrorReceiver.create()
    try:
        sender.endpoint.sendall(struct.pack("!Q", 3) + b"bad")
        sender.send((ValueError("valid"), "preserved"))
        errors = receiver.drain()
        assert len(errors) == 2
        assert "Could not decode" in errors[0][1]
        assert errors[1][1] == "preserved"
    finally:
        sender.close()
        receiver.close()


@pytest.mark.skipif(os.name != "posix", reason="bounded process-group harness requires POSIX")
@pytest.mark.parametrize(
    "start_method", [m for m in ("spawn", "fork") if m in mp.get_all_start_methods()]
)
@pytest.mark.parametrize("mode", ["healthy", "permissive"])
def test_real_target_success_and_explicit_permissive_behavior(mode, start_method):
    result = probe(mode, start_method)
    assert not result["timed_out"], result
    assert result["harness_exit"] == 0, result
    outcome = next(e for e in result["events"] if "outcome" in e)
    assert outcome["outcome"] == "returned", result
    assert outcome["state_output"], result
    assert outcome["channels"] == 0, result
    if mode == "healthy":
        assert outcome["worker_count"] == 2, result
        assert len([e for e in result["events"] if "state_write" in e]) == 2, result


@pytest.mark.parametrize("failure", ["construction", "start"])
def test_worker_creation_failure_closes_both_endpoints(monkeypatch, failure):
    from target_bigquery.process_errors import ErrorReceiver
    from target_bigquery.target import TargetBigQuery

    target = TargetBigQuery(
        config={
            "project": "offline",
            "dataset": "offline",
            "method": "streaming_insert",
            "options": {"process_pool": True},
        }
    )
    channels = []
    original_create = ErrorReceiver.create

    def create():
        channel = original_create()
        channels.append(channel)
        return channel

    def fail(*args, **kwargs):
        raise RuntimeError("injected startup failure")

    monkeypatch.setattr(ErrorReceiver, "create", create)
    if failure == "construction":
        sink_class = target.get_sink_class()
        monkeypatch.setattr(sink_class, "worker_cls_factory", fail)
        action = target.worker_factory
    else:
        original_factory = target.worker_factory

        def factory():
            worker = original_factory()
            worker.start = fail
            return worker

        target.worker_factory = factory
        action = target.resize_worker_pool
    try:
        with pytest.raises(RuntimeError, match="injected startup failure"):
            action()
        assert target._process_error_channels == {}
        receiver, sender = channels[0]
        assert receiver.endpoint.fileno() == -1
        assert sender.endpoint.fileno() == -1
    finally:
        target.queue.close()
        for name in (
            "job_notification",
            "job_notifier",
            "log_notification",
            "log_notifier",
            "error_notification",
            "error_notifier",
        ):
            getattr(target, name).close()


def _report_inherited_receiver(receiver, report):
    report.send(receiver.endpoint.fileno())


@pytest.mark.skipif("fork" not in mp.get_all_start_methods(), reason="fork-specific ownership")
def test_fork_child_drops_parent_receiving_endpoint():
    from target_bigquery.process_errors import ErrorReceiver

    receiver, sender = ErrorReceiver.create()
    context = mp.get_context("fork")
    received, sent = context.Pipe(False)
    worker = context.Process(target=_report_inherited_receiver, args=(receiver, sent))
    try:
        worker.start()
        assert received.poll(5)
        assert received.recv() == -1
        worker.join(5)
        assert worker.exitcode == 0
        assert receiver.endpoint.fileno() >= 0
        sender.send((ValueError("parent"), "still usable"))
        assert receiver.drain()[0][1] == "still usable"
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(5)
        worker.close()
        sender.close()
        receiver.close()
        received.close()
        sent.close()
