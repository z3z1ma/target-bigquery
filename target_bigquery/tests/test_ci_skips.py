"""Tests for CI-only integration skip rules."""

import pytest

from target_bigquery.tests.test_sync import (
    LEGACY_STREAMING_INSERT_CI_SKIP_REASON,
    _skip_legacy_streaming_insert_in_ci,
)


def test_legacy_streaming_insert_skips_in_github_actions(monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    with pytest.raises(pytest.skip.Exception, match=LEGACY_STREAMING_INSERT_CI_SKIP_REASON):
        _skip_legacy_streaming_insert_in_ci("streaming_insert")


def test_legacy_streaming_insert_does_not_skip_outside_github_actions(monkeypatch):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    _skip_legacy_streaming_insert_in_ci("streaming_insert")


def test_non_legacy_streaming_methods_do_not_skip_in_github_actions(monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    for method in ("batch_job", "gcs_stage", "storage_write_api"):
        _skip_legacy_streaming_insert_in_ci(method)
