"""Shared test fixtures."""

import os

import pytest


@pytest.fixture
def bigquery_config() -> dict[str, str]:
    """Return BigQuery config or skip when integration credentials are unavailable."""
    env_to_config = {
        "BQ_CREDS": "credentials_json",
        "BQ_PROJECT": "project",
        "BQ_DATASET": "dataset",
    }
    missing = [env_name for env_name in env_to_config if not os.environ.get(env_name)]
    if missing:
        pytest.skip(f"Missing BigQuery integration env vars: {', '.join(missing)}")
    return {config_key: os.environ[env_name] for env_name, config_key in env_to_config.items()}


@pytest.fixture
def bigquery_gcs_config(bigquery_config: dict[str, str]) -> dict[str, str]:
    """Return BigQuery and GCS config or skip when integration credentials are unavailable."""
    if not os.environ.get("GCS_BUCKET"):
        pytest.skip("Missing BigQuery integration env vars: GCS_BUCKET")
    return {**bigquery_config, "bucket": os.environ["GCS_BUCKET"]}
