"""Tests standard target features using the built-in SDK test class."""

import os

import pytest
from singer_sdk.testing import get_target_test_class

from target_bigquery.target import TargetBigQuery

REQUIRED_ENV_VARS = ("BQ_CREDS", "BQ_PROJECT", "BQ_DATASET")
MISSING_ENV_VARS = [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]

if MISSING_ENV_VARS:

    def test_standard_target_tests_require_bigquery_env():
        pytest.skip(f"Missing BigQuery integration env vars: {', '.join(MISSING_ENV_VARS)}")

else:
    TestTargetBigQuery = get_target_test_class(
        TargetBigQuery,
        config={
            "credentials_json": os.environ["BQ_CREDS"],
            "project": os.environ["BQ_PROJECT"],
            "dataset": os.environ["BQ_DATASET"],
        },
    )
