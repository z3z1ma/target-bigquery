"""Tests standard target features using the built-in SDK tests library."""
import os

from singer_sdk.testing import get_standard_target_tests
from target_bigquery.target import TargetBigQuery


# Run standard built-in target tests from the SDK:
def test_standard_target_tests():
    """Run standard target tests from the SDK."""
    tests = get_standard_target_tests(
        TargetBigQuery,
        config={
            "credentials_json": os.environ["BQ_CREDS"],
            "project": os.environ["BQ_PROJECT"],
            "dataset": os.environ["BQ_DATASET"],
        },
    )
    for test in tests:
        test()
