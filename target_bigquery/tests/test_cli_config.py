"""Exercise file and direct SDK environment configuration through the Click CLI."""

import copy
import json
import os

import pytest
from click.testing import CliRunner
from singer_sdk.configuration import _dict_config

from target_bigquery import core, storage_write
from target_bigquery.target import TargetBigQuery


@pytest.fixture
def cli_config(monkeypatch, tmp_path, caplog):
    # Isolate user settings and dotenv discovery; never read local credentials.
    monkeypatch.setattr(
        os,
        "environ",
        {
            name: value
            for name, value in os.environ.items()
            if not name.startswith("TARGET_BIGQUERY_")
        },
    )
    dotenv = tmp_path / ".env"
    dotenv.write_text("")
    monkeypatch.setattr(_dict_config, "find_dotenv", lambda: str(dotenv))
    observed = []
    initialize = TargetBigQuery.__init__

    def observe(self, *args, **kwargs):
        initialize(self, *args, **kwargs)
        observed.append(dict(self.config))

    def no_api(*args, **kwargs):
        pytest.fail("empty-input config smoke test must not construct an API client")

    monkeypatch.setattr(TargetBigQuery, "__init__", observe)
    monkeypatch.setattr(core, "bigquery_client_factory", no_api)
    monkeypatch.setattr(storage_write, "storage_client_factory", no_api)
    yield CliRunner(), observed
    assert "Config validation failed" not in caplog.text


def test_meltano_style_typed_json_config_completes_empty_input(cli_config, tmp_path):
    runner, observed = cli_config
    config = {
        "project": "offline-project",
        "dataset": "offline_dataset",
        "method": "storage_write_api",
        "denormalized": True,
        "batch_size": 500,
        "column_name_transforms": {"snake_case": True},
        "options": {"process_pool": False, "max_workers": 2},
        "upsert": ["orders*", "!orders_history"],
    }
    path = tmp_path / "config.json"
    path.write_text(json.dumps(config))
    result = runner.invoke(TargetBigQuery.cli, ["--config", str(path)], input="")
    assert result.exit_code == 0, result.output
    assert len(observed) == 1
    for key, value in config.items():
        if isinstance(value, dict):
            assert all(observed[0][key][child] == item for child, item in value.items())
        else:
            assert observed[0][key] == value
    assert observed[0]["options"]["process_pool"] is False
    assert observed[0]["options"]["storage_write_batch_mode"] is False
    assert "Config validation failed" not in result.output
    assert observed[0]["timeout"] == 600


def test_direct_env_config_casts_scalars_and_nested_json_objects(cli_config):
    runner, observed = cli_config
    result = runner.invoke(
        TargetBigQuery.cli,
        ["--config", "ENV"],
        input="",
        env={
            "TARGET_BIGQUERY_PROJECT": "offline-project",
            "TARGET_BIGQUERY_DATASET": "offline_dataset",
            "TARGET_BIGQUERY_DENORMALIZED": "false",
            "TARGET_BIGQUERY_BATCH_SIZE": "250",
            "TARGET_BIGQUERY_OPTIONS": '{"process_pool":false,"max_workers":2}',
            "TARGET_BIGQUERY_COLUMN_NAME_TRANSFORMS": '{"snake_case":true}',
            "TARGET_BIGQUERY_OVERWRITE": "false",
        },
    )
    assert result.exit_code == 0, result.output
    assert len(observed) == 1
    config = observed[0]
    assert config["denormalized"] is False
    assert config["batch_size"] == 250
    assert config["options"] == {
        "process_pool": False,
        "max_workers": 2,
        "storage_write_batch_mode": False,
    }
    assert config["column_name_transforms"]["snake_case"] is True
    assert config["column_name_transforms"]["lower"] is False
    assert config["overwrite"] is False
    assert not core.selection_matches(config["overwrite"], "orders")
    assert "Config validation failed" not in result.output


@pytest.mark.parametrize("env_first", [False, True])
def test_env_overrides_files_in_either_argument_order(cli_config, tmp_path, env_first):
    runner, observed = cli_config
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "project": "offline-project",
                "dataset": "from_file",
                "options": {"process_pool": False, "max_workers": 4},
            }
        )
    )
    paths = ["ENV", str(path)] if env_first else [str(path), "ENV"]
    args = [arg for path in paths for arg in ("--config", path)]
    result = runner.invoke(
        TargetBigQuery.cli,
        args,
        input="",
        env={
            "TARGET_BIGQUERY_DATASET": "from_env",
            "TARGET_BIGQUERY_OPTIONS": '{"process_pool":false}',
        },
    )
    assert result.exit_code == 0, result.output
    assert observed[0]["dataset"] == "from_env"
    # Top-level object settings replace the file object; this is not a deep merge.
    assert observed[0]["options"]["process_pool"] is False
    assert observed[0]["options"]["storage_write_batch_mode"] is False
    assert "max_workers" not in observed[0]["options"]


def test_malformed_env_object_fails_before_target_initialization(cli_config):
    runner, observed = cli_config
    result = runner.invoke(
        TargetBigQuery.cli,
        ["--config", "ENV"],
        input="",
        env={
            "TARGET_BIGQUERY_PROJECT": "offline-project",
            "TARGET_BIGQUERY_DATASET": "offline_dataset",
            "TARGET_BIGQUERY_OPTIONS": "process_pool=false",
        },
    )
    assert result.exit_code != 0
    assert observed == []
    assert isinstance(result.exception, ValueError)


@pytest.mark.parametrize("setting", ["upsert", "overwrite", "dedupe_before_upsert"])
@pytest.mark.parametrize("pattern", ["orders*,!orders_history", '["orders*", "!orders_history"]'])
def test_direct_env_preserves_stream_selection_patterns(cli_config, setting, pattern):
    runner, observed = cli_config
    schema = copy.deepcopy(TargetBigQuery.config_jsonschema["properties"][setting])
    result = runner.invoke(
        TargetBigQuery.cli,
        ["--config", "ENV"],
        input="",
        env={
            "TARGET_BIGQUERY_PROJECT": "offline-project",
            "TARGET_BIGQUERY_DATASET": "offline_dataset",
            "TARGET_BIGQUERY_" + setting.upper(): pattern,
        },
    )
    assert result.exit_code == 0, result.output
    assert core.selection_matches(observed[0][setting], "orders_current")
    assert not core.selection_matches(observed[0][setting], "orders_history")
    assert not core.selection_matches(observed[0][setting], "customers")
    assert TargetBigQuery.config_jsonschema["properties"][setting] == schema


@pytest.mark.parametrize("setting", ["upsert", "overwrite", "dedupe_before_upsert"])
@pytest.mark.parametrize(
    "value,selected",
    [
        ("true", True),
        ("false", False),
        ("YES", True),
        ("1", True),
        ("off", False),
        ("y", False),
        (" true ", False),
    ],
)
def test_direct_env_selection_booleans_keep_their_meaning(cli_config, setting, value, selected):
    runner, observed = cli_config
    result = runner.invoke(
        TargetBigQuery.cli,
        ["--config", "ENV"],
        input="",
        env={
            "TARGET_BIGQUERY_PROJECT": "offline-project",
            "TARGET_BIGQUERY_DATASET": "offline_dataset",
            "TARGET_BIGQUERY_" + setting.upper(): value,
        },
    )
    assert result.exit_code == 0, result.output
    assert core.selection_matches(observed[0][setting], "orders") is selected


def test_dotenv_stream_pattern_uses_same_cli_parser(cli_config, tmp_path):
    runner, observed = cli_config
    (tmp_path / ".env").write_text(
        "TARGET_BIGQUERY_PROJECT=offline-project\n"
        "TARGET_BIGQUERY_DATASET=offline_dataset\n"
        "TARGET_BIGQUERY_UPSERT=orders*,!orders_history\n"
    )
    result = runner.invoke(TargetBigQuery.cli, ["--config", "ENV"], input="")
    assert result.exit_code == 0, result.output
    assert core.selection_matches(observed[0]["upsert"], "orders_current")
    assert not core.selection_matches(observed[0]["upsert"], "orders_history")


def test_subclass_environment_prefix_preserves_patterns(cli_config):
    class RenamedTarget(TargetBigQuery):
        name = "target-config-probe"

    runner, observed = cli_config
    result = runner.invoke(
        RenamedTarget.cli,
        ["--config", "ENV"],
        input="",
        env={
            "TARGET_CONFIG_PROBE_PROJECT": "offline-project",
            "TARGET_CONFIG_PROBE_DATASET": "offline_dataset",
            "TARGET_CONFIG_PROBE_UPSERT": "orders*,!orders_history",
        },
    )
    assert result.exit_code == 0, result.output
    assert core.selection_matches(observed[0]["upsert"], "orders_current")
    assert not core.selection_matches(observed[0]["upsert"], "orders_history")
