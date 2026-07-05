# Copyright (c) 2023 Alex Butler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
import datetime
import gzip
import json
import mmap
import re
import shutil
import sys
import time
import traceback
import uuid
from abc import ABC, abstractmethod
from decimal import Decimal

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

from collections.abc import Iterable, Sequence
from contextlib import contextmanager, suppress
from copy import copy
from dataclasses import dataclass, field
from enum import Enum
from fnmatch import fnmatch
from io import BytesIO
from multiprocessing import Process, Queue
from multiprocessing.connection import Connection
from pathlib import Path
from subprocess import PIPE, Popen
from tempfile import TemporaryFile
from textwrap import dedent, indent
from typing import (
    IO,
    Any,
    cast,
)

import google.cloud.storage as storage
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType
from singer_sdk.exceptions import MissingKeyPropertiesError
from singer_sdk.sinks import BatchSink
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from target_bigquery.constants import DEFAULT_SCHEMA, SDC_FIELDS


class IngestionStrategy(Enum):
    FIXED = "fixed-schema"
    DENORMALIZED = "denormalized-schema"


class ParType(str, Enum):
    THREAD = "Thread"
    PROCESS = "Process"


PARTITION_STRATEGY = {
    "YEAR": TimePartitioningType.YEAR,
    "MONTH": TimePartitioningType.MONTH,
    "DAY": TimePartitioningType.DAY,
    "HOUR": TimePartitioningType.HOUR,
}

TRUE_STRINGS = {"1", "true", "t", "yes", "y", "on"}
FALSE_STRINGS = {"", "0", "false", "f", "no", "n", "off", "none", "null"}


class SchemaResolverVersion(Enum):
    """The schema resolver version to use."""

    V1 = 1
    V2 = 2

    def __str__(self) -> str:
        return str(self.value)


def _normalize_string_selection(selection: str) -> bool | list[Any]:
    normalized = selection.strip()
    lowered = normalized.lower()
    if lowered in TRUE_STRINGS:
        return True
    if lowered in FALSE_STRINGS:
        return False
    if normalized.startswith("["):
        with suppress(json.JSONDecodeError):
            parsed = json.loads(normalized)
            return parsed if isinstance(parsed, list) else bool(parsed)
    return [part.strip() for part in normalized.split(",") if part.strip()]


def _pattern_selection_result(pattern: str, stream_name: str) -> bool | None:
    inverted = pattern.startswith("!")
    pattern = pattern[1:] if inverted else pattern
    if not fnmatch(stream_name, pattern):
        return None
    return not inverted


def _list_selection_matches(selection: list[Any], stream_name: str) -> bool:
    selected = False
    for pattern in selection:
        if not isinstance(pattern, str):
            selected = bool(pattern)
            continue
        pattern_result = _pattern_selection_result(pattern, stream_name)
        selected = selected if pattern_result is None else pattern_result
    return selected


def selection_matches(selection: Any, stream_name: str) -> bool:
    """Return whether a boolean, string, or pattern list selects a stream."""
    if isinstance(selection, bool):
        return selection
    if isinstance(selection, str):
        selection = _normalize_string_selection(selection)
    if isinstance(selection, list):
        return _list_selection_matches(selection, stream_name)
    return bool(selection)


def make_json_compatible(value: Any) -> Any:
    """Return a value compatible with BigQuery JSON upload and load paths."""
    if isinstance(value, Decimal):
        if value.is_finite() and value == value.to_integral_value():
            return int(value)
        if value.is_finite():
            return float(value)
        return str(value)
    if isinstance(value, dict):
        return {key: make_json_compatible(item) for key, item in value.items()}
    if isinstance(value, list):
        return [make_json_compatible(item) for item in value]
    return value


@dataclass
class BigQueryTable:
    name: str
    """The name of the table."""
    dataset: str
    """The dataset that this table belongs to."""
    project: str
    """The project that this table belongs to."""
    jsonschema: dict[str, Any]
    """The jsonschema for this table."""
    ingestion_strategy: IngestionStrategy
    """The ingestion strategy for this table."""
    transforms: dict[str, bool] = field(default_factory=dict)
    """A dict of transformation rules to apply to the table schema."""
    schema_resolver_version: SchemaResolverVersion = SchemaResolverVersion.V1
    timestamp_format: str | None = None

    @property
    def schema_translator(self) -> "SchemaTranslator":
        """Returns a SchemaTranslator instance for this table."""
        if not hasattr(self, "_schema_translator"):
            self._schema_translator = SchemaTranslator(
                schema=self.jsonschema,
                transforms=self.transforms,
                resolver_version=self.schema_resolver_version,
                timestamp_format=self.timestamp_format,
            )
        return self._schema_translator

    def get_schema(self, apply_transforms: bool = False) -> list[bigquery.SchemaField]:
        """Returns the jsonschema to bigquery schema translation for this table."""
        if apply_transforms:
            return self.schema_translator.translated_schema_transformed
        return self.schema_translator.translated_schema

    def get_escaped_name(self, suffix: str = "") -> str:
        """Returns the table name as as escaped SQL string."""
        return f"`{self.project}`.`{self.dataset}`.`{self.name}{suffix}`"

    def get_resolved_schema(self, apply_transforms: bool = False) -> list[bigquery.SchemaField]:
        """Returns the schema for this table after factoring in the ingestion strategy."""
        if self.ingestion_strategy is IngestionStrategy.FIXED:
            return DEFAULT_SCHEMA
        elif self.ingestion_strategy is IngestionStrategy.DENORMALIZED:
            return self.get_schema(apply_transforms)
        else:
            raise ValueError(f"Invalid ingestion strategy: {self.ingestion_strategy}")

    def __str__(self) -> str:
        return f"{self.project}.{self.dataset}.{self.name}"

    def as_ref(self) -> bigquery.TableReference:
        """Returns a TableReference for this table."""
        return bigquery.TableReference.from_string(str(self))

    def as_dataset_ref(self) -> bigquery.DatasetReference:
        """Returns a DatasetReference for this table."""
        return bigquery.DatasetReference(self.project, self.dataset)

    def as_table(self, apply_transforms: bool = False, **kwargs) -> bigquery.Table:
        """Returns a Table instance for this table."""
        if hasattr(self, "_table"):
            return self._table

        table = bigquery.Table(
            self.as_ref(),
            schema=self.get_resolved_schema(apply_transforms),
        )
        config = {**self.default_table_options(), **kwargs}
        for option, value in config.items():
            setattr(table, option, value)
        return table

    def as_dataset(self, **kwargs) -> bigquery.Dataset:
        """Returns a Dataset instance for this dataset."""
        dataset = bigquery.Dataset(self.as_dataset_ref())
        config = {**self.default_dataset_options(), **kwargs}
        for option, value in config.items():
            setattr(dataset, option, value)
        return dataset

    def create_table(
        self,
        client: bigquery.Client,
        apply_transforms: bool = False,
        **kwargs,
    ) -> bool:
        """Creates a dataset and table for this table.

        This is a convenience method that wraps the creation of a dataset and
        table in a single method call. It is idempotent and will not create
        a new table if one already exists."""
        if not hasattr(self, "_dataset"):
            try:
                self._dataset = client.get_dataset(self.as_dataset_ref())
            except NotFound:
                self._dataset = client.create_dataset(self.as_dataset(**kwargs["dataset"]))
        if not hasattr(self, "_table"):
            try:
                self._table = client.get_table(self.as_ref())
            except NotFound:
                self._table = client.create_table(
                    self.as_table(
                        apply_transforms and self.ingestion_strategy != IngestionStrategy.FIXED,
                        **kwargs["table"],
                    )
                )
                # Wait for eventual consistency (for the sake of GRPC's default stream)
                time.sleep(5)

                # the table was created
                return True

        # the table already exists
        return False

    def default_table_options(self) -> dict[str, Any]:
        """Returns the default table options for this table."""
        schema_dump = json.dumps(self.jsonschema)
        return {
            "clustering_fields": ["_sdc_batched_at"],
            "description": (
                "Generated by target-bigquery.\nStream Schema\n{schema}\nBigQuery Ingestion"
                " Strategy: {strategy}".format(
                    schema=(
                        (schema_dump[:16000] + "...") if len(schema_dump) > 16000 else schema_dump
                    ),
                    strategy=self.ingestion_strategy,
                )
            ),
            "time_partitioning": TimePartitioning(
                type_=TimePartitioningType.MONTH, field="_sdc_batched_at"
            ),
        }

    @staticmethod
    def default_dataset_options() -> dict[str, Any]:
        """Returns the default dataset options for this dataset."""
        return {"location": "US"}

    def __hash__(self) -> int:
        return hash((self.name, self.dataset, self.project, json.dumps(self.jsonschema)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BigQueryTable):
            return NotImplemented
        return (
            self.name,
            self.dataset,
            self.project,
            self.jsonschema,
            self.ingestion_strategy,
            self.transforms,
            self.schema_resolver_version,
            self.timestamp_format,
        ) == (
            other.name,
            other.dataset,
            other.project,
            other.jsonschema,
            other.ingestion_strategy,
            other.transforms,
            other.schema_resolver_version,
            other.timestamp_format,
        )


@dataclass
class BigQueryCredentials:
    """BigQuery credentials."""

    path: str | Path | None = None
    json: str | None = None
    project: str | None = None

    def __hash__(self) -> int:
        return hash((self.path, self.json, self.project))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BigQueryCredentials):
            return NotImplemented
        return (self.path, self.json, self.project) == (other.path, other.json, other.project)


class BaseWorker(ABC):
    """Base class for workers."""

    def __init__(
        self,
        ext_id: str,
        queue: "Queue",
        credentials: BigQueryCredentials,
        # 3 connections for job, error, and non-error log notifications
        job_notifier: "Connection",
        error_notifier: "Connection",
        log_notifier: "Connection",
    ):
        super().__init__()
        self.ext_id: str = ext_id
        self.queue: Queue = queue
        self.credentials: BigQueryCredentials = credentials
        self.job_notifier: Connection = job_notifier
        self.error_notifier: Connection = error_notifier
        self.log_notifier: Connection = log_notifier

    @abstractmethod
    def run(self) -> None:
        """Run the worker."""
        raise NotImplementedError

    def serialize_exception(self, exc: Exception) -> str:
        """Serialize an exception to a string."""
        msg = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__, chain=False))
        msg += f"\nWorker ID: {self.ext_id}\n"
        return msg


# Base class for sinks which use a fixed schema, optionally
# including a view which is created on top of the table to unpack the data
class BaseBigQuerySink(BatchSink):
    """BigQuery target sink class, which handles writing streams."""

    include_sdc_metadata_properties: bool = True
    ingestion_strategy = IngestionStrategy.FIXED

    def __init__(
        self,
        target: Any,
        stream_name: str,
        schema: dict[str, Any],
        key_properties: Sequence[str] | None,
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._credentials = BigQueryCredentials(
            self.config.get("credentials_path"),
            self.config.get("credentials_json"),
            self.config["project"],
        )
        self.client = bigquery_client_factory(self._credentials)
        self.table_opts: dict[str, Any] = {
            "project": self.config["project"],
            "dataset": self.config["dataset"],
            "jsonschema": self.schema,
            "transforms": self.config.get("column_name_transforms", {}),
            "ingestion_strategy": self.ingestion_strategy,
            "schema_resolver_version": SchemaResolverVersion(
                self.config.get("schema_resolver_version", 1)
            ),
            "timestamp_format": self.config.get("timestamp_format"),
        }
        self.table = BigQueryTable(name=self.table_name, **self.table_opts)
        self._input_key_properties = list(key_properties or [])
        self._key_properties = self._target_key_properties(key_properties)

        created = self.create_target(key_properties=self.key_properties)
        if not created:
            self.update_schema()

        self.merge_target: BigQueryTable | None = None
        self.overwrite_target: BigQueryTable | None = None
        # In absence of dedupe or overwrite candidacy, we append to the target table directly
        # If the stream is marked for one of these strategies, we create a temporary table instead
        # and merge or overwrite the target table with the temporary table after the ingest.
        if (
            self.key_properties
            and self.ingestion_strategy is IngestionStrategy.DENORMALIZED
            and self._is_upsert_candidate()
        ):
            self.merge_target = copy(self.table)
            self._create_overwrite_table()
        elif self._is_overwrite_candidate():
            self.overwrite_target = copy(self.table)
            self._create_overwrite_table()

        self.global_par_typ = target.par_typ
        self.global_queue = target.queue
        self.increment_jobs_enqueued = target.increment_jobs_enqueued

    def _create_overwrite_table(self) -> None:
        expiration_hours = self.config.get("temporary_table_expiration_hours", 7 * 24)
        self.table = BigQueryTable(
            name=self._temporary_table_name(),
            **self.table_opts,
        )
        self.table.create_table(
            self._get_bigquery_client(),
            self.apply_transforms,
            **{
                "table": {
                    "expires": datetime.datetime.now(datetime.timezone.utc)
                    + datetime.timedelta(hours=expiration_hours),
                },
                "dataset": {
                    "location": self.config.get(
                        "location",
                        BigQueryTable.default_dataset_options()["location"],
                    )
                },
            },
        )
        time.sleep(2.5)  # Wait for eventual consistency

    def _temporary_table_name(self) -> str:
        """Return the configured temporary table name for upsert/overwrite staging."""
        template = self.config.get(
            "temporary_table_name_template",
            "{table_name}__{timestamp}__{uuid}",
        )
        formatted = template.format(
            table_name=self.table_name,
            timestamp=time.strftime("%Y%m%d%H%M%S"),
            uuid=uuid.uuid4().hex,
        )
        return re.sub(r"[^a-zA-Z0-9_]", "_", formatted)

    def _transformed_key_properties(self, key_properties: Sequence[str] | None) -> list[str] | None:
        """Return key properties as BigQuery column names for the active strategy."""
        if not key_properties:
            return None
        if not self.apply_transforms:
            return list(key_properties)
        transforms = {**self.table.transforms, "quote": False}
        return [transform_column_name(key, **transforms) for key in key_properties]

    def _target_key_properties(self, key_properties: Sequence[str] | None) -> list[str]:
        """Return SDK validation keys for the transformed target record shape."""
        if not self.apply_transforms:
            return []
        return self._transformed_key_properties(key_properties) or []

    def _is_upsert_candidate(self) -> bool:
        """Determine if this stream is an upsert candidate based on user configuration."""
        return selection_matches(self.config.get("upsert", False), self.stream_name)

    def _is_overwrite_candidate(self) -> bool:
        """Determine if this stream is an overwrite candidate based on user configuration."""
        return selection_matches(self.config.get("overwrite", False), self.stream_name)

    def _is_dedupe_before_upsert_candidate(self) -> bool:
        """Determine if this stream is a dedupe before upsert candidate based on user configuration."""
        # TODO: we can enable this for `overwrite` too if we want but the purpose would
        # be less for functional reasons (merge constraints) and more for convenience
        return selection_matches(
            self.config.get("dedupe_before_upsert", False),
            self.stream_name,
        )

    @property
    def table_name(self) -> str:
        """Returns the table name."""
        return re.sub(r"[^a-z0-9_]", "_", self.stream_name.lower())

    @property
    def max_size(self) -> int:
        """Maximum size of a batch."""
        return self.config.get("batch_size", 10_000)

    @property
    def apply_transforms(self) -> bool:
        """Whether to apply column name transforms."""
        return self.ingestion_strategy is not IngestionStrategy.FIXED

    @property
    def generate_view(self) -> bool:
        """Whether to generate a view on top of the table."""
        return self.ingestion_strategy is IngestionStrategy.FIXED and self.config.get(
            "generate_view", False
        )

    def _validate_and_parse(self, record: dict) -> dict:
        return record

    def _singer_validate_message(self, record: dict) -> None:
        """Validate key properties against the transformed target record shape."""
        if self.apply_transforms:
            super()._singer_validate_message(record)
            return
        if not self._input_key_properties:
            return
        data = record.get("data")
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                data = {}
        if not isinstance(data, dict) or any(
            key_property not in data for key_property in self._input_key_properties
        ):
            msg = (
                "Record is missing one or more key_properties. \n"
                f"Key Properties: {self._input_key_properties}, "
                f"Record Keys: {list(data.keys()) if isinstance(data, dict) else list(record.keys())}"
            )
            raise MissingKeyPropertiesError(msg)

    def preprocess_record(self, record: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        """Preprocess a record before writing it to the sink."""
        metadata = {
            k: record.pop(k, None)
            for k in (
                "_sdc_extracted_at",
                "_sdc_received_at",
                "_sdc_batched_at",
                "_sdc_deleted_at",
                "_sdc_sequence",
                "_sdc_table_version",
            )
        }
        return {"data": record, **metadata}

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        reraise=True,
    )
    def create_target(self, key_properties: Sequence[str] | None = None) -> bool:
        """Create the table in BigQuery."""
        kwargs: dict[str, dict[str, Any]] = {"table": {}, "dataset": {}}
        # Table opts
        clustering_fields = self.config.get("clustering_fields")
        if clustering_fields:
            kwargs["table"]["clustering_fields"] = tuple(clustering_fields[:4])
        elif key_properties and self.config.get("cluster_on_key_properties", False):
            kwargs["table"]["clustering_fields"] = tuple(key_properties[:4])
        partition_grain: str | None = self.config.get("partition_granularity")
        partition_expiration_days: int | None = self.config.get("partition_expiration_days")
        expiration_ms: int | None = (
            partition_expiration_days * 24 * 60 * 60 * 1000
            if partition_expiration_days is not None
            else None
        )
        if partition_grain:
            kwargs["table"]["time_partitioning"] = TimePartitioning(
                type_=PARTITION_STRATEGY[partition_grain.upper()],
                field="_sdc_batched_at",
                expiration_ms=expiration_ms,
            )
        # Dataset opts
        location: str = self.config.get(
            "location", BigQueryTable.default_dataset_options()["location"]
        )
        kwargs["dataset"]["location"] = location
        # Create the table
        is_created = self.table.create_table(self.client, self.apply_transforms, **kwargs)
        if self.generate_view:
            self.client.query(
                self.table.schema_translator.generate_view_statement(
                    self.table,
                )
            ).result()

        return is_created

    def update_schema(self) -> None:
        """Update the target schema in BigQuery."""
        pass

    def _get_bigquery_client(self) -> bigquery.Client:
        # If gcs_stage method was used, self.client is probably
        # an instance of storage.Client, instead of bigquery.Client
        return (
            self.client
            if isinstance(self.client, bigquery.Client)
            else bigquery_client_factory(self._credentials)
        )

    def pre_state_hook(self) -> None:
        # if we have a merge_target we need to merge the table before writing out state
        # otherwise we might end up with state being moved forward without data being written.
        if self.merge_target:
            self.merge_table(bigquery_client=self._get_bigquery_client())
            self._create_overwrite_table()

    @staticmethod
    @abstractmethod
    def worker_cls_factory(
        worker_executor_cls: type["Process"], config: dict[str, Any]
    ) -> type[BaseWorker] | type["Process"]:
        """Return a worker class for the given parallelization type."""
        raise NotImplementedError

    def merge_table(self, bigquery_client: bigquery.Client) -> None:
        merge_target = self.merge_target
        if merge_target is None:
            raise RuntimeError("merge_table called without a merge target")

        target = bigquery_client.get_table(merge_target.as_ref())
        ordering_columns = ["_sdc_extracted_at", "_sdc_received_at"]
        tmp, ctas_tmp = None, "SELECT 1 AS _no_op"
        if self._is_dedupe_before_upsert_candidate():
            # We can't use MERGE with a non-unique key, so we need to dedupe the temp table into
            # a _SESSION scoped intermediate table.
            tmp = f"{merge_target.name}__tmp"
            dedupe_query = (
                f"SELECT * FROM {self.table.get_escaped_name()} "
                f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {', '.join(f'`{p}`' for p in self.key_properties)} "
                f"ORDER BY {', '.join(f'{c} DESC' for c in ordering_columns)}) = 1"
            )
            ctas_tmp = f"CREATE OR REPLACE TEMP TABLE `{tmp}` AS {dedupe_query}"
        source_columns = {field.name for field in self.table.as_table().schema}
        target_columns = [field.name for field in target.schema if field.name in source_columns]
        if not target_columns:
            raise RuntimeError("merge_table called without shared source and target columns")
        key_properties = self.key_properties or []
        if not key_properties:
            raise RuntimeError("merge_table called without key properties")
        missing_keys = [field for field in key_properties if field not in target_columns]
        if missing_keys:
            raise RuntimeError(
                "merge_table key properties missing from shared schema: " + ", ".join(missing_keys)
            )
        merge_clause = (
            f"MERGE `{merge_target}` AS target USING `{tmp or self.table}` AS source ON "
            + " AND ".join(f"target.`{field}` = source.`{field}`" for field in key_properties)
        )
        update_columns = [field for field in target_columns if field not in key_properties]
        update_clause = (
            "UPDATE SET "
            + ", ".join(f"target.`{field}` = source.`{field}`" for field in update_columns)
            if update_columns
            else None
        )
        insert_clause = (
            f"INSERT ({', '.join(f'`{field}`' for field in target_columns)}) "
            f"VALUES ({', '.join(f'source.`{field}`' for field in target_columns)})"
        )
        matched_clause = f"WHEN MATCHED THEN {update_clause} " if update_clause else ""
        bigquery_client.query(
            f"{ctas_tmp}; {merge_clause} "
            f"{matched_clause}"
            f"WHEN NOT MATCHED THEN {insert_clause}; "
            f"DROP TABLE IF EXISTS {self.table.get_escaped_name()};"
        ).result()

    def clean_up(self) -> None:
        """Clean up the target table."""
        # If gcs_stage method was used, self.client is probably
        # an instance of storage.Client, instead of bigquery.Client
        bigquery_client = self._get_bigquery_client()
        if self.merge_target is not None:
            # We must merge the temp table into the target table.
            self.merge_table(bigquery_client=bigquery_client)
            self.table = self.merge_target
            self.merge_target = None
        elif self.overwrite_target is not None:
            # We must overwrite the target table with the temp table.
            bigquery_client.query(
                f"CREATE OR REPLACE TABLE {self.overwrite_target.get_escaped_name()} AS SELECT *"
                f" FROM {self.table.get_escaped_name()}; DROP TABLE IF EXISTS"
                f" {self.table.get_escaped_name()};"
            ).result()
            self.table = self.overwrite_target
            self.overwrite_target = None

        super().clean_up()


class Denormalized:
    """This class provides common overrides for denormalized sinks and should be subclassed
    with an existing sink with higher MRO: DenormalizedSink(Denormalized, ExistingSink)."""

    ingestion_strategy = IngestionStrategy.DENORMALIZED

    @retry(
        retry=retry_if_exception_type(ConnectionError),
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        reraise=True,
    )
    def update_schema(self) -> None:
        """Update the target schema."""
        sink = cast(BaseBigQuerySink, self)
        bigquery_client = sink._get_bigquery_client()
        table = bigquery_client.get_table(sink.table.as_ref())
        current_schema = table.schema[:]
        mut_schema = table.schema[:]
        for expected_field in sink.table.get_resolved_schema(sink.apply_transforms):
            if not any(field.name == expected_field.name for field in current_schema):
                mut_schema.append(expected_field)
        if len(mut_schema) > len(current_schema):
            table.schema = mut_schema
            bigquery_client.update_table(
                table,
                ["schema"],
                retry=bigquery.DEFAULT_RETRY.with_timeout(15),
            )

    def preprocess_record(
        self,
        record: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        """Preprocess a record before writing it to the sink."""
        sink = cast(BaseBigQuerySink, self)
        return sink.table.schema_translator.translate_record(record)


@contextmanager
def augmented_syspath(new_paths: Iterable[str] | None = None):
    """Context manager to temporarily add paths to sys.path."""
    original_sys_path = sys.path
    if new_paths is not None:
        sys.path = list(new_paths) + sys.path
    try:
        yield
    finally:
        sys.path = original_sys_path


@cache
def bigquery_client_factory(creds: BigQueryCredentials) -> bigquery.Client:
    """Get a BigQuery client."""
    if creds.path:
        return bigquery.Client.from_service_account_json(creds.path, project=creds.project)
    elif creds.json:
        return bigquery.Client.from_service_account_info(
            json.loads(creds.json), project=creds.project
        )
    return bigquery.Client(project=creds.project)


@cache
def gcs_client_factory(creds: BigQueryCredentials) -> storage.Client:
    """Get a GCS client."""
    if creds.path:
        return storage.Client.from_service_account_json(creds.path, project=creds.project)
    elif creds.json:
        return storage.Client.from_service_account_info(
            json.loads(creds.json), project=creds.project
        )
    return storage.Client(project=creds.project)


@cache
def storage_client_factory(
    creds: BigQueryCredentials,
) -> bigquery_storage_v1.BigQueryWriteClient:
    """Get a BigQuery Storage Write client."""
    if creds.path:
        return bigquery_storage_v1.BigQueryWriteClient.from_service_account_file(str(creds.path))
    elif creds.json:
        return bigquery_storage_v1.BigQueryWriteClient.from_service_account_info(
            json.loads(creds.json)
        )
    return bigquery_storage_v1.BigQueryWriteClient()


@dataclass
class _FieldProjection:
    projection: str
    alias: str

    def to_sql(self) -> str:
        """Return the SQL representation of this projection"""
        return f"{self.projection} as {self.alias.lstrip()},\n"


# This class translates a JSON schema into a BigQuery schema.
# It also uses the translated schema to generate a CREATE VIEW statement.
class SchemaTranslator:
    """Translate a JSON schema into a BigQuery schema."""

    def __init__(
        self,
        schema: dict[str, Any],
        transforms: dict[str, bool],
        resolver_version: SchemaResolverVersion = SchemaResolverVersion.V1,
        timestamp_format: str | None = None,
    ) -> None:
        self.schema = schema
        self.transforms = transforms
        self.resolver_version = resolver_version
        self.timestamp_format = timestamp_format
        # Used by fixed schema strategy where we defer transformation
        # to the view statement
        self._translated_schema: list[SchemaField] | None = None
        # Used by the denormalized strategy where we eagerly transform
        # the target schema
        self._translated_schema_transformed: list[SchemaField] | None = None

    @property
    def translated_schema(self) -> list[SchemaField]:
        """Translate the schema into a BigQuery schema."""
        if not self._translated_schema:
            self._translated_schema = [
                self._jsonschema_property_to_bigquery_column(name, contents)
                for name, contents in self.schema.get("properties", {}).items()
            ]
        return self._translated_schema

    @property
    def translated_schema_transformed(self) -> list[SchemaField]:
        """Translate the schema into a BigQuery schema using the SchemaTranslator `transforms`."""
        if not self._translated_schema_transformed:
            self._translated_schema_transformed = [
                self._jsonschema_property_to_bigquery_column(
                    transform_column_name(name, **self.transforms), contents
                )
                for name, contents in self.schema.get("properties", {}).items()
            ]

        field_name_count: dict[str, int] = {}
        for f in self._translated_schema_transformed:
            field_name = f.name
            count = field_name_count.get(field_name, 0)
            if count:
                f._properties["name"] += f"_{count}"
            field_name_count[field_name] = count + 1

        return self._translated_schema_transformed

    def translate_record(self, record: dict) -> dict:
        """Translate a record using the SchemaTranslator `transforms`."""
        if not self.transforms:
            return record
        output = dict(
            [
                (transform_column_name(k, **{**self.transforms, "quote": False}), v)
                for k, v in record.items()
            ]
        )
        for k, v in output.items():
            if isinstance(v, list):
                for i, inner in enumerate(v):
                    if isinstance(inner, dict):
                        output[k][i] = self.translate_record(inner)
            if isinstance(v, dict):
                output[k] = self.translate_record(v)
        return output

    def generate_view_statement(self, table_name: BigQueryTable) -> str:
        """Generate a CREATE VIEW statement for the SchemaTranslator `schema`."""
        projection = ""
        for field_ in self.translated_schema[:]:
            if field_.name.startswith("_sdc_") and field_.name not in SDC_FIELDS:
                continue
            if field_.mode == "REPEATED":
                projection += indent(self._wrap_json_array(field_, path="$", depth=1), " " * 4)
            else:
                projection += indent(self._bigquery_field_to_projection(field_).to_sql(), " " * 4)

        return (
            f"CREATE OR REPLACE VIEW {table_name.get_escaped_name('_view')} AS\nSELECT"
            f"\n{projection} FROM {table_name.get_escaped_name()}"
        )

    def _jsonschema_property_to_bigquery_column(
        self, name: str, schema_property: dict
    ) -> SchemaField:
        """Translate a JSON schema property into a BigQuery column.

        The v1 resolver is very similar to how existing target-bigquery implementations worked.
        The v2 resolver uses JSON in all cases the schema property is unresolvable making it _much_
        more flexible though the denormalization can only be said to be partial if a type is not
        resolved. Most of the time this is fine but for the sake of consistency, we default to v1.
        """
        if self.resolver_version == SchemaResolverVersion.V1:
            return self._jsonschema_property_to_bigquery_column_v1(name, schema_property)
        elif self.resolver_version == SchemaResolverVersion.V2:
            return self._jsonschema_property_to_bigquery_column_v2(name, schema_property)
        else:
            raise ValueError(f"Invalid resolver version: {self.resolver_version}")

    @staticmethod
    def _property_type_and_format(
        schema_property: dict, *, require_type: bool
    ) -> tuple[str | list[str], str | None]:
        """Return the JSON Schema type and format using the first anyOf option."""
        any_of = schema_property.get("anyOf")
        if any_of:
            return any_of[0].get("type", "string"), any_of[0].get("format")

        if require_type:
            return schema_property["type"], schema_property.get("format")

        return schema_property.get("type", "string"), schema_property.get("format")

    def _jsonschema_property_to_bigquery_column_v1(
        self, name: str, schema_property: dict
    ) -> SchemaField:
        """Translate a property using the original strict resolver."""
        property_type, property_format = self._property_type_and_format(
            schema_property, require_type=False
        )

        if "array" in property_type:
            return self._array_jsonschema_property_to_bigquery_column(
                name,
                schema_property,
                fallback_on_untyped_items=False,
                fallback_on_pattern_properties=False,
            )

        if "object" in property_type:
            return self._translate_record_to_bigquery_schema(name, schema_property)

        result_type = bigquery_type(property_type, property_format)
        return SchemaField(name, result_type, "NULLABLE")

    def _jsonschema_property_to_bigquery_column_v2(
        self, name: str, schema_property: dict
    ) -> SchemaField:
        """Translate a property using the lenient JSON fallback resolver."""
        try:
            property_type, property_format = self._property_type_and_format(
                schema_property, require_type=True
            )

            if "array" in property_type:
                return self._array_jsonschema_property_to_bigquery_column(
                    name,
                    schema_property,
                    fallback_on_untyped_items=True,
                    fallback_on_pattern_properties=True,
                )

            if "object" in property_type:
                if self._v2_object_should_fallback_to_json(schema_property):
                    return SchemaField(name, "JSON", "NULLABLE")
                return self._translate_record_to_bigquery_schema(name, schema_property)

            if "patternProperties" in schema_property:
                return SchemaField(name, "JSON", "NULLABLE")

            result_type = bigquery_type(property_type, property_format)
            return SchemaField(name, result_type, "NULLABLE")
        except Exception:
            return SchemaField(name, "JSON", "NULLABLE")

    def _array_jsonschema_property_to_bigquery_column(
        self,
        name: str,
        schema_property: dict,
        *,
        fallback_on_untyped_items: bool,
        fallback_on_pattern_properties: bool,
    ) -> SchemaField:
        """Translate an array property into a repeated BigQuery field."""
        if "items" not in schema_property:
            return SchemaField(name, "JSON", "REPEATED")

        items_schema: dict = schema_property["items"]
        if fallback_on_untyped_items and "type" not in items_schema:
            return SchemaField(name, "JSON", "REPEATED")

        if fallback_on_pattern_properties and "patternProperties" in items_schema:
            return SchemaField(name, "JSON", "REPEATED")

        items_type = bigquery_type(items_schema["type"], items_schema.get("format"))
        if items_type == "record":
            return self._translate_record_to_bigquery_schema(name, items_schema, "REPEATED")
        return SchemaField(name, items_type, "REPEATED")

    @staticmethod
    def _v2_object_should_fallback_to_json(schema_property: dict) -> bool:
        """Return whether the v2 resolver should keep an object as JSON."""
        return (
            "properties" not in schema_property
            or len(schema_property["properties"]) == 0
            or "patternProperties" in schema_property
        )

    def _translate_record_to_bigquery_schema(
        self, name: str, schema_property: dict, mode: str = "NULLABLE"
    ) -> SchemaField:
        """Translate a JSON schema record into a BigQuery schema."""
        properties = list(schema_property.get("properties", {}).items())

        # If no properties defined, store as JSON instead of RECORD
        if len(properties) == 0:
            return SchemaField(name, "JSON", mode)

        fields = [self._jsonschema_property_to_bigquery_column(col, t) for col, t in properties]
        return SchemaField(name, "RECORD", mode, fields=fields)

    def _bigquery_field_to_projection(
        self, field: SchemaField, path: str = "$", depth: int = 0, base: str = "data"
    ) -> _FieldProjection:
        """Translate a BigQuery schema field into a SQL projection."""
        # Pass-through _sdc columns into the projection as-is
        if field.name in SDC_FIELDS:
            return _FieldProjection(field.name, field.name)

        scalar = f"{base}.{field.name}"
        from_base = f"{path}.{field.name}" if base == "data" else "$"

        # Records are handled recursively
        if field.field_type.upper() == "RECORD":
            return _FieldProjection(
                (" " * depth * 2)
                + "STRUCT(\n{}\n".format(
                    "".join(
                        [
                            (
                                self._bigquery_field_to_projection(
                                    f, path=from_base, depth=depth + 1, base=base
                                ).to_sql()
                                if f.mode != "REPEATED"
                                else self._wrap_json_array(
                                    f, path=from_base, depth=depth, base=base
                                )
                            )
                            for f in field.fields
                        ]
                    ).rstrip(",\n"),
                )
                + (" " * depth * 2)
                + ")",
                f"{transform_column_name(field.name, **self.transforms)}",
            )
        # Nullable fields require a JSON_VALUE call which creates a 2-stage cast
        elif field.is_nullable:
            _field = self._wrap_nullable_json_value(field, path=path, base=base)
            return _FieldProjection((" " * depth * 2) + _field.projection, _field.alias)
        # These are not nullable so if the type is known, we can do a 1-stage extract & cast
        elif field.field_type.upper() == "STRING":
            return _FieldProjection(
                (" " * depth * 2) + f"STRING({scalar})",
                f"{transform_column_name(field.name, **self.transforms)}",
            )
        elif field.field_type.upper() == "INTEGER":
            return _FieldProjection(
                (" " * depth * 2) + f"INT64({scalar})",
                f"{transform_column_name(field.name, **self.transforms)}",
            )
        elif field.field_type.upper() == "FLOAT":
            return _FieldProjection(
                (" " * depth * 2) + f"FLOAT64({scalar})",
                f"{transform_column_name(field.name, **self.transforms)}",
            )
        elif field.field_type.upper() == "BOOLEAN":
            return _FieldProjection(
                (" " * depth * 2) + f"BOOL({scalar})",
                f"{transform_column_name(field.name, **self.transforms)}",
            )
        # Fallback to a 2-stage extract & cast
        else:
            _field = self._wrap_nullable_json_value(field, path, base)
            return _FieldProjection((" " * depth * 2) + _field.projection, _field.alias)

    def _wrap_json_array(
        self, field: SchemaField, path: str, depth: int = 0, base: str = "data"
    ) -> str:
        """Translate a BigQuery schema field into a SQL projection for a repeated field."""
        row_base = f"{field.name}__rows"
        _v = (
            self._bigquery_field_to_projection(field, path="$", depth=depth, base=row_base)
            if field.field_type.upper() == "RECORD"
            else self._bigquery_array_element_projection(field, depth=depth, base=row_base)
        )
        v = _v.to_sql().rstrip(", \n")
        return (" " * depth * 2) + indent(
            dedent(
                f"""
        ARRAY(
            SELECT {v}
            FROM UNNEST(
                JSON_QUERY_ARRAY({base}, '{path}.{field.name}')
            ) AS {field.name}__rows
            WHERE {_v.projection} IS NOT NULL
        """
                + (" " * depth * 2)
                + f") AS {field.name},\n"
            ).lstrip(),
            " " * depth * 2,
        )

    def _wrap_nullable_json_value(
        self, field: SchemaField, path: str = "$", base: str = "data"
    ) -> _FieldProjection:
        """Translate a BigQuery schema field into a SQL projection for a nullable field."""
        json_path = f"{path}.{field.name}"
        value = f"JSON_VALUE({base}, '{json_path}')"
        query = f"JSON_QUERY({base}, '{json_path}')"
        return self._json_scalar_projection(field, value, query)

    def _bigquery_array_element_projection(
        self, field: SchemaField, depth: int, base: str
    ) -> _FieldProjection:
        """Translate a repeated scalar BigQuery field from a JSON array element."""
        value = f"JSON_VALUE({base}, '$')"
        query = f"JSON_QUERY({base}, '$')"
        projection = self._json_scalar_projection(field, value, query)
        return _FieldProjection((" " * depth * 2) + projection.projection, projection.alias)

    def _json_scalar_projection(
        self,
        field: SchemaField,
        value_expression: str,
        query_expression: str,
    ) -> _FieldProjection:
        """Translate a JSON scalar extraction expression into a typed SQL projection."""
        typ = field.field_type.upper()
        alias = f" {transform_column_name(field.name, **self.transforms)}"
        if typ == "STRING":
            return _FieldProjection(value_expression, alias)
        if typ == "JSON":
            return _FieldProjection(query_expression, alias)
        if typ == "TIMESTAMP" and self.timestamp_format:
            escaped_format = self.timestamp_format.replace("'", "''")
            return _FieldProjection(
                f"PARSE_TIMESTAMP('{escaped_format}', {value_expression})",
                alias,
            )
        if typ == "FLOAT":
            typ = "FLOAT64"
        if typ in ("INT", "INTEGER"):
            typ = "INT64"
        return _FieldProjection(f"CAST({value_expression} as {typ})", alias)


class Compressor:
    """Compresses streams of bytes using gzip."""

    def __init__(self) -> None:
        self._buffer: IO[bytes] | BytesIO | None
        self._compressor = None
        self._gzip: IO[bytes] | gzip.GzipFile
        self._closed = False
        if shutil.which("gzip") is not None:
            # The buffer outlives __init__ and is closed explicitly in __del__.
            self._buffer = TemporaryFile()  # noqa: SIM115
            self._compressor = Popen(["gzip", "-"], stdin=PIPE, stdout=self._buffer)
            if self._compressor.stdin is None:
                raise RuntimeError("gzip stdin is None")
            self._gzip = self._compressor.stdin
        else:
            self._buffer = BytesIO()
            self._gzip = gzip.GzipFile(fileobj=self._buffer, mode="wb")

    def write(self, data: bytes) -> None:
        """Write data to the compressor."""
        if self._closed:
            raise ValueError("I/O operation on closed compressor.")
        self._gzip.write(data)

    def flush(self) -> None:
        """Flush the compressor buffer."""
        self._gzip.flush()
        self.buffer.flush()

    def close(self) -> None:
        """Close the compressor and wait for the gzip process to finish."""
        if self._closed:
            return
        self._gzip.close()
        if self._compressor is not None:
            self._compressor.wait()
        self.buffer.flush()
        self.buffer.seek(0)
        self._closed = True

    def getvalue(self) -> bytes:
        """Return the compressed buffer as a bytes object."""
        if not self._closed:
            self.close()
        if self._compressor is not None:
            return self.buffer.read()
        return self.buffer.getvalue()  # type: ignore

    def getbuffer(self) -> memoryview | mmap.mmap:
        """Return the compressed buffer as a memoryview or mmap."""
        if not self._closed:
            self.close()
        if self._compressor is not None:
            return mmap.mmap(self.buffer.fileno(), 0, access=mmap.ACCESS_READ)
        return self.buffer.getbuffer()  # type: ignore

    @property
    def buffer(self) -> IO[bytes]:
        """Return the compressed buffer as a file-like object."""
        return cast(IO[bytes], self._buffer)

    def __del__(self) -> None:
        """Close the compressor and wait for the gzip process to finish. Dereference the buffer."""
        self._dispose()

    def _dispose(self) -> None:
        """Release compressor resources."""
        if not self._closed:
            self.close()
        # Ignore incremented ref counts from memoryviews; GC will finish cleanup
        # once the worker dereferences the buffer.
        with suppress(BufferError):
            self.buffer.close()
        if self._compressor is not None and self._compressor.poll() is None:
            self._compressor.kill()
            self._compressor.wait()
        self._buffer = None


# pylint: disable=no-else-return,too-many-branches,too-many-return-statements
def bigquery_type(property_type: str | list[str], property_format: str | None = None) -> str:
    """Convert a JSON Schema type to a BigQuery type."""
    if property_format == "date-time":
        return "timestamp"
    if property_format == "date":
        return "date"
    elif property_format == "time":
        return "time"
    elif "number" in property_type:
        return "float"
    elif "integer" in property_type and "string" in property_type:
        return "string"
    elif "integer" in property_type:
        return "integer"
    elif "boolean" in property_type:
        return "boolean"
    elif "object" in property_type:
        return "record"
    else:
        return "string"


# Column name transforms are configurable and entirely opt-in.
# This allows users to only use the transforms they need and not
# become dependent on inconfigurable transforms outside their
# realm of control which must be recreated if migrating loaders.
@cache
def transform_column_name(
    name: str,
    quote: bool = False,
    lower: bool = False,
    add_underscore_when_invalid: bool = False,
    snake_case: bool = False,
    replace_period_with_underscore: bool = False,
    **kwargs,
) -> str:
    """Transform a column name to a valid BigQuery column name.
    kwargs is here to handle unspecified column tranforms, this can become an issue if config is added in main
    branch but code is versioned to run on an old commit.
    """
    if snake_case and not lower:
        lower = True
    was_quoted = name.startswith("`") and name.endswith("`")
    name = name.strip("`")
    if snake_case:
        name = re.sub("((?!^)(?<!_)[A-Z][a-z]+|(?<=[a-z0-9])[A-Z])", r"_\1", name)
    if lower:
        name = f"{name}".lower()
    if add_underscore_when_invalid and name and name[0].isdigit():
        name = f"_{name}"
    if quote or was_quoted:
        name = f"`{name}`"
    if replace_period_with_underscore:
        name = name.replace(".", "_")
    return name
