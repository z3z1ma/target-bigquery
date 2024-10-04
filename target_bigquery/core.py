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

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

from contextlib import contextmanager
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
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_storage_v1, storage
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType
from singer_sdk.sinks import BatchSink
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from target_bigquery.constants import DEFAULT_SCHEMA

if TYPE_CHECKING:
    from target_bigquery.target import TargetBigQuery


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


class SchemaResolverVersion(Enum):
    """The schema resolver version to use."""

    V1 = 1
    V2 = 2

    def __str__(self) -> str:
        return str(self.value)


@dataclass
class BigQueryTable:
    name: str
    """The name of the table."""
    dataset: str
    """The dataset that this table belongs to."""
    project: str
    """The project that this table belongs to."""
    jsonschema: Dict[str, Any]
    """The jsonschema for this table."""
    ingestion_strategy: IngestionStrategy
    """The ingestion strategy for this table."""
    transforms: Dict[str, bool] = field(default_factory=dict)
    """A dict of transformation rules to apply to the table schema."""
    schema_resolver_version: SchemaResolverVersion = SchemaResolverVersion.V1

    @property
    def schema_translator(self) -> "SchemaTranslator":
        """Returns a SchemaTranslator instance for this table."""
        if not hasattr(self, "_schema_translator"):
            self._schema_translator = SchemaTranslator(
                schema=self.jsonschema,
                transforms=self.transforms,
                resolver_version=self.schema_resolver_version,
            )
        return self._schema_translator

    def get_schema(self, apply_transforms: bool = False) -> List[bigquery.SchemaField]:
        """Returns the jsonschema to bigquery schema translation for this table."""
        if apply_transforms:
            return self.schema_translator.translated_schema_transformed
        return self.schema_translator.translated_schema

    def get_escaped_name(self, suffix: str = "") -> str:
        """Returns the table name as as escaped SQL string."""
        return f"`{self.project}`.`{self.dataset}`.`{self.name}{suffix}`"

    def get_resolved_schema(
        self, apply_transforms: bool = False
    ) -> List[bigquery.SchemaField]:
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

    @cache
    def as_table(self, apply_transforms: bool = False, **kwargs) -> bigquery.Table:
        """Returns a Table instance for this table."""
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
    ) -> Tuple[bigquery.Dataset, bigquery.Table]:
        """Creates a dataset and table for this table.

        This is a convenience method that wraps the creation of a dataset and
        table in a single method call. It is idempotent and will not create
        a new table if one already exists."""
        if not hasattr(self, "_dataset"):
            try:
                self._dataset = client.get_dataset(self.as_dataset_ref())
            except NotFound:
                self._dataset = client.create_dataset(
                    self.as_dataset(**kwargs["dataset"])
                )
        if not hasattr(self, "_table"):
            try:
                self._table = client.get_table(self.as_ref())
            except NotFound:
                self._table = client.create_table(
                    self.as_table(
                        apply_transforms
                        and self.ingestion_strategy != IngestionStrategy.FIXED,
                        **kwargs["table"],
                    )
                )
                # Wait for eventual consistency (for the sake of GRPC's default stream)
                time.sleep(5)
        return self._dataset, self._table

    def get_current_schema(self) -> List[bigquery.SchemaField]:
        return self._table.schema

    def default_table_options(self) -> Dict[str, Any]:
        """Returns the default table options for this table."""
        schema_dump = json.dumps(self.jsonschema)
        return {
            "clustering_fields": ["_sdc_batched_at"],
            "description": (
                "Generated by target-bigquery.\nStream Schema\n{schema}\nBigQuery Ingestion"
                " Strategy: {strategy}".format(
                    schema=(
                        (schema_dump[:16000] + "...")
                        if len(schema_dump) > 16000
                        else schema_dump
                    ),
                    strategy=self.ingestion_strategy,
                )
            ),
            "time_partitioning": TimePartitioning(
                type_=TimePartitioningType.MONTH, field="_sdc_batched_at"
            ),
        }

    @staticmethod
    def default_dataset_options() -> Dict[str, Any]:
        """Returns the default dataset options for this dataset."""
        return {"location": "US"}

    def __hash__(self) -> int:
        return hash(
            (self.name, self.dataset, self.project, json.dumps(self.jsonschema))
        )


@dataclass
class BigQueryCredentials:
    """BigQuery credentials."""

    path: Optional[Union[str, Path]] = None
    json: Optional[str] = None
    project: Optional[str] = None

    def __hash__(self) -> int:
        return hash((self.path, self.json, self.project))


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
        """Initialize a worker."""
        super().__init__()
        self.ext_id: str = ext_id
        self.queue: "Queue" = queue
        self.credentials: BigQueryCredentials = credentials
        self.job_notifier: "Connection" = job_notifier
        self.error_notifier: "Connection" = error_notifier
        self.log_notifier: "Connection" = log_notifier

    @abstractmethod
    def run(self) -> None:
        """Run the worker."""
        raise NotImplementedError

    def serialize_exception(self, exc: Exception) -> str:
        """Serialize an exception to a string."""
        msg = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__, chain=False)
        )
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
        target: "TargetBigQuery",
        stream_name: str,
        schema: Dict[str, Any],
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize the sink."""
        super().__init__(target, stream_name, schema, key_properties)
        self._credentials = BigQueryCredentials(
            self.config.get("credentials_path"),
            self.config.get("credentials_json"),
            self.config["project"],
        )
        self.client = bigquery_client_factory(self._credentials)
        self.table_opts = {
            "project": self.config["project"],
            "dataset": self.config["dataset"],
            "jsonschema": self.schema,
            "transforms": self.config.get("column_name_transforms", {}),
            "ingestion_strategy": self.ingestion_strategy,
            "schema_resolver_version": SchemaResolverVersion(
                self.config.get("schema_resolver_version", 1)
            ),
        }
        self.table = BigQueryTable(name=self.table_name, **self.table_opts)
        self.create_target(key_properties=key_properties)
        self.update_schema()
        self.merge_target: Optional[BigQueryTable] = None
        self.overwrite_target: Optional[BigQueryTable] = None
        # In absence of dedupe or overwrite candidacy, we append to the target table directly
        # If the stream is marked for one of these strategies, we create a temporary table instead
        # and merge or overwrite the target table with the temporary table after the ingest.
        if (
            key_properties
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
        self.table = BigQueryTable(
            name=f"{self.table_name}__{time.strftime('%Y%m%d%H%M%S')}__{uuid.uuid4()}",
            **self.table_opts,
        )
        self.table.create_table(
            self.client,
            self.apply_transforms,
            **{
                "table": {
                    "expires": datetime.datetime.now() + datetime.timedelta(days=1),
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

    def _is_upsert_candidate(self) -> bool:
        """Determine if this stream is an upsert candidate based on user configuration."""
        upsert_selection = self.config.get("upsert", False)
        upsert_candidate = False
        if isinstance(upsert_selection, list):
            selection: str
            for selection in upsert_selection:
                invert = selection.startswith("!")
                if invert:
                    selection = selection[1:]
                if fnmatch(self.stream_name, selection):
                    upsert_candidate = True ^ invert
        elif upsert_selection:
            upsert_candidate = True
        return upsert_candidate

    def _is_overwrite_candidate(self) -> bool:
        """Determine if this stream is an overwrite candidate based on user configuration."""
        overwrite_selection = self.config.get("overwrite", False)
        overwrite_candidate = False
        if isinstance(overwrite_selection, list):
            selection: str
            for selection in overwrite_selection:
                invert = selection.startswith("!")
                if invert:
                    selection = selection[1:]
                if fnmatch(self.stream_name, selection):
                    overwrite_candidate = True ^ invert
        elif overwrite_selection:
            overwrite_candidate = True
        return overwrite_candidate

    def _is_dedupe_before_upsert_candidate(self) -> bool:
        """Determine if this stream is a dedupe before upsert candidate based on user configuration."""
        # TODO: we can enable this for `overwrite` too if we want but the purpose would
        # be less for functional reasons (merge constraints) and more for convenience
        dedupe_before_upsert_selection = self.config.get("dedupe_before_upsert", False)
        dedupe_before_upsert_candidate = False
        if isinstance(dedupe_before_upsert_selection, list):
            selection: str
            for selection in dedupe_before_upsert_selection:
                invert = selection.startswith("!")
                if invert:
                    selection = selection[1:]
                if fnmatch(self.stream_name, selection):
                    dedupe_before_upsert_candidate = True ^ invert
        elif dedupe_before_upsert_selection:
            dedupe_before_upsert_candidate = True
        return dedupe_before_upsert_candidate

    @property
    def table_name(self) -> str:
        """Returns the table name."""
        return self.stream_name.lower().replace("-", "_").replace(".", "_")

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

    def preprocess_record(
        self, record: Dict[str, Any], context: Dict[str, Any]
    ) -> Dict[str, Any]:
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
    def create_target(self, key_properties: Optional[List[str]] = None) -> None:
        """Create the table in BigQuery."""
        kwargs = {"table": {}, "dataset": {}}
        # Table opts
        if key_properties and self.config.get("cluster_on_key_properties", False):
            kwargs["table"]["clustering_fields"] = tuple(key_properties[:4])
        partition_grain: Optional[str] = self.config.get("partition_granularity")
        if partition_grain:
            kwargs["table"]["time_partitioning"] = TimePartitioning(
                type_=PARTITION_STRATEGY[partition_grain.upper()],
                field="_sdc_batched_at",
            )
        # Dataset opts
        location: str = self.config.get(
            "location", BigQueryTable.default_dataset_options()["location"]
        )
        kwargs["dataset"]["location"] = location
        # Create the table
        self.table.create_table(self.client, self.apply_transforms, **kwargs)
        if self.generate_view:
            self.client.query(
                self.table.schema_translator.generate_view_statement(
                    self.table,
                )
            ).result()

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
        worker_executor_cls: Type["Process"], config: Dict[str, Any]
    ) -> Union[Type[BaseWorker], Type["Process"]]:
        """Return a worker class for the given parallelization type."""
        raise NotImplementedError

    def merge_table(self, bigquery_client:bigquery.Client) -> None:
        target = self.merge_target.as_table()
        date_columns = ["_sdc_extracted_at", "_sdc_received_at"]
        tmp, ctas_tmp = None, "SELECT 1 AS _no_op"
        if self._is_dedupe_before_upsert_candidate():
            # We can't use MERGE with a non-unique key, so we need to dedupe the temp table into
            # a _SESSION scoped intermediate table.
            tmp = f"{self.merge_target.name}__tmp"
            dedupe_query = (
                f"SELECT * FROM {self.table.get_escaped_name()} "
                f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {', '.join(f'`{p}`' for p in self.key_properties)} "
                f"ORDER BY COALESCE({', '.join(date_columns)}) DESC) = 1"
            )
            ctas_tmp = f"CREATE OR REPLACE TEMP TABLE `{tmp}` AS {dedupe_query}"
        merge_clause = (
                f"MERGE `{self.merge_target}` AS target USING `{tmp or self.table}` AS source ON "
                + " AND ".join(
            f"target.`{f}` = source.`{f}`" for f in self.key_properties
        )
        )
        update_clause = "UPDATE SET " + ", ".join(
            f"target.`{f.name}` = source.`{f.name}`" for f in target.schema
        )
        insert_clause = (
            f"INSERT ({', '.join(f'`{f.name}`' for f in target.schema)}) "
            f"VALUES ({', '.join(f'source.`{f.name}`' for f in target.schema)})"
        )
        bigquery_client.query(
            f"{ctas_tmp}; {merge_clause} "
            f"WHEN MATCHED THEN {update_clause} "
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
            # Do it in a transaction to avoid partial writes.
            target = self.overwrite_target.as_table()
            bigquery_client.query(
                f"DROP TABLE IF EXISTS {self.overwrite_target.get_escaped_name()}; CREATE TABLE"
                f" {self.overwrite_target.get_escaped_name()} AS SELECT * FROM"
                f" {self.table.get_escaped_name()}; DROP TABLE IF EXISTS"
                f" {self.table.get_escaped_name()};"
            ).result()
            self.table = cast(BigQueryTable, self.merge_target)
            self.overwrite_target = None


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
    def update_schema(self: BaseBigQuerySink) -> None:  # type: ignore
        """Update the target schema."""
        table = self.table.as_table()
        resolved_schema = self.table.get_resolved_schema(self.apply_transforms)
        current_schema = self.table.get_current_schema()[:]
        mut_schema = current_schema[:]
        for expected_field in resolved_schema:
            if not any(field.name == expected_field.name for field in mut_schema):
                mut_schema.append(expected_field)
        if len(mut_schema) > len(current_schema):
            table.schema = mut_schema
            self.client.update_table(
                table,
                ["schema"],
                retry=bigquery.DEFAULT_RETRY.with_timeout(15),
            )

    def preprocess_record(
        self: BaseBigQuerySink,  # type: ignore
        record: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Preprocess a record before writing it to the sink."""
        return self.table.schema_translator.translate_record(record)


@contextmanager
def augmented_syspath(new_paths: Optional[Iterable[str]] = None):
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
        return bigquery.Client.from_service_account_json(
            creds.path, project=creds.project
        )
    elif creds.json:
        return bigquery.Client.from_service_account_info(
            json.loads(creds.json), project=creds.project
        )
    return bigquery.Client(project=creds.project)


@cache
def gcs_client_factory(creds: BigQueryCredentials) -> storage.Client:
    """Get a GCS client."""
    if creds.path:
        return storage.Client.from_service_account_json(
            creds.path, project=creds.project
        )
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
        return bigquery_storage_v1.BigQueryWriteClient.from_service_account_file(
            str(creds.path)
        )
    elif creds.json:
        return bigquery_storage_v1.BigQueryWriteClient.from_service_account_info(
            json.loads(creds.json)
        )
    return bigquery_storage_v1.BigQueryWriteClient()


@dataclass
class _FieldProjection:
    projection: str
    alias: str

    def as_sql(self) -> str:
        """Return the SQL representation of this projection"""
        return f"{self.projection} as {self.alias.lstrip()},\n"


# This class translates a JSON schema into a BigQuery schema.
# It also uses the translated schema to generate a CREATE VIEW statement.
class SchemaTranslator:
    """Translate a JSON schema into a BigQuery schema."""

    def __init__(
        self,
        schema: Dict[str, Any],
        transforms: Dict[str, bool],
        resolver_version: SchemaResolverVersion = SchemaResolverVersion.V1,
    ) -> None:
        self.schema = schema
        self.transforms = transforms
        self.resolver_version = resolver_version
        # Used by fixed schema strategy where we defer transformation
        # to the view statement
        self._translated_schema = None
        # Used by the denormalized strategy where we eagerly transform
        # the target schema
        self._translated_schema_transformed = None

    @property
    def translated_schema(self) -> List[SchemaField]:
        """Translate the schema into a BigQuery schema."""
        if not self._translated_schema:
            self._translated_schema = [
                self._jsonschema_property_to_bigquery_column(name, contents)
                for name, contents in self.schema.get("properties", {}).items()
            ]
        return self._translated_schema

    @property
    def translated_schema_transformed(self) -> List[SchemaField]:
        """Translate the schema into a BigQuery schema using the SchemaTranslator `transforms`."""
        if not self._translated_schema_transformed:
            self._translated_schema_transformed = [
                self._jsonschema_property_to_bigquery_column(
                    transform_column_name(name, **self.transforms), contents
                )
                for name, contents in self.schema.get("properties", {}).items()
            ]
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
            if field_.mode == "REPEATED":
                projection += indent(
                    self._wrap_json_array(field_, path="$", depth=1), " " * 4
                )
            else:
                projection += indent(
                    self._bigquery_field_to_projection(field_).as_sql(), " " * 4
                )

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
            # This is the original resolver, which is used by the denormalized strategy
            if "anyOf" in schema_property and len(schema_property["anyOf"]) > 0:
                # I have only seen this used in the wild with tap-salesforce, which
                # is incidentally an important one so lets handle the anyOf case
                # by giving the 0th index priority.
                property_type = schema_property["anyOf"][0].get("type", "string")
                property_format = schema_property["anyOf"][0].get("format", None)
            else:
                property_type = schema_property.get("type", "string")
                property_format = schema_property.get("format", None)

            if "array" in property_type:
                if "items" not in schema_property:
                    return SchemaField(name, "JSON", "REPEATED")
                items_schema: dict = schema_property["items"]
                items_type = bigquery_type(
                    items_schema["type"], items_schema.get("format", None)
                )
                if items_type == "record":
                    return self._translate_record_to_bigquery_schema(
                        name, items_schema, "REPEATED"
                    )
                return SchemaField(name, items_type, "REPEATED")
            elif "object" in property_type:
                return self._translate_record_to_bigquery_schema(name, schema_property)
            else:
                result_type = bigquery_type(property_type, property_format)
                return SchemaField(name, result_type, "NULLABLE")
        elif self.resolver_version == SchemaResolverVersion.V2:
            # This is the new resolver, which is far more lenient and falls back to JSON
            # if it doesn't know how to translate a property.
            try:
                if "anyOf" in schema_property and len(schema_property["anyOf"]) > 0:
                    # I have only seen this used in the wild with tap-salesforce, which
                    # is incidentally an important one so lets handle the anyOf case
                    # by giving the 0th index priority.
                    property_type = schema_property["anyOf"][0].get("type", "string")
                    property_format = schema_property["anyOf"][0].get("format", None)
                else:
                    property_type = schema_property["type"]
                    property_format = schema_property.get("format", None)

                if "array" in property_type:
                    if (
                        "items" not in schema_property
                        or "type" not in schema_property["items"]
                    ):
                        return SchemaField(name, "JSON", "REPEATED")
                    items_schema: dict = schema_property["items"]
                    if "patternProperties" in items_schema:
                        return SchemaField(name, "JSON", "REPEATED")
                    items_type = bigquery_type(
                        items_schema["type"], items_schema.get("format", None)
                    )
                    if items_type == "record":
                        return self._translate_record_to_bigquery_schema(
                            name, items_schema, "REPEATED"
                        )
                    return SchemaField(name, items_type, "REPEATED")
                elif "object" in property_type:
                    if (
                        "properties" not in schema_property
                        or len(schema_property["properties"]) == 0
                        or "patternProperties" in schema_property
                    ):
                        return SchemaField(name, "JSON", "NULLABLE")
                    return self._translate_record_to_bigquery_schema(
                        name, schema_property
                    )
                else:
                    if "patternProperties" in schema_property:
                        return SchemaField(name, "JSON", "NULLABLE")
                    result_type = bigquery_type(property_type, property_format)
                    return SchemaField(name, result_type, "NULLABLE")
            except Exception:
                return SchemaField(name, "JSON", "NULLABLE")
        else:
            raise ValueError(f"Invalid resolver version: {self.resolver_version}")

    def _translate_record_to_bigquery_schema(
        self, name: str, schema_property: dict, mode: str = "NULLABLE"
    ) -> SchemaField:
        """Translate a JSON schema record into a BigQuery schema."""
        properties = list(schema_property.get("properties", {}).items())

        # If no properties defined, store as JSON instead of RECORD
        if len(properties) == 0:
            return SchemaField(name, "JSON", mode)

        fields = [
            self._jsonschema_property_to_bigquery_column(col, t)
            for col, t in properties
        ]
        return SchemaField(name, "RECORD", mode, fields=fields)

    def _bigquery_field_to_projection(
        self, field: SchemaField, path: str = "$", depth: int = 0, base: str = "data"
    ) -> _FieldProjection:
        """Translate a BigQuery schema field into a SQL projection."""
        # Pass-through _sdc columns into the projection as-is
        if field.name.startswith("_sdc_"):
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
                                ).as_sql()
                                if not f.mode == "REPEATED"
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
        _v = self._bigquery_field_to_projection(
            field, path="$", depth=depth, base=f"{field.name}__rows"
        )
        v = _v.as_sql().rstrip(", \n")
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
        typ = field.field_type.upper()
        if typ == "STRING":
            return _FieldProjection(
                f"JSON_VALUE({base}, '{path}.{field.name}')",
                f" {transform_column_name(field.name, **self.transforms)}",
            )
        if typ == "FLOAT":
            typ = "FLOAT64"
        if typ in ("INT", "INTEGER"):
            typ = "INT64"
        return _FieldProjection(
            f"CAST(JSON_VALUE({base}, '{path}.{field.name}') as {typ})",
            f" {transform_column_name(field.name, **self.transforms)}",
        )


class Compressor:
    """Compresses streams of bytes using gzip."""

    def __init__(self) -> None:
        """Initialize the compressor."""
        self._compressor = None
        self._closed = False
        if shutil.which("gzip") is not None:
            self._buffer = TemporaryFile()
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

    def getbuffer(self) -> Union[memoryview, mmap.mmap]:
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
        if not self._closed:
            self.close()
        # close the buffer, ignore error if we have an incremented rc due to memoryview
        # the gc will take care of the rest when the worker dereferences the buffer
        try:
            self.buffer.close()
        except BufferError:
            pass
        if self._compressor is not None and self._compressor.poll() is None:
            self._compressor.kill()
            self._compressor.wait()
        # dereference
        self._buffer = None


# pylint: disable=no-else-return,too-many-branches,too-many-return-statements
def bigquery_type(
    property_type: List[str], property_format: Optional[str] = None
) -> str:
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
    **kwargs
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
        name = "{}".format(name).lower()
    if add_underscore_when_invalid:
        if name[0].isdigit():
            name = "_{}".format(name)
    if quote or was_quoted:
        name = "`{}`".format(name)
    if replace_period_with_underscore:
        name = name.replace(".", "_")
    return name
