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
import gzip
import json
import mmap
import re
import shutil
import sys
import time
from abc import ABC, abstractmethod

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
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
)

from google.api_core.exceptions import Conflict
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


@dataclass
class BigQueryTable:
    name: str
    dataset: str
    project: str
    jsonschema: Dict[str, Any]
    ingestion_strategy: IngestionStrategy
    transforms: Dict[str, bool] = field(default_factory=dict)

    @property
    def schema_translator(self) -> "SchemaTranslator":
        """Returns a SchemaTranslator instance for this table."""
        if not hasattr(self, "_schema_translator"):
            self._schema_translator = SchemaTranslator(
                schema=self.jsonschema,
                transforms=self.transforms,
            )
        return self._schema_translator

    def get_schema(self, apply_transforms: bool = False) -> List[bigquery.SchemaField]:
        """Returns the jsonschema to bigquery schema translation for this table."""
        if apply_transforms:
            return self.schema_translator.translated_schema_transformed
        return self.schema_translator.translated_schema

    def get_resolved_schema(
        self, apply_transforms: bool = False
    ) -> List[bigquery.SchemaField]:
        """Returns the schema for this table after factoring in the ingestion strategy."""
        if self.ingestion_strategy == IngestionStrategy.FIXED:
            return DEFAULT_SCHEMA
        elif self.ingestion_strategy == IngestionStrategy.DENORMALIZED:
            return self.get_schema(apply_transforms)

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

    def as_dataset(self) -> bigquery.Dataset:
        """Returns a Dataset instance for this table."""
        return bigquery.Dataset(self.as_dataset_ref())

    def create_table(
        self,
        client: bigquery.Client,
        apply_transforms: bool = False,
        **kwargs,
    ) -> Tuple[bigquery.Dataset, bigquery.Table]:
        """Creates a dataset and table for this table."""
        if not hasattr(self, "_dataset"):
            self._dataset = client.create_dataset(self.as_dataset(), exists_ok=True)
        if not hasattr(self, "_table"):
            try:
                self._table = client.create_table(
                    self.as_table(
                        apply_transforms
                        and self.ingestion_strategy != IngestionStrategy.FIXED,
                        **kwargs,
                    )
                )
            except Conflict:
                self._table = client.get_table(self.as_ref())
            else:
                # Wait for eventual consistency
                time.sleep(5)
        return self._dataset, self._table

    def default_table_options(self) -> Dict[str, Any]:
        """Returns the default table options for this table."""
        schema_dump = json.dumps(self.jsonschema)
        return {
            "clustering_fields": ["_sdc_batched_at"],
            "description": "Generated by target-bigquery.\nStream Schema\n{schema}\nBigQuery Ingestion Strategy: {strategy}".format(
                schema=(schema_dump[:16000] + "...")
                if len(schema_dump) > 16000
                else schema_dump,
                strategy=self.ingestion_strategy,
            ),
            "time_partitioning": TimePartitioning(
                type_=TimePartitioningType.MONTH, field="_sdc_batched_at"
            ),
        }

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
        job_notifier: "Connection",
    ):
        super().__init__()
        self.ext_id: str = ext_id
        self.queue: "Queue" = queue
        self.credentials: BigQueryCredentials = credentials
        self.job_notifier: "Connection" = job_notifier

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError


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
        super().__init__(target, stream_name, schema, key_properties)
        self._credentials = BigQueryCredentials(
            self.config.get("credentials_path"),
            self.config.get("credentials_json"),
            self.config["project"],
        )
        self.client = bigquery_client_factory(self._credentials)
        self.table = BigQueryTable(
            name=self.table_name,
            dataset=self.config["dataset"],
            project=self.config["project"],
            jsonschema=self.schema,
            transforms=self.config.get("column_name_transforms", {}),
            ingestion_strategy=self.ingestion_strategy,
        )

        self.create_target(key_properties)
        self.update_schema()
        self.global_par_typ = target.par_typ
        self.global_queue = target.queue
        self.increment_jobs_enqueued = target.increment_jobs_enqueued

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

    # We override this because it offers us no benefit (that I have observed)
    # but halves throughput in multiple tests I have run.
    def _parse_timestamps_in_record(self, *args, **kwargs) -> None:
        pass

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
        kwargs = {}
        if key_properties and self.config.get("cluster_on_key_properties", False):
            kwargs["clustering_fields"] = key_properties[:4]
        partition_grain: str = self.config.get("partition_granularity")
        if partition_grain:
            kwargs["time_partitioning"] = TimePartitioning(
                type_=PARTITION_STRATEGY[partition_grain.upper()],
                field="_sdc_batched_at",
            )
        self.table.create_table(self.client, self.apply_transforms, **kwargs)
        if self.generate_view:
            self.client.query(
                self.table.schema_translator.generate_view_statement(
                    str(self.table),
                )
            ).result()

    def update_schema(self) -> None:
        """Update the target schema in BigQuery."""
        pass

    def pre_state_hook(self) -> None:
        """Called before state is emitted to stdout."""
        pass

    @staticmethod
    @abstractmethod
    def worker_cls_factory(
        worker_executor_cls: Type["Process"], config: Dict[str, Any]
    ) -> Union[Type[BaseWorker], Type["Process"]]:
        """Return a worker class for the given parallelization type."""
        raise NotImplementedError


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
    def update_schema(self: BaseBigQuerySink) -> None:
        """Update the target schema."""
        table = self.table.as_table()
        current_schema = table.schema[:]
        mut_schema = table.schema[:]
        for expected_field in self.table.get_resolved_schema(self.apply_transforms):
            if not any(field.name == expected_field.name for field in current_schema):
                mut_schema.append(expected_field)
        if len(mut_schema) > len(current_schema):
            table.schema = mut_schema
            self.client.update_table(
                table,
                ["schema"],
                retry=bigquery.DEFAULT_RETRY.with_timeout(15),
            )

    def preprocess_record(
        self: BaseBigQuerySink, record: Dict[str, Any], context: Dict[str, Any]
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
        return bigquery.Client.from_service_account_json(creds.path)
    elif creds.json:
        return bigquery.Client.from_service_account_info(json.loads(creds.json))
    return bigquery.Client(project=creds.project)


@cache
def gcs_client_factory(creds: BigQueryCredentials) -> storage.Client:
    """Get a GCS client."""
    if creds.path:
        return storage.Client.from_service_account_json(creds.path)
    elif creds.json:
        return storage.Client.from_service_account_info(json.loads(creds.json))
    return storage.Client(project=creds.project)


@cache
def storage_client_factory(
    creds: BigQueryCredentials,
) -> bigquery_storage_v1.BigQueryWriteClient:
    """Get a BigQuery Storage Write client."""
    if creds.path:
        return bigquery_storage_v1.BigQueryWriteClient.from_service_account_file(
            creds.path
        )
    elif creds.json:
        return bigquery_storage_v1.BigQueryWriteClient.from_service_account_info(
            json.loads(creds.json)
        )
    return bigquery_storage_v1.BigQueryWriteClient()


# This class translates a JSON schema into a BigQuery schema.
# It also uses the translated schema to generate a CREATE VIEW statement.
class SchemaTranslator:
    """Translate a JSON schema into a BigQuery schema."""

    def __init__(self, schema: Dict[str, Any], transforms: Dict[str, bool]):
        self.schema = schema
        self.transforms = transforms
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

    def generate_view_statement(self, table_name: str) -> str:
        """Generate a CREATE VIEW statement for the SchemaTranslator `schema`."""
        projection = ""
        for field in self.translated_schema[:]:
            if field.mode == "REPEATED":
                projection += indent(
                    self._wrap_json_array(field, path="$", depth=1), " " * 4
                )
            else:
                projection += indent(self._bigquery_field_to_projection(field), " " * 4)

        return f"CREATE OR REPLACE VIEW {table_name}_view AS \nSELECT \n{projection} FROM {table_name}"

    def _jsonschema_property_to_bigquery_column(
        self, name: str, schema_property: dict
    ) -> SchemaField:
        """Translate a JSON schema property into a BigQuery column."""
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

    def _translate_record_to_bigquery_schema(
        self, name: str, schema_property: dict, mode: str = "NULLABLE"
    ) -> SchemaField:
        """Translate a JSON schema record into a BigQuery schema."""
        fields = [
            self._jsonschema_property_to_bigquery_column(col, t)
            for col, t in schema_property.get("properties", {}).items()
        ]
        return SchemaField(name, "RECORD", mode, fields=fields)

    def _bigquery_field_to_projection(
        self,
        field: SchemaField,
        path: str = "$",
        depth: int = 0,
        base: str = "data",
        rebase: bool = False,
    ) -> str:
        """Translate a BigQuery schema field into a SQL projection."""
        # Pass-through _sdc columns into the projection as-is
        if field.name.startswith("_sdc_"):
            return f"{field.name},\n"
        scalar = f"{base}.{field.name}" if not rebase else f"{base}"

        # Records are handled recursively
        if field.field_type.upper() == "RECORD":
            return (
                (" " * depth * 2)
                + "STRUCT(\n{}\n".format(
                    "".join(
                        [
                            self._bigquery_field_to_projection(
                                f, f"{path}.{field.name}", depth + 1
                            )
                            if not f.mode == "REPEATED"
                            else self._wrap_json_array(
                                f, f"{path}.{field.name}", depth, base
                            )
                            for f in field.fields
                        ]
                    ).rstrip(",\n"),
                )
                + (" " * depth * 2)
                + f") as {transform_column_name(field.name, **self.transforms)},\n"
            )
        # Nullable fields require a JSON_VALUE call which creates a 2-stage cast
        elif field.is_nullable:
            return (" " * depth * 2) + self._wrap_nullable_json_value(
                field, f"{path}.{field.name}", base
            )
        # These are not nullable so if the type is known, we can do a 1-stage extract & cast
        elif field.field_type.upper() == "STRING":
            return (
                (" " * depth * 2)
                + f"STRING({scalar}) as {transform_column_name(field.name, **self.transforms)},\n"
            )
        elif field.field_type.upper() == "INTEGER":
            return (
                (" " * depth * 2)
                + f"INT64({scalar}) as {transform_column_name(field.name, **self.transforms)},\n"
            )
        elif field.field_type.upper() == "FLOAT":
            return (
                (" " * depth * 2)
                + f"FLOAT64({scalar}) as {transform_column_name(field.name, **self.transforms)},\n"
            )
        elif field.field_type.upper() == "BOOLEAN":
            return (
                (" " * depth * 2)
                + f"BOOL({scalar}) as {transform_column_name(field.name, **self.transforms)},\n"
            )
        # Fallback to a 2-stage extract & cast
        else:
            return (" " * depth * 2) + self._wrap_nullable_json_value(
                field, f"{path}.{field.name}" if not rebase else "$", base
            )

    def _wrap_json_array(
        self, field: SchemaField, path: str, depth: int = 0, base: str = "data"
    ) -> str:
        """Translate a BigQuery schema field into a SQL projection for a repeated field."""
        v = self._bigquery_field_to_projection(
            field, "$", depth, f"{field.name}__rows", rebase=True
        ).rstrip(", \n")
        return (" " * depth * 2) + indent(
            dedent(
                f"""
        ARRAY(
            SELECT {v}
            FROM UNNEST(
                JSON_QUERY_ARRAY({base}, '{path}.{field.name}')
            ) AS {field.name}__rows
        """
                + (" " * depth * 2)
                + f") AS {field.name},\n"
            ).lstrip(),
            " " * depth * 2,
        )

    def _wrap_nullable_json_value(
        self, field: SchemaField, path: str = "$", base: str = "data"
    ) -> str:
        """Translate a BigQuery schema field into a SQL projection for a nullable field."""
        typ = field.field_type.upper()
        if typ == "STRING":
            return f"JSON_VALUE({base}, '{path}') as {transform_column_name(field.name, **self.transforms)},\n"
        if typ == "FLOAT":
            typ = "FLOAT64"
        if typ in ("INT", "INTEGER"):
            typ = "INT64"
        return f"CAST(JSON_VALUE({base}, '{path}') as {typ}) as {transform_column_name(field.name, **self.transforms)},\n"


class Compressor:
    """Compresses streams of bytes using gzip."""

    def __init__(self) -> None:
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
        if self._closed:
            raise ValueError("I/O operation on closed compressor.")
        self._gzip.write(data)

    def flush(self) -> None:
        self._gzip.flush()
        self._buffer.flush()

    def close(self) -> None:
        if self._closed:
            return
        self._gzip.close()
        if self._compressor is not None:
            self._compressor.wait()
        self._buffer.flush()
        self._buffer.seek(0)
        self._closed = True

    def getvalue(self) -> bytes:
        if not self._closed:
            self.close()
        if self._compressor is not None:
            return self._buffer.read()
        return self._buffer.getvalue()

    def getbuffer(self) -> Union[memoryview, mmap.mmap]:
        if not self._closed:
            self.close()
        if self._compressor is not None:
            return mmap.mmap(self._buffer.fileno(), 0, access=mmap.ACCESS_READ)
        return self._buffer.getbuffer()

    @property
    def buffer(self) -> IO[bytes]:
        return self._buffer

    def __del__(self) -> None:
        if not self._closed:
            self.close()
        # close the buffer, ignore error if we have an incremented rc due to memoryview
        # the gc will take care of the rest when the worker dereferences the buffer
        try:
            self._buffer.close()
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
) -> str:
    """Transform a column name to a valid BigQuery column name."""
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
    return name
