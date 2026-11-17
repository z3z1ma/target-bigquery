<h1 align="center">Target-BigQuery</h1>

<p align="center">
<a href="https://github.com/z3z1ma/target-bigquery/actions/"><img alt="Actions Status" src="https://github.com/z3z1ma/target-bigquery/actions/workflows/ci.yml/badge.svg"></a>
<a href="https://github.com/z3z1ma/target-bigquery/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg"></a>
<a href="https://docs.astral.sh/ruff/"><img alt="Code style: ruff" src="https://img.shields.io/badge/code%20style-ruff-46a2f1.svg"></a>
</p>

**A rare 💎 you have stumbled upon**

`target-bigquery` is a Singer target for BigQuery.

It is the most versatile target for BigQuery. Extremely performant, resource efficient, and fast in all configurations enabling 20 different ingestion patterns. Denormalized variants indicate data is unpacked during load with a resultant schema in BigQuery based on the tap schema. Non-denormalized means we have a fixed schema which loads all data into an unstructured `JSON` column. They are both useful patterns. The latter allowing BigQuery to work with schemaless or rapidly changing sources such as MongoDB instantly, while the former is more performant and convenient to start modeling quickly.

**Patterns** 🛠 (more details below)

- Batch Job, Denormalized, Overwrite
- Batch Job, Denormalized, Upsert
- Batch Job, Denormalized, Append
- Batch Job, Fixed Schema, Overwrite
- Batch Job, Fixed Schema, Append
- GCS Staging Data Lake, Denormalized, Overwrite
- GCS Staging Data Lake, Denormalized, Upsert
- GCS Staging Data Lake, Denormalized, Append
- GCS Staging Data Lake, Fixed Schema, Overwrite
- GCS Staging Data Lake, Fixed Schema, Append
- Storage Write API, Denormalized, Overwrite
- Storage Write API, Denormalized, Upsert
- Storage Write API, Denormalized, Append
- Storage Write API, Fixed Schema, Overwrite
- Storage Write API, Fixed Schema, Append
- Legacy Streaming API, Denormalized, Overwrite
- Legacy Streaming API, Denormalized, Upsert
- Legacy Streaming API, Denormalized, Append
- Legacy Streaming API, Fixed Schema, Overwrite
- Legacy Streaming API, Fixed Schema, Append

## Installation 📈

The package on pypi is named `z3-target-bigquery` but the executable it ships with is simply `target-bigquery`. This allows me to release work without concerns of naming conflicts on the package index.

```bash
# Use pipx or pip
pipx install z3-target-bigquery
# Verify it is installed
target-bigquery --version
```

See the [capability and validation guide](docs/capabilities.md) for transport,
strategy, JSON, replay and migration limits. Unreleased work is tracked in the
[release-readiness candidate](docs/release-readiness.md).

## Usage Notes

### Denormalization

So denormalized is a loaded term, so lets clarify here what we mean by it:

> In the context of JSON objects, denormalization refers to the process of flattening a hierarchical or nested JSON object into a simpler, more "denormalized" structure that can be easier to work with for certain use cases.

Denormalized=False (default) means we load all data into a single `JSON` column. This means all access requires an accessor such as `SELECT data.my_column FROM table` instead of `SELECT my_column FROM table`. Hence the term denormalized is relative to the `data` column which is the default for this target. This is a tradeoff between convenience/resilience and performance. This is the default because most _resilient_. This reduces dependence on a stable source schema, but invalid JSON values, transport failures, permissions and quotas can still fail a load. You can denormalize later via `dbt`. However, it is not the most performant and slower to transform. If your tap has a high quality and consistent schema, denormalization is the way to go to get the best performance and start modeling quickly.

Denormalized=True means we unpack the data into a schema which is derived from the tap schema. It does _not_ mean we will flatten the data. There is a separate option for flattening. We will convert arrays to repeated fields and records to structs. All top level keys will end up as columns which is was you might expect from more typical targets.

#### Resolver Versions

There are 2 resolver versions. The config option `schema_resolver_version` lets you select which version you want to use. This versioning exists because we want to support evolving how we resolve schemas whilst not creating breaking changes for long-time users dependent on how a schema is resolved. The default is `1` which behaves very similarly to existing flavors of `target-bigquery`. It works well enough but has plenty of edge cases where it simply cannot resolve valid jsonschemas to a bq schema. The new version `2` is much more robust and will resolve most, if not all schemas due to it falling back to `JSON` when in doubt. You must opt-in to this version by setting `schema_resolver_version: 2` in your config.

### Overwrite vs Append

Sometimes you want to overwrite a table on every load. This can be achieved by either setting `overwrite: true` which will full refresh ALL tables **or** setting `overwrite: [table1, table2, table_*_other, !table_v1_other]` which will only overwrite the specified tables and supports pattern matching. String booleans and comma-separated string patterns are also accepted for env-based configuration. This is useful if you have a table which is a lookup table and you want to overwrite it on every load. You can also set `overwrite: false` which will append to the table. This is the default behavior.

Overwrite preserves the configured partitioning and clustering using `CREATE OR REPLACE TABLE`.
An existing destination must match **all** of these settings: time partition type,
partition field (`_sdc_batched_at`), partition expiration (including no expiration),
and clustering fields in order. Range-partitioned and ingestion-time-partitioned tables
are incompatible. The target checks compatibility before schema updates or staging.

For older tables that lost their layout during overwrite, either configure
`partition_granularity: none` and `clustering_fields: []` to retain an unpartitioned,
unclustered table, or migrate the destination separately to the desired layout.
For a monthly table, use `partition_granularity: month` and its existing clustering
and expiration settings. Per-stream overrides allow mixed layouts. Dataset default
partition expiration can affect newly created tables; set `partition_expiration_days`
explicitly to match it. Changing expiration or clustering is also rejected by this
conservative target policy, even where BigQuery can alter the setting separately.
Back up needed data and coordinate a migration before rerunning; the target never
automatically drops the destination to resolve a mismatch.

### Upsert (aka Merge)

If you want to merge data into a table, you can set `upsert: true` which will use the `MERGE` statement to upsert data. This supports pattern matching like the above setting, including comma-separated string patterns for env-based configuration. It requires `denormalized: true` and takes precedence over `overwrite`. It will only work on tables which have a primary key as defined by the `key_properties` sent by the tap. There is a supporting config option called `dedupe_before_upsert` which will dedupe the data before upserting. This is useful if you are replicating data which has a primary key but is not unique. This occurs when you are replicating data from a source which has a primary key but does not enforce it. This is the case for MongoDB. It can also happen when moving data from a data lake in S3/GCS to a database. This is not the default behavior because it is slower and requires more resources.

## Features ✨

- Autoscaling self-healing worker pool using either threads (default) or multiprocessing, configurable by the user for the _fastest_ possible data ingestion. Particularly when leveraging colocated compute in GCP.
- Denormalized load pattern where data is unpacked in flight into a statically typed BigQuery schema derived from the input stream json schemas.
- Fix schema load pattern where all data is loaded into a `JSON` column which has been GA in BigQuery since mid 2022.
- Autogenerated `VIEW` support for fixed schema load patterns which essentially overlays a statically typed schema allowing you to get the best of both worlds when using fixed schema ingestion.
- JIT compilation of protobuf schemas allowing the Storage Write API to use a denormalized load pattern.
- BATCH message support 😎

## Load Patterns 🏎

- `Batch Load Job` ingestion pattern using in memory compression (fixed schema + denormalized)
- `Storage Write API` ingestion pattern using gRPC and protocol buffers supporting both streaming and batch patterns. Capable of JIT compilation of BQ schemas to protobuf to enable denormalized loads of input structures only known at runtime. (fixed schema + denormalized 🎉)
- `GCS Staging` ingestion pattern using in memory compression and a GCS staging layer which generates a well organized data lake which backs the data warehouse providing additional failsafes and data sources (fixed schema + denormalized)
- `Legacy Streaming` ingestion pattern which emphasizes simplicity and fast start up / tear down. I would highly recommend the storage write API instead unless data volume is small (fixed schema + denormalized)


**Choosing between denormalized and fixed schema (JSON support)?** 🙇🏾‍♂️

The gap between the methods is closed due in part to the target's ability to  automatically generating a `VIEW` which will unpack (or rather provide typing as a more accurate take) a JSON based ingestion source for you. Unless operating at tens of millions of rows with JSON objects containing multiple hundreds of keys, its quite performant. Particularly if accessing a small subset of keys. It does however fall off (quite hard) given enough scale as I mentioned (I've pushed it to the limits). Denormalized is recommended for high volume where the schema is fairly consistent. Fixed is recommended for lower volume, inconsistent schemas or for taps which are inherently schemaless in which case its the ideal (only...) logical pattern. Fixed schema can also be used for taps which routinely break down on BQ due to json schemas being inexpressible in a static way (ie patternProperties, additionalProperties...)


**Old Header (still true, here for posterity)** 🧪

This is the first truly unstructured sink for BigQuery leveraging the recent GA feature in BigQuery for JSON support. This allows this target to load from essentially any tap regardless of the quality or explicitness of its jsonschema. Observations in existing taps note things such as `patternProperties` used in jsonschema objects which break down on all existing BigQuery taps due to the previous need for strong typing. Also taps such as MongoDB which inherently deal with unstructured data are seamlessly enabled by this target without klutzy collection scraping of a sample of records which we _hope_ are repesentative of all documents.


Built with the [Meltano Target SDK](https://sdk.meltano.com).


## Configuration 🔨

### Settings

First a valid example to give context to the below including a nested key example (denoted via a `.` in the setting path) as seen with `column_name_transforms.snake_case`

```json
{
    "project": "my-bq-project",
    "method": "storage_write_api",
    "denormalized": true,
    "credentials_path": "...",
    "dataset": "my_dataset",
    "location": "us-central1",
    "batch_size": 500,
    "column_name_transforms": {
      "snake_case": true
    }
}
```


| Setting                                            | Required |      Default      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|:---------------------------------------------------|:--------:|:-----------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| credentials_path                                   |  False   |       None        | The path to a gcp credentials json file. |
| credentials_json                                   |  False   |       None        | A JSON string of your service account JSON file. |
| project                                            |   True   |       None        | The target GCP project to materialize data into. |
| dataset                                            |   True   |       None        | The target dataset to materialize data into. |
| location                                           |  False   |        US         | The target dataset location to materialize data into. Applies also to the GCS bucket if using `gcs_stage` load method. |
| batch_size                                         |  False   |        500        | The maximum number of rows to send in a single batch to the worker. This should be configured based on load method. For `storage_write_api` and `streaming_insert` it should be `<=500`, for the LoadJob sinks, it can be much higher, ie `>100,000` |
| timeout                                            |  False   |        600        | Timeout in seconds for batch_job/gcs_stage LoadJobs and query submission/result waits (views, MERGE, overwrite). Query timeouts must be positive integers; SDK retries can exceed this value; it does not cancel remote jobs or impose a whole-process deadline. |
| fail_fast                                          |  False   |        True       | Fail the entire load job if any row fails to insert. |
| denormalized                                       |  False   |       False       | Determines whether to denormalize the data before writing to BigQuery. A false value will write data using a fixed JSON column based schema, while a true value will write data using a dynamic schema derived from the tap. |
| method                                             |   True   | storage_write_api | The method to use for writing to BigQuery. Must be one of `batch_job`, `storage_write_api`, `gcs_stage`, `streaming_insert` |
| generate_view                                      |  False   |       False       | Determines whether to generate a view based on the SCHEMA message parsed from the tap. Only valid if denormalized=false meaning you are using the fixed JSON column based schema. |
| timestamp_format                                   |  False   |       None        | Optional BigQuery `PARSE_TIMESTAMP` format string used for generated timestamp view columns when `generate_view=true`. |
| upsert                                             |  False   |       False       | Determines if we should upsert. Defaults to false. A value of true will write to a temporary table and then merge into the target table (upsert). This requires the target table to be unique on the key properties. A value of false will write to the target table directly (append). String booleans are accepted for env-based configuration. A string or array of strings will evaluate the strings in order using fnmatch. At the end of the array, the value of the last match will be used. If not matched, the default value is false (append). |
| overwrite                                          |  False   |       False       | Determines if the target table should be overwritten on load. Defaults to false. A value of true will write to a temporary table and then atomically replace the target table with `CREATE OR REPLACE TABLE`. A value of false will write to the target table directly (append). String booleans are accepted for env-based configuration. A string or array of strings will evaluate the strings in order using fnmatch. At the end of the array, the value of the last match will be used. If not matched, the default value is false. This is mutually exclusive with the `upsert` option. If both are set, `upsert` will take precedence. |
| dedupe_before_upsert                               |  False   |       False       | This option is only used if `upsert` is enabled for a stream. The selection criteria for the stream's candidacy is the same as upsert. If the stream is marked for deduping before upsert, we will create a _session scoped temporary table during the merge transaction to dedupe the ingested records. This is useful for streams that are not unique on the key properties during an ingest but are unique in the source system. String booleans and pattern strings are accepted for env-based configuration. Data lake ingestion is often a good example of this where the same unique record may exist in the lake at different points in time from different extracts. |
| temporary_table_expiration_hours                   |  False   |        168        | Number of hours before upsert and overwrite temporary tables expire. Defaults to 168 hours so long-running syncs do not lose staged data after one day. |
| temporary_table_name_template                      |  False   | `{table_name}__{timestamp}__{uuid}` | Template for upsert and overwrite temporary table names. Streaming methods require an unmodified `{uuid}` placeholder to prevent reuse of deleted staging names. Supports `{table_name}`, `{timestamp}`, and `{uuid}`; characters outside letters, digits, and underscores are normalized to underscores. |
| bucket                                             |  False   |       None        | The GCS bucket to use for staging data. Only used if method is gcs_stage. |
| cluster_on_key_properties                          |  False   |         0         | Determines whether to cluster on the key properties from the tap. Defaults to false. When false, clustering will be based on _sdc_batched_at instead. |
| clustering_fields                                  |  False   |       None        | Optional explicit BigQuery clustering fields. When set, this takes precedence over `cluster_on_key_properties` and does not change merge key semantics. An empty list (`[]`) disables clustering entirely, mirroring `partition_granularity=none`. |
| clustering_fields_by_stream                        |  False   |       None        | Optional mapping of `fnmatch` stream-name patterns to clustering field lists. The last matching pattern wins. An empty list disables clustering for matching streams. |
| partition_granularity                              |  False   |      "month"      | Indicates the granularity of the created table partitioning scheme which is based on `_sdc_batched_at`. By default the granularity is monthly. Must be one of: "none", "hour", "day", "month", "year". |
| partition_granularity_by_stream                    |  False   |       None        | Optional mapping of `fnmatch` stream-name patterns to partition granularities. The last matching pattern wins. |
| partition_expiration_days                          |  False   |       None        | If set for date- or timestamp-type partitions, the partition will expire that many days after the date it represents. |
| column_name_transforms.lower                       |  False   |       None        | Lowercase column names. |
| column_name_transforms.quote                       |  False   |       None        | Quote column names in any generated DDL. |
| column_name_transforms.add_underscore_when_invalid |  False   |       None        | Add an underscore to the column name if it starts with a digit to make it valid. |
| column_name_transforms.snake_case                  |  False   |       None        | Snake case all incoming column names. Does not apply to fixed schema loads but _does_ apply to the view auto-generated over them. |
| column_name_transforms.replace_period_with_underscore |  False   |       None        | Replace period with underscore. Period is not an [acceptable character](https://cloud.google.com/bigquery/docs/schemas#column_names) for a column name|
| options.storage_write_batch_mode                   |  False   |       None        | By default, we use the default stream (Committed mode) in the [storage_write_api](https://cloud.google.com/bigquery/docs/write-api) load method which results in streaming records which are immediately available and is generally fastest. If this is set to true, we will use the application created streams (pending mode) to transactionally batch data on STATE messages and at end of pipe. |
| options.process_pool                               |  False   |       None        | By default we use an autoscaling threadpool to write to BigQuery. If set to true, we will use a process pool. |
| options.max_workers                                |  False   |       None        | By default, each sink type has a preconfigured max worker pool limit. This sets an override for maximum number of workers in the pool. |
| schema_resolver_version                            |  False   |       1           | The version of the schema resolver to use. Defaults to 1. Version 2 uses JSON as a fallback during denormalization. This only has an effect if denormalized=true |
| stream_maps                                        |  False   |       None        | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config                                  |  False   |       None        | User-defined config values to be used within map expressions. |
| flattening_enabled                                 |  False   |       None        | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth                               |  False   |       None        | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `target-bigquery --about`

### Configure using environment variables ✏️

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization 👮🏽‍♂️

Authenticate via service account key file or Application Default Credentials (ADC)
https://cloud.google.com/bigquery/docs/authentication

## Capabilities ✨

* `about`
* `stream-maps`
* `schema-flattening`
* `batch`

## Usage 👷‍♀️

You can easily run `target-bigquery` by itself or in a pipeline using [Meltano](https://meltano.com/).


### Executing the Target Directly 🚧

```bash
target-bigquery --version
target-bigquery --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-bigquery --config /path/to/target-bigquery-config.json
```

## Developer Resources 👩🏼‍💻


### Initialize your Development Environment

This project supports Python 3.10 and newer.

```bash
pipx install uv
uv sync
```

### Create and Run Tests

Create tests within the `target_bigquery/tests` subfolder and
  then run:

```bash
uv run pytest
```

You can also test the `target-bigquery` CLI interface directly using `uv run`:

```bash
uv run target-bigquery --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-bigquery
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-bigquery --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano elt tap-carbon-intensity target-bigquery
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano SDK to
develop your own Singer taps and targets.

### Storage Write checkpoints

For denormalized upserts, checkpoints wait for Storage Write completion and a
successful merge into the destination before emitting Singer state. Pending
streams are finalized and committed first. Commit or merge failures stop the
checkpoint. After a merge rotates the staging table, subsequent batches use the
new table. This does not provide end-to-end exactly-once delivery: replay after
an ambiguous failure still requires stable upsert keys. Query jobs use explicit
IDs and do not automatically resubmit failed mutations. Reconcile uncertain jobs
before restarting; see [query recovery](docs/query-recovery.md) for the procedure
and retry-policy tradeoffs.

When any stream uses overwrite mode, intermediate Singer state is deferred until
all staged replacements are published at end of input. This applies to the whole
state message, including other streams. A failure before EOF or a handled termination signal retains the last published
checkpoint. Handled termination does not initiate publication or emit state, and exits
nonzero. A replacement query already submitted can still complete remotely.
Temporary tables remain subject to their configured expiration.
Append-only streams in a mixed run may replay rows; use stable upsert keys where
replay must be idempotent. Overwrites across several destination tables are not
one atomic transaction.

The target cannot distinguish a successful upstream snapshot from a failed tap
that closes its output. The orchestrator must check upstream success; EOF alone
is not proof of a complete snapshot.

### GCS checkpoint durability

With `gcs_stage`, each nonempty checkpoint waits for the uploaded objects to load
into BigQuery before acknowledging STATE. Upsert loads finish before MERGE and
staging rotation; overwrite loads accumulate in staging while STATE remains
withheld until successful EOF publication. Load submission and result waits use
`timeout`. Failed loads retain their job ID and pending URI list for diagnosis;
reconcile an unknown outcome before restarting.

This creates load jobs at checkpoints instead of only at EOF, increasing job
frequency and latency; consider BigQuery load-job quotas when choosing checkpoint
cadence. Successfully loaded URIs leave the in-memory pending list and are not
loaded again at EOF. GCS objects are not deleted by this change: retain your bucket
lifecycle policy. Replays can still duplicate append data. This behavior is tested
offline; no new GCS live validation is claimed.

## Maintenance roadmap

See [ROADMAP.md](ROADMAP.md) for audited priorities, acceptance tests, current status,
and the verification required before each change ships.


### Denormalized Storage Write JSON fields

With schema resolver v2, fields resolved as BigQuery JSON are encoded as JSON
text for protobuf transport, including JSON inside nested/repeated RECORDs.
Python strings are JSON scalar strings, not pre-encoded documents. A nullable
field's `None` remains SQL NULL; a `None` element in repeated JSON becomes JSON
null. Serialization copies affected record containers rather than changing them.

Non-finite float/Decimal values fail before the row is buffered. Integer values
from -2^63 through 2^64-1 retain their numeric values; larger integers fail rather
than round. Finite fractional Decimals follow the existing float conversion and
are not arbitrary-precision JSON numbers. These transport checks do not replace
BigQuery server validation. Fixed-schema encoding is unchanged. Existing column
name transforms also apply recursively to keys inside JSON documents during
preprocessing; configure transforms with that behavior in mind.

Denormalized RECORD schemas and records use the same column-name transforms at
every nested/repeated level. SQL quoting is not part of a physical field name;
source keys containing backticks are rejected. Existing incompatible nested
schemas require an explicit migration as described below.

### Column transform collisions

Denormalized loads reject distinct sibling keys that transform to the same name,
such as `CamelKey` and `camel_key` with snake_case enabled. Declared schema
collisions fail before destination creation/update. Dynamic keys, including keys
inside JSON documents, are checked during record preprocessing before buffering;
the error includes the stream, object path and conflicting source names. Rename
the source keys or change the transform configuration before retrying. Automatic
schema suffixes are no longer used because they did not preserve record values.
Fixed-schema raw ingestion keeps original document keys. This containment does
not migrate nested RECORD schemas or recover values lost by earlier runs.

### Nested schema migration

Before a denormalized sink creates or updates its destination or staging table,
it reads the destination schema. Existing RECORD fields must match the expected
nested names, order, types and modes, including repeated children. A mismatch
stops the stream with the destination and field path before those mutations.
The same check applies to append, upsert and overwrite. A completely new top-level
field can still be added; a new child inside an existing RECORD requires an
explicit schema update or migration. Extra historical top-level columns remain
supported. Nested order and mode checks are conservative so whole-STRUCT MERGE
assignments do not silently change meaning. Metadata access errors stop the load.

Older versions could create raw nested names while transforming record keys.
For example, `PacketData.ChildValue` with snake_case now resolves to
`packet_data.child_value`; an existing `packet_data.ChildValue` is rejected.
The target does not automatically rename, add, remove or backfill nested fields.
It does not recover values discarded by earlier loads.

To migrate, stop writers and retain the old table and checkpoint for rollback.
Inspect the actual schema and populated fields, including raw/transformed names;
do not infer that historical rows are complete. Choose one explicit approach:

- Reload into a new dataset/destination using the intended transforms and a full
  source replay. An incremental checkpoint alone cannot rebuild historical rows.
- Backfill into a separately created table with the intended schema. Explicitly
  map each old nested field, preserving null STRUCTs, array order and values.
  Reconcile historical gaps from the source where required. Do not infer that
  adding a renamed child copies the old child's values.

For either approach, preserve/review partitioning, clustering, expiration,
descriptions, policy tags, constraints and access settings. Validate row counts,
keys and nested values before switching readers/writers and checkpoints. Keep the
old table until cutover is verified. New-table backfill and cutover are operator
operations; this target performs none of them. BigQuery documents supported
[schema changes](https://docs.cloud.google.com/bigquery/docs/managing-table-schemas),
but this target does not automate nested evolution. Concurrent external schema
changes after preflight are not transactionally prevented; coordinate schema
changes with ingestion. Live migration validation remains outstanding.

JSON document keys retain their existing recursive transform behavior, including
collision rejection. Fixed-schema raw ingestion and generated-view alias behavior
are unchanged by this nested physical-schema fix.
