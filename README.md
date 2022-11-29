# target-bigquery

`target-bigquery` is a Singer target for BigQuery.

The most versatile target for BigQuery. Extremely performant, resource efficient, and
fast in all configurations of which there are 7. Denormalized variants indicate data is 
unpacked during load with a resultant schema in BigQuery based on the tap. Non-denormalized
means we have a fixed schema which loads all data into an unstructured JSON column. 
They are both useful patterns. The latter allowing bigquery to work with schemaless or rapidly
changing sources such as MongoDB seamlessly, while the former is faster to query. 

The gap between the methods is closed due in part to this target automatically generating
a VIEW which will unpack a JSON based ingestion source for you. Unless operating at 
tens of millions of rows with 3-4-500 key objects, its reasonably performant. It does however
fall off given enough scale in the current state of the engineering at Google regarding BQ. Choose wisely.


Sink names (you will most liekly be configuring this target via yaml or json so scroll on for the config table):

```python
# batch job based
BigQueryBatchDenormalizedSink
BigQueryBatchSink

# gcs staging bucket -> load job
BigQueryGcsStagingDenormalizedSink
BigQueryGcsStagingSink

# streaming api
BigQueryLegacyStreamingDenormalizedSink
BigQueryLegacyStreamingSink

# storage write api
BigQueryStorageWriteSink
```

**Old Header (still true)**

This is the first truly unstructured sink for BigQuery leveraging the recent GA feature 
in BigQuery for JSON support. This allows this target to load from essentially any tap
regardless of the quality or explicitness of its jsonschema. Observations in existing taps 
note things such as `patternProperties` used in jsonschema objects which break down on 
all existing BigQuery taps due to the previous need for strong typing. Also taps such as
MongoDB which inherently deal with unstructured data are seamlessly enabled by this target. 


Built with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

```bash
pipx install target-bigquery
```

## Configuration

### Settings

Note: Either credentials_path or credentials_json (str) must be provided.

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| credentials_path    | False    | None    | The path to a gcp credentials json file. |
| credentials_json    | False    | None    | A JSON string of your service account JSON file. |
| project             | True     | None    | The target GCP project to materialize data into. |
| dataset             | True     | None    | The target dataset to materialize data into. |
| batch_size          | False    |  250000 | The maximum number of rows to send in a single batch or commit. |
| denormalized        | False    |       0 | Determines whether to denormalize the data before writing to BigQuery. A false value will write data using a fixed JSON column based schema, while a true value will write data using a dynamic schema derived from the tap. Denormalization is only supported for the batch_job, streaming_insert, and gcs_stage methods. |
| method              | True     | batch_job | The method to use for writing to BigQuery. |
| generate_view       | False    |       0 | Determines whether to generate a view based on the SCHEMA message parsed from the tap. Only valid if denormalized=false meaning you are using the fixed JSON column based schema. |
| gcs_bucket          | False    | None    | The GCS bucket to use for staging data. Only used if method is gcs_stage. |
| gcs_buffer_size     | False    |      15 | The size of the buffer for GCS stream before flushing a multipart upload chunk. Value in megabytes. Only used if method is gcs_stage. This eager flushing in conjunction with zlib results in very low memory usage. |
| gcs_max_file_size   | False    |     250 | The maximum file size in megabytes for a bucket file. This is used as the batch indicator for GCS based ingestion. Only used if method is gcs_stage. |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `target-bigquery --about`

A full list of supported settings and capabilities is available by running: `target-bigquery --about`

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

https://cloud.google.com/bigquery/docs/authentication

## Capabilities

* `about`
* `stream-maps`
* `schema-flattening`

## Usage

You can easily run `target-bigquery` by itself or in a pipeline using [Meltano](https://meltano.com/).


### Executing the Target Directly

```bash
target-bigquery --version
target-bigquery --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-bigquery --config /path/to/target-bigquery-config.json
```

## Developer Resources


### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_bigquery/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-bigquery` CLI interface directly using `poetry run`:

```bash
poetry run target-bigquery --help
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
