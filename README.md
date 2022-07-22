# target-bigquery

`target-bigquery` is a Singer target for BigQuery.

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
| credentials_json    | False    | None    | The str representation of gcp service account credentials. |
| project             | True     | None    | The target GCP project to materialize data into. |
| dataset             | True     | None    | The target dataset to materialize data into. |
| batch_size          | False    |      50 | The maximum number of rows to send in a single batch. |
| threads             | False    |       8 | The number of threads to use for writing to BigQuery. |
| method              | False    | storage | The method to use for writing to BigQuery. Accepted values are: storage, batch, stream, gcs |
| bucket              | False    | None    | The GCS bucket to use for staging data. Only used if method is gcs. |
| gcs_buffer_size     | False    |     2.5 | The size of the buffer for GCS stream before flushing. Value in megabytes. Only used if method is gcs. |
| stream_maps         | False    | None    | Config object for stream maps capability. |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

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

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

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
