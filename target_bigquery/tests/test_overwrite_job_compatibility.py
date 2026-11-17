"""Offline API compatibility spike; none of these requests reaches BigQuery."""

import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery


@pytest.mark.parametrize(
    "config_cls,kind", [(bigquery.QueryJobConfig, "query"), (bigquery.LoadJobConfig, "load")]
)
@pytest.mark.parametrize(
    "schema_options",
    [[], ["ALLOW_FIELD_ADDITION"], ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]],
)
def test_truncate_data_serializes_for_documented_job_types(config_cls, kind, schema_options):
    config = config_cls(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        schema_update_options=schema_options,
    )
    wire = config.to_api_repr()[kind]
    assert wire["writeDisposition"] == "WRITE_TRUNCATE_DATA"
    assert wire["createDisposition"] == "CREATE_NEVER"
    assert wire["schemaUpdateOptions"] == schema_options
    assert config_cls.from_api_repr({kind: wire}).to_api_repr()[kind] == wire


def test_select_publication_reaches_jobs_insert_with_explicit_destination(monkeypatch):
    client = bigquery.Client(project="example", location="EU", credentials=AnonymousCredentials())
    requests = []

    def capture(*args, **kwargs):
        requests.append(kwargs)
        return {**kwargs["data"], "status": {"state": "DONE"}}

    monkeypatch.setattr(client, "_call_api", capture)
    config = bigquery.QueryJobConfig(
        destination="example.dataset.destination",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
        use_legacy_sql=False,
    )
    sql = "SELECT id FROM `example.dataset.staging`"
    job = client.query(sql, job_config=config, job_id="offline-probe", job_retry=None, timeout=17)
    assert len(requests) == 1
    request = requests[0]
    assert request["method"] == "POST"
    assert request["path"] == "/projects/example/jobs"
    assert request["timeout"] == 17
    assert request["data"]["jobReference"] == {
        "projectId": "example",
        "jobId": "offline-probe",
        "location": "EU",
    }
    query = request["data"]["configuration"]["query"]
    assert query["query"] == sql
    assert query["destinationTable"] == {
        "projectId": "example",
        "datasetId": "dataset",
        "tableId": "destination",
    }
    assert query["writeDisposition"] == "WRITE_TRUNCATE_DATA"
    assert query["createDisposition"] == "CREATE_NEVER"
    assert query["useLegacySql"] is False
    assert "schemaUpdateOptions" not in query
    assert "timePartitioning" not in query
    assert "clustering" not in query
    assert job.job_id == "offline-probe"


def test_serialization_alone_does_not_validate_server_support():
    # Copy jobs do not document this disposition, but the SDK accepts any string.
    # Keep this counterexample so a successful serializer cannot become a support claim.
    config = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA)
    assert config.to_api_repr()["copy"]["writeDisposition"] == "WRITE_TRUNCATE_DATA"
