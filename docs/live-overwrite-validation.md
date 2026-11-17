# Live overwrite validation — 2026-11-17

Validated the reconciled #152 runtime plus UUID staging containment on Python 3.12.
The authorized scope was generated data in the existing CI dataset, temporary
tables only, cleanup, and at most $1 usage. No IAM, project, or dataset was created.

## Final result

Both `batch_job` and `storage_write_api` passed two overwrites (2 tests in 73.48s).
Each first pass contained exactly IDs 1 and 2; each second pass contained only ID 3.
After each publication, assertions verified MONTH partitioning on `_sdc_batched_at`,
40-day partition expiration, and clustering on `_sdc_batched_at`.
All six generated destination/staging tables were deleted and metadata reads
confirmed NotFound. The full offline suite passed 421 tests (15 cloud skips);
Ruff and mypy passed. Independent review covered the reconciliation and guard.

## Findings and containment

The initial harness rejected quoted column identifiers before submitting queries;
its scope parser was corrected. The original fixture used seven-day expiration,
which is unsafe for current-month data because expiration starts at the partition
boundary. Retention now exceeds a full month. It also reused a fixed staging name:
Storage Write's second overwrite returned no rows despite successful append and
publication. Distinct staging names passed, followed by the final full-UUID run.
The exact gRPC/server cause is not established; the observable unsafe configuration
is rejected before table mutations. Streaming upsert/overwrite templates require
an unmodified `{uuid}` placeholder. Append-only configurations are unaffected.
The documented legacy streaming metadata hazard is supporting context, not proof
of the gRPC internals: [Google troubleshooting](https://docs.cloud.google.com/bigquery/docs/troubleshoot-storage-api).
[Partition expiration](https://docs.cloud.google.com/bigquery/docs/managing-partitioned-tables#set_partition_expiration)
is measured from the partition boundary.

## Cost and cleanup evidence

The local harness restricted table references to generated names in the authorized
dataset, tracked creations, refused preexisting tables and dataset creation, and
capped each of four queries per run at 10 MiB. It performed metadata/tabledata reads
for verification and deleted only tracked tables. Across all attempts, 20 generated
table names were confirmed absent; 12 submitted queries reported a combined
40 MiB billed (about $0.00024 at $6.25/TiB, before free tiers). Tiny generated-row
write/storage charges are additional; this is not an invoice claim. Usage remains
well below the $1 cap. No unrelated tables were changed.

Final query job evidence:

| Job | Status | Billed bytes |
| --- | --- | --- |
| `fa6b436d-a905-43a7-9dd3-e85573832b4d` | DONE; no errors | 0 |
| `7ec70e9c-872b-4f84-abd5-2b9ed908e966` | DONE; no errors | 0 |
| `de810a76-e49c-41be-bbf0-37aace2e3689` | DONE; no errors | 10485760 |
| `ed7fed9b-306f-4bca-9d8f-afe587f5e663` | DONE; no errors | 10485760 |

This gate covers these overwrite scenarios only. It does not claim full live
coverage for GCS, legacy streaming inserts, nested migrations, or failure/restart.
