# Downstream Integration Guide

This guide is for engineers connecting external systems to the Iceberg
warehouse this pipeline produces. The pipeline itself doesn't push data
anywhere — every external system pulls from S3 Tables on its own
schedule.

If you're operating the pipeline, see
[`DEPLOYMENT.md`](DEPLOYMENT.md). This document is for the team
**downstream** of the Iceberg warehouse.

---

## What you're integrating with

Every customer install produces three classes of Iceberg tables:

| Table family | Purpose | Update cadence |
|---|---|---|
| `<ns>.<table>_raw` | Append-only history of every change CDA emitted | Every ingest run |
| `<ns>.<table>_merged` | Current resolved state per id | Every ingest run |
| `<ns>.cda_recon_results` | Audit log of every reconciliation check | Every ingest run + every recon run |

Default namespace is `cda`. The S3 Tables bucket name is
`<customer>-cda-iceberg-<account>-<region>`.

Most consumers want `<table>_merged` — that's the table you'd treat as
"the current state of Guidewire." `<table>_raw` is the audit trail and
useful for time-travel queries or full-history backfills. The recon
table is operational, not analytical.

---

## Integration patterns

### 1. Athena (recommended for ad-hoc and BI tools)

Federation through Glue lets Athena query S3 Tables with no separate
catalog setup per analyst.

**One-time pipeline-side setup:** deploy the optional `AnalyticsStack`
(see [`ANALYTICS.md`](ANALYTICS.md)) or run the manual setup in
[`DEPLOYMENT.md` §3.4](DEPLOYMENT.md#34-athena-glue-and-lake-formation).

**Per-analyst access:** Lake Formation grant (CLI or via
AnalyticsStack's `analystRoleArns` context flag).

**Query pattern:**

```sql
SELECT * FROM "s3tablescatalog/<bucket>"."cda"."cc_account_merged"
WHERE policy_status = 'ACTIVE'
LIMIT 100;
```

**BI tools** (Tableau, QuickSight, Looker) connect via the standard
Athena JDBC/ODBC driver. Point them at the federated catalog name and
the analyst's permitted workgroup.

### 2. Snowflake (external volume + Iceberg integration)

Snowflake reads S3 Tables natively via external volumes. This is
read-only — Snowflake doesn't write back into Iceberg.

**Setup outline** (Snowflake docs are authoritative; this is a sketch):

```sql
-- 1. Create an external volume pointing at the S3 Tables bucket
CREATE EXTERNAL VOLUME cda_iceberg_volume
  STORAGE_LOCATIONS =
  (
    (
      NAME = 'cda_iceberg'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://<bucket>/'
      STORAGE_AWS_ROLE_ARN = '<snowflake-iam-role-arn>'
      STORAGE_AWS_EXTERNAL_ID = '<external-id-from-snowflake>'
    )
  );

-- 2. Create an Iceberg catalog integration
CREATE CATALOG INTEGRATION cda_iceberg_catalog
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'cda'
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.<region>.amazonaws.com/iceberg'
    CATALOG_NAME = '<account>:s3tablescatalog/<bucket>'
  )
  REST_AUTHENTICATION = ( TYPE = SIGV4 SIGV4_REGION = '<region>' );

-- 3. Create a Snowflake Iceberg table from each Guidewire table
CREATE ICEBERG TABLE cc_account_merged
  EXTERNAL_VOLUME = 'cda_iceberg_volume'
  CATALOG = 'cda_iceberg_catalog'
  CATALOG_TABLE_NAME = 'cc_account_merged';
```

**Access control:** Snowflake's IAM role (the one you put in
`STORAGE_AWS_ROLE_ARN`) needs:
- `s3:GetObject` on `<bucket>/*`
- `s3:ListBucket` on `<bucket>`
- `glue:GetTable` / `glue:GetDatabase` on the federated catalog

**Refresh:** Snowflake auto-refreshes Iceberg metadata; queries see new
data within a few seconds of a pipeline commit.

### 3. Trino / Presto

Trino's Iceberg connector reads S3 Tables natively when the catalog
is registered as a Glue federated catalog.

```properties
# trino-server/etc/catalog/cda.properties
connector.name=iceberg
iceberg.catalog.type=glue
hive.metastore.glue.region=<region>
hive.metastore.glue.catalogid=<account>:s3tablescatalog/<bucket>
```

Trino respects Lake Formation grants the same way Athena does — so
a Trino service role allow-listed via Lake Formation gets the same
table access as an Athena analyst.

**Query pattern:**

```sql
USE cda;
SELECT count(*) FROM cc_account_merged;
SELECT * FROM cc_account_merged FOR TIMESTAMP AS OF TIMESTAMP '2025-12-01 00:00:00';
```

### 4. Spark (bring-your-own EMR / Glue / Databricks)

If your team is already on Spark, you can read directly with the
S3 Tables Iceberg catalog used by this pipeline:

```scala
spark.sparkContext.setLogLevel("WARN")
val accountId = "<account>"
val bucket = "<customer>-cda-iceberg-<account>-<region>"

spark.conf.set("spark.sql.extensions",
  "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.cda",
  "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.cda.catalog-impl",
  "software.amazon.s3tables.iceberg.S3TablesCatalog")
spark.conf.set("spark.sql.catalog.cda.warehouse",
  s"arn:aws:s3tables:us-east-1:$accountId:bucket/$bucket")

val df = spark.sql("SELECT * FROM cda.cda.cc_account_merged WHERE active = true")
df.show(10)
```

This is the same client config the ingest pipeline itself uses. Your
Spark job's IAM role needs the same S3 Tables permissions documented
in `cdk/lib/emr-stack.ts` (`s3tables:GetTable`, `GetTableData`, etc.).

### 5. Custom Lambda subscribers to the SNS topic

The pipeline's notification SNS topic
(`<customer>-cda-iceberg-notifications`) emits messages on every
state machine completion, failure, and CDA reset. Any Lambda
subscribed to it can take downstream action.

**Use cases:**
- Forward critical alerts to PagerDuty / Opsgenie
- Create JIRA tickets on `CdaReset` notifications
- Update an internal dashboard / status page on completion
- Trigger downstream pipelines (dbt, Airflow) when ingest succeeds

**Subscription:**

```bash
# Get the topic ARN
aws cloudformation describe-stacks \
  --stack-name <customer>-iceberg-orchestration \
  --query 'Stacks[0].Outputs[?OutputKey==`NotificationTopicArn`].OutputValue' \
  --output text

# Subscribe a Lambda
aws sns subscribe \
  --topic-arn <topic-arn> \
  --protocol lambda \
  --notification-endpoint <your-lambda-arn>
```

**Message shapes:**

```json
// On ingest completion
{
  "Subject": "<customer> CDA Iceberg run completed",
  "Message": "All per-table jobs succeeded."
}

// On CDA reset
{
  "Subject": "<customer> CDA Iceberg — CDA RESET DETECTED — ingest paused",
  "Message": "{ \"resets\": [...] }"
}

// On launch-condition failure
{
  "Subject": "<customer> CDA Iceberg launch-condition failed",
  "Message": "{ \"Error\": \"...\", \"Cause\": \"...\" }"
}
```

The full set of message subjects is in `orchestration-stack.ts`
(grep for `subject:`).

### 6. Custom Lambda subscribers to the recon table

For programmatic access to Tier A/B/D findings (rather than emails on
ingest completion), query `cda_recon_results` directly:

```python
import boto3
import time

athena = boto3.client('athena')
QUERY = """
  SELECT tier, table_name, check_name, status, expected, actual, delta
  FROM "s3tablescatalog/<bucket>"."cda"."cda_recon_results"
  WHERE run_id = (SELECT max(run_id) FROM cda_recon_results)
    AND status NOT IN ('OK', 'METRICS_PARTIAL')
"""

# Run query, poll, fetch results, fan out per finding
exec_id = athena.start_query_execution(
    QueryString=QUERY,
    ResultConfiguration={'OutputLocation': 's3://your-results-bucket/'},
)['QueryExecutionId']

# ... poll get_query_execution until SUCCEEDED ...
results = athena.get_query_results(QueryExecutionId=exec_id)
for row in results['ResultSet']['Rows'][1:]:
    fields = [c.get('VarCharValue', '') for c in row['Data']]
    # Take action: page, ticket, or auto-rebuild based on severity
```

This is the recommended pattern for **scheduled health checks** rather
than reactive alerting (use SNS for reactive).

### 7. Custom Athena saved queries

The optional `AnalyticsStack` ships four saved queries; you can add
more via your own Athena `CfnNamedQuery` resources or the AWS console.

Three patterns we've seen customers want:

```sql
-- "Did this customer see expected data on Date X?"
SELECT date_trunc('day', cda_load_ts) AS load_day,
       count(*) AS rows_landed
FROM "s3tablescatalog/<bucket>"."cda"."cc_account_raw"
WHERE cda_load_ts >= timestamp '2025-12-01'
GROUP BY 1 ORDER BY 1;

-- "Show me late-arriving change events for one id"
SELECT cda_fingerprint, gwcbi___seqval_hex, gwcbi___operation, cda_load_ts
FROM "s3tablescatalog/<bucket>"."cda"."cc_account_raw"
WHERE id = '<some-id>'
ORDER BY LPAD(gwcbi___seqval_hex, 32, '0');

-- "Which tables haven't ingested today?"
SELECT table_name, max(committed_at) AS last_seen
FROM "s3tablescatalog/<bucket>"."cda"."cda_recon_results"
GROUP BY table_name
HAVING max(committed_at) < current_date - interval '1' day
ORDER BY 2;
```

---

## Common scenarios

### Scenario 1: Snowflake-fronted enterprise data warehouse

Your team owns a Snowflake account that already houses non-Guidewire
data. You want Guidewire data to land alongside it without standing
up a separate query layer.

**Recommended pattern:**

1. Deploy this pipeline + `AnalyticsStack` (for monitoring)
2. Create Snowflake external volume + catalog integration (Pattern 2 above)
3. Create one Snowflake Iceberg table per Guidewire table you need
4. Build dbt/Snowflake views on top of those Iceberg tables for
   analytics-ready transformations

**What this avoids:** copying data into Snowflake. Iceberg metadata
is managed by S3 Tables; Snowflake reads parquet directly from S3.

### Scenario 2: Hybrid — Athena for ops, Snowflake for analytics

Operations team uses Athena for ad-hoc reconciliation queries; the
analytics team uses Snowflake for production reporting.

**Both can read the same data.** Lake Formation grants Athena access
to analyst roles; Snowflake's external volume role gets independent
S3 + Glue permissions. They don't conflict because they're independent
clients of the same Iceberg metadata.

### Scenario 3: Reactive paging on `CdaReset`

Your on-call should be paged immediately when CDA regresses, but you
don't want every "no changes" notification waking them up.

**Pattern:**

1. Create a Lambda subscribed to the SNS topic
2. In the Lambda, parse `event.Subject` and pattern-match
   `CDA RESET DETECTED`
3. On match, call your PagerDuty / Opsgenie webhook
4. Ignore other subjects (or forward to a low-priority channel)

The pipeline's SNS topic is intentionally one stream of mixed-severity
messages so customers can route them however they want.

### Scenario 4: Backfill into a non-Iceberg system

You need to push the entire `cc_account_merged` table into a downstream
SQL system (one-shot, not a recurring sync).

**Recommended pattern:**

1. Use Athena `UNLOAD` to write the table's data to an intermediate S3
   prefix in CSV / parquet:
   ```sql
   UNLOAD (SELECT * FROM "s3tablescatalog/<bucket>"."cda"."cc_account_merged")
   TO 's3://your-staging-bucket/cc_account_merged/'
   WITH (format = 'PARQUET');
   ```
2. Use whatever loader your target system supports (`COPY` for Postgres,
   `COPY INTO` for Snowflake, `LOAD DATA` for MySQL).

**Don't** read the raw S3 Tables parquet files directly — they're
under Iceberg's control and may be compacted away mid-read. Always go
through the catalog.

### Scenario 5: Custom transformation pipeline

Your team builds dbt models on top of Guidewire data; you want those
models to refresh after every successful ingest.

**Pattern:**

1. Subscribe a Lambda to the SNS topic
2. On `<customer> CDA Iceberg run completed`, trigger a dbt Cloud
   webhook or kick off a Step Functions execution that runs dbt
3. dbt reads from the merged tables via Athena / Snowflake / Trino

This decouples ingest cadence from transformation cadence — a 15-min
ingest that produces no new data won't cycle dbt unnecessarily
because the pipeline won't fire `run completed` on no-op cycles
(it fires `no changes` instead).

---

## Authentication & access control

| Consumer | Auth method | Permissions needed |
|---|---|---|
| Athena (analyst role) | IAM + Lake Formation grant | `SELECT`, `DESCRIBE` on `cda` namespace |
| Snowflake | IAM role (storage integration) | `s3:Get/List`, `glue:GetTable/Database` |
| Trino | IAM role | Same as Snowflake + Lake Formation grant |
| Spark | IAM role (instance / pod) | `s3tables:Get*`, `s3tables:ListNamespaces` |
| Lambda subscriber | Lambda execution role | `sns:Subscribe` (one-time), no ongoing |

Customer-managed KMS keys (CMKs) are not currently configured on the
S3 Tables bucket — see
[`DEPLOYMENT.md` §11.1](DEPLOYMENT.md#111-encryption-at-rest).
If your consumer requires CMK access, file an issue.

---

## Performance & cost considerations

### Read amplification

Iceberg time-travel queries (`FOR TIMESTAMP AS OF`) read more files
than a current-state query because they include older snapshot
references. Cost grows with snapshot retention window — default 5
days. Long retention = more storage + more files scanned per query.

### Compaction lag

S3 Tables compacts in the background on a schedule controlled by
AWS. Newly-written data may be in many small parquet files for the
first few hours; query performance improves as compaction merges
them. This is invisible to consumers but explains why the same query
gets faster a few hours after a load.

### Concurrent readers

S3 Tables doesn't rate-limit concurrent readers. You can have Athena,
Snowflake, and Trino all reading the same table simultaneously
without contention. The bottleneck is each consumer's own service
(Athena workgroup limits, Snowflake warehouse size).

### Pricing impact

Heavy downstream reads (dbt running every hour, Snowflake materialized
views refreshing, etc.) increase **Snowflake / Athena / Trino cost**,
not pipeline cost. The pipeline ends at the Iceberg commit; consumer
queries are billed independently.

---

## What's out of scope

This pipeline doesn't:

- **Push data anywhere outside your AWS account.** Consumer pull only.
- **Provide a REST API.** Use Athena or your downstream tool's query API.
- **Send Kafka events.** SNS is the only event surface; build Kafka
  fan-out yourself if you need it.
- **Manage downstream dbt / transformation pipelines.** Build those
  separately and trigger via SNS subscriber.
- **Support write-back to Guidewire.** Read-only consumption.

If your integration needs any of the above, those are external
systems you build alongside the pipeline, not modifications to it.

---

## Cross-references

- [`README.md`](../README.md) — pipeline overview
- [`DEPLOYMENT.md`](DEPLOYMENT.md) — deployment guide
- [`ANALYTICS.md`](ANALYTICS.md) — optional analytics stack (federation, dashboards, alarms)
- [`lambdas.md`](lambdas.md) — pipeline-internal Lambda functions
