# Sample: Guidewire CDA → Iceberg (S3 Tables)

A **sample / reference implementation** of a Spark-on-EMR-Serverless ingest
path for Guidewire CDA → **S3 Tables (Iceberg)** in your own AWS account. One
install per customer; one S3 Tables warehouse per customer.

> This is sample code published as a reference pattern for extracting data
> from Guidewire CDA into Apache Iceberg. Review, adapt, and test it against
> your own requirements before using it in production. Provided as-is under
> the MIT-0 license (see [LICENSE](LICENSE)).

## Why this matters

Guidewire CDA writes incremental change data to S3 in a fingerprint-folder
layout that's not directly queryable by analytics tools. This pipeline
lands that data into Iceberg tables in your AWS account and keeps them
in sync — so your analysts query Snowflake/Athena/Trino and get the
current state of every Guidewire table, with full change history retained.

Concretely, deploying this gets you:

- **Reduced ETL maintenance** — schema evolution, cursor management,
  reconciliation are all automated. No custom ETL code per Guidewire
  table to maintain
- **Vendor-authoritative reconciliation** — every ingest verifies row
  counts against Guidewire's own `batch-metrics.json`. You'll know if
  data went missing, not guess
- **Operational visibility** — CDA-side regressions (manifest rollback,
  pruned fingerprints) trigger paged alerts before bad data lands
- **Per-table parallelism** — 717-table fan-out completes in ~4 hours
  on a typical customer profile. No one-Spark-driver bottleneck
- **Fixed monthly cost** — EMR Serverless billing is per-job, not
  per-cluster-hour. Quiet customers see most cron triggers no-op

This implementation aligns with Guidewire's recommended **Structured
Data Lake** integration pattern. See
[When NOT to use CDA](docs/DEPLOYMENT.md#when-not-to-use-cda) for
time-sensitive flows that should use App Events or Integration Gateway
instead.

## Solution capabilities

This pipeline lands CDA data in S3 Tables (Iceberg) — the
**Structured Data Lake** consumption pattern Guidewire recommends for
analytics, ML, and regulatory reporting.

| Capability | What you get |
|---|---|
| **Target** | S3 Tables / Iceberg in your own account |
| **Best for** | Analytics, ML feature stores, regulatory reporting |
| **Query tools** | Athena / Snowflake / Trino / Spark |
| **Compaction & file management** | Managed automatically by S3 Tables |
| **Schema evolution** | Automatic, bidirectional (add / drop / widen columns) |
| **Reconciliation** | 4-tier, validated against Guidewire's own `batch-metrics.json` |
| **Per-table failure isolation** | Step Functions Map fan-out per table |
| **CDA reset detection** | Built-in; pauses ingest and alerts via SNS |
| **Customization** | Per-table exclusion, per-column exclusion, per-table executor sizing |

If you instead need CDA data in a relational database (RDS Postgres /
MySQL / SQL Server / Oracle), a direct-to-RDS consumption path is a
better fit than this repo — this pipeline targets the data-lake
pattern. Hydrating RDS from the Iceberg merged tables is **out of scope
for this sample**; the merged tables are a clean source to build such a
job against if you need both.

## Architecture

```
                Guidewire AWS account                  │           Your AWS account
                                                       │
       ┌──────────────────────────────┐                │     ┌────────────────────────────────────┐
       │ s3://customer-cda-source/    │                │     │ Ingest state machine (15 min cron) │
       │   manifest.json              │ ──── reads ──► │ ──► │   1. CheckCDAChanges (Lambda)      │
       │   <table>/<fp>/<ts>/*.pq     │                │     │      ├─ START   → Map per table   │
       │   <table>/<fp>/<ts>/.cda/    │                │     │      ├─ STOP    → notify           │
       │     batch-metrics.json       │                │     │      └─ CDA_RESET → alert + Fail   │
       └──────────────────────────────┘                │     │   2. Map (per table, max=8):       │
                                                       │     │      EMR Serverless StartJobRun   │
                                                       │     │      with size-class spark conf   │
                                                       │     │      (Spark writes cursors + HWM) │
                                                       │     └─────────────┬──────────────────────┘
                                                       │                   ▼
                                                       │     ┌────────────────────────────────────┐
                                                       │     │ S3 Tables warehouse                │
                                                       │     │   <ns>.<table>_raw    (history)    │
                                                       │     │   <ns>.<table>_merged (current)    │
                                                       │     │   <ns>.cda_recon_results (audit)   │
                                                       │     └────────────────────────────────────┘
                                                       │
                                                       │     ┌────────────────────────────────────┐
                                                       │     │ Recon state machine (daily 02:00)  │
                                                       │     │   Per-table Tier D semantic checks │
                                                       │     └────────────────────────────────────┘
```

For each changed table the Spark job:
1. Reads only timestamp folders strictly newer than its DDB cursor and ≤ manifest's HWM
2. Evolves `_raw`'s schema if needed (ADD COLUMN, ALTER TYPE, NULL-fill for dropped)
3. Appends parquet into `_raw` (partitioned by `cda_fingerprint`)
4. Advances per-fingerprint cursor in DynamoDB (atomic, max-merge)
5. Mirrors schema evolution to `_merged`
6. `MERGE INTO _merged` with `lpad(seqval_hex, 32, '0')` strict-greater guard
7. `DELETE FROM _merged` for tombstones (`gwcbi___operation = 1`)
8. Writes Tier A reconciliation row from `batch-metrics.json` deltas

Tables are **created on first touch** by the job — no separate DDL
bootstrap step.

## Quick start

```bash
# 1. Build the jar (~19 MB)
JAVA_HOME=/path/to/jdk21 ./gradlew test shadowJar

# 2. Deploy base infrastructure (no CDA source bucket yet)
cd cdk
npm install
export CDK_DEFAULT_ACCOUNT=<your account>
export CDK_DEFAULT_REGION=us-east-1
npx cdk bootstrap
npx cdk deploy --all --require-approval never

# 3. Send Guidewire the EmrJobRoleArn (CFN output) for source bucket allow-list

# 4. Upload the jar
aws s3 cp ../build/libs/cda-iceberg-client-1.0.jar \
  s3://<ArtifactBucket from outputs>/jars/

# 5. Smoke test against one table
cd .. && export APP_ID=<ServerlessApplicationId> ROLE_ARN=<EmrJobRoleArn> \
  ARTIFACT_BUCKET=<ArtifactBucket> LOGS_BUCKET=<LogsBucket> \
  TABLE_BUCKET_ARN=<TableBucketArn> SOURCE_BUCKET=<cda-source-bucket> \
  STATE_TABLE=<LaunchConditionStateTable>
./scripts/smoke-test.sh cctl_accidenttype

# 6. Re-deploy with source bucket and arm the schedule
cd cdk
npx cdk deploy --all \
  --context cdaSourceBucketArn=arn:aws:s3:::<cda-source-bucket> \
  --context scheduleEnabled=true \
  --context notificationEmails=ops@example.com
```

For production rollout — pre-flight checklist, cross-account IAM
sequencing, schedule arming gates, multi-environment patterns,
runbooks, and DR — see
[`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

## Run a full load with the script

Instead of the manual steps above, `scripts/data_load.sh` builds + deploys,
loads **all** tables, and times the run. **All configuration is centralized
in one block at the top of that script** — you don't pass long flag lists.

### What you must update before running

Open `scripts/data_load.sh` and edit the 5 values in the `CONFIGURATION`
block to point at **your** account and source (the defaults are our sample
environment):

| Variable | Change to |
|---|---|
| `ACCOUNT` | your 12-digit AWS account id |
| `AWS_REGION` | your region (e.g. `us-east-1`) |
| `CUSTOMER` | your short name/prefix for resources (e.g. `acme`) |
| `SRC_BUCKET` | your CDA source bucket **name** |
| `MANIFEST_KEY` | manifest path in that bucket (e.g. `manifest.json`) |

Everything else (EMR capacity, concurrency, size thresholds, cron, recon,
retention, …) already defaults to the last validated load's values, so you
only touch those if you want to change them. Either edit the default in the
block, or override per-run with an environment variable — see
[`docs/data-load.md`](docs/data-load.md) for the full knob catalogue and how
the layering works.

### Then run the phases

```bash
# Existing deployment → clean reload:
./scripts/data_load.sh ship     # build jar, upload, cdk deploy (your config)
./scripts/data_load.sh wipe     # DELETE all tables + cursors (type WIPE to confirm)
./scripts/data_load.sh run      # trigger the full load, print wall-clock timing
./scripts/data_load.sh report   # per-table EMR durations + percentiles

# Fresh AWS account (nothing deployed yet):
./scripts/data_load.sh bootstrap   # stands everything up first, then: run, report

# Optional analytics (Glue federation, Athena, dashboards) — AFTER a load:
ANALYST_ROLE_ARNS=arn:aws:iam::<acct>:role/Analyst ./scripts/data_load.sh analytics
```

> Need a few table failures tolerated so the run still reports SUCCEEDED?
> `TOLERATED_FAILURE_PCT=10 ./scripts/data_load.sh ship`. Full guide:
> [`docs/data-load.md`](docs/data-load.md).

## Prerequisites

- AWS account with permission to create VPC / S3 / EMR Serverless /
  Step Functions / Lambda / DynamoDB / IAM
- Node 20+, JDK 21 (build only — runtime is JDK 17 on EMR Serverless),
  AWS CLI v2
- The CDA source bucket must already exist; the EMR job role's ARN
  must be allow-listed by Guidewire on it

## Project structure

```
sample-guidewire-cda-iceberg/
├── README.md                             # this file
├── LICENSE                               # MIT-0
├── CONTRIBUTING.md
├── CODE_OF_CONDUCT.md
├── build.gradle                          # shadowJar build (~19 MB)
├── gradle.properties                     # Spark 4.0.1 / Scala 2.13 / Iceberg 1.5.2
├── docs/
│   ├── DEPLOYMENT.md                     # full deployment guide (15 sections)
│   ├── ANALYTICS.md                      # AnalyticsStack reference
│   ├── lambdas.md                        # Lambda function reference
│   └── integrations.md                   # downstream consumer patterns
├── scripts/
│   ├── smoke-test.sh                     # one-table validation runner
│   ├── rebuild-table.sh                  # drop + rebuild one table from CDA
│   └── data_load.sh                      # full-load runbook + timing (centralized config; see docs/data-load.md)
├── src/main/scala/gw/cda/iceberg/
│   ├── IcebergIngest.scala               # Spark entry point (ingest + --recon-only)
│   ├── manifest/                         # CDA manifest.json parsing
│   ├── state/                            # per-fingerprint cursor in DynamoDB
│   ├── recon/                            # 4-tier reconciliation
│   └── utils/
├── src/test/scala/                       # 87 Scala unit tests
└── cdk/                                  # AWS CDK (TypeScript)
    ├── test/                             # 23 CDK assertion tests (synth + cdk-nag)
    └── lambdas/**/test/                  # 36 Lambda unit tests
    ├── bin/app.ts
    ├── cdk.json                          # default context flags
    ├── lib/
    │   ├── network-stack.ts              # VPC + S3 gateway endpoint (or BYO)
    │   ├── iceberg-stack.ts              # S3 Tables bucket + namespace
    │   ├── emr-stack.ts                  # EMR Serverless + IAM job role
    │   ├── runtime-stack.ts              # artifact + logs buckets
    │   ├── orchestration-stack.ts        # ingest SM + recon SM + Lambdas
    │   └── analytics-stack.ts            # OPTIONAL: Glue federation, dashboards, alarms
    └── lambdas/                          # 2 Node.js Lambdas (launch-condition, recon-list)
```

## Documentation

- [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) — full deployment guide:
  pre-flight, IAM coordination with Guidewire, phased rollout,
  bulk-load monitoring, schedule arming, multi-env patterns, Lifecycle
  Events activation, monitoring, security/compliance, upgrade
  procedure, runbooks, rollback/DR, automation policy
- [`docs/ANALYTICS.md`](docs/ANALYTICS.md) — optional `AnalyticsStack`
  reference: Glue federation, Lake Formation grants, Athena saved
  queries, CloudWatch dashboard, alarms
- [`docs/lambdas.md`](docs/lambdas.md) — per-Lambda reference for
  launch-condition, recon-list
- [`docs/integrations.md`](docs/integrations.md) — downstream consumer
  patterns: Snowflake external volume, Trino on Iceberg, SNS topic
  fan-out for paging, custom alerting
- [`docs/data-load.md`](docs/data-load.md) — the `data_load.sh` full-load
  runbook and how its centralized configuration works (env var → script
  default → cdk.json layering, the knob catalogue, common recipes)
- [`docs/security/cdk-nag-report.md`](docs/security/cdk-nag-report.md) —
  cdk-nag (AwsSolutions) security scan report for AppSec review: 0
  unaddressed findings, with fixes and documented suppressions for all 6
  stacks. Regenerate with `cdk synth --context cdkNag=true`.

## Querying the data

> The optional **AnalyticsStack** automates Glue federation, Lake
> Formation grants, Athena saved queries, dashboards, and alarms.
> See [`docs/ANALYTICS.md`](docs/ANALYTICS.md) for the reference.
> Manual setup is in
> [`docs/DEPLOYMENT.md` §3.4](docs/DEPLOYMENT.md#34-athena-glue-and-lake-formation).

Once federation is set up:

```sql
SELECT * FROM "s3tablescatalog/<bucket>"."cda"."cc_account_merged" LIMIT 10;
```

For Snowflake / Trino / other consumers, see
[`docs/integrations.md`](docs/integrations.md).

## Reconciliation

Every ingest run writes audit rows to `cda_recon_results`. Three tiers
run inline; a fourth runs on a separate, less frequent schedule.

| Tier | Check | When | Cost |
|---|---|---|---|
| **A** | Per-batch row count vs Guidewire's `batch-metrics.json` | Every ingest | Low |
| **B** | Per-table cumulative count | Every ingest | Negligible |
| **C** | Drop-rate trends (query-only) | Operator-driven SQL | Zero |
| **D** | Tombstone integrity, latest-seqval, no-orphans, count-formula | Daily 02:00 (configurable) | Medium |

Operator query pattern:

```sql
SELECT * FROM cda_recon_results
WHERE run_id = '<run-id>' AND status <> 'OK';
```

Detailed semantics for each tier:
[`docs/DEPLOYMENT.md` §15](docs/DEPLOYMENT.md#15-automation-policy)
covers when each fires; Tier D's four checks are documented in
[`docs/DEPLOYMENT.md` §13.3](docs/DEPLOYMENT.md#133-tier-d-mismatch).

## Pricing

Three customer-size tiers based on what you actually pay your AWS bill:

| Customer profile | Total CDA rows | **MRR (AWS)** |
|---|---|---|
| Small carrier | < 1 B rows | **$80-180** |
| Mid carrier | 1-5 B rows | **$300-800** |
| Large carrier | > 5 B rows | **$2 000-5 000** |

Tier D recon dominates the spread at large scale (50-70% of MRR for
big carriers). Drop to weekly cadence to roughly halve total cost.

Initial bulk load is a one-shot ~$40-200 depending on data volume,
not recurring.

NOT included: Guidewire CDA tier costs, Direct Connect, downstream
query costs (Snowflake/Athena), customer's base AWS account costs.

Detailed component breakdown + cost-tuning knobs:
[`docs/DEPLOYMENT.md` §10](docs/DEPLOYMENT.md#10-performance-baselines).

## Configuration

All knobs live in `cdk/cdk.json`. Override per-deploy with
`--context KEY=VAL`. The most commonly used:

| Key | Default | Notes |
|---|---|---|
| `customerName` | `cda` | Prefixes every resource. Use a short token like your customer's name |
| `cdaSourceBucketArn` | (empty) | Required for the orchestration stack |
| `scheduleEnabled` | `false` | Set `true` after smoke-test passes |
| `cronExpression` | `0/15 * * * ? *` | Every 15 min |
| `mapStateConcurrency` | `8` | Parallel per-table EMR jobs |
| `notificationEmails` | (empty) | Comma-sep SNS subscribers |
| `enableAnalyticsStack` | `false` | Opt-in: dashboards, alarms, Athena setup |
| `lifecycleEventsEnabled` | `false` | Opt-in: CDA Lifecycle Events EAP trigger |
| `tablesToExclude` | (empty) | Comma-sep tables to skip entirely (see Customization) |
| `columnsToExclude` | (empty) | Comma-sep columns to drop (see Customization) |

Full list, defaults, and tuning guidance:
[`docs/DEPLOYMENT.md` §1.4](docs/DEPLOYMENT.md#14-naming-and-namespace-decisions).

## Customization

Two common customer asks — skipping whole tables, and dropping specific
columns — are configured with context flags. No code changes needed.

### Exclude tables

Skip tables you don't want ingested at all (e.g. tables already in
another warehouse, or ones you're not licensed to replicate):

```bash
npx cdk deploy cda-iceberg-orchestration \
  --context cdaSourceBucketArn=arn:aws:s3:::<bucket> \
  --context tablesToExclude=cc_activity,cc_note,cctl_largetypecode
```

Comma-separated, case-insensitive. Excluded tables are skipped during
change detection — both the ingest scheduler and the Tier D recon
scheduler ignore them, so no Iceberg tables are ever created for them.

To start ingesting a previously-excluded table, remove it from the
list and re-deploy; the next run picks it up from the manifest.

> **Note:** `tablesToExclude` is a *consumer-side* skip — it doesn't
> make CDA produce less data. To reduce what CDA writes at the source
> (and speed up the bulk load), use Guidewire's Data Platform block
> list instead. See
> [`docs/DEPLOYMENT.md` §1.5](docs/DEPLOYMENT.md#15-data-scope-excluding-tables-and-columns).

### Exclude columns

Drop specific columns so they never land in `_raw` or `_merged` — for
PII you don't want copied, large blob columns you don't query, or the
`gwcbi___` writer columns once you trust the pipeline:

```bash
# Drop ssn + taxid from EVERY table, and description from cc_claim only.
# No spaces — spark-submit splits the value on whitespace.
npx cdk deploy cda-iceberg-orchestration \
  --context cdaSourceBucketArn=arn:aws:s3:::<bucket> \
  --context columnsToExclude=ssn,taxid,cc_claim:description
```

Entry format:
- `colName` — exclude from **every** table
- `table:colName` — exclude from **one** table only

Columns are dropped from the source DataFrame **before** the append, so
the data never enters the warehouse (not just hidden from queries).
Case-insensitive on both table and column names.

**Protected columns cannot be excluded:** `id`,
`gwcbi___seqval_hex`, and `gwcbi___operation` are required by the MERGE
for identity, ordering, and tombstoning. Attempting to exclude one
fails the job at parse time with a clear error — it won't silently
ignore the request.

**Caveat on already-loaded tables:** column exclusion changes the
table schema. If you add an exclusion for a table that's already
ingested, drop and rebuild that table (see
[`docs/DEPLOYMENT.md` §14.2](docs/DEPLOYMENT.md#142-rebuild-a-single-table))
so its schema reflects the new exclusion. New tables pick it up
automatically.

**Alternative — hide without dropping:** if you want the column kept in
the warehouse but hidden from certain consumers, don't exclude it —
create an Athena/Snowflake VIEW that selects only the columns those
consumers should see. See
[`docs/integrations.md`](docs/integrations.md). Exclusion is for data
you don't want copied at all; views are for access control over data
you keep.

## Troubleshooting

The three issues most often hit on first deploy:

### 1. Lambda fails with `PermanentRedirect` reading manifest

**Cause:** CDA source bucket is in a different region than your
deployment. The launch-condition Lambda's S3 client follows region
redirects when properly configured; if you see this, confirm your
deploy is current — `followRegionRedirects: true` was added in v1.0+.

### 2. Athena query returns "Insufficient Lake Formation permission(s)"

**Cause:** the analyst role doesn't have the Lake Formation grant on
the cda namespace. Run:

```bash
aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier":"<analyst-role>"}' \
  --resource '{"Table":{"CatalogId":"<account>:s3tablescatalog/<bucket>","DatabaseName":"cda","TableWildcard":{}}}' \
  --permissions SELECT DESCRIBE
```

Full troubleshooting in
[`docs/ANALYTICS.md` §6](docs/ANALYTICS.md#6-troubleshooting).

### 3. EMR job fails with "No space left on device"

**Cause:** Spark shuffle spilled more than the executor's local disk
during MERGE on a large table.

**Fix:** lower the `large` size-class threshold so this table rides
the higher-disk profile:

```bash
npx cdk deploy --context sizeThresholdLarge=500000000  # default 1B
```

The full runbook (with all 9+ failure modes) lives in
[`docs/DEPLOYMENT.md` §13](docs/DEPLOYMENT.md#13-operational-runbooks).

## Testing

Three layers, no extra test dependencies (Scala uses ScalaTest via gradle;
CDK + Lambda tests use the built-in `node --test` runner + the already-present
`ts-node` and `aws-cdk-lib/assertions`).

```bash
# 1. Scala unit tests (data-plane logic: SQL builders, sanitizer, cursors,
#    recon invariants, manifest parsing)
JAVA_HOME=/path/to/jdk21 ./gradlew test          # 87 tests

# 2. CDK assertion tests (security posture: least-priv IAM, read-only source
#    grant, role-trust scoping, bucket SSE/BlockPublicAccess/TLS, DistributedMap,
#    --jars vs --packages, plus a cdk-nag clean-template guard)
cd cdk && npm test                                # 23 tests

# 3. Lambda unit tests (launch-condition decision, recon-list selection,
#    detect-reset, federation idempotency)
cd cdk && npm run test:lambdas                    # 36 tests

# CDK + Lambda together
cd cdk && npm run test:all
```

The CDK assertion tests double as a security regression guard: they fail if a
change broadens the EMR job role, weakens a bucket, drops the SNS TLS policy,
reverts `--jars` to `--packages`, or introduces a new un-suppressed cdk-nag
finding. See [`docs/security/threat-model.md`](docs/security/threat-model.md)
for the threats these controls map to.

## Support

- **Architecture / design questions**: GitHub issues on this repo
- **CDA-side issues** (manifest, bulk load, lifecycle events):
  Guidewire support
- **AWS-side issues** (S3 Tables, EMR Serverless, Lake Formation):
  AWS Support
- **Reconciliation findings**: query `cda_recon_results` directly,
  see [`docs/DEPLOYMENT.md` §5.3](docs/DEPLOYMENT.md#53-useful-queries-during-bulk-load)

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for how to
report a potential security issue. Please do **not** open a public GitHub
issue for security findings.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This project has adopted the
[Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md).

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
