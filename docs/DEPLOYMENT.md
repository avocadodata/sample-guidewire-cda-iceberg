# cda-iceberg-client — Deployment Guide

This guide is for the operators deploying and running `cda-iceberg-client`
in your AWS account. It covers everything from pre-flight planning
through day-2 operations, runbooks, and disaster recovery.

If this is your first time, start with the [README](../README.md) for an
overview of what the pipeline does, then come back here when you're ready
to plan the deployment.

---

## What you're deploying

`cda-iceberg-client` runs in your AWS account and:

- Reads parquet files from your Guidewire CDA source bucket
- Writes them to your own S3 Tables (Iceberg) warehouse
- Maintains two views per Guidewire table — `_raw` (append-only history)
  and `_merged` (current resolved state)
- Runs reconciliation against Guidewire's own `batch-metrics.json` to
  prove every row is accounted for
- Fires alerts when CDA-side regressions or pipeline-side errors occur

It does **not** push data anywhere outside your AWS account. Downstream
analytics tools (Athena, Snowflake, Trino, Tableau) are your choice and
read from the Iceberg warehouse on whatever cadence you want.

### Architecture

```
                 Guidewire AWS account                   │           Your AWS account
                                                         │
       ┌──────────────────────────────┐                  │     ┌────────────────────────────────────┐
       │ s3://customer-cda-source/    │                  │     │ Ingest state machine (15 min cron) │
       │   manifest.json              │ ──── reads ────► │ ──► │   1. CheckCDAChanges (Lambda)      │
       │   <table>/<fp>/<ts>/*.pq     │                  │     │      ├─ START   → Map per table   │
       │   <table>/<fp>/<ts>/.cda/    │                  │     │      ├─ STOP    → notify           │
       │     batch-metrics.json       │                  │     │      └─ CDA_RESET → alert + Fail   │
       └──────────────────────────────┘                  │     │   2. Map (per table, max=8):       │
                                                         │     │      Choice on $.sizeClass        │
       ┌──────────────────────────────┐                  │     │      → small / medium / large     │
       │ Lifecycle Events (optional)  │ ── triggers ──►  │ ──► │      EMR Serverless StartJobRun   │
       │ partner event source         │                  │     │      (Spark writes cursors + HWM) │
       └──────────────────────────────┘                  │     └─────────────┬──────────────────────┘
                                                         │                   ▼
                                                         │     ┌────────────────────────────────────┐
                                                         │     │ S3 Tables warehouse                │
                                                         │     │   <ns>.<table>_raw   (history)     │
                                                         │     │   <ns>.<table>_merged (current)    │
                                                         │     │   <ns>.cda_recon_results (audit)   │
                                                         │     └────────────────────────────────────┘
                                                         │
                                                         │     ┌────────────────────────────────────┐
                                                         │     │ Recon state machine (daily 02:00)  │
                                                         │     │   ListTables → Map (max=4):       │
                                                         │     │     EMR --recon-only (Tier D)      │
                                                         │     └────────────────────────────────────┘
                                                         │
                                                         │     ┌────────────────────────────────────┐
                                                         │     │ Operator monitoring (your tools)   │
                                                         │     │   SNS topic ──► email/PagerDuty   │
                                                         │     │   CloudWatch alarms (§9)           │
                                                         │     │   Athena queries vs cda_recon_*    │
                                                         │     └────────────────────────────────────┘
```

The pipeline only reads from the Guidewire side. Everything else lives
in your AWS account, paid for from your bill, governed by your IAM
policies.

### Responsibility matrix

| Concern | You | Guidewire | This pipeline |
|---|---|---|---|
| AWS account setup, IAM identities, networking | ✅ | | |
| AWS service quotas (EMR Serverless vCPU, etc.) | ✅ | | |
| Coordination to allow-list EMR role on source bucket | ✅ | ✅ | |
| Producing parquet and `manifest.json` in source bucket | | ✅ | |
| Bucket-side IAM policy on source bucket | | ✅ | |
| Optional: enrolling in CDA Lifecycle Events EAP | | ✅ | |
| Scheduling cadence (15-min vs hourly vs lifecycle-event-driven) | ✅ | | |
| Tier D recon frequency (daily vs weekly vs on-demand) | ✅ | | |
| Per-table size-class thresholds | ✅ | | |
| Block-list of source-side tables (not relevant for this pipeline) | | ✅ | |
| Tag conventions, log retention, KMS choice | ✅ | | |
| Customer-managed KMS keys (CMKs) | ✅ | | (open issue if required) |
| Pipeline-internal IAM (each component scoped role) | | | ✅ |
| Incremental, idempotent reads from source bucket | | | ✅ |
| Schema evolution as Guidewire's tables evolve | | | ✅ |
| Reconciliation against Guidewire's batch metrics | | | ✅ |
| Per-fingerprint cursor management (crash → no duplicates) | | | ✅ |
| Detecting CDA-side regressions, pausing ingest before bad data lands | | | ✅ |
| Monitoring, alerting, paging integration | ✅ | | (signals provided) |
| Cost management within your AWS account | ✅ | | |
| Backup vs snapshot (ransomware-resilient backup is your choice) | ✅ | | |
| Compliance certification (SOC 2 / HIPAA / etc.) | ✅ | | (no warranties) |
| Right-to-be-forgotten / GDPR procedure | ✅ | ✅ | (procedure documented in §11.6) |
| Upgrade testing and canary in your environments | ✅ | | (procedure in §12) |
| Source code maintenance (security patches, dependencies) | | | ✅ |
| Architecture / design questions | | | ✅ (GitHub issues) |
| AWS service support tickets | ✅ | | |
| Guidewire CDA support tickets | | ✅ | |

---

## When NOT to use CDA

CDA (and therefore this pipeline) is built for **analytical** consumption
of Guidewire data — a near-real-time mirror of every table for queries,
ML, and reporting. It is the right tool when you want the *whole dataset*
kept in sync.

It is the **wrong** tool when you need:

| You need… | Use instead |
|---|---|
| Sub-second reaction to a single business event (e.g. "policy bound → call an API") | Guidewire **App Events** / Event Messaging |
| Synchronous request/response with a Guidewire system | **Integration Gateway** / Cloud APIs |
| Only a handful of fields from one table, on demand | A Guidewire **REST API** call, not a full data mirror |
| To push data *into* Guidewire | Inbound integration APIs (this pipeline is read-only) |

The deciding factor is **latency and shape**: CDA delivers the full
dataset on a batch cadence (typically every 90–120 s on the producer
side, consumed here every 15 min by default). If your use case can't
tolerate that lag, or only needs a thin slice of one table, a
data-lake mirror is the wrong shape — reach for the event/API patterns
above. If you want a queryable, reconciled, history-preserving copy of
Guidewire data for analytics, you're in the right place.

---

## Table of contents

1. [Pre-deployment planning](#1-pre-deployment-planning)
2. [Network mode selection](#2-network-mode-selection)
3. [Cross-account IAM with Guidewire](#3-cross-account-iam-with-guidewire)
4. [Phased rollout](#4-phased-rollout)
5. [Bulk-load monitoring](#5-bulk-load-monitoring)
6. [Schedule arming gates](#6-schedule-arming-gates)
7. [Multi-environment patterns](#7-multi-environment-patterns)
8. [Lifecycle Events activation](#8-lifecycle-events-activation)
9. [Monitoring and alerting](#9-monitoring-and-alerting)
10. [Performance baselines](#10-performance-baselines)
11. [Security and compliance](#11-security-and-compliance)
12. [Upgrade procedure](#12-upgrade-procedure)
13. [Operational runbooks](#13-operational-runbooks)
14. [Rollback and DR](#14-rollback-and-dr)
15. [Automation policy](#15-automation-policy)

---

## 1. Pre-deployment planning

Answer these before opening a CDK shell.

### 1.1 Coordination with Guidewire

Three pieces of information come from Guidewire and have nothing to do
with AWS. None can be substituted from your side.

| Need | Provided by | Typical lead time |
|---|---|---|
| CDA source bucket name | Guidewire CDA team | days, sometimes weeks |
| CDA source bucket region | Guidewire CDA team | included with above |
| Allow-list slot for your EMR job role ARN | Guidewire CDA team | hours-to-days after you provide the ARN |

Guidewire will not accept a placeholder ARN. The deployment is a
two-step round-trip:

1. Deploy the base infrastructure first (no source bucket configured)
2. Send Guidewire the EMR job role ARN that CloudFormation outputs
3. Wait for Guidewire to update their bucket policy
4. Deploy a second time with the source bucket ARN configured

Plan for this sequencing. The longest single delay in a typical
deployment is the Guidewire round-trip, not the AWS work.

### 1.2 AWS account preparation

Before starting, confirm:

- **S3 Tables is available in your region.** S3 Tables is generally
  available in most commercial regions (late 2025); GovCloud and a few
  newer regions may not have it yet. The command
  `aws s3tables list-table-buckets` returns an error in unsupported
  regions.
- **EMR Serverless quotas are sufficient.** Default per-account quotas
  cover a typical mid-size carrier. Large carriers running concurrent
  Tier-D recon jobs may need to raise the
  `Maximum concurrent vCPUs per application` quota above the default.
  Raise via the AWS Service Quotas console.
- **Lake Formation access mode.** This pipeline registers an S3 Tables
  federated catalog through Glue. If your account has Lake Formation
  in strict mode, the deployer's IAM identity must be a Lake Formation
  data lake admin **before** Athena queries against the Iceberg data
  work. Section 3.4 covers this.
- **CDK bootstrap has been run.** `cdk bootstrap` once per
  account+region is enough; existing v17+ bootstraps need no
  re-run.

### 1.3 Deployer IAM permissions

The deployer's IAM identity needs write permissions on a long list of
services. The minimum set:

```
cloudformation:*    iam:*               (the pipeline mints its own EMR job role)
s3:*                s3tables:*
ec2:*               vpc-related actions (only if creating a new VPC)
emr-serverless:*
events:*            (creating EventBridge rules and bus)
states:*            (creating the state machines)
lambda:*
dynamodb:*
sns:*
logs:*
glue:CreateCatalog                          (only if you set up federation manually)
lakeformation:PutDataLakeSettings, GrantPermissions
                                            (only if bootstrapping LF in this flow)
```

Most enterprise IAM teams will not grant these as a single `*` policy.
Negotiate a custom policy named e.g. `cda-iceberg-deployer` with these
specific actions; AWS Service-managed policies don't bundle this exact
combination.

### 1.4 Naming and namespace decisions

These knobs are easy to set on first deploy and painful to change later.

| Knob | Default | What to consider |
|---|---|---|
| `customerName` | `cda` | Prefixes every resource. Use a short, lowercase token like your customer's name (e.g. `acme`). Don't change after first deploy — every bucket, role, table is named with it. |
| `icebergNamespace` | `cda` | Iceberg namespace under the table bucket. If you plan to support multiple InsuranceSuite apps in one warehouse, plan for one namespace per app (`cc`, `pc`, `bc`) and override per-deploy. |
| `tags` | (empty) | Resource tags applied to every taggable resource. Set this once; tags propagate via stack-level `Tags.of`. Most enterprises require `CostCenter`, `Environment`, `Project`, sometimes `DataClassification`. |

The default `customerName=cda` produces resource names like
`cda-cda-iceberg-...`. That's cosmetic, not broken. Override
`customerName` to your short token to get cleaner names.

### 1.5 Data scope: excluding tables and columns

By default the pipeline ingests every table CDA writes, with every
column. Two context flags narrow that scope. Both are pure
configuration — no code changes, no fork.

There are **three** layers at which you can reduce what lands in the
warehouse. Pick the one that matches your intent:

| Layer | Where | What it does | Cost effect |
|---|---|---|---|
| Guidewire block list | CDA source (Guidewire-side) | CDA never *writes* the table/column to S3 | Cheapest — less data produced, smaller bulk load |
| `tablesToExclude` | This CDK | We never *read* the table; no Iceberg table created | CDA still writes it; you just don't pay to ingest |
| `columnsToExclude` | This CDK | We read the table but *drop* columns before append | CDA still writes them; not stored in `_raw`/`_merged` |

If you control the source-side block list (coordinate with your
Guidewire CSM — see §1.1), that's the most efficient lever for a large
bulk load: data you never produce is data you never pay to move,
store, or compact. Use the CDK-side flags for scope decisions you own
on the consumer side, or for source data you can't change.

#### Excluding tables — `tablesToExclude`

Comma-separated, case-insensitive list of CDA table names to skip:

```bash
npx cdk deploy cda-iceberg-orchestration \
  --context cdaSourceBucketArn=arn:aws:s3:::<bucket> \
  --context tablesToExclude=cc_activity,cc_note,cctl_largetypecode
```

Excluded tables are filtered out during change detection in **both**
the ingest launch-condition Lambda and the Tier D recon-list Lambda, so
no Step Functions Map iteration is ever dispatched for them and no
Iceberg tables (`_raw`/`_merged`) are created. To begin ingesting a
previously-excluded table, drop it from the list and re-deploy; the
next scheduled run discovers it from the manifest and bulk-loads it
from its earliest fingerprint.

#### Excluding columns — `columnsToExclude`

Comma-separated list (**no spaces** — `spark-submit` splits the driver
env var on whitespace, and `cdk synth` fails loudly if you include
any). Each entry is either:

- `colName` — drop from **every** table
- `table:colName` — drop from **one** table only

```bash
npx cdk deploy cda-iceberg-orchestration \
  --context cdaSourceBucketArn=arn:aws:s3:::<bucket> \
  --context columnsToExclude=ssn,taxid,cc_claim:description
```

The Spark job drops these columns from the source DataFrame **before**
the append to `_raw`, so they never enter the warehouse at all — this
is data minimization, not query-time masking. Table and column names
are matched case-insensitively (CDA emits lowercase).

**Protected columns.** `id`, `gwcbi___seqval_hex`, and
`gwcbi___operation` cannot be excluded — the MERGE relies on them for
row identity, change ordering, and tombstone handling. Listing one
(globally or per-table) fails the job at parse time with an explicit
error rather than silently ignoring the request. This is enforced in
`ColumnExclusion.parse` and covered by unit tests.

**Already-loaded tables.** Column exclusion changes a table's schema.
If you add an exclusion for a table that has already been ingested,
rebuild that table so its stored schema reflects the change — see
[§14.2 Rebuild a single table](#142-rebuild-a-single-table). Newly
discovered tables apply the exclusion automatically on first load. The
pipeline's bidirectional schema evolution will *not* retroactively drop
a column that is already a stored field; it only adds/widens.

**When to use a view instead.** If you want the column kept in the
warehouse but hidden from a subset of consumers, do **not** exclude it.
Create an Athena or Snowflake view that selects only the permitted
columns and grant on the view. Exclusion is for data you don't want
copied at all; views are access control over data you keep. See
[`integrations.md`](integrations.md).

---

## 2. Network mode selection

Two modes available: a new VPC (greenfield, dev) or BYO VPC (production).
Pick based on who owns network policy in your organization.

### 2.1 New VPC mode

```bash
npx cdk deploy --all --context newVpcCidr=10.40.0.0/16
```

Provisions a 2-tier VPC (PUBLIC + PRIVATE_WITH_EGRESS) with one NAT
gateway, one S3 gateway endpoint, and the EMR security group. Use this
when starting fresh or when the deployment doesn't need to integrate
with existing analytics workloads.

Cost note: the NAT gateway is roughly $32/month regardless of traffic.
The S3 gateway endpoint provisioned is free and routes all S3 traffic
without egress charges.

### 2.2 BYO VPC mode

```bash
npx cdk deploy --all \
  --context vpcId=vpc-0123456789abcdef \
  --context existingVpcPrivateSubnetIds=subnet-aaa,subnet-bbb,subnet-ccc
```

The existing VPC must have:

- **Two or more private subnets across distinct Availability Zones**
  for EMR Serverless to schedule across. Single-AZ subnets work but
  eliminate the redundancy benefit.
- **An S3 gateway endpoint** (or VPC interface endpoint for S3) on
  those subnets. EMR Serverless reads parquet from the source bucket
  and writes to the warehouse on every job. Without the endpoint,
  every byte transits NAT, which can cost thousands of dollars per
  month for a large carrier.
- **Outbound internet access** through NAT or proxy is *not* required
  for the Iceberg runtime. As of the `--jars` change (see §below), the
  Iceberg + S3 Tables runtime jars are staged once to
  `s3://<artifact>/deps/` by `data_load.sh` (gradle `stageRuntimeDeps`)
  and referenced with `--jars` over the S3 gateway endpoint — no Maven
  fetch at job start. (Earlier builds used `--packages`, which resolved
  from Maven Central on every cold start and flaked under Map
  concurrency: simultaneous jobs hammered Maven and a fraction got
  transient "module not found" failures. The escape hatch
  `useMavenPackages=true` restores the old `--packages` behaviour if you
  ever need it.) NAT/proxy egress may still be wanted for other AWS
  service calls, but the runtime classpath no longer depends on it.

If `existingVpcPrivateSubnetIds` is empty, the pipeline uses whichever
subnets CDK's `Vpc.fromLookup` finds tagged `aws-cdk:subnet-type=Private`.
Most VPCs have these tagged subnets, but enterprise VPCs sometimes
don't — pass the subnet IDs explicitly to be safe.

### 2.3 Validation after deploy

```bash
# Confirm EMR Serverless app picked up the right subnets
aws emr-serverless get-application --application-id <id> \
  --query 'application.networkConfiguration'

# Confirm S3 gateway endpoint is associated with those subnets
aws ec2 describe-vpc-endpoints \
  --filters "Name=service-name,Values=com.amazonaws.<region>.s3" \
  --query 'VpcEndpoints[*].[VpcId,RouteTableIds,SubnetIds]'
```

---

## 3. Cross-account IAM with Guidewire

This is the most common project blocker. Plan it out early.

### 3.1 The sequencing

The CDA source bucket lives in **Guidewire's** AWS account. Your EMR
job role lives in **your** account. Both sides need policies that
reference the other side. Order matters:

1. Deploy the network + iceberg + emr + runtime stacks (no source
   bucket configured yet — pass `cdaSourceBucketArn=""`).
2. CloudFormation outputs the new role ARN as `EmrJobRoleArn`.
3. Send Guidewire the role ARN and request bucket-side allow-list.
4. Wait for Guidewire to confirm the bucket policy is in place. This
   step can take hours to days.
5. Re-deploy the EMR stack with `cdaSourceBucketArn=arn:aws:s3:::<bucket>`
   to attach the role-side policy. The role can technically read
   without this — IAM is identity-side AND resource-side — but the
   role-side policy is what scopes down the role's permissions.
6. Re-deploy with `scheduleEnabled=false` until smoke-testing
   confirms reads work.

### 3.2 Bucket policy to request from Guidewire

Send Guidewire this JSON (substituting your role ARN and their bucket
name):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowCdaIcebergClientRead",
    "Effect": "Allow",
    "Principal": { "AWS": "arn:aws:iam::<your-account>:role/<EmrJobRoleArn-name>" },
    "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
    "Resource": [
      "arn:aws:s3:::<cda-source-bucket>",
      "arn:aws:s3:::<cda-source-bucket>/*"
    ]
  }]
}
```

`s3:GetBucketLocation` is required so the launch-condition Lambda's S3
client can follow region redirects when the source bucket is in a
different region.

### 3.3 Validate the cross-account read

Once Guidewire confirms the policy is live:

```bash
# From a session that can assume the EMR job role
aws sts assume-role --role-arn <EmrJobRoleArn> \
  --role-session-name validation
# (export the credentials from the assume-role output)

# Confirm read works
aws s3 ls s3://<cda-source-bucket>/
aws s3 cp s3://<cda-source-bucket>/manifest.json /tmp/test-manifest.json
```

If either fails with `AccessDenied`, the bucket policy isn't applied
yet (or has a typo). Don't move on until both succeed.

### 3.4 Athena, Glue, and Lake Formation

For analysts using Athena to query the Iceberg data, one-time setup:

```bash
# Federate the S3 Tables bucket into Glue
aws glue create-catalog --name s3tablescatalog --catalog-input '{
  "FederatedCatalog": {
    "Identifier": "arn:aws:s3tables:<region>:<account>:bucket/*",
    "ConnectionName": "aws:s3tables"
  },
  "CreateDatabaseDefaultPermissions": [],
  "CreateTableDefaultPermissions": []
}'

# Add the analyst role(s) as Lake Formation admins (or use grant-permissions
# scoped to specific tables)
aws lakeformation put-data-lake-settings --data-lake-settings '{
  "DataLakeAdmins": [
    {"DataLakePrincipalIdentifier": "arn:aws:iam::<account>:role/<analyst-role>"}
  ],
  "CreateDatabaseDefaultPermissions": [
    {"Principal":{"DataLakePrincipalIdentifier":"IAM_ALLOWED_PRINCIPALS"},"Permissions":["ALL"]}
  ],
  "CreateTableDefaultPermissions": [
    {"Principal":{"DataLakePrincipalIdentifier":"IAM_ALLOWED_PRINCIPALS"},"Permissions":["ALL"]}
  ]
}'

# Grant SELECT/DESCRIBE on the cda namespace
aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier":"arn:aws:iam::<account>:role/<analyst-role>"}' \
  --resource '{"Table":{"CatalogId":"<account>:s3tablescatalog/<bucket-name>","DatabaseName":"cda","TableWildcard":{}}}' \
  --permissions SELECT DESCRIBE
```

Lake Formation returns an opaque error
(`Insufficient Lake Formation permission(s)`) that doesn't say which
permission is missing. If a query fails, run the `grant-permissions`
again with `--permissions SELECT DESCRIBE` on the specific table — the
table wildcard above doesn't always cover newly-created tables.

---

## 4. Phased rollout

A phased approach is strongly recommended. The phases below map to
clear validation gates between each step.

### Phase 4.1 — Base infrastructure (no source bucket)

```bash
cd cdk
npm install
export CDK_DEFAULT_ACCOUNT=<account>
export CDK_DEFAULT_REGION=us-east-1
npx cdk bootstrap
npx cdk deploy --all --require-approval never
```

This deploys network, iceberg, emr, and runtime stacks. The
orchestration stack is **not** deployed at this point because it
requires a source bucket to bind the launch-condition Lambda.

**Validation:**
- All four stacks are `CREATE_COMPLETE`
- CloudFormation outputs include `EmrJobRoleArn`,
  `ServerlessApplicationId`, `TableBucketArn`, `ArtifactBucket`,
  `LogsBucket`
- `aws s3tables list-namespaces --table-bucket-arn <arn>` returns the
  `cda` namespace
- The artifact bucket exists and is empty

**Time:** 5–8 minutes for a fresh VPC.

### Phase 4.2 — Cross-account IAM (out-of-band)

Send the role ARN to Guidewire, wait for confirmation, validate as in
[§3.3](#33-validate-the-cross-account-read).

### Phase 4.3 — Build and upload the jar

```bash
JAVA_HOME=/path/to/jdk21 ./gradlew clean test shadowJar
ls -la build/libs/cda-iceberg-client-1.0.jar    # ~19 MB
aws s3 cp build/libs/cda-iceberg-client-1.0.jar \
  s3://<artifact-bucket>/jars/

# Stage the Iceberg + S3 Tables runtime jars too — EMR references them
# with --jars (not --packages from Maven) at job start.
JAVA_HOME=/path/to/jdk21 ./gradlew stageRuntimeDeps
aws s3 sync build/runtime-libs/ s3://<artifact-bucket>/deps/ \
  --exclude '*' --include '*.jar'
```

(`data_load.sh ship` / `bootstrap` do both uploads automatically via
`ship_deps`; the commands above are the manual equivalent.)

**Validation:** the jar is in S3 at
`s3://<artifact-bucket>/jars/cda-iceberg-client-1.0.jar` and the two
runtime jars are at `s3://<artifact-bucket>/deps/`.

### Phase 4.4 — Single-table smoke test

This is the cheapest end-to-end validation. Pick a small table — a
typecode like `cctl_accidenttype` is ideal (typically a small number
of rows, single fingerprint).

```bash
PARAMS="--class gw.cda.iceberg.IcebergIngest \
  --jars s3://<artifact-bucket>/deps/iceberg-spark-runtime-3.5_2.13-1.5.2.jar,s3://<artifact-bucket>/deps/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.s3tables=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.s3tables.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
  --conf spark.sql.catalog.s3tables.warehouse=<TableBucketArn> \
  --conf spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED \
  --conf spark.executor.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED \
  --conf spark.emr-serverless.executor.disk=200G \
  --conf spark.sql.shuffle.partitions=400"

cat > /tmp/smoke.json <<EOF
{
  "sparkSubmit": {
    "entryPoint": "s3://<artifact-bucket>/jars/cda-iceberg-client-1.0.jar",
    "entryPointArguments": ["cctl_accidenttype", "cda", "<cda-source-bucket>", "manifest.json", "<state-table-name>"],
    "sparkSubmitParameters": "$PARAMS"
  }
}
EOF

aws emr-serverless start-job-run --application-id <app-id> \
  --execution-role-arn <EmrJobRoleArn> \
  --name smoke-test \
  --job-driver file:///tmp/smoke.json \
  --configuration-overrides '{"monitoringConfiguration":{"s3MonitoringConfiguration":{"logUri":"s3://<logs-bucket>/"}}}'
```

The orchestration stack creates the DynamoDB state table on first
deploy. For a manual smoke test before deploying orchestration, the
`<state-table-name>` argument can be any string — `CursorStore` will
silently fail to write cursors, but the ingest itself completes (raw +
merged tables get created). The simpler path is to deploy
orchestration first (Phase 4.5), then smoke-test.

**Validation:**
- Job state reaches `SUCCESS` in 60–120 seconds
- `aws s3tables list-tables --table-bucket-arn <TableBucketArn> --namespace cda`
  shows `cctl_accidenttype_raw` and `cctl_accidenttype_merged`
- The merged table has rows visible via Athena (after Lake Formation
  grants from §3.4)

**Common failure modes:**

- `Unrecognized option: --add-opens` — your spark-submit string has
  multiple `--add-opens` inside one `--conf` value. Each `--conf`
  accepts exactly one `--add-opens` because spark-submit splits on
  whitespace. Use one `--conf` per `--add-opens`.
- `PermanentRedirect` on the manifest fetch — source bucket is in a
  different region than the EMR job. Already handled by
  `followRegionRedirects: true` in the launch-condition Lambda; if
  you see this in a Spark job, the EMR-side Hadoop S3A client also
  handles redirects but may need `fs.s3a.endpoint.region` set
  explicitly. Rare.
- `BadRequestException: The specified table name is not valid` — S3
  Tables rejects names starting with underscore. Don't use leading
  underscores when adding custom Iceberg tables.

### Phase 4.5 — Deploy orchestration with schedule disabled

```bash
npx cdk deploy --all \
  --context cdaSourceBucketArn=arn:aws:s3:::<cda-source-bucket> \
  --context cdaManifestKey=manifest.json \
  --context scheduleEnabled=false \
  --context reconScheduleEnabled=false
```

`scheduleEnabled=false` provisions the EventBridge rule but leaves it
disabled. Same for recon. Both state machines can still be triggered
manually.

**Validation:** trigger the ingest state machine once manually:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <ingest-sm-arn> \
  --name validation-$(date +%s)
```

Watch the execution in the console. Expected flow on a fresh deploy:

1. `CheckCDAChanges` Lambda runs, returns `START` with the full table list
2. Map fans out at concurrency 8
3. Each table runs its EMR Serverless job (60s–15 min depending on size).
   The Spark job advances both the per-fingerprint cursors and the
   table-level high-water mark in DynamoDB from the manifest snapshot it
   read — there is no separate post-Map bookmark Lambda.
4. `NotifyCompletion` SNS publish
5. Execution `SUCCEEDED`

If the source bucket has the full bulk-load already complete, this run
takes hours and ingests everything. See
[§5](#5-bulk-load-monitoring) before starting it.

If CDA is still doing its initial bulk load, the manifest will only
have a subset of tables; this run ingests them incrementally as
Guidewire emits each one. That's expected — each subsequent run picks
up new tables as the bulk load progresses.

### Phase 4.6 — Arm the cron schedule

After validating an end-to-end run, re-deploy with
`scheduleEnabled=true`:

```bash
npx cdk deploy --all \
  --context cdaSourceBucketArn=arn:aws:s3:::<cda-source-bucket> \
  --context scheduleEnabled=true \
  --context cronExpression='0/15 * * * ? *' \
  --context notificationEmails=ops@example.com
```

EventBridge now triggers ingest every 15 minutes.
[§5](#5-bulk-load-monitoring) covers what's normal during the
CDA-side bulk load that's now feeding your pipeline; §5 and
[§6](#6-schedule-arming-gates) together describe the first 24-hour
and first-week watch.

---

## 5. Bulk-load monitoring

The CDA-side bulk load takes 30–48 hours per TB of source data per
Guidewire's published benchmarks. During that window, the manifest
gradually grows as each table completes its first parquet emission.

A running CDA bulk load doesn't need any special handling on the
pipeline side. The launch-condition Lambda picks up new tables
naturally as their `lastSuccessfulWriteTimestamp` appears in the
manifest. Knowing what's normal vs. not helps spot real problems.

### 5.-1 Why the per-table fan-out is a Distributed Map

Both the ingest and recon state machines fan out per table using a
Step Functions **Distributed Map**, not an inline Map. This is required
for full-catalogue (700+ table) loads:

- An **inline** Map runs every per-table step — including the
  `Wait → GetJobRun → Choice` poll loop that repeats every
  ~`serverlessPollIntervalSeconds` for the life of each table's EMR job —
  inside the **parent** execution's history. Step Functions caps an
  execution at **25,000 history events**. A 717-table load blows that
  ceiling partway through, and the execution dies with
  `States.Runtime: reached the maximum number of history events (25000)`
  even though every table that ran succeeded.
- A **Distributed Map** runs each table as its **own child STANDARD
  execution** with its own 25,000-event budget; the parent records only
  one event per child. The full fan-out then stays well under the limit.

Operator-visible consequences:
- Each table appears as a **child execution** in the Step Functions
  console (under the parent's "Map Run"), not as inline iterations.
- `mapToleratedFailurePercentage` (§5.0) is a native Distributed Map
  setting.
- The state-machine role is granted `states:StartExecution` /
  `DescribeExecution` / `StopExecution` on its own child executions —
  expected, and documented in the cdk-nag report.

### 5.0 Strict vs. tolerated failures (`mapToleratedFailurePercentage`)

By default the per-table Map is **strict**: if any single table's EMR
job fails, the whole Step Functions execution ends `FAILED` and SNS
alerts fire. That is the right behavior for steady-state incremental
runs — a failing table is a real signal an operator should see, and a
silently-tolerated failure is how data quietly goes missing.

For a **large initial bulk load**, strict fail-fast has a downside: one
transient EMR capacity blip on table #600 fails the entire multi-hour
execution even though 599 tables succeeded. To run a resilient bulk
load, set a tolerance:

```bash
npx cdk deploy cda-iceberg-orchestration \
  --context cdaSourceBucketArn=arn:aws:s3:::<bucket> \
  --context mapToleratedFailurePercentage=10   # tolerate up to 10% per-table failures
```

With this set, the execution reports `SUCCEEDED` as long as the
failure rate stays under the threshold, and the failed tables are
simply not advanced (their cursors don't move, so the next run retries
them). **Completeness is then proven by reconciliation (Tier A/D) and
the per-table EMR report — not by the execution status.** Always check
both after a tolerated-failure run.

Default is `0` (strict). Revert to strict for steady-state before you
arm the cron schedule (§6). The same knob applies to the Tier D recon
Map.

### 5.1 What's normal during bulk load

- Manifest size grows over hours-to-days
- New tables appear in `cda_recon_results` as the schedule processes
  them
- `metrics_dropped > 0` in Tier A is normal — CDA legitimately drops
  duplicates, blocklisted rows, and rows without primary keys
- Some tables show `METRICS_PARTIAL` if CDA hasn't yet flushed the
  `.cda/batch-metrics.json` file for every timestamp folder. Should
  resolve on the next ingest cycle.

### 5.2 What's NOT normal

- `cda_recon_results` rows with `status='MISMATCH'` and large negative
  `delta` — landed less than expected. Most common cause: a parquet
  read failed silently and Spark continued. Check the EMR Serverless
  driver logs for `ERROR` lines in the affected table's run.
- `metrics_missing` listing folders that *do* exist on `aws s3 ls` —
  bucket-side IAM regression. Re-validate
  [§3.3](#33-validate-the-cross-account-read).
- Step Functions execution stuck in `RUNNING` for hours past expected —
  one of the per-table EMR jobs is stuck. The state machine times out
  per-job at 24 hours by default. See
  [§13.4](#134-emr-serverless-capacity-exhaustion).

### 5.3 Useful queries during bulk load

The Athena queries below assume the Glue federation + Lake Formation
grants from [§3.4](#34-athena-glue-and-lake-formation) are in place.
Without that one-time setup, the table reference path
`s3tablescatalog/<bucket>` is not resolvable.

```sql
-- How many tables loaded so far?
SELECT count(distinct table_name)
FROM "s3tablescatalog/<bucket>"."cda"."cda_recon_results";

-- Most recent run summary
SELECT status, count(*) AS rows
FROM "s3tablescatalog/<bucket>"."cda"."cda_recon_results"
WHERE run_id = (SELECT max(run_id) FROM cda_recon_results)
GROUP BY status;

-- Tables loaded today
SELECT table_name, max(committed_at) AS last_recon
FROM cda_recon_results
WHERE committed_at > current_date
GROUP BY table_name
ORDER BY 2;
```

---

## 6. Schedule arming gates

Don't enable the schedule until all of these gates pass.

| Gate | How to verify |
|---|---|
| End-to-end manual run succeeded | Step Functions execution `SUCCEEDED`, ≥1 row in `cda_recon_results` with status `OK` |
| At least one large table loaded successfully | Find the largest table in the manifest by `totalProcessedRecordsCount`, confirm `<table>_raw` exists with the expected row count |
| Tier A reconciliation is clean | `SELECT count(*) FROM cda_recon_results WHERE tier='A' AND status<>'OK'` returns 0 (or only `METRICS_PARTIAL` rows) |
| Notification topic delivers | Subscribed email gets the `notifyCompletion` message |
| Lake Formation queries work | Athena returns rows from `_merged` for your validation user |

After arming:

| Watch | First 24 hr | First week |
|---|---|---|
| EMR Serverless cost | Should match MRR estimate ± 50%, not 10× | Within MRR band |
| EMR Serverless queue depth | Should drain between 15-min intervals | Same |
| Step Functions failed executions | Investigate any | Investigate any new ones |
| `cda_recon_results` MISMATCH count | Investigate any | Should trend toward zero |
| DynamoDB read/write capacity | <10 RCU/WCU/sec on average | Same |
| SNS delivery | Subscribers receiving expected mix of completion / no-change messages | Same |

The first 24-hour check is the most important one — most production
issues appear within the first dozen runs. After that, behavior is
generally stable.

---

## 7. Multi-environment patterns

Three common patterns, in order of how cleanly they isolate environments:

### 7.1 One-AWS-account-per-environment (recommended)

```
account-dev   → customerName=acme, icebergNamespace=cda
account-stg   → customerName=acme, icebergNamespace=cda
account-prod  → customerName=acme, icebergNamespace=cda
```

Pros: complete blast-radius isolation, no IAM cross-talk, easiest IAM
review (each account has its own EMR job role).

Cons: three accounts to manage, three Guidewire allow-list rounds (one
per environment).

This is the recommended pattern for production. Most enterprises
already have account-per-environment for other reasons.

### 7.2 One account, multiple `customerName` prefixes

```
account-shared → customerName=acme-dev,  icebergNamespace=cda
                customerName=acme-stg,   icebergNamespace=cda
                customerName=acme-prod,  icebergNamespace=cda
```

Pros: one Guidewire allow-list round (one EMR role per env, but same
account boundary).

Cons: blast radius isn't enforced at the account level — a deployer
with stack-update rights in dev can technically touch prod stacks.
Mitigate via Service Control Policies or distinct deployer IAM
identities.

### 7.3 One account, one `customerName`, multiple `icebergNamespace`

**Don't use this pattern.** Stacks share names because `customerName`
is the prefix; the second deploy overwrites the first. CloudFormation
will block this, but the failure mode is confusing. Multi-environment
in one account requires distinct `customerName` values.

### 7.4 Tag conventions for multi-environment

```bash
npx cdk deploy --all \
  --context tags='Environment=prod,CostCenter=12345,Project=cda-iceberg'
```

The `tags` knob applies tags to every taggable resource. AWS Cost
Explorer groups by tags out of the box, so per-env spend can be broken
out without account separation.

---

## 8. Lifecycle Events activation

Guidewire's CDA Lifecycle Events feature is **Early Access** as of late
2025. Treat the cron schedule as the production contract; treat
lifecycle events as a near-real-time accelerator on top.

### 8.1 Enrollment

1. Enroll in Guidewire's CDA Lifecycle Events EAP via your Guidewire
   CSM or product contact
2. Guidewire provides a **partner event source name** in the form
   `aws.partner/guidewire.com/<customer-id>/<source-name>`
3. The partner source appears in the EventBridge console under
   **Partner event sources**, status `Pending`

### 8.2 Accept the partner source

This step must be done manually in the EventBridge console.
CloudFormation cannot perform the partner-source acceptance handshake
(it requires interactive consent from a human in your account).

1. EventBridge console → Partner event sources
2. Find the row matching the source name
3. Click **Associate with event bus**
4. CDK will then create the matching `events.EventBus` referencing
   this source on next deploy

### 8.3 Deploy with lifecycle events on

```bash
npx cdk deploy --all \
  --context cdaSourceBucketArn=arn:aws:s3:::<cda-source-bucket> \
  --context scheduleEnabled=true \
  --context lifecycleEventsEnabled=true \
  --context lifecyclePartnerEventSource=aws.partner/guidewire.com/<id>/<source> \
  --context lifecycleSourceApp=cc
```

`lifecycleSourceApp` filters to one InsuranceSuite app; omit (or
empty-string) to match all.

### 8.4 Validate

Wait for CDA to emit a `streamingBatchCompleted` event (typically
~6 min of customer-side write activity), then:

```bash
aws stepfunctions list-executions \
  --state-machine-arn <ingest-sm-arn> \
  --max-results 5 \
  --query 'executions[*].[name,status,startDate]'
```

You should see executions matching the cadence of CDA emissions in
addition to the 15-min cron triggers. If only cron-named executions
appear, the partner source isn't accepted yet — return to §8.2.

### 8.5 Tuning for high event velocity

If your CDA stream is high-velocity, consider:

| Knob | Default | Tune to |
|---|---|---|
| `cronExpression` | `0/15 * * * ? *` | `0 * * * ? *` (hourly safety net only) |
| `emrIdleTimeoutMinutes` | 15 | 60 (avoids cold start on every burst) |
| `mapStateConcurrency` | 8 | 16 (parallelize per-event work harder) |

---

## 9. Monitoring and alerting

The pipeline emits all the signals needed for production monitoring.
Wire them into your existing operational practice — this section
suggests a starting point.

### 9.1 Built-in alerts

These CloudWatch / SNS signals come pre-wired to the notification topic:

| Signal | Source | Severity | Wire to |
|---|---|---|---|
| `NotifyLaunchConditionFailure` | SNS | High | Pager rotation |
| `NotifyMapFailure` | SNS | High | Pager rotation |
| `NotifyCdaReset` | SNS | **Critical** | Pager rotation + on-call lead |
| `ReconWriteFailed` alarm | CloudWatch metric filter → SNS | High | Pager rotation |
| `NotifyCompletion` | SNS | Info | Optional log channel only |
| `NotifyNoRun` | SNS | Info | Optional log channel only |

Subscribe email addresses or Lambda fan-out to PagerDuty / Opsgenie /
Slack via the topic ARN in the CFN output `NotificationTopicArn`.

**`ReconWriteFailed`** deserves a note: a recon write (Tier A/B) failing
is *non-fatal* to ingest — the Spark job still succeeds, so the row count
in `_raw`/`_merged` is correct, but `cda_recon_results` would be missing
rows for that table. The job emits a structured `RECON_WRITE_FAILED ...`
log line; a CloudWatch metric filter on the EMR log group
(`/aws/emr-serverless/<customer>-cda-iceberg`) turns that into this alarm.
Without it, a recon-write regression would be invisible (a "successful"
load with silently empty reconciliation). On alarm, grep the named table
in the EMR driver logs.

### 9.2 CloudWatch alarms (recommended additions)

These aren't deployed by default. Add them via your monitoring
infrastructure-as-code or via console after first deploy:

| Alarm | Metric | Threshold | Why |
|---|---|---|---|
| Step Functions ingest failure rate | `ExecutionsFailed` on the ingest state machine | >0 in any 1-hr window | Catch silent failures the SNS topics miss |
| EMR Serverless billed vCPU spike | `BilledVCpu` on the EMR application | >2× rolling-7-day-avg for 1 hr | Catch runaway compute |
| Lambda error rate | `Errors` on each of 3 Lambdas | >5 in any 5-min window | Catch broken IAM, broken redirects |
| DynamoDB throttles | `UserErrors` + `SystemErrors` on the cursor table | >0 | Cursor write failure means future ingests will re-read |
| SNS delivery failures | `NumberOfNotificationsFailed` | >0 | Operations losing visibility into pipeline state |

A reference CloudWatch alarm CDK construct stack is not provided —
each customer's monitoring practice differs.

### 9.3 Recommended dashboards

Build one CloudWatch dashboard per environment with these widgets:

- **State machine status** — line chart of `ExecutionsStarted`,
  `ExecutionsSucceeded`, `ExecutionsFailed`, `ExecutionsTimedOut`
  on both the ingest and recon state machines
- **EMR Serverless capacity** — line chart of `BilledVCpu`,
  `BilledMemoryGB`, `BilledDiskGB` on the application
- **Reconciliation findings** — Athena saved query rendered in
  CloudWatch, showing 24-hour rolling MISMATCH count by tier
- **DynamoDB capacity** — `ConsumedReadCapacityUnits`,
  `ConsumedWriteCapacityUnits` on the cursor table
- **Lambda durations** — average and P99 of each of the 3 functions

### 9.4 Reconciliation as monitoring

The most actionable signal is `cda_recon_results`. Schedule this query
to run every hour and alert on any non-zero result:

```sql
SELECT count(*) AS bad_rows
FROM "s3tablescatalog/<bucket>"."cda"."cda_recon_results"
WHERE run_id = (SELECT max(run_id) FROM cda_recon_results WHERE tier='B')
  AND status NOT IN ('OK', 'METRICS_PARTIAL');
```

`METRICS_PARTIAL` is benign (CDA writer race window); other
non-`OK` statuses warrant investigation per [§13.3](#133-tier-d-mismatch).

---

## 10. Performance baselines

Reference numbers from the validated synthetic test workload.

### 10.1 Per-table run times

| Table size class | Manifest record count | Cold start | Warm start (ingest) |
|---|---|---|---|
| Typecode (small) | ~100s | ~30 s | ~30 s |
| Small (millions) | 1M–10M | ~60 s | ~60–90 s |
| Medium | 10M–500M | ~90 s | ~2–4 min |
| Large | 500M–1B | ~2 min | ~5–10 min |
| Very large | >1B | ~3 min | ~10–20 min |

These are wall-clock times for **incremental** ingest of a single
fingerprint's worth of new timestamp folders. First-time bulk loads
of the same table take longer because raw + merged are being
populated from scratch.

### 10.2 Full-tier-suite bulk load

A single full bulk load of all 717 tables in the synthetic dataset
took **~4 hours** wall-clock at concurrency 8 with the validated
infrastructure profile. Real customer data with larger tables: scale
linearly with manifest record counts.

> **⚠️ This figure predates two changes and is expected to improve.** It
> was measured with the old inline-Map fan-out at `mapStateConcurrency=8`
> / `emrMaxVcpu=800`. The pipeline now uses a Distributed Map (§5.-1) and
> the defaults are `mapStateConcurrency=16` / `emrMaxVcpu=1600 vCPU`, which
> should roughly halve wall-clock. Treat ~4 h as a conservative upper
> bound until a full load is re-timed on the current configuration; update
> this number when you do (`./scripts/data_load.sh report` prints the
> authoritative per-run wall-clock).

### 10.3 Tier D recon

Each Tier D check scans both `_raw` and `_merged` for one table.

| Table size class | Time per check | All 4 checks |
|---|---|---|
| Small | ~10 s | ~40 s |
| Medium | ~30 s | ~2 min |
| Large | ~2 min | ~8 min |
| Very large | ~5 min | ~20 min |

`reconMapConcurrency` defaults to **16** (recon jobs are light — four SQL
counts, no source read or MERGE — so they run at ingest concurrency). A
full 717-table synthetic recon completes in **~1.5 h** at this setting;
the old default of 4 took ~6.5 h. Tune higher for faster runs at higher
cost.

> Recon jobs all append their findings to the **one** `cda_recon_results`
> table, so concurrent writers race on the Iceberg metadata commit. The
> recon table is created with `commit.retry.*` properties and `writeRows`
> retries on conflict (appends are commutative) — without this, a
> high-concurrency recon run fails with
> `States.ExceedToleratedFailureThreshold`. If you raise
> `reconMapConcurrency` much higher, that retry is what keeps the writes
> from colliding.

#### Point-in-time consistency under continuous ingest

CDA writes to the source bucket continuously (every ~60–90 s), so an
ingest cycle can append to `_raw` and MERGE into `_merged` **while** a
Tier D check is mid-scan. To avoid false MISMATCHes from that race, each
table's four invariants are **pinned to a consistent snapshot pair**
before they run:

- `_merged` is pinned to its current snapshot `Sm` (committed at `Tm`);
- `_raw` is pinned to the snapshot current **at-or-before `Tm`** — i.e.
  `_raw` as it looked when that MERGE committed.

Because the per-table ingest loop is strictly sequential (append raw →
MERGE), raw-as-of-`Tm` holds exactly the rows merge `Sm` reflected.
Rows CDA lands after `Tm` are excluded on **both** sides, so all four
checks reconcile to one instant regardless of concurrent ingest. This
means **you do not need to quiesce the schedule to get a clean Tier D
run** — recon and ingest can run concurrently.

Snapshot IDs are read via the Iceberg **catalog API**
(`Spark3Util.loadIcebergTable → currentSnapshot()`), not a `.snapshots`
metadata-table SQL query — the S3 Tables catalog mis-parses the metadata-
table name as an extra namespace level, which silently defeated an earlier
SQL-based attempt (it always fell back to UNPINNED). The API resolves the
table the same way the data reads do, so pinning works wherever the data
queries work. If a table has no snapshots yet, recon falls back to an
unpinned live read — still correct, just without the cross-snapshot
guarantee. The pin/no-pin decision is logged per table
(`pinned merged@<id> raw@<id>` vs `UNPINNED`).

### 10.4 Cost per run

Validated cost per typical medium-class ingest job:

```
vCPU       12 × 4/60 × $0.052624  = $0.042
memory     32 × 4/60 × $0.0057785 = $0.012
disk      180 × 4/60 × $0.000111  = $0.001
                                  ─────────
                                   ~$0.055 / job
```

Multiply by ~average jobs per day for daily compute cost. See README's
[Pricing section](../README.md#pricing) for tier-by-tier MRR.

### 10.5 Performance tuning knobs

The pipeline has several knobs that trade cost for speed. Defaults are
sized for a tri-suite mid-size carrier.

#### Warm pool (cold-start reduction)

`emrInitialDriverCount` (default 4) and `emrInitialExecutorCount`
(default 8) pre-provision EMR Serverless workers. At
`mapStateConcurrency=8`, the defaults cover the first 4 of 8
concurrent jobs without cold start. For zero-cold-start bulk loads,
set both to match concurrency (e.g. 8 drivers / 24 executors) — costs
~$5-7/hr idle billing while warm. For sparse / cost-minimizing
workloads, drop to 1 / 2.

#### Batch-metrics fetch parallelism

Tier A reconciliation reads one `batch-metrics.json` per timestamp
folder. These S3 GetObjects are parallelized (default 16 threads).
Override with the `BATCH_METRICS_PARALLELISM` env var on the EMR job
if a customer has very deep timestamp hierarchies. Rarely needed.

#### Incremental MERGE filter (advanced, OFF by default)

**This is a correctness-sensitive optimization. Validate before using
it in production.**

By default, the MERGE source re-scans the entire `cda_fingerprint`
partition of `_raw` on every run and re-windows it to find the latest
seqval per id. For a billion-row raw table, a 200-row incremental
still re-windows a billion rows.

When enabled (env var `INCREMENTAL_MERGE_FILTER=true` on the EMR job),
the MERGE source is scoped to rows with `cda_load_ts` newer than a
**merge watermark** — a per-(table, fingerprint) timestamp that
advances **only after a successful MERGE+DELETE**. This is distinct
from the ingest cursor (which advances after the raw append). The two
advance independently so that a crash between the raw append and the
MERGE leaves the watermark behind, and the next run's MERGE
re-includes the un-merged rows. No data loss.

**Why it's gated off:**
- It changes the data-correctness boundary of the MERGE.
- The default path (full re-window) is proven correct by the
  validated 717-table run; this path is not yet validated against a
  real multi-cycle incremental workload.

**How to validate before trusting it:**
1. Pick one table with multiple fingerprints in a non-prod environment.
2. Run an initial load with the flag OFF; record `_merged` row count
   and a few sample ids' seqvals.
3. Enable the flag, simulate an incremental (new timestamp folder with
   a known set of updates + one tombstone).
4. Confirm: the updated ids show new seqvals in `_merged`, the
   tombstoned id is gone, and `cda_recon_results` Tier D
   (`latest_seqval`, `tombstone_integrity`, `count_formula`) all show
   OK.
5. Crash-test: kill the job between raw append and MERGE (e.g. cancel
   the EMR job after the "appended" log line). Re-run. Confirm the
   un-merged rows still land in `_merged` (watermark didn't skip them).

Only enable in production after step 5 passes. Submit the flag on the
manual `start-job-run` first; wiring it into the orchestration state
machine is a deliberate follow-up once you trust it.

---

## 11. Security and compliance

This section gives security and compliance teams what they need to
sign off on the deployment.

### 11.1 Encryption at rest

| Resource | Encryption |
|---|---|
| Artifact + logs S3 buckets | SSE-S3 (AWS-managed AES-256) |
| S3 Tables warehouse | SSE-S3 (AWS-managed) |
| DynamoDB cursor table | AWS-managed KMS |
| Step Functions execution data | AWS-managed |
| CloudWatch Logs | AWS-managed |

To use customer-managed KMS keys (CMKs) for any of these: not
configurable via the current `cdk.json` knobs. Open an issue on this
repo for CMK support if your compliance posture requires it.

### 11.2 Encryption in transit

- All AWS service calls use HTTPS by default
- S3 bucket policies enforce `aws:SecureTransport=true` on the artifact
  and logs buckets (constructed in `runtime-stack.ts`)
- The SNS notification topic enforces TLS via a deny-non-TLS resource
  policy (constructed in `orchestration-stack.ts`)
- EMR Serverless to S3 traffic uses the S3 gateway endpoint, never
  leaves the AWS network

### 11.3 Network isolation

- The EMR Serverless application runs in private subnets only
- A dedicated security group (`ComputeSg`) restricts traffic
- Egress is via S3 gateway endpoint (S3 — including the runtime jars in
  `deps/`, referenced with `--jars`) + NAT gateway (AWS service APIs).
  The Iceberg runtime no longer requires Maven egress unless
  `useMavenPackages=true`.
- No public IPs are assigned to any pipeline resources

### 11.4 IAM least privilege

Each component has a scoped role, not a shared one:

| Component | Role | Privileges |
|---|---|---|
| EMR job | `EmrJobRole` | Read `<artifact-bucket>`, write `<logs-bucket>`, read `<cda-source-bucket>`, read+write S3 Tables warehouse, GetItem+UpdateItem on cursor table, CloudWatch Logs delivery |
| Launch-condition Lambda | `LaunchConditionRole` | Read `manifest.json` only, GetItem on cursor table |
| Recon-list Lambda | `ReconListRole` | Read `manifest.json` only |
| State machine | `StateMachineRole` | Invoke specific Lambdas, publish to specific SNS, start/get specific EMR jobs, PassRole on EmrJobRole |

No role uses `*` on `s3:*`, `iam:*`, or any other unrestricted action.

### 11.5 Audit trail

| Trail | Retention | Use |
|---|---|---|
| CloudTrail (account-wide) | Customer-configured | All AWS API calls including all CDK deploys |
| Step Functions execution history | 90 days (CloudWatch Logs) | Every state transition for every run, queryable via console |
| EMR Serverless job logs | Configurable via `logRetentionDays` | Spark driver/executor stderr/stdout |
| Lambda invocation logs | Configurable via `logRetentionDays` | Function logs including launch-condition decisions |
| `cda_recon_results` Iceberg table | Bound by snapshot retention | Permanent recon audit, queryable via Athena |
| SNS topic | N/A (delivery only) | Notification history if subscribers store messages |

For SOC 2 / ISO 27001: the combination of CloudTrail + Step Functions
history + EMR job logs + recon table provides a complete audit trail
from CDA-side write to Iceberg-side commit.

### 11.6 GDPR / right-to-be-forgotten

Iceberg's append-only model means a row in `_raw` is not naturally
deletable — but it is *expirable*. The procedure for removing a
specific id:

```sql
-- 1. Delete from merged (immediate, customer-visible removal)
DELETE FROM "s3tablescatalog/<bucket>"."cda"."<table>_merged"
WHERE id = '<gdpr-target-id>';

-- 2. Delete from raw (history removal)
DELETE FROM "s3tablescatalog/<bucket>"."cda"."<table>_raw"
WHERE id = '<gdpr-target-id>';

-- 3. Force snapshot expiration so old snapshots no longer contain the row
ALTER TABLE "s3tablescatalog/<bucket>"."cda"."<table>_raw"
EXECUTE expire_snapshots(retain_last => 1);

ALTER TABLE "s3tablescatalog/<bucket>"."cda"."<table>_merged"
EXECUTE expire_snapshots(retain_last => 1);
```

**Caveats:**
- The next ingest run **may re-add the row** if the source CDA bucket
  still contains the id. Coordinate with Guidewire to remove the id
  from the source first, or add it to `tablesToExclude` (full-table
  exclusion).
- S3 Tables managed compaction may take up to 24 hours to physically
  delete the underlying parquet files. Until then, the data is
  logically gone but not physically purged.
- This procedure is **manual**. The pipeline does not automate GDPR
  compliance.

### 11.7 Backup vs. snapshot

Iceberg snapshots are **history**, not **backup**. They protect against
accidental DELETE / UPDATE within the configured retention window
(default 5 days). They do **not** protect against:

- Account compromise / ransomware on the AWS account itself
- S3 Tables service outage in the region
- Accidental table-bucket deletion

For ransomware-resilient backups, replicate the underlying parquet
files to a separate AWS account or a non-S3 store (e.g., AWS Backup
with a vault in a different account). This is not configured by the
pipeline; it requires customer-side backup infrastructure.

### 11.8 Compliance posture

| Framework | Pipeline support |
|---|---|
| SOC 2 Type II | Audit trail covers it; customer's overall AWS posture must qualify |
| HIPAA | All resources are HIPAA-eligible (S3, DynamoDB, EMR Serverless, Step Functions, Lambda); customer must execute BAA with AWS |
| PCI DSS | All resources support PCI; customer's data classification dictates whether CDA-flowed data is in scope |
| GDPR | Manual procedures documented in §11.6; no automation |

The pipeline does not certify any compliance posture by itself. The
customer's overall AWS account configuration determines compliance.

---

## 12. Upgrade procedure

When a new jar version is published to this repo, follow this
procedure to roll out the new version safely.

### 12.1 Pre-upgrade checklist

- [ ] Read the release notes for the new version
- [ ] Identify any new context flags introduced; decide whether to
      override or accept defaults
- [ ] Confirm no active Step Functions executions are running
      (`aws stepfunctions list-executions --status-filter RUNNING`)
- [ ] Note the current jar version's S3 object-version-id for rollback
- [ ] Note the current CDK stack template (CFN console → stack →
      Template tab)

### 12.2 Canary procedure (recommended)

1. **Pause the schedule:**
   ```bash
   npx cdk deploy --all --context scheduleEnabled=false
   ```

2. **Upload the new jar with a versioned name:**
   ```bash
   aws s3 cp build/libs/cda-iceberg-client-1.0.jar \
     s3://<artifact-bucket>/jars/cda-iceberg-client-1.0-canary.jar
   ```

3. **Run the new jar manually against one table** by editing the
   spark-submit `entryPoint` to point at the canary jar. Compare
   `cda_recon_results` to a reference table — Tier A and Tier B
   should match the previous run's deltas exactly (zero drift).

4. **If canary passes**, promote:
   ```bash
   aws s3 cp s3://<artifact-bucket>/jars/cda-iceberg-client-1.0-canary.jar \
     s3://<artifact-bucket>/jars/cda-iceberg-client-1.0.jar
   ```

5. **Re-arm the schedule** after watching one production run succeed:
   ```bash
   npx cdk deploy --all --context scheduleEnabled=true
   ```

### 12.3 Rollback procedure

If the new version misbehaves:

1. **Pause the schedule** (same as 12.2 step 1)

2. **Restore the previous jar version:**
   ```bash
   # Find previous version-id
   aws s3api list-object-versions \
     --bucket <artifact-bucket> \
     --prefix jars/cda-iceberg-client-1.0.jar
   # Promote previous version to current
   aws s3 cp s3://<artifact-bucket>/jars/cda-iceberg-client-1.0.jar \
            s3://<artifact-bucket>/jars/cda-iceberg-client-1.0.jar \
            --copy-source-version-id <previous-version-id>
   ```

3. **Roll back CDK stack** if a CDK change was part of the upgrade:
   ```bash
   aws cloudformation rollback-stack --stack-name cda-iceberg-orchestration
   ```

4. **Re-arm the schedule** (same as 12.2 step 5)

The artifact bucket has versioning enabled by default, so jar rollbacks
are always available within the bucket's retention window.

### 12.4 Compatibility guarantees

The pipeline maintains backward compatibility within a major version:

- Schema evolution to `_raw` and `_merged` is forward-compatible (new
  versions can read old schemas)
- DynamoDB cursor format is forward-compatible (new versions read old
  cursors)
- `cda_recon_results` schema additions are non-breaking column adds
- CDK context flag deprecations get a major-version-bump and a
  migration note in release notes

A major version bump (1.x → 2.0) may include:
- Required schema changes (re-deploy with rebuild of `_recon_results`)
- Required context flag renames
- DynamoDB schema changes (one-time migration step)

Major version bumps will document the migration in release notes.

### 12.5 CI/CD pipeline patterns

Most enterprises run CDK through a CI/CD pipeline rather than from a
deployer's laptop. Below are reference patterns for two common
toolchains. The pipeline-side configuration in your repository will
need to be authored by your team — these patterns are starting points,
not turn-key configs.

#### Common pattern (any toolchain)

A safe production pipeline has at least these stages:

```
PR → build/test → deploy:dev → manual gate → deploy:stg → manual gate → deploy:prod
        │              │                          │                          │
        ▼              ▼                          ▼                          ▼
     gradle test   smoke-test         smoke-test + 24-hr        canary jar in §12.2
     + tsc        single-table       schedule-armed soak
```

Per environment, the deploy step should:

1. Build the jar (`./gradlew shadowJar`) and the CDK assets
   (`npx cdk synth`)
2. Diff against the deployed stack (`npx cdk diff --all`)
3. Apply only if the diff is reviewed and accepted (manual gate, or
   automatic for non-prod)
4. Upload the jar to the artifact bucket
5. Run a smoke-test execution against one table
6. Verify `cda_recon_results` shows OK for the smoke-test run
7. Re-arm the schedule if it was paused for the deploy

#### GitHub Actions reference

```yaml
# .github/workflows/deploy.yml (illustrative — adapt to your needs)
name: Deploy cda-iceberg-client
on:
  push: { branches: [main] }
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: corretto, java-version: '21' }
      - uses: actions/setup-node@v4
        with: { node-version: '20' }
      - run: ./gradlew test shadowJar
      - run: cd cdk && npm install && npx tsc --noEmit
      - uses: actions/upload-artifact@v4
        with:
          name: jar
          path: build/libs/cda-iceberg-client-1.0.jar

  deploy-dev:
    needs: build
    runs-on: ubuntu-latest
    environment: dev    # protects with required reviewers in repo settings
    permissions: { id-token: write, contents: read }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/download-artifact@v4
        with: { name: jar, path: build/libs }
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::<dev-account>:role/<deployer-role>
          aws-region: us-east-1
      - run: cd cdk && npm install && npx cdk diff --all
      - run: cd cdk && npx cdk deploy --all --require-approval never \
               --context customerName=acme \
               --context cdaSourceBucketArn=$DEV_CDA_SOURCE_ARN \
               --context tags='Environment=dev,CostCenter=12345'
      - run: aws s3 cp build/libs/cda-iceberg-client-1.0.jar \
               s3://<dev-artifact-bucket>/jars/

  deploy-prod:
    needs: deploy-dev
    runs-on: ubuntu-latest
    environment: prod   # gated by required reviewers
    permissions: { id-token: write, contents: read }
    # ... mirror of deploy-dev with prod role and prod context values
```

Key points:
- **OIDC role assumption** (no long-lived AWS credentials in GitHub
  secrets). Configure a trust relationship from your AWS account to
  GitHub's OIDC provider once.
- **Environment protection rules** in GitHub repo settings provide the
  manual gate between stages without needing a separate approval tool.
- **Artifact passing** between jobs avoids rebuilding the jar per stage.
- **Per-environment context values** stored as GitHub Actions
  `vars`/`secrets`, not committed to the repo.

#### AWS CodePipeline / CDK Pipelines reference

CDK Pipelines is purpose-built for this and worth using if your
organization is already on AWS-native tooling:

```typescript
// pipeline-stack.ts (illustrative — your team authors this in your repo)
import { CodePipeline, CodePipelineSource, ShellStep } from 'aws-cdk-lib/pipelines';

const pipeline = new CodePipeline(this, 'CdaIcebergPipeline', {
  synth: new ShellStep('Synth', {
    input: CodePipelineSource.gitHub('your-org/cda-iceberg-client', 'main'),
    commands: [
      'JAVA_HOME=/path/to/jdk21 ./gradlew test shadowJar',
      'cd cdk && npm install && npx cdk synth',
    ],
    primaryOutputDirectory: 'cdk/cdk.out',
  }),
});

pipeline.addStage(new CdaIcebergStage(this, 'dev', { account: '<dev>', ... }));
pipeline.addStage(new CdaIcebergStage(this, 'prod', { account: '<prod>', ... }), {
  pre: [new ManualApprovalStep('PromoteToProd')],
  post: [new ShellStep('SmokeTest', {
    commands: ['./scripts/smoke-test.sh'],
  })],
});
```

Key points:
- **Self-mutating** — the pipeline updates itself when its own
  definition changes
- **Cross-account deploys** via CodePipeline cross-account roles
  (configured once per target account)
- **Quality gates** as `ShellStep` post-actions running smoke tests
  and recon verification

#### What to NOT automate

- **First deploy** (Phase 4.1 in §4) — needs out-of-band Guidewire
  IAM coordination. Run from a deployer's session for the first deploy
  in each new environment, then hand off to the pipeline.
- **Cross-account IAM updates on Guidewire's side** — that's a
  manual ticket to Guidewire, not pipeline-able.
- **Schedule arming** — should be manual the first time it goes
  on in production (after the canary in §12.2 succeeds).

#### Branch protection recommendations

For the repo containing your CDK code:

- Require PR review before merge to `main`
- Require status check `gradle test` before merge
- Require status check `cdk diff` to surface any infra changes for
  reviewer awareness
- Block force-push and direct push to `main`
- Optional: require signed commits if your security posture mandates it

---

## 13. Operational runbooks

Each subsection covers a specific failure mode the pipeline detects.
Each runbook: how to recognize it, what's affected, how to respond.

### 13.1 CDA reset detected

**Symptom:** SNS notification subject `<customer> CDA Iceberg — CDA
RESET DETECTED — ingest paused`. Step Functions execution failed with
`error=CdaReset`.

**What happened:** the launch-condition Lambda compared manifest state
against DynamoDB cursors and found a regression — either:
- `kind=hwm_regression` — manifest's `lastSuccessfulWriteTimestamp`
  is older than the bookmark. CDA-side re-deploy or replay.
- `kind=fingerprint_pruned` — a fingerprint with a cursor is no
  longer in `schemaHistory`. CDA recreated the table.

**Affected:** ingest paused for *all* tables (Lambda returns
`CDA_RESET` if any table regresses). Tier D recon is unaffected.

**Response:**

1. Read the SNS message; note which tables and which `kind` per the
   `resets[]` payload
2. Confirm with Guidewire what happened on their side — bulk load
   re-run, manual replay, connector failure recovery
3. Decide per affected table:
   - **Drop-and-rebuild**: drop `<table>_raw` and `<table>_merged`
     from S3 Tables, delete the DDB row for that table, re-deploy.
     Next ingest re-runs the table from scratch.
   - **Wait-and-watch**: if Guidewire is mid-recovery, wait for them
     to finish, then re-arm the schedule. Cursors will mostly still
     match once their replay catches up.
4. Re-trigger the state machine manually after taking action

```bash
# Drop one affected table
aws s3tables delete-table --table-bucket-arn <arn> \
  --namespace cda --name <table>_raw
aws s3tables delete-table --table-bucket-arn <arn> \
  --namespace cda --name <table>_merged
aws dynamodb delete-item --table-name <state-table> \
  --key '{"tableName":{"S":"<table>"}}'
```

### 13.2 Schema-incompatible failure

**Symptom:** EMR job for one table fails with
`IllegalStateException: Incompatible type change for column ...`

**What happened:** the schema-evolution planner detected a type change
it can't safely apply — e.g. `int` → `string`, or a decimal scale change.

**Affected:** one table. Other tables continue.

**Response:**

1. Read the job log (S3 logs bucket →
   `applications/<app>/jobs/<run>/SPARK_DRIVER/stderr.gz`)
2. Identify the offending column from the stack trace
3. Decide:
   - **Real schema change**: Guidewire's app has legitimately changed
     a column type. Drop+rebuild the table (§13.1 response steps
     for one table). The new schema becomes the baseline.
   - **Bug in the planner**: open an issue. Type compatibility rules
     are in `IcebergIngest.canWiden` / `canCast` — extend if a new
     safe pair has been missed.
4. Re-trigger ingest after rebuild

### 13.3 Tier D mismatch

**Symptom:** `cda_recon_results` rows with `tier='D'` and `status='MISMATCH'`.

**What it means depends on the check:**

| Check | MISMATCH meaning |
|---|---|
| `tombstone_integrity` | Tombstoned ids still in merged. The DELETE didn't fire or arrived out of order. |
| `latest_seqval` | Merged row's seqval ≠ lpad-32 max from raw. The MERGE picked the wrong winner. |
| `no_orphans` | Ids in merged not in raw. Data corruption or out-of-band insert. |
| `count_formula` | `count(merged) ≠ count(distinct id with latest op ∈ {0,2,4})`. MERGE missed ids or stale rows persist. |

**Important:** on synthetic test data with random `gwcbi___operation`
values, Tier D reports MISMATCH by design. On real CDA data all four
should be OK. If you're testing with non-Guidewire data, expect
mismatches.

**Response on real CDA:**

1. Note the `actual` column (= violation count); large violations are
   higher priority
2. Sample affected ids — Tier D's `details` field has a small sample
   for investigation
3. Run a manual recon to see if it persists:
   ```bash
   aws stepfunctions start-execution \
     --state-machine-arn <recon-sm-arn> \
     --name manual-recon
   ```
4. If the mismatch persists, drop+rebuild the affected table

### 13.4 EMR Serverless capacity exhaustion

**Symptom:** EMR job fails with `Job failed as application has exceeded
maximumCapacity settings`.

**What happened:** the cumulative concurrent vCPU + memory + disk
across all running jobs exceeded the application's `maximumCapacity`.

**Affected:** one or more in-flight jobs; subsequent jobs queue.

**Response:**

1. Check current settings: `aws emr-serverless get-application
   --application-id <id>`
2. Increase via re-deploy:
   ```bash
   npx cdk deploy cda-iceberg-emr \
     --context emrMaxVcpu='1600 vCPU' \
     --context emrMaxMemory='6400 GB' \
     --context emrMaxDisk='24000 GB'
   ```
3. Check AWS service quota: `aws service-quotas get-service-quota
   --service-code emr-serverless --quota-code L-...`. If the account's
   quota is below the new `emrMaxVcpu`, raise it via the AWS Service
   Quotas console.
4. To re-deploy a stopped EMR Serverless app, stop it first if
   changing maximumCapacity. CDK handles this:
   ```bash
   aws emr-serverless stop-application --application-id <id>
   # Wait until state=STOPPED
   npx cdk deploy cda-iceberg-emr ...
   ```

### 13.5 Disk full during MERGE

**Symptom:** EMR job fails with `No space left on device:
/tmp/blockmgr-...`

**What happened:** Spark shuffle spilled more than the executor's
local disk. Common on the largest tables during MERGE INTO.

**Affected:** one table, typically a multi-billion-row table.

**Response:**

1. Confirm the table is genuinely large by checking
   `manifest.totalProcessedRecordsCount`
2. Tune the size-class threshold so this table rides the `large`
   config:
   ```bash
   npx cdk deploy cda-iceberg-orchestration \
     --context sizeThresholdLarge='500000000'   # default 1B; lower it
   ```
3. The `large` config provides 400 GB executor disk + 1000 shuffle
   partitions, sufficient for ~10B-row tables on standard worker class
4. For tables larger than 10B rows, fall back to manually running
   just that table via `start-job-run` with bumped
   `spark.emr-serverless.executor.disk` and
   `spark.sql.shuffle.partitions`

### 13.6 Lambda timeout

**Symptom:** `CheckCDAChanges` Lambda errors with
`Task timed out after 120.00 seconds`.

**What happened:** manifest is huge (1000+ tables) or DDB read
latency is high.

**Affected:** that one Step Functions execution; pipeline pauses
until the next trigger.

**Response:**

1. Confirm manifest size: `aws s3api head-object --bucket
   <source-bucket> --key manifest.json --query 'ContentLength'`
2. If manifest is genuinely huge (>5 MB): the Lambda timeout
   (`Duration.minutes(2)`) is currently hardcoded in
   `cdk/lib/orchestration-stack.ts`. The supported workaround is to
   fork the CDK source and raise it; no context knob exists for this
   yet (file an issue if your manifest is consistently large enough
   to need this).
3. If DDB latency is the cause (CloudWatch metric
   `SuccessfulRequestLatency`): move from on-demand to provisioned
   capacity. Rare; on-demand handles 700+ reads in <10 seconds in
   typical workloads.

### 13.7 SNS delivery failures

**Symptom:** subscribers stop receiving completion / failure emails.

**Response:**

1. Check SNS topic confirmed-subscriber list:
   ```bash
   aws sns list-subscriptions-by-topic --topic-arn <topic-arn>
   ```
2. Re-confirm any subscriptions in `PendingConfirmation` state by
   asking the subscriber to click the email link
3. Check CloudWatch metrics on the topic for
   `NumberOfNotificationsFailed`
4. Email provider blocks: corporate spam filter may need to whitelist
   `no-reply@sns.amazonaws.com`

### 13.8 Iceberg snapshot retention exhausted

**Symptom:** unable to time-travel-query old data:
`SELECT * FROM <table> FOR TIMESTAMP AS OF '<old-date>'` returns
empty.

**What happened:** S3 Tables managed snapshot expiration deleted
snapshots older than `snapshotRetentionDays` (default 5).

**Response:**

1. If longer history is required, re-deploy with higher retention:
   ```bash
   npx cdk deploy cda-iceberg-warehouse \
     --context snapshotRetentionDays=30
   ```
2. The change applies to **future** snapshots only. Past data is gone.
3. Storage cost grows linearly with retention window. Communicate
   this trade-off to stakeholders before increasing.

### 13.9 Bulk-load taking longer than expected

**Symptom:** stakeholders asking why their tables aren't loaded yet.

**Diagnostic:**

1. Manifest progress: how many table entries does the manifest have
   vs. the customer's expected total?
   `cat manifest.json | jq 'keys | length'`
2. Per-table state: which tables ARE loaded, which aren't?
   ```sql
   SELECT distinct table_name FROM cda_recon_results ORDER BY 1;
   ```
3. Is CDA actually emitting? Check the Guidewire-side Datadog
   dashboard if available, or contact Guidewire support.

**Most common cause:** CDA is still in bulk-load mode. Per Guidewire's
benchmarks: 30–48 hours per TB. Communicate timing expectations;
consider a Guidewire CDA tier upgrade if the timeline is unacceptable.

### 13.10 Cost spike investigation

**Symptom:** AWS Cost Explorer shows EMR Serverless spend > 2× normal.

**Diagnostic:**

1. Check Step Functions executions per hour:
   ```bash
   aws stepfunctions list-executions \
     --state-machine-arn <ingest-sm-arn> \
     --max-results 100 \
     --query 'executions[?status==`SUCCEEDED`].[name,startDate]'
   ```
2. If executions are running per cron + lifecycle but most are
   no-ops, investigate why. Most common: `cda_recon_results` shows
   high MISMATCH rate, meaning real ingest happens every cycle.
3. Check if Tier D recon was inadvertently armed at high cadence —
   check `aws events describe-rule --name <recon-rule-name>` for the
   schedule expression.

**Response:**

1. If recon cadence is the cause, lower it:
   ```bash
   npx cdk deploy --context reconCronExpression='0 2 ? * SUN *'
   ```
   (Sunday-only.)
2. If ingest cost is the cause and lifecycle events are firing too
   frequently for the data volume, drop cron back to hourly:
   ```bash
   npx cdk deploy --context cronExpression='0 * * * ? *'
   ```
3. If a single table is consuming most compute, check which one via
   `aws emr-serverless list-job-runs --query 'jobRuns[?totalExecutionDurationSeconds > 600]'`
   and consider lowering its size-class threshold to ride the `large`
   config (more efficient large-MERGE).

---

## 14. Rollback and DR

### 14.1 Roll back a CDK deploy

CDK deploys are CloudFormation under the hood, which means standard
CFN rollback semantics apply:

```bash
# View pending change set without applying
npx cdk diff --all

# Roll back to the previous template
aws cloudformation rollback-stack --stack-name <stack-name>
```

What rolls back cleanly:
- Lambda code changes
- IAM policy changes
- EventBridge rule changes
- Step Functions definition changes
- EMR application config (with caveats — see [§13.4](#134-emr-serverless-capacity-exhaustion))

What **doesn't** roll back:
- S3 Tables data (snapshots are bound to the bucket, not the CFN stack)
- DynamoDB cursor table contents
- Iceberg `_merged` table state from a botched MERGE

If the issue is a bad jar, roll back the **jar in S3**, not the CDK
stack. See §12.3.

### 14.2 Rebuild a single table

If a table's `_raw` or `_merged` is corrupted (rare; usually only if a
bug landed and bad data was ingested):

```bash
# Drop both Iceberg tables
aws s3tables delete-table --table-bucket-arn <arn> \
  --namespace cda --name <table>_raw
aws s3tables delete-table --table-bucket-arn <arn> \
  --namespace cda --name <table>_merged

# Clear the bookmark so the next ingest re-reads from scratch
aws dynamodb delete-item --table-name <state-table> \
  --key '{"tableName":{"S":"<table>"}}'

# Trigger ingest manually
aws stepfunctions start-execution \
  --state-machine-arn <ingest-sm-arn> \
  --name rebuild-<table>-$(date +%s)
```

The state machine processes only this table because all other tables
have current bookmarks; the launch-condition Lambda returns `START`
with just `<table>`.

Time depends on table size. See [§10.1](#101-per-table-run-times).

### 14.3 Full rebuild of all tables

When this is appropriate:
- After resolving a CDA-side data quality issue
- Migrating to a new `customerName` or `icebergNamespace`
- Disaster recovery from a bad pipeline release that ingested bad
  data for many tables

```bash
# 1. Pause the schedule
npx cdk deploy cda-iceberg-orchestration \
  --context scheduleEnabled=false \
  --context reconScheduleEnabled=false

# 2. Drop all tables in the namespace
aws s3tables list-tables --table-bucket-arn <arn> --namespace cda \
  --query 'tables[].name' --output text | tr '\t' '\n' | while read -r t; do
  [ -n "$t" ] && aws s3tables delete-table --table-bucket-arn <arn> \
    --namespace cda --name "$t"
done

# 3. Truncate the DDB cursor table (preserves capacity / KMS / PITR config)
aws dynamodb scan --table-name <state-table> \
  --projection-expression 'tableName' --output json | \
  jq -r '.Items[] | .tableName.S' | while read -r t; do
  aws dynamodb delete-item --table-name <state-table> \
    --key "{\"tableName\":{\"S\":\"$t\"}}"
done

# 4. Re-arm and trigger
npx cdk deploy cda-iceberg-orchestration \
  --context scheduleEnabled=true
aws stepfunctions start-execution --state-machine-arn <ingest-sm-arn> \
  --name full-rebuild-$(date +%s)
```

Time is dominated by re-ingesting from CDA. See [§10.2](#102-full-tier-suite-bulk-load).

### 14.4 Disaster: lost the entire AWS account / region

CDK is the source of truth. To rebuild from scratch in a new
account/region:

1. Deploy the same CDK code with the same context values into the
   new account/region (5–10 min)
2. Coordinate with Guidewire on a new bucket policy entry for the
   new `EmrJobRoleArn`
3. Re-upload the jar to the new artifact bucket
4. Deploy orchestration with `scheduleEnabled=false` and trigger one
   manual run to rebuild from CDA's current manifest

**Recovery time objective (RTO):** ~24-48 hours, dominated by the
Guidewire allow-list round-trip. The pipeline-side build is hours.

**Recovery point objective (RPO):** zero data loss as long as the CDA
source bucket is intact. The pipeline is downstream of CDA; CDA is
the authoritative copy.

### 14.5 Customer-initiated decommission

When shutting down a deployment:

```bash
# 1. Pause everything
npx cdk deploy cda-iceberg-orchestration \
  --context scheduleEnabled=false \
  --context reconScheduleEnabled=false

# 2. (Optional) export Iceberg data to plain parquet for archival
#    (data export is out of scope for this guide)

# 3. Drop CDK stacks (in dependency order)
npx cdk destroy cda-iceberg-orchestration \
  cda-iceberg-runtime cda-iceberg-emr \
  cda-iceberg-warehouse cda-iceberg-network
```

**What survives `cdk destroy`** (intentional):
- Artifact + logs S3 buckets (`RemovalPolicy.RETAIN`)
- DynamoDB cursor table (`RemovalPolicy.RETAIN`)
- S3 Tables warehouse (`RemovalPolicy.RETAIN`)

**To fully clean up:**

```bash
aws s3 rb s3://<artifact-bucket> --force
aws s3 rb s3://<logs-bucket> --force
aws dynamodb delete-table --table-name <state-table>
aws s3tables delete-table-bucket --table-bucket-arn <arn>
```

This is intentionally a manual step. The pipeline preserves data by
default to protect against accidental destruction.

---

## 15. Automation policy

This pipeline ships with deliberate seams between automated and manual
steps. This section explains what's in each bucket and **why**, so that
customers building their own automation on top understand the
boundaries. Crossing some of these seams introduces real risk.

### 15.1 What's automated by this repo

| Step | Where |
|---|---|
| Network, IAM, EMR Serverless, S3 Tables, Lambdas, Step Functions | CDK (5 base stacks) |
| Per-fingerprint cursor management | Spark job (`CursorStore`) |
| Bidirectional schema evolution | Spark job (`evolveAndAlign`) |
| 4-tier reconciliation against batch-metrics | Spark job + Iceberg recon table |
| CDA reset detection + alert | Launch-condition Lambda + state machine `CDA_RESET` branch |
| Glue federation + Lake Formation grants | Optional `AnalyticsStack` (§15.2) |
| Athena saved queries | Optional `AnalyticsStack` |
| CloudWatch dashboard + alarms | Optional `AnalyticsStack` |
| Single-table smoke test | `scripts/smoke-test.sh` |
| Rebuild one table from CDA | `scripts/rebuild-table.sh` |
| Full-load runbook + timing (centralized config) | `scripts/data_load.sh` — see [`data-load.md`](data-load.md) |

### 15.2 The optional `AnalyticsStack`

Set `enableAnalyticsStack=true` to deploy a 6th CDK stack that
automates the analyst-facing setup. This is an **opt-in** stack, not
included in the default `cdk deploy --all` flow.

> Detailed reference for AnalyticsStack — what it provisions, pre-flight,
> deploy, verify, troubleshoot, decommission — is in
> [`docs/ANALYTICS.md`](ANALYTICS.md). The summary below is enough to
> decide whether to deploy it.

**When to deploy AnalyticsStack:**

- After the pipeline is validated end-to-end (Phase 4.6 complete)
- After at least one ingest run has populated `cda_recon_results`
- When you have a list of analyst IAM role ARNs to grant query access to

**When NOT to deploy AnalyticsStack:**

- Before the first successful ingest — the dashboards will be empty
  and confusing
- If your account already has a Glue `s3tablescatalog` from another
  tenant (the stack tolerates this but warn the team to avoid
  conflicting catalog updates)
- If your customer's analyst access is governed by a different IAM
  framework (Okta + permission sets, etc.) — handle grants there
  instead of via Lake Formation

**Deploy:**

```bash
npx cdk deploy --all \
  --context cdaSourceBucketArn=arn:aws:s3:::<cda-source-bucket> \
  --context enableAnalyticsStack=true \
  --context analystRoleArns=arn:aws:iam::<account>:role/<analyst-role-1>,arn:aws:iam::<account>:role/<analyst-role-2> \
  --context athenaWorkgroup=primary
```

What gets created:

| Resource | Purpose |
|---|---|
| Glue `s3tablescatalog` (parent + per-bucket child) | Federates S3 Tables into Glue so Athena can query |
| Lake Formation `SELECT + DESCRIBE` grants on `cda` namespace + table-wildcard | Per analyst role ARN; not data-lake-admin |
| 4 Athena named queries (recon failures, table coverage, drop-rate trend, Tier D violations) | Pre-populated in the workgroup so analysts don't paste from docs |
| CloudWatch dashboard `<customer>-cda-iceberg-ops` | State machine, EMR, recon, DDB widgets |
| 3 CloudWatch alarms (ingest failures, EMR vCPU spike, DDB throttles) wired to existing SNS topic | Out-of-the-box paging signals |

What this stack **deliberately does not do**:

- **Does not make analysts Lake Formation admins.** The
  table-wildcard `SELECT + DESCRIBE` grant is enough for query access;
  admin privilege would let analysts grant permissions to anyone.
- **Does not subscribe operators to the SNS topic.** AWS requires
  email confirmation; we can't fake it. Subscribe via console or CLI
  separately.
- **Does not configure consumer-side tools (Snowflake, Trino, etc.).**
  Each customer's tooling is different; document the federation
  catalog ID and let them configure their tools.

### 15.3 What's deliberately NOT automated

These steps look automatable but introduce real risk if they are.

#### Cross-account IAM with Guidewire (§3)

**Why manual:** Guidewire owns the source bucket and has their own
ticketing, security review, and SLA. Automating the request is
technically possible but bypasses their human approval, which is
their compliance contract with you. The lead time for the manual
request is days, not hours; you can't shorten that with automation.

**What we do:** §1.1 documents the lead time honestly so you plan for
it. §3 walks through the exact bucket policy to send Guidewire.

#### Partner event source acceptance (§8.2)

**Why manual:** EventBridge requires a human consent click to associate
a partner source with a custom event bus. This is by AWS design — it
prevents an attacker who compromises CDK from silently subscribing to
a partner's event stream.

**What we do:** §8.2 documents the one-time UI step and confirms CDK
will handle the bus creation on the *next* deploy after acceptance.

#### First production schedule arming (§4.6)

**Why manual:** the first time `scheduleEnabled=true` is deployed, you
need a human to verify the smoke test ran clean, recon shows OK, and
the SNS topic has confirmed subscribers. Automating this would let a
misconfigured deploy start ingesting bad data into production.

**What we do:** ship `scheduleEnabled=false` as the default. CI/CD
pipelines (§12.5) include a manual gate before the prod deploy step.

#### CDA reset response (§13.1)

**Why manual:** when the launch-condition Lambda detects a regression,
the operator's options are "drop and rebuild" vs "wait for Guidewire's
recovery to catch up." The right answer depends on how deep the
regression is, which Guidewire can tell you. Auto-rebuilding could
discard data Guidewire is in the middle of replaying.

**What we do:** the state machine pauses ingest with `error=CdaReset`
and fires a critical SNS alert. Human inspects, decides, runs
`scripts/rebuild-table.sh` per table.

#### GDPR right-to-be-forgotten (§11.6)

**Why manual:** a misconfigured automation that takes a list of "ids
to forget" and deletes them across `_raw` + `_merged` is a foot-gun.
A typo in the input list could delete the wrong customer.

**What we do:** §11.6 documents the procedure as four explicit SQL
statements with human-readable values. Operators run them with full
context.

#### Schedule change for high-velocity events (§8.5)

**Why manual:** these are tuning knobs based on observed behavior.
Auto-tuning would react to noise; manual tuning lets the operator
correlate behavior with their CDA release cadence.

**What we do:** document the knobs and their default values; let the
operator change them based on observation.

#### AWS service quota requests

**Why manual:** quota increases need business justification, and AWS
support takes 1-3 days to process them. Filing them programmatically
doesn't shorten that. Worse, automated retries can flag the account.

**What we do:** §1.2 calls out the quotas to check; you file requests
through your AWS account team's normal channel.

#### CFN stack rollback (§14.1)

**Why manual:** rolling back is the right call sometimes (broken jar)
and the wrong call other times (broken CDA bucket policy that needs a
forward fix). Auto-rollback on alarm would mask real problems.

**What we do:** document `aws cloudformation rollback-stack` as a
deliberate operator action; encourage canary releases (§12.2) so
rollbacks are rare.

### 15.4 What we wish we could automate (and why we don't)

A few things look automatable but are blocked by upstream limitations.
Listing them so you understand the gap rather than assuming we forgot.

| Want | Blocker | Workaround |
|---|---|---|
| Customer-managed KMS keys (CMKs) on every resource | Each AWS service supports CMK differently; not a clean knob | Document SSE-S3 / AWS-managed keys as defaults; open an issue if your compliance posture requires CMKs |
| Auto-detect partner event source name from the customer's account | EventBridge API doesn't list pending invitations programmatically | Manual UI step in §8.2 |
| Automatic block-list updates on the source bucket | Guidewire-side configuration; not in our account | Document at §1.1 and recommend coordinating with Guidewire CSM |
| Cross-region DR replica of S3 Tables | S3 Tables doesn't have native CRR (late 2025) | Document in §11.7 the gap and the customer-side backup pattern |
| Self-healing on Tier D MISMATCH | Each MISMATCH type has different right answer | Document in §13.3; rebuild via script when warranted |

### 15.5 Building your own automation on top

If you want to automate further than this guide goes, two principles
help avoid foot-guns:

1. **Don't automate steps where the pipeline already pages an
   operator.** The pipeline pauses ingest and fires SNS for a reason.
   Automation should *augment* the operator's response, not replace
   the operator entirely. Acceptable: a Lambda subscribed to the SNS
   topic that creates a JIRA ticket. Unacceptable: a Lambda that
   auto-runs `rebuild-table.sh` on `CdaReset`.

2. **Keep your automation in your own repo, not this one.** This repo
   is the pipeline; your repo is your operations layer. PRs to this
   repo that add customer-specific automation will likely be rejected
   in favor of customer-side tooling.

If you build something and find a real gap in the pipeline (rather
than a customer-specific need), open a GitHub issue. We'll evaluate
adding it to the next version.

---

## Getting help

- **Architecture / design questions**: open a GitHub issue on this repo
- **CDA-side issues** (manifest, bulk load, lifecycle events):
  Guidewire support
- **AWS-side issues** (S3 Tables, EMR Serverless, Lake Formation):
  AWS Support
- **Reconciliation findings**: read `cda_recon_results` with the
  queries in [§5.3](#53-useful-queries-during-bulk-load) and
  [§9.4](#94-reconciliation-as-monitoring) plus the README's
  [Reconciliation section](../README.md#reconciliation)

The README's Reconciliation section documents the four tiers in
detail — that's the operational signal to rely on, not log-reading.
