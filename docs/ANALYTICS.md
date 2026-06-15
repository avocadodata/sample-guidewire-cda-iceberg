# cda-iceberg-client — Analytics Stack

This guide covers the optional **AnalyticsStack** — the 6th CDK stack
that automates analyst-facing setup. If you're new to the pipeline,
start with the [README](../README.md) and [DEPLOYMENT.md](DEPLOYMENT.md)
first; this document assumes the base 5 stacks are already deployed
and at least one ingest run has populated `cda_recon_results`.

For the policy framing of "why this is opt-in" see
[`DEPLOYMENT.md` §15](DEPLOYMENT.md#15-automation-policy).

---

## Table of contents

1. [What it provisions](#1-what-it-provisions)
2. [Pre-flight checklist](#2-pre-flight-checklist)
3. [Deploy and verify](#3-deploy-and-verify)
4. [Onboarding additional analysts](#4-onboarding-additional-analysts)
5. [Customizing dashboard and alarms](#5-customizing-dashboard-and-alarms)
6. [Troubleshooting](#6-troubleshooting)
7. [Decommissioning](#7-decommissioning)
8. [What this stack deliberately doesn't do](#8-what-this-stack-deliberately-doesnt-do)

---

## 1. What it provisions

When you set `enableAnalyticsStack=true` and deploy, the stack provisions
exactly five things in your AWS account.

### 1.1 Glue federated catalog

A two-level federation that lets Athena query S3 Tables data through
the path `"s3tablescatalog/<bucket>"."<namespace>"."<table>"`.

- **Parent catalog** named `s3tablescatalog` (account-singleton). The
  stack handles the case where it already exists from another tenant.
- **Child catalog** named `<your-bucket-name>` referencing your
  specific S3 Tables bucket ARN.

The Lambda-backed custom resource that creates these uses **try-create-or-skip**
semantics — if either catalog already exists, the deploy succeeds and
moves on. Both are intentionally retained on `cdk destroy` (see
[§7](#7-decommissioning)).

### 1.2 Lake Formation grants

For each ARN in `analystRoleArns` (comma-separated context flag), the
stack issues two Lake Formation grants:

- `DESCRIBE` on the database (Iceberg namespace, default `cda`)
- `SELECT + DESCRIBE` on the table-wildcard within that database

This means new tables added to the namespace **after** the grant are
covered automatically — no need to re-run grants when the pipeline
adds tables. Analyst roles are **not** made data-lake admins; they
can read but not grant permissions to others.

### 1.3 Athena saved queries (named queries)

Four pre-built queries are loaded into the Athena workgroup
(default `primary`, override via `athenaWorkgroup` context):

| Saved query name | What it answers |
|---|---|
| `<customer>-cda-iceberg-recon-failures` | All recon rows in the most recent run with non-OK status |
| `<customer>-cda-iceberg-table-coverage` | Tables seen so far + last recon timestamp per table |
| `<customer>-cda-iceberg-drop-rate-trend` | CDA-side drop rate per table per day (Tier C) |
| `<customer>-cda-iceberg-tier-d-violations` | All Tier D semantic-invariant violations, most recent first |

Analysts find these in the Athena console under **Saved queries** and
run them with one click instead of pasting SQL from documentation.

### 1.4 CloudWatch dashboard

A dashboard named `<customer>-cda-iceberg-ops` with four widgets:

| Widget | Metric source | Period |
|---|---|---|
| Ingest state machine — executions | `AWS/States` ExecutionsStarted/Succeeded/Failed | 15 min |
| EMR Serverless — billed capacity | `AWS/EMRServerless` BilledVCpu, BilledMemoryGB | 5 min |
| Recon state machine — executions | `AWS/States` ExecutionsStarted/Failed | 1 hr |
| DynamoDB — cursor table capacity | `AWS/DynamoDB` ConsumedReadCapacityUnits/WriteCapacityUnits | 15 min |

The dashboard URL is in the stack's CFN output `DashboardUrl`.

### 1.5 CloudWatch alarms

Three alarms wired to the **existing** SNS notification topic from the
orchestration stack — no new topic, no new subscriptions.

| Alarm name | Metric | Threshold | Period | What it catches |
|---|---|---|---|---|
| `<customer>-cda-iceberg-ingest-sm-failures` | `AWS/States` ExecutionsFailed (ingest SM) | > 0 | 1 hr | Failed ingest execution in last hour |
| `<customer>-cda-iceberg-emr-vcpu-spike` | `AWS/EMRServerless` BilledVCpu | > 100 | 1 hr | Runaway compute |
| `<customer>-cda-iceberg-ddb-throttles` | `AWS/DynamoDB` UserErrors on cursor table | > 0 | 15 min | Cursor write failures (future ingests will re-read) |

The 100 vCPU-hr/hr threshold on the EMR alarm is **deliberately
conservative**. For a small carrier this fires on normal full loads;
for a large carrier it might be too low. Tune in your own monitoring
config (see [§5](#5-customizing-dashboard-and-alarms)) once you've
seen one week of normal behavior.

---

## 2. Pre-flight checklist

Confirm before deploying:

### 2.1 Pipeline is validated and running

- [ ] All 5 base stacks are `CREATE_COMPLETE`
  - `<customer>-iceberg-network`
  - `<customer>-iceberg-warehouse`
  - `<customer>-iceberg-emr`
  - `<customer>-iceberg-runtime`
  - `<customer>-iceberg-orchestration`
- [ ] At least one Step Functions ingest execution succeeded
- [ ] At least one row in `cda_recon_results` (any status)
- [ ] SNS notification topic has confirmed subscribers (so the new
      alarms have somewhere to deliver)

If you deploy AnalyticsStack before recon has rows, the saved queries
will return empty results and the dashboard widgets will show no data
— the deploy succeeds either way, but it's confusing to onboard
analysts onto an empty stack.

### 2.2 Glue catalog availability

The parent catalog `s3tablescatalog` is **account-singleton**. If
another tenant in the same AWS account already created it, the stack's
custom resource will detect this and skip creation. Verify:

```bash
aws glue get-catalog --catalog-id "<account>:s3tablescatalog" 2>&1 | head -5
```

- **Returns the catalog** → it already exists, the stack will skip
  creation. Confirm with the other tenant that they're OK with you
  adding a child catalog under it.
- **Returns `EntityNotFoundException`** → the stack will create it.

### 2.3 Lake Formation access mode

If the AWS account has Lake Formation set to **strict mode** (no
`IAM_ALLOWED_PRINCIPALS` default permissions), the deployer's IAM
identity must be a Lake Formation data lake admin **before** the
custom resource can issue grants on behalf of the analyst roles:

```bash
aws lakeformation get-data-lake-settings --query 'DataLakeSettings.DataLakeAdmins'
```

If your deployer ARN is not in the list, add it once before deploy:

```bash
aws lakeformation put-data-lake-settings --data-lake-settings '{
  "DataLakeAdmins": [
    {"DataLakePrincipalIdentifier": "<deployer-role-arn>"}
  ],
  "CreateDatabaseDefaultPermissions": [
    {"Principal":{"DataLakePrincipalIdentifier":"IAM_ALLOWED_PRINCIPALS"},"Permissions":["ALL"]}
  ],
  "CreateTableDefaultPermissions": [
    {"Principal":{"DataLakePrincipalIdentifier":"IAM_ALLOWED_PRINCIPALS"},"Permissions":["ALL"]}
  ]
}'
```

This is **out-of-band** to CDK; it's an account-level setting.

### 2.4 Athena workgroup choice

The default is `primary`. If your account uses dedicated workgroups
per team or per cost-allocation tag, override via the `athenaWorkgroup`
context flag. Confirm the workgroup exists and the analyst roles have
`athena:GetWorkGroup` on it:

```bash
aws athena get-work-group --work-group <workgroup-name>
```

### 2.5 Analyst role ARNs

Collect the IAM role ARNs for the analysts who will query the data.
Each gets `SELECT + DESCRIBE` on the entire `cda` namespace. Format:

```
arn:aws:iam::<account>:role/<role-name>
```

Comma-separate multiple ARNs:

```
arn:aws:iam::123456789012:role/AnalystRole,arn:aws:iam::123456789012:role/DataEngineerRole
```

If the list is empty, the federation and dashboards still deploy but
no one is granted query access — you can add analysts later by either
re-deploying with an updated `analystRoleArns` or running the
Lake Formation grant manually (see [§4](#4-onboarding-additional-analysts)).

---

## 3. Deploy and verify

### 3.1 Deploy

```bash
npx cdk deploy --all \
  --context cdaSourceBucketArn=arn:aws:s3:::<cda-source-bucket> \
  --context enableAnalyticsStack=true \
  --context analystRoleArns=arn:aws:iam::<account>:role/<analyst-role-1>,arn:aws:iam::<account>:role/<analyst-role-2> \
  --context athenaWorkgroup=primary
```

The new stack is `<customer>-iceberg-analytics`. Deploy time: ~3-5 min
(custom-resource Lambda creation dominates).

### 3.2 Verify federation

```bash
aws glue get-databases \
  --catalog-id "<account>:s3tablescatalog/<bucket-name>" \
  --query 'DatabaseList[].Name'
```

Should return `["cda"]` (or whatever your `icebergNamespace` is). If
it returns `Insufficient Lake Formation permission(s)`, see
[§6](#6-troubleshooting).

### 3.3 Verify saved queries

In the Athena console:

1. Switch to the configured workgroup (top-right dropdown)
2. **Saved queries** tab
3. Look for the four `<customer>-cda-iceberg-*` queries

Run `<customer>-cda-iceberg-table-coverage` first — it should show
every table the pipeline has loaded so far. If it returns empty, the
pipeline hasn't run yet (see Pre-flight 2.1).

### 3.4 Verify dashboard

CFN output `DashboardUrl` from the stack. Open it in the AWS console.
The four widgets should populate within 5-10 minutes if the pipeline
has been running.

If a widget shows "No data available" past the first 24 hours of
deploying, the underlying CloudWatch metric isn't being emitted —
likely because the resource doesn't exist (e.g., the recon SM widget
will be empty until at least one recon execution has run).

### 3.5 Verify alarms

```bash
aws cloudwatch describe-alarms \
  --alarm-name-prefix "<customer>-cda-iceberg-" \
  --query 'MetricAlarms[*].[AlarmName,StateValue]' --output table
```

All three alarms should be in state `OK` or `INSUFFICIENT_DATA`. If
an alarm is in `ALARM` immediately after deploy, that's a real
finding — investigate per the alarm description rather than dismissing
it as noise.

---

## 4. Onboarding additional analysts

After first deploy, you can grant additional analyst roles in two ways.

### 4.1 Option A: re-deploy with updated context (recommended)

```bash
npx cdk deploy <customer>-iceberg-analytics \
  --context cdaSourceBucketArn=... \
  --context enableAnalyticsStack=true \
  --context analystRoleArns=<existing-arns>,<new-arn> \
  --context athenaWorkgroup=primary
```

Pros: automation-managed; the list lives in one place; revoke by removing.

Cons: requires CDK access; the deployer must have Lake Formation admin
to issue grants.

### 4.2 Option B: ad-hoc grant via CLI

```bash
aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier":"<analyst-role-arn>"}' \
  --resource '{"Database":{"CatalogId":"<account>:s3tablescatalog/<bucket>","Name":"cda"}}' \
  --permissions DESCRIBE

aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier":"<analyst-role-arn>"}' \
  --resource '{"Table":{"CatalogId":"<account>:s3tablescatalog/<bucket>","DatabaseName":"cda","TableWildcard":{}}}' \
  --permissions SELECT DESCRIBE
```

Pros: faster turnaround for one-off requests.

Cons: drift — the grant lives outside CDK and won't be revoked by a
re-deploy. Tracked nowhere except the Lake Formation grants table.

### 4.3 Revocation

```bash
aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier":"<analyst-role-arn>"}' \
  --resource '{"Database":{"CatalogId":"<account>:s3tablescatalog/<bucket>","Name":"cda"}}' \
  --permissions DESCRIBE

aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier":"<analyst-role-arn>"}' \
  --resource '{"Table":{"CatalogId":"<account>:s3tablescatalog/<bucket>","DatabaseName":"cda","TableWildcard":{}}}' \
  --permissions SELECT DESCRIBE
```

If you used Option A, also remove the role from `analystRoleArns` in
your next deploy so it doesn't get re-granted.

---

## 5. Customizing dashboard and alarms

The default dashboard widgets and alarm thresholds are starting points.
Customize via your own monitoring layer rather than forking this stack.

### 5.1 Why we don't expose alarm thresholds as context flags

Each customer's "normal" is different. A `BilledVCpu > 100` alarm
makes sense for a small/mid carrier; a large carrier might run at
500+ vCpu sustained during incremental processing. Adding context
knobs for every threshold would mean either shipping confusing
defaults or asking customers to tune knobs they don't have evidence
for yet.

The recommended pattern is to ship the AnalyticsStack as a starting
baseline, watch one week of normal behavior, then either:

- Override the alarms in your own monitoring repo (Terraform,
  CloudFormation, Datadog, etc.)
- Open a GitHub issue requesting per-alarm threshold knobs if you
  consistently want to tune on deploy

### 5.2 Adding custom widgets

The CloudWatch dashboard JSON is editable directly in the console.
Changes survive future re-deploys of the stack only if the dashboard
name doesn't change (which it won't unless you change `customerName`).

Alternative: write your own dashboard CDK construct in a separate stack
that imports the CFN outputs from `<customer>-iceberg-analytics`. This
keeps customization in your repo, not ours.

### 5.3 Disabling specific alarms

To disable an alarm without uninstalling AnalyticsStack: set its state
to `INSUFFICIENT_DATA` action via console (Actions tab → disable). This
survives stack updates because we don't manage the actions field after
initial deploy.

To remove an alarm entirely: fork the stack. The alarms are not
individually toggleable via context; if you find yourself needing this,
that's a signal to file an issue.

---

## 6. Troubleshooting

### 6.1 `Insufficient Lake Formation permission(s)` after deploy

**Symptom:** Athena query fails with this opaque error; the saved
queries also fail.

**Cause:** Lake Formation isn't actually granting the permissions the
custom resource issued. Most common reasons:

1. The deployer wasn't a Lake Formation data lake admin when CDK ran
   the custom resource. The grants succeeded on paper but didn't take
   effect.
2. The analyst role was specified with the wrong ARN format (e.g.,
   `assumed-role` instead of `role`).
3. Lake Formation's `IAM_ALLOWED_PRINCIPALS` was disabled and the
   custom resource's grants are the only authority.

**Fix:**

```bash
# Confirm grants were actually issued
aws lakeformation list-permissions \
  --principal DataLakePrincipalIdentifier=<analyst-role-arn> \
  --resource '{"Database":{"CatalogId":"<account>:s3tablescatalog/<bucket>","Name":"cda"}}'
```

If the list is empty, re-issue the grants manually per
[§4.2](#42-option-b-ad-hoc-grant-via-cli).

If the list shows `IMPLICIT` source instead of `LF-TAG`, the grants
exist but came from `IAM_ALLOWED_PRINCIPALS` and may be revoked when
that's disabled. Re-grant explicitly.

### 6.2 Glue catalog conflict

**Symptom:** deploy fails with `EntityAlreadyExistsException` on
either the parent or child catalog despite the try-create-or-skip
logic.

**Cause:** the catalog exists but with a different connection name or
identifier. The custom resource only catches "already exists" — not
"exists with conflicting properties."

**Fix:**

```bash
aws glue get-catalog --catalog-id "<account>:s3tablescatalog"
```

If the existing catalog's `FederatedCatalog.Identifier` doesn't
include `arn:aws:s3tables:...:bucket/*` or the `ConnectionName` isn't
`aws:s3tables`, you have a tenant collision. Resolve with the other
tenant before re-deploying.

### 6.3 Athena query returns "Table does not exist"

**Symptom:** `SELECT * FROM "s3tablescatalog/<bucket>"."cda"."cc_account_merged"`
returns table-not-found.

**Cause sequence:**

1. Federation is provisioned but the analyst doesn't have `DESCRIBE` on
   the table (Lake Formation still gates this even after table-wildcard
   grant in some scenarios)
2. The table actually doesn't exist yet — pipeline hasn't ingested
   `cc_account` because CDA hasn't emitted it yet
3. The table name has a typo (e.g., `cc_acount`)

**Fix:** verify the table exists first:

```bash
aws s3tables list-tables --table-bucket-arn <arn> --namespace cda \
  --query 'tables[?starts_with(name, `cc_acc`)]'
```

If the table is there, it's a Lake Formation grant issue —
[§6.1](#61-insufficient-lake-formation-permissions-after-deploy).

### 6.4 Dashboard shows "No data available"

**Symptom:** widgets are blank past the first hour of deploy.

**Cause:** the CloudWatch metric isn't being emitted. Each widget
sources from a different metric:

| Widget blank | Why |
|---|---|
| Ingest state machine | Ingest SM hasn't run since deploy. Trigger one manually. |
| EMR Serverless | EMR application hasn't run a job since the dashboard was created. Wait for next ingest. |
| Recon state machine | Recon SM hasn't run. Check `reconScheduleEnabled=true` and wait until 02:00 UTC, or trigger manually. |
| DynamoDB | DDB cursor table is in-region — should always have a tiny amount of capacity from health checks. If blank for >24 hr, suspect tagging or region drift. |

### 6.5 Alarms firing immediately after deploy

**Symptom:** one or more alarms in `ALARM` state right after deploy,
with no history to explain why.

**Most common cause:** the EMR vCPU alarm at threshold 100 fires during
an initial bulk load that's running concurrently. This is **expected**
during the first run after the schedule is armed.

**What to do:** confirm the alarm's metric matches your expectations
in the dashboard. If the bulk load is genuinely larger than the alarm
expects, raise the threshold in your monitoring layer (see
[§5.1](#51-why-we-dont-expose-alarm-thresholds-as-context-flags)).

If the alarm fires later during steady state, that's a real finding
— investigate per the alarm description.

---

## 7. Decommissioning

### 7.1 Standard `cdk destroy` behavior

```bash
npx cdk destroy <customer>-iceberg-analytics
```

This removes:
- CloudWatch alarms (3)
- CloudWatch dashboard
- Athena named queries (4)
- Lambda function backing the federation custom resource

This **does not remove**:
- Glue federated catalog (parent or child)
- Lake Formation grants on analyst roles

The retention is intentional. If another tenant or workflow depends on
the federation, removing it would break them. Lake Formation grants
are similarly preserved so analysts don't lose query access from a
stack delete.

### 7.2 Full cleanup (if no other tenant depends on federation)

```bash
# Revoke all Lake Formation grants the stack issued
for arn in <analyst-arn-1> <analyst-arn-2>; do
  aws lakeformation revoke-permissions \
    --principal "{\"DataLakePrincipalIdentifier\":\"$arn\"}" \
    --resource "{\"Database\":{\"CatalogId\":\"<account>:s3tablescatalog/<bucket>\",\"Name\":\"cda\"}}" \
    --permissions DESCRIBE
  aws lakeformation revoke-permissions \
    --principal "{\"DataLakePrincipalIdentifier\":\"$arn\"}" \
    --resource "{\"Table\":{\"CatalogId\":\"<account>:s3tablescatalog/<bucket>\",\"DatabaseName\":\"cda\",\"TableWildcard\":{}}}" \
    --permissions SELECT DESCRIBE
done

# Delete child catalog
aws glue delete-catalog --catalog-id "<account>:s3tablescatalog/<bucket-name>"

# Delete parent (only if no other tenant uses it)
aws glue delete-catalog --catalog-id "<account>:s3tablescatalog"
```

The parent deletion is the dangerous one — confirm with all tenants
before running it.

---

## 8. What this stack deliberately doesn't do

Listed here so customers don't assume features are missing:

- **No SNS topic of its own.** Reuses the orchestration stack's topic.
  Avoids fanout / subscription drift.
- **No analyst onboarding workflow.** Add analysts via CDK re-deploy
  or Lake Formation CLI.
- **No automated alarm subscriptions.** AWS requires email
  confirmation per subscriber; can't be CFN-automated.
- **No dashboard customization knobs.** Customize in a separate
  monitoring stack you own.
- **No KMS configuration.** All resources use AWS-managed keys; for
  CMK, see [DEPLOYMENT §11.1](DEPLOYMENT.md#111-encryption-at-rest).
- **No Snowflake / Trino / Databricks consumer setup.** Each has its
  own integration pattern; document the federation catalog ID and
  let the consumer team configure their tool.
- **No GDPR / right-to-be-forgotten automation.** Manual procedure
  per [DEPLOYMENT §11.6](DEPLOYMENT.md#116-gdpr--right-to-be-forgotten).

If a missing capability is blocking adoption, file a GitHub issue
with the specific gap. Several of the absences above are intentional
design decisions ([DEPLOYMENT §15.4](DEPLOYMENT.md#154-what-we-wish-we-could-automate-and-why-we-dont))
rather than oversights.

---

## Cross-references

- [README.md](../README.md) — pipeline overview, pricing, querying
- [DEPLOYMENT.md §15](DEPLOYMENT.md#15-automation-policy) — automation policy framing
- [DEPLOYMENT.md §3.4](DEPLOYMENT.md#34-athena-glue-and-lake-formation) — manual Athena/Glue/LF setup if you don't deploy AnalyticsStack
- [DEPLOYMENT.md §11](DEPLOYMENT.md#11-security-and-compliance) — security context, IAM model, encryption
