# cdk-nag Security Scan Report

**Audience:** AppSec / security review
**Project:** CDA → S3 Tables (Iceberg) client CDK
**Scan tool:** [cdk-nag](https://github.com/cdklabs/cdk-nag) `v2.38.2`, rule pack **AwsSolutions**
**CDK:** `aws-cdk-lib v2.257.0`

## How to reproduce

cdk-nag runs as a synth-time Aspect, gated behind a context flag so routine
deploys stay fast. To regenerate this report:

```bash
cd cdk
npx cdk synth \
  --context cdkNag=true \
  --context cdaSourceBucketArn=arn:aws:s3:::<your-cda-bucket> \
  --context enableAnalyticsStack=true \
  --context notificationEmails=<you@example.com> \
  --context analystRoleArns=<an-analyst-role-arn>
```

A clean run exits `0` and prints no `[Error]`/`[Warning]` lines. Per-stack
machine-readable reports are written to `cdk/cdk.out/AwsSolutions--*-NagReport.csv`;
the copies in this folder were generated with a placeholder account
(`111111111111`) so no real account ID or ARN is committed.

> All six stacks are scanned in one pass, including the two optional stacks
> (`orchestration`, `analytics`) — the scan deliberately enables everything
> so AppSec sees the full surface, not just the always-on base.

## Result summary

| | Count |
|---|---|
| Stacks scanned | 6 |
| Total rule evaluations | 63 |
| **Non-compliant (unaddressed)** | **0** |
| Compliant (passed outright) | 43 |
| Suppressed (accepted, with documented evidence) | 20 |

Every finding the AwsSolutions pack raised was either **fixed in code** or
**suppressed with a written rationale that is embedded in the synthesized
template** (`cdk-nag` records the reason in the resource metadata and in the
CSV `Exception Reason` column). There are no silent waivers.

## What was fixed in code

These were genuine gaps the scan caught; they are now corrected and pass the
rule cleanly (no suppression):

| Rule | Finding | Fix |
|---|---|---|
| `AwsSolutions-VPC7` | New VPC had no flow log | Added a CloudWatch-Logs VPC flow log (ALL traffic) on the provisioned VPC. `network-stack.ts`. |
| `AwsSolutions-S1` | Artifact bucket had no server access logging | Artifact bucket now logs to the logs bucket under `access-logs/artifacts/`. `runtime-stack.ts`. |
| `AwsSolutions-L1` | Lambda functions on an older Node runtime | All three first-party Lambdas bumped to the latest runtime (`NODEJS_24_X`). `orchestration-stack.ts`, `analytics-stack.ts`. |

Already-compliant controls confirmed by the scan (not newly added, but worth
noting for the reviewer): S3 public-access block + `enforceSSL` on both
buckets (`S2`/`S5`/`S10`), DynamoDB point-in-time recovery (`DDB3`), SNS SSL
enforcement (`SNS3`), Step Functions ALL-event logging + X-Ray tracing
(`SF1`/`SF2`), security-group description + no-open-ingress (`EC23`/`EC27`),
and least-privilege scoping on the EMR/S3 Tables grants.

## What was suppressed (and why)

20 evaluations are suppressed. Every suppression is **scoped to a specific
rule + resource + `appliesTo` pattern** — none are blanket stack-level
ignores. They fall into five buckets:

### 1. Object-key / sub-resource wildcards on pinned ARNs (`IAM5`) — 9

The bucket/log-group/application **ARNs are pinned**; only the trailing
`/*` (object keys, log streams, job-run IDs) is wildcarded, because those
values do not exist at deploy time. This is the expected least-privilege
shape, not a service-level `*`.

- `emr` EmrJobRole — `s3:GetObject/PutObject` on `<bucket>/*` for the
  artifact, logs, CDA-source, and S3 Tables buckets; `logs:*` on
  `/aws/emr-serverless/*`.
- `orchestration` StateMachineRole — `emr-serverless:GetJobRun` on
  `.../jobruns/*`; `lambda:InvokeFunction` on `<fn-arn>:*` (the `:*` is the
  version/alias qualifier CDK auto-appends; the function ARNs are pinned).

### 2. AWS-mandated `Resource:*` APIs (`IAM5`) — 2

A few AWS actions have **no resource-level ARN** and require `Resource:*`
by API contract:

- `orchestration` StateMachineRole — CloudWatch Logs *log-delivery* and
  X-Ray trace APIs (`CreateLogDelivery`, `PutTraceSegments`, …). This
  statement is emitted by the CDK `StateMachine` construct **because we
  enabled logging + tracing** (themselves `SF1`/`SF2` improvements).
- `analytics` FederationCrRole — `glue:*Catalog` and `lakeformation:*`
  account-singleton admin APIs. This role runs only at stack create/update
  as a one-shot federation bootstrap and is not attached to any data-plane
  compute. The analytics stack is **opt-in** (`enableAnalyticsStack=false`
  by default).

### 3. AWS-managed Lambda execution policy (`IAM4`) — 4

`AWSLambdaBasicExecutionRole` grants a function write access to **its own
CloudWatch log group only** — the AWS-recommended logging baseline.
Suppressed on the launch-condition role, the recon-list role, the analytics
federation role, and the CDK provider-framework role.

### 4. CDK-generated provider framework (`L1`, `IAM4`, `IAM5`) — 1 (+ overlaps above)

The `analytics` stack uses a custom-resource `Provider`, whose onEvent
framework Lambda + role are synthesized by `aws-cdk-lib` itself. Its Node
runtime and managed-policy/invoke-wildcard are **not configurable from this
repo** — they advance only when the CDK version is upgraded.

### Terminal log-sink bucket (`S1`) — 1

The logs bucket is the terminal server-access-log + EMR-monitoring sink.
Enabling access logging on it would require yet another log bucket (infinite
regress) or self-logging (recursive writes). Public access is fully blocked
and SSL enforced.

### 5. Distributed Map child-execution grant (`IAM5`) — 4

Both state machines run their per-table Map in **Distributed** mode (each
table is a child STANDARD execution — this is what keeps a 717-table load
under the 25,000 execution-history-event limit). CDK auto-adds a
`DistributedMapPolicy` granting `states:StartExecution` /
`DescribeExecution` / `StopExecution` on
`arn:aws:states:…:execution:<this-state-machine>:*` (and `/*:*`). The child
execution IDs are generated by Step Functions at run time and are not
knowable at deploy time; the ARN is scoped to **each state machine's own
executions only**. This is the AWS-required form for the Distributed Map
service integration — there is no narrower scoping. 2 ARN-shape findings ×
2 state machines (ingest + recon) = 4.

## Files in this report

| File | Contents |
|---|---|
| `cdk-nag-report.md` | This summary (the document AppSec should start with). |
| `cdk-nag-consolidated.csv` | All 63 evaluations across all stacks, one row each, with the suppression reason inline. |
| `cdk-nag-reports/<stack>.csv` | Per-stack machine-readable reports as emitted by cdk-nag. |

All suppression rationales also live **in code** next to the resources they
apply to (search `NagSuppressions` in `cdk/lib/*.ts`), so they are reviewed
in the same PR as any future change to those resources.
