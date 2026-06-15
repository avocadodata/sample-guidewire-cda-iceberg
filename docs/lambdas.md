# Lambda Functions

This pipeline ships three Lambda functions. Each has a narrow job and
a small attack surface. This page documents the environment contract,
input/output formats, IAM scope, and error modes for each.

For the source code, see `cdk/lambdas/<function-name>/`.

## Overview

| Function | Trigger | Purpose |
|---|---|---|
| [`launch-condition`](#1-launch-condition) | Step Functions (ingest SM) | Reads CDA manifest, diffs against DDB cursors, returns START / STOP / CDA_RESET |
| [`recon-list`](#2-recon-list) | Step Functions (recon SM) | Reads CDA manifest, returns full table list for Tier D recon fan-out |

Both are deployed by `cdk/lib/orchestration-stack.ts` with scoped IAM
roles. Neither has a public endpoint.

> **No bookmark Lambda.** Earlier versions had an `advance-state`
> Lambda that wrote the table-level high-water mark after the Map state
> completed. It was removed: the Spark job is now the single
> authoritative writer of both the per-fingerprint cursors AND the
> high-water mark, advancing the HWM from the manifest snapshot it read
> at job time. A post-Map Lambda used the launch-condition Lambda's
> older manifest read, so its HWM lagged the cursors — at CDA's
> 90-120 s update cadence that lag caused spurious re-dispatch every
> cron cycle. See the [Bookmark consistency](#bookmark-consistency)
> note below.

---

## 1. launch-condition

**Source:** `cdk/lambdas/launch-condition/`
**Function name (deployed):** `<customer>-cda-iceberg-launch-condition`
**Runtime:** Node.js 20.x
**Memory:** 256 MB
**Timeout:** 2 min

The most logically dense of the three. Decides whether ingest should
run at all, and detects CDA-side regressions before they pollute the
warehouse.

### Environment contract

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SOURCE_BUCKET` | yes | — | CDA writer S3 bucket name |
| `MANIFEST_KEY` | yes | — | Object key of `manifest.json` inside `SOURCE_BUCKET` |
| `STATE_TABLE` | yes | — | DynamoDB table holding per-table state |
| `TABLES_TO_EXCLUDE` | no | `""` | Comma-separated table names to skip |
| `SIZE_THRESHOLD_MEDIUM` | no | `100000000` | Row-count threshold for medium-class jobs |
| `SIZE_THRESHOLD_LARGE` | no | `1000000000` | Row-count threshold for large-class jobs |
| `AWS_REGION` | implicit | `us-east-1` | Set by Lambda runtime |

### Input

The state machine invokes with an empty event:

```json
{}
```

### Output (one of three shapes)

**START** — at least one table has new data and no regression detected:

```json
{
  "status": "START",
  "changedTables": [
    { "tableName": "cc_account", "ts": "1700000123000", "sizeClass": "medium" },
    { "tableName": "cctl_accidenttype", "ts": "1700000123000", "sizeClass": "small" }
  ],
  "sourceBucket": "my-cda-bucket",
  "manifestKey": "manifest.json"
}
```

**STOP** — manifest unchanged for every table since last run:

```json
{
  "status": "STOP",
  "changedTables": [],
  "sourceBucket": "my-cda-bucket",
  "manifestKey": "manifest.json"
}
```

**CDA_RESET** — at least one table has regressed (HWM rolled back or
fingerprint pruned):

```json
{
  "status": "CDA_RESET",
  "resets": [
    { "tableName": "cc_account", "kind": "hwm_regression",
      "detail": "stored=1700000200000 > manifest=1700000100000" },
    { "tableName": "cc_claim", "kind": "fingerprint_pruned",
      "detail": "cursors for fp(s) no longer in schemaHistory: abc123" }
  ],
  "changedTables": [],
  "sourceBucket": "my-cda-bucket",
  "manifestKey": "manifest.json"
}
```

The state machine routes on `$.status`:
- `START` → Map fan-out (Spark advances cursors + HWM) → SUCCESS
- `STOP`  → SNS info "no changes" → SUCCESS
- `CDA_RESET` → SNS critical alert → FAIL with `error=CdaReset`

### Reset detection

Two regression kinds, both compared per-table:

| Kind | Triggered when | Cause |
|---|---|---|
| `hwm_regression` | stored `lastSuccessfulWriteTimestamp` > manifest's HWM (BigInt compare) | CDA re-deploy or replay |
| `fingerprint_pruned` | DDB has a cursor for a fingerprint no longer in `schemaHistory` | CDA recreated the table |

The pure detection logic lives in `detect-reset.mjs` (separated from
the SDK-importing `index.mjs` so it's unit-testable). Tests:
`cdk/lambdas/launch-condition/test/detect-reset.test.mjs` (7 cases).

### IAM scope

| Permission | Resource | Why |
|---|---|---|
| `s3:GetObject` | exact `manifest.json` ARN only | Read manifest |
| `dynamodb:GetItem` | cursor table | Read per-table state |
| `logs:*` | own log group only | Lambda boilerplate |

No write to S3, no write to DDB. Pure read.

### Error modes

| Symptom | Cause | Response |
|---|---|---|
| `Failed to read manifest ... PermanentRedirect` | Source bucket in different region | SDK client uses `followRegionRedirects: true`; verify deploy is current |
| `Failed to read manifest ... AccessDenied` | Bucket policy missing on source side | Re-validate cross-account IAM ([DEPLOYMENT §3.3](DEPLOYMENT.md#33-validate-the-cross-account-read)) |
| `Manifest ... is not valid JSON` | Source bucket has a corrupted/partial manifest | Contact Guidewire support |
| `Task timed out after 120 seconds` | Manifest is huge (1000+ tables) or DDB latency | [DEPLOYMENT §13.6](DEPLOYMENT.md#136-lambda-timeout) |

### Troubleshooting

Driver log is in CloudWatch under
`/aws/lambda/<customer>-cda-iceberg-launch-condition`. Look for
the per-table comparison output:

```
INFO: cc_account: stored ts=1700000123000, manifest ts=1700000123000, sizeClass=medium
INFO: cc_claim: regression detected, kind=hwm_regression
INFO: returning status=CDA_RESET, 1 reset, 0 changed
```

If you see `status=STOP` but expect new data, verify the manifest's
`lastSuccessfulWriteTimestamp` actually advanced from CDA's side — most
"why didn't it run?" questions are CDA-side.

---

## 2. recon-list

**Source:** `cdk/lambdas/recon-list/`
**Function name (deployed):** `<customer>-cda-iceberg-recon-list`
**Runtime:** Node.js 20.x
**Memory:** 256 MB
**Timeout:** 2 min

Simpler than `launch-condition`: dumps the manifest's full table list,
no diffing. Used by the recon state machine to fan out Tier D checks
across every table regardless of whether new data has arrived.

### Environment contract

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SOURCE_BUCKET` | yes | — | CDA writer S3 bucket name |
| `MANIFEST_KEY` | yes | — | Object key of `manifest.json` |
| `TABLES_TO_EXCLUDE` | no | `""` | Comma-separated table names to skip |
| `AWS_REGION` | implicit | `us-east-1` | Set by Lambda runtime |

### Input

Empty event from Step Functions:

```json
{}
```

### Output

```json
{
  "tables": [
    { "tableName": "cc_account",   "sourceBucket": "my-cda-bucket", "manifestKey": "manifest.json" },
    { "tableName": "cc_activity",  "sourceBucket": "my-cda-bucket", "manifestKey": "manifest.json" }
  ],
  "count": 717
}
```

The Map state in `cda-iceberg-recon` iterates over `$.tables` and
fires one EMR `--recon-only` job per entry.

### IAM scope

| Permission | Resource | Why |
|---|---|---|
| `s3:GetObject` | exact `manifest.json` ARN only | Read manifest |
| `logs:*` | own log group only | Lambda boilerplate |

No DDB access (recon doesn't touch cursors). No write of any kind.

### Error modes

Same S3 / region-redirect / JSON-parse errors as `launch-condition`.
There is no DDB component, so the timeout cause is purely manifest
size or S3 latency.

---

## Bookmark consistency

Earlier versions used a post-Map `advance-state` Lambda to write the
table-level high-water mark (`lastSuccessfulWriteTimestamp`). It has
been removed.

**Why it was removed.** The Spark ingest job reads the CDA manifest at
job time — minutes after the `launch-condition` Lambda read it at the
start of the run. CDA advances `lastSuccessfulWriteTimestamp` every
90-120 seconds, so the Spark job's manifest read is almost always newer
than launch-condition's. The Spark job writes accurate per-fingerprint
cursors from its own read; a separate Lambda writing the HWM from
launch-condition's older read produced a bookmark that **lagged the
cursors**. Result: every cron cycle, `launch-condition` saw
`manifest.HWM != stored.HWM`, dispatched an EMR job, and the job found
nothing new (cursors were accurate) and no-op'd — wasted EMR cost on
most tables every 15 minutes.

**The fix.** The Spark job is now the single authoritative writer of
**both** the per-fingerprint cursors and the table-level HWM. It
advances the HWM (max-merge, never backward) from the manifest snapshot
it read, on two paths:

- after all fingerprints commit successfully, and
- on the no-op early-return (table already caught up), so the HWM still
  advances even when there's no new data to ingest.

Both writes go through the EMR job role's existing `UpdateItem` grant
on the cursor table — no extra IAM, no extra Lambda. See
`IcebergIngest.run`'s `advanceHighWaterMark` calls and
`CursorStore.advanceHighWaterMark`.

**Crash safety.** The HWM and the per-fingerprint cursors are separate
attributes advanced at different points. A crash mid-table leaves the
HWM behind (it advances only after all fingerprints succeed), so the
next run re-checks the table. The cursors, advanced per-fingerprint
right after each raw append, prevent re-reading folders already
ingested. The two together give exactly-once raw ingest with no
spurious re-dispatch.

## Cross-cutting

### Logging

All three Lambdas use CloudFormation-managed log groups under
`/aws/lambda/<customer>-cda-iceberg-<function-name>`. Retention is
controlled by the `logRetentionDays` context flag (default 30 days).
See [DEPLOYMENT §11.5](DEPLOYMENT.md#115-audit-trail).

### Region

The CDA source bucket may be in a different region than the
deployment. Both `launch-condition` and `recon-list` use
`followRegionRedirects: true` on the S3 client and require
`s3:GetBucketLocation` on the source bucket policy.

### Testing

`detect-reset.mjs` is the only Lambda module with unit tests
(`detect-reset.test.mjs`, 7 cases). The other Lambdas are thin glue;
their behavior is exercised end-to-end by the state-machine smoke
test in [DEPLOYMENT §4.4](DEPLOYMENT.md#phase-44--single-table-smoke-test).

```bash
node cdk/lambdas/launch-condition/test/detect-reset.test.mjs
```

### Updating Lambda code

Lambda source changes deploy via standard `npx cdk deploy` — CDK
detects the file mtime change in `cdk/lambdas/<function>/` and
re-uploads. No separate Lambda packaging step.

### Why no API Gateway

None of these Lambdas have public endpoints. They're invoked only by
the state machines (via `LambdaInvoke` task). There's no incoming
network surface to secure.

### Customizing

If you fork the Lambdas, keep these invariants:

1. **launch-condition's reset detection should not be removed.**
   The state machine relies on `status: "CDA_RESET"` to halt ingest.
   Disabling it would let regressed manifests silently corrupt data.
2. **The bookmark writers must use conditional max-merge, never an
   unconditional overwrite.** The Spark job advances both the cursors
   and the HWM with `UpdateItem` + a `< :new` condition so a stale or
   concurrent writer can never move a bookmark backward. An
   unconditional `PutItem`/SET would clobber the `fingerprintCursors`
   map or rewind the HWM. See [Bookmark consistency](#bookmark-consistency).
3. **Both Lambdas should remain stateless.** They read inputs and write
   outputs; they don't keep cross-invocation memory.

If you find yourself adding state, that's a signal the logic belongs
in the Spark job (which has DDB write access) or in the state machine
(which has its own execution context).

---

## Cross-references

- [`README.md`](../README.md) — pipeline overview
- [`DEPLOYMENT.md`](DEPLOYMENT.md) — deployment guide
- [`DEPLOYMENT.md` §13](DEPLOYMENT.md#13-operational-runbooks) — runbooks for each error mode above
- `cdk/lib/orchestration-stack.ts` — Lambda IAM and trigger wiring
