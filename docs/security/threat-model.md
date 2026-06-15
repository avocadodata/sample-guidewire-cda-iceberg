# Threat Model — cda-iceberg-client

**Methodology:** STRIDE, produced with the
[awslabs/threat-modeling-mcp-server](https://github.com/awslabs/threat-modeling-mcp-server)
9-phase guided workflow (business context → architecture → threat actors →
trust boundaries → asset flows → STRIDE threat identification → mitigation
planning → code validation → residual risk).

**Date:** 2026-06-03
**Scope:** the `cda-iceberg-client` reference pipeline (CDK infra + Spark/Scala
data plane) deployed to a single AWS account per customer, us-east-1.
**Raw tool output:** [`.threatmodel/cda-iceberg-threat-model.md`](../../.threatmodel/cda-iceberg-threat-model.md)
and `.json` (machine-readable). This document is the curated, code-cross-referenced
view; the `.threatmodel/` artifacts are the generator's raw export.

> This is a **defensive** artifact for a reference implementation on **synthetic**
> data. Findings are modelled threats, not observed incidents. The pipeline already
> implements most of the controls below; the recommended items are hardening for a
> production deployment against real CDA data.

---

## 1. System overview

`cda-iceberg-client` ingests Guidewire CDA (Cloud Data Access) insurance parquet
exports from a **vendor-owned, cross-account S3 bucket** into Apache Iceberg tables
on **Amazon S3 Tables**. EMR Serverless (Spark 4) jobs are orchestrated by **Step
Functions DistributedMap** (one job per table, up to 717); **DynamoDB** holds
per-table cursors/watermarks; an optional **Glue federation + Lake Formation +
Athena** layer serves analysts. Data is insurance **PII and financial** content
(policyholders, claims, amounts).

### Trust boundaries

| Boundary | From → To | Control at the crossing |
|---|---|---|
| **Cross-account** | Vendor account (CDA source bucket) → our EMR compute | Vendor bucket-policy allow-list of the EMR job-role ARN; read-only |
| **Account perimeter** | Operator workstation → control plane | IAM/SSO; EMR runs in private subnets, S3 gateway endpoint, no public IPs |
| **Data authorization** | Analytics consumption (Athena) → S3 Tables data | Lake Formation grants; column-exclusion at ingest; SSE at rest |

### Crown-jewel assets

1. **Policyholder PII / claims data** (Regulated) — at rest in S3 Tables, in transit from source.
2. **Financial amounts** (Confidential) — integrity-critical for downstream reporting.
3. **EMR job-role credentials** (Confidential) — read source + read/write warehouse.
4. **Application + runtime jars** (Internal) — executed in the Spark driver; supply-chain target.
5. **Ingest cursors / watermarks** (Internal) — integrity controls the re-read/dedupe boundary.

---

## 2. Findings summary

12 threats across all six STRIDE categories. Severity/likelihood as recorded in
the model. "Status" reflects whether a mitigating control already exists in this
codebase (**Mitigated**), is partially present (**Partial**), or is a hardening
**Recommendation** for production.

| ID | STRIDE | Threat (abbrev.) | Severity | Likelihood | Status |
|----|--------|------------------|----------|-----------|--------|
| T-TAMPER-SOURCE | Tampering | Vendor poisons source parquet / manifest | High | Possible | Partial |
| T-INFO-CROSSACCT | Info Disclosure | Stolen EMR role exfiltrates PII | High | Unlikely | Mitigated |
| T-ELEV-OPERATOR | Elevation of Priv | Stolen operator creds → full compromise/wipe | High | Unlikely | **Recommendation** |
| T-SUPPLY-JAR | Tampering | Trojaned dependency/jar → RCE in Spark driver | High | Unlikely | Partial |
| T-SPOOF-ROLE | Spoofing | Unintended principal assumes EMR job role | High | Unlikely | Mitigated |
| T-INFO-ANALYST | Info Disclosure | Over-privileged analyst reads PII columns | Medium | Possible | Partial |
| T-MANIFEST-REPLAY | Tampering | Forged manifest timestamps force re-ingest/skip | Medium | Unlikely | Mitigated |
| T-DOS-CONCURRENCY | Denial of Service | Runaway fan-out exhausts EMR / cost blow-up | Medium | Unlikely | Mitigated |
| T-REPUDIATION-INGEST | Repudiation | Out-of-band warehouse edits, weak attribution | Medium | Unlikely | Partial |
| T-TAMPER-CURSOR | Tampering | Corrupt DynamoDB cursors → data loss/dupes | Medium | Unlikely | Mitigated |
| T-INFO-LOGS | Info Disclosure | Logs leak data-model / field names | Low | Possible | Mitigated |
| T-DOS-POISON-SCHEMA | Denial of Service | Pathological schema changes break ingest | Low | Possible | Partial |

**Posture:** 6 Mitigated, 5 Partial, 1 Recommendation-only. No Critical-severity
threats; the High-severity set is dominated by credential/supply-chain compromise
(low likelihood, high impact) and the one genuinely-residual item, **vendor source
data poisoning (T-TAMPER-SOURCE)**, which structural reconciliation detects but does
not prevent.

---

## 3. Threats and mitigations

### T-TAMPER-SOURCE — Vendor poisons the source data *(Tampering, High / Possible)*
The CDA source bucket is **vendor-owned and trusted as the data origin**. Ingest
validates *schema*, not *content provenance*, so poisoned parquet rows or a
manipulated `manifest.json` would propagate into the warehouse.

- **Existing controls:**
  - **4-tier reconciliation incl. Tier D invariants** (`no_orphans`,
    `count_formula`, `latest_seqval`, `tombstone_integrity`) — `recon/MergedInvariants.scala`,
    `recon/ReconStore.scala`. Detects structural corruption / data loss after the fact;
    append-only `run_id` ledger.
  - **Identifier sanitization + catalog-API DDL** — `Identifiers.requireValid`
    (`Identifiers.scala:44`) and `IcebergCatalog` use typed `Identifier`/`StructType`
    rather than built SQL strings, so malicious table/column names from the source
    can't become SQL-injection sinks (semgrep-clean).
- **Residual risk:** recon's invariants are *structural* (row math, sequence,
  tombstones). They will **not** catch semantically-plausible poisoned values
  (e.g. altered financial amounts that still satisfy the count formula).
- **Recommendation (M-CONTENT-VALIDATION):** add content-level anomaly detection
  — per-table row-count deltas vs history, value-range/domain checks on key
  financial fields — beyond schema validation.

### T-INFO-CROSSACCT — Stolen EMR role exfiltrates PII *(Information Disclosure, High / Unlikely)*
An attacker with the EMR job-role credentials could read the warehouse and the
cross-account source.

- **Mitigated:**
  - **Least-privilege job role** — read-only (`GetObject`/`ListBucket`/`GetBucketLocation`)
    on the CDA source, scoped read/write on warehouse+logs+state, no wildcard admin
    (`emr-stack.ts:97,116-123`); cdk-nag IAM5 suppressions documented per statement.
  - **SSE at rest** on all buckets + S3 Tables; **BlockPublicAccess** everywhere;
    **BucketOwnerEnforced** on logs.
  - **TLS in transit** — `enforceSSL` on buckets, S3 gateway endpoint.
  - **Private networking** — EMR in private subnets, dedicated SG, no public IPs.
  - **Column-exclusion** (`ColumnExclusion.scala`) lets carriers drop sensitive
    columns *before* they're written, shrinking the exfiltration target.

### T-ELEV-OPERATOR — Stolen operator credentials *(Elevation of Privilege, High / Unlikely)*
`data_load.sh` runs with powerful deploy credentials (CDK deploy, **wipe**, jar
upload). Theft → deploy malicious infra, disable column-exclusion, upload a
trojaned jar, or destroy data.

- **Existing:** the destructive `wipe` phase requires a typed confirmation.
- **Recommendation (M-OPERATOR-MFA) — primary action item:**
  - Enforce **MFA + short-lived SSO** credentials for operators.
  - **Separate the deploy role from the run/trigger role** (least privilege for
    routine operation; elevation only for deploys).
  - Require an approval gate for `wipe` beyond the local typed confirmation
    (e.g. a second approver / change ticket).

### T-SUPPLY-JAR — Trojaned dependency or artifact jar *(Tampering, High / Unlikely)*
The Spark driver executes the application jar + `--jars` runtime deps from S3.

- **Partial (M-JARS-PINNED):** runtime deps are **staged to S3 (`deps/`) and
  referenced with `--jars`** rather than resolved from Maven at job start; the
  artifact bucket is **versioned**; deps are deterministic. This removes the
  runtime Maven-resolution dependency and the associated tampering/availability surface.
- **Recommendation (M-ARTIFACT-INTEGRITY):** sign or checksum the jar + deps and
  verify at job start; enable **S3 Object Lock / MFA-delete** on the artifact bucket.

### T-SPOOF-ROLE — Unintended principal assumes the job role *(Spoofing, High / Unlikely)*
- **Mitigated:** the EMR job-role trust policy is restricted to the
  `emr-serverless.amazonaws.com` service principal only (`emr-stack.ts:87`) — no
  cross-account or wildcard principals. Private networking further constrains use.

### T-INFO-ANALYST — Over-privileged analyst reads PII *(Information Disclosure, Medium / Possible)*
Lake Formation grants are `SELECT`/`DESCRIBE`; a **table-wildcard** grant exposes
every column, including any PII not stripped at ingest.

- **Partial:** analysts are **not** made LF admins; column-exclusion can pre-strip
  PII. The analytics stack is opt-in (`enableAnalyticsStack=false` by default).
- **Recommendation:** prefer **column-level LF grants** (not table-wildcard) for
  tables containing PII, and document a required-exclusion list (SSN-like fields)
  in `COLUMNS_TO_EXCLUDE` for production loads.

### T-MANIFEST-REPLAY — Forged manifest timestamps *(Tampering, Medium / Unlikely)*
- **Mitigated:** the launch-condition Lambda runs **`detectReset`**
  (`cdk/lambdas/launch-condition/detect-reset.mjs`) before change detection; an
  HWM rollback or pruned fingerprint routes to a high-priority alert + `Fail`
  terminal, refusing to ingest with untrusted timestamps.

### T-DOS-CONCURRENCY — Runaway fan-out *(Denial of Service, Medium / Unlikely)*
- **Mitigated:** EMR `maximumCapacity` vCPU ceiling + DistributedMap
  `maxConcurrency` bound the blast radius; `toleratedFailurePercentage` governs
  partial-failure handling; CloudWatch **EMR vCPU-spike** and **SFN failure**
  alarms fire to SNS. The `--jars` change also removed Maven Central as a
  job-start dependency (a prior real cause of mass job failures).

### T-REPUDIATION-INGEST — Out-of-band warehouse edits *(Repudiation, Medium / Unlikely)*
Iceberg tables are writable by anyone with sufficient IAM; recon is the integrity check.

- **Partial:** Tier D invariants detect divergence; the Spark job is the single
  authoritative writer of cursors/HWM.
- **Recommendation (M-CLOUDTRAIL):** enable **CloudTrail data events** on the
  warehouse + artifact buckets and DynamoDB for attribution of out-of-band changes.

### T-TAMPER-CURSOR — Corrupt DynamoDB cursors *(Tampering, Medium / Unlikely)*
- **Mitigated:** the Spark job is the **single authoritative writer** of
  cursors/HWM, advancing from the manifest snapshot it read at job time (no lagging
  post-Map Lambda); DynamoDB write is scoped to the job role. Recon detects the
  data-level symptoms (loss/dupes) if cursors are tampered.

### T-INFO-LOGS — Logs leak the data model *(Information Disclosure, Low / Possible)*
- **Mitigated:** logs bucket is **BucketOwnerEnforced**, private, SSE, 30-day
  lifecycle; CloudWatch log groups are account-scoped. Logs carry table/column
  names and row counts, **not row data**.

### T-DOS-POISON-SCHEMA — Pathological schema changes *(Denial of Service, Low / Possible)*
Schema evolution auto-applies additive `ADD COLUMN` / type-widen from source parquet.

- **Partial:** evolution is additive-only and validated; recon catches downstream effects.
- **Recommendation (M-SCHEMA-GUARD):** bound column counts and alert on unexpected
  schema churn per table.

---

## 4. Recommended actions (prioritized)

| Priority | Action | Addresses | Effort |
|----------|--------|-----------|--------|
| **P1** | Operator identity hardening: MFA/SSO, split deploy vs run/trigger roles, approval gate for `wipe` | T-ELEV-OPERATOR | Medium |
| **P1** | Source **content** validation / anomaly detection (row-count deltas, value-range checks) | T-TAMPER-SOURCE | Medium |
| **P2** | Sign/checksum jar + deps and verify at job start; S3 Object Lock on artifact bucket | T-SUPPLY-JAR | Medium |
| **P2** | Column-level Lake Formation grants for PII tables; documented required `COLUMNS_TO_EXCLUDE` | T-INFO-ANALYST | Low |
| **P3** | CloudTrail data events on warehouse/artifact buckets + DynamoDB | T-REPUDIATION-INGEST, T-TAMPER-CURSOR | Low |
| **P3** | Schema-churn guardrail (column-count bound + alert) | T-DOS-POISON-SCHEMA | Low |

All P1/P2/P3 items are **production-hardening** recommendations. The reference
implementation's baseline posture (least-privilege IAM, encryption everywhere,
private networking, reconciliation, identifier sanitization, pinned dependencies)
already mitigates the majority of the modelled attack surface — consistent with the
[cdk-nag report](./cdk-nag-report.md).

---

## 5. Assumptions & scope notes

- **Synthetic sample data** uses random `gwcbi___operation` codes, so Tier D recon
  reports `MISMATCH` on the sample — this is a data artifact, not a finding.
- Single-account-per-customer; multi-tenant isolation is out of scope.
- The analytics stack (Glue/LF/Athena) is **opt-in** and assumed deployed only after
  pipeline validation; T-INFO-ANALYST applies only when it is enabled.
- Physical/host security of EMR Serverless and AWS service-side controls are
  inherited from AWS and out of scope.

---

## 6. How to reproduce / update this threat model

The threat model is generated by the `threat-modeling-mcp-server` MCP server.
Because the server keeps state in-memory (non-persistent across processes), the
full 9-phase workflow is driven in a single run. The driver script and raw exports:

- Driver (one-shot, all phases): the workflow populates business context,
  architecture, threat actors, trust boundaries, assets/flows, then 12 STRIDE
  threats, 19 mitigations, and 30 mitigation→threat links, and exports.
- Raw output: `.threatmodel/cda-iceberg-threat-model.{md,json}`
- Curated view: this file.

To regenerate, re-run the workflow and re-export; then reconcile any new threats
into the table in §2 and cross-reference controls to code as above.
