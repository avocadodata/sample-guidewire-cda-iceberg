# `data_load.sh` — full-load runbook & centralized configuration

`scripts/data_load.sh` is the operator runbook for standing up (or
re-running) a **full table load** and timing it. It is also the place where
**all configuration is centralized**: one block at the top of the script is
the single source of truth for a load, layered cleanly on top of the CDK's
`cdk.json` defaults.

This page explains, step by step, how that centralized configuration works
so you can change a load's behavior confidently.

> For *what the phases do* (ship / wipe / run / report / bootstrap) see the
> "Phases" section at the end. For *infrastructure* knobs in general, the
> authoritative reference is still [`cdk.json`](../cdk/cdk.json) and
> [`DEPLOYMENT.md` §1.4](DEPLOYMENT.md#14-naming-and-namespace-decisions);
> this page is specifically about the load runbook.

---

## 1. Where the configuration lives

Open `scripts/data_load.sh`. Everything between these two banners is the
config — **the only place you edit**:

```
###############################################################################
#                          ▼▼▼  CONFIGURATION  ▼▼▼                            #
   ... all knobs ...
###############################################################################
#                          ▲▲▲  END CONFIG  ▲▲▲                              #
# ─────────────────────────── DERIVED (do not edit) ───────────────────────── #
```

Below the `DERIVED` line, resource names, ARNs, and stack names are
**computed** from the config — you never edit those by hand. For example the
state table, table-bucket ARN, and state-machine ARN are all derived from
`CUSTOMER`, `ACCOUNT`, and `AWS_REGION`.

---

## 2. The three layers of configuration

A value is resolved in this precedence order (highest wins):

| Layer | How to set it | Scope |
|---|---|---|
| **1. Environment variable** | `MAP_CONCURRENCY=16 ./scripts/data_load.sh ship` | one run, no file edit |
| **2. Script default** | edit the `:-<default>` in the config block | every run from this checkout |
| **3. `cdk.json` default** | the CDK's own default | inherited only when the script leaves a knob empty |

Every knob is written as a Bash *default-substitution*:

```bash
MAP_CONCURRENCY="${MAP_CONCURRENCY:-16}"
#                 └── env var ──┘ └─ script default
```

- If `MAP_CONCURRENCY` is set in the environment, that value is used (layer 1).
- Otherwise the script default `8` is used (layer 2).
- If the script default were empty (`:-`), the knob is omitted at deploy time
  and `cdk.json`'s value applies (layer 3).

So **you never have to edit the file for a one-off** — prefix the command
with the variable. Editing the default is for a change you want every time.

---

## 3. How the script defaults were chosen

The baked-in script defaults are **the values the last full load actually
deployed with**, verified against the live deployment (not just copied from
`cdk.json`). Each knob's comment records this, e.g.:

```bash
MAP_CONCURRENCY="${MAP_CONCURRENCY:-16}"               # parallel per-table ingest jobs
EMR_MAX_VCPU="${EMR_MAX_VCPU:-1600 vCPU}"              # max app vCPU (40% of the 4000 account quota)
SIZE_THRESHOLD_MEDIUM="${SIZE_THRESHOLD_MEDIUM:-100000000}"  # rows >= this ride medium conf (100M)
```

This means **running the script with no changes reproduces the last load.**
That is the whole point of centralizing here: a teammate can re-run an
identical load without reconstructing flags from memory.

> One value differs from the `cdk.json` default by design: `MANIFEST_KEY`
> defaults to `synthetic/manifest.json` (what the sample-data loads use),
> while `cdk.json`'s `cdaManifestKey` default is `manifest.json`. The script
> default is correct for the synthetic dataset.

---

## 4. How config reaches the deployment

The script does **not** mutate `cdk.json`. At deploy time (`ship` /
`bootstrap`) it builds a list of `--context KEY=VAL` flags from the config
and passes them to `cdk deploy`. This is done by one helper, `context_args()`:

```bash
context_args() {
  # always-on: identity, source, namespace, manifest key, schedule off,
  # tolerated-failure
  local -a a=( --context "customerName=${CUSTOMER}" ... )

  # every override: added ONLY if non-empty (empty => inherit cdk.json)
  local pairs=( "mapStateConcurrency=${MAP_CONCURRENCY}" "cronExpression=${CRON_EXPRESSION}" ... )
  for p in "${pairs[@]}"; do [[ "${p#*=}" != "" ]] && a+=(--context "$p"); done

  # escape hatch: arbitrary key=val pairs
  for kv in $EXTRA_CONTEXT; do [[ "$kv" == *=* ]] && a+=(--context "$kv"); done
}
```

Key behaviors this gives you:

- **A non-empty script value overrides `cdk.json`** for that deploy.
- **An empty script value is omitted**, so `cdk.json`'s default applies —
  this is how genuinely-optional knobs (notification emails, table/column
  excludes, BYO-VPC, analyst roles, lifecycle source) stay inherited.
- **Values with spaces survive intact** (e.g. `cronExpression=0/15 * * * ? *`,
  `emrMaxVcpu=1600 vCPU`) because they are passed as single array elements.

### Trade-off to be aware of

Because the populated knobs are passed as `--context`, they **override
`cdk.json` on every deploy through this script**. That makes a load
reproducible regardless of later `cdk.json` edits — but it also means
**editing `cdk.json` alone will not change this script's deploys** for those
knobs. To change them, edit the script default (or pass an env override).
Knobs left empty in the script still track `cdk.json` normally.

---

## 5. The knob catalogue

Grouped exactly as they appear in the config block:

| Group | Knobs (env var → `cdk.json` key) |
|---|---|
| **AWS target** | `ACCOUNT`, `AWS_REGION` |
| **Identity** | `CUSTOMER` → customerName, `NAMESPACE` → icebergNamespace |
| **CDA source** | `SRC_BUCKET` → cdaSourceBucketArn, `MANIFEST_KEY` → cdaManifestKey |
| **Load behavior** | `TOLERATED_FAILURE_PCT` → mapToleratedFailurePercentage |
| **Scheduling** | `CRON_EXPRESSION`, `RECON_CRON_EXPRESSION`, `RECON_SCHEDULE_ENABLED`, `RECON_MAP_CONCURRENCY`, `MAP_CONCURRENCY`, `NOTIFICATION_EMAILS` |
| **Data scope** | `TABLES_TO_EXCLUDE`, `COLUMNS_TO_EXCLUDE` |
| **Per-table sizing** | `SIZE_THRESHOLD_MEDIUM`, `SIZE_THRESHOLD_LARGE` |
| **EMR capacity / warm pool** | `EMR_MAX_VCPU`, `EMR_MAX_MEMORY`, `EMR_MAX_DISK`, `EMR_IDLE_TIMEOUT_MIN`, `EMR_RELEASE_LABEL`, `EMR_INITIAL_DRIVERS`, `EMR_INITIAL_EXECUTORS` |
| **Retention** | `SNAPSHOT_RETENTION_DAYS`, `LOG_RETENTION_DAYS` |
| **Network** | `VPC_ID`, `VPC_PRIVATE_SUBNET_IDS`, `NEW_VPC_CIDR` |
| **Tagging / scan** | `TAGS`, `CDK_NAG` |
| **Analytics** | `ENABLE_ANALYTICS`, `ANALYST_ROLE_ARNS`, `ATHENA_WORKGROUP`, `RECON_TABLE_NAME` |
| **Lifecycle Events** | `LIFECYCLE_EVENTS_ENABLED`, `LIFECYCLE_PARTNER_EVENT_SOURCE`, `LIFECYCLE_SOURCE_APP` |
| **Build** | `JDK21_HOME` (resolve a JDK 21 for the build) |
| **Escape hatch** | `EXTRA_CONTEXT` — space-separated `key=val` pairs passed verbatim |

### Two things that are NOT script knobs (by design)

1. **`scheduleEnabled` is pinned to `false`.** A cron firing mid-load would
   corrupt the clean state and the timing, so the runbook never arms the
   ingest schedule. Arm it separately after the load — see
   [`DEPLOYMENT.md` §6](DEPLOYMENT.md#6-schedule-arming-gates).
2. **Spark *executor* sizing** (driver/executor cores, memory, disk, shuffle
   partitions) is **not** a `cdk.json` knob and therefore not here. The
   per-size-class conf lives in `SIZE_CONFS` in
   [`cdk/lib/orchestration-stack.ts`](../cdk/lib/orchestration-stack.ts).
   From the script you tune *which tables land in each class* via
   `SIZE_THRESHOLD_*`; to change the conf values themselves, edit
   `SIZE_CONFS` and redeploy.

### The analytics stack: deploy it separately, after the load

The optional 6th `AnalyticsStack` (Glue federation + Lake Formation grants +
Athena saved queries + dashboard + alarms) is **not** part of a load. The
`ship` / `bootstrap` / `run` phases never deploy it (`ENABLE_ANALYTICS`
defaults to `false`). Deploy it as its own step — **after** the pipeline is
validated and at least one load has run — with the dedicated phase:

```bash
ANALYST_ROLE_ARNS=arn:aws:iam::<account>:role/Analyst \
  ./scripts/data_load.sh analytics
```

The `analytics` phase enforces the correct ordering with pre-flight gates
before it deploys (see [`ANALYTICS.md` §2](ANALYTICS.md#2-pre-flight-checklist)):

1. all 5 base stacks are `*_COMPLETE`;
2. at least one ingest execution `SUCCEEDED` (else federation/queries would
   target empty tables — it warns and asks to confirm);
3. the warehouse namespace has tables;
4. `ANALYST_ROLE_ARNS` is set (else grants are a no-op — warns and confirms);
5. it prints the deployer ARN and current Lake Formation admins, so you can
   confirm the deployer is an LF admin in strict-mode accounts (required for
   the grant custom resource — [`ANALYTICS.md` §2.3](ANALYTICS.md#23-lake-formation-access-mode)).

It then deploys **only** the analytics stack (it targets that stack by name,
so it won't re-deploy the base 5) and prints verification commands.

> **Why not just flip `ENABLE_ANALYTICS=true` on `ship`?** Two reasons. (a)
> Ordering — analytics grants/queries only make sense once tables exist, so a
> dedicated gated phase is safer than folding it into a load. (b) Removal is
> not symmetric: the script has no destroy phase, and `cdk deploy` never
> *prunes*, so toggling `ENABLE_ANALYTICS` back to `false` does NOT remove an
> already-deployed analytics stack. To remove it, run
> `cdk destroy <customer>-iceberg-analytics` explicitly.

---

## 6. End-to-end sequences

### Greenfield — a brand-new AWS account

Analytics is the **last** step here: it cannot run until the pipeline exists
and one ingest has succeeded (the `analytics` phase enforces both). There is
**no `wipe`** — nothing exists to delete.

```bash
# 0. Edit the 5 must-change values in the CONFIGURATION block of the script:
#      ACCOUNT, AWS_REGION, CUSTOMER, SRC_BUCKET, MANIFEST_KEY
#    (NAMESPACE usually stays "cda"). Everything else inherits the
#    last-validated-load defaults.

# 1. Stand up the 5 base stacks. For a REAL customer CDA bucket this PAUSES
#    and prints the EMR job role ARN — send it to Guidewire to allow-list on
#    their source bucket, then press ENTER. (Auto-skipped for the synthetic
#    sample bucket, which is already cross-account readable.)
./scripts/data_load.sh bootstrap

# 2. Load all tables and time it.
./scripts/data_load.sh run

# 3. Confirm per-table success/fail before trusting the load.
./scripts/data_load.sh report

# 4. Reconcile (Tier D semantic invariants). Run AFTER the load finishes.
./scripts/data_load.sh recon

# 5. ONLY NOW deploy analytics (Glue federation, LF grants, Athena, dashboards).
#    Gated: refuses until base stacks + a SUCCEEDED ingest exist.
ANALYST_ROLE_ARNS=arn:aws:iam::<account>:role/<analyst-role> \
  ./scripts/data_load.sh analytics
```

Greenfield gotchas:
- **Guidewire allow-list round-trip** (step 1) is the one step outside your
  control — plan for hours-to-days on Guidewire's side for a real bucket.
- **Lake Formation admin**: in a strict-LF account, your deployer must be an
  LF admin *before* step 4 or the grant fails. The `analytics` phase prints
  the deployer + current admins so you can spot this; fix per
  [`ANALYTICS.md` §2.3](ANALYTICS.md#23-lake-formation-access-mode).
- **No analyst role yet?** Run step 4 with `ANALYST_ROLE_ARNS=` empty (it
  warns + asks to confirm) — federation/dashboards deploy, but nobody gets
  query access until you re-run `analytics` with roles set.

### Existing account — clean reload from zero state

```bash
./scripts/data_load.sh ship      # build + upload jar, cdk deploy (your config)
./scripts/data_load.sh wipe      # DELETE all tables + cursors (type WIPE)
./scripts/data_load.sh run       # full load + wall-clock timing
./scripts/data_load.sh report    # per-table durations + percentiles
./scripts/data_load.sh recon     # Tier D reconciliation + findings summary
# analytics, if not already deployed, is the same final step as greenfield:
ANALYST_ROLE_ARNS=arn:aws:iam::<account>:role/<analyst-role> \
  ./scripts/data_load.sh analytics
```

---

## 7. Common recipes

```bash
# Reproduce the last full load exactly (clean reload):
./scripts/data_load.sh ship && ./scripts/data_load.sh wipe \
  && ./scripts/data_load.sh run && ./scripts/data_load.sh report

# Resilient timing run — tolerate a few table failures so the execution
# reports SUCCEEDED and you get a clean wall-clock:
TOLERATED_FAILURE_PCT=10 ./scripts/data_load.sh ship

# Faster fan-out + a bigger EMR ceiling for a one-off:
MAP_CONCURRENCY=16 EMR_MAX_VCPU="1200 vCPU" ./scripts/data_load.sh ship

# Exclude PII columns and skip two tables for this load:
COLUMNS_TO_EXCLUDE=ssn,taxid,cc_claim:description \
  TABLES_TO_EXCLUDE=cc_activity,cc_note ./scripts/data_load.sh ship

# Override a knob with no dedicated slot:
EXTRA_CONTEXT="snapshotRetentionDays=7 logRetentionDays=90" ./scripts/data_load.sh ship

# Permanent change for every future run from this checkout:
#   edit the default in the config block, e.g. MAP_CONCURRENCY="${MAP_CONCURRENCY:-12}"
```

---

## 8. Phases (what each subcommand does)

| Subcommand | Deploys? | Destructive? | What it does |
|---|---|---|---|
| `bootstrap` | yes | no | Greenfield: deploy base infra → upload jar → (Guidewire allow-list pause) → deploy orchestration |
| `ship` | yes | no | Existing account: build jar, upload, `cdk deploy --all` with the config's `--context` flags |
| `wipe` | no | **YES** | Delete every Iceberg table in the namespace + clear all cursors (requires typing `WIPE`) |
| `run` | no | no | Trigger one full-load execution, poll to terminal, print wall-clock |
| `report` | no | no | Paginate all EMR job runs in the load window; per-table durations + percentiles |
| `recon` | no | no | Trigger the Tier D reconciliation state machine, wait for it, and summarize findings from `cda_recon_results`. Warns if an ingest is still running (would reconcile a partial warehouse) |
| `analytics` | yes | no | Deploy the optional `AnalyticsStack` after the pipeline is validated — gated pre-flight (base stacks, ≥1 successful ingest, tables present, analyst roles, LF admin), then deploys that stack only |
| `all` | yes | maybe | Auto: `bootstrap` (greenfield) or `ship`+`wipe` (existing), then `run`+`report`. Does NOT include `analytics` |

Each phase runs a pre-flight that refuses to proceed on the wrong account,
while the ingest schedule is armed, or while another execution is running.

> Run artifacts (job lists, timing CSVs) are written to `.load-timing/`,
> which is git-ignored.
