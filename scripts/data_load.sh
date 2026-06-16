#!/usr/bin/env bash
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0

#
# data_load.sh — ship current code, run a full 717-table load from
# scratch, and time it. Works for two starting points:
#
#   GREENFIELD (nothing deployed yet — fresh AWS account):
#     ./scripts/data_load.sh bootstrap   # deploy infra → upload jar → deploy orchestration
#     ./scripts/data_load.sh run          # trigger full load, time it
#     ./scripts/data_load.sh report
#   → bootstrap pauses before wiring the source bucket so you can do the
#     Guidewire EmrJobRoleArn allow-list round-trip (skipped automatically
#     for the synthetic sample bucket, which is already cross-account read).
#
#   CLEAN RELOAD (stacks already deployed — re-time from zero state):
#     ./scripts/data_load.sh ship         # build jar, upload, cdk deploy
#     ./scripts/data_load.sh wipe          # DELETE all tables + cursors (typed confirm)
#     ./scripts/data_load.sh run
#     ./scripts/data_load.sh report
#
#   POST-LOAD (after a load has succeeded):
#     ./scripts/data_load.sh recon        # trigger Tier D recon, summarize findings
#     ANALYST_ROLE_ARNS=arn:...:role/X ./scripts/data_load.sh analytics  # deploy AnalyticsStack
#
#   ./scripts/data_load.sh all   # auto: bootstrap (greenfield) OR ship+wipe (existing), then run+report
#                                  (does NOT include recon/analytics — run those deliberately)
#
# DESTRUCTIVE: `wipe` deletes every Iceberg table in the `cda` namespace
# and clears every cursor. Recovery = re-load from CDA, not a restore.
#
# For a clean TIMING run, set per-table failure tolerance so a few
# synthetic-data table failures don't mark the whole execution FAILED:
#   TOLERATED_FAILURE_PCT=10 ./scripts/data_load.sh ship
#   TOLERATED_FAILURE_PCT=10 ./scripts/data_load.sh all
# Default is 0 (strict). The value is applied by ship/bootstrap at deploy
# time, so set it on the SAME invocation that deploys (ship/bootstrap/all).
#
# Pre-reqs: AWS credentials for YOUR target account/region, JDK 21, Node.
# This is a REFERENCE operator script — set the CONFIG block below (or the
# matching env vars) to your own account, region, customer name, and CDA
# source bucket before running. ACCOUNT defaults to the account your AWS
# credentials resolve to (via sts get-caller-identity).
set -euo pipefail

###############################################################################
#                          ▼▼▼  CONFIGURATION  ▼▼▼                            #
#                                                                             #
#  This is the ONE place to edit. Set ACCOUNT (or leave empty to auto-detect), #
#  CUSTOMER, and the CDA SRC_BUCKET to your own values before running. The     #
#  remaining knobs ship with sensible production defaults. Override any knob   #
#  two ways:                                                                   #
#    1. edit its default here, or                                             #
#    2. set the same-named env var for a one-off run, e.g.:                   #
#         MAP_CONCURRENCY=16 TOLERATED_FAILURE_PCT=10 ./scripts/data_load.sh ship
#  Non-empty knobs are passed as --context and OVERRIDE cdk.json at deploy    #
#  time; knobs left empty inherit cdk.json. Everything below the "DERIVED"    #
#  line is computed — don't edit.                                            #
###############################################################################

# ── AWS target ───────────────────────────────────────────────────────────
# Leave ACCOUNT empty to auto-detect from your AWS credentials (recommended),
# or pin it to a specific 12-digit account id to guard against deploying to
# the wrong account. AWS_REGION defaults to us-east-1.
ACCOUNT="${ACCOUNT:-}"                              # AWS account id (empty = auto-detect from caller identity)
AWS_REGION="${AWS_REGION:-us-east-1}"              # region

# ── This deployment's identity ─────────────────────────────────────────────
CUSTOMER="${CUSTOMER:-cda}"                        # customerName (resource name prefix); set to your own short tag
NAMESPACE="${NAMESPACE:-cda}"                      # Iceberg namespace (+ --context icebergNamespace)

# ── CDA source ─────────────────────────────────────────────────────────────
# REQUIRED: set SRC_BUCKET to the S3 bucket NAME where Guidewire CDA writes
# your account's parquet + manifest. This is typically a Guidewire-owned,
# cross-account bucket allow-listed to your EMR job-role ARN (bootstrap pauses
# for that round-trip). MANIFEST_KEY is the manifest object key within it.
SRC_BUCKET="${SRC_BUCKET:-REPLACE_WITH_YOUR_CDA_SOURCE_BUCKET}"  # CDA source bucket NAME (REQUIRED — no default)
MANIFEST_KEY="${MANIFEST_KEY:-manifest.json}"                    # manifest key (+ --context cdaManifestKey)

# ── Load behavior ───────────────────────────────────────────────────────────
# Per-table failure tolerance (%). 0 = strict (one failed table fails the
# whole execution). For a clean TIMING run set e.g. 10 so a few
# synthetic-data table failures don't mark the run FAILED — wall-clock then
# reports SUCCEEDED and `report` shows per-table detail. Applied at deploy
# time (ship/bootstrap), so set it on the invocation that deploys.
# Default 0 = strict (fail-fast), the production-correct setting.
TOLERATED_FAILURE_PCT="${TOLERATED_FAILURE_PCT:-0}"

# ── Config knobs (default to the LAST FULL LOAD's values) ───────────────────
# Each knob defaults to the value the most recent full load deployed with.
# Override per-run via env var (e.g. MAP_CONCURRENCY=16 ./scripts/data_load.sh
# ship) or by editing the default here. Any value set here is passed as
# --context at deploy time; empty values are omitted (so they inherit
# cdk.json). The baked defaults below were verified against the live last
# deployment, not just cdk.json.
#
# TRADEOFF: because these are passed as --context, they OVERRIDE cdk.json on
# every deploy. That makes the load reproducible regardless of later cdk.json
# edits — but it also means changing cdk.json alone won't take effect here;
# change the value in this block (or pass an env override) instead. Knobs
# left empty below (notifications, excludes, vpcId, analyst roles, lifecycle
# source) still inherit cdk.json.
#
# Orchestration / scheduling
CRON_EXPRESSION="${CRON_EXPRESSION:-0/60 * * * ? *}"   # ingest cron (hourly). NOTE: schedule itself stays DISABLED for a load
RECON_CRON_EXPRESSION="${RECON_CRON_EXPRESSION:-0 2 * * ? *}" # Tier D recon cron (daily 02:00 UTC)
RECON_SCHEDULE_ENABLED="${RECON_SCHEDULE_ENABLED:-false}"    # arm recon schedule
RECON_MAP_CONCURRENCY="${RECON_MAP_CONCURRENCY:-32}"   # parallel recon jobs. Recon is light (4 SQL counts, no source read/MERGE) so this matches ingest; 4 was needlessly slow (~6.5h for 717 tables), 16 ~halved it; 32 (medium profile, ~16-24 vCPU/job → ~512-768 of 2000 ceiling) ~halves again
MAP_CONCURRENCY="${MAP_CONCURRENCY:-32}"                # parallel per-table ingest jobs (mapStateConcurrency)
NOTIFICATION_EMAILS="${NOTIFICATION_EMAILS:-}"         # comma-sep SNS subscribers (empty = inherit cdk.json)
#
# Data scope
TABLES_TO_EXCLUDE="${TABLES_TO_EXCLUDE:-}"             # comma-sep skip list (empty = none, all tables load)
COLUMNS_TO_EXCLUDE="${COLUMNS_TO_EXCLUDE:-}"           # NO spaces, e.g. 'ssn,cc_claim:description' (empty = none)
#
# Per-table sizing (which tables ride medium/large spark conf)
SIZE_THRESHOLD_MEDIUM="${SIZE_THRESHOLD_MEDIUM:-100000000}"  # rows >= this ride medium conf (100M)
SIZE_THRESHOLD_LARGE="${SIZE_THRESHOLD_LARGE:-1000000000}"   # rows >= this ride large conf (1B)
#
# EMR Serverless capacity + warm pool
EMR_MAX_VCPU="${EMR_MAX_VCPU:-2000 vCPU}"               # max app vCPU
EMR_MAX_MEMORY="${EMR_MAX_MEMORY:-8000 GB}"            # max app memory
EMR_MAX_DISK="${EMR_MAX_DISK:-24000 GB}"               # max app disk
EMR_IDLE_TIMEOUT_MIN="${EMR_IDLE_TIMEOUT_MIN:-15}"     # idle min before stop (emrIdleTimeoutMinutes)
EMR_RELEASE_LABEL="${EMR_RELEASE_LABEL:-emr-spark-8.0-preview}" # EMR release (emrServerlessReleaseLabel)
EMR_INITIAL_DRIVERS="${EMR_INITIAL_DRIVERS:-32}"        # warm-pool drivers (emrInitialDriverCount)
EMR_INITIAL_EXECUTORS="${EMR_INITIAL_EXECUTORS:-64}"    # warm-pool executors (emrInitialExecutorCount)
#
# Iceberg / retention
SNAPSHOT_RETENTION_DAYS="${SNAPSHOT_RETENTION_DAYS:-5}"  # PITR window (snapshotRetentionDays)
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-30}"          # CloudWatch Logs retention (logRetentionDays)
#
# Network (first-deploy only; changing on an existing VPC is disruptive)
VPC_ID="${VPC_ID:-}"                                   # existing VPC (vpcId); empty = provision new (last load: new)
VPC_PRIVATE_SUBNET_IDS="${VPC_PRIVATE_SUBNET_IDS:-}"   # comma-sep (existingVpcPrivateSubnetIds; empty = none)
NEW_VPC_CIDR="${NEW_VPC_CIDR:-10.40.0.0/16}"           # CIDR for a provisioned VPC (newVpcCidr)
#
# Tagging / security scan
TAGS="${TAGS:-}"                                       # 'k1=v1,k2=v2' on all resources (empty = only cda:customer)
CDK_NAG="${CDK_NAG:-false}"                            # 'true' to run cdk-nag during synth (cdkNag)
#
# Analytics stack (deploy AFTER pipeline validated — see docs/ANALYTICS.md)
ENABLE_ANALYTICS="${ENABLE_ANALYTICS:-false}"          # deploy analytics stack (enableAnalyticsStack)
ANALYST_ROLE_ARNS="${ANALYST_ROLE_ARNS:-}"             # roles granted SELECT/DESCRIBE (empty = none)
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:-primary}"        # Athena workgroup (athenaWorkgroup)
RECON_TABLE_NAME="${RECON_TABLE_NAME:-cda_recon_results}" # recon results table (reconTableName)
#
# CDA Lifecycle Events (EAP — opt-in event-driven trigger)
LIFECYCLE_EVENTS_ENABLED="${LIFECYCLE_EVENTS_ENABLED:-false}" # enable (lifecycleEventsEnabled)
LIFECYCLE_PARTNER_EVENT_SOURCE="${LIFECYCLE_PARTNER_EVENT_SOURCE:-}" # partner source (empty = none)
LIFECYCLE_SOURCE_APP="${LIFECYCLE_SOURCE_APP:-}"       # cc/pc/bc or empty=all (lifecycleSourceApp)
#
# Escape hatch for any knob still not named above. Space-separated key=val
# pairs, passed verbatim as --context, e.g.:
#   EXTRA_CONTEXT="someNewKnob=value anotherKnob=42"
EXTRA_CONTEXT="${EXTRA_CONTEXT:-}"
#
# NOTE on spark EXECUTOR sizing: the actual per-class spark conf (driver/
# executor cores+memory, disk, shuffle partitions) is NOT a cdk.json knob —
# it lives in SIZE_CONFS in cdk/lib/orchestration-stack.ts. What you tune
# from here is which tables land in each class, via SIZE_THRESHOLD_*. To
# change the conf values themselves, edit SIZE_CONFS and redeploy.
# NOTE on cron: this runbook always deploys with scheduleEnabled=false (a
# cron firing mid-load would corrupt the clean state + timing). CRON_EXPRESSION
# just pre-sets the expression for when you later arm the schedule separately
# (see docs/DEPLOYMENT.md §6). scheduleEnabled is therefore NOT overridable
# here — it is always passed as false.

# ── Build toolchain ─────────────────────────────────────────────────────────
# JDK 21 is required to build (Gradle 9.x won't run on 11/17-). Resolved
# independently of the ambient JAVA_HOME (which may point at an older JDK).
# Override with JDK21_HOME if java_home can't find a 21 JVM.
JDK21="${JDK21_HOME:-$(/usr/libexec/java_home -v 21 2>/dev/null || echo /Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home)}"

###############################################################################
#                          ▲▲▲  END CONFIG  ▲▲▲                              #
# ─────────────────────────── DERIVED (do not edit) ───────────────────────── #
export AWS_PAGER=""
export AWS_REGION
# Auto-detect the account from the caller's AWS credentials when ACCOUNT is
# left empty (the default). Pinning ACCOUNT in CONFIG instead turns this into
# a guard: preflight refuses to run if the resolved caller account differs.
if [[ -z "${ACCOUNT}" ]]; then
  ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  [[ -n "${ACCOUNT}" && "${ACCOUNT}" != "None" ]] || {
    echo "ERROR: could not resolve AWS account. Set ACCOUNT=<id> or configure AWS credentials." >&2
    exit 1
  }
fi
SRC_BUCKET_ARN="arn:aws:s3:::${SRC_BUCKET}"
# Naming: CDK stack IDs are "<customer>-iceberg-X" (single prefix); the
# resources INSIDE them are "<customer>-<customer>-iceberg-X" (doubled,
# because customerName also prefixes within each stack).
STACK_NETWORK="${CUSTOMER}-iceberg-network"
STACK_WAREHOUSE="${CUSTOMER}-iceberg-warehouse"
STACK_EMR="${CUSTOMER}-iceberg-emr"
STACK_RUNTIME="${CUSTOMER}-iceberg-runtime"
STACK_ORCH="${CUSTOMER}-iceberg-orchestration"
STACK_ANALYTICS="${CUSTOMER}-iceberg-analytics"
RES_PREFIX="${CUSTOMER}-${CUSTOMER}-iceberg"            # doubled resource prefix
STATE_TABLE="${RES_PREFIX}-state"
TABLE_BUCKET_ARN="arn:aws:s3tables:${AWS_REGION}:${ACCOUNT}:bucket/${RES_PREFIX}-${ACCOUNT}-${AWS_REGION}"
ARTIFACT_BUCKET="${RES_PREFIX}-artifacts-${ACCOUNT}-${AWS_REGION}"
SM_ARN="arn:aws:states:${AWS_REGION}:${ACCOUNT}:stateMachine:${RES_PREFIX}-orchestration"
RECON_SM_ARN="arn:aws:states:${AWS_REGION}:${ACCOUNT}:stateMachine:${RES_PREFIX}-recon"
ORCH_RULE_NAME="${RES_PREFIX}-orchestration"           # EventBridge schedule rule
JAR="build/libs/cda-iceberg-client-1.0.jar"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUNDIR="${REPO_ROOT}/.load-timing"
mkdir -p "$RUNDIR"

cd "$REPO_ROOT"

say()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
ok()   { printf '\033[0;32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*"; }

# Emit --context args for every override that is set (non-empty). Unset
# overrides are omitted entirely, so cdk.json's default is inherited.
# Always emits customerName, the source bucket, scheduleEnabled=false (this
# runbook never arms the schedule), and the tolerated-failure setting.
context_args() {
  # Always-on: identity, source, namespace, manifest key, and the two
  # settings this runbook controls unconditionally (schedule off,
  # tolerated-failure). icebergNamespace + cdaManifestKey are passed so the
  # deployed Lambdas use the SAME values the script uses for its own calls.
  local -a a=(
    --context "customerName=${CUSTOMER}"
    --context "cdaSourceBucketArn=${SRC_BUCKET_ARN}"
    --context "cdaManifestKey=${MANIFEST_KEY}"
    --context "icebergNamespace=${NAMESPACE}"
    --context "scheduleEnabled=false"
    --context "mapToleratedFailurePercentage=${TOLERATED_FAILURE_PCT}"
  )
  # Pair each cdk.json key with its script override var; add only if set
  # (empty = inherit cdk.json default).
  local pairs=(
    "cronExpression=${CRON_EXPRESSION}"
    "reconCronExpression=${RECON_CRON_EXPRESSION}"
    "reconScheduleEnabled=${RECON_SCHEDULE_ENABLED}"
    "reconMapConcurrency=${RECON_MAP_CONCURRENCY}"
    "mapStateConcurrency=${MAP_CONCURRENCY}"
    "notificationEmails=${NOTIFICATION_EMAILS}"
    "tablesToExclude=${TABLES_TO_EXCLUDE}"
    "columnsToExclude=${COLUMNS_TO_EXCLUDE}"
    "sizeThresholdMedium=${SIZE_THRESHOLD_MEDIUM}"
    "sizeThresholdLarge=${SIZE_THRESHOLD_LARGE}"
    "emrMaxVcpu=${EMR_MAX_VCPU}"
    "emrMaxMemory=${EMR_MAX_MEMORY}"
    "emrMaxDisk=${EMR_MAX_DISK}"
    "emrIdleTimeoutMinutes=${EMR_IDLE_TIMEOUT_MIN}"
    "emrServerlessReleaseLabel=${EMR_RELEASE_LABEL}"
    "emrInitialDriverCount=${EMR_INITIAL_DRIVERS}"
    "emrInitialExecutorCount=${EMR_INITIAL_EXECUTORS}"
    "snapshotRetentionDays=${SNAPSHOT_RETENTION_DAYS}"
    "logRetentionDays=${LOG_RETENTION_DAYS}"
    "vpcId=${VPC_ID}"
    "existingVpcPrivateSubnetIds=${VPC_PRIVATE_SUBNET_IDS}"
    "newVpcCidr=${NEW_VPC_CIDR}"
    "tags=${TAGS}"
    "cdkNag=${CDK_NAG}"
    "enableAnalyticsStack=${ENABLE_ANALYTICS}"
    "analystRoleArns=${ANALYST_ROLE_ARNS}"
    "athenaWorkgroup=${ATHENA_WORKGROUP}"
    "reconTableName=${RECON_TABLE_NAME}"
    "lifecycleEventsEnabled=${LIFECYCLE_EVENTS_ENABLED}"
    "lifecyclePartnerEventSource=${LIFECYCLE_PARTNER_EVENT_SOURCE}"
    "lifecycleSourceApp=${LIFECYCLE_SOURCE_APP}"
  )
  local p
  for p in "${pairs[@]}"; do
    [[ "${p#*=}" != "" ]] && a+=(--context "$p")
  done
  # Escape-hatch: arbitrary key=val pairs passed verbatim.
  local kv
  for kv in $EXTRA_CONTEXT; do
    [[ "$kv" == *=* ]] && a+=(--context "$kv")
  done
  printf '%s\n' "${a[@]}"
}

# True if the orchestration stack is deployed (i.e. NOT a greenfield account).
stacks_exist() {
  aws cloudformation describe-stacks --stack-name "$STACK_ORCH" >/dev/null 2>&1
}

# True if the source bucket needs no Guidewire allow-list round-trip — i.e.
# it's already cross-account-readable by this account (e.g. a same-account
# test bucket, or one Guidewire has already allow-listed). By default the
# list is empty, so bootstrap pauses for the allow-list round-trip on ANY
# source bucket. Add bucket name(s) to NO_ALLOWLIST to skip that pause.
NO_ALLOWLIST="${NO_ALLOWLIST:-}"
src_is_own_account() {
  case " $NO_ALLOWLIST " in *" $SRC_BUCKET "*) return 0;; *) return 1;; esac
}

preflight() {
  say "Pre-flight"
  local who; who=$(aws sts get-caller-identity --query Account --output text)
  [[ "$who" == "$ACCOUNT" ]] || { echo "WRONG ACCOUNT: $who (expected $ACCOUNT)"; exit 1; }
  ok "account $who, region $AWS_REGION"
  if stacks_exist; then
    ok "orchestration stack deployed (existing-account mode)"
    # Refuse to run if the schedule is armed (a cron trigger mid-load corrupts timing + clean state).
    local st
    st=$(aws events list-rules --query "Rules[?Name=='${ORCH_RULE_NAME}'].State" --output text)
    [[ "$st" == "DISABLED" ]] || { echo "Ingest schedule is $st — disable it first (cdk deploy --context scheduleEnabled=false)"; exit 1; }
    ok "ingest schedule DISABLED"
    local running
    running=$(aws stepfunctions list-executions --state-machine-arn "$SM_ARN" --status-filter RUNNING --query "length(executions)" --output text)
    [[ "$running" == "0" ]] || { echo "$running execution(s) RUNNING — wait for them to finish"; exit 1; }
    ok "no executions running"
  else
    ok "no orchestration stack yet (greenfield mode — use 'bootstrap')"
  fi
}

confirm_destructive() {
  local tables cursors
  tables=$(aws s3tables list-tables --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NAMESPACE" --query "length(tables)" --output text 2>/dev/null || echo "?")
  cursors=$(aws dynamodb scan --table-name "$STATE_TABLE" --select COUNT --query Count --output text 2>/dev/null || echo "?")
  warn "About to DELETE $tables Iceberg tables and $cursors cursors. This is NOT reversible."
  read -r -p "Type 'WIPE' to proceed: " a
  [[ "$a" == "WIPE" ]] || { echo "aborted"; exit 1; }
}

# ── Phase 0: greenfield bootstrap (fresh account, nothing deployed) ───────
# Correct ordering for a brand-new account:
#   1. build jar
#   2. deploy base infra WITHOUT a source bucket → creates the artifact
#      bucket + EMR job role (orchestration stack is skipped when
#      cdaSourceBucketArn is empty — see cdk/bin/app.ts)
#   3. upload the jar (the bucket now exists)
#   4. Guidewire allow-list round-trip (skipped for the synthetic sample)
#   5. deploy again WITH the source bucket → creates orchestration + recon
bootstrap() {
  if stacks_exist; then
    warn "orchestration stack already exists — this is NOT a greenfield account."
    warn "Use 'ship' (+ 'wipe' for a clean reload) instead. Aborting bootstrap."
    exit 1
  fi
  say "Phase 0a — build jar + stage runtime deps"
  JAVA_HOME="$JDK21" ./gradlew clean test shadowJar stageRuntimeDeps
  ok "jar built: $(ls -la "$JAR" | awk '{print $5" bytes"}')"

  say "Phase 0b — deploy base infrastructure (no source bucket yet)"
  # No cdaSourceBucketArn → app.ts skips the orchestration + analytics
  # stacks, deploying only network/warehouse/emr/runtime.
  ( cd cdk && npm install --silent && npx cdk bootstrap "aws://${ACCOUNT}/${AWS_REGION}" >/dev/null 2>&1 || true
    npx cdk deploy --all --require-approval never --context customerName="$CUSTOMER" )
  ok "base infra deployed (network, warehouse, emr, runtime)"

  local role_arn
  role_arn=$(aws cloudformation describe-stacks --stack-name "$STACK_EMR" \
    --query "Stacks[0].Outputs[?OutputKey=='EmrJobRoleArn'].OutputValue" --output text)

  say "Phase 0c — upload jar + runtime deps"
  aws s3 cp "$JAR" "s3://${ARTIFACT_BUCKET}/jars/" --only-show-errors
  # Iceberg + S3 Tables runtime jars → deps/. EMR references these with
  # --jars at job start instead of resolving --packages from Maven Central
  # (Maven resolution flakes under Map concurrency — see orchestration-stack).
  aws s3 sync build/runtime-libs/ "s3://${ARTIFACT_BUCKET}/deps/" --exclude '*' --include '*.jar' --only-show-errors
  ok "jar + runtime deps uploaded to s3://${ARTIFACT_BUCKET}/"

  say "Phase 0d — source-bucket access"
  if src_is_own_account; then
    ok "source is the synthetic sample bucket (cross-account read already works) — no allow-list needed"
  else
    warn "Cross-account source bucket detected: $SRC_BUCKET"
    warn "Send Guidewire this EMR job role ARN to allow-list on the source bucket:"
    echo "      $role_arn"
    read -r -p "  Press ENTER once Guidewire confirms the allow-list (or Ctrl-C to stop): " _
  fi

  say "Phase 0e — deploy orchestration (wire source bucket, schedule OFF, tolerated-failure=${TOLERATED_FAILURE_PCT}%)"
  local -a CTX=(); while IFS= read -r _a; do CTX+=("$_a"); done < <(context_args)
  ( cd cdk && npx cdk deploy --all --require-approval never "${CTX[@]}" )
  ok "orchestration + recon deployed. Greenfield bootstrap complete — run 'run' next."
}

# ── Phase 1: ship current code (existing account) ─────────────────────────
ship() {
  if ! stacks_exist; then
    warn "No orchestration stack — this looks like a fresh account. Use 'bootstrap' instead."
    exit 1
  fi
  say "Phase 1 — build + upload jar, deploy CDK (tolerated-failure=${TOLERATED_FAILURE_PCT}%)"
  JAVA_HOME="$JDK21" ./gradlew clean test shadowJar stageRuntimeDeps
  ok "jar built: $(ls -la "$JAR" | awk '{print $5" bytes"}')"
  aws s3 cp "$JAR" "s3://${ARTIFACT_BUCKET}/jars/" --only-show-errors
  # Iceberg + S3 Tables runtime jars → deps/. EMR references these with
  # --jars at job start instead of resolving --packages from Maven Central
  # (Maven resolution flakes under Map concurrency — see orchestration-stack).
  aws s3 sync build/runtime-libs/ "s3://${ARTIFACT_BUCKET}/deps/" --exclude '*' --include '*.jar' --only-show-errors
  ok "jar + runtime deps uploaded to s3://${ARTIFACT_BUCKET}/"
  local -a CTX=(); while IFS= read -r _a; do CTX+=("$_a"); done < <(context_args)
  echo "  context: ${CTX[*]}"
  ( cd cdk && npm install --silent && npx cdk deploy --all --require-approval never "${CTX[@]}" )
  ok "cdk deploy complete (schedule stays disabled)"
}

# ── Phase 2: wipe warehouse + cursors ────────────────────────────────────
wipe() {
  say "Phase 2 — WIPE warehouse tables + cursors"
  confirm_destructive

  echo "  Listing all tables…"
  aws s3tables list-tables --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NAMESPACE" \
    --query "tables[].name" --output text | tr '\t' '\n' > "$RUNDIR/tables-to-delete.txt"
  local n; n=$(wc -l < "$RUNDIR/tables-to-delete.txt" | tr -d ' ')
  warn "deleting $n tables (parallel x8)…"
  # Parallel delete; tolerate already-gone.
  cat "$RUNDIR/tables-to-delete.txt" | xargs -P 8 -I{} bash -c '
    aws s3tables delete-table --table-bucket-arn "'"$TABLE_BUCKET_ARN"'" \
      --namespace "'"$NAMESPACE"'" --name "{}" >/dev/null 2>&1 || true'
  local left; left=$(aws s3tables list-tables --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NAMESPACE" --query "length(tables)" --output text)
  ok "tables remaining: $left"

  echo "  Clearing cursors…"
  # Scan all partition keys, batch-delete in chunks of 25.
  aws dynamodb scan --table-name "$STATE_TABLE" --projection-expression "tableName" \
    --query "Items[].tableName.S" --output text | tr '\t' '\n' > "$RUNDIR/cursors-to-delete.txt"
  while read -r key; do
    [[ -z "$key" ]] && continue
    aws dynamodb delete-item --table-name "$STATE_TABLE" \
      --key "{\"tableName\":{\"S\":\"$key\"}}" >/dev/null 2>&1 || true
  done < "$RUNDIR/cursors-to-delete.txt"
  local cleft; cleft=$(aws dynamodb scan --table-name "$STATE_TABLE" --select COUNT --query Count --output text)
  ok "cursors remaining: $cleft"
  [[ "$left" == "0" && "$cleft" == "0" ]] || { echo "WARN: wipe incomplete (tables=$left cursors=$cleft) — re-run wipe"; exit 1; }
  ok "warehouse + cursors clean"
}

# ── Phase 3: trigger one full load and time it ───────────────────────────
run() {
  say "Phase 3 — trigger full load + time it"

  # IMPORTANT: this state machine has no ToleratedFailurePercentage on its
  # per-table Map, so a SINGLE failed table makes the whole execution end
  # FAILED — even if hundreds of tables loaded fine. With the synthetic
  # sample's quirky gwcbi___operation values, some tables routinely fail.
  # So we treat FAILED as "load finished, inspect per-table report", NOT as
  # a hard error — and ALWAYS print the wall-clock + point you at `report`.
  warn "Map has no tolerated-failure setting: one bad table => execution status FAILED."
  warn "That's expected with the synthetic sample. Use 'report' to see per-table success/fail."

  local name="load-$(date +%Y%m%d-%H%M%S)"
  local exec_arn
  exec_arn=$(aws stepfunctions start-execution --state-machine-arn "$SM_ARN" --name "$name" \
    --query executionArn --output text)
  echo "$exec_arn" > "$RUNDIR/last-exec-arn.txt"
  ok "started: $name"
  echo "  exec ARN saved to $RUNDIR/last-exec-arn.txt (so 'report' can scope to this load)"
  echo "  Polling every 60s (full load is typically a few hours)…"

  local status
  while :; do
    # Don't let a transient describe-execution blip kill the whole run
    # (set -e would otherwise abort and we'd lose live polling).
    status=$(aws stepfunctions describe-execution --execution-arn "$exec_arn" \
               --query status --output text 2>/dev/null || echo "RUNNING")
    printf '\r  status=%s  wallclock-now=%s   ' "$status" "$(date -u +%H:%M:%S)"
    case "$status" in
      SUCCEEDED|FAILED|TIMED_OUT|ABORTED) echo; break ;;
      RUNNING|PENDING_REDRIVE|*) : ;;   # keep polling
    esac
    sleep 60
  done

  # Authoritative start/stop from the execution record. These are ISO-8601
  # strings (NOT epoch) in --output text, so parse with fromisoformat.
  local s e
  read -r s e <<<"$(aws stepfunctions describe-execution --execution-arn "$exec_arn" \
    --query "[startDate,stopDate]" --output text)"
  python3 - "$s" "$e" "$status" "$exec_arn" <<'PY' | tee "$RUNDIR/summary.txt"
import sys, datetime
s, e, status, arn = sys.argv[1:5]
def parse(x):
    if x in ("", "None"): return None
    try: return datetime.datetime.fromtimestamp(float(x))      # epoch fallback
    except ValueError: return datetime.datetime.fromisoformat(x)  # ISO-8601 (actual)
ds, de = parse(s), parse(e)
print("\n=== FULL CLEAN LOAD TIMING ===")
print(f"status   : {status}")
print(f"start    : {ds}")
print(f"stop     : {de}")
if ds and de:
    dur = de - ds
    print(f"wallclock: {dur}  ({dur.total_seconds():.0f}s)")
print(f"exec     : {arn}")
if status != "SUCCEEDED":
    print("\nNOTE: status != SUCCEEDED. With this state machine, one failed")
    print("table fails the whole execution — this does NOT mean the load")
    print("didn't run. Run 'report' to see how many tables actually succeeded.")
PY
  ok "wall-clock summary saved to $RUNDIR/summary.txt — now run: $0 report"
}

# ── Phase 4: per-table EMR timing report ─────────────────────────────────
report() {
  say "Phase 4 — per-table EMR job timing"
  local app_id
  app_id=$(aws cloudformation describe-stacks --stack-name "$STACK_EMR" \
    --query "Stacks[0].Outputs[?OutputKey=='ServerlessApplicationId'].OutputValue" --output text)

  # Window: only count jobs created at/after this load's execution start, so
  # a re-report doesn't fold in earlier loads. startDate from Step Functions
  # is epoch-float; EMR createdAt is ISO-8601 — the Python below normalizes
  # both to epoch. Falls back to "all job runs" if no recorded start.
  local since=""
  if [[ -f "$RUNDIR/last-exec-arn.txt" ]]; then
    local exec_arn; exec_arn=$(cat "$RUNDIR/last-exec-arn.txt")
    since=$(aws stepfunctions describe-execution --execution-arn "$exec_arn" \
      --query startDate --output text 2>/dev/null || echo "")
    [[ -n "$since" && "$since" != "None" ]] && ok "scoping to jobs since load start (epoch $since)"
  fi
  [[ -z "$since" || "$since" == "None" ]] && warn "no recorded load start — reporting on ALL job runs"

  # Paginate the FULL job-run list (a full load is 717 jobs; the API caps
  # each page, so loop on nextToken). Collect raw JSON for robust stats.
  echo "  Fetching all EMR job runs (paginated)…"
  local token="" page=0
  : > "$RUNDIR/jobs.ndjson"
  while :; do
    local out
    if [[ -z "$token" ]]; then
      out=$(aws emr-serverless list-job-runs --application-id "$app_id" --max-results 50 --output json)
    else
      out=$(aws emr-serverless list-job-runs --application-id "$app_id" --max-results 50 --next-token "$token" --output json)
    fi
    echo "$out" | python3 -c "import sys,json;[print(json.dumps(j)) for j in json.load(sys.stdin).get('jobRuns',[])]" >> "$RUNDIR/jobs.ndjson"
    token=$(echo "$out" | python3 -c "import sys,json;print(json.load(sys.stdin).get('nextToken',''))")
    page=$((page+1)); printf '\r  page %d (%d jobs so far)…   ' "$page" "$(wc -l < "$RUNDIR/jobs.ndjson")"
    [[ -z "$token" ]] && break
  done
  echo

  # Compute stats from JSON (no fragile table-scraping). Filter to the
  # load window, build a per-table CSV, and print percentiles.
  python3 - "$RUNDIR/jobs.ndjson" "$since" "$RUNDIR/per-table-timing.csv" <<'PY' | tee "$RUNDIR/timing-summary.txt"
import sys, json, datetime

ndjson, since, csv_out = sys.argv[1], sys.argv[2], sys.argv[3]

def to_epoch(v):
    """Normalize an EMR/SFN timestamp to epoch seconds. Accepts epoch
    int/float (Step Functions --output text) or ISO-8601 (EMR JSON)."""
    if v in (None, "", "None"):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        pass
    try:
        return datetime.datetime.fromisoformat(v).timestamp()
    except ValueError:
        return None

since_ts = to_epoch(since)
rows = []
for line in open(ndjson):
    line = line.strip()
    if not line:
        continue
    j = json.loads(line)
    created = to_epoch(j.get("createdAt"))
    updated = to_epoch(j.get("updatedAt"))
    if since_ts and created and created < since_ts:
        continue
    # Duration: prefer the reported field; fall back to updatedAt-createdAt
    # (the list API often returns null for totalExecutionDurationSeconds).
    sec = j.get("totalExecutionDurationSeconds")
    if not sec and created and updated:
        sec = max(0, round(updated - created))
    rows.append({"name": j.get("name",""), "state": j.get("state",""), "sec": int(sec or 0)})

# Per-table CSV, slowest first (the long pole drives wall-clock).
rows.sort(key=lambda r: r["sec"], reverse=True)
with open(csv_out, "w") as f:
    f.write("name,state,seconds\n")
    for r in rows:
        f.write(f'{r["name"]},{r["state"]},{r["sec"]}\n')

n = len(rows)
states = {}
for r in rows:
    states[r["state"]] = states.get(r["state"], 0) + 1
print("\n=== PER-TABLE EMR TIMING ===")
print(f"jobs in window : {n}")
print("states         : " + ", ".join(f"{k}={v}" for k,v in sorted(states.items())))

# Percentiles over SUCCESS jobs only — a CANCELLED/FAILED job's
# createdAt→updatedAt gap is wall-clock-to-termination, not a real
# table-load time, and skews the max badly.
succ = sorted(r["sec"] for r in rows if r["state"] == "SUCCESS")
if succ:
    m = len(succ)
    pct = lambda p: succ[min(m-1, int(p/100*m))]
    print(f"\nSUCCESS jobs only ({m}); duration derived from createdAt→updatedAt when API duration is null:")
    print(f"  min / p50 / p90 / p99 / max (s): "
          f"{succ[0]} / {pct(50)} / {pct(90)} / {pct(99)} / {succ[-1]}")
    print(f"  sum compute-seconds: {sum(succ)}  (~{sum(succ)/3600:.1f} compute-hours, parallelized across the fan-out)")
    print(f"\n  Top 10 slowest SUCCESS tables:")
    for r in [x for x in rows if x["state"] == "SUCCESS"][:10]:
        print(f"    {r['sec']:>6}s  {r['name']}")

failed = [r for r in rows if r["state"] != "SUCCESS"]
if failed:
    print(f"\nNon-SUCCESS jobs ({len(failed)}) — investigate before trusting the load:")
    for r in failed[:30]:
        print(f"  {r['state']:<12} {r['name']}")
print(f"\nFull per-table CSV (all states, slowest first): {csv_out}")
PY
}

# ── Phase 4b: trigger Tier D reconciliation and wait ──────────────────────
# Runs the separate cda-iceberg-recon state machine: per-table --recon-only
# Spark jobs that run the 4 merged-vs-raw invariants (snapshot-pinned, so
# they're consistent even if ingest runs concurrently) and write findings to
# the cda_recon_results Iceberg table.
recon() {
  say "Phase 4b — trigger Tier D reconciliation"

  # Guard: reconciling a half-loaded warehouse flags every not-yet-loaded
  # table as a mismatch. Warn if an ingest execution is still RUNNING.
  local ingest_running
  ingest_running=$(aws stepfunctions list-executions --state-machine-arn "$SM_ARN" \
                     --status-filter RUNNING --query "length(executions)" --output text 2>/dev/null || echo 0)
  if [[ "$ingest_running" != "0" ]]; then
    warn "An ingest execution is still RUNNING — recon would reconcile a"
    warn "partially-loaded warehouse and flag not-yet-loaded tables as MISMATCH."
    read -r -p "  Run recon anyway? [yes/NO] " a; [[ "$a" == "yes" ]] || { echo "aborted"; exit 1; }
  fi

  # On the synthetic sample, Tier D legitimately reports MISMATCH because
  # gwcbi___operation values don't follow real-CDA {0,1,2,4} semantics.
  warn "On synthetic sample data, MISMATCH is EXPECTED (operation-code quirk),"
  warn "not a pipeline bug. On real CDA data all 4 invariants should be OK."

  local name="recon-$(date +%Y%m%d-%H%M%S)"
  local exec_arn
  exec_arn=$(aws stepfunctions start-execution --state-machine-arn "$RECON_SM_ARN" --name "$name" \
    --query executionArn --output text)
  echo "$exec_arn" > "$RUNDIR/last-recon-arn.txt"
  ok "started: $name"
  echo "  Polling every 60s (Tier D scans raw+merged per table)…"

  local status
  while :; do
    status=$(aws stepfunctions describe-execution --execution-arn "$exec_arn" \
               --query status --output text 2>/dev/null || echo "RUNNING")
    printf '\r  status=%s  wallclock-now=%s   ' "$status" "$(date -u +%H:%M:%S)"
    case "$status" in
      SUCCEEDED|FAILED|TIMED_OUT|ABORTED) echo; break ;;
      RUNNING|PENDING_REDRIVE|*) : ;;
    esac
    sleep 60
  done
  ok "recon execution finished: $status"

  # Summarize the Tier D findings written by THIS run, via Athena.
  recon_findings
}

# Query cda_recon_results (Tier D) via Athena and print a status breakdown.
recon_findings() {
  say "Tier D findings (from cda_recon_results)"
  local catalog="s3tablescatalog/${RES_PREFIX}-${ACCOUNT}-${AWS_REGION}"
  local fqtn="\"${catalog}\".\"${NAMESPACE}\".\"${RECON_TABLE_NAME}\""
  local logs_bucket="${RES_PREFIX}-logs-${ACCOUNT}-${AWS_REGION}"
  local out="s3://${logs_bucket}/athena-results/"
  local sql="SELECT check_name, status, count(*) AS n FROM ${fqtn} WHERE tier='D' GROUP BY check_name, status ORDER BY check_name, status"

  local qid
  qid=$(aws athena start-query-execution --query-string "$sql" \
          --work-group "${ATHENA_WORKGROUP:-primary}" \
          --result-configuration "OutputLocation=${out}" \
          --query QueryExecutionId --output text 2>/dev/null || echo "")
  if [[ -z "$qid" || "${qid:0:6}" == "aws: [" ]]; then
    warn "Couldn't start Athena query (Glue federation may not be set up — run '$0 analytics')."
    echo "  Query the recon table directly once federation exists:"
    echo "    SELECT check_name,status,count(*) FROM \"${catalog}\".\"${NAMESPACE}\".\"${RECON_TABLE_NAME}\""
    echo "    WHERE tier='D' GROUP BY 1,2 ORDER BY 1,2;"
    return 0
  fi
  local qs="RUNNING"
  for _ in $(seq 1 30); do
    qs=$(aws athena get-query-execution --query-execution-id "$qid" --query "QueryExecution.Status.State" --output text 2>/dev/null || echo RUNNING)
    case "$qs" in SUCCEEDED|FAILED|CANCELLED) break;; esac
    sleep 3
  done
  if [[ "$qs" != "SUCCEEDED" ]]; then
    warn "Athena query did not succeed ($qs). Query cda_recon_results manually."
    return 0
  fi
  # Rows come back flattened (check_name, status, n) triples after the header.
  aws athena get-query-results --query-execution-id "$qid" \
    --query "ResultSet.Rows[].Data[].VarCharValue" --output text 2>/dev/null \
    | python3 -c "
import sys
vals=sys.stdin.read().split('\t')
vals=[v.strip() for v in vals if v.strip()!='']
# drop the 3 header cells, then group into (check,status,n) triples
body=vals[3:]
rows=[body[i:i+3] for i in range(0,len(body)-2,3)]
if not rows:
    print('  (no Tier D rows yet — recon may not have written, or table empty)')
else:
    print(f'  {\"check\":<22}{\"status\":<14}{\"count\":>8}')
    for c,s,n in rows: print(f'  {c:<22}{s:<14}{n:>8}')
    mm=sum(int(n) for c,s,n in rows if s!='OK')
    print(f'\n  non-OK rows: {mm}'+('  (expected on synthetic data — operation-code quirk)' if mm else ''))
"
  echo "  Detail (non-OK):  SELECT check_name,table_name,actual AS violations FROM <catalog>.${NAMESPACE}.${RECON_TABLE_NAME} WHERE tier='D' AND status<>'OK';"
}

# ── Phase 5: deploy the optional AnalyticsStack (correct ordering) ─────────
# AnalyticsStack (Glue federation + Lake Formation grants + Athena saved
# queries + dashboard + alarms) is deployed AFTER the pipeline is validated,
# not during a load — its grants/queries only make sense once tables exist.
# This phase enforces that ordering with pre-flight gates, then deploys with
# enableAnalyticsStack=true and verifies. See docs/ANALYTICS.md.
analytics() {
  say "Phase 5 — deploy AnalyticsStack (after pipeline validation)"

  # ── Gate 1: all 5 base stacks must be deployed and healthy ──
  if ! stacks_exist; then
    echo "Base pipeline not deployed (no orchestration stack). Run 'ship'/'bootstrap' + a load first."; exit 1
  fi
  local s st
  for s in "$STACK_NETWORK" "$STACK_WAREHOUSE" "$STACK_EMR" "$STACK_RUNTIME" "$STACK_ORCH"; do
    st=$(aws cloudformation describe-stacks --stack-name "$s" --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo "MISSING")
    case "$st" in
      CREATE_COMPLETE|UPDATE_COMPLETE|UPDATE_ROLLBACK_COMPLETE) ok "base stack OK: $s ($st)";;
      *) echo "base stack $s is '$st' — fix it before deploying analytics"; exit 1;;
    esac
  done

  # ── Gate 2: pipeline has actually run (>=1 successful ingest) ──
  local ok_exec
  ok_exec=$(aws stepfunctions list-executions --state-machine-arn "$SM_ARN" \
              --status-filter SUCCEEDED --max-results 1 --query "length(executions)" --output text 2>/dev/null || echo 0)
  if [[ "$ok_exec" == "0" ]]; then
    warn "No SUCCEEDED ingest execution found. AnalyticsStack federation/queries"
    warn "will target empty/absent tables. Recommended: run a load first."
    read -r -p "  Deploy analytics anyway? [yes/NO] " a; [[ "$a" == "yes" ]] || { echo "aborted"; exit 1; }
  else
    ok "pipeline has at least one SUCCEEDED ingest execution"
  fi

  # ── Gate 3: warehouse has tables (saved queries/dashboards won't be empty) ──
  local tbl_present
  tbl_present=$(aws s3tables list-tables --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace "$NAMESPACE" \
                  --query "length(tables)" --output text 2>/dev/null || echo 0)
  if [[ "$tbl_present" -gt 0 ]]; then ok "warehouse has $tbl_present table objects"
  else warn "warehouse namespace looks empty — dashboards/saved queries will show no data"; fi

  # ── Gate 4: analyst roles supplied (else grants are a no-op) ──
  if [[ -z "$ANALYST_ROLE_ARNS" ]]; then
    warn "ANALYST_ROLE_ARNS is empty — federation + dashboards deploy, but NO ONE"
    warn "is granted query access. Set ANALYST_ROLE_ARNS=arn:...:role/Analyst to grant."
    read -r -p "  Deploy without analyst grants? [yes/NO] " a; [[ "$a" == "yes" ]] || { echo "aborted"; exit 1; }
  else
    ok "analyst roles to grant: $ANALYST_ROLE_ARNS"
  fi

  # ── Gate 5: Lake Formation admin (strict-mode accounts) — informational ──
  # In strict LF mode the deployer must be an LF admin or the grant
  # custom-resource fails. We surface deployer + current admins so the
  # operator can confirm; see docs/ANALYTICS.md 2.3 to add the admin.
  local me admins
  me=$(aws sts get-caller-identity --query Arn --output text)
  admins=$(aws lakeformation get-data-lake-settings --query 'DataLakeSettings.DataLakeAdmins[].DataLakePrincipalIdentifier' --output text 2>/dev/null || echo "(unreadable)")
  echo "  deployer:  $me"
  echo "  LF admins: ${admins:-(none — account may be in IAM_ALLOWED_PRINCIPALS mode)}"
  warn "If LF is in strict mode and the deployer is not an admin above, the grant"
  warn "step will fail — add it once per docs/ANALYTICS.md 2.3, then re-run."

  # ── Deploy ONLY the analytics stack (enableAnalyticsStack=true forces it
  # into the app; targeting $STACK_ANALYTICS avoids re-deploying the base 5).
  # Force ENABLE_ANALYTICS=true so context_args itself emits the right value
  # (no reliance on duplicate-key --context ordering). ──
  say "Deploying $STACK_ANALYTICS (enableAnalyticsStack=true)"
  ENABLE_ANALYTICS=true
  local -a CTX=(); while IFS= read -r _a; do CTX+=("$_a"); done < <(context_args)
  ( cd cdk && npm install --silent && \
    npx cdk deploy "$STACK_ANALYTICS" --require-approval never "${CTX[@]}" )

  ok "analytics stack deployed: $STACK_ANALYTICS"
  echo "  Verify federation:"
  echo "    aws glue get-catalog --catalog-id ${ACCOUNT}:s3tablescatalog/${RES_PREFIX}-${ACCOUNT}-${AWS_REGION}"
  echo "  Verify grants / dashboard / alarms: see docs/ANALYTICS.md §3.2–3.5"
}

case "${1:-}" in
  bootstrap)  preflight; bootstrap ;;
  ship)       preflight; ship ;;
  wipe)       preflight; wipe ;;
  run)        preflight; run ;;
  report)     report ;;
  recon)      preflight; recon ;;
  analytics)  preflight; analytics ;;
  all)
    preflight
    if stacks_exist; then
      # Existing account: ship new code, wipe to zero state, reload.
      ship; wipe
    else
      # Greenfield: stand everything up first (no wipe needed — nothing exists).
      bootstrap
    fi
    run; report ;;
  *) echo "usage: $0 {bootstrap|ship|wipe|run|report|recon|analytics|all}"; exit 2 ;;
esac
