#!/usr/bin/env bash
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0

#
# rebuild-table.sh — drop one table's _raw and _merged in S3 Tables, clear
# its DynamoDB cursor, and trigger a re-ingest from scratch.
#
# Use case:
#   - CDA reset on one table (DEPLOYMENT.md §13.1)
#   - Schema-incompatible failure (§13.2)
#   - Tier D persistent mismatch (§13.3)
#
# DESTRUCTIVE: deletes both Iceberg tables for the named CDA table.
# The pipeline rebuilds them from CDA on the next ingest run. Make sure
# you actually want this — once the data is gone, recovery requires
# CDA-side state.
#
# Required env vars:
#   TABLE_BUCKET_ARN   S3 Tables bucket ARN
#   STATE_TABLE        DynamoDB cursor table name
#   INGEST_SM_ARN      Ingest state machine ARN (optional — if set,
#                      script kicks off a manual run after cleanup)
#
# Usage:
#   ./scripts/rebuild-table.sh <table-name> [namespace]
#
# Example:
#   ./scripts/rebuild-table.sh cc_appcritcoveragetype cda

set -euo pipefail

TABLE="${1:?missing argument: table name}"
NAMESPACE="${2:-cda}"

: "${TABLE_BUCKET_ARN:?required env var TABLE_BUCKET_ARN not set}"
: "${STATE_TABLE:?required env var STATE_TABLE not set}"

echo "About to:"
echo "  1. Drop $NAMESPACE.${TABLE}_raw"
echo "  2. Drop $NAMESPACE.${TABLE}_merged"
echo "  3. Delete DynamoDB row for $TABLE"
if [[ -n "${INGEST_SM_ARN:-}" ]]; then
  echo "  4. Start a manual ingest execution"
fi
read -r -p "Proceed? [yes/NO] " confirm
[[ "$confirm" == "yes" ]] || { echo "aborted"; exit 1; }

echo "Dropping ${TABLE}_raw..."
aws s3tables delete-table \
  --table-bucket-arn "$TABLE_BUCKET_ARN" \
  --namespace "$NAMESPACE" \
  --name "${TABLE}_raw" 2>&1 || echo "  (already absent or failed — continuing)"

echo "Dropping ${TABLE}_merged..."
aws s3tables delete-table \
  --table-bucket-arn "$TABLE_BUCKET_ARN" \
  --namespace "$NAMESPACE" \
  --name "${TABLE}_merged" 2>&1 || echo "  (already absent or failed — continuing)"

echo "Deleting cursor for $TABLE..."
aws dynamodb delete-item \
  --table-name "$STATE_TABLE" \
  --key "{\"tableName\":{\"S\":\"$TABLE\"}}"

if [[ -n "${INGEST_SM_ARN:-}" ]]; then
  EXEC=$(aws stepfunctions start-execution \
    --state-machine-arn "$INGEST_SM_ARN" \
    --name "rebuild-$TABLE-$(date +%s)" \
    --query 'executionArn' --output text)
  echo "Started ingest: $EXEC"
  echo "Watch in console; on a fresh deploy a single-table rebuild typically"
  echo "takes 1-15 min depending on table size."
else
  echo "Done. Re-ingest will happen on the next scheduled run, or trigger"
  echo "manually with: aws stepfunctions start-execution --state-machine-arn <arn>"
fi
