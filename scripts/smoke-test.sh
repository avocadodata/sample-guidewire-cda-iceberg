#!/usr/bin/env bash
#
# smoke-test.sh — submit one EMR Serverless ingest job for a single
# CDA table and watch it to terminal state.
#
# Use case: validating a fresh deploy or a new jar version. Prints the
# job-run id, polls until SUCCESS / FAILED, and exits non-zero if the
# job didn't succeed.
#
# Required env vars (set by your shell or a .env file):
#   APP_ID             EMR Serverless application id
#   ROLE_ARN           EMR job role ARN (cfn output: EmrJobRoleArn)
#   ARTIFACT_BUCKET    S3 bucket holding the jar (cfn output: ArtifactBucket)
#   LOGS_BUCKET        S3 bucket for monitoring logs (cfn output: LogsBucket)
#   TABLE_BUCKET_ARN   S3 Tables bucket ARN (cfn output: TableBucketArn)
#   SOURCE_BUCKET      CDA source bucket NAME (not ARN)
#   MANIFEST_KEY       Path to manifest.json inside SOURCE_BUCKET (default: manifest.json)
#   STATE_TABLE        DynamoDB cursor table name (cfn output: LaunchConditionStateTable)
#
# Usage:
#   ./scripts/smoke-test.sh <table-name> [namespace]
#
# Example:
#   ./scripts/smoke-test.sh cctl_accidenttype cda

set -euo pipefail

TABLE="${1:?missing argument: table name}"
NAMESPACE="${2:-cda}"

: "${APP_ID:?required env var APP_ID not set}"
: "${ROLE_ARN:?required env var ROLE_ARN not set}"
: "${ARTIFACT_BUCKET:?required env var ARTIFACT_BUCKET not set}"
: "${LOGS_BUCKET:?required env var LOGS_BUCKET not set}"
: "${TABLE_BUCKET_ARN:?required env var TABLE_BUCKET_ARN not set}"
: "${SOURCE_BUCKET:?required env var SOURCE_BUCKET not set}"
: "${STATE_TABLE:?required env var STATE_TABLE not set}"
MANIFEST_KEY="${MANIFEST_KEY:-manifest.json}"

# Spark conf — matches the medium-class profile used by the orchestration
# state machine. If your table is very large (>1B rows), bump
# spark.emr-serverless.executor.disk to 400G and shuffle.partitions to 1000.
PARAMS="--class gw.cda.iceberg.IcebergIngest \
  --packages software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.5,org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.s3tables=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.s3tables.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
  --conf spark.sql.catalog.s3tables.warehouse=$TABLE_BUCKET_ARN \
  --conf spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED \
  --conf spark.executor.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED \
  --conf spark.emr-serverless.executor.disk=200G \
  --conf spark.sql.shuffle.partitions=400"

DRIVER_JSON=$(mktemp)
trap 'rm -f "$DRIVER_JSON"' EXIT

cat > "$DRIVER_JSON" <<EOF
{
  "sparkSubmit": {
    "entryPoint": "s3://$ARTIFACT_BUCKET/jars/cda-iceberg-client-1.0.jar",
    "entryPointArguments": ["$TABLE", "$NAMESPACE", "$SOURCE_BUCKET", "$MANIFEST_KEY", "$STATE_TABLE"],
    "sparkSubmitParameters": "$PARAMS"
  }
}
EOF

JR=$(aws emr-serverless start-job-run \
  --application-id "$APP_ID" \
  --execution-role-arn "$ROLE_ARN" \
  --name "smoke-$TABLE-$(date +%s)" \
  --job-driver "file://$DRIVER_JSON" \
  --configuration-overrides "{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://$LOGS_BUCKET/\"}}}" \
  --query 'jobRunId' --output text)

echo "Started job: $JR"
echo "Polling..."

for _ in $(seq 1 60); do
  STATE=$(aws emr-serverless get-job-run \
    --application-id "$APP_ID" --job-run-id "$JR" \
    --query 'jobRun.state' --output text)
  echo "  state=$STATE"
  case "$STATE" in
    SUCCESS|SUCCEEDED) echo "✓ smoke test passed"; exit 0 ;;
    FAILED|CANCELLED|CANCELLING)
      echo "✗ smoke test failed"
      aws emr-serverless get-job-run \
        --application-id "$APP_ID" --job-run-id "$JR" \
        --query 'jobRun.stateDetails' --output text
      exit 1 ;;
  esac
  sleep 30
done

echo "✗ timed out waiting for terminal state (job may still be running)"
exit 2
