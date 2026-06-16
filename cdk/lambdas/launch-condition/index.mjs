// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand } from '@aws-sdk/lib-dynamodb';
import { decideLaunch } from './decide.mjs';

// Environment contract (set by CDK):
//   SOURCE_BUCKET           - CDA writer S3 bucket
//   MANIFEST_KEY            - object key of manifest.json inside SOURCE_BUCKET
//   STATE_TABLE             - DynamoDB table holding per-table state
//   TABLES_TO_EXCLUDE       - comma-separated table names to skip (optional)
//   SIZE_THRESHOLD_MEDIUM   - row count threshold for medium-class jobs (default 100M)
//   SIZE_THRESHOLD_LARGE    - row count threshold for large-class jobs (default 1B)
//
// Returns one of:
//   { status: "START",     changedTables: [{tableName, ts, sizeClass}], ... }
//   { status: "STOP",      changedTables: [], ... }
//   { status: "CDA_RESET", resets: [{tableName, kind, detail}], ... }
//
// CDA_RESET fires when the Lambda detects that CDA has gone backward — see
// detectResets below for the exact conditions. The state machine routes
// CDA_RESET to a high-priority SNS notification + Fail terminal; ingest does
// not run because cursors are no longer trustworthy.
//
// sizeClass drives per-table executor sizing.

const REGION = process.env.AWS_REGION ?? 'us-east-1';
const s3 = new S3Client({ region: REGION, followRegionRedirects: true });
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({ region: REGION }));

async function readManifest(bucket, key) {
  let out;
  try {
    out = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  } catch (e) {
    throw new Error(`Failed to read manifest s3://${bucket}/${key}: ${e.name ?? 'Error'}: ${e.message}`);
  }
  const body = await out.Body.transformToString('utf-8');
  try {
    return JSON.parse(body);
  } catch (e) {
    throw new Error(`Manifest s3://${bucket}/${key} is not valid JSON (${body.length} bytes): ${e.message}`);
  }
}

/** Read the full per-table state from DDB. Returns null if no row exists. */
async function getTableState(table, tableName) {
  const out = await ddb.send(
    new GetCommand({ TableName: table, Key: { tableName }, ConsistentRead: true }),
  );
  return out.Item ?? null;
}

export const handler = async () => {
  const sourceBucket = required('SOURCE_BUCKET');
  const manifestKey = required('MANIFEST_KEY');
  const stateTable = required('STATE_TABLE');

  const manifest = await readManifest(sourceBucket, manifestKey);

  // Pre-fetch each non-excluded table's DDB state, then hand the pure decision
  // logic (decideLaunch) a synchronous lookup. Keeps the I/O here and the
  // branching logic in a unit-testable module.
  const stateCache = new Map();
  for (const rawName of Object.keys(manifest)) {
    const tableName = rawName.toLowerCase();
    stateCache.set(tableName, await getTableState(stateTable, tableName));
  }

  return decideLaunch(manifest, (t) => stateCache.get(t) ?? null, {
    excludeCsv: process.env.TABLES_TO_EXCLUDE ?? '',
    mediumThreshold: parseInt(process.env.SIZE_THRESHOLD_MEDIUM ?? '100000000', 10),
    largeThreshold: parseInt(process.env.SIZE_THRESHOLD_LARGE ?? '1000000000', 10),
    sourceBucket,
    manifestKey,
  });
};

function required(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var ${name}`);
  return v;
}
