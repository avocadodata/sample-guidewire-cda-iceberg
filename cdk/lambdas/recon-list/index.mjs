import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { selectReconTables } from './select-tables.mjs';

// Environment contract:
//   SOURCE_BUCKET      - CDA writer S3 bucket (cross-region tolerated)
//   MANIFEST_KEY       - object key of manifest.json
//   TABLES_TO_EXCLUDE  - comma-separated table names to skip
//
// Returns: { tables: [{tableName, sourceBucket, manifestKey}, ...] }
//
// Unlike launch-condition, we don't diff against bookmark state — Tier D
// is a periodic deep audit and runs on every scheduled trigger,
// regardless of whether ingest has happened recently.

const REGION = process.env.AWS_REGION ?? 'us-east-1';
const s3 = new S3Client({ region: REGION, followRegionRedirects: true });

async function readManifest(bucket, key) {
  const out = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const body = await out.Body.transformToString('utf-8');
  return JSON.parse(body);
}

export const handler = async () => {
  const sourceBucket = required('SOURCE_BUCKET');
  const manifestKey = required('MANIFEST_KEY');

  const manifest = await readManifest(sourceBucket, manifestKey);
  return selectReconTables(manifest, {
    excludeCsv: process.env.TABLES_TO_EXCLUDE ?? '',
    sourceBucket,
    manifestKey,
  });
};

function required(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var ${name}`);
  return v;
}
