import * as fs from 'fs';
import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { NetworkStack } from '../lib/network-stack';
import { IcebergStack } from '../lib/iceberg-stack';
import { EmrStack } from '../lib/emr-stack';

/**
 * Build a CDK App seeded with the project's cdk.json `context` (feature
 * flags). Tests MUST use this rather than `new cdk.App()` so that synth
 * matches production — notably the @aws-cdk/aws-s3 feature flags that decide
 * whether server-access logging uses a bucket policy (compatible with
 * BucketOwnerEnforced) vs. a legacy ACL (which throws).
 */
export function testApp(): cdk.App {
  const cdkJson = JSON.parse(
    fs.readFileSync(path.join(__dirname, '..', 'cdk.json'), 'utf-8'),
  );
  return new cdk.App({ context: cdkJson.context ?? {} });
}

export const TEST_ENV = { account: '111122223333', region: 'us-east-1' };

/**
 * Construct EmrStack the way bin/app.ts does — wired to a real NetworkStack
 * (VPC) and IcebergStack (S3 Tables warehouse). This matters for fidelity:
 * the EMR job role's IAM5 suppression matches the warehouse ARN by its
 * cross-stack token shape (`<CdaTableBucket...>`), which only renders that way
 * when the ARN actually comes from IcebergStack — not from a hand-written
 * literal. Returns the EmrStack plus its dependencies so tests can synth it.
 */
export function realEmrStack(
  app: cdk.App,
  opts: { cdaSourceBucketArn?: string; customerName?: string } = {},
): EmrStack {
  const customerName = opts.customerName ?? 'cda';
  const network = new NetworkStack(app, 'cda-iceberg-network', {
    env: TEST_ENV, customerName,
    existingVpcId: '', existingPrivateSubnetIds: '', newVpcCidr: '10.40.0.0/16',
  });
  const iceberg = new IcebergStack(app, 'cda-iceberg-warehouse', {
    env: TEST_ENV, customerName, namespaceName: 'cda', snapshotRetentionDays: 5,
  });
  const account = TEST_ENV.account, region = TEST_ENV.region;
  return new EmrStack(app, 'cda-iceberg-emr', {
    env: TEST_ENV,
    customerName,
    vpc: network.vpc,
    computeSubnets: network.computeSubnets,
    serverlessReleaseLabel: 'emr-spark-8.0-preview',
    cdaSourceBucketArn: opts.cdaSourceBucketArn ?? 'arn:aws:s3:::vendor-cda-source',
    icebergTableBucketArn: iceberg.tableBucketArn,
    bucketNames: {
      artifact: `${customerName}-cda-iceberg-artifacts-${account}-${region}`,
      logs: `${customerName}-cda-iceberg-logs-${account}-${region}`,
    },
    emrMaxVcpu: '800 vCPU', emrMaxMemory: '3200 GB', emrMaxDisk: '12000 GB',
    emrIdleTimeoutMinutes: 15, emrInitialDriverCount: 4, emrInitialExecutorCount: 8,
  });
}
