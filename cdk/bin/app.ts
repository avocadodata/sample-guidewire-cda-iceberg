#!/usr/bin/env node
// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { Aspects } from 'aws-cdk-lib';
import { AwsSolutionsChecks } from 'cdk-nag';
import { NetworkStack } from '../lib/network-stack';
import { IcebergStack } from '../lib/iceberg-stack';
import { EmrStack } from '../lib/emr-stack';
import { RuntimeStack } from '../lib/runtime-stack';
import { OrchestrationStack } from '../lib/orchestration-stack';
import { AnalyticsStack } from '../lib/analytics-stack';

const app = new cdk.App();

// cdk-nag (AWS Solutions rule pack) is opt-in via `--context cdkNag=true`,
// so routine `cdk deploy` synths stay fast and quiet. When enabled, every
// stack is scanned and findings are emitted as synth-time annotations; we
// generate the AppSec compliance report by synthing with this flag on.
// Suppressions live next to the resources they apply to (see lib/*-stack.ts)
// with a documented rationale, so the report is self-explaining.
if (app.node.tryGetContext('cdkNag') === 'true') {
  Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }));
}

const env: cdk.Environment = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const ctx = <T = string>(key: string, fallback?: T): T => {
  const v = app.node.tryGetContext(key);
  if (v === undefined || v === '') {
    if (fallback === undefined) {
      throw new Error(`Missing required context value '${key}'. Pass with --context ${key}=...`);
    }
    return fallback;
  }
  return v as T;
};

if (!env.account || !env.region) {
  throw new Error('CDK_DEFAULT_ACCOUNT / CDK_DEFAULT_REGION must be set (or pass env to the App).');
}

const customerName = ctx<string>('customerName', 'cda');

/**
 * Parse the `tags` context value into a {key:value} map. Format:
 *   "CostCenter=12345,Environment=prod,DataClassification=pii"
 * Always returns at least `cda:customer=<customerName>` for blast-radius
 * accounting; user-supplied tags merge on top and override.
 */
function parseTags(raw: string, customer: string): Record<string, string> {
  const out: Record<string, string> = { 'cda:customer': customer };
  raw.split(',').map((s) => s.trim()).filter(Boolean).forEach((pair) => {
    const eq = pair.indexOf('=');
    if (eq <= 0) {
      throw new Error(`Invalid tag '${pair}' — expected 'key=value' format`);
    }
    out[pair.slice(0, eq).trim()] = pair.slice(eq + 1).trim();
  });
  return out;
}
const tags = parseTags(ctx<string>('tags', ''), customerName);

/** Apply the parsed tag map to every resource in a stack (CDK propagates
 * stack-level tags to all taggable children automatically). */
function tagStack(stack: cdk.Stack): void {
  Object.entries(tags).forEach(([k, v]) => cdk.Tags.of(stack).add(k, v));
}

// Pre-compute bucket names: EMR's role policy needs them as literal ARNs to
// avoid a stack-cycle with RuntimeStack (RuntimeStack -> EMR jobRole, and EMR
// role policy -> RuntimeStack bucket).
const bucketNames = {
  artifact: `${customerName}-cda-iceberg-artifacts-${env.account}-${env.region}`,
  logs:     `${customerName}-cda-iceberg-logs-${env.account}-${env.region}`,
};

const network = new NetworkStack(app, `${customerName}-iceberg-network`, {
  env,
  description: 'CDA Iceberg client: VPC + S3 gateway endpoint (or BYO VPC)',
  customerName,
  existingVpcId: ctx<string>('vpcId', ''),
  existingPrivateSubnetIds: ctx<string>('existingVpcPrivateSubnetIds', ''),
  newVpcCidr: ctx<string>('newVpcCidr', '10.40.0.0/16'),
});

const iceberg = new IcebergStack(app, `${customerName}-iceberg-warehouse`, {
  env,
  description: 'CDA Iceberg client: S3 Tables bucket + namespace',
  customerName,
  namespaceName: ctx<string>('icebergNamespace', 'cda'),
  snapshotRetentionDays: Number(ctx<string>('snapshotRetentionDays', '5')),
});

const cdaSourceBucketArn = ctx<string>('cdaSourceBucketArn', '');

const emr = new EmrStack(app, `${customerName}-iceberg-emr`, {
  env,
  description: 'CDA Iceberg client: EMR Serverless app + IAM job role',
  customerName,
  vpc: network.vpc,
  computeSubnets: network.computeSubnets,
  serverlessReleaseLabel: ctx<string>('emrServerlessReleaseLabel', 'emr-spark-8.0-preview'),
  cdaSourceBucketArn,
  icebergTableBucketArn: iceberg.tableBucketArn,
  bucketNames,
  // EMR Serverless capacity + idle timeout. Defaults match the validated
  // mid/large profile from the synthetic 717-table run; small customers
  // can tune these down with --context emrMax... to shrink AWS quota
  // footprint (and surprise per-job cost spikes).
  emrMaxVcpu:            ctx<string>('emrMaxVcpu',            '800 vCPU'),
  emrMaxMemory:          ctx<string>('emrMaxMemory',          '3200 GB'),
  emrMaxDisk:            ctx<string>('emrMaxDisk',            '12000 GB'),
  emrIdleTimeoutMinutes: Number(ctx<string>('emrIdleTimeoutMinutes', '15')),
  // Warm pool sized to cover the first wave of a bulk-load fan-out
  // without cold-starting all 8 concurrent jobs. See the per-flag
  // comments in EmrStackProps for sizing rationale.
  emrInitialDriverCount:   Number(ctx<string>('emrInitialDriverCount',   '4')),
  emrInitialExecutorCount: Number(ctx<string>('emrInitialExecutorCount', '8')),
});
emr.addDependency(network);
emr.addDependency(iceberg);

const runtime = new RuntimeStack(app, `${customerName}-iceberg-runtime`, {
  env,
  description: 'CDA Iceberg client: artifact + logs buckets',
  customerName,
  emrJobRoleArn: emr.jobRole.roleArn,
  emrServerlessApplicationId: emr.serverlessApplicationId,
  bucketNames,
});

// Column exclusion spec. Passed to the Spark job as a driver env var via
// spark-submit, which splits on whitespace — so the value must contain no
// spaces. Validate here so a bad value fails at synth, not at job time.
const columnsToExclude = ctx<string>('columnsToExclude', '');
if (/\s/.test(columnsToExclude)) {
  throw new Error(
    `columnsToExclude must not contain whitespace (spark-submit splits on it). ` +
    `Use a comma-separated list with no spaces, e.g. 'ssn,taxid,cc_claim:description'. ` +
    `Got: ${JSON.stringify(columnsToExclude)}`);
}

// Orchestration is opt-in: requires a real CDA source bucket so the
// launch-condition Lambda has something to read. Operators can deploy the
// rest of the stacks first (to upload the jar, smoke-test a single job
// manually) and then re-deploy with --context cdaSourceBucketArn=... to
// wire up the schedule.
if (cdaSourceBucketArn) {
  const orchestration = new OrchestrationStack(app, `${customerName}-iceberg-orchestration`, {
    env,
    description: 'CDA Iceberg client: launch-condition Lambda + Step Functions Map',
    customerName,
    emrServerlessApplicationId: emr.serverlessApplicationId,
    emrJobRoleArn: emr.jobRole.roleArn,
    icebergTableBucketArn: iceberg.tableBucketArn,
    icebergNamespace: iceberg.namespaceName,
    cdaSourceBucketArn,
    cdaManifestKey: ctx<string>('cdaManifestKey', 'manifest.json'),
    tablesToExclude: ctx<string>('tablesToExclude', ''),
    columnsToExclude: columnsToExclude,
    bucketNames,
    cronExpression: ctx<string>('cronExpression', '0 * * * ? *'),
    scheduleEnabled: ctx<string>('scheduleEnabled', 'false') === 'true',
    notificationEmails: ctx<string>('notificationEmails', ''),
    mapStateConcurrency: Number(ctx<string>('mapStateConcurrency', '8')),
    // 0 = strict (one failed table fails the execution — production default).
    // Set >0 (e.g. 10) for a resilient bulk load; completeness is then
    // proven by reconciliation + the per-table report, not execution status.
    mapToleratedFailurePercentage: Number(ctx<string>('mapToleratedFailurePercentage', '0')),
    serverlessPollIntervalSeconds: Number(ctx<string>('serverlessPollIntervalSeconds', '60')),
    sparkScalaVersion: ctx<string>('sparkScalaVersion', '3.5_2.13'),
    icebergRuntimeVersion: ctx<string>('icebergRuntimeVersion', '1.5.2'),
    s3TablesCatalogVersion: ctx<string>('s3TablesCatalogVersion', '0.1.5'),
    // Default false: reference the S3-staged runtime jars with --jars, not
    // --packages from Maven Central (Maven resolution flakes under Map
    // concurrency — see OrchestrationStack.runtimeClasspathTokens). Escape
    // hatch only — set useMavenPackages=true if the deps weren't staged.
    useMavenPackages: ctx<string>('useMavenPackages', 'false') === 'true',
    // Per-table size class thresholds (rows). >= medium gets the medium
    // spark conf; >= large gets the large conf. Tune via context if your
    // CDA dataset's "big" looks different.
    sizeThresholdMedium: Number(ctx<string>('sizeThresholdMedium', '100000000')),
    sizeThresholdLarge:  Number(ctx<string>('sizeThresholdLarge',  '1000000000')),
    logRetentionDays:    Number(ctx<string>('logRetentionDays',    '30')),
    // Tier D recon — separate state machine + EventBridge schedule.
    // Default daily 02:00 UTC; turn on via reconScheduleEnabled=true.
    reconCronExpression:  ctx<string>('reconCronExpression', '0 2 * * ? *'),
    reconScheduleEnabled: ctx<string>('reconScheduleEnabled', 'false') === 'true',
    reconMapConcurrency:  Number(ctx<string>('reconMapConcurrency', '4')),
    // CDA Lifecycle Events (EAP) — opt-in event-driven ingest trigger.
    // Operator must accept the partner-source invitation in EventBridge
    // before this deploys cleanly. Default off; cron schedule remains
    // the production default until lifecycle events leave EAP.
    lifecycleEventsEnabled:      ctx<string>('lifecycleEventsEnabled', 'false') === 'true',
    lifecyclePartnerEventSource: ctx<string>('lifecyclePartnerEventSource', ''),
    lifecycleSourceApp:          ctx<string>('lifecycleSourceApp', ''),
  });
  orchestration.addDependency(emr);
  orchestration.addDependency(iceberg);
  tagStack(orchestration);

  // Optional analytics-side automation. Deploy AFTER the pipeline is
  // validated and the schedule is armed (see docs/DEPLOYMENT.md §15).
  // Off by default to avoid surprising customers with new IAM resources
  // and Lake Formation grants on first deploy.
  if (ctx<string>('enableAnalyticsStack', 'false') === 'true') {
    const analystRoles = ctx<string>('analystRoleArns', '')
      .split(',').map((s) => s.trim()).filter(Boolean);
    const analytics = new AnalyticsStack(app, `${customerName}-iceberg-analytics`, {
      env,
      description: 'CDA Iceberg client: optional Athena federation, dashboards, alarms',
      customerName,
      icebergNamespace: iceberg.namespaceName,
      tableBucketName: iceberg.tableBucketName,
      analystRoleArns: analystRoles,
      ingestStateMachineArn: orchestration.stateMachine.stateMachineArn,
      reconStateMachineArn:  orchestration.reconStateMachine.stateMachineArn,
      notificationTopicArn:  orchestration.notificationTopic.topicArn,
      emrServerlessApplicationId: emr.serverlessApplicationId,
      cursorTableName: orchestration.cursorTableName,
      athenaWorkgroup: ctx<string>('athenaWorkgroup', 'primary'),
      reconTableName:  ctx<string>('reconTableName', 'cda_recon_results'),
    });
    analytics.addDependency(orchestration);
    analytics.addDependency(iceberg);
    tagStack(analytics);
  }
}

// Apply tags to the always-deployed stacks. Stack-level tags propagate to
// all taggable child resources automatically (CDK's Tags.of mechanism).
tagStack(network);
tagStack(iceberg);
tagStack(emr);
tagStack(runtime);
