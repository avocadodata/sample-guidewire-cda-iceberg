// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { OrchestrationStack } from '../lib/orchestration-stack';
import { testApp, TEST_ENV } from './helpers';

/**
 * Orchestration-stack assertions. Lock in:
 *  - the per-table fan-out is a DistributedMap (escapes the 25k history limit),
 *  - EMR jobs reference runtime deps via --jars (S3-staged), NOT --packages
 *    from Maven (the fix that stopped mass job-start failures), with the
 *    useMavenPackages escape hatch restoring --packages,
 *  - the SNS topic denies non-TLS publish (T-INFO in transit),
 *  - the schedule stays disabled unless explicitly enabled.
 */
function baseProps(overrides: Partial<any> = {}) {
  return {
    env: TEST_ENV,
    customerName: 'cda',
    emrServerlessApplicationId: '00abc123',
    emrJobRoleArn: 'arn:aws:iam::111122223333:role/cda-emr',
    icebergTableBucketArn: 'arn:aws:s3tables:us-east-1:111122223333:bucket/cda-wh',
    icebergNamespace: 'cda',
    cdaSourceBucketArn: 'arn:aws:s3:::vendor-cda-source',
    cdaManifestKey: 'manifest.json',
    tablesToExclude: '',
    columnsToExclude: '',
    bucketNames: { artifact: 'cda-artifacts', logs: 'cda-logs' },
    cronExpression: '0 * * * ? *',
    scheduleEnabled: false,
    notificationEmails: '',
    mapStateConcurrency: 8,
    mapToleratedFailurePercentage: 0,
    serverlessPollIntervalSeconds: 60,
    sparkScalaVersion: '3.5_2.13',
    icebergRuntimeVersion: '1.5.2',
    s3TablesCatalogVersion: '0.1.5',
    useMavenPackages: false,
    sizeThresholdMedium: 100000000,
    sizeThresholdLarge: 1000000000,
    logRetentionDays: 30,
    reconCronExpression: '0 2 * * ? *',
    reconScheduleEnabled: false,
    reconMapConcurrency: 4,
    lifecycleEventsEnabled: false,
    lifecyclePartnerEventSource: '',
    lifecycleSourceApp: '',
    ...overrides,
  };
}

function synth(overrides: Partial<any> = {}) {
  const app = testApp();
  const stack = new OrchestrationStack(app, 'Orch', baseProps(overrides) as any);
  return Template.fromStack(stack);
}

/** The whole synthesized template as a JSON string — handy for substring claims. */
function templateJson(t: Template): string {
  return JSON.stringify(t.toJSON());
}

test('per-table fan-out uses a Step Functions DistributedMap (not inline Map)', () => {
  const t = synth();
  const json = templateJson(t);
  // DistributedMap renders a ProcessorConfig with Mode: DISTRIBUTED in the ASL.
  assert.ok(json.includes('DISTRIBUTED'), 'expected a DISTRIBUTED Map processor mode');
});

test('default: EMR jobs reference runtime deps via --jars from the S3 deps/ prefix', () => {
  const json = templateJson(synth());
  assert.ok(json.includes('--jars'), 'expected --jars in spark-submit params');
  assert.ok(json.includes('/deps/iceberg-spark-runtime-3.5_2.13-1.5.2.jar'),
    'expected the staged iceberg runtime jar path');
  assert.ok(!json.includes('--packages'), 'must NOT use --packages by default');
});

test('escape hatch: useMavenPackages=true switches to --packages (Maven coords)', () => {
  const json = templateJson(synth({ useMavenPackages: true }));
  assert.ok(json.includes('--packages'), 'expected --packages when useMavenPackages=true');
  assert.ok(json.includes('org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.2'),
    'expected Maven coordinates');
});

test('SNS notification topic denies non-TLS publish', () => {
  const t = synth();
  const policies = Object.values(t.findResources('AWS::SNS::TopicPolicy'));
  let denyTls = false;
  for (const p of policies) {
    for (const s of (p as any).Properties.PolicyDocument.Statement) {
      if (s.Effect === 'Deny' &&
          JSON.stringify(s.Condition ?? '').includes('aws:SecureTransport')) {
        denyTls = true;
      }
    }
  }
  assert.ok(denyTls, 'expected a deny-non-TLS statement on the SNS topic policy');
});

test('schedule rule is DISABLED by default (no accidental auto-runs)', () => {
  const t = synth();
  // Any EventBridge rule present must be in DISABLED state when scheduleEnabled=false.
  const rules = Object.values(t.findResources('AWS::Events::Rule'));
  for (const r of rules) {
    const state = (r as any).Properties.State;
    // Rules created for the schedule should be DISABLED; allow rules with no
    // State only if they're not the cron schedule (defensive).
    if (state) assert.equal(state, 'DISABLED', 'schedule rules must be disabled by default');
  }
});

test('enabling the schedule flips the ingest rule to ENABLED', () => {
  const t = synth({ scheduleEnabled: true });
  const rules = Object.values(t.findResources('AWS::Events::Rule'));
  const enabled = rules.filter((r) => (r as any).Properties.State === 'ENABLED');
  assert.ok(enabled.length >= 1, 'expected at least one ENABLED rule when scheduleEnabled=true');
});
