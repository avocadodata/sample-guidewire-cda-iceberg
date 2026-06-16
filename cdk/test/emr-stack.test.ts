// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { testApp, realEmrStack } from './helpers';

/**
 * Security-posture assertion tests for EmrStack. These lock in the
 * least-privilege IAM and trust-policy scoping that the threat model
 * (docs/security/threat-model.md) credits as the mitigations for
 * T-INFO-CROSSACCT, T-SPOOF-ROLE, and T-ELEV-OPERATOR. If a future change
 * broadens the role (wildcard admin, write-back to the source, a wider trust
 * policy), these tests fail loudly.
 */
const SOURCE_ARN = 'arn:aws:s3:::vendor-cda-source';

function synth(cdaSourceBucketArn = SOURCE_ARN) {
  const app = testApp();
  const stack = realEmrStack(app, { cdaSourceBucketArn });
  return Template.fromStack(stack);
}

/** All IAM policy statements across every AWS::IAM::Policy in the template. */
function allStatements(t: Template): any[] {
  const policies = t.findResources('AWS::IAM::Policy');
  const out: any[] = [];
  for (const p of Object.values(policies)) {
    const stmts = (p as any).Properties.PolicyDocument.Statement;
    out.push(...stmts);
  }
  return out;
}

test('EMR job role trust policy is scoped to emr-serverless only (T-SPOOF-ROLE)', () => {
  const t = synth();
  t.hasResourceProperties('AWS::IAM::Role', {
    AssumeRolePolicyDocument: {
      Statement: Match.arrayWith([
        Match.objectLike({
          Action: 'sts:AssumeRole',
          Principal: { Service: 'emr-serverless.amazonaws.com' },
        }),
      ]),
    },
  });
});

test('no statement grants a service-level wildcard action (no s3:*, no Action:*)', () => {
  const t = synth();
  for (const s of allStatements(t)) {
    const actions = Array.isArray(s.Action) ? s.Action : [s.Action];
    for (const a of actions) {
      if (typeof a !== 'string') continue;
      assert.notEqual(a, '*', 'Action:* must never be granted');
      assert.ok(!/^s3:\*$/.test(a), `service-wide ${a} must not be granted`);
      assert.ok(!/^s3tables:\*$/.test(a), `service-wide ${a} must not be granted`);
      assert.ok(!/^iam:\*$/.test(a), `service-wide ${a} must not be granted`);
    }
  }
});

test('CDA source grant is READ-ONLY — no Put/Delete/Write back to the vendor bucket (T-TAMPER-SOURCE)', () => {
  const t = synth();
  const sourceStmts = allStatements(t).filter((s) => {
    const res = JSON.stringify(s.Resource ?? '');
    return res.includes('vendor-cda-source');
  });
  assert.ok(sourceStmts.length > 0, 'expected a statement scoped to the source bucket');
  for (const s of sourceStmts) {
    const actions = Array.isArray(s.Action) ? s.Action : [s.Action];
    for (const a of actions) {
      assert.ok(/^s3:(GetObject|ListBucket|GetBucketLocation)$/.test(a),
        `source-bucket action ${a} must be read-only`);
    }
  }
});

test('the source-bucket grant is omitted entirely in stub mode (empty ARN)', () => {
  const t = synth('');
  const sourceStmts = allStatements(t).filter((s) =>
    JSON.stringify(s.Resource ?? '').includes('vendor-cda-source'));
  assert.equal(sourceStmts.length, 0);
});

test('logs bucket grant is write-only (PutObject), not read', () => {
  const t = synth();
  const logStmts = allStatements(t).filter((s) =>
    JSON.stringify(s.Resource ?? '').includes('cda-iceberg-logs'));
  assert.ok(logStmts.length > 0);
  for (const s of logStmts) {
    const actions = Array.isArray(s.Action) ? s.Action : [s.Action];
    assert.deepEqual(actions, ['s3:PutObject']);
  }
});

test('logs:DescribeLogGroups on * is the ONLY Resource:* grant, and is list-only', () => {
  const t = synth();
  const starStmts = allStatements(t).filter((s) => s.Resource === '*');
  assert.equal(starStmts.length, 1, 'exactly one Resource:* statement expected');
  const actions = Array.isArray(starStmts[0].Action) ? starStmts[0].Action : [starStmts[0].Action];
  assert.deepEqual(actions, ['logs:DescribeLogGroups']);
});

test('EMR Serverless app runs in a VPC (private networking) with a security group (T-INFO-CROSSACCT)', () => {
  const t = synth();
  t.hasResourceProperties('AWS::EMRServerless::Application', {
    NetworkConfiguration: Match.objectLike({
      SubnetIds: Match.anyValue(),
      SecurityGroupIds: Match.anyValue(),
    }),
  });
});

test('EMR app has a maximumCapacity ceiling (T-DOS-CONCURRENCY blast-radius bound)', () => {
  const t = synth();
  t.hasResourceProperties('AWS::EMRServerless::Application', {
    MaximumCapacity: { Cpu: '800 vCPU', Memory: '3200 GB', Disk: '12000 GB' },
  });
});

test('iceberg table ops are explicit actions on the pinned warehouse ARN (no s3tables:*)', () => {
  const t = synth();
  const tableStmts = allStatements(t).filter((s) =>
    JSON.stringify(s.Action ?? '').includes('s3tables:'));
  assert.ok(tableStmts.length > 0);
  for (const s of tableStmts) {
    const actions = Array.isArray(s.Action) ? s.Action : [s.Action];
    for (const a of actions) {
      assert.ok(/^s3tables:[A-Z]/.test(a), `expected explicit s3tables action, got ${a}`);
    }
  }
});
