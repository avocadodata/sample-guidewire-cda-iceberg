import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { Template } from 'aws-cdk-lib/assertions';
import { RuntimeStack } from '../lib/runtime-stack';
import { testApp, TEST_ENV } from './helpers';

/**
 * Bucket-hardening assertions for RuntimeStack (artifact + logs buckets).
 * These lock in the data-at-rest / transit controls the threat model credits
 * for T-INFO-CROSSACCT and T-INFO-LOGS: SSE, BlockPublicAccess, enforced TLS,
 * BucketOwnerEnforced on the logs sink.
 */
function synth() {
  const app = testApp();
  const stack = new RuntimeStack(app, 'Runtime', {
    env: TEST_ENV,
    customerName: 'cda',
    emrJobRoleArn: 'arn:aws:iam::111122223333:role/cda-emr',
    emrServerlessApplicationId: '00abc',
    bucketNames: { artifact: 'cda-artifacts', logs: 'cda-logs' },
  });
  return Template.fromStack(stack);
}

test('exactly two S3 buckets (artifact + logs)', () => {
  synth().resourceCountIs('AWS::S3::Bucket', 2);
});

test('every bucket blocks ALL public access', () => {
  const t = synth();
  const buckets = t.findResources('AWS::S3::Bucket');
  for (const [id, b] of Object.entries(buckets)) {
    assert.deepEqual(
      (b as any).Properties.PublicAccessBlockConfiguration,
      { BlockPublicAcls: true, BlockPublicPolicy: true, IgnorePublicAcls: true, RestrictPublicBuckets: true },
      `${id} must block all public access`);
  }
});

test('every bucket is encrypted at rest (SSE)', () => {
  const t = synth();
  const buckets = t.findResources('AWS::S3::Bucket');
  for (const [id, b] of Object.entries(buckets)) {
    const enc = (b as any).Properties.BucketEncryption;
    assert.ok(enc?.ServerSideEncryptionConfiguration?.length > 0, `${id} must have SSE`);
  }
});

test('logs bucket uses BucketOwnerEnforced (ACLs disabled)', () => {
  const t = synth();
  // The logs bucket is the one with ownership controls set.
  const buckets = Object.values(t.findResources('AWS::S3::Bucket'));
  const enforced = buckets.filter((b) => {
    const oc = (b as any).Properties.OwnershipControls;
    return JSON.stringify(oc ?? '').includes('BucketOwnerEnforced');
  });
  assert.ok(enforced.length >= 1, 'expected at least one BucketOwnerEnforced bucket (logs)');
});

test('TLS is enforced via a deny-non-SecureTransport bucket policy on every bucket', () => {
  const t = synth();
  const policies = Object.values(t.findResources('AWS::S3::BucketPolicy'));
  assert.ok(policies.length >= 2, 'expected a bucket policy per bucket');
  let denyTlsCount = 0;
  for (const p of policies) {
    const stmts = (p as any).Properties.PolicyDocument.Statement;
    for (const s of stmts) {
      if (s.Effect === 'Deny' &&
          JSON.stringify(s.Condition ?? '').includes('aws:SecureTransport')) {
        denyTlsCount++;
      }
    }
  }
  assert.ok(denyTlsCount >= 2, `expected >=2 deny-non-TLS statements, got ${denyTlsCount}`);
});

test('buckets are RETAINed (data not destroyed on stack delete)', () => {
  const t = synth();
  const buckets = t.findResources('AWS::S3::Bucket');
  for (const [id, b] of Object.entries(buckets)) {
    assert.equal((b as any).DeletionPolicy, 'Retain', `${id} must be retained`);
  }
});
