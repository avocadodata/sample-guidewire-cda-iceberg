// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import * as cdk from 'aws-cdk-lib';
import { Annotations, Match } from 'aws-cdk-lib/assertions';
import { AwsSolutionsChecks } from 'cdk-nag';
import { RuntimeStack } from '../lib/runtime-stack';
import { testApp, TEST_ENV, realEmrStack } from './helpers';

/**
 * cdk-nag regression guard: the AwsSolutions ruleset must report NO
 * un-suppressed errors/warnings on the security-critical stacks. Every
 * accepted finding has a documented NagSuppression in the stack (see
 * docs/security/cdk-nag-report.md); this test fails if a future change
 * introduces a NEW finding without a justification.
 */
function findings(stack: cdk.Stack) {
  cdk.Aspects.of(stack).add(new AwsSolutionsChecks());
  const annotations = Annotations.fromStack(stack);
  return {
    errors: annotations.findError('*', Match.stringLikeRegexp('AwsSolutions-.*')),
    warnings: annotations.findWarning('*', Match.stringLikeRegexp('AwsSolutions-.*')),
  };
}

function ids(arr: any[]): string[] {
  return arr.map((e) => `${e.id} :: ${(e.entry?.data ?? '').toString().slice(0, 120)}`);
}

test('EmrStack has no un-suppressed cdk-nag findings', () => {
  const app = testApp();
  const stack = realEmrStack(app);
  const { errors, warnings } = findings(stack);
  assert.deepEqual(errors, [], `cdk-nag errors:\n${ids(errors).join('\n')}`);
  assert.deepEqual(warnings, [], `cdk-nag warnings:\n${ids(warnings).join('\n')}`);
});

test('RuntimeStack has no un-suppressed cdk-nag findings', () => {
  const app = testApp();
  const stack = new RuntimeStack(app, 'Runtime', {
    env: TEST_ENV,
    customerName: 'cda',
    emrJobRoleArn: 'arn:aws:iam::111122223333:role/cda-emr',
    emrServerlessApplicationId: '00abc',
    bucketNames: { artifact: 'cda-artifacts', logs: 'cda-logs' },
  });
  const { errors, warnings } = findings(stack);
  assert.deepEqual(errors, [], `cdk-nag errors:\n${ids(errors).join('\n')}`);
  assert.deepEqual(warnings, [], `cdk-nag warnings:\n${ids(warnings).join('\n')}`);
});
