// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { detectReset } from '../detect-reset.mjs';

test('HWM regression: stored newer than manifest -> hwm_regression', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '200', fingerprintCursors: {} },
    { lastSuccessfulWriteTimestamp: '100', schemaHistory: {} },
  );
  assert.equal(r?.kind, 'hwm_regression');
});

test('HWM equal: not a regression', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '100' },
    { lastSuccessfulWriteTimestamp: '100', schemaHistory: {} },
  );
  assert.equal(r, null);
});

test('HWM advancing (manifest newer): not a regression', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '100' },
    { lastSuccessfulWriteTimestamp: '200', schemaHistory: {} },
  );
  assert.equal(r, null);
});

test('HWM compared numerically (BigInt) not lexically: stored "1000" vs "999" IS a regression', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '1000' },
    { lastSuccessfulWriteTimestamp: '999', schemaHistory: {} },
  );
  assert.equal(r?.kind, 'hwm_regression');
});

test('HWM numeric: stored "9" vs manifest "10" is NOT a regression (lexical would be wrong)', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '9', fingerprintCursors: {} },
    { lastSuccessfulWriteTimestamp: '10', schemaHistory: {} },
  );
  assert.equal(r, null);
});

test('fingerprint pruned: cursor for a fp not in manifest schemaHistory', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '100', fingerprintCursors: { fp_old: '50', fp_current: '90' } },
    { lastSuccessfulWriteTimestamp: '100', schemaHistory: { fp_current: '60' } },
  );
  assert.equal(r?.kind, 'fingerprint_pruned');
  assert.match(r.detail, /fp_old/);
});

test('all cursor fingerprints still known: not a reset', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '100', fingerprintCursors: { fp_a: '50', fp_b: '90' } },
    { lastSuccessfulWriteTimestamp: '100', schemaHistory: { fp_a: '20', fp_b: '60' } },
  );
  assert.equal(r, null);
});

test('HWM regression takes precedence over a pruned fingerprint', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '200', fingerprintCursors: { gone: '1' } },
    { lastSuccessfulWriteTimestamp: '100', schemaHistory: {} },
  );
  assert.equal(r?.kind, 'hwm_regression');
});

test('first-ever run (null state): not a reset', () => {
  const r = detectReset(null, { lastSuccessfulWriteTimestamp: '100', schemaHistory: { fp_a: '50' } });
  assert.equal(r, null);
});

test('null manifest entry: not a reset', () => {
  assert.equal(detectReset({ lastSuccessfulWriteTimestamp: '100' }, null), null);
});

test('missing fingerprintCursors / schemaHistory tolerated (no pruned)', () => {
  const r = detectReset(
    { lastSuccessfulWriteTimestamp: '100' },
    { lastSuccessfulWriteTimestamp: '100' },
  );
  assert.equal(r, null);
});
