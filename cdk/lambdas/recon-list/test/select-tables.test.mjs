// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { selectReconTables } from '../select-tables.mjs';

test('lists every table (recon audits all, no change-diff)', () => {
  const manifest = { cc_account: {}, cc_policyperiod: {} };
  const r = selectReconTables(manifest);
  assert.equal(r.count, 2);
  assert.deepEqual(r.tables.map((t) => t.tableName), ['cc_account', 'cc_policyperiod']);
});

test('output is sorted ascending by table name', () => {
  const manifest = { zzz: {}, aaa: {}, mmm: {} };
  const r = selectReconTables(manifest);
  assert.deepEqual(r.tables.map((t) => t.tableName), ['aaa', 'mmm', 'zzz']);
});

test('table names are lowercased', () => {
  const r = selectReconTables({ CC_Account: {} });
  assert.equal(r.tables[0].tableName, 'cc_account');
});

test('excluded tables are dropped (trim + lowercase + drop empties)', () => {
  const manifest = { keep: {}, drop_me: {}, also_drop: {} };
  const r = selectReconTables(manifest, { excludeCsv: ' DROP_ME , also_drop ,, ' });
  assert.deepEqual(r.tables.map((t) => t.tableName), ['keep']);
  assert.equal(r.count, 1);
});

test('each item carries sourceBucket + manifestKey for the Map fan-out', () => {
  const r = selectReconTables({ t: {} }, { sourceBucket: 'b', manifestKey: 'm.json' });
  assert.deepEqual(r.tables[0], { tableName: 't', sourceBucket: 'b', manifestKey: 'm.json' });
});

test('empty manifest -> zero tables', () => {
  const r = selectReconTables({});
  assert.equal(r.count, 0);
  assert.deepEqual(r.tables, []);
});
