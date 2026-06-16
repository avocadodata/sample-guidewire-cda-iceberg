// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { decideLaunch, parseExclude } from '../decide.mjs';

// stateOf helper from a plain object map (lowercased keys)
const states = (obj) => (t) => obj[t] ?? null;

test('STOP when nothing changed (all manifest HWMs equal stored)', () => {
  const manifest = {
    cc_account: { lastSuccessfulWriteTimestamp: '100', totalProcessedRecordsCount: 1, schemaHistory: {} },
  };
  const r = decideLaunch(manifest, states({ cc_account: { lastSuccessfulWriteTimestamp: '100' } }));
  assert.equal(r.status, 'STOP');
  assert.deepEqual(r.changedTables, []);
});

test('START with the changed table when manifest HWM differs from stored', () => {
  const manifest = {
    cc_account: { lastSuccessfulWriteTimestamp: '200', totalProcessedRecordsCount: 1, schemaHistory: {} },
  };
  const r = decideLaunch(manifest, states({ cc_account: { lastSuccessfulWriteTimestamp: '100' } }));
  assert.equal(r.status, 'START');
  assert.equal(r.changedTables.length, 1);
  assert.equal(r.changedTables[0].tableName, 'cc_account');
  assert.equal(r.changedTables[0].ts, '200');
});

test('START for a brand-new table (no stored state)', () => {
  const manifest = {
    cc_new: { lastSuccessfulWriteTimestamp: '50', totalProcessedRecordsCount: 1, schemaHistory: {} },
  };
  const r = decideLaunch(manifest, states({}));
  assert.equal(r.status, 'START');
  assert.equal(r.changedTables[0].tableName, 'cc_new');
});

test('only changed tables are included; unchanged ones are skipped', () => {
  const manifest = {
    a: { lastSuccessfulWriteTimestamp: '2', totalProcessedRecordsCount: 1, schemaHistory: {} },
    b: { lastSuccessfulWriteTimestamp: '1', totalProcessedRecordsCount: 1, schemaHistory: {} },
  };
  const r = decideLaunch(manifest, states({ a: { lastSuccessfulWriteTimestamp: '1' }, b: { lastSuccessfulWriteTimestamp: '1' } }));
  assert.equal(r.status, 'START');
  assert.deepEqual(r.changedTables.map((c) => c.tableName), ['a']);
});

test('CDA_RESET when a table regressed; ingest is suppressed (no changedTables)', () => {
  const manifest = {
    a: { lastSuccessfulWriteTimestamp: '50', totalProcessedRecordsCount: 1, schemaHistory: {} },
  };
  const r = decideLaunch(manifest, states({ a: { lastSuccessfulWriteTimestamp: '100' } }));
  assert.equal(r.status, 'CDA_RESET');
  assert.equal(r.resets[0].tableName, 'a');
  assert.equal(r.resets[0].kind, 'hwm_regression');
  assert.deepEqual(r.changedTables, []);
});

test('CDA_RESET wins even if other tables changed normally (cursors untrustworthy)', () => {
  const manifest = {
    good: { lastSuccessfulWriteTimestamp: '200', totalProcessedRecordsCount: 1, schemaHistory: {} },
    bad: { lastSuccessfulWriteTimestamp: '50', totalProcessedRecordsCount: 1, schemaHistory: {} },
  };
  const r = decideLaunch(manifest, states({
    good: { lastSuccessfulWriteTimestamp: '100' },
    bad: { lastSuccessfulWriteTimestamp: '100' },
  }));
  assert.equal(r.status, 'CDA_RESET');
});

test('excluded tables are never considered', () => {
  const manifest = {
    keep: { lastSuccessfulWriteTimestamp: '2', totalProcessedRecordsCount: 1, schemaHistory: {} },
    skip_me: { lastSuccessfulWriteTimestamp: '2', totalProcessedRecordsCount: 1, schemaHistory: {} },
  };
  const r = decideLaunch(manifest, states({}), { excludeCsv: 'skip_me' });
  assert.equal(r.status, 'START');
  assert.deepEqual(r.changedTables.map((c) => c.tableName), ['keep']);
});

test('table names are lowercased (manifest may have mixed case)', () => {
  const manifest = {
    CC_Account: { lastSuccessfulWriteTimestamp: '2', totalProcessedRecordsCount: 1, schemaHistory: {} },
  };
  const r = decideLaunch(manifest, states({}));
  assert.equal(r.changedTables[0].tableName, 'cc_account');
});

test('entries without a HWM timestamp are skipped', () => {
  const manifest = {
    a: { totalProcessedRecordsCount: 1, schemaHistory: {} }, // no lastSuccessfulWriteTimestamp
  };
  const r = decideLaunch(manifest, states({}));
  assert.equal(r.status, 'STOP');
});

test('sizeClass: small / medium / large by record count thresholds', () => {
  const mk = (rows) => ({ lastSuccessfulWriteTimestamp: '2', totalProcessedRecordsCount: rows, schemaHistory: {} });
  const manifest = { small: mk(10), medium: mk(100000000), large: mk(1000000000) };
  const r = decideLaunch(manifest, states({}));
  const byName = Object.fromEntries(r.changedTables.map((c) => [c.tableName, c.sizeClass]));
  assert.equal(byName.small, 'small');
  assert.equal(byName.medium, 'medium');
  assert.equal(byName.large, 'large');
});

test('sizeClass respects custom thresholds', () => {
  const manifest = { t: { lastSuccessfulWriteTimestamp: '2', totalProcessedRecordsCount: 500, schemaHistory: {} } };
  const r = decideLaunch(manifest, states({}), { mediumThreshold: 100, largeThreshold: 1000 });
  assert.equal(r.changedTables[0].sizeClass, 'medium');
});

test('parseExclude trims, lowercases, drops empties', () => {
  const s = parseExclude(' A , b ,, C ');
  assert.deepEqual([...s].sort(), ['a', 'b', 'c']);
  assert.equal(parseExclude('').size, 0);
  assert.equal(parseExclude(undefined).size, 0);
});
