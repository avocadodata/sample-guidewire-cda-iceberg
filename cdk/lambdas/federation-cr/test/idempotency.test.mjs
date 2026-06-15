import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import { isAlreadyExists } from '../idempotency.mjs';

test('swallows Glue "already exists" message', () => {
  assert.equal(isAlreadyExists(new Error('Catalog s3tablescatalog already exists')), true);
});

test('swallows AlreadyExistsException by message', () => {
  assert.equal(isAlreadyExists(new Error('AlreadyExistsException: catalog present')), true);
});

test('swallows AlreadyExistsException by error name (SDK shape)', () => {
  const e = new Error('boom');
  e.name = 'AlreadyExistsException';
  assert.equal(isAlreadyExists(e), true);
});

test('case-insensitive on the message', () => {
  assert.equal(isAlreadyExists(new Error('Already Exists')), true);
});

test('does NOT swallow AccessDenied (must rethrow real failures)', () => {
  const e = new Error('User is not authorized to perform glue:CreateCatalog');
  e.name = 'AccessDeniedException';
  assert.equal(isAlreadyExists(e), false);
});

test('does NOT swallow throttling', () => {
  const e = new Error('Rate exceeded');
  e.name = 'ThrottlingException';
  assert.equal(isAlreadyExists(e), false);
});

test('tolerates null/undefined/empty error objects (returns false, rethrows)', () => {
  assert.equal(isAlreadyExists(null), false);
  assert.equal(isAlreadyExists(undefined), false);
  assert.equal(isAlreadyExists({}), false);
});
