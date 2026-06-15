/**
 * Idempotency predicate for the federation custom resource: returns true iff
 * an error from Glue CreateCatalog is the benign "already exists" case that
 * should be swallowed (CFN may re-run the CR; the parent catalog is an
 * account singleton). Any OTHER error must be rethrown so a real failure
 * (permissions, throttling, bad ARN) is not silently masked.
 *
 * Extracted so this safety-critical "swallow only already-exists" rule is
 * unit-tested without the AWS SDK.
 */
export function isAlreadyExists(err) {
  const msg = err?.message ?? '';
  const name = err?.name ?? '';
  return /already exists|AlreadyExistsException/i.test(msg) ||
         /AlreadyExistsException/i.test(name);
}
