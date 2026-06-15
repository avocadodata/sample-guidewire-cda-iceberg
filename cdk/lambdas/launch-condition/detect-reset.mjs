/**
 * Pure logic for CDA reset detection. Compares stored DDB state for one
 * table against the current manifest entry and returns a {kind, detail}
 * descriptor when CDA has gone backward, or null otherwise.
 *
 * Two regression types:
 *   hwm_regression     stored HWM > manifest HWM (compared as BigInt;
 *                      epoch ms strings can be lex-different from numeric)
 *   fingerprint_pruned cursor exists for a fingerprint that's no longer
 *                      in manifest.schemaHistory
 *
 * Pulled out of index.mjs so it can be unit-tested without the AWS SDK
 * imports.
 */
export function detectReset(state, manifestEntry) {
  if (!state || !manifestEntry) return null;

  const manifestHwm = manifestEntry.lastSuccessfulWriteTimestamp;
  const storedHwm = state.lastSuccessfulWriteTimestamp;
  if (storedHwm && manifestHwm && BigInt(storedHwm) > BigInt(manifestHwm)) {
    return {
      kind: 'hwm_regression',
      detail: `stored=${storedHwm} > manifest=${manifestHwm}`,
    };
  }

  const knownFps = new Set(Object.keys(manifestEntry.schemaHistory ?? {}));
  const cursorFps = Object.keys(state.fingerprintCursors ?? {});
  const pruned = cursorFps.filter((fp) => !knownFps.has(fp));
  if (pruned.length > 0) {
    return {
      kind: 'fingerprint_pruned',
      detail: `cursors for fp(s) no longer in schemaHistory: ${pruned.join(',')}`,
    };
  }

  return null;
}
