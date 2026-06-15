import { detectReset } from './detect-reset.mjs';

/**
 * Pure launch decision: given the manifest, the per-table stored DDB state,
 * and config, decide whether to START ingest (and for which tables), STOP
 * (nothing changed), or flag CDA_RESET (a table regressed and cursors are no
 * longer trustworthy).
 *
 * Extracted from index.mjs's handler so the decision logic can be unit-tested
 * without the AWS SDK. The handler is now just: read manifest, fetch each
 * table's state from DDB, then call this.
 *
 * @param {object}   manifest      table -> manifest entry (lastSuccessfulWriteTimestamp, totalProcessedRecordsCount, schemaHistory)
 * @param {Function} stateOf       (lowercasedTableName) => storedState|null
 * @param {object}   opts
 * @param {string}   [opts.excludeCsv]        comma-separated table names to skip
 * @param {number}   [opts.mediumThreshold]   row count >= this => 'medium'
 * @param {number}   [opts.largeThreshold]    row count >= this => 'large'
 * @param {string}   [opts.sourceBucket]
 * @param {string}   [opts.manifestKey]
 */
export function decideLaunch(manifest, stateOf, opts = {}) {
  const {
    excludeCsv = '',
    mediumThreshold = 100000000,
    largeThreshold = 1000000000,
    sourceBucket,
    manifestKey,
  } = opts;

  const exclude = new Set(
    excludeCsv.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean),
  );

  const changed = [];
  const resets = [];

  for (const [rawName, entry] of Object.entries(manifest)) {
    const tableName = rawName.toLowerCase();
    if (exclude.has(tableName)) continue;
    const ts = entry?.lastSuccessfulWriteTimestamp;
    if (!ts) continue;

    const state = stateOf(tableName);

    // Reset detection runs BEFORE the change check — a regressed manifest can
    // still differ from stored, but we must not ingest with bad timestamps.
    const reset = detectReset(state, entry);
    if (reset) {
      resets.push({ tableName, kind: reset.kind, detail: reset.detail });
      continue;
    }

    if (state?.lastSuccessfulWriteTimestamp !== ts) {
      const records = Number(entry?.totalProcessedRecordsCount ?? 0);
      const sizeClass =
        records >= largeThreshold ? 'large' :
        records >= mediumThreshold ? 'medium' :
        'small';
      changed.push({ tableName, ts, sizeClass });
    }
  }

  if (resets.length > 0) {
    return { status: 'CDA_RESET', resets, changedTables: [], sourceBucket, manifestKey };
  }
  if (changed.length === 0) {
    return { status: 'STOP', changedTables: [], sourceBucket, manifestKey };
  }
  return { status: 'START', changedTables: changed, sourceBucket, manifestKey };
}

/** Parse a comma-separated exclude list into a lowercased Set. Exported for reuse/testing. */
export function parseExclude(csv) {
  return new Set((csv ?? '').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean));
}
