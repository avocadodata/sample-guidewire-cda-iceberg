// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

/**
 * Pure table-selection logic for the recon (Tier D) fan-out: lowercase every
 * manifest table name, drop excluded ones, sort, and shape each into a Map item.
 *
 * Extracted from index.mjs so the selection/exclusion/sort behaviour is
 * unit-testable without the AWS SDK. Unlike launch-condition this does NOT
 * diff against bookmark state — recon audits every table on every run.
 */
export function selectReconTables(manifest, { excludeCsv = '', sourceBucket, manifestKey } = {}) {
  const exclude = new Set(
    (excludeCsv ?? '').split(',').map((s) => s.trim().toLowerCase()).filter(Boolean),
  );
  const tables = Object.keys(manifest)
    .map((n) => n.toLowerCase())
    .filter((n) => !exclude.has(n))
    .sort()
    .map((tableName) => ({ tableName, sourceBucket, manifestKey }));
  return { tables, count: tables.length };
}
