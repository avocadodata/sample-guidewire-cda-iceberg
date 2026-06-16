// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg.recon

import org.apache.iceberg.spark.Spark3Util
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import scala.jdk.CollectionConverters._

/**
  * Tier D: merged-table semantic invariants. These prove the MERGE
  * picked the right winners and tombstones got applied correctly.
  *
  * Each check counts violation rows; status=OK iff count=0.
  *
  *   - tombstone_integrity: no id has a tombstone (op=1) as its latest
  *     raw entry while still present in merged.
  *   - latest_seqval:       every merged row's seqval equals the lpad-32
  *     max from raw (filtered to upsert ops 0/2/4) for that id.
  *   - no_orphans:          every id in merged exists in raw.
  *   - count_formula:       count(merged) = count(distinct id in raw with
  *     latest op in {0,2,4}).
  *
  * The queries are scoped per-table and run via Spark SQL. They scan
  * raw + merged once each, so cost grows with table size. Caller gates
  * Tier D via env var on the EMR job.
  */
object MergedInvariants {
  private val log = LogManager.getLogger(getClass.getName)

  /**
    * Run all four invariants for one table; write four rows to recon.
    *
    * POINT-IN-TIME PINNING (why this matters): CDA writes to the source
    * bucket continuously (every ~60–90s), so an ingest cycle can append to
    * `_raw` and then MERGE into `_merged` WHILE recon is mid-scan. Without
    * pinning, the four queries would each read a different live state, and
    * worse, `_raw` can momentarily be "ahead" of `_merged` (rows appended
    * but not yet merged) — producing false MISMATCHes that reconcile
    * seconds later.
    *
    * Fix: pin `_merged` to its current snapshot Sm (committed at Tm), and
    * pin `_raw` to the snapshot that was current AT-OR-BEFORE Tm — i.e.
    * `_raw` as it looked when the last MERGE committed. Because the
    * per-table ingest loop is strictly sequential (append raw → MERGE),
    * raw-as-of-Tm contains exactly the rows merge Sm reflected. Rows CDA
    * lands after Tm are excluded on BOTH sides, so all four invariants stay
    * mutually consistent regardless of concurrent ingest.
    *
    * If snapshot metadata can't be read (brand-new table, metadata-table
    * access denied), we fall back to unpinned/live reads — recon still
    * runs, just without the consistency guarantee (prior behavior).
    */
  def runAll(
    spark: SparkSession,
    namespace: String,
    runId: String,
    cdaTable: String,
    rawTable: String,
    mergedTable: String,
  ): Unit = {
    val (rawRef, mergedRef, pinNote) = pinnedRefs(spark, rawTable, mergedTable)
    log.info(s"Tier D — $cdaTable ${pinNote.getOrElse("UNPINNED (live read — snapshot metadata unavailable)")}")
    Seq(
      ("tombstone_integrity", tombstoneIntegritySql(rawRef, mergedRef)),
      ("latest_seqval",       latestSeqvalSql(rawRef, mergedRef)),
      ("no_orphans",          noOrphansSql(rawRef, mergedRef)),
      ("count_formula",       countFormulaSql(rawRef, mergedRef)),
    ).foreach { case (name, sql) =>
      runOne(spark, namespace, runId, cdaTable, name, sql)
    }
  }

  /**
    * Compute the FROM-clause table expressions for raw + merged, pinned to
    * a mutually-consistent point in time (see runAll docs). Returns
    * (rawRef, mergedRef, Some(note)) when pinning succeeds, or
    * (rawTable, mergedTable, None) as an unpinned fallback.
    *
    * Pinning by snapshot id (not timestamp string) avoids timezone-format
    * fragility in the AS OF clause; the only timestamp comparison is the
    * `committed_at <= timestamp_millis(Tm)` filter when locating raw's
    * snapshot, which reuses the same catalog clock for both sides.
    */
  private[iceberg] def pinnedRefs(
    spark: SparkSession,
    rawTable: String,
    mergedTable: String,
  ): (String, String, Option[String]) = {
    val pinned = for {
      (mSid, mTsMillis) <- latestSnapshot(spark, mergedTable)
      rSid              <- rawSnapshotAtOrBefore(spark, rawTable, mTsMillis)
    } yield (rSid, mSid)
    formatRefs(rawTable, mergedTable, pinned)
  }

  /** Pure formatter for pinnedRefs — separated so it can be unit-tested
    * without a SparkSession. Given the resolved (rawSnapshotId,
    * mergedSnapshotId), produce the time-travel FROM-clause refs; with
    * None, return the bare table names (unpinned fallback). */
  private[iceberg] def formatRefs(
    rawTable: String,
    mergedTable: String,
    pinned: Option[(Long, Long)],
  ): (String, String, Option[String]) =
    pinned match {
      case Some((rSid, mSid)) =>
        (s"$rawTable VERSION AS OF $rSid",
         s"$mergedTable VERSION AS OF $mSid",
         Some(s"pinned merged@$mSid raw@$rSid"))
      case None =>
        (rawTable, mergedTable, None)
    }

  /** (snapshot_id, committed_at-millis) of the table's current snapshot.
    *
    * Reads via the Iceberg catalog API (Spark3Util.loadIcebergTable →
    * currentSnapshot), NOT a `.snapshots` metadata-table SQL query. The SQL
    * form (`<cat>.<ns>.<tbl>.snapshots`) is mis-parsed by the S3 Tables
    * catalog as an extra namespace level and silently fails — earlier
    * string-munging attempts (plain, and back-tick-quoted) both fell back
    * to UNPINNED every time. The catalog API resolves the table the same
    * way the data queries do, so it works wherever the data reads work.
    * Returns None (→ unpinned recon) if the table has no snapshots yet. */
  private def latestSnapshot(spark: SparkSession, table: String): Option[(Long, Long)] =
    scala.util.Try {
      val t = Spark3Util.loadIcebergTable(spark, table)
      Option(t.currentSnapshot()).map(s => (s.snapshotId(), s.timestampMillis()))
    }.toOption.flatten

  /** snapshot_id of raw's latest snapshot committed at-or-before tsMillis —
    * i.e. raw as it looked when the merge we're pinning to committed. Walks
    * the table's snapshot history via the Iceberg API (same rationale as
    * latestSnapshot). */
  private def rawSnapshotAtOrBefore(spark: SparkSession, rawTable: String, tsMillis: Long): Option[Long] =
    scala.util.Try {
      val t = Spark3Util.loadIcebergTable(spark, rawTable)
      t.snapshots().asScala
        .filter(_.timestampMillis() <= tsMillis)
        .reduceOption((a, b) => if (a.timestampMillis() >= b.timestampMillis()) a else b)
        .map(_.snapshotId())
    }.toOption.flatten

  private def runOne(
    spark: SparkSession,
    namespace: String,
    runId: String,
    cdaTable: String,
    checkName: String,
    sql: String,
  ): Unit = {
    val violations: Long = try {
      spark.sql(sql).collect().headOption.map(_.getLong(0)).getOrElse(0L)
    } catch {
      case e: Throwable =>
        // If the query itself errors (e.g., raw missing a column the
        // invariant references), record an ERROR row rather than failing
        // the whole job. Operator can investigate.
        log.warn(s"Tier D — $checkName errored on $cdaTable: ${e.getMessage}")
        ReconStore.append(
          spark, namespace, runId, cdaTable,
          None, "D", checkName, "ERROR",
          None, None,
          Some(e.getClass.getSimpleName + ": " + Option(e.getMessage).getOrElse("")),
        )
        return
    }

    ReconStore.writeInvariantRow(
      spark, namespace, runId, cdaTable, checkName,
      violations,
    )
  }

  /**
    * Tombstone integrity: id whose latest raw op is 1 (tombstone) MUST
    * NOT appear in merged. The MERGE+DELETE pipeline handles this — if
    * the count is non-zero, either the DELETE didn't fire or arrived
    * out of order.
    */
  private[iceberg] def tombstoneIntegritySql(rawTable: String, mergedTable: String): String =
    s"""
      |SELECT count(*) FROM (
      |  SELECT m.id
      |  FROM $mergedTable m
      |  WHERE m.id IN (
      |    SELECT id FROM (
      |      SELECT id,
      |             FIRST_VALUE(gwcbi___operation) OVER (
      |               PARTITION BY id
      |               ORDER BY LPAD(gwcbi___seqval_hex, 32, '0') DESC
      |             ) AS latest_op
      |      FROM $rawTable
      |    ) WHERE latest_op = 1
      |  )
      |)
      |""".stripMargin

  /**
    * Latest-seqval invariant: for every id in merged, m.seqval must
    * equal the lpad-32 max seqval from raw filtered to upsert ops
    * (0,2,4). If they differ, the MERGE didn't pick the winner the
    * lpad ordering would have picked — likely a bug in the MERGE
    * predicate or the row got picked up out-of-order.
    */
  private[iceberg] def latestSeqvalSql(rawTable: String, mergedTable: String): String =
    s"""
      |SELECT count(*) FROM (
      |  SELECT m.id
      |  FROM $mergedTable m
      |  JOIN (
      |    SELECT id,
      |           MAX(LPAD(gwcbi___seqval_hex, 32, '0')) AS expected_seqval
      |    FROM $rawTable
      |    WHERE gwcbi___operation IN (0, 2, 4)
      |    GROUP BY id
      |  ) e ON m.id = e.id
      |  WHERE LPAD(m.gwcbi___seqval_hex, 32, '0') <> e.expected_seqval
      |)
      |""".stripMargin

  /**
    * No orphans: every id in merged must exist in raw. A merged row
    * with no raw counterpart means data was inserted into merged
    * outside the pipeline (corruption) or raw was wiped while merged
    * survived.
    */
  private[iceberg] def noOrphansSql(rawTable: String, mergedTable: String): String =
    s"""
      |SELECT count(*) FROM (
      |  SELECT m.id
      |  FROM $mergedTable m
      |  LEFT ANTI JOIN $rawTable r ON m.id = r.id
      |)
      |""".stripMargin

  /**
    * Count formula: |merged| = |distinct ids in raw whose latest op is
    * one of (0,2,4)|. If they differ, either MERGE missed an id (under)
    * or merged has stale rows from earlier pipeline state (over).
    */
  private[iceberg] def countFormulaSql(rawTable: String, mergedTable: String): String =
    s"""
      |SELECT
      |  ABS(
      |    (SELECT count(*) FROM $mergedTable)
      |    -
      |    (SELECT count(*) FROM (
      |       SELECT id,
      |              FIRST_VALUE(gwcbi___operation) OVER (
      |                PARTITION BY id
      |                ORDER BY LPAD(gwcbi___seqval_hex, 32, '0') DESC
      |              ) AS latest_op
      |       FROM $rawTable
      |     ) WHERE latest_op IN (0, 2, 4))
      |  )
      |""".stripMargin
}
