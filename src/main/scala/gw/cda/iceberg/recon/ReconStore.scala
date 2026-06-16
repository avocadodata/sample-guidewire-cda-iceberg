// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg.recon

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Iceberg `cda_recon_results` table — one row per (run, table, check).
  * Generalized across all reconciliation tiers; each row identifies its
  * tier + check_name explicitly.
  *
  * Schema:
  *   run_id        STRING        EMR Serverless jobRunId (or test fixture)
  *   committed_at  TIMESTAMP     when this row was written
  *   table_name    STRING        CDA table the check covers
  *   fingerprint   STRING NULL   per-fingerprint scope; NULL for table-grain checks
  *   tier          STRING        "A" | "B" | "D"
  *   check_name    STRING        identifier within tier, e.g. "batch_metrics",
  *                               "cumulative", "tombstone_integrity",
  *                               "latest_seqval", "no_orphans", "count_formula"
  *   status        STRING        OK | MISMATCH | METRICS_PARTIAL |
  *                               METRICS_MISSING | ERROR
  *   expected      BIGINT NULL   the spec-side count
  *   actual        BIGINT NULL   the data-side count
  *   delta         BIGINT NULL   actual - expected
  *   details       STRING NULL   free-form context (folder lists, sample IDs, etc.)
  *   notes         STRING NULL   operator-facing extra info
  *
  * Customer query pattern:
  *   SELECT * FROM s3tables.<ns>.cda_recon_results
  *   WHERE run_id = '<run-id>' AND status <> 'OK'
  */
object ReconStore {
  private val log = LogManager.getLogger(getClass.getName)
  // S3 Tables rejects names starting with underscore.
  val TableSuffix = "cda_recon_results"
  private val Catalog = "s3tables"

  val ReconSchema: StructType = StructType(Seq(
    StructField("run_id",       StringType,    nullable = false),
    StructField("committed_at", TimestampType, nullable = false),
    StructField("table_name",   StringType,    nullable = false),
    StructField("fingerprint",  StringType,    nullable = true),
    StructField("tier",         StringType,    nullable = false),
    StructField("check_name",   StringType,    nullable = false),
    StructField("status",       StringType,    nullable = false),
    StructField("expected",     LongType,      nullable = true),
    StructField("actual",       LongType,      nullable = true),
    StructField("delta",        LongType,      nullable = true),
    StructField("details",      StringType,    nullable = true),
    StructField("notes",        StringType,    nullable = true),
  ))

  // Validate the namespace at the one point it enters every recon DDL/DML
  // string. TableSuffix is a hardcoded constant, so namespace is the only
  // variable identifier here. (Callers in the ingest path already validate
  // it in main, but recon's own entry points reach here too.)
  private def tableName(namespace: String): String =
    s"s3tables.${gw.cda.iceberg.Identifiers.requireValid(namespace, "namespace")}.$TableSuffix"

  /** Idempotent CREATE. Partition by table_name so per-table queries
    * prune cleanly.
    *
    * CONCURRENT-WRITE SAFETY: every recon job (up to reconMapConcurrency in
    * parallel) appends its rows to THIS one table. Concurrent Iceberg
    * appends race on the metadata commit — two writers read base metadata
    * vN and both try to commit vN+1; one wins, the other throws
    * CommitFailedException. The commit.retry.* properties make the loser
    * re-read the new base and retry (appends are commutative, so a retry
    * always eventually succeeds) instead of failing the recon job. Without
    * these, a full recon run fails with ExceedToleratedFailureThreshold.
    * commit.retry.num-retries is raised well above the Iceberg default (4)
    * because the contention window scales with concurrency. */
  def ensureTableExists(spark: SparkSession, namespace: String): Unit = {
    val ns = gw.cda.iceberg.Identifiers.requireValid(namespace, "namespace")
    val name = s"$Catalog.$ns.$TableSuffix"
    val props = Map(
      "write.format.default"         -> "parquet",
      "write.target-file-size-bytes" -> "67108864",
      "write.distribution-mode"      -> "hash",
      // Concurrent-append commit retry — see writeRows; without this a
      // high-concurrency recon run fails with ExceedToleratedFailureThreshold.
      "commit.retry.num-retries"     -> "20",
      "commit.retry.min-wait-ms"     -> "200",
      "commit.retry.max-wait-ms"     -> "10000",
    )
    // DDL via the connector catalog API (typed Identifier + StructType), not
    // a built CREATE TABLE string — no SQL string to inject into.
    gw.cda.iceberg.IcebergCatalog.ensureTable(
      spark, Catalog, ns, TableSuffix, ReconSchema,
      Seq(gw.cda.iceberg.IcebergCatalog.identity("table_name")), props)
    // Also (re)apply the props to a PRE-EXISTING table — ensureTable no-ops
    // if the table already exists, so a recon table created before the
    // commit-retry change wouldn't otherwise get them. Idempotent.
    scala.util.Try {
      gw.cda.iceberg.IcebergCatalog.setProperties(spark, name, Map(
        "commit.retry.num-retries" -> "20",
        "commit.retry.min-wait-ms" -> "200",
        "commit.retry.max-wait-ms" -> "10000",
      ))
    }.recover { case e => log.warn(s"recon — could not set commit-retry props on $name: ${e.getMessage}") }
    log.info(s"recon — ensured table $name (concurrent-append commit-retry enabled)")
  }

  /** Status for the Tier A batch-metrics-vs-raw check. */
  private[iceberg] def deriveStatus(
    actualLanded: Long,
    expectedWritten: Long,
    okFolders: Int,
    missingFolders: Seq[String],
    corruptFolders: Seq[(String, String)],
  ): String = {
    val anyOk = okFolders > 0
    val anyMissing = missingFolders.nonEmpty
    val anyCorrupt = corruptFolders.nonEmpty
    if (!anyOk && (anyMissing || anyCorrupt)) "METRICS_MISSING"
    else if (actualLanded != expectedWritten) "MISMATCH"
    else if (anyMissing || anyCorrupt) "METRICS_PARTIAL"
    else "OK"
  }

  /** Build a single recon Row (no write). Pure — lets callers buffer
    * rows and write them in one batched Iceberg append (one metadata
    * commit instead of one per row). */
  private[recon] def buildRow(
    runId: String,
    tableNameVal: String,
    fingerprint: Option[String],
    tier: String,
    checkName: String,
    status: String,
    expected: Option[Long],
    actual: Option[Long],
    details: Option[String] = None,
    notes: Option[String] = None,
  ): Row = {
    val delta = for { a <- actual; e <- expected } yield a - e
    Row(
      runId,
      new java.sql.Timestamp(System.currentTimeMillis()),
      tableNameVal,
      fingerprint.orNull,
      tier,
      checkName,
      status,
      expected.map(java.lang.Long.valueOf).orNull,
      actual.map(java.lang.Long.valueOf).orNull,
      delta.map(java.lang.Long.valueOf).orNull,
      details.orNull,
      notes.orNull,
    )
  }

  /** Append a batch of pre-built recon rows in ONE Iceberg commit.
    * No-op on an empty batch.
    *
    * App-level retry on CommitFailedException: the table's commit.retry.*
    * properties (see ensureTableExists) handle most concurrent-append
    * conflicts, but a heavy burst (many recon jobs committing at once) can
    * still exhaust them. Since appends are commutative, retrying the whole
    * write is always safe — re-create the DataFrame and append again after a
    * short jittered backoff. This is the difference between a recon run
    * finishing vs. dying with ExceedToleratedFailureThreshold. */
  def writeRows(spark: SparkSession, namespace: String, rows: Seq[Row]): Unit = {
    if (rows.isEmpty) return
    val name = tableName(namespace)
    val maxAttempts = 8
    var attempt = 0
    var done = false
    while (!done) {
      attempt += 1
      try {
        val df = spark.createDataFrame(java.util.Arrays.asList(rows: _*), ReconSchema)
        df.writeTo(name).append()
        log.info(s"recon — wrote ${rows.size} row(s) to $name (attempt $attempt)")
        done = true
      } catch {
        case e: Throwable if isCommitConflict(e) && attempt < maxAttempts =>
          // Jittered backoff: base 250ms * attempt, plus up to 250ms jitter,
          // capped ~5s. Spreads contending writers apart.
          val backoff = math.min(5000L, 250L * attempt) + (System.nanoTime() % 250L)
          log.warn(s"recon — append conflict on $name (attempt $attempt/$maxAttempts), " +
            s"retrying in ${backoff}ms: ${e.getClass.getSimpleName}")
          Thread.sleep(backoff)
        // non-conflict errors, or conflict on the last attempt: rethrow so
        // the caller's RECON_WRITE_FAILED marker fires and the operator sees it.
      }
    }
  }

  /** True if the throwable (or any cause) is an Iceberg optimistic-commit
    * conflict — the retryable case for concurrent recon appends. */
  private[iceberg] def isCommitConflict(t: Throwable): Boolean = {
    var c: Throwable = t
    while (c != null) {
      val n = c.getClass.getName
      if (n.contains("CommitFailedException") ||
          n.contains("CommitStateUnknownException") ||
          Option(c.getMessage).exists(_.contains("is not the same as current metadata location")))
        return true
      c = c.getCause
    }
    false
  }

  /** Append one recon row (single-commit convenience for callers that
    * don't batch, e.g. Tier D's per-invariant writes). */
  private[recon] def append(
    spark: SparkSession,
    namespace: String,
    runId: String,
    tableNameVal: String,
    fingerprint: Option[String],
    tier: String,
    checkName: String,
    status: String,
    expected: Option[Long],
    actual: Option[Long],
    details: Option[String] = None,
    notes: Option[String] = None,
  ): Unit = {
    val row = buildRow(runId, tableNameVal, fingerprint, tier, checkName,
      status, expected, actual, details, notes)
    writeRows(spark, namespace, Seq(row))
    log.info(s"recon — $tier/$checkName $status table=$tableNameVal " +
      s"fp=${fingerprint.getOrElse("∅")} expected=${expected.getOrElse("∅")} actual=${actual.getOrElse("∅")}")
  }

  // --- Tier A: batch-metrics vs raw, per-fingerprint per-batch ---------

  /** Build (don't write) one Tier A batch-metrics row. Caller buffers
    * and batch-writes via writeRows. */
  def batchMetricsRow(
    runId: String,
    tableNameVal: String,
    fingerprint: String,
    actualLanded: Long,
    metrics: MetricsAggregate,
    timestampsProcessed: Int,
    notes: String = "",
  ): Row = {
    val status = deriveStatus(
      actualLanded, metrics.writtenSum,
      metrics.okFolders, metrics.missingFolders, metrics.corruptFolders,
    )
    val details = s"""{"timestamps_processed":$timestampsProcessed,""" +
      s""""metrics_read_sum":${metrics.readSum},""" +
      s""""metrics_dropped":${metrics.droppedSum},""" +
      s""""metrics_missing":${metrics.missingFolders.map(s => "\"" + s + "\"").mkString("[", ",", "]")},""" +
      s""""metrics_corrupt_count":${metrics.corruptFolders.size}}"""
    buildRow(
      runId, tableNameVal,
      Some(fingerprint), "A", "batch_metrics",
      status, Some(metrics.writtenSum), Some(actualLanded),
      Some(details), Some(notes).filter(_.nonEmpty),
    )
  }

  // --- Tier B: cumulative table-grain check ----------------------------

  /** Build (don't write) the Tier B cumulative row. */
  def cumulativeRow(
    runId: String,
    tableNameVal: String,
    expectedFromMetrics: Long,
    actualFromRaw: Long,
  ): Row = {
    val status = if (expectedFromMetrics == actualFromRaw) "OK" else "MISMATCH"
    buildRow(
      runId, tableNameVal,
      None, "B", "cumulative",
      status, Some(expectedFromMetrics), Some(actualFromRaw),
    )
  }

  // --- Tier D: merged-table semantic invariants ------------------------

  /** Generic per-invariant write. `actual` is the count of rows that
    * VIOLATE the invariant; `expected` is always 0. status=OK iff actual=0. */
  def writeInvariantRow(
    spark: SparkSession,
    namespace: String,
    runId: String,
    tableNameVal: String,
    checkName: String,
    violations: Long,
    sampleDetails: Option[String] = None,
  ): Unit = {
    val status = if (violations == 0L) "OK" else "MISMATCH"
    append(
      spark, namespace, runId, tableNameVal,
      None, "D", checkName,
      status, Some(0L), Some(violations),
      sampleDetails,
    )
  }
}
