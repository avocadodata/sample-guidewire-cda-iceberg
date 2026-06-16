// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg

import gw.cda.iceberg.manifest.{ManifestEntry, ManifestReader}
import gw.cda.iceberg.recon.{BatchMetrics, MergedInvariants, MetricsAggregate, ReconStore}
import gw.cda.iceberg.state.{CursorStore, TimestampFolderSelector}
import gw.cda.iceberg.utils.S3ClientSupplier
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Try

/**
  * Spark entry point for ingesting one CDA table into Iceberg (S3 Tables).
  *
  * Replaces OSR's writeJdbcRaw + writeJdbcMerged with three SQL operations
  * per fingerprint: append into raw, MERGE into merged with the lpad-32
  * seqval guard, DELETE tombstones.
  *
  * Invocation (one job per table from Step Functions Map):
  *   spark-submit --class gw.cda.iceberg.IcebergIngest <jar>
  *     <table>         e.g. cc_account
  *     <namespace>     e.g. cda
  *     <srcBucket>     CDA source bucket name
  *     <manifestKey>   e.g. synthetic/manifest.json
  *
  * Catalog binding (s3tables.<namespace>.<table>_raw / _merged) is supplied
  * by EMR Serverless's applicationConfiguration.
  */
object IcebergIngest {

  private val log = LogManager.getLogger(getClass.getName)

  // Column added to raw to track which CDA fingerprint each row came from.
  // Partitioning the raw table by this lets us scope MERGE and DELETE to a
  // single fingerprint, and lets old fingerprints be queried independently
  // for audit / replay.
  private val FingerprintCol = "cda_fingerprint"
  private val LoadTsCol = "cda_load_ts"
  // The Spark catalog name the S3 Tables warehouse is bound to (set via the
  // spark.sql.catalog.s3tables.* confs at job submit). All tables live as
  // <Catalog>.<namespace>.<table>.
  private val Catalog = "s3tables"

  private val BookkeepingCols = Set(FingerprintCol, LoadTsCol)

  def main(args: Array[String]): Unit = {
    // Two arg shapes:
    //   ingest:     <table> <namespace> <srcBucket> <manifestKey> <stateTable>
    //   recon-only: --recon-only <table> <namespace>
    val isRecon = args.headOption.contains("--recon-only")
    if (isRecon) {
      if (args.length != 3) {
        System.err.println("Usage: IcebergIngest --recon-only <table> <namespace>")
        sys.exit(1)
      }
      val Array(_, rawTableArg, rawNsArg) = args
      // SANITIZE at the entry point: requireValid returns a NEW string
      // rebuilt from a safe alphabet (not the original arg), so everything
      // downstream uses the sanitized value, not the tainted CLI input.
      val table = Identifiers.requireValid(rawTableArg, "table")
      val namespace = Identifiers.requireValid(rawNsArg, "namespace")
      val spark = SparkSession.builder().appName(s"iceberg-recon-$table").getOrCreate()
      try {
        runReconOnly(spark, table, namespace)
      } finally {
        spark.stop()
      }
    } else {
      if (args.length != 5) {
        System.err.println(
          "Usage: IcebergIngest <table> <namespace> <srcBucket> <manifestKey> <stateTable>"
        )
        sys.exit(1)
      }
      val Array(rawTableArg, rawNsArg, srcBucket, manifestKey, stateTable) = args
      // SANITIZE at the entry point: requireValid returns a NEW string
      // rebuilt from a safe alphabet (not the original arg), so everything
      // downstream uses the sanitized value, not the tainted CLI input.
      val table = Identifiers.requireValid(rawTableArg, "table")
      val namespace = Identifiers.requireValid(rawNsArg, "namespace")
      val spark = SparkSession.builder().appName(s"iceberg-ingest-$table").getOrCreate()
      try {
        run(spark, table, namespace, srcBucket, manifestKey, stateTable)
      } finally {
        spark.stop()
      }
    }
  }

  /** Recon-only path: scan raw + merged for one table, write Tier D rows.
    * Does NOT re-read source parquet, does NOT touch DDB cursors, does
    * NOT advance state. Designed for a separate scheduled audit that
    * runs less frequently than ingest. If raw or merged doesn't exist,
    * exits cleanly — first-time customers shouldn't get errors before
    * their first ingest completes. */
  def runReconOnly(spark: SparkSession,
                   table: String,
                   namespace: String): Unit = {
    // Sanitize locally (fresh allow-listed strings) before building the
    // table refs that feed DESCRIBE / recon DDL.
    val ns = Identifiers.requireValid(namespace, "namespace")
    val tbl = Identifiers.requireValid(table, "table")
    val rawTable = s"s3tables.$ns.${tbl}_raw"
    val mergedTable = s"s3tables.$ns.${tbl}_merged"
    val runId = spark.sparkContext.applicationId
    val tablesExist = Try {
      spark.sql(s"DESCRIBE TABLE $rawTable").collect()
      spark.sql(s"DESCRIBE TABLE $mergedTable").collect()
    }.isSuccess
    if (!tablesExist) {
      log.warn(s"recon-only: '$tbl' raw or merged doesn't exist yet; skipping")
      return
    }
    ReconStore.ensureTableExists(spark, ns)
    log.info(s"recon-only: running Tier D for '$tbl' runId=$runId")
    MergedInvariants.runAll(spark, ns, runId, tbl, rawTable, mergedTable)
    log.info(s"recon-only: '$tbl' done")
  }

  /** Core loop. Public for unit testing. */
  def run(spark: SparkSession,
          table: String,
          namespace: String,
          srcBucket: String,
          manifestKey: String,
          stateTable: String): Unit = {

    // Sanitize the identifiers locally so the table refs below — which feed
    // CREATE/ALTER DDL — are built from provably-safe strings (requireValid
    // returns a fresh allow-listed string, not the caller's value). Public
    // entry point, so don't assume the caller sanitized.
    val ns = Identifiers.requireValid(namespace, "namespace")
    val tbl = Identifiers.requireValid(table, "table")

    val manifest = ManifestReader.processManifest(srcBucket, manifestKey)
    val entry = manifest.getOrElse(tbl, {
      log.error(s"Table '$tbl' not present in manifest s3://$srcBucket/$manifestKey")
      sys.exit(2)
    })

    val rawTable = s"s3tables.$ns.${tbl}_raw"
    val mergedTable = s"s3tables.$ns.${tbl}_merged"

    val cursors = new CursorStore(stateTable)
    val state = cursors.readState(table)
    val runId = spark.sparkContext.applicationId
    // One deterministic load timestamp for the whole run. Stamped onto
    // every row this run ingests (so a run's rows share a cda_load_ts),
    // and used as the merge-watermark value when the incremental-merge
    // filter is enabled. Captured once here, not per-row.
    val runLoadTsMillis = System.currentTimeMillis()
    // Incremental MERGE filter (P7): when enabled, the MERGE source is
    // scoped to rows newer than the last *successful merge* watermark,
    // avoiding a full re-window of the fingerprint partition on every
    // incremental. OFF by default — it changes the data-correctness
    // boundary and must be validated against a controlled incremental
    // before production use. See INCREMENTAL_MERGE_FILTER env var.
    val incrementalMergeFilter =
      sys.env.get("INCREMENTAL_MERGE_FILTER").exists(_.equalsIgnoreCase("true"))
    // Per-table column exclusion (COLUMNS_TO_EXCLUDE env var). Excluded
    // columns are dropped from the source before the raw append, so they
    // never land in raw or merged. Parsed once; protected columns
    // (id / seqval / operation) fail parse loudly.
    val excludeCols = ColumnExclusion.parse(sys.env.getOrElse("COLUMNS_TO_EXCLUDE", ""))
      .columnsFor(table)
    if (excludeCols.nonEmpty)
      log.info(s"'$table' — excluding columns: ${excludeCols.toSeq.sorted.mkString(", ")}")
    log.info(s"Ingesting '$table' — basePath=${entry.dataFilesPath} " +
      s"upperBound=${entry.lastSuccessfulWriteTimestamp} " +
      s"cursors=${state.cursors.size} hwm=${state.highWaterMark.getOrElse("∅")} " +
      s"runId=$runId incrementalMergeFilter=$incrementalMergeFilter")

    val fingerprints = chronologicalFingerprints(entry)

    // For each fingerprint, list timestamp folders directly via S3 (not
    // glob). The doc explicitly requires:
    //   1. only read folders strictly greater than our cursor
    //   2. only read folders <= manifest's lastSuccessfulWriteTimestamp
    //      (folders past the watermark may be mid-write)
    // If none qualify after filtering, skip the fingerprint — no read,
    // no append, no cursor advance.
    //
    // cursorFor returns the per-fingerprint cursor if set, or the
    // table-level high-water mark as a migration fallback (so an old
    // deployment that has lastSuccessfulWriteTimestamp but no
    // fingerprintCursors yet doesn't re-load already-loaded data).
    val plan: Seq[(String, Seq[String])] = fingerprints.map { fp =>
      val all = Try(TimestampFolderSelector.listTimestampFolders(
        S3ClientSupplier.s3Client, srcBucket, dataFilesKey(entry.dataFilesPath, srcBucket), fp,
      )).getOrElse(Seq.empty)
      val cursor = state.cursorFor(fp)
      val selected = TimestampFolderSelector.select(all, cursor, entry.lastSuccessfulWriteTimestamp)
      log.info(s"'$table' fingerprint=$fp — ${all.size} folder(s) on disk, " +
        s"${selected.size} new (cursor=${cursor.getOrElse("∅")})")
      (fp, selected)
    }

    val nonEmpty = plan.filter { case (_, ts) => ts.nonEmpty }
    if (nonEmpty.isEmpty) {
      // Nothing new to read — this table is already caught up to the
      // manifest's lastSuccessfulWriteTimestamp. Still advance the stored
      // HWM to that value so launch-condition stops re-dispatching this
      // table next cycle. Best-effort: a transient DDB failure self-heals
      // on the next run (which also no-ops and re-advances).
      Try(cursors.advanceHighWaterMark(table, entry.lastSuccessfulWriteTimestamp))
        .recover { case e => log.warn(s"HWM advance (no-op path) failed for $table: ${e.getMessage}") }
      log.info(s"'$table' — already up to date, no folders to read")
      return
    }

    // Bootstrap raw+merged from the first fingerprint that has new data.
    // Schema comes from a 0-row sample of one of its timestamp folders.
    val (fpBootstrap, tsBootstrap :: _) = nonEmpty.head
    val sample = readTimestamps(spark, entry.dataFilesPath, fpBootstrap, Seq(tsBootstrap),
      runLoadTsMillis, excludeCols).limit(0)
    ensureTablesExist(spark, sample, ns, tbl, rawTable, mergedTable)
    ReconStore.ensureTableExists(spark, ns)

    val sourceKey = dataFilesKey(entry.dataFilesPath, srcBucket)
    // Use the sanitized ns/tbl downstream — these feed recon DDL (ReconCtx →
    // ReconStore) and the MERGE statements (processFingerprint).
    val reconCtx = new ReconCtx(ns, runId, srcBucket, sourceKey)
    nonEmpty.foreach { case (fp, timestamps) =>
      processFingerprint(spark, tbl, entry.dataFilesPath, fp, timestamps,
        rawTable, mergedTable, cursors, reconCtx,
        runLoadTsMillis, incrementalMergeFilter, state.mergeWatermarkFor(fp), excludeCols)
    }

    // All fingerprints committed successfully (the loop would have thrown
    // otherwise). Advance the table-level HWM to the manifest snapshot
    // THIS job read — the authoritative value, consistent with the
    // per-fingerprint cursors written above. This replaces the old
    // post-Map AdvanceState Lambda, which used the launch-condition
    // Lambda's older manifest read and so lagged the cursors. Not
    // best-effort: if this fails the run fails, because a missing HWM
    // advance would make launch-condition re-dispatch needlessly.
    cursors.advanceHighWaterMark(table, entry.lastSuccessfulWriteTimestamp)

    // Tier B: cumulative recon — sum what Tier A saw across all
    // fingerprints, compare to total raw count for this table. Built
    // here, then flushed together with all the buffered Tier A rows in
    // ONE Iceberg commit (vs. N+1 commits previously).
    Try {
      val expected = reconCtx.tierAExpectedSum
      val actual = rawRowCount(spark, rawTable)
      val tierBRow = ReconStore.cumulativeRow(runId, table, expected, actual)
      ReconStore.writeRows(spark, namespace, reconCtx.allRowsWith(tierBRow))
    }.recover { case e =>
      // Recon write failure is non-fatal to ingest, but it must NOT be
      // silent — a swallowed failure here once left cda_recon_results empty
      // for a whole load while every job reported SUCCESS. Emit a structured,
      // greppable marker so the CloudWatch metric filter (RECON_WRITE_FAILED
      // → SNS alarm; see orchestration-stack) turns it into an operator alert.
      log.error(s"RECON_WRITE_FAILED table=$table tier=A+B reason=${e.getClass.getSimpleName}: ${e.getMessage}")
    }

    // Tier D runs as a separate scheduled job (state machine
    // cda-iceberg-recon, jar invoked with --recon-only). Keeping it out
    // of the hot ingest path: 4 full table scans per run would push the
    // ingest cron envelope. See docs/RECON.md for the recon contract.

    log.info(s"'$table' — done")
  }

  /** Recon-related context threaded into processFingerprint. Mutable
    * because we accumulate per-fingerprint Tier-A aggregates as the loop
    * runs, then read them out for Tier B at the end. Scoped per-table
    * (one ReconCtx per run() call) so concurrency isn't a concern. */
  private[iceberg] class ReconCtx(
    val namespace: String,
    val runId: String,
    val srcBucket: String,
    val dataFilesKey: String,
  ) {
    private var expectedSum: Long = 0L
    private val bufferedRows = scala.collection.mutable.ArrayBuffer.empty[Row]

    /** Record one fingerprint's Tier A result: tally the expected sum and
      * buffer the recon row for a single batched write at table end. */
    def accumulateTierA(landed: Long, agg: MetricsAggregate, row: Row): Unit = {
      expectedSum += agg.writtenSum
      bufferedRows += row
      val _ = landed  // retained for future per-fp diagnostics
    }
    def tierAExpectedSum: Long = expectedSum

    /** Add the Tier B row to the buffer, then return all buffered rows
      * for a single Iceberg append. */
    def allRowsWith(tierBRow: Row): Seq[Row] = (bufferedRows :+ tierBRow).toSeq
  }

  /** Strip the s3://bucket/ prefix from dataFilesPath so we can use it as
    * an S3 key prefix. Manifest's dataFilesPath is a full URL. */
  private[iceberg] def dataFilesKey(dataFilesPath: String, bucket: String): String = {
    val expectedPrefix = s"s3://$bucket/"
    if (dataFilesPath.startsWith(expectedPrefix)) dataFilesPath.substring(expectedPrefix.length)
    else dataFilesPath.stripPrefix("s3://").dropWhile(_ != '/').stripPrefix("/")
  }

  /**
    * Idempotent CREATE TABLE for both raw and merged. Uses the supplied
    * sample DataFrame's schema (already enriched with cda_fingerprint /
    * cda_load_ts) for raw, and a copy without those bookkeeping columns
    * for merged.
    *
    * Subsequent fingerprints with new columns are absorbed via
    * evolveAndAlign on each append. Type widening (int→long, float→double,
    * decimal-precision) is also handled in-flight; incompatible types
    * cause a loud failure with a clear column-level message.
    */
  private[iceberg] def ensureTablesExist(spark: SparkSession,
                                         sample: DataFrame,
                                         namespace: String,
                                         table: String,
                                         rawTable: String,
                                         mergedTable: String): Unit = {
    // SANITIZE → rebuild. requireValid returns a fresh allow-listed string;
    // we re-derive the table refs from those sanitized roots so the DDL
    // below is built ONLY from provably-safe values, never the caller's
    // (possibly tainted) inputs. Identifiers can't be SQL bind parameters,
    // so allow-list sanitization is the correct defense.
    val ns  = Identifiers.requireValid(namespace, "namespace")
    val tbl = Identifiers.requireValid(table, "table")

    // DDL via the connector catalog API (IcebergCatalog), NOT spark.sql
    // string-built DDL — identifiers are passed as typed objects, so there
    // is no SQL string for a name to be injected into. Functionally
    // identical to CREATE NAMESPACE/TABLE; one-time per table.
    IcebergCatalog.ensureNamespace(spark, Catalog, ns)

    val rawSchema = sample.schema
    // Merged drops the load-tracking bookkeeping cols (cda_fingerprint,
    // cda_load_ts) — those have no value to a consumer querying the
    // resolved current state. The gwcbi___ writer cols stay on merged for
    // now: the MERGE statement uses them and consumers may still want
    // them for audit. A future toggle can strip them per-customer.
    val mergedSchema = sample.drop(FingerprintCol, LoadTsCol).schema

    val tblProps = Map(
      "write.format.default"          -> "parquet",
      "write.target-file-size-bytes"  -> "536870912",
      "write.distribution-mode"       -> "hash",
    )

    IcebergCatalog.ensureTable(spark, Catalog, ns, s"${tbl}_raw",
      rawSchema, Seq(IcebergCatalog.identity(FingerprintCol)), tblProps)
    log.info(s"'$tbl' — ensured raw table s3tables.$ns.${tbl}_raw")

    IcebergCatalog.ensureTable(spark, Catalog, ns, s"${tbl}_merged",
      mergedSchema, Seq(IcebergCatalog.bucket(64, "id")), tblProps)
    log.info(s"'$tbl' — ensured merged table s3tables.$ns.${tbl}_merged")
  }

  /** Chronological fingerprint order driven by the manifest's schemaHistory
    * timestamps. Filters happen at processing time. */
  private[iceberg] def chronologicalFingerprints(entry: ManifestEntry): Seq[String] =
    entry.schemaHistory.toSeq.sortBy(_._2.toLong).map(_._1)

  private def processFingerprint(spark: SparkSession,
                                 table: String,
                                 dataFilesPath: String,
                                 fingerprint: String,
                                 timestamps: Seq[String],
                                 rawTable: String,
                                 mergedTable: String,
                                 cursors: CursorStore,
                                 recon: ReconCtx,
                                 runLoadTsMillis: Long,
                                 incrementalMergeFilter: Boolean,
                                 mergeWatermark: Option[String],
                                 excludeCols: Set[String]): Unit = {
    log.info(s"'$table' fingerprint=$fingerprint — reading ${timestamps.size} timestamp folder(s) " +
      s"(${timestamps.head}..${timestamps.last})")

    val src = readTimestamps(spark, dataFilesPath, fingerprint, timestamps, runLoadTsMillis, excludeCols)

    // 1. Plan + apply DDL to raw, then build a source DataFrame whose
    // schema matches raw exactly. Handles four cases: source has new
    // columns (ADD), source missing columns raw has (NULL-fill), source
    // type wider than raw's (ALTER COLUMN TYPE), source type narrower
    // (CAST in-flight, no DDL). Incompatible types fail loudly here
    // rather than at append time with a confusing Spark error.
    val (srcAligned, rawSchemaAfterEvolve) =
      evolveAndAlign(spark, rawTable, src, includeBookkeeping = true)

    // 2. Count for recon BEFORE the append (cheaper to materialize once
    // and reuse for both count + write than to re-scan raw afterwards).
    // Spark caches the partitioned plan; the count action triggers it
    // and the subsequent writeTo reuses the same DAG.
    srcAligned.cache()
    val landed = srcAligned.count()

    srcAligned.writeTo(rawTable).append()
    log.info(s"'$table' fingerprint=$fingerprint — appended ${srcAligned.rdd.getNumPartitions} partition(s) to $rawTable")
    srcAligned.unpersist()

    // 3. Advance cursor IMMEDIATELY after the raw commit. Iceberg's
    // append is atomic, so once it returns the data is durable. If the
    // job crashes between here and step 5, raw is correct and the next
    // run will skip these timestamp folders (no duplicate raw rows).
    cursors.advanceCursor(table, fingerprint, timestamps.last)

    // 4. Tier A reconciliation: sum batch-metrics numRecordsWritten for
    // the timestamp folders we just read; compare to landed rows. The
    // row is BUFFERED, not written here — all of a table's recon rows
    // are flushed in one Iceberg commit at the end of the table run
    // (one metadata commit instead of one per fingerprint).
    // Recon failure does NOT fail the job — it lands as MISMATCH /
    // METRICS_PARTIAL in the recon table for operator triage.
    Try {
      val agg = BatchMetrics.aggregate(recon.srcBucket, recon.dataFilesKey, fingerprint, timestamps)
      val row = ReconStore.batchMetricsRow(
        recon.runId, table, fingerprint,
        actualLanded = landed,
        metrics = agg,
        timestampsProcessed = timestamps.size,
        notes = s"timestamps=${timestamps.head}..${timestamps.last}",
      )
      recon.accumulateTierA(landed, agg, row)
    }.recover { case e =>
      // Structured marker (see RECON_WRITE_FAILED metric filter). A Tier A
      // build failure means this fingerprint's batch-metrics row is missing
      // from cda_recon_results — operator should know, not just a debug warn.
      log.error(s"RECON_WRITE_FAILED table=$table fingerprint=$fingerprint tier=A reason=${e.getClass.getSimpleName}: ${e.getMessage}")
    }

    // 5. Mirror the same evolution on merged. Reuse the post-evolve raw
    // schema captured above instead of re-reading raw's metadata.
    syncMergedSchema(spark, mergedTable, rawSchemaAfterEvolve)

    // 6. MERGE + DELETE. When incrementalMergeFilter is on, the MERGE
    // source is scoped to rows with cda_load_ts > mergeWatermark — only
    // rows newer than the last successful merge — so we don't re-window
    // the whole fingerprint partition on every incremental. The
    // tombstone DELETE is similarly scoped.
    val loadTsLowerBound = if (incrementalMergeFilter) mergeWatermark else None
    val mergeSql = upsertSql(rawTable, mergedTable, fingerprint, loadTsLowerBound)
    spark.sql(mergeSql)
    log.info(s"'$table' fingerprint=$fingerprint — MERGE upsert applied" +
      loadTsLowerBound.map(w => s" (load_ts > $w)").getOrElse(""))

    val deleteSql = deleteSqlStmt(rawTable, mergedTable, fingerprint, loadTsLowerBound)
    spark.sql(deleteSql)
    log.info(s"'$table' fingerprint=$fingerprint — tombstone DELETE applied")

    // 7. Advance the merge watermark — ONLY now, after MERGE+DELETE both
    // succeeded. If the job crashed before this point, the watermark stays
    // behind and the next run re-includes these rows in its MERGE source.
    // No data loss: cursor (raw) and watermark (merged) advance
    // independently, so a partial failure can't strand un-merged rows.
    if (incrementalMergeFilter) {
      cursors.advanceMergeWatermark(table, fingerprint, runLoadTsMillis.toString)
    }
  }

  /**
    * Total live row count of an Iceberg table, O(1) from snapshot metadata
    * rather than a data scan. raw is append-only, so the current snapshot's
    * `total-records` summary property equals count(*).
    *
    * Reads via the Iceberg catalog API (Spark3Util.loadIcebergTable →
    * currentSnapshot().summary()), NOT a `<table>.files` metadata-table SQL
    * query — that SQL form is mis-parsed by the S3 Tables catalog as an
    * extra namespace level and throws ("S3 Tables only supports one
    * [namespace level]"), which silently broke Tier A/B recon. The catalog
    * API resolves the table the same way the data reads do.
    *
    * Falls back to a real count(*) if the summary lacks total-records or the
    * table object can't be loaded (e.g. brand-new empty table).
    */
  private[iceberg] def rawRowCount(spark: SparkSession, rawTable: String): Long = {
    Try {
      val t = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, rawTable)
      Option(t.currentSnapshot())
        .flatMap(s => Option(s.summary().get("total-records")))
        .map(_.toLong)
    }.toOption.flatten
      .getOrElse(spark.table(rawTable).count())
  }

  /**
    * Read a specific list of timestamp folders for one fingerprint. Each
    * path becomes a separate parquet load argument so Spark's reader sees
    * exactly the files we want and nothing else. Returns a DataFrame
    * tagged with cda_fingerprint + cda_load_ts.
    */
  private[iceberg] def readTimestamps(spark: SparkSession,
                                      dataFilesPath: String,
                                      fingerprint: String,
                                      timestamps: Seq[String],
                                      runLoadTsMillis: Long,
                                      excludeCols: Set[String] = Set.empty): DataFrame = {
    val paths = timestamps.map(ts => s"$dataFilesPath/$fingerprint/$ts/")
    val raw = spark.read.parquet(paths: _*)
    // Drop excluded columns at the source, before bookkeeping columns are
    // added, so they never reach raw or merged. drop() silently ignores
    // names not present, which is fine — a column excluded but absent in
    // this fingerprint's parquet is a no-op.
    val pruned = if (excludeCols.isEmpty) raw else raw.drop(excludeCols.toSeq: _*)
    // Stamp a single deterministic load timestamp for the whole run
    // (not current_timestamp(), which is non-deterministic per-row). This
    // makes cda_load_ts a clean run-level marker the MERGE watermark can
    // compare against exactly.
    pruned
      .withColumn(FingerprintCol, lit(fingerprint))
      .withColumn(LoadTsCol, lit(new java.sql.Timestamp(runLoadTsMillis)))
  }

  // ------------------------------------------------------------------
  // Schema evolution + alignment
  // ------------------------------------------------------------------

  /** One change to apply to the target table or to the source DataFrame. */
  private[iceberg] sealed trait SchemaChange
  /** ALTER TABLE … ADD COLUMN — target lacks this column. */
  private[iceberg] final case class AddColumn(name: String, dataType: DataType) extends SchemaChange
  /** ALTER TABLE … ALTER COLUMN … TYPE — target's type widens to source's. */
  private[iceberg] final case class WidenColumn(name: String, from: DataType, to: DataType) extends SchemaChange

  /**
    * Pure planner: diff source against target, return DDL changes the
    * target needs and the projection plan for the source DataFrame.
    *
    * The projection plan is the ordered sequence of (name, expr) the
    * caller uses to build a DataFrame whose schema matches target's
    * post-DDL schema exactly:
    *   - target col present in source: cast to target type if needed
    *   - target col missing from source: lit(null) of target type
    *   - source col missing from target: ADD COLUMN, then project as-is
    *
    * Throws IllegalStateException with a precise message when types are
    * incompatible (e.g. int → string, decimal scale change). The caller
    * upstream surfaces this as a job failure with the table+col context.
    */
  private[iceberg] def planEvolution(target: StructType,
                                     source: StructType): (Seq[SchemaChange], Seq[(String, DataType, Option[DataType])]) = {
    val srcByName = source.fields.map(f => f.name -> f).toMap
    val tgtByName = target.fields.map(f => f.name -> f).toMap

    val changes = scala.collection.mutable.ArrayBuffer.empty[SchemaChange]

    // Widen target columns whose types are narrower than source's. Walk
    // target order so the eventual table schema stays predictable.
    target.fields.foreach { tgtField =>
      srcByName.get(tgtField.name).foreach { srcField =>
        if (tgtField.dataType != srcField.dataType) {
          if (canWiden(tgtField.dataType, srcField.dataType)) {
            changes += WidenColumn(tgtField.name, tgtField.dataType, srcField.dataType)
          } else if (!canCast(srcField.dataType, tgtField.dataType)) {
            throw new IllegalStateException(
              s"Incompatible type change for column '${tgtField.name}': " +
                s"target=${tgtField.dataType.sql}, source=${srcField.dataType.sql}. " +
                s"Iceberg cannot widen target, and source isn't safely castable to target.",
            )
          }
        }
      }
    }

    // Add source columns that the target lacks.
    source.fields.foreach { srcField =>
      if (!tgtByName.contains(srcField.name)) {
        changes += AddColumn(srcField.name, srcField.dataType)
      }
    }

    // Build the projection plan from the post-DDL target shape.
    // post-DDL = current target with widened types + appended new columns.
    val widenedTypes: Map[String, DataType] = changes.collect {
      case WidenColumn(n, _, t) => n -> t
    }.toMap
    val postTarget = target.fields.map { f =>
      f.copy(dataType = widenedTypes.getOrElse(f.name, f.dataType))
    } ++ changes.collect {
      case AddColumn(n, t) => StructField(n, t, nullable = true)
    }

    val projection = postTarget.toSeq.map { f =>
      val srcType = srcByName.get(f.name).map(_.dataType)
      (f.name, f.dataType, srcType)
    }

    (changes.toSeq, projection)
  }

  /** Iceberg-supported type promotions (subset that matters for CDA):
    *    int → long, float → double, decimal(p1,s) → decimal(p2,s) when p2 > p1.
    *  Everything else returns false — including scale changes, which would
    *  be data-changing.
    */
  private[iceberg] def canWiden(from: DataType, to: DataType): Boolean = (from, to) match {
    case (IntegerType, LongType) => true
    case (FloatType, DoubleType) => true
    case (a: DecimalType, b: DecimalType) => b.precision > a.precision && a.scale == b.scale
    case _ => false
  }

  /** Whether `from` casts to `to` without losing or reinterpreting data.
    * Used as a fallback when canWiden is false but the source is already
    * smaller-or-equal in domain (e.g. target=long, source=int — target
    * stays long, source casts up; no DDL needed). */
  private[iceberg] def canCast(from: DataType, to: DataType): Boolean = (from, to) match {
    case (a, b) if a == b => true
    case (IntegerType, LongType) => true
    case (FloatType, DoubleType) => true
    case (a: DecimalType, b: DecimalType) => b.precision >= a.precision && a.scale == b.scale
    case _ => false
  }

  /** Apply the DDL side of the plan against `target`. Idempotent — safe to
    * call before every fingerprint. */
  private[iceberg] def applyEvolution(spark: SparkSession,
                                      target: String,
                                      changes: Seq[SchemaChange],
                                      includeBookkeeping: Boolean): Unit = {
    changes.foreach {
      case AddColumn(name, _) if !includeBookkeeping && BookkeepingCols.contains(name) =>
        // Don't add bookkeeping cols to merged.
      case AddColumn(name, dt) =>
        // Column name comes from the CDA source schema — validate it, then
        // apply via the catalog API (typed name + DataType), NOT a built
        // ALTER string. No SQL string = no injection sink.
        val col = Identifiers.requireValid(name, "column")
        log.info(s"schema sync — ADD COLUMN $col ${dt.sql} on $target")
        IcebergCatalog.addColumn(spark, target, col, dt)
      case WidenColumn(name, _, to) =>
        val col = Identifiers.requireValid(name, "column")
        log.info(s"schema sync — ALTER COLUMN $col TYPE ${to.sql} on $target")
        IcebergCatalog.updateColumnType(spark, target, col, to)
    }
  }

  /**
    * Plan, apply, and align in one shot. Returns a DataFrame whose schema
    * matches the post-DDL target schema (column order, types, and
    * bookkeeping placement included).
    */
  private[iceberg] def evolveAndAlign(spark: SparkSession,
                                      target: String,
                                      src: DataFrame,
                                      includeBookkeeping: Boolean): (DataFrame, StructType) = {
    val tgtSchema = spark.table(target).schema
    val (changes, projection) = planEvolution(tgtSchema, src.schema)

    val effectiveChanges =
      if (includeBookkeeping) changes
      else changes.filterNot { case AddColumn(n, _) => BookkeepingCols.contains(n); case _ => false }

    applyEvolution(spark, target, effectiveChanges, includeBookkeeping)

    val cols: Seq[Column] = projection.map { case (name, tgtType, srcType) =>
      srcType match {
        case Some(st) if st == tgtType => col(name)
        case Some(_)                   => col(name).cast(tgtType).as(name)
        case None                      => lit(null).cast(tgtType).as(name)
      }
    }
    // The projection IS the post-DDL target shape (planEvolution builds it
    // from target ++ adds, with widened types applied). Reuse it as the
    // authoritative post-evolve schema so the caller doesn't re-read the
    // catalog. nullable=true is fine — Iceberg columns are nullable.
    val postSchema = StructType(projection.map { case (name, tpe, _) =>
      StructField(name, tpe, nullable = true)
    })
    (src.select(cols: _*), postSchema)
  }

  /** Bring merged up to the supplied raw schema (minus the bookkeeping
    * cols). The caller passes raw's post-evolve schema so we don't
    * re-read it from the catalog. Runs after raw's evolve step so raw
    * is the source of truth. */
  private[iceberg] def syncMergedSchema(spark: SparkSession,
                                        mergedTable: String,
                                        rawSchema: StructType): Unit = {
    val mergedSchema = spark.table(mergedTable).schema
    val (changes, _) = planEvolution(mergedSchema, rawSchema)
    applyEvolution(spark, mergedTable, changes, includeBookkeeping = false)
  }

  /**
    * MERGE INTO merged USING (the latest row per id, ops 0/2/4 only) ON id.
    * The lpad-32 big-int sort matches OSR's canonical seqval ordering.
    *
    * MATCHED guard: only update if the source row's seqval is strictly
    * greater than the target's. This makes the operation idempotent under
    * retry — replaying the same fingerprint is a no-op.
    */
  /** SQL fragment scoping a raw scan to rows newer than the merge
    * watermark (epoch-ms string). Empty when no watermark / filter off —
    * the MERGE then sees the full fingerprint partition (original
    * behavior). `timestamp_millis` converts the stored ms string to a
    * TimestampType matching the cda_load_ts column. */
  private[iceberg] def loadTsPredicate(loadTsLowerBound: Option[String]): String =
    loadTsLowerBound match {
      case Some(ms) => s"      AND $LoadTsCol > timestamp_millis($ms)\n"
      case None     => ""
    }

  private[iceberg] def upsertSql(rawTable: String,
                                 mergedTable: String,
                                 fingerprint: String,
                                 loadTsLowerBound: Option[String] = None): String =
    s"""
      |MERGE INTO $mergedTable tgt
      |USING (
      |  SELECT * EXCEPT (rn, $FingerprintCol, $LoadTsCol) FROM (
      |    SELECT *,
      |      ROW_NUMBER() OVER (
      |        PARTITION BY id
      |        ORDER BY LPAD(gwcbi___seqval_hex, 32, '0') DESC
      |      ) AS rn
      |    FROM $rawTable
      |    WHERE $FingerprintCol = '$fingerprint'
      |      AND gwcbi___operation IN (0, 2, 4)
      |${loadTsPredicate(loadTsLowerBound)}  ) WHERE rn = 1
      |) src
      |ON tgt.id = src.id
      |WHEN MATCHED AND
      |  LPAD(tgt.gwcbi___seqval_hex, 32, '0') < LPAD(src.gwcbi___seqval_hex, 32, '0')
      |  THEN UPDATE SET *
      |WHEN NOT MATCHED THEN INSERT *
      |""".stripMargin

  /** DELETE merged rows whose latest raw entry is a tombstone (op = 1). */
  private[iceberg] def deleteSqlStmt(rawTable: String,
                                     mergedTable: String,
                                     fingerprint: String,
                                     loadTsLowerBound: Option[String] = None): String =
    s"""
      |DELETE FROM $mergedTable
      |WHERE id IN (
      |  SELECT id FROM $rawTable
      |  WHERE $FingerprintCol = '$fingerprint'
      |    AND gwcbi___operation = 1
      |${loadTsPredicate(loadTsLowerBound)})
      |""".stripMargin
}
