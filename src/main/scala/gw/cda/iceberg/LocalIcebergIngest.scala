// Copyright Amazon.com and its affiliates; all rights reserved.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession
import java.io.File

/**
  * Local runner for ingesting Guidewire CDA Parquet files into local Apache Iceberg tables.
  *
  * Directly exercises the core production codebase:
  * - [[IcebergCatalog]] for DDL (safe API)
  * - [[IcebergIngest.ensureTablesExist]] for raw & merged table creation
  * - [[IcebergIngest.readTimestamps]] for Parquet read & metadata enrichment
  * - [[IcebergIngest.evolveAndAlign]] for in-flight schema evolution
  * - [[IcebergIngest.syncMergedSchema]] for keeping merged in sync with raw
  * - [[IcebergIngest.upsertSql]] for canonical LPAD-32 deduplication MERGE
  * - [[IcebergIngest.deleteSqlStmt]] for tombstone DELETE (op = 1)
  *
  * All cloud-specific dependencies (S3, DynamoDB, Lambda, EMR Serverless) are bypassed.
  */
object LocalIcebergIngest {

  private val log = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: LocalIcebergIngest <table> <namespace> <sourcePath> [catalog] [fingerprint]")
      sys.exit(1)
    }

    val table = args(0)
    val namespace = args(1)
    val sourcePath = args(2)
    val catalog = if (args.length > 3) args(3) else "local"
    val fingerprint = if (args.length > 4) args(4) else "fp_v1"

    val spark = SparkSession.builder()
      .appName(s"local-iceberg-ingest-$table")
      .getOrCreate()

    try {
      run(spark, table, namespace, sourcePath, catalog, fingerprint)
    } finally {
      spark.stop()
    }
  }

  /**
    * Executes the local ingestion pipeline for a given table.
    */
  def run(spark: SparkSession,
          table: String,
          namespace: String,
          sourcePath: String,
          catalog: String = "local",
          fingerprint: String = "fp_v1"): Unit = {

    val tbl = Identifiers.requireValid(table, "table")
    val ns  = Identifiers.requireValid(namespace, "namespace")
    val cat = Identifiers.requireValid(catalog, "catalog")
    val fp  = Identifiers.requireValid(fingerprint, "fingerprint")

    val rawTable = s"$cat.$ns.${tbl}_raw"
    val mergedTable = s"$cat.$ns.${tbl}_merged"

    log.info(s"Starting LocalIcebergIngest for table='$tbl', namespace='$ns', catalog='$cat', sourcePath='$sourcePath'")

    // Discover local timestamp folders
    val fpDir = new File(s"$sourcePath/$fp")
    if (!fpDir.exists() || !fpDir.isDirectory) {
      throw new IllegalArgumentException(s"Source fingerprint directory does not exist: ${fpDir.getAbsolutePath}")
    }

    val timestampFolders = fpDir.listFiles()
      .filter(_.isDirectory)
      .map(_.getName)
      .filter(name => name.matches("\\d+"))
      .sorted
      .toSeq

    if (timestampFolders.isEmpty) {
      log.warn(s"No timestamp folders found in ${fpDir.getAbsolutePath}")
      return
    }

    log.info(s"Discovered ${timestampFolders.size} timestamp folder(s): ${timestampFolders.mkString(", ")}")

    // Bootstrap table schemas from a 0-row sample of the first timestamp folder
    val runLoadTsMillis = System.currentTimeMillis()
    val sample = IcebergIngest.readTimestamps(spark, sourcePath, fp, Seq(timestampFolders.head), runLoadTsMillis).limit(0)
    IcebergIngest.ensureTablesExist(spark, sample, ns, tbl, rawTable, mergedTable, catalogName = cat)

    val excludeCols = ColumnExclusion.parse(sys.env.getOrElse("COLUMNS_TO_EXCLUDE", "")).columnsFor(tbl)

    // Process each timestamp folder in chronological order
    timestampFolders.foreach { ts =>
      val tsLoadMillis = System.currentTimeMillis()
      log.info(s"Processing timestamp folder '$ts' for $tbl...")

      val src = IcebergIngest.readTimestamps(spark, sourcePath, fp, Seq(ts), tsLoadMillis, excludeCols)

      // 1. Schema evolution and alignment against raw
      val (srcAligned, rawSchemaAfterEvolve) =
        IcebergIngest.evolveAndAlign(spark, rawTable, src, includeBookkeeping = true)

      // 2. Append into raw (atomic Iceberg append)
      srcAligned.writeTo(rawTable).append()
      log.info(s"Appended batch '$ts' into $rawTable")

      // 3. Mirror schema evolution onto merged table
      IcebergIngest.syncMergedSchema(spark, mergedTable, rawSchemaAfterEvolve)

      // 4. Canonical Guidewire MERGE upsert (deduped by LPAD-32 seqval)
      val mergeSql = IcebergIngest.upsertSql(rawTable, mergedTable, fp, loadTsLowerBound = None)
      spark.sql(mergeSql)
      log.info(s"MERGE upsert applied to $mergedTable for timestamp '$ts'")

      // 5. Canonical Guidewire tombstone DELETE (gwcbi___operation = 1)
      val deleteSql = IcebergIngest.deleteSqlStmt(rawTable, mergedTable, fp, loadTsLowerBound = None)
      spark.sql(deleteSql)
      log.info(s"Tombstone DELETE applied to $mergedTable for timestamp '$ts'")
    }

    log.info(s"LocalIcebergIngest completed successfully for '$tbl'.")
  }
}
