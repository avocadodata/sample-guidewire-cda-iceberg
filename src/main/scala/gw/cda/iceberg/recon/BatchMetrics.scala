// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg.recon

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.JsonMappingException
import gw.cda.iceberg.utils.{ObjectMapperSupplier, S3ClientSupplier}
import org.apache.logging.log4j.LogManager

import java.util.concurrent.ForkJoinPool
import scala.collection.parallel.CollectionConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.{Failure, Success, Try}

/**
  * One CDA-emitted batch-metrics.json file. Per the doc:
  *   tableName, schemaId, batchTimestamp, numRecordsRead,
  *   numRecordsWritten, numRecordsDropped
  *
  * For raw-side reconciliation we trust numRecordsWritten — that's
  * exactly the count CDA committed to the parquet folder, after duplicate
  * dedup, blocklist filtering, and missing-PK drops.
  */
final case class BatchMetrics(
  tableName: String,
  schemaId: String,
  batchTimestamp: String,
  numRecordsRead: Long,
  numRecordsWritten: Long,
  numRecordsDropped: Long,
)

/** Outcome of one folder's batch-metrics fetch. */
sealed trait MetricsRead
final case class MetricsOk(metrics: BatchMetrics) extends MetricsRead
/** Folder didn't have a batch-metrics.json (CDA writer race window or
  * older CDA version). Tier A skipped for this folder; doesn't fail the
  * job. */
final case class MetricsMissing(folder: String) extends MetricsRead
/** Object exists but failed to parse. Surfaces as MISMATCH in recon. */
final case class MetricsCorrupt(folder: String, reason: String) extends MetricsRead

object BatchMetrics {
  private val log = LogManager.getLogger(getClass.getName)
  private val MetricsKeyName = ".cda/batch-metrics.json"

  /** S3 key for the batch-metrics file under one timestamp folder.
    * `dataFilesKey` is the table's basePrefix relative to the bucket
    * (no s3:// scheme), e.g. "synthetic/cc_account". */
  def metricsKey(dataFilesKey: String, fingerprint: String, timestamp: String): String =
    s"${dataFilesKey.stripSuffix("/")}/$fingerprint/$timestamp/$MetricsKeyName"

  /** Read one folder's batch-metrics.json. Wraps S3 errors so a missing
    * file becomes MetricsMissing rather than a job failure. */
  def read(bucket: String, dataFilesKey: String, fingerprint: String, timestamp: String): MetricsRead = {
    val key = metricsKey(dataFilesKey, fingerprint, timestamp)
    Try(S3ClientSupplier.s3Client.getObjectAsString(bucket, key)) match {
      case Failure(_: com.amazonaws.services.s3.model.AmazonS3Exception) =>
        // Most likely 404. Could also be 403 if the bucket policy doesn't
        // include the .cda prefix — in either case treat as missing and
        // let the caller log it. (Distinguishing 404 from 403 here would
        // double the API surface area for no real benefit.)
        log.info(s"batch-metrics missing for $key")
        MetricsMissing(timestamp)
      case Failure(e) =>
        MetricsCorrupt(timestamp, e.getClass.getSimpleName + ": " + e.getMessage)
      case Success(body) =>
        Try(ObjectMapperSupplier.jsonMapper.readValue(body, new TypeReference[BatchMetrics]() {})) match {
          case Success(m) => MetricsOk(m)
          case Failure(e: JsonMappingException) =>
            MetricsCorrupt(timestamp, "json-mapping: " + e.getOriginalMessage)
          case Failure(e) =>
            MetricsCorrupt(timestamp, e.getClass.getSimpleName + ": " + e.getMessage)
        }
    }
  }

  /** Bounded thread pool used by `aggregate` to fetch batch-metrics.json
    * files in parallel. S3 GetObject is network-bound, so oversubscribing
    * the driver's vCPU count is fine. 16 is a safe ceiling that keeps
    * the AWS SDK v1 connection pool (default 50) from being exhausted
    * while still hiding the per-request latency.
    *
    * Lazy-initialized at first use; daemon threads so the JVM can exit
    * on Spark stop without explicit shutdown. */
  private lazy val parallelism = math.max(4, math.min(16,
    sys.env.get("BATCH_METRICS_PARALLELISM").flatMap(_.toIntOption).getOrElse(16)))
  private lazy val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(parallelism))

  /** Aggregate the writer-counts for a list of timestamp folders. Missing
    * files contribute 0 expected and increment the missing counter so the
    * caller can decide whether to flag the recon as partial.
    *
    * S3 GetObject is parallelized across `parallelism` threads (default 16)
    * because per-request latency dominates and we'd otherwise be doing
    * hundreds of serial calls per fingerprint commit. */
  def aggregate(bucket: String,
                dataFilesKey: String,
                fingerprint: String,
                timestamps: Seq[String]): MetricsAggregate = {
    val par = timestamps.par
    par.tasksupport = taskSupport
    val reads = par.map(ts => read(bucket, dataFilesKey, fingerprint, ts)).seq
    val ok = reads.collect { case MetricsOk(m) => m }
    val missing = reads.collect { case MetricsMissing(ts) => ts }
    val corrupt = reads.collect { case MetricsCorrupt(ts, why) => (ts, why) }
    MetricsAggregate(
      writtenSum = ok.map(_.numRecordsWritten).sum,
      readSum    = ok.map(_.numRecordsRead).sum,
      droppedSum = ok.map(_.numRecordsDropped).sum,
      okFolders = ok.size,
      missingFolders = missing,
      corruptFolders = corrupt,
    )
  }
}

/** Roll-up across one fingerprint's timestamp folders. */
final case class MetricsAggregate(
  writtenSum: Long,
  readSum: Long,
  droppedSum: Long,
  okFolders: Int,
  missingFolders: Seq[String],
  corruptFolders: Seq[(String, String)],
) {
  /** Was every folder accounted for (no missing, no corrupt)? Drives
    * the recon status — partial coverage gets flagged separately. */
  def isComplete: Boolean = missingFolders.isEmpty && corruptFolders.isEmpty
}
