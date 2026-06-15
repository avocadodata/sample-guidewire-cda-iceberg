package gw.cda.iceberg.state

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Selects which timestamp folders to read for one fingerprint, given:
  *   - cursor: lastTimestampFolder we've already loaded for this fingerprint
  *             (string, parsed as Long for compare). Empty string = first run.
  *   - upperBound: manifest's lastSuccessfulWriteTimestamp
  *                 (only read folders <= this; CDA may still be writing
  *                  to folders past this watermark).
  *
  * Pure logic — given a Seq of candidate timestamp strings, returns the
  * filtered + sorted list.
  */
object TimestampFolderSelector {

  /** Filter `folders` to those strictly greater than `cursor` and less
    * than or equal to `upperBound`. Result is sorted ascending so the
    * Spark job processes them in chronological order (matters for the
    * final cursor value). Non-numeric folder names are discarded
    * defensively. */
  def select(folders: Seq[String], cursor: Option[String], upperBound: String): Seq[String] = {
    val lo = cursor.flatMap(c => Try(c.toLong).toOption).getOrElse(Long.MinValue)
    val hi = Try(upperBound.toLong).getOrElse(Long.MaxValue)
    folders.flatMap(f => Try(f.toLong).toOption.map(n => (f, n)))
      .filter { case (_, n) => n > lo && n <= hi }
      .sortBy(_._2)
      .map(_._1)
  }

  /** List timestamp folders directly under
    * s3://<bucket>/<basePrefix>/<fingerprint>/ — uses S3's CommonPrefixes
    * with a delimiter so we don't enumerate every parquet object.
    *
    * Returns timestamp folder names (the leaf segment), not full paths.
    */
  def listTimestampFolders(s3: AmazonS3,
                           bucket: String,
                           basePrefix: String,
                           fingerprint: String): Seq[String] = {
    val prefix = s"${basePrefix.stripSuffix("/")}/${fingerprint}/"
    val req = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(prefix)
      .withDelimiter("/")
    val acc = scala.collection.mutable.ArrayBuffer.empty[String]
    var token: String = null
    do {
      if (token != null) req.setContinuationToken(token)
      val res = s3.listObjectsV2(req)
      res.getCommonPrefixes.asScala.foreach { p =>
        // p looks like "<basePrefix>/<fp>/<ts>/"; extract <ts>
        val trimmed = p.stripSuffix("/")
        val idx = trimmed.lastIndexOf('/')
        if (idx > 0) acc += trimmed.substring(idx + 1)
      }
      token = if (res.isTruncated) res.getNextContinuationToken else null
    } while (token != null)
    acc.toSeq
  }
}
