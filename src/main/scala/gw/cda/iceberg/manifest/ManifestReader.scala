package gw.cda.iceberg.manifest

import com.fasterxml.jackson.core.`type`.TypeReference
import gw.cda.iceberg.utils.{ObjectMapperSupplier, S3ClientSupplier}
import org.apache.logging.log4j.LogManager

/** One row of CDA's `manifest.json`.
  *
  * Shape (excerpt) for one table:
  *   "cc_account": {
  *     "lastSuccessfulWriteTimestamp": "1639166206419",
  *     "totalProcessedRecordsCount": 240000,
  *     "dataFilesPath": "s3://gw-cda/synthetic/cc_account",
  *     "schemaHistory": { "<fp1>": "1639166206419", "<fp2>": "1641103254000" }
  *   }
  *
  * Field semantics:
  *   - lastSuccessfulWriteTimestamp: high-water mark; advances each time
  *     CDA emits a new micro-batch for any fingerprint of this table.
  *   - dataFilesPath: base S3 URI; per-fingerprint folders live under it.
  *   - schemaHistory: fingerprint → epoch ms when that fingerprint was
  *     first seen. CDA may prune older fingerprints; relying on S3
  *     listings (rather than just this map) is the robust path — see
  *     IcebergIngest.discoverFingerprints.
  */
final case class ManifestEntry(
  lastSuccessfulWriteTimestamp: String,
  totalProcessedRecordsCount: Long,
  dataFilesPath: String,
  schemaHistory: Map[String, String]
)

object ManifestReader {
  type ManifestMap = Map[String, ManifestEntry]
  private val log = LogManager.getLogger(getClass.getName)

  /** Read manifest.json from S3 and return its parsed contents. Cross-account
    * is fine as long as the EMR job role has GetObject on the manifest key. */
  def processManifest(bucketName: String, manifestKey: String): ManifestMap = {
    val raw = getManifestJson(bucketName, manifestKey)
    parseManifestJson(raw)
  }

  private[manifest] def parseManifestJson(json: String): ManifestMap = {
    val typeRef = new TypeReference[Map[String, ManifestEntry]]() {}
    val parsed = ObjectMapperSupplier.jsonMapper.readValue(json, typeRef)
    log.info(s"Parsed manifest: ${parsed.size} table(s)")
    parsed
  }

  private[manifest] def getManifestJson(bucketName: String, manifestKey: String): String = {
    val body = S3ClientSupplier.s3Client.getObjectAsString(bucketName, manifestKey)
    log.info(s"Read manifest from s3://$bucketName/$manifestKey (${body.length} chars)")
    body
  }
}
