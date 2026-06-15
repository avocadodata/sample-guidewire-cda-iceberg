package gw.cda.iceberg.state

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, GetItemRequest, UpdateItemRequest}
import org.apache.logging.log4j.LogManager

import scala.jdk.CollectionConverters._

/**
  * Per-(table, fingerprint) cursor store backed by the launch-condition
  * DynamoDB table. Schema:
  *
  *   PK: tableName (S)
  *     lastSuccessfulWriteTimestamp (S)   high-water from manifest;
  *                                        advanced by AdvanceState lambda
  *     fingerprintCursors (M)             { fingerprintId -> lastTimestampFolder (S) }
  *
  * The Spark job:
  *   1. reads the cursor map at the start of each table run
  *   2. uses cursor[fp] to filter timestamp folders during read
  *   3. writes cursor[fp] = max(processedTimestamp) at the end of each
  *      fingerprint commit so a crash mid-table doesn't lose progress on
  *      already-committed fingerprints
  *
  * Idempotent under retry: cursor advance only moves forward (max-merge),
  * never resets. Replaying a fingerprint sees cursor >= processed.max → no
  * timestamp folders selected → no-op.
  */
object CursorStore {
  /** Snapshot of one CDA table's bookmark in DynamoDB. */
  final case class State(
    cursors: Map[String, String],
    highWaterMark: Option[String],
    mergeWatermarks: Map[String, String] = Map.empty,
  ) {
    /** Effective cursor for a given fingerprint:
      *   - per-fingerprint cursor if present (newer pipeline state)
      *   - else the table-level high-water mark (migrated bookmark)
      *   - else None (first-ever run for this table)
      */
    def cursorFor(fingerprint: String): Option[String] =
      cursors.get(fingerprint).orElse(highWaterMark)

    /** Merge watermark (a cda_load_ts epoch-ms string) for a fingerprint.
      * Advances ONLY after a successful MERGE+DELETE — distinct from the
      * cursor, which advances after the raw append. Used to scope the
      * MERGE source to rows newer than the last successful merge when the
      * incremental-merge-filter optimization is enabled. None = no merge
      * has succeeded yet for this fingerprint (MERGE sees all rows). */
    def mergeWatermarkFor(fingerprint: String): Option[String] =
      mergeWatermarks.get(fingerprint)
  }
}

class CursorStore(tableName: String) {
  private val log = LogManager.getLogger(getClass.getName)
  private val ddb: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient()

  /** Read both the per-fingerprint cursor map and the high-water mark
    * (lastSuccessfulWriteTimestamp). The HWM is the migration fallback —
    * old deployments had only the HWM and no per-fingerprint cursors;
    * treating the HWM as the cursor for every fingerprint means
    * "everything ≤ HWM is already loaded," which is exactly the
    * incremental-read invariant we need. */
  def readState(cdaTable: String): CursorStore.State = {
    val req = new GetItemRequest()
      .withTableName(tableName)
      .withKey(Map("tableName" -> new AttributeValue(cdaTable)).asJava)
      .withConsistentRead(true)
    val resp = ddb.getItem(req)
    val item = Option(resp.getItem)
    val cursors = item
      .flatMap(it => Option(it.get("fingerprintCursors")))
      .map(_.getM.asScala.toMap.view.mapValues(_.getS).toMap)
      .getOrElse(Map.empty)
    val highWater = item
      .flatMap(it => Option(it.get("lastSuccessfulWriteTimestamp")))
      .map(_.getS)
    val mergeWatermarks = item
      .flatMap(it => Option(it.get("mergeWatermarks")))
      .map(_.getM.asScala.toMap.view.mapValues(_.getS).toMap)
      .getOrElse(Map.empty)
    CursorStore.State(cursors, highWater, mergeWatermarks)
  }

  /** Convenience pass-through for callers that only need the cursor map. */
  def readCursors(cdaTable: String): Map[String, String] =
    readState(cdaTable).cursors

  /** Move cursor[fingerprint] forward to `newCursor`, but only if it's
    * strictly greater than the existing value. Idempotent — concurrent or
    * retried writers all converge on max().
    *
    * Two phases because DynamoDB can't SET map.key if the parent map
    * doesn't exist yet:
    *   1. Ensure fingerprintCursors map exists (no-op if present).
    *   2. Conditional SET on the specific fingerprint key. ConditionalCheck
    *      failure means another worker advanced past us — nothing to do.
    */
  def advanceCursor(cdaTable: String, fingerprint: String, newCursor: String): Unit = {
    // Phase 1: ensure parent map exists. if_not_exists keeps an existing
    // value if one's there; the empty-map fallback is set on first touch.
    val initReq = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(Map("tableName" -> new AttributeValue(cdaTable)).asJava)
      .withUpdateExpression("SET fingerprintCursors = if_not_exists(fingerprintCursors, :empty)")
      .withExpressionAttributeValues(
        Map(":empty" -> new AttributeValue().withM(Map.empty[String, AttributeValue].asJava)).asJava,
      )
    ddb.updateItem(initReq)

    // Phase 2: max-merge the cursor. Only writes when current is missing
    // or strictly less than newCursor.
    val advReq = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(Map("tableName" -> new AttributeValue(cdaTable)).asJava)
      .withUpdateExpression("SET fingerprintCursors.#fp = :new")
      .withConditionExpression(
        "attribute_not_exists(fingerprintCursors.#fp) OR fingerprintCursors.#fp < :new",
      )
      .withExpressionAttributeNames(Map("#fp" -> fingerprint).asJava)
      .withExpressionAttributeValues(Map(":new" -> new AttributeValue(newCursor)).asJava)
    try {
      ddb.updateItem(advReq)
      log.info(s"cursor advance — table=$cdaTable fp=$fingerprint -> $newCursor")
    } catch {
      case _: com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException =>
        log.info(s"cursor unchanged — table=$cdaTable fp=$fingerprint already >= $newCursor")
    }
  }

  /** Advance the table-level high-water mark (lastSuccessfulWriteTimestamp).
    * The Spark job is the authoritative writer: it reads the manifest at
    * job time (minutes after the launch-condition Lambda did) and knows
    * the true `lastSuccessfulWriteTimestamp` it processed up to. Writing
    * the HWM here — rather than in a post-Map Lambda using the Lambda's
    * older manifest read — keeps the HWM consistent with the
    * fingerprintCursors this same job wrote.
    *
    * Max-merge conditional: only advances forward. Concurrent/retried
    * writers converge; a stale value never moves the HWM backward. */
  def advanceHighWaterMark(cdaTable: String, newHwm: String): Unit = {
    val req = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(Map("tableName" -> new AttributeValue(cdaTable)).asJava)
      .withUpdateExpression("SET lastSuccessfulWriteTimestamp = :new")
      .withConditionExpression(
        "attribute_not_exists(lastSuccessfulWriteTimestamp) OR lastSuccessfulWriteTimestamp < :new",
      )
      .withExpressionAttributeValues(Map(":new" -> new AttributeValue(newHwm)).asJava)
    try {
      ddb.updateItem(req)
      log.info(s"hwm advance — table=$cdaTable -> $newHwm")
    } catch {
      case _: com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException =>
        log.info(s"hwm unchanged — table=$cdaTable already >= $newHwm")
    }
  }

  /** Advance the merge watermark for a fingerprint. MUST be called only
    * after a successful MERGE+DELETE. Decoupled from advanceCursor so a
    * crash between the raw append (cursor advances) and the merge leaves
    * the watermark behind — the next run's MERGE then re-includes the
    * un-merged rows. Same 2-phase pattern as advanceCursor; max-merge
    * conditional so retries/concurrent writers converge.
    *
    * `newWatermark` should be the max cda_load_ts of the rows just
    * merged — in practice the ingest run's own load timestamp. */
  def advanceMergeWatermark(cdaTable: String, fingerprint: String, newWatermark: String): Unit = {
    val initReq = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(Map("tableName" -> new AttributeValue(cdaTable)).asJava)
      .withUpdateExpression("SET mergeWatermarks = if_not_exists(mergeWatermarks, :empty)")
      .withExpressionAttributeValues(
        Map(":empty" -> new AttributeValue().withM(Map.empty[String, AttributeValue].asJava)).asJava,
      )
    ddb.updateItem(initReq)

    val advReq = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(Map("tableName" -> new AttributeValue(cdaTable)).asJava)
      .withUpdateExpression("SET mergeWatermarks.#fp = :new")
      .withConditionExpression(
        "attribute_not_exists(mergeWatermarks.#fp) OR mergeWatermarks.#fp < :new",
      )
      .withExpressionAttributeNames(Map("#fp" -> fingerprint).asJava)
      .withExpressionAttributeValues(Map(":new" -> new AttributeValue(newWatermark)).asJava)
    try {
      ddb.updateItem(advReq)
      log.info(s"merge watermark advance — table=$cdaTable fp=$fingerprint -> $newWatermark")
    } catch {
      case _: com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException =>
        log.info(s"merge watermark unchanged — table=$cdaTable fp=$fingerprint already >= $newWatermark")
    }
  }
}
