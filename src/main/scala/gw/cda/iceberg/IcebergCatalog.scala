package gw.cda.iceberg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsNamespaces, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.jdk.CollectionConverters._

/**
  * Thin wrapper over the Spark connector catalog API for DDL
  * (CREATE NAMESPACE / CREATE TABLE / ALTER TABLE).
  *
  * WHY this exists (security): the equivalent `spark.sql("CREATE TABLE ...")`
  * builds the table/column identifiers INTO a SQL string. Identifiers cannot
  * be SQL bind parameters, so static analysis (and good practice) treats
  * name-in-SQL-string as an injection sink. The connector catalog API takes
  * identifiers as typed objects (`Identifier`, `String[]`, `TableChange`)
  * and schemas as `StructType` — there is no SQL string to inject into.
  * This is the "use a safe library instead of building SQL" pattern.
  *
  * Functionally identical to the SQL form: `spark.sql` would parse-and-analyze
  * the DDL down to these same catalog calls. No data-plane impact — these are
  * one-time, per-table lifecycle operations, not per-row.
  *
  * Identifiers are still validated by [[Identifiers]] before reaching here;
  * the catalog API is defense-in-depth on top of that, and removes the
  * string-construction entirely.
  */
object IcebergCatalog {

  /** The configured Iceberg catalog (e.g. "s3tables"), as both a
    * TableCatalog and a SupportsNamespaces — Iceberg's SparkCatalog is both. */
  private def catalog(spark: SparkSession, catalogName: String): TableCatalog with SupportsNamespaces =
    spark.sessionState.catalogManager.catalog(catalogName) match {
      case c: TableCatalog with SupportsNamespaces => c
      case other =>
        throw new IllegalStateException(
          s"Catalog '$catalogName' is ${other.getClass.getName}, expected a " +
          s"TableCatalog with SupportsNamespaces (Iceberg SparkCatalog).")
    }

  /** CREATE NAMESPACE IF NOT EXISTS <catalog>.<namespace> */
  def ensureNamespace(spark: SparkSession, catalogName: String, namespace: String): Unit = {
    val cat = catalog(spark, catalogName)
    val ns = Array(namespace)
    if (!cat.namespaceExists(ns))
      // Tolerate a concurrent creator: if create fails but the namespace now
      // exists, another job won the race — treat as success. We re-check
      // existence rather than match a specific exception type because the
      // underlying catalog (S3 Tables) throws its OWN ConflictException
      // wrapped in RuntimeException, not Spark's NamespaceAlreadyExistsException.
      try cat.createNamespace(ns, Map.empty[String, String].asJava)
      catch {
        case _: org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException => ()
        case e: Throwable if cat.namespaceExists(ns) => ()  // concurrent creator won
        case e: Throwable => throw e
      }
  }

  /**
    * CREATE TABLE IF NOT EXISTS — no-op when the table already exists.
    * `partitionTransforms` are Iceberg partition specs (e.g. identity(col) or
    * bucket(64, "id")). `props` are TBLPROPERTIES.
    */
  def ensureTable(
    spark: SparkSession,
    catalogName: String,
    namespace: String,
    table: String,
    schema: StructType,
    partitionTransforms: Seq[Transform],
    props: Map[String, String],
  ): Unit = {
    val cat = catalog(spark, catalogName)
    val id = Identifier.of(Array(namespace), table)
    if (!cat.tableExists(id)) {
      // StructType overload: deprecated since Spark 3.4 in favour of a
      // Column[] form, but the converter (CatalogV2Util) is Spark-internal
      // (private[sql]) and not callable here. The deprecated overload is
      // stable and still the public path; a deprecation warning is
      // acceptable vs. depending on a private API.
      //
      // Concurrent-create race: many parallel jobs call ensureTable for the
      // SAME shared table (e.g. cda_recon_results). tableExists() can return
      // false in two jobs at once; both then call createTable and one loses.
      // The loser must treat "already exists" as success. We can't match a
      // single exception type: the S3 Tables catalog throws its OWN
      // ConflictException ("A table with an identical name already exists")
      // wrapped in RuntimeException — NOT Spark's TableAlreadyExistsException.
      // So on any create failure, re-check existence: if the table is now
      // there, a concurrent creator won and we succeed; otherwise rethrow.
      try cat.createTable(id, schema, partitionTransforms.toArray, props.asJava): Unit
      catch {
        case _: org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException => ()
        case e: Throwable if cat.tableExists(id) => ()  // concurrent creator won
        case e: Throwable => throw e
      }
    }
  }

  /** ALTER TABLE ADD COLUMN <name> <type> (nullable), addressed by a full
    * "<catalog>.<namespace>.<table>" ref. */
  def addColumn(spark: SparkSession, fullTableRef: String, columnName: String, dataType: DataType): Unit = {
    val (catName, id) = parseRef(fullTableRef)
    catalog(spark, catName).alterTable(id, TableChange.addColumn(Array(columnName), dataType, true))
    ()
  }

  /** ALTER TABLE ALTER COLUMN <name> TYPE <type> (widening), addressed by a
    * full "<catalog>.<namespace>.<table>" ref. */
  def updateColumnType(spark: SparkSession, fullTableRef: String, columnName: String, dataType: DataType): Unit = {
    val (catName, id) = parseRef(fullTableRef)
    catalog(spark, catName).alterTable(id, TableChange.updateColumnType(Array(columnName), dataType))
    ()
  }

  /** ALTER TABLE SET TBLPROPERTIES, by full ref. Idempotent. */
  def setProperties(spark: SparkSession, fullTableRef: String, props: Map[String, String]): Unit = {
    val (catName, id) = parseRef(fullTableRef)
    val changes = props.map { case (k, v) => TableChange.setProperty(k, v) }.toArray
    catalog(spark, catName).alterTable(id, changes: _*)
    ()
  }

  /** Split a "<catalog>.<namespace>.<table>" reference into its catalog name
    * and a connector Identifier(namespace, table). Requires exactly three
    * dot-separated parts (the shape this pipeline always uses). */
  private[iceberg] def parseRef(fullTableRef: String): (String, Identifier) = {
    val parts = fullTableRef.split('.')
    require(parts.length == 3,
      s"Expected <catalog>.<namespace>.<table>, got '$fullTableRef'")
    (parts(0), Identifier.of(Array(parts(1)), parts(2)))
  }

  // Partition-transform constructors (kept here so callers don't import the
  // Spark expressions API directly).
  def identity(col: String): Transform = Expressions.identity(col)
  def bucket(n: Int, col: String): Transform = Expressions.bucket(n, col)
}
