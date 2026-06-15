package gw.cda.iceberg

/**
  * Per-table column exclusion. A customer who needs to keep certain CDA
  * columns out of their warehouse — PII they don't want copied, large
  * blob columns they don't query, or the gwcbi___ writer columns once
  * they trust the pipeline — configures a spec here.
  *
  * Excluded columns are dropped from the source DataFrame BEFORE the raw
  * append, so they never land in either `_raw` or `_merged`. (Dropping
  * only from merged would leave them in raw — a half-measure that still
  * copies the data.)
  *
  * Spec format (the COLUMNS_TO_EXCLUDE env var, comma-separated):
  *   - `colName`            — exclude this column from EVERY table
  *   - `table:colName`      — exclude this column from one table only
  * Whitespace around entries is trimmed. Case-insensitive on column names
  * (CDA lowercases everything) and table names.
  *
  * Example:
  *   "ssn, taxid, cc_claim:description, __synth_col_0_ext"
  *   → drops ssn + taxid + __synth_col_0_ext from all tables, and
  *     description only from cc_claim.
  *
  * PROTECTED columns can never be excluded — they're load-bearing for the
  * MERGE. Attempting to exclude one is a loud failure, not a silent skip,
  * because silently keeping a column the operator asked to drop (e.g. a
  * PII field) is worse than failing the job.
  */
object ColumnExclusion {

  /** Columns the MERGE depends on; excluding any of these would break
    * upsert ordering / identity / tombstoning. */
  val Protected: Set[String] = Set("id", "gwcbi___seqval_hex", "gwcbi___operation")

  /** Parsed exclusion spec. */
  final case class Spec(
    /** Columns excluded from every table. */
    global: Set[String],
    /** table -> columns excluded from just that table. */
    perTable: Map[String, Set[String]],
  ) {
    /** All columns to drop for a given table (global ∪ table-specific). */
    def columnsFor(table: String): Set[String] =
      global ++ perTable.getOrElse(table.toLowerCase, Set.empty)

    def isEmpty: Boolean = global.isEmpty && perTable.isEmpty
  }

  val Empty: Spec = Spec(Set.empty, Map.empty)

  /**
    * Parse the COLUMNS_TO_EXCLUDE spec string. Throws IllegalArgumentException
    * if any entry targets a protected column — the operator must know their
    * exclusion request can't be honored rather than have it silently ignored.
    */
  def parse(raw: String): Spec = {
    val entries = Option(raw).getOrElse("")
      .split(",").map(_.trim).filter(_.nonEmpty)

    val global = scala.collection.mutable.Set.empty[String]
    val perTable = scala.collection.mutable.Map.empty[String, Set[String]]

    entries.foreach { entry =>
      val colonIdx = entry.indexOf(':')
      if (colonIdx < 0) {
        val col = entry.toLowerCase
        requireNotProtected(col, scope = "all tables")
        global += col
      } else {
        val tbl = entry.substring(0, colonIdx).trim.toLowerCase
        val col = entry.substring(colonIdx + 1).trim.toLowerCase
        if (tbl.isEmpty || col.isEmpty)
          throw new IllegalArgumentException(
            s"Invalid columnsToExclude entry '$entry' — expected 'col' or 'table:col'")
        requireNotProtected(col, scope = s"table '$tbl'")
        perTable.update(tbl, perTable.getOrElse(tbl, Set.empty) + col)
      }
    }
    Spec(global.toSet, perTable.toMap)
  }

  private def requireNotProtected(col: String, scope: String): Unit =
    if (Protected.contains(col))
      throw new IllegalArgumentException(
        s"Cannot exclude protected column '$col' (requested for $scope). " +
        s"Columns ${Protected.mkString(", ")} are required by the MERGE for " +
        s"identity, ordering, and tombstoning.")
}
