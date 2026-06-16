// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg

/**
  * Validation for SQL identifiers (namespace, table, column names) that get
  * interpolated into Spark DDL strings.
  *
  * Why this exists: table/column names cannot be passed as SQL bind
  * parameters — they're identifiers, not values — so DDL like
  * `CREATE TABLE $name (...)` and `ALTER TABLE $t ADD COLUMN $c ...` must
  * build the name into the string. Those names originate from CLI args (the
  * orchestrator passes the CDA table name) and from the CDA source parquet
  * schema (column names Guidewire emits). That is upstream data, so we
  * validate it against a strict allow-list BEFORE it reaches any DDL —
  * rejecting anything that isn't a plain identifier closes the
  * SQL-injection path a malformed/hostile manifest or schema could open.
  *
  * CDA identifiers are lower-case alphanumeric + underscore (e.g.
  * `cc_policyperiod`, `gwcbi___seqval_hex`, `ccx_hipropertyother_ext`). The
  * pattern is deliberately strict: a name with a space, quote, semicolon,
  * backtick, dot, or dash is rejected rather than escaped.
  */
object Identifiers {

  /** Allowed: a letter/underscore start, then letters/digits/underscores.
    * Bounded length guards against absurd inputs. */
  private val Valid = "^[A-Za-z_][A-Za-z0-9_]{0,127}$".r

  def isValid(id: String): Boolean =
    id != null && Valid.pattern.matcher(id).matches()

  /**
    * SANITIZER for SQL identifiers. Validates `id` against the strict
    * allow-list and returns a NEW string rebuilt character-by-character from
    * a known-safe alphabet — it does NOT return the original reference.
    * Throws IllegalArgumentException on any disallowed character.
    *
    * Rebuilding (rather than returning `id` unchanged) is deliberate: it
    * makes the returned value provably a function of the allow-list alone,
    * so it is safe to interpolate into DDL where identifiers cannot be bind
    * parameters. This is the only safe way to put a table/column name into
    * `CREATE`/`ALTER` SQL — prepared statements cannot parameterize
    * identifiers.
    */
  def requireValid(id: String, role: String): String = {
    if (id == null)
      throw new IllegalArgumentException(s"Invalid SQL identifier for $role: null")
    val sb = new StringBuilder(id.length)
    var i = 0
    while (i < id.length) {
      val c = id.charAt(i)
      val ok =
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c == '_') || (i > 0 && c >= '0' && c <= '9')
      if (!ok)
        throw new IllegalArgumentException(
          s"Invalid SQL identifier for $role: ${quoteForMessage(id)}. " +
          s"Expected [A-Za-z_][A-Za-z0-9_]{0,127} (CDA names are lowercase " +
          s"alphanumeric + underscore). Rejected to prevent SQL injection " +
          s"via table/column names.")
      sb.append(c)            // append the LITERAL safe char, not a slice of id
      i += 1
    }
    if (sb.isEmpty || sb.length > 128)
      throw new IllegalArgumentException(
        s"Invalid SQL identifier length for $role: ${quoteForMessage(id)}")
    sb.toString()             // fresh string, derived only from the allow-list
  }

  /** Validate every name in a collection (e.g. all columns of a schema). */
  def requireAllValid(ids: Iterable[String], role: String): Unit =
    ids.foreach(requireValid(_, role))

  private def quoteForMessage(id: String): String =
    if (id == null) "null" else "'" + id.replace("\n", "\\n").take(64) + "'"
}
