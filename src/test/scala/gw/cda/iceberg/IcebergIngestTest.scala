// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg

import gw.cda.iceberg.manifest.ManifestEntry
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IcebergIngestTest extends AnyFunSpec with Matchers {

  private def schema(fields: (String, DataType)*): StructType =
    StructType(fields.map { case (n, t) => StructField(n, t, nullable = true) })

  describe("IcebergCatalog.parseRef") {
    import gw.cda.iceberg.IcebergCatalog
    it("splits <catalog>.<namespace>.<table> into catalog + Identifier") {
      val (cat, id) = IcebergCatalog.parseRef("s3tables.cda.cc_account_raw")
      cat shouldEqual "s3tables"
      id.namespace().toSeq shouldEqual Seq("cda")
      id.name() shouldEqual "cc_account_raw"
    }
    it("rejects a ref that isn't exactly 3 parts") {
      an [IllegalArgumentException] should be thrownBy IcebergCatalog.parseRef("cda.cc_account_raw")
      an [IllegalArgumentException] should be thrownBy IcebergCatalog.parseRef("a.b.c.d")
    }
  }

  describe("Identifiers.requireValid / isValid") {
    it("accepts normal CDA identifiers") {
      Seq("cc_policyperiod", "gwcbi___seqval_hex", "ccx_hipropertyother_ext",
          "cda", "cda_recon_results", "_x", "A1").foreach { id =>
        Identifiers.isValid(id) shouldBe true
        Identifiers.requireValid(id, "table") shouldEqual id
      }
    }
    it("rejects injection / malformed identifiers") {
      Seq(
        "cc; DROP TABLE x", "a b", "a-b", "a.b", "a`b", "a'b", "a\"b",
        "1abc", "", "a)", "x--", "tbl/*", null,
      ).foreach { id =>
        Identifiers.isValid(id) shouldBe false
        an [IllegalArgumentException] should be thrownBy Identifiers.requireValid(id, "table")
      }
    }
    it("rejects an over-long identifier (>128 chars)") {
      Identifiers.isValid("a" * 129) shouldBe false
    }
    it("requireAllValid throws on the first bad name in a collection") {
      an [IllegalArgumentException] should be thrownBy
        Identifiers.requireAllValid(Seq("ok_one", "bad name", "ok_two"), "column")
    }
    it("returns a freshly-rebuilt string (sanitizer), value-equal to input") {
      // The return is built char-by-char from the safe alphabet, NOT the
      // original reference — this is what breaks the taint flow into DDL.
      val in = new String("cc_policyperiod".toCharArray)  // force a distinct instance
      val out = Identifiers.requireValid(in, "table")
      out shouldEqual in            // same characters
      (out eq in) shouldBe false    // but a different String object (rebuilt)
    }
  }

  describe("ColumnExclusion.parse") {
    it("returns Empty for an empty / null spec") {
      ColumnExclusion.parse("").isEmpty shouldBe true
      ColumnExclusion.parse(null).isEmpty shouldBe true
    }

    it("parses a global column (no colon) applied to all tables") {
      val s = ColumnExclusion.parse("ssn")
      s.global shouldEqual Set("ssn")
      s.columnsFor("cc_claim") shouldEqual Set("ssn")
      s.columnsFor("cc_policy") shouldEqual Set("ssn")
    }

    it("parses a table-scoped column (table:col)") {
      val s = ColumnExclusion.parse("cc_claim:description")
      s.global shouldBe empty
      s.columnsFor("cc_claim") shouldEqual Set("description")
      s.columnsFor("cc_policy") shouldBe empty
    }

    it("merges global and table-scoped for a given table") {
      val s = ColumnExclusion.parse("ssn, taxid, cc_claim:description")
      s.columnsFor("cc_claim") shouldEqual Set("ssn", "taxid", "description")
      s.columnsFor("cc_policy") shouldEqual Set("ssn", "taxid")
    }

    it("lowercases column and table names (CDA is lowercase)") {
      val s = ColumnExclusion.parse("CC_Claim:Description, SSN")
      s.columnsFor("cc_claim") shouldEqual Set("description", "ssn")
    }

    it("trims whitespace around entries") {
      val s = ColumnExclusion.parse("  ssn ,  cc_claim:description  ")
      s.columnsFor("cc_claim") shouldEqual Set("ssn", "description")
    }

    it("rejects excluding a protected column globally") {
      an [IllegalArgumentException] should be thrownBy ColumnExclusion.parse("id")
      an [IllegalArgumentException] should be thrownBy ColumnExclusion.parse("gwcbi___seqval_hex")
      an [IllegalArgumentException] should be thrownBy ColumnExclusion.parse("gwcbi___operation")
    }

    it("rejects excluding a protected column per-table") {
      an [IllegalArgumentException] should be thrownBy ColumnExclusion.parse("cc_claim:id")
    }

    it("rejects a malformed table:col entry") {
      an [IllegalArgumentException] should be thrownBy ColumnExclusion.parse("cc_claim:")
      an [IllegalArgumentException] should be thrownBy ColumnExclusion.parse(":description")
    }
  }

  describe("IcebergIngest.chronologicalFingerprints") {
    it("orders fingerprints by timestamp value, not lexically") {
      // Two fingerprints with timestamps where lex order disagrees with
      // numeric order: '999' lex-greater than '1000' but numerically smaller.
      val entry = ManifestEntry(
        lastSuccessfulWriteTimestamp = "1000",
        totalProcessedRecordsCount = 0,
        dataFilesPath = "s3://bucket/table",
        schemaHistory = Map(
          "fp_a" -> "1000",
          "fp_b" -> "999"
        )
      )
      IcebergIngest.chronologicalFingerprints(entry) shouldEqual Seq("fp_b", "fp_a")
    }

    it("returns the only fingerprint when there is one") {
      val entry = ManifestEntry("1", 0, "s3://b/t", Map("only" -> "1"))
      IcebergIngest.chronologicalFingerprints(entry) shouldEqual Seq("only")
    }

    it("returns empty when schemaHistory is empty") {
      val entry = ManifestEntry("0", 0, "s3://b/t", Map.empty)
      IcebergIngest.chronologicalFingerprints(entry) shouldBe empty
    }
  }

  describe("IcebergIngest.upsertSql") {
    val sql = IcebergIngest.upsertSql(
      "s3tables.cda.cc_account_raw",
      "s3tables.cda.cc_account_merged",
      "abc123"
    )

    it("scopes the source CTE to the given fingerprint") {
      sql should include ("cda_fingerprint = 'abc123'")
    }

    it("filters to upsert ops 0/2/4 only — no tombstones") {
      sql should include ("gwcbi___operation IN (0, 2, 4)")
    }

    it("uses lpad-32 ordering for the within-batch dedup") {
      sql should include ("ORDER BY LPAD(gwcbi___seqval_hex, 32, '0') DESC")
    }

    it("guards the UPDATE with the lpad-32 seqval comparison") {
      sql should include (
        "LPAD(tgt.gwcbi___seqval_hex, 32, '0') < LPAD(src.gwcbi___seqval_hex, 32, '0')"
      )
    }

    it("inserts when the id is not already present") {
      sql should include ("WHEN NOT MATCHED THEN INSERT *")
    }

    it("excludes the per-row metadata cols from INSERT/UPDATE") {
      sql should include ("EXCEPT (rn, cda_fingerprint, cda_load_ts)")
    }

    it("does NOT include a load_ts predicate when no lower bound given") {
      sql should not include ("timestamp_millis")
    }

    it("scopes the source to load_ts > watermark when a lower bound is given") {
      val filtered = IcebergIngest.upsertSql(
        "s3tables.cda.cc_account_raw",
        "s3tables.cda.cc_account_merged",
        "abc123",
        Some("1700000000000"),
      )
      filtered should include ("cda_load_ts > timestamp_millis(1700000000000)")
      // The dedup + guard semantics must be unchanged by the filter.
      filtered should include ("gwcbi___operation IN (0, 2, 4)")
      filtered should include (
        "LPAD(tgt.gwcbi___seqval_hex, 32, '0') < LPAD(src.gwcbi___seqval_hex, 32, '0')"
      )
    }
  }

  describe("IcebergIngest.deleteSqlStmt") {
    val sql = IcebergIngest.deleteSqlStmt(
      "s3tables.cda.cc_account_raw",
      "s3tables.cda.cc_account_merged",
      "abc123"
    )

    it("scopes deletes to the given fingerprint") {
      sql should include ("cda_fingerprint = 'abc123'")
    }

    it("targets only tombstone rows (op = 1)") {
      sql should include ("gwcbi___operation = 1")
    }

    it("deletes from the merged table") {
      sql should include regex """DELETE FROM s3tables\.cda\.cc_account_merged"""
    }

    it("scopes the tombstone scan to load_ts > watermark when given") {
      val filtered = IcebergIngest.deleteSqlStmt(
        "s3tables.cda.cc_account_raw",
        "s3tables.cda.cc_account_merged",
        "abc123",
        Some("1700000000000"),
      )
      filtered should include ("cda_load_ts > timestamp_millis(1700000000000)")
      filtered should include ("gwcbi___operation = 1")
    }
  }

  describe("IcebergIngest.loadTsPredicate") {
    it("is empty when no lower bound (filter off / no watermark)") {
      IcebergIngest.loadTsPredicate(None) shouldEqual ""
    }
    it("emits an AND cda_load_ts > timestamp_millis(...) clause when bounded") {
      IcebergIngest.loadTsPredicate(Some("123")) should include (
        "AND cda_load_ts > timestamp_millis(123)"
      )
    }
  }

  describe("MergedInvariants SQL builders") {
    import gw.cda.iceberg.recon.MergedInvariants
    val raw = "s3tables.cda.cc_account_raw"
    val merged = "s3tables.cda.cc_account_merged"

    it("tombstone integrity counts merged rows whose latest raw op is 1") {
      val sql = MergedInvariants.tombstoneIntegritySql(raw, merged)
      sql should include ("FROM s3tables.cda.cc_account_merged m")
      sql should include ("WHERE latest_op = 1")
      // Must use lpad-32 ordering when picking the latest operation.
      sql should include ("LPAD(gwcbi___seqval_hex, 32, '0') DESC")
    }

    it("latest seqval invariant joins merged against raw's lpad-max per id") {
      val sql = MergedInvariants.latestSeqvalSql(raw, merged)
      sql should include ("MAX(LPAD(gwcbi___seqval_hex, 32, '0'))")
      // Filtered to upsert ops; tombstones are not candidates for the
      // "latest seqval that would have been merged" computation.
      sql should include ("gwcbi___operation IN (0, 2, 4)")
      sql should include ("LPAD(m.gwcbi___seqval_hex, 32, '0') <> e.expected_seqval")
    }

    it("no orphans uses LEFT ANTI JOIN against raw") {
      val sql = MergedInvariants.noOrphansSql(raw, merged)
      sql should include ("LEFT ANTI JOIN s3tables.cda.cc_account_raw r")
      sql should include ("ON m.id = r.id")
    }

    it("count formula is |merged| - |distinct ids whose latest op is 0/2/4|") {
      val sql = MergedInvariants.countFormulaSql(raw, merged)
      sql should include ("ABS")
      sql should include ("count(*) FROM s3tables.cda.cc_account_merged")
      sql should include ("latest_op IN (0, 2, 4)")
    }
  }

  describe("MergedInvariants.formatRefs (point-in-time pinning)") {
    import gw.cda.iceberg.recon.MergedInvariants
    val raw = "s3tables.cda.cc_account_raw"
    val merged = "s3tables.cda.cc_account_merged"

    it("pins both tables to their resolved snapshot ids via VERSION AS OF") {
      val (rawRef, mergedRef, note) = MergedInvariants.formatRefs(raw, merged, Some((111L, 222L)))
      rawRef shouldEqual "s3tables.cda.cc_account_raw VERSION AS OF 111"
      mergedRef shouldEqual "s3tables.cda.cc_account_merged VERSION AS OF 222"
      note shouldEqual Some("pinned merged@222 raw@111")
    }

    it("falls back to bare table names (unpinned) when snapshots can't be resolved") {
      val (rawRef, mergedRef, note) = MergedInvariants.formatRefs(raw, merged, None)
      rawRef shouldEqual raw
      mergedRef shouldEqual merged
      note shouldBe empty
    }

    it("pinned refs compose into valid-shaped invariant SQL (alias after AS OF)") {
      // The SQL builders alias the table (e.g. 'FROM <ref> m'); a
      // VERSION AS OF clause must sit before the alias. Verify the
      // composed string keeps that order.
      val (rawRef, mergedRef, _) = MergedInvariants.formatRefs(raw, merged, Some((5L, 9L)))
      val sql = MergedInvariants.noOrphansSql(rawRef, mergedRef)
      sql should include ("FROM s3tables.cda.cc_account_merged VERSION AS OF 9 m")
      sql should include ("LEFT ANTI JOIN s3tables.cda.cc_account_raw VERSION AS OF 5 r")
    }
  }

  describe("ReconStore.isCommitConflict") {
    import gw.cda.iceberg.recon.ReconStore

    it("matches a CommitFailedException by class name") {
      class CommitFailedException(m: String) extends RuntimeException(m)
      ReconStore.isCommitConflict(new CommitFailedException("boom")) shouldBe true
    }
    it("matches the S3 Tables 'not the same as current metadata location' message") {
      ReconStore.isCommitConflict(
        new RuntimeException("Base metadata location X is not the same as current metadata location Y in DDB")
      ) shouldBe true
    }
    it("matches a conflict nested as a cause") {
      class CommitStateUnknownException(m: String) extends RuntimeException(m)
      val wrapped = new RuntimeException("spark wrapper", new CommitStateUnknownException("inner"))
      ReconStore.isCommitConflict(wrapped) shouldBe true
    }
    it("does NOT match an unrelated error (so it rethrows, not retries forever)") {
      ReconStore.isCommitConflict(new IllegalArgumentException("bad schema")) shouldBe false
    }
  }

  describe("ReconStore.deriveStatus") {
    import gw.cda.iceberg.recon.ReconStore

    it("returns OK when counts match and every folder has metrics") {
      ReconStore.deriveStatus(
        actualLanded = 100, expectedWritten = 100,
        okFolders = 5, missingFolders = Nil, corruptFolders = Nil,
      ) shouldEqual "OK"
    }

    it("returns MISMATCH when counts differ but coverage is complete") {
      ReconStore.deriveStatus(
        actualLanded = 100, expectedWritten = 99,
        okFolders = 5, missingFolders = Nil, corruptFolders = Nil,
      ) shouldEqual "MISMATCH"
    }

    it("returns METRICS_PARTIAL when counts match but some folders are missing metrics") {
      ReconStore.deriveStatus(
        actualLanded = 100, expectedWritten = 100,
        okFolders = 4, missingFolders = Seq("ts1"), corruptFolders = Nil,
      ) shouldEqual "METRICS_PARTIAL"
    }

    it("returns METRICS_MISSING when no folder produced any metrics") {
      ReconStore.deriveStatus(
        actualLanded = 100, expectedWritten = 0,
        okFolders = 0, missingFolders = Seq("ts1", "ts2"), corruptFolders = Nil,
      ) shouldEqual "METRICS_MISSING"
    }

    it("MISMATCH wins over PARTIAL when both counts diverge AND folders are missing") {
      // Operator should see the mismatch first; partial coverage is a
      // weaker signal compared to actual count divergence.
      ReconStore.deriveStatus(
        actualLanded = 100, expectedWritten = 80,
        okFolders = 4, missingFolders = Seq("ts1"), corruptFolders = Nil,
      ) shouldEqual "MISMATCH"
    }
  }

  describe("BatchMetrics.metricsKey") {
    import gw.cda.iceberg.recon.BatchMetrics

    it("builds the .cda/batch-metrics.json key relative to dataFilesKey") {
      BatchMetrics.metricsKey(
        "synthetic/cc_account",
        "abc123",
        "1700000000",
      ) shouldEqual "synthetic/cc_account/abc123/1700000000/.cda/batch-metrics.json"
    }

    it("normalizes a trailing slash in dataFilesKey") {
      BatchMetrics.metricsKey(
        "synthetic/cc_account/",
        "abc123",
        "1700000000",
      ) shouldEqual "synthetic/cc_account/abc123/1700000000/.cda/batch-metrics.json"
    }
  }

  describe("CursorStore.State.cursorFor") {
    import gw.cda.iceberg.state.CursorStore

    it("returns the per-fingerprint cursor when present") {
      val s = CursorStore.State(Map("fp1" -> "200"), Some("100"))
      s.cursorFor("fp1") shouldEqual Some("200")
    }

    it("falls back to the high-water mark when fingerprint cursor is missing (migration)") {
      // OSR-style legacy state: only lastSuccessfulWriteTimestamp, no per-fp.
      // Treating it as the cursor for every fingerprint stops the new code
      // from re-loading already-loaded data on the first incremental run.
      val s = CursorStore.State(Map.empty, Some("100"))
      s.cursorFor("any") shouldEqual Some("100")
    }

    it("returns None when neither cursor nor HWM exist (first-ever run)") {
      val s = CursorStore.State(Map.empty, None)
      s.cursorFor("fp1") shouldBe empty
    }

    it("prefers per-fp cursor over HWM even when HWM is newer") {
      // Should never happen in practice (cursor is always >= HWM if both
      // are set), but the function should be deterministic anyway.
      val s = CursorStore.State(Map("fp1" -> "100"), Some("999"))
      s.cursorFor("fp1") shouldEqual Some("100")
    }
  }

  describe("CursorStore.State.mergeWatermarkFor") {
    import gw.cda.iceberg.state.CursorStore

    it("returns the per-fingerprint merge watermark when present") {
      val s = CursorStore.State(Map("fp1" -> "200"), Some("100"),
        mergeWatermarks = Map("fp1" -> "150"))
      s.mergeWatermarkFor("fp1") shouldEqual Some("150")
    }

    it("returns None when no watermark exists (no merge succeeded yet)") {
      // Critical: None means the MERGE sees ALL rows. The watermark must
      // NOT fall back to the cursor or HWM — those advance after the raw
      // append, before the merge, so using them would skip un-merged rows.
      val s = CursorStore.State(Map("fp1" -> "200"), Some("100"))
      s.mergeWatermarkFor("fp1") shouldBe empty
    }

    it("defaults to an empty watermark map for legacy state shapes") {
      val s = CursorStore.State(Map("fp1" -> "200"), Some("100"))
      s.mergeWatermarks shouldBe empty
    }
  }

  describe("TimestampFolderSelector.select") {
    import gw.cda.iceberg.state.TimestampFolderSelector

    it("returns empty when there are no folders") {
      TimestampFolderSelector.select(Seq.empty, None, "9999") shouldBe empty
    }

    it("on first run (no cursor), returns all folders <= upperBound") {
      val folders = Seq("100", "200", "300", "400")
      TimestampFolderSelector.select(folders, None, "300") shouldEqual Seq("100", "200", "300")
    }

    it("on subsequent run, returns folders strictly greater than cursor and <= upperBound") {
      val folders = Seq("100", "200", "300", "400", "500")
      TimestampFolderSelector.select(folders, Some("200"), "400") shouldEqual Seq("300", "400")
    }

    it("excludes folders past the upperBound (still being written by CDA)") {
      val folders = Seq("100", "200", "300", "400")
      TimestampFolderSelector.select(folders, Some("100"), "300") shouldEqual Seq("200", "300")
    }

    it("returns empty when cursor is at or past the upperBound (already up to date)") {
      val folders = Seq("100", "200", "300")
      TimestampFolderSelector.select(folders, Some("300"), "300") shouldBe empty
    }

    it("sorts numerically, not lexically") {
      // '999' > '1000' lex, but 999 < 1000 numerically.
      val folders = Seq("1000", "999", "1001")
      TimestampFolderSelector.select(folders, None, "1001") shouldEqual Seq("999", "1000", "1001")
    }

    it("discards non-numeric folder names defensively") {
      val folders = Seq("100", "garbage", "200", ".cda", "300")
      TimestampFolderSelector.select(folders, None, "300") shouldEqual Seq("100", "200", "300")
    }

    it("treats malformed cursor as no cursor (full read)") {
      val folders = Seq("100", "200")
      TimestampFolderSelector.select(folders, Some("not-a-number"), "300") shouldEqual Seq("100", "200")
    }
  }

  // NOTE: rawRowCount and recon snapshot-pinning now read snapshot metadata
  // via the Iceberg catalog API (Spark3Util.loadIcebergTable), not metadata-
  // table SQL — so there is no longer a pure string helper to unit-test here.
  // Those paths require a live SparkSession + Iceberg table and are exercised
  // by the recon integration run (look for "pinned merged@.. raw@.." in the
  // EMR driver log to confirm pinning engaged).

  describe("IcebergIngest.dataFilesKey") {
    it("strips s3://bucket/ prefix when bucket matches") {
      IcebergIngest.dataFilesKey(
        "s3://gw-cda-bucket/synthetic/cc_account",
        "gw-cda-bucket",
      ) shouldEqual "synthetic/cc_account"
    }

    it("falls back to scheme-strip when bucket doesn't match the URL") {
      IcebergIngest.dataFilesKey(
        "s3://other-bucket/path/table",
        "gw-cda-bucket",
      ) shouldEqual "path/table"
    }
  }

  describe("IcebergIngest.planEvolution") {
    import IcebergIngest.{AddColumn, WidenColumn}

    it("emits no changes when source schema matches target") {
      val s = schema("id" -> LongType, "name" -> StringType)
      val (changes, projection) = IcebergIngest.planEvolution(s, s)
      changes shouldBe empty
      projection.map(_._1) shouldEqual Seq("id", "name")
    }

    it("ADDs columns the source has but the target lacks") {
      val tgt = schema("id" -> LongType)
      val src = schema("id" -> LongType, "new_col" -> StringType)
      val (changes, _) = IcebergIngest.planEvolution(tgt, src)
      changes should contain (AddColumn("new_col", StringType))
    }

    it("plans a NULL projection for target columns missing from source") {
      val tgt = schema("id" -> LongType, "old_col" -> StringType)
      val src = schema("id" -> LongType)
      val (changes, projection) = IcebergIngest.planEvolution(tgt, src)
      changes shouldBe empty
      // old_col still in projection but with no source — caller fills with lit(null).
      projection should contain (("old_col", StringType, None))
    }

    it("widens int → long when source is wider") {
      val tgt = schema("id" -> IntegerType)
      val src = schema("id" -> LongType)
      val (changes, _) = IcebergIngest.planEvolution(tgt, src)
      changes should contain (WidenColumn("id", IntegerType, LongType))
    }

    it("widens float → double when source is wider") {
      val tgt = schema("metric" -> FloatType)
      val src = schema("metric" -> DoubleType)
      val (changes, _) = IcebergIngest.planEvolution(tgt, src)
      changes should contain (WidenColumn("metric", FloatType, DoubleType))
    }

    it("widens decimal precision when scale matches") {
      val tgt = schema("amount" -> DecimalType(10, 2))
      val src = schema("amount" -> DecimalType(20, 2))
      val (changes, _) = IcebergIngest.planEvolution(tgt, src)
      changes should contain (WidenColumn("amount", DecimalType(10, 2), DecimalType(20, 2)))
    }

    it("does NOT widen when source narrower (target stays, source casts up)") {
      val tgt = schema("id" -> LongType)
      val src = schema("id" -> IntegerType)
      val (changes, projection) = IcebergIngest.planEvolution(tgt, src)
      changes shouldBe empty
      // Caller will cast int → long in flight.
      projection should contain (("id", LongType, Some(IntegerType)))
    }

    it("throws on incompatible type changes") {
      val tgt = schema("amount" -> IntegerType)
      val src = schema("amount" -> StringType)
      an [IllegalStateException] should be thrownBy IcebergIngest.planEvolution(tgt, src)
    }

    it("throws on decimal scale change") {
      val tgt = schema("amount" -> DecimalType(10, 2))
      val src = schema("amount" -> DecimalType(10, 4))
      an [IllegalStateException] should be thrownBy IcebergIngest.planEvolution(tgt, src)
    }

    it("handles structs (top-level type equality, no inner walk)") {
      // CDA spatial: struct<wkb: binary, srid: int>. Top-level type equality
      // means a struct with identical inner fields requires no DDL.
      val sp = StructType(Seq(StructField("wkb", BinaryType), StructField("srid", IntegerType)))
      val tgt = schema("geom" -> sp)
      val src = schema("geom" -> sp)
      val (changes, _) = IcebergIngest.planEvolution(tgt, src)
      changes shouldBe empty
    }

    it("ADDs a struct column when introduced in a later fingerprint") {
      val sp = StructType(Seq(StructField("wkb", BinaryType), StructField("srid", IntegerType)))
      val tgt = schema("id" -> LongType)
      val src = schema("id" -> LongType, "geom" -> sp)
      val (changes, _) = IcebergIngest.planEvolution(tgt, src)
      changes should contain (AddColumn("geom", sp))
    }
  }
}
