// Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
// SPDX-License-Identifier: MIT-0

package gw.cda.iceberg.manifest

import org.junit.runner.RunWith
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

/**
  * Unit tests for [[ManifestReader.parseManifestJson]] — the parser for the
  * vendor-supplied CDA `manifest.json`. This is an EXTERNAL-INPUT boundary
  * (the file is written by Guidewire CDA in a cross-account bucket), so its
  * parsing behaviour is security- and correctness-relevant: it must tolerate
  * the real CDA shape, surface malformed input loudly, and not silently
  * mis-map fields.
  *
  * Only `parseManifestJson` is exercised — `getManifestJson`/`processManifest`
  * hit S3 and are covered operationally, not in unit tests.
  */
@RunWith(classOf[JUnitRunner])
class ManifestReaderTest extends AnyFunSpec with Matchers {

  describe("ManifestReader.parseManifestJson") {

    it("parses a single-table manifest into a ManifestEntry") {
      val json =
        """{
          |  "cc_account": {
          |    "lastSuccessfulWriteTimestamp": "1639166206419",
          |    "totalProcessedRecordsCount": 240000,
          |    "dataFilesPath": "s3://gw-cda/synthetic/cc_account",
          |    "schemaHistory": {
          |      "fp1": "1639166206419",
          |      "fp2": "1641103254000"
          |    }
          |  }
          |}""".stripMargin

      val m = ManifestReader.parseManifestJson(json)
      m should have size 1
      val e = m("cc_account")
      e.lastSuccessfulWriteTimestamp shouldEqual "1639166206419"
      e.totalProcessedRecordsCount shouldEqual 240000L
      e.dataFilesPath shouldEqual "s3://gw-cda/synthetic/cc_account"
      e.schemaHistory shouldEqual Map("fp1" -> "1639166206419", "fp2" -> "1641103254000")
    }

    it("parses a multi-table manifest and keys by table name") {
      val json =
        """{
          |  "cc_account":      {"lastSuccessfulWriteTimestamp":"100","totalProcessedRecordsCount":1,"dataFilesPath":"s3://b/cc_account","schemaHistory":{"a":"100"}},
          |  "cc_policyperiod": {"lastSuccessfulWriteTimestamp":"200","totalProcessedRecordsCount":2,"dataFilesPath":"s3://b/cc_policyperiod","schemaHistory":{"b":"200"}}
          |}""".stripMargin

      val m = ManifestReader.parseManifestJson(json)
      m.keySet shouldEqual Set("cc_account", "cc_policyperiod")
      m("cc_policyperiod").totalProcessedRecordsCount shouldEqual 2L
    }

    it("returns an empty map for an empty JSON object") {
      ManifestReader.parseManifestJson("{}") shouldBe empty
    }

    it("handles an empty schemaHistory (CDA may prune all older fingerprints)") {
      val json =
        """{"t": {"lastSuccessfulWriteTimestamp":"1","totalProcessedRecordsCount":0,"dataFilesPath":"s3://b/t","schemaHistory":{}}}"""
      val e = ManifestReader.parseManifestJson(json)("t")
      e.schemaHistory shouldBe empty
      e.totalProcessedRecordsCount shouldEqual 0L
    }

    it("keeps lastSuccessfulWriteTimestamp as a String (CDA emits epoch-ms as a quoted string)") {
      // Regression guard: the HWM is compared as a string/parsed downstream;
      // it must NOT be coerced to a number here (leading-zero / precision risk).
      val json =
        """{"t": {"lastSuccessfulWriteTimestamp":"01680000000000","totalProcessedRecordsCount":1,"dataFilesPath":"s3://b/t","schemaHistory":{}}}"""
      ManifestReader.parseManifestJson(json)("t").lastSuccessfulWriteTimestamp shouldEqual "01680000000000"
    }

    it("parses a large totalProcessedRecordsCount as Long (billions of rows)") {
      val json =
        """{"t": {"lastSuccessfulWriteTimestamp":"1","totalProcessedRecordsCount":5000000000,"dataFilesPath":"s3://b/t","schemaHistory":{}}}"""
      ManifestReader.parseManifestJson(json)("t").totalProcessedRecordsCount shouldEqual 5000000000L
    }

    it("throws on malformed JSON (loud failure, not a silent empty map)") {
      an[Exception] should be thrownBy ManifestReader.parseManifestJson("{ not json ")
    }

    it("throws when a required field type is wrong (e.g. records count is not numeric)") {
      val json =
        """{"t": {"lastSuccessfulWriteTimestamp":"1","totalProcessedRecordsCount":"oops","dataFilesPath":"s3://b/t","schemaHistory":{}}}"""
      an[Exception] should be thrownBy ManifestReader.parseManifestJson(json)
    }
  }
}
