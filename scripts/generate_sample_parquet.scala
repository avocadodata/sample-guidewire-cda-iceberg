// Copyright Amazon.com and its affiliates; all rights reserved.
// SPDX-License-Identifier: MIT-0

// scripts/generate_sample_parquet.scala
// Helper script to generate realistic local Guidewire CDA Parquet micro-batches.

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val sourceBaseDir = "data/source/cc_account"
val fingerprint = "fp_v1"

println(s"=== Generating sample Guidewire CDA Parquet files in $sourceBaseDir ===")

// Batch 1: Initial load / Insert records (gwcbi___operation = 0)
val batch1 = Seq(
  (101L, "ACC-101", "Acme Corporation", "Active", 1000.0, 0, "00000001"),
  (102L, "ACC-102", "Global Tech", "Active", 2500.0, 0, "00000002"),
  (103L, "ACC-103", "Stark Industries", "Pending", 5000.0, 0, "00000003")
)
val df1 = spark.createDataFrame(batch1).toDF(
  "id", "accountnumber", "name", "status", "balance", "gwcbi___operation", "gwcbi___seqval_hex"
)

val batch1Path = s"$sourceBaseDir/$fingerprint/1710000001000"
df1.write.mode("overwrite").parquet(batch1Path)
println(s"✓ Batch 1 written to: $batch1Path (3 records)")

// Batch 2: CDC Micro-batch
// - id=101: Update name and balance (gwcbi___operation = 2, higher seqval)
// - id=102: Tombstone delete (gwcbi___operation = 1)
// - id=104: New insert (gwcbi___operation = 0)
val batch2 = Seq(
  (101L, "ACC-101", "Acme Corporation Ltd", "Active", 1500.0, 2, "00000004"),
  (102L, "ACC-102", "Global Tech", "Inactive", 0.0, 1, "00000005"),
  (104L, "ACC-104", "Wayne Enterprises", "Active", 9900.0, 0, "00000006")
)
val df2 = spark.createDataFrame(batch2).toDF(
  "id", "accountnumber", "name", "status", "balance", "gwcbi___operation", "gwcbi___seqval_hex"
)

val batch2Path = s"$sourceBaseDir/$fingerprint/1710000002000"
df2.write.mode("overwrite").parquet(batch2Path)
println(s"✓ Batch 2 written to: $batch2Path (3 records)")

println("=== Sample Parquet generation complete ===")
System.exit(0)
