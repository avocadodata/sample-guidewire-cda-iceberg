// Copyright Amazon.com and its affiliates; all rights reserved.
// SPDX-License-Identifier: MIT-0

// scripts/local_iceberg_pipeline.scala
// Executes the local Guidewire CDA -> Iceberg pipeline by directly calling the src/main codebase.

import gw.cda.iceberg.LocalIcebergIngest

val sourceTableDir = "data/source/cc_account"
val catalogName    = "local"
val namespace      = "cda"
val tableName      = "cc_account"
val fingerprint    = "fp_v1"

println("============================================================")
println("Running Local Iceberg Pipeline (calling src/main codebase)")
println(s"Source Directory: $sourceTableDir")
println(s"Target Catalog:   $catalogName")
println(s"Target Namespace: $namespace")
println(s"Target Table:     $tableName")
println("============================================================")

// Directly invokes gw.cda.iceberg.LocalIcebergIngest from the built jar
LocalIcebergIngest.run(
  spark       = spark,
  table       = tableName,
  namespace   = namespace,
  sourcePath  = sourceTableDir,
  catalog     = catalogName,
  fingerprint = fingerprint
)

println("\n============================================================")
println(s"Current Reconciled State ($catalogName.$namespace.${tableName}_merged):")
println("============================================================")
spark.sql(s"""
  SELECT id, accountnumber, name, status, balance, gwcbi___operation, gwcbi___seqval_hex
  FROM $catalogName.$namespace.${tableName}_merged
  ORDER BY id
""").show(false)

println("\n============================================================")
println(s"Audit History in Raw Table ($catalogName.$namespace.${tableName}_raw):")
println("============================================================")
spark.sql(s"""
  SELECT id, name, gwcbi___operation, gwcbi___seqval_hex, cda_fingerprint, cda_load_ts
  FROM $catalogName.$namespace.${tableName}_raw
  ORDER BY id, gwcbi___seqval_hex
""").show(false)

println("============================================================")
println("✓ Local Iceberg pipeline executed successfully!")
println("============================================================")

System.exit(0)
