# Local Setup & Validation Guide: Guidewire CDA to Apache Iceberg

This document provides step-by-step instructions for running and validating the Guidewire CDA (Cloud Data Access) to Apache Iceberg ingestion pipeline **locally on your machine** using **Spark 4.0.0**, **Scala 2.13**, and **Java 21**.

All cloud-specific dependencies (AWS Lambda, S3, DynamoDB cursor state, EMR Serverless) are bypassed. You will read from a local directory containing Parquet micro-batches, ingest into local Apache Iceberg tables (`raw` and `merged`), and verify the tables interactively using `spark-shell`.

---

## 1. Environment & Package Coordinates

| Component | Active Version | Notes |
| :--- | :--- | :--- |
| **Spark** | `4.0.0` | Homebrew Apache Spark (`setsparkversion 4.0`) |
| **Scala** | `2.13.16` | System Scala (`setscalaversion 2.13`) |
| **Java** | `21.0.7` | OpenJDK 21 (`setjavaversion 21`) |
| **Iceberg Runtime** | `1.10.0` (Spark 4.0 / Scala 2.13) | `org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0` |

### Environment Setup
You can switch all three versions in your active terminal with:
```bash
source scripts/set-env.sh
```
*(This script internally calls your existing `setjavaversion 21`, `setscalaversion 2.13`, and `setsparkversion 4.0` functions).*

Confirm with:
```bash
spark-shell --version
# Output: version 4.0.0, Using Scala version 2.13.16, OpenJDK 64-Bit Server VM, 21.0.7
```

---

## 2. Guidewire CDA Pipeline Architecture

Guidewire CDA writes Change Data Capture (CDC) Parquet files partitioned by schema fingerprint and timestamp:
```
<source_directory>/<fingerprint>/<timestamp_folder>/*.parquet
```

Each record contains Guidewire-specific tracking columns:
- `id`: Primary key of the entity.
- `gwcbi___operation`:
  - `0`: Insert / Create
  - `2`: Update
  - `4`: Initial load / Snapshot
  - `1`: Tombstone (Delete)
- `gwcbi___seqval_hex`: Hexadecimal sequence number reflecting database commit order.

### Target Iceberg Tables:
1. **`local.cda.cc_account_raw`**:
   - **Append-only** audit table retaining all incoming events.
   - Partitioned by `cda_fingerprint`.
   - Enriched with `cda_fingerprint` and `cda_load_ts` load tracking columns.
2. **`local.cda.cc_account_merged`**:
   - **Current-state** reconciled table.
   - Partitioned by `bucket(64, id)`.
   - Updated via `MERGE INTO` using canonical `LPAD(gwcbi___seqval_hex, 32, '0')` deduplication to ensure the latest sequence value always wins.
   - Rows with `gwcbi___operation = 1` are pruned via `DELETE`.

---

## 3. Step-by-Step Local Execution

### Step 1: Build the Project JAR (Contains `src/main` Codebase)
The local pipeline executes the real project classes in `src/main` (`LocalIcebergIngest`, `IcebergCatalog`, `IcebergIngest`, `Identifiers`, etc.).

Build the shadowJar:
```bash
./gradlew shadowJar
```
This produces `build/libs/cda-iceberg-client-1.0.jar`.

---

### Step 2: Generate Local Sample Parquet Files
Run the sample data generator script:
```bash
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/data/warehouse \
  -i scripts/generate_sample_parquet.scala
```

This creates realistic Guidewire CDA test micro-batches under `data/source/cc_account/fp_v1/`:
- `1710000001000`: Batch 1 (inserts for `id` 101, 102, 103)
- `1710000002000`: Batch 2 (update for `101`, tombstone delete for `102`, insert for `104`)

---

### Step 3: Run the Ingestion Pipeline (Calling `src/main` Codebase)

Run the local pipeline script, which passes the built jar and invokes `gw.cda.iceberg.LocalIcebergIngest`:

```bash
spark-shell \
  --jars build/libs/cda-iceberg-client-1.0.jar \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/data/warehouse \
  -i scripts/local_iceberg_pipeline.scala
```

*(Alternatively, you can run it directly via `spark-submit`:)*
```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/data/warehouse \
  --class gw.cda.iceberg.LocalIcebergIngest \
  build/libs/cda-iceberg-client-1.0.jar \
  cc_account cda data/source/cc_account local fp_v1
```

---

### Step 4: Interactive Verification via `spark-shell`

Launch an interactive `spark-shell`:
```bash
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/data/warehouse
```

Once the `scala>` prompt appears, run:

## 1. Show Namespaces and Tables
```scala
spark.sql("SHOW NAMESPACES IN local").show()
spark.sql("SHOW TABLES IN local.cda").show()
```

## 2. Query Reconciled Current State (`cc_account_merged`)
```scala
spark.sql("""
  SELECT id, accountnumber, name, status, balance, gwcbi___operation, gwcbi___seqval_hex
  FROM local.cda.cc_account_merged
  ORDER BY id
""").show(false)
```
*Expected: `id=101` has updated name & balance, `id=102` has been removed by tombstone, `id=103` unchanged, `id=104` newly inserted.*

## 3. Query Immutable Audit History (`cc_account_raw`)
```scala
spark.sql("""
  SELECT id, name, gwcbi___operation, gwcbi___seqval_hex, cda_fingerprint, cda_load_ts
  FROM local.cda.cc_account_raw
  ORDER BY id, gwcbi___seqval_hex
""").show(false)
```
*Expected: All 6 incoming events across both batches are preserved.*

## 4. Query Iceberg Snapshots & History
```scala
// View Table History & Snapshots
// 1. View table commit snapshots
spark.sql("SELECT snapshot_id, operation, committed_at FROM local.cda.cc_account_merged.snapshots").show(false)

// 2. Entrie snapshot for raw table
spark.sql("select * from local.cda.cc_account_merged.snapshots").show(false)

// 3. View history timeline
spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor FROM local.cda.cc_account_merged.history").show(false)

// 4. Inspect Data Files and Physical Stats
spark.sql("SELECT file_path, record_count, file_size_in_bytes, partition FROM local.cda.cc_account_merged.files").show(false)

// 5. View Table stats
spark.sql("DESCRIBE DETAIL local.cda.cc_account_merged").show(false)
// 5th does not worked on local 


// 6. Analyze Per-Partition Distribution
spark.sql("SELECT partition, record_count, file_count FROM local.cda.cc_account_merged.partitions").show(false)

// 7. View Manifest Files
spark.sql("SELECT path, added_snapshot_id, added_data_files_count FROM local.cda.cc_account_merged.manifests").show(false)

```


<!--  -->
To exit:
```scala
:quit
```
