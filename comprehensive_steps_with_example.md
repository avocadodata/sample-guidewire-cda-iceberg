# Guidewire CDA to Apache Iceberg: Comprehensive Incremental Ingestion Guide

This document provides a comprehensive, end-to-end breakdown of how incremental Parquet ingestion works in this repository. It covers both **Case 1: First-Time Ingestion (Initial Load)** and **Case 2: Second-Time Ingestion (Incremental / No-Op)** with concrete data examples and line-by-line source code references.

---

## 1. System Architecture & Foundational Concepts

Guidewire Cloud Data Access (CDA) streams database mutations as Parquet files into an S3 bucket and updates a `manifest.json` file. This repository consumes those files and synchronizes them into Apache Iceberg (AWS S3 Tables or local catalog).

```
   ┌───────────────────────┐
   │ Guidewire CDA (S3)    │
   │  - manifest.json      │
   │  - <fp>/<ts>/*.parquet│
   └──────────┬────────────┘
              │
              ▼
   ┌──────────────────────────────────────────────┐
   │ Orchestration (Step Functions / Lambda)      │  ◄── Level 1: Launch Condition
   │ [decide.mjs] (Compare Manifest vs DynamoDB)  │      Skips run if HWM unchanged
   └──────────┬───────────────────────────────────┘
              │ (Only if new data found)
              ▼
   ┌──────────────────────────────────────────────┐
   │ Spark Job: [IcebergIngest.scala]             │  ◄── Level 2: Micro-Batch Reader
   │  1. Read cursors from DynamoDB [CursorStore] │      Only reads ts > cursor && ts <= HWM
   │  2. Filter S3 folders [TimestampFolderSelector]     Exits immediately if 0 folders
   │  3. Read specific Parquet paths only         │
   │  4. Append to <table>_raw                    │
   │  5. Advance DynamoDB cursor [advanceCursor]  │
   │  6. MERGE / DELETE into <table>_merged       │
   │  7. Advance DynamoDB HWM [advanceHighWater]  │
   └──────────────────────────────────────────────┘
```

### The Three State Elements
1. **Manifest File (`manifest.json` on S3)**: Produced by CDA. Contains:
   - `lastSuccessfulWriteTimestamp`: Global high-water mark (epoch ms). Any timestamp folder $\le$ this is committed and ready. Folders $>$ this may still be mid-write.
   - `schemaHistory`: Map of `{ fingerprintId -> firstSeenEpochMs }` describing schema evolution generations.
   - `dataFilesPath`: S3 URI prefix where Parquet files reside.
2. **State Store (DynamoDB)**: Tracked per-table in [`CursorStore.scala`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/state/CursorStore.scala):
   - `lastSuccessfulWriteTimestamp` (HWM): Manifest high-water mark processed up to this point.
   - `fingerprintCursors`: Map of `{ fingerprintId -> lastProcessedTimestampFolder }`.
   - `mergeWatermarks`: Map of `{ fingerprintId -> lastMergedCdaLoadTs }`.
3. **Dual Iceberg Tables**:
   - `s3tables.<namespace>.<table>_raw`: Append-only audit table partitioned by `cda_fingerprint`.
   - `s3tables.<namespace>.<table>_merged`: Current deduplicated state partitioned by `bucket(64, id)`.

---

## 2. Working Example Scenario

To illustrate both cases, assume table **`cc_account`**:

### S3 Parquet Folder Structure:
```
s3://my-cda-bucket/cc_account/
  ├── fp_100/                          # Fingerprint 1 (Initial schema)
  │    ├── 1639166000000/              # Timestamp folder 1
  │    │    └── part-0000.parquet
  │    └── 1639166100000/              # Timestamp folder 2
  │         └── part-0000.parquet
  └── fp_200/                          # Fingerprint 2 (Schema evolved: e.g. added email column)
       └── 1639166200000/              # Timestamp folder 3
            └── part-0000.parquet
```

---

## 3. Case 1: First-Time Ingestion (Initial Load)

### Walkthrough & State Changes

1. **Manifest State**:
   ```json
   {
     "cc_account": {
       "lastSuccessfulWriteTimestamp": "1639166200000",
       "totalProcessedRecordsCount": 150000,
       "dataFilesPath": "s3://my-cda-bucket/cc_account",
       "schemaHistory": {
         "fp_100": "1639166000000",
         "fp_200": "1639166200000"
       }
     }
   }
   ```
2. **DynamoDB State (Before Run)**: Empty / Record does not exist for `cc_account`.
3. **Folders Selected**:
   - For `fp_100`: `["1639166000000", "1639166100000"]`
   - For `fp_200`: `["1639166200000"]`
4. **Execution Outcome**:
   - Tables `cc_account_raw` and `cc_account_merged` created.
   - All 3 Parquet folders read chronologically.
   - Rows appended to `raw` and deduplicated into `merged`.
   - DynamoDB updated:
     - `fingerprintCursors`: `{"fp_100": "1639166100000", "fp_200": "1639166200000"}`
     - `lastSuccessfulWriteTimestamp`: `"1639166200000"`

### Line-by-Line Code References (Case 1)

| Step | Action | Code Location | What the Code Does |
|---|---|---|---|
| **1** | Read `manifest.json` | [`IcebergIngest.scala:L140-144`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L140-L144)<br>[`ManifestReader.scala:L42-58`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/manifest/ManifestReader.scala#L42-L58) | Downloads and parses `manifest.json` into a `ManifestEntry` containing `dataFilesPath`, `lastSuccessfulWriteTimestamp`, and `schemaHistory`. |
| **2** | Query DynamoDB State | [`IcebergIngest.scala:L149-150`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L149-L150)<br>[`CursorStore.scala:L68-87`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/state/CursorStore.scala#L68-L87) | Executes `GetItemRequest` for `tableName = "cc_account"`. Returns `cursors = Map.empty` and `highWaterMark = None`. |
| **3** | Resolve Fingerprints | [`IcebergIngest.scala:L178`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L178)<br>[`IcebergIngest.scala:L363-364`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L363-L364) | Sorts fingerprints by timestamp: `entry.schemaHistory.toSeq.sortBy(_._2.toLong).map(_._1)` $\rightarrow$ `Seq("fp_100", "fp_200")`. |
| **4** | List S3 Folders | [`IcebergIngest.scala:L193-195`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L193-L195)<br>[`TimestampFolderSelector.scala:L45-68`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/state/TimestampFolderSelector.scala#L45-L68) | Calls S3 `listObjectsV2` with delimiter `/` on `cc_account/<fp>/` to get timestamp folder names without listing every individual Parquet file. |
| **5** | Filter with Cursor (`None`) | [`IcebergIngest.scala:L196-197`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L196-L197)<br>[`TimestampFolderSelector.scala:L30-37`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/state/TimestampFolderSelector.scala#L30-L37) | `cursor` is `None`, so `lo = Long.MinValue`. The filter `n > lo && n <= hi` keeps **all** folders $\le$ `lastSuccessfulWriteTimestamp`. |
| **6** | Bootstrap Iceberg Tables | [`IcebergIngest.scala:L218-221`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L218-L221)<br>[`IcebergIngest.scala:L316-359`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L316-L359) | Reads 0 rows (`.limit(0)`) from the first timestamp folder to infer schema. Calls `IcebergCatalog.ensureTable` to create `_raw` (partitioned by `cda_fingerprint`) and `_merged` (partitioned by `bucket(64, id)`). |
| **7** | Read Targeted Parquet Paths | [`IcebergIngest.scala:L382`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L382)<br>[`IcebergIngest.scala:L500-514`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L500-L514) | Converts timestamp list into exact paths (`s3://.../cc_account/fp_100/1639166000000/`) and calls `spark.read.parquet(paths: _*)`. Injects bookkeeping columns `cda_fingerprint` and `cda_load_ts`. |
| **8** | Evolve Schema & Append Raw | [`IcebergIngest.scala:L390-401`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L390-L401) | Compares DataFrame schema with Iceberg table, applies non-destructive `ADD COLUMN` or `ALTER COLUMN TYPE` if needed, then executes `srcAligned.writeTo(rawTable).append()`. |
| **9** | Save Fingerprint Cursor | [`IcebergIngest.scala:L408`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L408)<br>[`CursorStore.scala:L103-133`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/state/CursorStore.scala#L103-L133) | Advances cursor in DynamoDB immediately after the atomic raw commit: `cursors.advanceCursor(table, fp, timestamps.last)`. Uses conditional update to ensure strictly increasing cursors. |
| **10** | MERGE + DELETE into Merged | [`IcebergIngest.scala:L444-451`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L444-L451)<br>[`IcebergIngest.scala:L706-743`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L706-L743) | Runs SQL `MERGE INTO <tbl>_merged` using latest `seqval_hex` per ID (filtering operations 0, 2, 4) and SQL `DELETE FROM <tbl>_merged` for tombstones (operation = 1). |
| **11** | Commit Global High-Water Mark | [`IcebergIngest.scala:L242`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L242)<br>[`CursorStore.scala:L145-161`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/state/CursorStore.scala#L145-L161) | Commits table-level HWM to DynamoDB: `cursors.advanceHighWaterMark(table, entry.lastSuccessfulWriteTimestamp)`. |

---

## 4. Case 2: Second-Time Ingestion (Incremental Load)

There are two possible scenarios on subsequent runs:

---

### Scenario A: No New Parquet Files / Up-to-Date Table

In this scenario, CDA has not produced new data (`lastSuccessfulWriteTimestamp` remains `"1639166200000"`).

#### Level 1: Launch Condition Lambda Fast-Path (No Spark execution)
The Step Functions state machine calls the `launch-condition` Lambda before spinning up EMR:
- File: [`cdk/lambdas/launch-condition/decide.mjs:L41-74`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/cdk/lambdas/launch-condition/decide.mjs#L41-L74)
  ```javascript
  if (state?.lastSuccessfulWriteTimestamp !== ts) {
    changed.push({ tableName, ts, sizeClass });
  }
  ```
  Since `state.lastSuccessfulWriteTimestamp` in DynamoDB (`"1639166200000"`) equals `manifest.lastSuccessfulWriteTimestamp` (`"1639166200000"`), `changed` is empty.
- Line 70-71:
  ```javascript
  if (changed.length === 0) {
    return { status: 'STOP', changedTables: [], sourceBucket, manifestKey };
  }
  ```
  The Step Function immediately stops with `status: "STOP"`. **Zero EMR or Spark resources are launched.**

#### Level 2: Spark Job Fast-Path (If Spark is invoked directly or standalone)
If Spark is executed (e.g. manually or via test script):

1. **Read Cursor**:
   [`IcebergIngest.scala:L196`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L196) gets `cursor = Some("1639166100000")` for `fp_100` and `Some("1639166200000")` for `fp_200`.
2. **Filter Folders**:
   [`TimestampFolderSelector.scala:L31-34`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/state/TimestampFolderSelector.scala#L31-L34):
   - `lo` = `cursor.toLong` (`1639166100000` / `1639166200000`).
   - Condition: `n > lo && n <= hi`.
   - Because all folders on disk have $n \le lo$, `selected` is `Seq.empty`.
3. **Per-fingerprint Log**:
   [`IcebergIngest.scala:L198-199`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L198-L199):
   ```scala
   log.info(s"'$table' fingerprint=$fp — ${all.size} folder(s) on disk, " +
     s"${selected.size} new (cursor=${cursor.getOrElse("∅")})")
   ```
4. **Immediate Early Return (Zero I/O)**:
   [`IcebergIngest.scala:L203-214`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L203-L214):
   ```scala
   val nonEmpty = plan.filter { case (_, ts) => ts.nonEmpty }
   if (nonEmpty.isEmpty) {
     Try(cursors.advanceHighWaterMark(table, entry.lastSuccessfulWriteTimestamp))
       .recover { case e => log.warn(s"HWM advance (no-op path) failed for $table: ${e.getMessage}") }
     log.info(s"'$table' — already up to date, no folders to read")
     return
   }
   ```
   **Execution stops here.**
   - No Parquet files are read.
   - No Iceberg catalog or metadata queries are executed.
   - No `MERGE` or `DELETE` statements run.
   - The job outputs the single log line:
     `'cc_account' — already up to date, no folders to read`
     and exits cleanly.

---

### Scenario B: New Parquet Files Available

Suppose CDA writes two new timestamp batches:
- `s3://my-cda-bucket/cc_account/fp_200/1639166300000/`
- `s3://my-cda-bucket/cc_account/fp_200/1639166400000/`

And `manifest.json` updates `lastSuccessfulWriteTimestamp` to `"1639166400000"`.

#### Execution Flow:

1. **Lambda Detection**:
   [`decide.mjs:L57`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/cdk/lambdas/launch-condition/decide.mjs#L57) detects `state.lastSuccessfulWriteTimestamp ("1639166200000") !== ts ("1639166400000")`. Dispatches `cc_account` to Spark on EMR.
2. **Cursor Evaluation**:
   - For `fp_100`: Cursor = `"1639166100000"`, S3 has no new folders $\rightarrow$ `selected = Seq.empty`.
   - For `fp_200`: Cursor = `"1639166200000"`, S3 has folders `["1639166200000", "1639166300000", "1639166400000"]`.
3. **Folder Selection Filtering**:
   [`TimestampFolderSelector.scala:L30-37`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/state/TimestampFolderSelector.scala#L30-L37):
   ```scala
   val lo = 1639166200000L
   val hi = 1639166400000L
   folders.filter { case (_, n) => n > lo && n <= hi }
   ```
   - `1639166200000` is dropped ($n \not> lo$).
   - `1639166300000` is kept ($lo < n \le hi$).
   - `1639166400000` is kept ($lo < n \le hi$).
   - Result: `selected = Seq("1639166300000", "1639166400000")`.
4. **Selective Read**:
   [`IcebergIngest.scala:L500-501`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L500-L501):
   Spark only reads the 2 new paths:
   ```scala
   val paths = Seq(
     "s3://my-cda-bucket/cc_account/fp_200/1639166300000/",
     "s3://my-cda-bucket/cc_account/fp_200/1639166400000/"
   )
   val raw = spark.read.parquet(paths: _*)
   ```
   **The previous folder (`1639166200000`) and older `fp_100` folders are never touched by Spark.**
5. **Atomic Raw Append**:
   [`IcebergIngest.scala:L400`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L400):
   Only the new micro-batches are appended to `cc_account_raw`.
6. **Advance Fingerprint Cursor**:
   [`IcebergIngest.scala:L408`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L408):
   DynamoDB cursor for `fp_200` advances to `"1639166400000"`.
7. **Incremental MERGE / DELETE**:
   [`IcebergIngest.scala:L444-450`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L444-L450):
   Updates `cc_account_merged`.
   *(Optional optimization: If `INCREMENTAL_MERGE_FILTER=true`, lines 700-704 append `AND cda_load_ts > timestamp_millis(<mergeWatermark>)`, preventing Spark from rescanning old raw records during MERGE).*
8. **Advance Global HWM**:
   [`IcebergIngest.scala:L242`](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L242):
   DynamoDB `lastSuccessfulWriteTimestamp` is set to `"1639166400000"`.

---

## 5. Summary Matrix

| Metric / Step | First-Time Run | Incremental Run (New Data) | Incremental Run (No New Data) |
|---|---|---|---|
| **DynamoDB Cursor Check** | Returns empty / `None` | Returns previous timestamp folder string | Returns previous timestamp folder string |
| **S3 Folders Selected** | All folders $\le$ `lastSuccessfulWriteTimestamp` | Only folders $> cursor$ and $\le$ `lastSuccessfulWriteTimestamp` | `0` folders selected (`Seq.empty`) |
| **Parquet Files Read** | All historical Parquet files | **Only new Parquet micro-batches** | **None** (zero S3 read actions) |
| **Iceberg Raw Append** | Full dataset appended | Delta appended | Skipped |
| **Iceberg Merged MERGE** | Full initial deduplication | Incremental upsert / tombstone delete | Skipped |
| **Log Output When Complete** | Logged append and merge counts | Logged new partitions and MERGE counts | `'<table> — already up to date, no folders to read'` ([IcebergIngest.scala:L212](file:///Users/krishna/Avocado/datalake/raj_projects/sample-guidewire-cda-iceberg/src/main/scala/gw/cda/iceberg/IcebergIngest.scala#L212)) |
