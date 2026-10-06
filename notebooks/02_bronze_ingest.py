# Bronze Ingestion

Reads whatever JSON files are sitting in `landing/` using Auto Loader and
appends them, as-is, into an append-only Bronze Delta table.
Nothing is cleaned, deduplicated, or dropped here — that happens in Silver.
Re-running this notebook should NOT add already-ingested rows again
(idempotent via the Auto Loader checkpoint).
CATALOG = "main"
SCHEMA = "vehicle_telemetry"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_telemetry"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_telemetry_schema"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_telemetry"
from pyspark.sql import functions as F
## Read new landing files with Auto Loader

`cloudFiles` tracks which files it has already seen via the checkpoint, so a
second run with no new files ingests zero rows. `rescuedDataColumn` catches
any field that doesn't match the inferred schema instead of silently dropping it.
raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load(LANDING_PATH)
)
## Add ingestion metadata and write to Bronze

`_source_file` and `_ingest_ts` let you trace any row back to exactly which
landing file and when it was picked up — useful for debugging and reconciliation.
bronze_stream = (
    raw_stream
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingest_ts", F.current_timestamp())
)

query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()
print(f"Bronze ingestion complete. Table: {BRONZE_TABLE}")
## Sanity checks

1. Row count in Bronze.
2. Re-run this whole notebook once more — the count below should NOT change,
   proving the ingestion is idempotent (no duplicate ingestion on re-run).
display(spark.sql(f"SELECT COUNT(*) AS bronze_row_count FROM {BRONZE_TABLE}"))
display(spark.sql(f"SELECT * FROM {BRONZE_TABLE} LIMIT 20"))
## Reconciliation: Bronze count vs. landing file line count

Confirms every line written by the generator actually made it into Bronze,
duplicates included (Bronze is append-only and keeps everything).
landing_files = dbutils.fs.ls(LANDING_PATH)
total_lines = 0
for f in landing_files:
    content = dbutils.fs.head(f.path, 10_000_000)
    total_lines += len([l for l in content.split("\n") if l.strip()])

bronze_count = spark.sql(f"SELECT COUNT(*) AS c FROM {BRONZE_TABLE}").collect()[0]["c"]

print(f"Landing file lines : {total_lines}")
print(f"Bronze row count    : {bronze_count}")
print(f"Match: {total_lines == bronze_count}")
