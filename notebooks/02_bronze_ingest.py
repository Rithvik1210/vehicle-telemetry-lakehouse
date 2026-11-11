# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC Reads whatever JSON files are sitting in `landing/` using Auto Loader and
# MAGIC appends them, as-is, into an append-only Bronze Delta table.
# MAGIC Nothing is cleaned, deduplicated, or dropped here — that happens in Silver.
# MAGIC Re-running this notebook should NOT add already-ingested rows again
# MAGIC (idempotent via the Auto Loader checkpoint).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "vehicle_telemetry", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_telemetry"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_telemetry_schema"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_telemetry"
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read new landing files with Auto Loader
# MAGIC
# MAGIC `cloudFiles` tracks which files it has already seen via the checkpoint, so a
# MAGIC second run with no new files ingests zero rows. `rescuedDataColumn` catches
# MAGIC any field that doesn't match the inferred schema instead of silently dropping it.

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load(LANDING_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add ingestion metadata and write to Bronze
# MAGIC
# MAGIC `_source_file` and `_ingest_ts` let you trace any row back to exactly which
# MAGIC landing file and when it was picked up — useful for debugging and reconciliation.

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC
# MAGIC 1. Row count in Bronze.
# MAGIC 2. Re-run this whole notebook once more — the count below should NOT change,
# MAGIC    proving the ingestion is idempotent (no duplicate ingestion on re-run).

# COMMAND ----------

display(spark.sql(f"SELECT COUNT(*) AS bronze_row_count FROM {BRONZE_TABLE}"))
display(spark.sql(f"SELECT * FROM {BRONZE_TABLE} LIMIT 20"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation: Bronze count vs. landing file line count
# MAGIC
# MAGIC Confirms every line written by the generator actually made it into Bronze,
# MAGIC duplicates included (Bronze is append-only and keeps everything).

# COMMAND ----------

landing_files = dbutils.fs.ls(LANDING_PATH)
total_lines = 0
for f in landing_files:
    content = dbutils.fs.head(f.path, 10_000_000)
    total_lines += len([l for l in content.split("\n") if l.strip()])

bronze_count = spark.sql(f"SELECT COUNT(*) AS c FROM {BRONZE_TABLE}").collect()[0]["c"]

print(f"Landing file lines : {total_lines}")
print(f"Bronze row count    : {bronze_count}")
print(f"Match: {total_lines == bronze_count}")
