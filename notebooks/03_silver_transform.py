# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Clean Telemetry
# MAGIC
# MAGIC Reads the raw Bronze table and produces two outputs:
# MAGIC 1. `silver_telemetry` - typed, deduplicated, trustworthy rows (late rows
# MAGIC    kept but flagged, since "late" isn't the same as "bad").
# MAGIC 2. `quarantine_telemetry` - rows with a genuine problem (missing key
# MAGIC    fields, impossible sensor values), kept separately with a reason
# MAGIC    column instead of being silently dropped.
# MAGIC
# MAGIC This notebook is a **batch overwrite**: every run reads the full Bronze
# MAGIC table and rebuilds Silver + Quarantine from scratch. That makes it
# MAGIC idempotent (safe to re-run) without needing streaming/merge logic yet.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "vehicle_telemetry", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_telemetry"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_telemetry"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.quarantine_telemetry"

print(f"Reading from:  {BRONZE_TABLE}")
print(f"Writing to:    {SILVER_TABLE}")
print(f"Quarantine to: {QUARANTINE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze and cast types
# MAGIC
# MAGIC Bronze stores everything as loosely-typed JSON-derived columns. Silver is
# MAGIC where we commit to real types, since every downstream consumer should be
# MAGIC able to trust that `event_ts` is actually a timestamp, not a string.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

bronze_df = spark.table(BRONZE_TABLE)

typed_df = (
    bronze_df
    .withColumn("event_ts", F.col("event_ts").cast(TimestampType()))
    .withColumn("speed_kmph", F.col("speed_kmph").cast(DoubleType()))
    .withColumn("rpm", F.col("rpm").cast(IntegerType()))
    .withColumn("engine_temp_c", F.col("engine_temp_c").cast(DoubleType()))
    .withColumn("battery_v", F.col("battery_v").cast(DoubleType()))
    .withColumn("fuel_pct", F.col("fuel_pct").cast(DoubleType()))
    .withColumn("lat", F.col("lat").cast(DoubleType()))
    .withColumn("lon", F.col("lon").cast(DoubleType()))
    .withColumn("odometer_km", F.col("odometer_km").cast(DoubleType()))
)

print(f"Bronze rows loaded: {typed_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remove exact duplicates
# MAGIC
# MAGIC The generator deliberately re-sends some readings as exact duplicates.
# MAGIC A duplicate here means every field matches except the ingestion
# MAGIC bookkeeping columns (`_source_file`, `_ingest_ts`), which will always
# MAGIC differ slightly even for a true duplicate reading.

# COMMAND ----------

business_cols = [
    "vehicle_id", "event_ts", "speed_kmph", "rpm", "engine_temp_c",
    "battery_v", "fuel_pct", "lat", "lon", "odometer_km", "dtc_code",
]

before_count = typed_df.count()
deduped_df = typed_df.dropDuplicates(business_cols)
after_count = deduped_df.count()

print(f"Rows before dedup: {before_count}")
print(f"Rows after dedup:  {after_count}")
print(f"Duplicates removed: {before_count - after_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define what counts as "bad" vs "late"
# MAGIC
# MAGIC These thresholds are config, not guesses - each one should be easy to
# MAGIC justify in an interview:
# MAGIC - Required fields missing -> can't trust the row at all -> quarantine
# MAGIC - Sensor value outside a physically plausible range -> quarantine
# MAGIC - Timestamp older than the ingest time by more than a few minutes ->
# MAGIC   not wrong, just late -> keep in Silver, flagged

# COMMAND ----------

LATE_THRESHOLD_MINUTES = 5

is_missing_required = (
    F.col("vehicle_id").isNull()
    | F.col("event_ts").isNull()
)

is_out_of_range = (
    (F.col("speed_kmph") < 0) | (F.col("speed_kmph") > 200)
    | (F.col("rpm") < 0) | (F.col("rpm") > 8000)
    | (F.col("engine_temp_c") < -20) | (F.col("engine_temp_c") > 150)
    | (F.col("battery_v") < 0) | (F.col("battery_v") > 20)
    | (F.col("fuel_pct") < 0) | (F.col("fuel_pct") > 100)
    | (F.col("lat") < -90) | (F.col("lat") > 90)
    | (F.col("lon") < -180) | (F.col("lon") > 180)
)

is_bad = is_missing_required | is_out_of_range

is_late = (
    F.col("event_ts").isNotNull()
    & F.col("_ingest_ts").isNotNull()
    & (
        (F.col("_ingest_ts").cast("long") - F.col("event_ts").cast("long"))
        > (LATE_THRESHOLD_MINUTES * 60)
    )
)

reason_col = (
    F.when(is_missing_required, F.lit("missing_required_field"))
    .when(is_out_of_range, F.lit("out_of_range_value"))
    .otherwise(F.lit(None))
)

flagged_df = (
    deduped_df
    .withColumn("_is_bad", is_bad)
    .withColumn("_is_late", is_late)
    .withColumn("_quarantine_reason", reason_col)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split into Silver (good) and Quarantine (bad)
# MAGIC
# MAGIC Note: a row can be both "late" and "good" at the same time - lateness
# MAGIC alone never sends a row to quarantine, it's just informational.

# COMMAND ----------

silver_df = (
    flagged_df
    .filter(~F.col("_is_bad"))
    .drop("_quarantine_reason", "_is_bad")
)

quarantine_df = (
    flagged_df
    .filter(F.col("_is_bad"))
    .withColumn("_quarantined_at", F.current_timestamp())
)

silver_count = silver_df.count()
quarantine_count = quarantine_df.count()
late_count = silver_df.filter(F.col("_is_late")).count()

print(f"Silver rows (good):      {silver_count}")
print(f"  of which late-arriving: {late_count}")
print(f"Quarantine rows (bad):   {quarantine_count}")
print(f"Total: {silver_count + quarantine_count} (should equal deduped count {after_count})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver and Quarantine tables
# MAGIC
# MAGIC Overwrite mode: this notebook always rebuilds both tables from the
# MAGIC current Bronze contents, so re-running it is safe and gives the same
# MAGIC result as long as Bronze hasn't changed.

# COMMAND ----------

(
    silver_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

(
    quarantine_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_TABLE)
)

print("Silver and Quarantine tables written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine breakdown by reason (useful for the Week 3 QA scorecard)

# COMMAND ----------

display(
    spark.table(QUARANTINE_TABLE)
    .groupBy("_quarantine_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency check
# MAGIC
# MAGIC Run this cell, then run the whole notebook again, then run this cell
# MAGIC again. The two counts should match.

# COMMAND ----------

check_silver = spark.sql(f"SELECT COUNT(*) AS c FROM {SILVER_TABLE}").collect()[0]["c"]
check_quarantine = spark.sql(f"SELECT COUNT(*) AS c FROM {QUARANTINE_TABLE}").collect()[0]["c"]
print(f"Silver row count:      {check_silver}")
print(f"Quarantine row count:  {check_quarantine}")
