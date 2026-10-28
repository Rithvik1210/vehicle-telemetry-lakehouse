# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Trip Summaries + Vehicle Daily Health
# MAGIC
# MAGIC Reads `silver_telemetry` and produces two business-facing tables:
# MAGIC 1. `gold_trip_summary` - one row per detected trip per vehicle
# MAGIC    (start/end time, distance, avg/max speed, max engine temp).
# MAGIC 2. `gold_vehicle_daily_health` - one row per vehicle per day
# MAGIC    (avg/max temp, min battery voltage, that day's quarantine rate).
# MAGIC
# MAGIC These are the tables someone would actually query or put in a
# MAGIC dashboard - Bronze and Silver exist to make these trustworthy.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "vehicle_telemetry", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_telemetry"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.quarantine_telemetry"
GOLD_TRIP_TABLE = f"{CATALOG}.{SCHEMA}.gold_trip_summary"
GOLD_HEALTH_TABLE = f"{CATALOG}.{SCHEMA}.gold_vehicle_daily_health"

print(f"Reading from: {SILVER_TABLE}")
print(f"Writing to:   {GOLD_TRIP_TABLE}")
print(f"Writing to:   {GOLD_HEALTH_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_df = spark.table(SILVER_TABLE)
quarantine_df = spark.table(QUARANTINE_TABLE)

print(f"Silver rows: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 1: Trip summaries
# MAGIC
# MAGIC A "trip" is a stretch of readings for one vehicle with no gap longer
# MAGIC than `TRIP_GAP_MINUTES`. A gap that long means the vehicle was off or
# MAGIC the feed paused, so a new trip starts after it. This is a simple,
# MAGIC explainable rule - not perfect, but it's exactly the kind of rule
# MAGIC you'd refine with a real domain expert in production.

# COMMAND ----------

TRIP_GAP_MINUTES = 10

vehicle_window = Window.partitionBy("vehicle_id").orderBy("event_ts")

with_gaps_df = (
    silver_df
    .withColumn("prev_ts", F.lag("event_ts").over(vehicle_window))
    .withColumn(
        "gap_minutes",
        (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0,
    )
    .withColumn(
        "is_new_trip",
        (F.col("prev_ts").isNull()) | (F.col("gap_minutes") > TRIP_GAP_MINUTES),
    )
)

# Assign a trip number per vehicle: running count of "new trip" flags
trip_id_df = with_gaps_df.withColumn(
    "trip_num",
    F.sum(F.col("is_new_trip").cast("int")).over(
        vehicle_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ),
).withColumn(
    "trip_id", F.concat_ws("-", F.col("vehicle_id"), F.col("trip_num").cast("string"))
)

# COMMAND ----------

trip_summary_df = (
    trip_id_df.groupBy("vehicle_id", "trip_id")
    .agg(
        F.min("event_ts").alias("trip_start_ts"),
        F.max("event_ts").alias("trip_end_ts"),
        F.count("*").alias("reading_count"),
        F.round(F.avg("speed_kmph"), 1).alias("avg_speed_kmph"),
        F.round(F.max("speed_kmph"), 1).alias("max_speed_kmph"),
        F.round(F.max("engine_temp_c"), 1).alias("max_engine_temp_c"),
        F.round(
            F.max("odometer_km") - F.min("odometer_km"), 1
        ).alias("distance_km"),
    )
    .withColumn(
        "trip_duration_minutes",
        F.round(
            (F.col("trip_end_ts").cast("long") - F.col("trip_start_ts").cast("long")) / 60.0,
            1,
        ),
    )
    # drop trivial single-reading "trips" - just noise, not a real trip
    .filter(F.col("reading_count") > 1)
)

trip_count = trip_summary_df.count()
print(f"Trips detected: {trip_count}")

(
    trip_summary_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_TRIP_TABLE)
)

print(f"Wrote {trip_count} rows to {GOLD_TRIP_TABLE}")
display(spark.table(GOLD_TRIP_TABLE).orderBy("vehicle_id", "trip_start_ts").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold table 2: Vehicle daily health
# MAGIC
# MAGIC One row per vehicle per day. Includes that day's quarantine rate so
# MAGIC a dashboard could flag "this vehicle's feed got noisy on this day,"
# MAGIC not just its sensor readings.

# COMMAND ----------

daily_silver_df = (
    silver_df
    .withColumn("event_date", F.to_date("event_ts"))
    .groupBy("vehicle_id", "event_date")
    .agg(
        F.round(F.avg("engine_temp_c"), 1).alias("avg_engine_temp_c"),
        F.round(F.max("engine_temp_c"), 1).alias("max_engine_temp_c"),
        F.round(F.min("battery_v"), 2).alias("min_battery_v"),
        F.round(F.avg("battery_v"), 2).alias("avg_battery_v"),
        F.round(F.max("speed_kmph"), 1).alias("max_speed_kmph"),
        F.count("*").alias("silver_reading_count"),
    )
)

daily_quarantine_df = (
    quarantine_df
    .withColumn("event_date", F.to_date("event_ts"))
    .groupBy("vehicle_id", "event_date")
    .agg(F.count("*").alias("quarantine_reading_count"))
)

daily_health_df = (
    daily_silver_df
    .join(daily_quarantine_df, on=["vehicle_id", "event_date"], how="left")
    .fillna({"quarantine_reading_count": 0})
    .withColumn(
        "total_readings",
        F.col("silver_reading_count") + F.col("quarantine_reading_count"),
    )
    .withColumn(
        "daily_quarantine_rate",
        F.round(F.col("quarantine_reading_count") / F.col("total_readings"), 4),
    )
)

health_count = daily_health_df.count()
print(f"Vehicle-day rows: {health_count}")

(
    daily_health_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_HEALTH_TABLE)
)

print(f"Wrote {health_count} rows to {GOLD_HEALTH_TABLE}")
display(spark.table(GOLD_HEALTH_TABLE).orderBy("vehicle_id", "event_date").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

expected_vehicle_days = 20 * 7  # 20 vehicles x 7 days
print(f"Expected vehicle-days: {expected_vehicle_days}")
print(f"Actual vehicle-days:   {health_count}")
print(f"Match: {health_count == expected_vehicle_days}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Idempotency check
# MAGIC
# MAGIC Overwrite mode means re-running this notebook should give identical
# MAGIC counts as long as Silver/Quarantine haven't changed.

# COMMAND ----------

check_trips = spark.sql(f"SELECT COUNT(*) AS c FROM {GOLD_TRIP_TABLE}").collect()[0]["c"]
check_health = spark.sql(f"SELECT COUNT(*) AS c FROM {GOLD_HEALTH_TABLE}").collect()[0]["c"]
print(f"Gold trip rows:   {check_trips}")
print(f"Gold health rows: {check_health}")

