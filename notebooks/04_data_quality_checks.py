# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC
# MAGIC Runs a set of named rules against `silver_telemetry` and
# MAGIC `quarantine_telemetry`, and writes the results - pass/fail plus the
# MAGIC actual numbers - to a `dq_results` table. This is the "scorecard":
# MAGIC proof the cleaning actually worked, not just a claim that it did.
# MAGIC
# MAGIC Each rule has a plain-language reason, since "why this threshold" is
# MAGIC exactly what an interviewer will ask.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "vehicle_telemetry", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_telemetry"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.quarantine_telemetry"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_telemetry"
DQ_RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.dq_results"

print(f"Checking:  {SILVER_TABLE}")
print(f"Writing to: {DQ_RESULTS_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

silver_df = spark.table(SILVER_TABLE)
quarantine_df = spark.table(QUARANTINE_TABLE)
bronze_df = spark.table(BRONZE_TABLE)

silver_count = silver_df.count()
quarantine_count = quarantine_df.count()
bronze_count = bronze_df.count()

run_ts = datetime.utcnow()
print(f"Run timestamp: {run_ts.isoformat()}")
print(f"Bronze: {bronze_count} | Silver: {silver_count} | Quarantine: {quarantine_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the rules
# MAGIC
# MAGIC Each rule returns: a metric value, and whether it passes a threshold.
# MAGIC Thresholds are deliberately loose/generous at first - the point of
# MAGIC Week 3 is to *measure* quality, not to silently tune thresholds until
# MAGIC everything passes.

# COMMAND ----------

results = []


def add_result(rule_name, metric_value, threshold, passed, description):
    results.append({
        "run_ts": run_ts,
        "rule_name": rule_name,
        "metric_value": float(metric_value),
        "threshold": float(threshold),
        "passed": bool(passed),
        "description": description,
    })


# Rule 1: overall quarantine rate should be small - if most data is bad,
# something upstream is broken, not just a few noisy sensors.
quarantine_rate = quarantine_count / bronze_count if bronze_count else 0
QUARANTINE_RATE_THRESHOLD = 0.05  # no more than 5% of rows should be bad
add_result(
    "quarantine_rate",
    quarantine_rate,
    QUARANTINE_RATE_THRESHOLD,
    quarantine_rate <= QUARANTINE_RATE_THRESHOLD,
    "Share of Bronze rows that ended up in quarantine. High values suggest "
    "an upstream sensor or feed problem, not just normal noise.",
)

# Rule 2: null rate on core numeric fields in Silver - Silver should be
# mostly complete, since clearly-bad rows were already removed.
core_fields = ["speed_kmph", "engine_temp_c", "battery_v", "fuel_pct"]
null_counts = silver_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in core_fields
]).collect()[0].asDict()
total_core_values = silver_count * len(core_fields)
total_nulls = sum(null_counts.values())
null_rate = total_nulls / total_core_values if total_core_values else 0
NULL_RATE_THRESHOLD = 0.05
add_result(
    "silver_null_rate_core_fields",
    null_rate,
    NULL_RATE_THRESHOLD,
    null_rate <= NULL_RATE_THRESHOLD,
    "Share of null values across core sensor fields in Silver. These nulls "
    "are expected (simulated sensor gaps) but should stay a small minority.",
)

# Rule 3: freshness - every vehicle should have at least one Silver reading.
# A vehicle with zero rows would mean its entire feed got lost or quarantined.
vehicles_with_data = silver_df.select("vehicle_id").distinct().count()
EXPECTED_VEHICLES = 20
vehicle_coverage = vehicles_with_data / EXPECTED_VEHICLES
add_result(
    "vehicle_coverage",
    vehicle_coverage,
    1.0,
    vehicles_with_data >= EXPECTED_VEHICLES,
    "Fraction of expected vehicles that have at least one Silver row. "
    "Should be 100% - a missing vehicle means its whole feed was lost.",
)

# Rule 4: no duplicate (vehicle_id, event_ts) pairs should remain in Silver -
# confirms the Week 2 dedup logic actually worked.
dup_check = (
    silver_df.groupBy("vehicle_id", "event_ts")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
add_result(
    "silver_duplicate_keys",
    dup_check,
    0,
    dup_check == 0,
    "Count of (vehicle_id, event_ts) pairs appearing more than once in "
    "Silver. Should be zero - confirms the Silver dedup step worked.",
)

# Rule 5: timestamp sanity - no Silver row should have an event_ts in the
# future relative to when it was ingested.
future_ts_count = silver_df.filter(
    F.col("event_ts") > F.col("_ingest_ts")
).count()
add_result(
    "no_future_timestamps",
    future_ts_count,
    0,
    future_ts_count == 0,
    "Count of rows where the event timestamp is after the ingestion "
    "timestamp. Should be zero - a reading can't be ingested before it happened.",
)

for r in results:
    status = "PASS" if r["passed"] else "FAIL"
    print(f"[{status}] {r['rule_name']}: {r['metric_value']:.4f} (threshold {r['threshold']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write results to the dq_results table
# MAGIC
# MAGIC Append mode on purpose - every run adds a new row per rule, so the
# MAGIC table becomes a history of quality over time, not just the latest check.

# COMMAND ----------

results_df = spark.createDataFrame(results)

(
    results_df.write
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(DQ_RESULTS_TABLE)
)

print(f"Wrote {len(results)} rule results to {DQ_RESULTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall summary for this run

# COMMAND ----------

latest_run = (
    spark.table(DQ_RESULTS_TABLE)
    .filter(F.col("run_ts") == run_ts)
)

total_rules = latest_run.count()
passed_rules = latest_run.filter(F.col("passed")).count()

print(f"Rules passed: {passed_rules} / {total_rules}")
display(latest_run.orderBy("rule_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality trend over time (meaningful after a few runs)

# COMMAND ----------

display(
    spark.table(DQ_RESULTS_TABLE)
    .groupBy("run_ts")
    .agg(
        F.count("*").alias("total_rules"),
        F.sum(F.col("passed").cast("int")).alias("rules_passed"),
    )
    .orderBy("run_ts")
)
