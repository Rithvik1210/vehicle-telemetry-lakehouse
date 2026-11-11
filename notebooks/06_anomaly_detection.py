# Databricks notebook source
# MAGIC %md
# MAGIC # Anomaly Detection: Isolation Forest + MLflow
# MAGIC
# MAGIC This notebook asks whether an unsupervised model, which never sees the
# MAGIC labels, can find the fault windows the generator injected.
# MAGIC
# MAGIC 1. **Features.** Silver readings are rolled up into 5-minute windows for each
# MAGIC    vehicle. Daily Gold rows are too coarse here, because a fault lasts only
# MAGIC    20 to 90 minutes.
# MAGIC 2. **Model.** An Isolation Forest is fit on every window. Labels are never
# MAGIC    used for training.
# MAGIC 3. **Evaluation.** Scores are compared with `labels/fault_windows.json`,
# MAGIC    which notebook 01 wrote and no other step has read.
# MAGIC 4. **Tracking.** Parameters, metrics, plots and the model are logged to
# MAGIC    MLflow, and the model is registered in Unity Catalog.
# MAGIC
# MAGIC **Leakage guard:** `dtc_code` is not used as a feature. The generator sets
# MAGIC it only inside fault windows, so including it would make the task trivial.
# MAGIC Instead it serves as a rule-based baseline for comparison.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "vehicle_telemetry", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_telemetry"
SCORES_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomaly_scores"
LABELS_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/labels/fault_windows.json"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.telemetry_anomaly_iforest"

# Config. These are fixed up front, not tuned until the metrics look good.
WINDOW_MINUTES = 5
N_ESTIMATORS = 200
# Alert budget: the share of windows we're willing to flag for review.
# 0.5% of ~40k windows is about 200 alerts per week for a 20-vehicle fleet.
CONTAMINATION = 0.005
RANDOM_STATE = 42

FEATURE_COLS = [
    "avg_engine_temp_c", "max_engine_temp_c", "std_engine_temp_c",
    "avg_battery_v", "min_battery_v", "std_battery_v",
    "avg_speed_kmph", "avg_rpm",
]

print(f"Reading from: {SILVER_TABLE}")
print(f"Labels from:  {LABELS_PATH}")
print(f"Writing to:   {SCORES_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build 5-minute window features from Silver
# MAGIC
# MAGIC Silver can still contain nulls in non-key sensor fields, since only
# MAGIC missing keys are quarantined. Spark's `avg`/`min`/`max` skip nulls, and any
# MAGIC window that ends up with no value for a feature is filled with that
# MAGIC feature's median.

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table(SILVER_TABLE)

windows_df = (
    silver_df
    .groupBy("vehicle_id", F.window("event_ts", f"{WINDOW_MINUTES} minutes").alias("w"))
    .agg(
        F.avg("engine_temp_c").alias("avg_engine_temp_c"),
        F.max("engine_temp_c").alias("max_engine_temp_c"),
        F.stddev("engine_temp_c").alias("std_engine_temp_c"),
        F.avg("battery_v").alias("avg_battery_v"),
        F.min("battery_v").alias("min_battery_v"),
        F.stddev("battery_v").alias("std_battery_v"),
        F.avg("speed_kmph").alias("avg_speed_kmph"),
        F.avg("rpm").alias("avg_rpm"),
        F.count("*").alias("reading_count"),
        # Kept for the baseline only. Not a model feature.
        F.max(F.col("dtc_code").isNotNull().cast("int")).alias("has_dtc"),
    )
    .select(
        "vehicle_id",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        *FEATURE_COLS, "reading_count", "has_dtc",
    )
)

pdf = windows_df.toPandas()
for c in FEATURE_COLS:
    pdf[c] = pdf[c].astype(float).fillna(pdf[c].median())

print(f"Windows: {len(pdf):,}  (vehicles: {pdf.vehicle_id.nunique()})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach ground-truth labels
# MAGIC
# MAGIC A window counts as a fault window if it overlaps an injected fault
# MAGIC interval for the same vehicle. Labels are used only from this point on.

# COMMAND ----------

import pandas as pd

labels_pdf = (
    spark.read.option("multiline", "true").json(LABELS_PATH)
    .select(
        "vehicle_id", "fault_type",
        F.col("fault_start").cast("timestamp").alias("fault_start"),
        F.col("fault_end").cast("timestamp").alias("fault_end"),
    )
    .toPandas()
)
labels_pdf["fault_id"] = labels_pdf.index
display(labels_pdf)

pdf["is_fault"] = 0
pdf["fault_id"] = -1
for _, f in labels_pdf.iterrows():
    hit = (
        (pdf.vehicle_id == f.vehicle_id)
        & (pdf.window_start <= f.fault_end)
        & (pdf.window_end > f.fault_start)
    )
    pdf.loc[hit, "is_fault"] = 1
    pdf.loc[hit, "fault_id"] = f.fault_id

print(f"Fault windows: {pdf.is_fault.sum()} of {len(pdf):,} ({pdf.is_fault.mean():.3%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train, score and log to MLflow

# COMMAND ----------

import mlflow
import numpy as np
import matplotlib.pyplot as plt
from mlflow.models import infer_signature
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score, average_precision_score, precision_score, recall_score,
)

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/vehicle-telemetry-anomaly")
mlflow.set_registry_uri("databricks-uc")

X = pdf[FEATURE_COLS]
y = pdf["is_fault"].values


def event_recall(flags):
    """Share of injected faults with at least one flagged window."""
    caught = pdf.loc[(flags == 1) & (pdf.fault_id >= 0), "fault_id"].nunique()
    return caught / len(labels_pdf)


with mlflow.start_run(run_name="iforest_5min") as run:
    model = Pipeline([
        ("scale", StandardScaler()),
        ("iforest", IsolationForest(
            n_estimators=N_ESTIMATORS,
            contamination=CONTAMINATION,
            random_state=RANDOM_STATE,
        )),
    ])
    model.fit(X)  # unsupervised: y is never passed in

    # Higher = more anomalous
    pdf["anomaly_score"] = -model.score_samples(X)
    pdf["is_flagged"] = (model.predict(X) == -1).astype(int)

    # The DTC baseline flags any window that contains a fault code
    dtc_flags = pdf["has_dtc"].values

    metrics = {
        "roc_auc": roc_auc_score(y, pdf.anomaly_score),
        "avg_precision": average_precision_score(y, pdf.anomaly_score),
        "precision_at_budget": precision_score(y, pdf.is_flagged, zero_division=0),
        "recall_at_budget": recall_score(y, pdf.is_flagged),
        "event_recall_at_budget": event_recall(pdf.is_flagged.values),
        "flagged_windows": int(pdf.is_flagged.sum()),
        "fault_windows": int(y.sum()),
        "total_windows": len(pdf),
        "baseline_dtc_precision": precision_score(y, dtc_flags, zero_division=0),
        "baseline_dtc_recall": recall_score(y, dtc_flags),
    }

    mlflow.log_params({
        "window_minutes": WINDOW_MINUTES,
        "n_estimators": N_ESTIMATORS,
        "contamination": CONTAMINATION,
        "features": ",".join(FEATURE_COLS),
        "silver_table": SILVER_TABLE,
    })
    mlflow.log_metrics(metrics)

    # Plot: score distribution for normal and fault windows
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(pdf.loc[pdf.is_fault == 0, "anomaly_score"], bins=80, alpha=0.6, label="normal", density=True)
    ax.hist(pdf.loc[pdf.is_fault == 1, "anomaly_score"], bins=30, alpha=0.6, label="injected fault", density=True)
    threshold = pdf.loc[pdf.is_flagged == 1, "anomaly_score"].min()
    ax.axvline(threshold, color="k", ls="--", label="alert threshold")
    ax.set_xlabel("anomaly score")
    ax.set_ylabel("density")
    ax.legend()
    ax.set_title("Isolation Forest scores, 5-minute windows")
    mlflow.log_figure(fig, "score_distribution.png")
    plt.show()

    # Log and register the model
    signature = infer_signature(X, model.score_samples(X))
    mlflow.sklearn.log_model(
        model, artifact_path="model",
        signature=signature, input_example=X.head(5),
        registered_model_name=MODEL_NAME,
    )

    run_id = run.info.run_id

for k, v in metrics.items():
    print(f"{k:26s} {v:.4f}" if isinstance(v, float) else f"{k:26s} {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which windows were flagged, and why
# MAGIC
# MAGIC Flagged windows outside a labeled fault interval are worth a look before
# MAGIC calling them false positives. The generator shifts 3% of readings back by
# MAGIC 1 to 30 minutes, so a real fault reading can land just before its labeled
# MAGIC interval.

# COMMAND ----------

flagged = pdf[pdf.is_flagged == 1].sort_values("anomaly_score", ascending=False)
display(spark.createDataFrame(
    flagged[["vehicle_id", "window_start", "anomaly_score", "is_fault", "has_dtc"] + FEATURE_COLS]
))

fp = flagged[flagged.is_fault == 0]
print(f"Flagged but unlabeled: {len(fp)}")
print(f"  of which contain a DTC code (likely a time-shifted fault reading): {int(fp.has_dtc.sum())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write scores to Gold
# MAGIC
# MAGIC This table is overwritten on every run. Each row carries `mlflow_run_id`,
# MAGIC so any score can be traced back to the model that produced it.

# COMMAND ----------

out_cols = ["vehicle_id", "window_start", "window_end", *FEATURE_COLS,
            "reading_count", "anomaly_score", "is_flagged", "is_fault"]
scores_df = (
    spark.createDataFrame(pdf[out_cols])
    .withColumn("mlflow_run_id", F.lit(run_id))
    .withColumn("_scored_at", F.current_timestamp())
)
(
    scores_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SCORES_TABLE)
)
print(f"Wrote {scores_df.count():,} rows to {SCORES_TABLE}")
