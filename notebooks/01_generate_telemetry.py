# Vehicle Telemetry Generator

Simulates ~20 vehicles sending telemetry every 10 seconds over 7 days.
Writes data in hourly JSON batches to `landing/` to mimic real arrival,
on purpose including duplicates, nulls, out-of-range values, late timestamps,
and GPS jumps so the Bronze/Silver layers have something real to clean.

All data is 100% synthetic. No real vehicles, people, or company data.
import json
import random
import math
from datetime import datetime, timedelta

random.seed(42)  # reproducible runs
## Config — change these to scale the dataset up or down
CATALOG = "main"                       # change to your catalog
SCHEMA = "vehicle_telemetry"
VOLUME = "landing"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

NUM_VEHICLES = 20
NUM_DAYS = 7
INTERVAL_SECONDS = 10
START_TIME = datetime(2026, 1, 1, 0, 0, 0)

# "messiness" rates — these are intentionally injected, not measured from anywhere
DUPLICATE_RATE = 0.02
NULL_RATE = 0.015
OUT_OF_RANGE_RATE = 0.01
LATE_ARRIVAL_RATE = 0.03          # timestamp shuffled backwards within the batch
GPS_JUMP_RATE = 0.005

# fault injection: a handful of vehicles get a deliberate fault window each,
# used later as ground truth to test data quality rules and (optionally) anomaly detection
FAULT_VEHICLE_COUNT = 4
## Set up catalog, schema, and landing volume
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.labels")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")

dbutils.fs.mkdirs(LANDING_PATH)
## Vehicle fleet setup

Each vehicle gets a home base GPS point and a baseline driving profile.
A few vehicles are pre-assigned a fault window (e.g. overheating, battery drain).
HYDERABAD_LAT, HYDERABAD_LON = 17.3850, 78.4867

vehicles = []
for i in range(NUM_VEHICLES):
    vehicles.append({
        "vehicle_id": f"VH{i+1:03d}",
        "home_lat": HYDERABAD_LAT + random.uniform(-0.3, 0.3),
        "home_lon": HYDERABAD_LON + random.uniform(-0.3, 0.3),
        "odometer_km": round(random.uniform(5000, 80000), 1),
        "fuel_pct": round(random.uniform(40, 100), 1),
    })

fault_vehicle_ids = random.sample([v["vehicle_id"] for v in vehicles], FAULT_VEHICLE_COUNT)

fault_windows = []
for vid in fault_vehicle_ids:
    fault_type = random.choice(["overheating", "battery_drain"])
    fault_day = random.randint(1, NUM_DAYS - 1)
    fault_start = START_TIME + timedelta(days=fault_day, hours=random.randint(0, 20))
    fault_duration_min = random.randint(20, 90)
    fault_end = fault_start + timedelta(minutes=fault_duration_min)
    fault_windows.append({
        "vehicle_id": vid,
        "fault_type": fault_type,
        "fault_start": fault_start.isoformat(),
        "fault_end": fault_end.isoformat(),
    })

print(f"Injected faults: {fault_windows}")
## Generate one reading for a vehicle at a given timestamp

Applies the fault window (if any) to push temp/battery/rpm out of normal range,
then separately applies random messiness on top.
def in_fault_window(vehicle_id, ts):
    for fw in fault_windows:
        if fw["vehicle_id"] == vehicle_id:
            start = datetime.fromisoformat(fw["fault_start"])
            end = datetime.fromisoformat(fw["fault_end"])
            if start <= ts <= end:
                return fw["fault_type"]
    return None

def make_reading(vehicle, ts, state):
    fault = in_fault_window(vehicle["vehicle_id"], ts)

    # baseline driving behavior — slow random walk
    speed = max(0, state["speed"] + random.uniform(-8, 8))
    speed = min(speed, 120)
    rpm = int(800 + speed * 35 + random.uniform(-100, 100))
    engine_temp = 85 + random.uniform(-3, 3)
    battery_v = 12.4 + random.uniform(-0.2, 0.2)
    lat = state["lat"] + random.uniform(-0.0008, 0.0008)
    lon = state["lon"] + random.uniform(-0.0008, 0.0008)
    odometer = state["odometer_km"] + speed * (INTERVAL_SECONDS / 3600)
    dtc_code = None

    if fault == "overheating":
        engine_temp = 115 + random.uniform(0, 15)
        dtc_code = "P0217"
    elif fault == "battery_drain":
        battery_v = 10.5 - random.uniform(0, 1.0)
        dtc_code = "P0562"

    state.update({"speed": speed, "lat": lat, "lon": lon, "odometer_km": odometer})

    reading = {
        "vehicle_id": vehicle["vehicle_id"],
        "event_ts": ts.isoformat(),
        "speed_kmph": round(speed, 1),
        "rpm": rpm,
        "engine_temp_c": round(engine_temp, 1),
        "battery_v": round(battery_v, 2),
        "fuel_pct": round(max(0, vehicle["fuel_pct"] - random.uniform(0, 0.002)), 2),
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "odometer_km": round(odometer, 1),
        "dtc_code": dtc_code,
    }
    return reading
## Apply deliberate messiness

Takes a clean reading and randomly corrupts it, so downstream layers have
real problems to detect and fix.
def mess_up(reading):
    r = dict(reading)

    if random.random() < NULL_RATE:
        field = random.choice(["speed_kmph", "engine_temp_c", "battery_v", "lat", "lon"])
        r[field] = None

    if random.random() < OUT_OF_RANGE_RATE:
        field = random.choice(["speed_kmph", "engine_temp_c", "battery_v", "rpm"])
        r[field] = r[field] * random.choice([10, -5]) if r[field] else 9999

    if random.random() < GPS_JUMP_RATE:
        r["lat"] = round(r["lat"] + random.uniform(-5, 5), 6) if r["lat"] else None
        r["lon"] = round(r["lon"] + random.uniform(-5, 5), 6) if r["lon"] else None

    if random.random() < LATE_ARRIVAL_RATE:
        ts = datetime.fromisoformat(r["event_ts"])
        r["event_ts"] = (ts - timedelta(minutes=random.randint(1, 30))).isoformat()

    return r
## Generate and write hourly batches

Each hour of simulated time becomes one JSON file, so Auto Loader in Bronze
can pick files up incrementally like a real streaming source.
states = {v["vehicle_id"]: {
    "speed": 0.0, "lat": v["home_lat"], "lon": v["home_lon"], "odometer_km": v["odometer_km"]
} for v in vehicles}

total_readings = 0
total_hours = NUM_DAYS * 24
readings_per_hour = int(3600 / INTERVAL_SECONDS)

for hour in range(total_hours):
    batch_start = START_TIME + timedelta(hours=hour)
    batch_readings = []

    for step in range(readings_per_hour):
        ts = batch_start + timedelta(seconds=step * INTERVAL_SECONDS)
        for vehicle in vehicles:
            reading = make_reading(vehicle, ts, states[vehicle["vehicle_id"]])
            reading = mess_up(reading)
            batch_readings.append(reading)
            # occasionally duplicate the send
            if random.random() < DUPLICATE_RATE:
                batch_readings.append(reading)

    file_name = f"telemetry_{batch_start.strftime('%Y%m%d_%H%M%S')}.json"
    file_path = f"{LANDING_PATH}/{file_name}"
    content = "\n".join(json.dumps(r) for r in batch_readings)
    dbutils.fs.put(file_path, content, overwrite=True)

    total_readings += len(batch_readings)

    if hour % 24 == 0:
        print(f"Day {hour // 24 + 1}/{NUM_DAYS} written ({total_readings} readings so far)")

print(f"Done. {total_readings} readings written across {total_hours} files to {LANDING_PATH}")
## Write ground-truth fault labels

This file is NOT used by Bronze/Silver — it's kept aside to later check whether
data quality rules and (optionally) anomaly detection actually catch these faults.
labels_path = f"/Volumes/{CATALOG}/{SCHEMA}/labels/fault_windows.json"
dbutils.fs.put(labels_path, json.dumps(fault_windows, indent=2), overwrite=True)
print(f"Fault labels written to {labels_path}")
display(spark.createDataFrame(fault_windows))
