# Vehicle Telemetry Lakehouse

A small bronze/silver/gold lakehouse on Databricks for simulated vehicle telemetry, with data quality checks. Personal learning project.

## Data
Fully synthetic. A seeded generator produces telemetry for ~20 vehicles (speed, rpm, engine temp, battery voltage, GPS, fault codes), with deliberate duplicates, nulls, out-of-range values and late events. Injected faults are recorded in a labels file for testing.
No employer or client data is used.

## Planned architecture
generator -> landing files -> Bronze (Auto Loader, append-only Delta)
-> Silver (typed, deduplicated, bad rows quarantined)
-> Gold (trip summaries, vehicle daily health)
Data quality rules run between layers and write results to a table.

## Roadmap
- [ ] Week 1: generator + bronze ingestion
- [ ] Week 2: silver + quarantine
- [ ] Week 3: data quality checks
- [ ] Week 4: gold tables + Databricks Job
- [ ] Week 5: tests, diagram, docs
- [ ] Optional: MLflow anomaly detection

## Status
In progress.
