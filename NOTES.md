# Project Notes

## Known finding: Week 3 duplicate-key check fails (documented, not a bug)

The `silver_duplicate_keys` data quality rule checks that each
(vehicle_id, event_ts) pair appears only once in the Silver table. It
currently fails: ~33,257 keys have more than one row, some with 3-4
colliding readings on the same timestamp.

**Root cause:** the generator's late-arrival simulation shifts a reading's
timestamp backward by a whole number of minutes (1-30). Since readings are
spaced exactly 10 seconds apart, a whole-minute shift always lands exactly
on another real tick's timestamp for the same vehicle. With shifts
happening independently across many ticks, multiple different readings can
coincidentally land on the same timestamp slot.

**Why Week 2's dedup didn't remove these:** Week 2 only removes *exact*
duplicates (identical values across all fields). These colliding rows have
different sensor values — they are genuinely different readings that
happen to share a timestamp, not re-sent copies of the same reading.

**Takeaway:** `(vehicle_id, event_ts)` is not a safe natural key for this
kind of telemetry data. A production system would need either a proper
unique event ID per reading, or a defined merge/conflict rule (e.g.
last-write-wins) for genuinely colliding timestamps.


## Update: same root cause resurfaced in Week 4 Gold tables

The Week 3 late-arrival timestamp shift also affects Gold aggregations:
shifting a timestamp backward can push it across the midnight boundary into
the day before the dataset's intended start. Confirmed with:

```sql
SELECT MIN(event_date), MAX(event_date), COUNT(DISTINCT event_date)
FROM main.vehicle_telemetry.gold_vehicle_daily_health;
```

Result: dates ranged from 2025-12-31 to 2026-01-07 — 8 distinct days.
instead of the intended 7. This produced 158 vehicle-day rows instead of
140, and a few extra small "trips" formed by these boundary-date readings.

This confirms the Week 3 finding is systemic (one generator design
choice), not an isolated glitch — it resurfaces wherever data gets grouped
by calendar date or ordered by timestamp.
