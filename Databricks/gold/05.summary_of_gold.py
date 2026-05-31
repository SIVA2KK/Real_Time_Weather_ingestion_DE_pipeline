# ── Summary of all Gold tables ─────────────────────────────
print("═" * 55)
print("GOLD LAYER SUMMARY")
print("═" * 55)

hourly_df   = spark.read.format("delta").load(GOLD_HOURLY)
anomaly_df2 = spark.read.format("delta").load(GOLD_ANOMALY)
snapshot_df = spark.read.format("delta").load(GOLD_SNAPSHOT)

print(f"Hourly aggregates : {hourly_df.count()} rows")
print(f"Anomaly records   : {anomaly_df2.filter(F.col('is_anomaly')).count()} flagged")
print(f"City snapshot     : {snapshot_df.count()} cities")

print("\n── Current conditions per city ──")
snapshot_df.select(
    "city_name",
    "temperature_celsius",
    "comfort_level",
    "weather_description",
    "is_daytime"
).orderBy("city_name").show(truncate=False)