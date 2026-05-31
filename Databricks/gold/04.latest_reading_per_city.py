# ── Latest reading per city ────────────────────────────────
# Used for live dashboard — one row per city, most recent only

window_latest = (
    Window
    .partitionBy("city_id")
    .orderBy(F.desc("event_timestamp"))
)

snapshot = (
    silver
    .withColumn("rank", F.row_number().over(window_latest))
    .filter(F.col("rank") == 1)
    .drop("rank")
    .select(
        "city_name", "country",
        "latitude", "longitude",
        "temperature_celsius", "feels_like_celsius",
        "humidity_pct", "pressure_hpa",
        "wind_speed_mps", "wind_direction_label",
        "weather_main", "weather_description",
        "rain_1h_mm", "comfort_level",
        "temp_category", "heat_index",
        "is_daytime", "event_timestamp"
    )
)

snapshot.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_SNAPSHOT)

print(f"✅ Gold snapshot written: {snapshot.count()} cities")
snapshot.show(truncate=False)