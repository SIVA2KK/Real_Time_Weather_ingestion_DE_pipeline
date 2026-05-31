# ── Hourly aggregates per city ─────────────────────────────
hourly = (
    silver
    .groupBy(
        "city_id",
        "city_name",
        "country",
        F.date_trunc("hour", "event_timestamp").alias("hour_window")
    )
    .agg(
        F.round(F.avg("temperature_celsius"), 2).alias("avg_temp"),
        F.round(F.max("temperature_celsius"), 2).alias("max_temp"),
        F.round(F.min("temperature_celsius"), 2).alias("min_temp"),
        F.round(F.avg("feels_like_celsius"),  2).alias("avg_feels_like"),
        F.round(F.avg("humidity_pct"),        2).alias("avg_humidity"),
        F.round(F.avg("wind_speed_mps"),      2).alias("avg_wind_speed"),
        F.round(F.max("wind_gust_mps"),       2).alias("max_gust"),
        F.round(F.avg("pressure_hpa"),        2).alias("avg_pressure"),
        F.round(F.sum("rain_1h_mm"),          2).alias("total_rain_mm"),
        F.round(F.sum("snow_1h_mm"),          2).alias("total_snow_mm"),
        F.round(F.avg("cloud_cover_pct"),     2).alias("avg_cloud_cover"),
        F.round(F.avg("heat_index"),          2).alias("avg_heat_index"),
        F.first("comfort_level").alias("dominant_comfort"),
        F.first("temp_category").alias("dominant_temp_category"),
        F.count("*").alias("reading_count")
    )
    .orderBy("city_name", "hour_window")
)

hourly.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("city_name") \
    .save(GOLD_HOURLY)

print(f"✅ Gold hourly written: {hourly.count()} rows")
hourly.show(10, truncate=False)