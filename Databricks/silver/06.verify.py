# ── Verify ─────────────────────────────────────────────────
silver = spark.read.format("delta").load(SILVER_PATH)

print(f"Total silver records: {silver.count()}")
print("\n── Records per city ──")
silver.groupBy("city_name").count().orderBy("city_name").show()

print("\n── Comfort levels ──")
silver.groupBy("comfort_level").count().orderBy("comfort_level").show()

print("\n── Temp categories ──")
silver.groupBy("temp_category").count().orderBy("temp_category").show()

print("\n── Sample enriched record ──")
silver.select(
    "city_name", "temperature_celsius", "humidity_pct",
    "heat_index", "comfort_level", "temp_category",
    "wind_direction_label", "is_daytime"
).show(5, truncate=False)