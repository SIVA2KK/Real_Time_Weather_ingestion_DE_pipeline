# Cell 3 — Verify
spark.sql("SHOW TABLES IN workspace.weather_gold").show()

spark.sql("""
    SELECT city_name, temperature_celsius, comfort_level, weather_description
    FROM workspace.weather_gold.snapshot
    ORDER BY temperature_celsius DESC
""").show(truncate=False)