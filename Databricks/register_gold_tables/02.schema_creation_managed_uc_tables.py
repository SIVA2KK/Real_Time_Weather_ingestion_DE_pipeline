# Cell 2 — Create schema and write as managed UC tables (no LOCATION)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.weather_gold")

snapshot.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.weather_gold.snapshot")

hourly.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.weather_gold.hourly")

anomalies.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.weather_gold.anomalies")

print("✅ All 3 tables registered in Unity Catalog")