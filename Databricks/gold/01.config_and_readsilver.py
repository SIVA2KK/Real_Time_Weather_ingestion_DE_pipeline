from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Config ─────────────────────────────────────────────────
SILVER_PATH   = "/Volumes/workspace/default/weather_data/silver/"
GOLD_HOURLY   = "/Volumes/workspace/default/weather_data/gold/hourly/"
GOLD_ANOMALY  = "/Volumes/workspace/default/weather_data/gold/anomalies/"
GOLD_SNAPSHOT = "/Volumes/workspace/default/weather_data/gold/snapshot/"

# ── Read Silver ────────────────────────────────────────────
silver = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver records loaded: {silver.count()}")