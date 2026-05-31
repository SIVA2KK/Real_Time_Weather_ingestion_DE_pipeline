import boto3
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Config ─────────────────────────────────────────────────
BRONZE_PATH     = "/Volumes/workspace/default/weather_data/bronze/"
SILVER_PATH     = "/Volumes/workspace/default/weather_data/silver/"
SILVER_DLQ_PATH = "/Volumes/workspace/default/weather_data/silver_dlq/"

# ── Read Bronze ────────────────────────────────────────────
bronze = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze records: {bronze.count()}")