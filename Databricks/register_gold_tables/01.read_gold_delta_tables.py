# Cell 1 — Read Gold Delta tables from Volumes
from pyspark.sql import functions as F

GOLD_SNAPSHOT  = "/Volumes/workspace/default/weather_data/gold/snapshot/"
GOLD_HOURLY    = "/Volumes/workspace/default/weather_data/gold/hourly/"
GOLD_ANOMALIES = "/Volumes/workspace/default/weather_data/gold/anomalies/"

snapshot  = spark.read.format("delta").load(GOLD_SNAPSHOT)
hourly    = spark.read.format("delta").load(GOLD_HOURLY)
anomalies = spark.read.format("delta").load(GOLD_ANOMALIES)

print(f"snapshot : {snapshot.count()} rows")
print(f"hourly   : {hourly.count()} rows")
print(f"anomalies: {anomalies.count()} rows")