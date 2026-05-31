import boto3
import json
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ── Config ─────────────────────────────────────────────────
BUCKET      = "<YOUR_S3_BUCKET>"
RAW_PREFIX  = "raw/weather/"
BRONZE_PATH = "/Volumes/workspace/default/weather_data/bronze/"

AWS_ACCESS_KEY = "<AWS_ACCESS_KEY>"
AWS_SECRET_KEY = "<AWS_SECRET_KEY>"
REGION         = "us-east-1"

# ── Explicit schema passed to createDataFrame ──────────────
weather_schema = StructType([
    StructField("ingestion_id",        StringType(),  True),
    StructField("city_id",             StringType(),  True),
    StructField("city_name",           StringType(),  True),
    StructField("country",             StringType(),  True),
    StructField("latitude",            DoubleType(),  True),
    StructField("longitude",           DoubleType(),  True),
    StructField("temperature_celsius", DoubleType(),  True),
    StructField("feels_like_celsius",  DoubleType(),  True),
    StructField("temp_min",            DoubleType(),  True),
    StructField("temp_max",            DoubleType(),  True),
    StructField("humidity_pct",        IntegerType(), True),
    StructField("pressure_hpa",        IntegerType(), True),
    StructField("visibility_meters",   IntegerType(), True),
    StructField("cloud_cover_pct",     IntegerType(), True),
    StructField("wind_speed_mps",      DoubleType(),  True),
    StructField("wind_direction_deg",  IntegerType(), True),
    StructField("wind_gust_mps",       DoubleType(),  True),
    StructField("rain_1h_mm",          DoubleType(),  True),
    StructField("snow_1h_mm",          DoubleType(),  True),
    StructField("weather_main",        StringType(),  True),
    StructField("weather_description", StringType(),  True),
    StructField("weather_icon",        StringType(),  True),
    StructField("sunrise_utc",         StringType(),  True),
    StructField("sunset_utc",          StringType(),  True),
    StructField("event_timestamp",     StringType(),  True),
    StructField("api_dt",              StringType(),  True),
    StructField("source_file",         StringType(),  True),
])

# ── Force correct Python types before Spark sees records ───
def normalize(rec):
    return {
        "ingestion_id":        str(rec.get("ingestion_id", "") or ""),
        "city_id":             str(rec.get("city_id", "") or ""),
        "city_name":           str(rec.get("city_name", "") or ""),
        "country":             str(rec.get("country", "") or ""),
        "latitude":            float(rec.get("latitude") or 0.0),
        "longitude":           float(rec.get("longitude") or 0.0),
        "temperature_celsius": float(rec.get("temperature_celsius") or 0.0),
        "feels_like_celsius":  float(rec.get("feels_like_celsius") or 0.0),
        "temp_min":            float(rec.get("temp_min") or 0.0),
        "temp_max":            float(rec.get("temp_max") or 0.0),
        "humidity_pct":        int(rec.get("humidity_pct") or 0),
        "pressure_hpa":        int(rec.get("pressure_hpa") or 0),
        "visibility_meters":   int(rec.get("visibility_meters") or 0),
        "cloud_cover_pct":     int(rec.get("cloud_cover_pct") or 0),
        "wind_speed_mps":      float(rec.get("wind_speed_mps") or 0.0),
        "wind_direction_deg":  int(rec.get("wind_direction_deg") or 0),
        "wind_gust_mps":       float(rec.get("wind_gust_mps") or 0.0),
        "rain_1h_mm":          float(rec.get("rain_1h_mm") or 0.0),
        "snow_1h_mm":          float(rec.get("snow_1h_mm") or 0.0),
        "weather_main":        str(rec.get("weather_main", "") or ""),
        "weather_description": str(rec.get("weather_description", "") or ""),
        "weather_icon":        str(rec.get("weather_icon", "") or ""),
        "sunrise_utc":         str(rec.get("sunrise_utc", "") or ""),
        "sunset_utc":          str(rec.get("sunset_utc", "") or ""),
        "event_timestamp":     str(rec.get("event_timestamp", "") or ""),
        "api_dt":              str(rec.get("api_dt", "") or ""),
        "source_file":         str(rec.get("source_file", "") or ""),
    }

# ── Read all JSON files from S3 ────────────────────────────
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=REGION
)

def list_all_files(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])
    return keys

def read_json_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))

print("Reading files from S3...")
all_keys = list_all_files(BUCKET, RAW_PREFIX)
print(f"Found {len(all_keys)} files")

records = []
for key in all_keys:
    try:
        rec = read_json_from_s3(BUCKET, key)
        rec['source_file'] = key
        records.append(normalize(rec))   # ← normalize before Spark
    except Exception as e:
        print(f"⚠️ Skipped {key}: {e}")

print(f"Loaded {len(records)} records")

# ── Create DataFrame with explicit schema ──────────────────
df = spark.createDataFrame(records, schema=weather_schema)

# ── Add timestamp and partition columns ────────────────────
bronze_df = (
    df
    .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    .withColumn("api_dt",          F.to_timestamp("api_dt"))
    .withColumn("sunrise_utc",     F.to_timestamp("sunrise_utc"))
    .withColumn("sunset_utc",      F.to_timestamp("sunset_utc"))
    .withColumn("ingest_date",     F.to_date("event_timestamp"))
    .withColumn("ingest_hour",     F.hour("event_timestamp"))
)

# ── Write Delta ────────────────────────────────────────────
(
    bronze_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("ingest_date", "city_name")
        .save(BRONZE_PATH)
)

print("✅ Bronze Delta table written!")
print(f"Total records: {bronze_df.count()}")