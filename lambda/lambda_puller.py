import json
import boto3
import requests
import uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import os

s3  = boto3.client('s3', region_name='us-east-1')
ssm = boto3.client('ssm')

def get_api_key():
    resp = ssm.get_parameter(
        Name='/weather-pipeline/openweather-api-key',
        WithDecryption=True
    )
    return resp['Parameter']['Value']

CITIES = [
    {"name": "New_York",   "id": "5128581", "lat": 40.71,  "lon": -74.00},
    {"name": "London",     "id": "2643743", "lat": 51.51,  "lon": -0.13 },
    {"name": "Tokyo",      "id": "1850147", "lat": 35.68,  "lon": 139.69},
    {"name": "Dubai",      "id": "292223",  "lat": 25.20,  "lon": 55.27 },
    {"name": "Sydney",     "id": "2147714", "lat": -33.86, "lon": 151.20},
    {"name": "Mumbai",     "id": "1275339", "lat": 19.07,  "lon": 72.87 },
    {"name": "Chicago",    "id": "4887398", "lat": 41.85,  "lon": -87.65},
    {"name": "Singapore",  "id": "1880252", "lat": 1.28,   "lon": 103.85},
]

def fetch_weather(city: dict, api_key: str) -> dict:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"lat": city["lat"], "lon": city["lon"], "appid": api_key, "units": "metric"}

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    d = resp.json()

    return {
        "ingestion_id":          str(uuid.uuid4()),
        "city_id":               city["id"],
        "city_name":             city["name"],
        "country":               d["sys"]["country"],
        "latitude":              d["coord"]["lat"],
        "longitude":             d["coord"]["lon"],
        "temperature_celsius":   d["main"]["temp"],
        "feels_like_celsius":    d["main"]["feels_like"],
        "temp_min":              d["main"]["temp_min"],
        "temp_max":              d["main"]["temp_max"],
        "humidity_pct":          d["main"]["humidity"],
        "pressure_hpa":          d["main"]["pressure"],
        "visibility_meters":     d.get("visibility", 0),
        "cloud_cover_pct":       d["clouds"]["all"],
        "wind_speed_mps":        d["wind"]["speed"],
        "wind_direction_deg":    d["wind"].get("deg", 0),
        "wind_gust_mps":         d["wind"].get("gust", 0.0),
        "rain_1h_mm":            d.get("rain", {}).get("1h", 0.0),
        "snow_1h_mm":            d.get("snow", {}).get("1h", 0.0),
        "weather_main":          d["weather"][0]["main"],
        "weather_description":   d["weather"][0]["description"],
        "weather_icon":          d["weather"][0]["icon"],
        "sunrise_utc":           datetime.fromtimestamp(d["sys"]["sunrise"], tz=timezone.utc).isoformat(),
        "sunset_utc":            datetime.fromtimestamp(d["sys"]["sunset"],  tz=timezone.utc).isoformat(),
        "event_timestamp":       datetime.now(timezone.utc).isoformat(),
        "api_dt":                datetime.fromtimestamp(d["dt"], tz=timezone.utc).isoformat(),
    }


def write_to_s3(record: dict, bucket: str):
    """
    S3 key structure Auto Loader will use in Week 2:
      raw/weather/date=YYYY-MM-DD/city=<CityName>/<ingestion_id>.json
    One file per city per invocation — small, cheap, easy to reprocess.
    """
    now   = datetime.now(timezone.utc)
    key   = (
        f"raw/weather/"
        f"date={now.strftime('%Y-%m-%d')}/"
        f"city={record['city_name']}/"
        f"{record['ingestion_id']}.json"
    )
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(record, ensure_ascii=False),
        ContentType="application/json",
    )
    print(f"✅ s3://{bucket}/{key}")


def lambda_handler(event, context):
    api_key = get_api_key()
    bucket  = os.environ['S3_BUCKET']

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch_weather, city, api_key) for city in CITIES]
        records = [f.result() for f in futures]

    for record in records:
        write_to_s3(record, bucket)

    return {
        "statusCode":     200,
        "records_written": len(records),
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }