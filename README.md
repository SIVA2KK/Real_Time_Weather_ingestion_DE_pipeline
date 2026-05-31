<div align="center">

# Real-Time Weather Streaming Pipeline

**End-to-end cloud data pipeline — live weather data for 8 global cities, every 60 seconds**

![AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3%20%7C%20SSM-FF9900?style=flat-square&logo=amazonaws&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-FF3621?style=flat-square&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID%20Tables-00ADD8?style=flat-square)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=flat-square&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python&logoColor=white)

</div>

---

## 📌 Project Overview

A production-grade, event-driven streaming pipeline that ingests live weather data from the [OpenWeatherMap API](https://openweathermap.org/api) for **8 global cities every 60 seconds**, processes it through a **Medallion Architecture** (Bronze → Silver → Gold), and surfaces insights through a **Databricks SQL Dashboard**.

Everything is provisioned as code via Terraform, orchestrated via Databricks Workflows, and secured with AWS SSM Parameter Store — no credentials in source code, ever.

> **Portfolio project demonstrating core Data Engineering competencies:**
> event-driven ingestion · IaC · data quality · feature engineering · anomaly detection · Delta Lake · DAG orchestration

---

## 🌍 Cities Monitored

| City | Region | Climate |
|------|--------|---------|
| Dubai | Middle East | Hot desert — extreme heat |
| Mumbai | South Asia | Tropical — monsoon season |
| Tokyo | East Asia | Humid subtropical |
| Singapore | SE Asia | Equatorial — year-round rain |
| Sydney | Oceania | Oceanic — southern hemisphere |
| London | Europe | Oceanic — frequent cloud cover |
| New York | Americas | Continental — four seasons |
| Chicago | Americas | Continental — lake effect wind |

---

## 🏗️ Architecture

```
OpenWeatherMap API
        │  every 60 seconds · 8 cities in parallel
        ▼
AWS Lambda (lambda_puller.py)
        │  flatten JSON → 26 fields · write one file per city
        ▼
Amazon S3 — Raw Landing Zone
        │  s3://[bucket]/raw/weather/date=YYYY-MM-DD/city=Name/[uuid].json
        ▼
Databricks — Bronze Layer (Delta)
        │  typed schema · partitioned · source_file traceability
        ▼
Databricks — Silver Layer (Delta)
        │  dedup · range validation · feature enrichment · DLQ
        ▼
Databricks — Gold Layer (Delta)
        │  weather_hourly · weather_anomalies · weather_snapshot
        ▼
Databricks Workflows (3-task DAG · hourly schedule)
        │
        ▼
Databricks SQL Dashboard (5 live widgets)
```

### Key Design Decisions

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Streaming transport | S3 file drop | Kinesis Data Streams | 8 records/min is tiny — Kinesis adds cost/complexity only justified at high throughput |
| Processing engine | Databricks + PySpark | AWS Glue | Unified platform: notebooks, jobs, Delta Lake, SQL in one place |
| Table format | Delta Lake | Plain Parquet | ACID transactions, schema enforcement, time travel, efficient upserts |
| Orchestration | Databricks Workflows | Apache Airflow (MWAA) | Zero additional setup, included in Databricks, no idle cost |
| Secrets | SSM Parameter Store | Environment variables | Encrypted at rest, auditable, rotatable without code changes |

---

## 📁 Project Structure

```
WEATHER_INGESTION_FILES/
│
├── Databricks/
│   │
│   ├── bronze/
│   │   ├── 01.config_and_extract.py        # S3 config, boto3 read, normalize(), createDataFrame
│   │   └── 02.verify.py                    # Count check, schema print, sample rows
│   │
│   ├── silver/
│   │   ├── 01.config_and_readbronze.py     # Load Bronze Delta, config paths
│   │   ├── 02.remove_dups.py               # Window dedup — city_id + minute partition
│   │   ├── 03.validate_physical_ranges.py  # Range checks → is_valid flag
│   │   ├── 04.enrich_the_derived.py        # heat_index, comfort_level, wind_dir_label, is_daytime, temp_category
│   │   ├── 05.split_valid_and_invalid.py   # Valid → Silver Delta | Invalid → DLQ Delta
│   │   └── 06.verify.py                    # Count, comfort breakdown, sample enriched rows
│   │
│   ├── gold/
│   │   ├── 01.config_and_readsilver.py     # Load Silver Delta, config Gold paths
│   │   ├── 02.hourlyagg.py                 # Hourly aggregations per city
│   │   ├── 03.anomaly_detection.py         # Rolling z-score, anomaly_type flags
│   │   ├── 04.latest_reading_per_city.py   # Snapshot — row_number() rank=1 per city
│   │   └── 05.summary_of_gold.py           # Print all 3 Gold table counts + samples
│   │
│   ├── register_gold_tables/
│   │   ├── 01.read_gold_delta_tables.py        # Read Gold Delta paths
│   │   ├── 02.schema_creation_managed_uc_tables.py  # CREATE TABLE in Unity Catalog
│   │   └── 03.verify.py                        # SHOW TABLES IN weather_gold
│   │
│   └── Weather Pipeline Dashboard.lvdash.json  # Databricks SQL Dashboard export
│
├── infrastructure/
│   └── terraform/
│       ├── iam.tf              # Least-privilege IAM role + policy
│       ├── lambda.tf           # Lambda function + EventBridge rule
│       ├── s3.tf               # S3 bucket + 30-day lifecycle on raw/
│       └── variables.tf        # account_id input variable
│
└── lambda/
    ├── lambda_puller.py        # Weather API fetcher → S3 writer
    └── requirements.txt        # requests, boto3

S3 Bucket (auto-created by Terraform)/
└── raw/weather/
    └── date=YYYY-MM-DD/
        └── city=CityName/
            └── [ingestion-uuid].json
```

---

## ⚙️ Tech Stack

| Layer | Technology | Version / Notes |
|-------|-----------|-----------------|
| Data Source | OpenWeatherMap API | v2.5 · free tier |
| Ingestion | AWS Lambda | Python 3.12 · 256 MB · 30s timeout |
| Scheduling | AWS EventBridge | rate(1 minute) |
| Storage | Amazon S3 | Versioning + lifecycle policies |
| Secrets | AWS SSM Parameter Store | SecureString encrypted |
| IaC | Terraform | AWS provider v6.47 |
| Processing | Databricks Free Edition | Serverless XXS compute |
| Distributed compute | Apache Spark (PySpark) | Databricks Runtime 14.3 LTS |
| Table format | Delta Lake | ACID · time travel · schema enforcement |
| Governance | Unity Catalog | Volumes for managed storage |
| Orchestration | Databricks Workflows | 3-task DAG · hourly |
| Dashboard | Databricks SQL | 5 live widgets |
| S3 SDK | boto3 | Python AWS SDK |
| Language | Python | 3.12 |
| CLI | PowerShell + AWS CLI | Windows development |

---

## 📦 Data Schema

Each weather record contains **26 fields**:

```python
{
    "ingestion_id":          "uuid-v4",          # deduplication key
    "city_id":               "2643743",
    "city_name":             "London",
    "country":               "GB",
    "latitude":              51.5074,
    "longitude":             -0.1278,
    "temperature_celsius":   18.22,
    "feels_like_celsius":    18.0,
    "temp_min":              16.1,
    "temp_max":              20.4,
    "humidity_pct":          73,
    "pressure_hpa":          1021,
    "visibility_meters":     10000,
    "cloud_cover_pct":       85,
    "wind_speed_mps":        1.54,
    "wind_direction_deg":    315,
    "wind_gust_mps":         2.1,
    "rain_1h_mm":            0.0,
    "snow_1h_mm":            0.0,
    "weather_main":          "Clouds",
    "weather_description":   "overcast clouds",
    "weather_icon":          "04d",
    "sunrise_utc":           "2026-05-29T04:52:00+00:00",
    "sunset_utc":            "2026-05-29T20:08:00+00:00",
    "event_timestamp":       "2026-05-29T06:03:57+00:00",
    "api_dt":                "2026-05-29T06:03:55+00:00"
}
```

---

## 🔄 Pipeline Stages

### Week 1 — Ingestion Layer

**`lambda_puller.py`**

- Fetches all 8 cities **in parallel** using `ThreadPoolExecutor(max_workers=8)`
- API key retrieved from **AWS SSM Parameter Store** (never hardcoded)
- Flattens the nested OpenWeatherMap JSON into 26 flat fields
- Writes one JSON file per city with Hive-style S3 key:
  `raw/weather/date=YYYY-MM-DD/city=CityName/[uuid].json`
- Triggered every 60 seconds by **AWS EventBridge**

**Terraform** provisions: S3 bucket, Lambda function, EventBridge rule, IAM role with least-privilege policy (PutObject to `raw/weather/*` only)

---

### Week 2 — Bronze Layer

**`bronze_weather.py`**

- Reads all raw JSON files from S3 using **boto3** (Free Edition has no `s3a://` Hadoop config support)
- `normalize()` function converts every field to the correct Python type before Spark sees the data — prevents `CANNOT_MERGE_TYPE` errors caused by JSON inference
- Explicit `StructType` schema passed to `createDataFrame` — no inference
- Adds `ingest_date`, `ingest_hour`, `source_file` columns
- Writes to **Delta Lake** partitioned by `ingest_date` and `city_name`

---

### Week 3 — Silver Layer

**`silver_weather.py`**

Three transformations applied in sequence:

**1. Deduplication**
Window function partitioned by `city_id` + minute-truncated timestamp, ordered by `event_timestamp DESC`. Keeps `row_number = 1`. Removes Lambda retry duplicates.

**2. Range Validation**

| Field | Rule |
|-------|------|
| temperature_celsius | −90 to 60°C |
| humidity_pct | 0 to 100% |
| pressure_hpa | 870 to 1084 hPa |
| wind_speed_mps | ≥ 0 |

Invalid records → **Dead Letter Queue** (DLQ) Delta path. Never silently dropped.

**3. Feature Enrichment** — 5 new columns:

| Column | Logic |
|--------|-------|
| `heat_index` | Steadman formula — applied when temp ≥ 27°C and humidity ≥ 40% |
| `wind_dir_label` | Converts 0–360° to 8-point compass label (N, NE, E... NW) |
| `is_daytime` | Boolean — event_timestamp between sunrise_utc and sunset_utc |
| `comfort_level` | Comfortable / Extreme Heat / Freezing / Very Humid / Moderate |
| `temp_category` | Freezing / Cold / Cool / Warm / Hot / Extreme |

---

### Week 4 — Gold Layer

**`gold_weather.py`** — Three purpose-built analytics tables:

**`weather_hourly`** — 1 row per city per hour
Aggregates: avg/min/max temperature, total rainfall, average humidity, wind speed, dominant comfort level, reading count (data completeness indicator)

**`weather_anomalies`** — 1 row per reading with flags
Rolling z-score over last 1,440 rows per city (~24 hours). Anomaly types:
- `Unusual Heat` — z-score > +2.5σ
- `Unusual Cold` — z-score < −2.5σ
- `High Wind Alert` — wind speed > 20 m/s
- `Heavy Rain Alert` — rain > 50 mm/hr

**`weather_snapshot`** — Always exactly 8 rows
Latest reading per city via `row_number()` window. Source for live dashboard.

---

### Week 5 — Orchestration & Dashboard

**Databricks Workflow** (`weather_pipeline`)

```
bronze_ingestion → silver_transform → gold_aggregation
```

- Scheduled: **every hour at :05** (5-minute buffer for Lambda files to land)
- Each task depends on the previous completing successfully
- Runs on Serverless compute — no cluster management

**`register_gold_tables.py`**

Registers Gold Delta tables in Unity Catalog so Databricks SQL can query them directly.

**SQL Dashboard** — 5 widgets:
- Current Temperature by City (Bar chart)
- Hourly Temperature Trend — all cities (Line chart)
- Comfort Level Distribution (Pie chart)
- Live City Conditions (Table)
- Total Rainfall by City (Bar chart)

---

## 📊 Results

From the first session:

| Metric | Value |
|--------|-------|
| Records collected | 320+ |
| Cities monitored | 8 |
| Duplicate records removed | 8 |
| Invalid records | 0 |
| Enriched columns added | 5 |
| Gold tables produced | 3 |
| Estimated monthly AWS cost | ~$0.20 |

**Live snapshot (last captured reading):**

| City | Temp | Condition | Comfort |
|------|------|-----------|---------|
| Dubai | 36.0°C | Clear Sky | Extreme Heat 🔥 |
| Mumbai | 33.0°C | Haze | Moderate |
| Tokyo | 30.4°C | Scattered Cloud | Moderate |
| Singapore | 28.2°C | Light Rain 🌧️ | Very Humid |
| Sydney | 19.6°C | Few Clouds | Moderate |
| London | 18.2°C | Overcast | Moderate |
| New York | 16.5°C | Clear Sky | Moderate 🌙 |
| Chicago | 11.8°C | Broken Clouds | Moderate 🌙 |

---

## 🚀 Getting Started

### Prerequisites

- AWS account with IAM user (programmatic access)
- [AWS CLI](https://aws.amazon.com/cli/) configured (`aws configure`)
- [Terraform](https://terraform.io/downloads) installed
- [Databricks Free Edition](https://community.cloud.databricks.com) account
- [OpenWeatherMap API key](https://openweathermap.org/api) (free tier)
- Python 3.12

### 1. Store API Key

```powershell
aws ssm put-parameter `
  --name "/weather-pipeline/openweather-api-key" `
  --value "YOUR_API_KEY_HERE" `
  --type SecureString `
  --region us-east-1
```

### 2. Package Lambda

```powershell
cd weather_ingestion
python -m pip install requests boto3 -t ./package
cp lambda_puller.py ./package/
cd package
Compress-Archive -Path * -DestinationPath ../lambda.zip -Force
cd ..
```

### 3. Deploy Infrastructure

```powershell
cd infrastructure/terraform
terraform init
terraform apply -var="account_id=YOUR_AWS_ACCOUNT_ID"
```

This creates: S3 bucket · Lambda function · EventBridge rule · IAM role

### 4. Verify Ingestion

After ~2 minutes, check S3 for files:

```powershell
aws s3 ls s3://weather-pipeline-lake-[account-id]/raw/weather/ --recursive
```

You should see 8 JSON files — one per city.

### 5. Run Databricks Notebooks

In your Databricks workspace, run in order:
1. `bronze_weather.py`
2. `silver_weather.py`
3. `gold_weather.py`
4. `register_gold_tables.py`

### 6. Create Workflow

```
Databricks → Jobs & Pipelines → Create job → weather_pipeline
Add tasks: bronze_ingestion → silver_transform → gold_aggregation
Schedule: Every hour at :05
```

---

## 🔐 Security

- **Zero credentials in source code** — API key stored in AWS SSM Parameter Store as `SecureString`
- **Least-privilege IAM** — Lambda execution role can only `s3:PutObject` on `raw/weather/*`; cannot read, list, or delete
- **S3 lifecycle policy** — Raw JSON files auto-expire after 30 days
- **No hardcoded account IDs** — passed as Terraform input variable

---

## ⚠️ Known Limitations

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| Databricks Free Edition | No `s3a://` Hadoop config | Used boto3 for S3 reads directly |
| DBFS root disabled | Can't use `/dbfs/` paths | Used Unity Catalog Volumes |
| Bronze overwrites on rerun | Delta log grows on reruns | Acceptable for learning; use MERGE in production |
| Anomaly baseline thin early | z-score needs 24h+ history | Improves automatically as pipeline accumulates data |
| OpenWeatherMap free tier | No UV index / air quality | Upgrade to paid plan for additional fields |

---

## 🔭 Future Enhancements

### Near-Term
- [ ] **SNS anomaly alerts** — publish to SNS topic when `anomaly_type IS NOT NULL`; deliver real-time email/SMS
- [ ] **Expand to 30+ cities** — only requires adding entries to `CITIES` list in Lambda
- [ ] **Data quality metrics table** — capture `valid_count`, `invalid_count`, `duplicate_count` per Silver run

### Mid-Term
- [ ] **Delta Live Tables (DLT)** — migrate Bronze→Silver→Gold to DLT pipeline with built-in quality expectations and visual DAG
- [ ] **ML temperature forecasting** — train Prophet or Spark MLlib model on `weather_hourly` table; serve 24-hr predictions
- [ ] **Geographic heatmap** — add world map widget using lat/lon fields from snapshot table
- [ ] **Historical backfill** — use OpenWeatherMap historical API to seed 90 days of past data for richer anomaly baselines

### Long-Term
- [ ] **Kinesis + Auto Loader** — replace S3 file-drop with Kinesis Data Streams for true sub-minute streaming at scale (justified once cities > 50)
- [ ] **Databricks on AWS** — mount S3 via IAM instance profile; enables Auto Loader streaming mode natively
- [ ] **Multi-environment Terraform** — dev/staging/prod workspaces with separate buckets and Databricks workspaces
- [ ] **Cost governance dashboard** — AWS Cost Explorer tags per pipeline component; daily cost tracking

---

## 🧠 Skills Demonstrated

| Skill | Tool / Technique |
|-------|-----------------|
| Event-Driven Ingestion | AWS Lambda + EventBridge |
| Infrastructure as Code | Terraform (AWS provider) |
| Secrets Management | AWS SSM Parameter Store |
| Medallion Architecture | Bronze / Silver / Gold layers |
| Data Quality + DLQ | Range validation + dead letter queue |
| Feature Engineering | PySpark `when/otherwise` chains |
| Window Functions | Dedup, ranking, rolling statistics |
| Anomaly Detection | Rolling z-score via PySpark Window |
| Delta Lake | ACID transactions, time travel, schema enforcement |
| DAG Orchestration | Databricks Workflows dependency graph |
| Data Governance | Unity Catalog managed volumes |
| Cost Optimisation | S3 lifecycle rules, serverless compute |

---

<div align="center">

**Built with curiosity. Shipped with precision.**

*Data Engineering Portfolio Project · 2026*

</div>
