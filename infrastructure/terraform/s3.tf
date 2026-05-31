resource "aws_s3_bucket" "weather_lake" {
  bucket = "weather-pipeline-lake-${var.account_id}"
  tags   = { Project = "weather-pipeline" }
}

resource "aws_s3_bucket_versioning" "weather_lake" {
  bucket = aws_s3_bucket.weather_lake.id
  versioning_configuration { status = "Enabled" }
}

# Lifecycle: expire raw files after 30 days (Delta layers keep the data)
resource "aws_s3_bucket_lifecycle_configuration" "raw_expiry" {
  bucket = aws_s3_bucket.weather_lake.id

  rule {
    id     = "expire-raw-json"
    status = "Enabled"
    filter { prefix = "raw/weather/" }
    expiration { days = 30 }
  }
}