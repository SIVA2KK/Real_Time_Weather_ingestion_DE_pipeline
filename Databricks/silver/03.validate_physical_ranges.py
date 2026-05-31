# ── Validate physical ranges ───────────────────────────────
validated = (
    deduped
    .withColumn("is_valid",
        (F.col("temperature_celsius").between(-90, 60)) &
        (F.col("humidity_pct").between(0, 100))         &
        (F.col("pressure_hpa").between(870, 1084))      &
        (F.col("wind_speed_mps") >= 0)                  &
        (F.col("city_name").isNotNull())                 &
        (F.col("event_timestamp").isNotNull())
    )
)

valid_count   = validated.filter( F.col("is_valid")).count()
invalid_count = validated.filter(~F.col("is_valid")).count()
print(f"Valid records:   {valid_count}")
print(f"Invalid records: {invalid_count}")