# ── Anomaly detection via rolling z-score ─────────────────
# Flag readings that deviate > 2.5 std from city's own average

city_window = (
    Window
    .partitionBy("city_id")
    .orderBy(F.col("event_timestamp").cast("long"))
    .rowsBetween(-1440, 0)   # last 24hrs worth of rows
)

anomaly_df = (
    silver
    .withColumn("rolling_avg_temp",
        F.avg("temperature_celsius").over(city_window)
    )
    .withColumn("rolling_std_temp",
        F.stddev("temperature_celsius").over(city_window)
    )
    .withColumn("temp_z_score",
        F.when(
            F.col("rolling_std_temp") > 0,
            (F.col("temperature_celsius") - F.col("rolling_avg_temp")) /
            F.col("rolling_std_temp")
        ).otherwise(0.0)
    )
    .withColumn("anomaly_type",
        F.when(F.col("temp_z_score") >  2.5, "Unusual Heat")
         .when(F.col("temp_z_score") < -2.5, "Unusual Cold")
         .when(F.col("wind_speed_mps") > 20,  "High Wind Alert")
         .when(F.col("rain_1h_mm") > 50,       "Heavy Rain Alert")
         .otherwise(None)
    )
    .withColumn("is_anomaly",
        F.col("anomaly_type").isNotNull()
    )
)

# Save all records with anomaly flags
anomaly_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("city_name") \
    .save(GOLD_ANOMALY)

flagged = anomaly_df.filter(F.col("is_anomaly"))
print(f"✅ Gold anomalies written")
print(f"Total records with anomaly flags: {anomaly_df.count()}")
print(f"Flagged anomalies: {flagged.count()}")

if flagged.count() > 0:
    flagged.select(
        "city_name", "event_timestamp",
        "temperature_celsius", "temp_z_score", "anomaly_type"
    ).show(10, truncate=False)
else:
    print("No anomalies detected yet — need more data history")