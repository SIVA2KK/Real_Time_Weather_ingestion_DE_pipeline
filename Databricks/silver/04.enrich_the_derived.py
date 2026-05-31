# ── Enrich with derived columns ────────────────────────────
enriched = (
    validated
    .withColumn("heat_index",
        F.when(
            (F.col("temperature_celsius") >= 27) &
            (F.col("humidity_pct") >= 40),
            F.col("temperature_celsius") +
            (0.33 * F.col("humidity_pct") / 100) - 4
        ).otherwise(F.col("temperature_celsius"))
    )

    .withColumn("wind_direction_label",
        F.when(F.col("wind_direction_deg").between(0,   45),  "N")
         .when(F.col("wind_direction_deg").between(45,  90),  "NE")
         .when(F.col("wind_direction_deg").between(90,  135), "E")
         .when(F.col("wind_direction_deg").between(135, 180), "SE")
         .when(F.col("wind_direction_deg").between(180, 225), "S")
         .when(F.col("wind_direction_deg").between(225, 270), "SW")
         .when(F.col("wind_direction_deg").between(270, 315), "W")
         .otherwise("NW")
    )

    .withColumn("is_daytime",
        F.col("event_timestamp").between(
            F.col("sunrise_utc"),
            F.col("sunset_utc")
        )
    )

    .withColumn("comfort_level",
        F.when(
            (F.col("temperature_celsius").between(18, 26)) &
            (F.col("humidity_pct").between(30, 60))        &
            (F.col("wind_speed_mps") < 5),
            "Comfortable"
        )
        .when(F.col("temperature_celsius") > 35, "Extreme Heat")
        .when(F.col("temperature_celsius") < 0,  "Freezing")
        .when(F.col("humidity_pct") > 80,         "Very Humid")
        .otherwise("Moderate")
    )

    .withColumn("temp_category",
        F.when(F.col("temperature_celsius") < 0,         "Freezing")
         .when(F.col("temperature_celsius").between(0,  10),  "Cold")
         .when(F.col("temperature_celsius").between(10, 20),  "Cool")
         .when(F.col("temperature_celsius").between(20, 30),  "Warm")
         .when(F.col("temperature_celsius").between(30, 40),  "Hot")
         .otherwise("Extreme")
    )
)