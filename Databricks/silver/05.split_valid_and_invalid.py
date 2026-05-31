# ── Split valid vs invalid ─────────────────────────────────
silver_df = enriched.filter( F.col("is_valid")).drop("is_valid")
dlq_df    = enriched.filter(~F.col("is_valid"))

# ── Write Silver ───────────────────────────────────────────
(
    silver_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("ingest_date", "city_name")
        .save(SILVER_PATH)
)
print(f"✅ Silver written: {silver_df.count()} records")

# ── Write DLQ (bad records) ────────────────────────────────
if dlq_df.count() > 0:
    dlq_df.write.format("delta").mode("overwrite").save(SILVER_DLQ_PATH)
    print(f"⚠️ DLQ written: {dlq_df.count()} bad records")
else:
    print("✅ No bad records — DLQ empty")