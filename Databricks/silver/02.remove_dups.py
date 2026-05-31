# ── Remove duplicates ──────────────────────────────────────
# Lambda retries can cause same city+timestamp to arrive twice
# Keep one record per city per minute

window_dedup = (
    Window
    .partitionBy("city_id", F.date_trunc("minute", "event_timestamp"))
    .orderBy(F.desc("event_timestamp"))
)

deduped = (
    bronze
    .withColumn("row_num", F.row_number().over(window_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

removed = bronze.count() - deduped.count()
print(f"Removed {removed} duplicates")
print(f"After dedup: {deduped.count()} records")