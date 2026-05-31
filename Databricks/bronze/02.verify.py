bronze = spark.read.format("delta").load("/Volumes/workspace/default/weather_data/bronze/")
print(f"Total records: {bronze.count()}")
bronze.groupBy("city_name").count().orderBy("city_name").show()