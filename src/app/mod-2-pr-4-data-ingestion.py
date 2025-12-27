"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/app/mod-2-pr-4-data-ingestion.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

path_restaurants = "/opt/spark/storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl"

df_restaurants = spark.read \
    .option("multiline", "true") \
    .option("mode", "PERMISSIVE") \
    .json(path_restaurants)

print(f"\n#########\n")
df_restaurants.show(5)
print(f"\n#########\n")
df_restaurants.printSchema()
print(f"\n#########\n")

spark.stop()