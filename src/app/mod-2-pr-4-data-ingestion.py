"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/app/mod-2-pr-4-data-ingestion.py
"""

from pyspark.sql import SparkSession
import pyspark.sql.types as t

spark = SparkSession.builder \
    .getOrCreate()

path_restaurants = "/opt/spark/storage/mysql/restaurants/*.jsonl"

restaurant_schema = t.StructType([
    t.StructField("address", t.StringType(), True),
    t.StructField("average_rating", t.DoubleType(), True),
    t.StructField("city", t.StringType(), True),
    t.StructField("closing_time", t.StringType(), True),
    t.StructField("cnpj", t.StringType(), True),
    t.StructField("country", t.StringType(), True),
    t.StructField("cuisine_type", t.StringType(), True),
    t.StructField("dt_current_timestamp", t.StringType(), True),
    t.StructField("name", t.StringType(), True),
    t.StructField("num_reviews", t.IntegerType(), True),
    t.StructField("opening_time", t.StringType(), True),
    t.StructField("phone_number", t.StringType(), True),
    t.StructField("restaurant_id", t.IntegerType(), True),
    t.StructField("uuid", t.StringType(), True)
])

df_restaurants = spark.read \
    .schema(restaurant_schema) \
    .json(path_restaurants)

# qty_restaurant_rows = df_restaurants.count()

print(f"\n#########\n")
print(f"Quantidade de linhas: {df_restaurants.count()}")
print(f"\n#########\n")
df_restaurants.printSchema()
print(f"\n#########\n")

spark.stop()