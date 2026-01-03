"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/app/mod-2-pr-6-complex-transformation.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,avg,count,col,sum,max,min,round,desc
from pyspark.sql.functions import concat,lit,when,expr

spark = SparkSession.builder \
    .getOrCreate()

restaurants_df = spark.read.json("/opt/spark/storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("/opt/spark/storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("/opt/spark/storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")

restaurants_df.printSchema()
drivers_df.printSchema()
orders_df.printSchema()

# TODO 1. aggregations and grouping

restaurants_df.groupBy("cuisine_type") \
    .count() \
    .orderBy(desc("count")) #\
    #.show(5) ## Order by é uma operação mt custosa pelo shuffle

cuisine_stats = restaurants_df \
  .groupBy("cuisine_type") \
  .agg(
    count("*") \
    .alias("count"),
    round(avg("average_rating"),2) \
    .alias("rating"),
    max("average_rating") \
    .alias("highest"),
    min("average_rating") \
    .alias("lowest")
  ) \
  .orderBy(desc("rating")) # Preocupação com order by por shuffle

print(f"\n#######################################\n")
# cuisine_stats.show(5)

# TODO 2. filtering aggregated
print(f"\n#######################################\n")
cuisine_stats.filter(
    (col("count")>10) &
    (col("rating")>=4) &
    (col("lowest")>=3.5)
) \
  .orderBy(desc(col("rating"))) #\
  #.show(5)
# TODO 3. joining datasets

# restaurants_df.select("cnpj","name").show(3)
# orders_df.select("order_id","restaurant_key").show(3)

df_orders_merged = orders_df.join(
    restaurants_df,
    orders_df.restaurant_key==restaurants_df.cnpj,
    "inner"
) \
  .select(
      col("order_id"),
      col("name").alias("restaurant"),
      col("cuisine_type"),
      col("total_amount")
  )
df_orders_merged.show(3)

# TODO 4. advanced functions

spark.stop()
