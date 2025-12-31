"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/app/mod-2-pr-6-complex-transformation.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

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
    .orderBy(desc("count")) \
    .show(5) ## Order by é uma operação mt custosa pelo shuffle

# TODO 2. filtering aggregated

# TODO 3. joining datasets


# TODO 4. advanced functions

