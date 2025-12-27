"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/app/mod-2-pr-4-data-ingestion.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

path_restaurants = "./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl"

df_restaurants = spark.read.json(path_restaurants)

spark.stop()