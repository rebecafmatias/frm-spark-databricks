"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/app/mod-2-pr-6-complex-transformation.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

restaurants_path = "/opt/spark/storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl"
drivers_path = "/opt/spark/storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl"
orders_path = "/opt/spark/storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl"

# TODO 1. aggregations and grouping


# TODO 2. filtering aggregated

# TODO 3. joining datasets


# TODO 4. advanced functions

