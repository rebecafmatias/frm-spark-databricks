"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 2g \
  --executor-cores 2 \
  --total-executor-cores 4 \
  /opt/spark/jobs/app/mod-2-pr-2.py
"""

from pyspark.sql import SparkSession

# TODO 1 = create spark session

spark = SparkSession.builder \
    .getOrCreate()
    # .appName("testingSpark") \
    # .config("spark.executer.memory","512m") \
    # .config("spark.driver.memory","1g") \
    # .getOrCreate()

# TODO 2 = create spark context

sc = spark.sparkContext

print(f"\n###########\n")
print(f"Spark Version: {spark.version}")
print(f"Application ID: {sc.applicationId}")
print(f"Available cores: {sc.defaultParallelism}")
print(f"\n###########\n")

for item in sorted(sc.getConf().getAll()):
    print(f"{item[0]}: {item[1]}")

print(f"\n###########\n")

# TODO 3 = the spark code here

# TODO 4 = stop the spark context

spark.stop()