"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/app/mod-2-pr-5-basic-transformation.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, upper, lower, round, sqrt

spark = SparkSession.builder \
    .getOrCreate()


# # TODO read file

file_path = "/opt/spark/storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl"
df_rest = spark.read.json(file_path)

# # df_rest.show(5)
# # print(f"\n###################\n")
# # df_rest.printSchema()
# # print(f"\n###################\n")
# # print(f"\nROWS COUNT: {df_rest.count()}\n")
# # print(f"\n###################\n")

# # TODO 1. selecting columns

df_basic_select_rest = df_rest \
    .select(
        col("cuisine_type"),
        col("num_reviews"),
        col("opening_time"),
        col("closing_time")
    )

# # df_basic_select_rest.show(5)
# # df_basic_select_rest.printSchema()

# # TODO 2. renaming columns

df_renamed_rest = df_rest \
    .withColumnRenamed("name","restaurant") \
    .withColumnRenamed("num_reviews","reviews") \
    .withColumnRenamed("cuisine_type","cuisine") \
    .withColumnRenamed("opening_time","open") \
    .withColumnRenamed("closing_time","close")

# df_renamed_rest.show(5)
# print(f"\n###################\n")
# df_renamed_rest.printSchema()


# # TODO 3. filtering rows

df_high_rated_rest = df_rest \
    .filter(
        col("num_reviews")> 1000
    )

# df_high_rated_rest \
#     .select("name","cuisine_type") \
#     .show(5)

df_italian_rest = df_rest \
    .filter(
        col("cuisine_type")=="Italian"
    )

# df_italian_rest \
#     .select("name","cuisine_type") \
#     .show(5)

# # TODO 4. using logical operators

df_good_italian_rest = df_rest \
    .filter(
        (col("cuisine_type")=="Italian") &
        (col("num_reviews")>500)
    )

# df_good_italian_rest \
#     .select("name","cuisine_type","num_reviews") \
#     .show(5)

# # TODO 5. transforming columns

df_uppercase_rest = df_rest \
    .select(
        upper(col("name"))\
            .alias("restaurant"),
        concat(
            col("city"),
            lit(", "),
            col("country")
        ) \
            .alias("location")
    )
# df_uppercase_rest \
#     .select("restaurant","location") \
#     .show(5)

# # TODO 6. adding new columns

df_categorized_rest = df_rest \
    .withColumn(
        "category",
        when(col("average_rating") >= 4.5, "Excelent")
        .when(col("average_rating") >= 4.0, "Very Good")
        .when(col("average_rating") >= 3.5, "Good")
        .when(col("average_rating")> 3.0, "Average")
        .otherwise("Poor")
    )

# df_categorized_rest \
#     .select("name","cuisine_type","average_rating","category") \
#     .show(5)

# # TODO 7. dropping columns

df_rest_drop = df_rest \
    .drop("city","country","name")

# df_rest.printSchema()
# df_rest_drop.printSchema()

spark.stop()