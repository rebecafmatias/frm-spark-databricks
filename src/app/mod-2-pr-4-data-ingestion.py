"""
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/spark/jobs/app/mod-2-pr-4-data-ingestion.py
"""

from pyspark.sql