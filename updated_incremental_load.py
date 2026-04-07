import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Logging Configuration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/tmp/michael/logs/incremental.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Spark Session

spark = SparkSession.builder.appName("IncrementalLoad").getOrCreate()

jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"
properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

BRONZE_WATCH = "/tmp/michael/project/bronze/watch_history"
OFFSET_FILE = "/home/ec2-user/250226batch/michael/watch_history_offset.txt"
BATCH_SIZE = 10000


# Offset Helpers

def get_last_offset():
    if not os.path.exists(OFFSET_FILE):
        logger.info("Offset file not found. Starting from offset 0.")
        return 0
    with open(OFFSET_FILE, "r") as f:
        offset = int(f.read().strip())
        logger.info(f"Loaded last offset: {offset}")
        return offset

def save_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))
    logger.info(f"Saved new offset: {offset}")


# Incremental Loader

def incremental_watch_history():
    logger.info("=== INCREMENTAL LOAD STARTED ===")

    last_offset = get_last_offset()

    query = f"(SELECT * FROM michael.watch_history_initial_load ORDER BY CAST(watch_date AS DATE) LIMIT {BATCH_SIZE} OFFSET {last_offset}) AS t"
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)

    if df.count() == 0:
        logger.info("No new rows found. Incremental load complete.")
        return
        
    if last_offset == 0:
      df.write.mode("overwrite").partitionBy("watch_date").parquet(BRONZE_WATCH)
      logger.info("Initial batch written to Bronze (partitioned by watch_date).")
    else:
      df.write.mode("append").partitionBy("watch_date").parquet(BRONZE_WATCH)
      logger.info("Incremental batch appended to Bronze (partitioned by watch_date).")

    new_offset = last_offset + BATCH_SIZE
    save_offset(new_offset)

    logger.info(f"Loaded rows {last_offset} to {new_offset} into Bronze.")
    logger.info("=== INCREMENTAL LOAD COMPLETED ===")

incremental_watch_history()
