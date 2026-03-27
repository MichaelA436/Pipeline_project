# Create sparksession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

spark = SparkSession.builder.appName("IncrementalLoad").getOrCreate()

# Connect to postgre database
jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"
properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Create variables for bronze path and offset file

BRONZE_WATCH = "/tmp/michael/project/bronze/watch_history"
OFFSET_FILE = "/home/ec2-user/250226batch/michael/watch_history_offset.txt"



BATCH_SIZE = 1000      

# Get previous offset stored in file

def get_last_offset():
    if not os.path.exists(OFFSET_FILE):
        return 0
    with open(OFFSET_FILE, "r") as f:
        return int(f.read().strip())

# Save new offset after increment in the file

def save_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))


def incremental_watch_history():
    """
    Incrementally loads watch history data from the source table into the Bronze layer.

    This function:
    - Retrieves the last processed offset
    - Reads the next batch of records using LIMIT and OFFSET
    - Writes data to the Bronze layer (overwrite for first load, append thereafter)
    - Updates and saves the new offset for future runs

    Stops execution when no more new rows are available.
    """

    last_offset = get_last_offset()
    
    query = f"(SELECT * FROM michael.watch_history_initial_load ORDER BY CAST(watch_date AS DATE) LIMIT {BATCH_SIZE} OFFSET {last_offset}) AS t"
    
    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=properties
    )

    if df.count() == 0:
        print("No more new rows to load.")
        return

    if last_offset == 0:
        df.write.mode("overwrite").parquet(BRONZE_WATCH)
    else:
        df.write.mode("append").parquet(BRONZE_WATCH)

 
    new_offset = last_offset + BATCH_SIZE
    save_offset(new_offset)

    print(f"Loaded rows {last_offset} to {new_offset} into Bronze.")


incremental_watch_history()
