
import logging
from pyspark.sql import SparkSession

# Logging Configuration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/tmp/michael/logs/full_load.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Spark Session

spark = SparkSession.builder.appName("FullLoad").getOrCreate()

jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"
properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Load Users

def users_file():
    logger.info("Starting USERS full load from PostgreSQL...")
    df = spark.read.jdbc(url=jdbc_url, table="michael.users", properties=properties)
    df.write.mode("overwrite").parquet("/tmp/michael/project/bronze/users")
    logger.info("Users written to Bronze layer.")

# Load Movies

def movies_file():
    logger.info("Starting MOVIES full load from PostgreSQL...")
    df = spark.read.jdbc(url=jdbc_url, table="michael.movies", properties=properties)

    raw_path = "250226hdfs/michael/project/bronze/movies"
    df.write.mode("overwrite").parquet(raw_path)
    logger.info("Movies written to RAW zone.")

    df_raw = spark.read.parquet(raw_path)
    df_raw.write.mode("overwrite").parquet("/tmp/michael/project/bronze/movies")
    logger.info("Movies written RAW to Bronze.")

# Create Empty parquet file
def watch_history_file():
    empty_schema.write.mode("overwrite").parquet("/tmp/michael/project/silver/watch_history")

# Execute

logger.info("=== FULL LOAD STARTED ===")
users_file()
movies_file()
logger.info("=== FULL LOAD COMPLETED ===")

