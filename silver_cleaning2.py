import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Logging Configuration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/tmp/michael/logs/cleaning.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Spark Session

spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

BRONZE = "/tmp/michael/project/bronze"
SILVER = "/tmp/michael/project/silver"


# Clean Movies

def clean_movies():
    logger.info("Cleaning MOVIES Bronze → Silver...")
    df = spark.read.parquet(f"{BRONZE}/movies")
    df = df.replace("", None)

    df = (
        df.withColumn("release_year", col("release_year").cast(IntegerType()))
          .withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))
          .withColumn("imdb_rating", col("imdb_rating").cast(FloatType()))
          .withColumn("production_budget", col("production_budget").cast(FloatType()))
          .withColumn("box_office_revenue", col("box_office_revenue").cast(FloatType()))
          .withColumn("number_of_seasons", col("number_of_seasons").cast(IntegerType()))
          .withColumn("number_of_episodes", col("number_of_episodes").cast(IntegerType()))
          .withColumn("is_netflix_original", col("is_netflix_original").cast(BooleanType()))
          .withColumn("content_warning", col("content_warning").cast(BooleanType()))
          .withColumn("added_to_platform", to_date("added_to_platform"))
    )

    string_cols = ["title", "content_type", "genre_primary", "genre_secondary", "language", "country_of_origin", "rating"]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))

    df = df.dropDuplicates(["movie_id"])
    df = df.withColumn("ingestion_date", current_date()).withColumn("source_system", lit("bronze"))

    df.coalesce(1).write.mode("overwrite").parquet(f"{SILVER}/movies")
    logger.info("Movies written to Silver.")


# Clean Users

def clean_users():
    logger.info("Cleaning USERS Bronze → Silver...")
    df = spark.read.parquet(f"{BRONZE}/users")
    df = df.replace("", None)

    df = (
        df.withColumn("age", col("age").cast(IntegerType()))
          .withColumn("monthly_spend", col("monthly_spend").cast(FloatType()))
          .withColumn("is_active", col("is_active").cast(BooleanType()))
          .withColumn("subscription_start_date", to_date("subscription_start_date"))
          .withColumn("created_at", to_timestamp("created_at"))
    )

    df = df.withColumn("email", lower(trim(col("email"))))

    for c in ["country", "state_province", "city"]:
        df = df.withColumn(c, initcap(trim(col(c))))

    df = df.dropDuplicates(["user_id"])
    df = df.withColumn("ingestion_date", current_date()).withColumn("source_system", lit("bronze"))

    df.coalesce(1).write.mode("overwrite").parquet(f"{SILVER}/users")
    logger.info("Users written to Silver.")


# Clean Watch History

def clean_watch_history():
    logger.info("Cleaning WATCH_HISTORY Bronze → Silver...")

    bronze_batch_path = f"{BRONZE}/watch_history"
    bronze_stream_path = f"{BRONZE}/watch_history_stream"
    silver_path = f"{SILVER}/watch_history"

    expected_cols = [
        "session_id", "user_id", "movie_id", "watch_date", "device_type",
        "watch_duration_minutes", "progress_percentage", "action", "quality",
        "location_country", "is_download", "user_rating"
    ]

    def align_schema(df):
        return (
            df.withColumn("watch_date", col("watch_date").cast("string"))
              .withColumn("device_type", col("device_type").cast("string"))
              .withColumn("watch_duration_minutes", col("watch_duration_minutes").cast("int"))
              .withColumn("progress_percentage", col("progress_percentage").cast("float"))
              .withColumn("action", col("action").cast("string"))
              .withColumn("quality", col("quality").cast("string"))
              .withColumn("location_country", col("location_country").cast("string"))
              .withColumn("is_download", col("is_download").cast("boolean"))
              .withColumn("user_rating", col("user_rating").cast("int"))
              .select(expected_cols)
        )

    try:
        silver_existing = spark.read.parquet(silver_path)
        silver_exists = True
        logger.info("Existing Silver watch_history found.")
    except:
        silver_existing = None
        silver_exists = False
        logger.info("No existing Silver watch_history found.")

    bronze_dfs = []

    try:
        bronze_batch = spark.read.parquet(bronze_batch_path)
        bronze_dfs.append(align_schema(bronze_batch))
        logger.info("Loaded Bronze batch watch_history.")
    except:
        logger.info("No Bronze batch watch_history found.")

    try:
        bronze_stream = spark.read.parquet(bronze_stream_path)
        bronze_dfs.append(align_schema(bronze_stream))
        logger.info("Loaded Bronze stream watch_history.")
    except:
        logger.info("No Bronze stream watch_history found.")

    if not bronze_dfs:
        logger.info("No Bronze watch_history data found. Skipping.")
        return

    bronze_df = bronze_dfs[0]
    for extra in bronze_dfs[1:]:
        bronze_df = bronze_df.union(extra)

    if silver_exists:
        bronze_new = bronze_df.join(silver_existing.select("session_id"), on="session_id", how="left_anti")
    else:
        bronze_new = bronze_df

    if bronze_new.count() == 0:
        logger.info("No new watch_history rows to clean.")
        return

    df = bronze_new.replace("", None)

    df = (
        df.withColumn("watch_date", to_date("watch_date"))
          .withColumn("watch_duration_minutes", col("watch_duration_minutes").cast(IntegerType()))
          .withColumn("progress_percentage", col("progress_percentage").cast(FloatType()))
          .withColumn("user_rating", col("user_rating").cast(IntegerType()))
          .withColumn("is_download", col("is_download").cast(BooleanType()))
    )

    for c in ["device_type", "action", "quality", "location_country"]:
        df = df.withColumn(c, initcap(trim(col(c))))

    df = df.dropDuplicates(["session_id"])
    df = df.withColumn("ingestion_date", current_date()).withColumn("source_system", lit("bronze"))

    if silver_exists:
        df = silver_existing.union(df)

    df = df.dropDuplicates(["session_id"])
    df.write.mode("overwrite").partitionBy("watch_date").parquet(silver_path)
    logger.info("Silver watch_history updated.")


# Execute

logger.info("=== CLEANING STARTED ===")
clean_movies()
clean_users()
clean_watch_history()
logger.info("=== CLEANING COMPLETED ===")
