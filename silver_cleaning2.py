from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

BRONZE = "/tmp/michael/project/bronze"
SILVER = "/tmp/michael/project/silver"



#  CLEAN MOVIES  (STATIC DATA, FULL OVERWRITE)

def clean_movies():
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

    string_cols = [
        "title", "content_type", "genre_primary", "genre_secondary",
        "language", "country_of_origin", "rating"
    ]

    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))

    df = df.dropDuplicates(["movie_id"])

    df = (
        df.withColumn("ingestion_date", current_date())
          .withColumn("source_system", lit("bronze"))
    )

    df = df.coalesce(1)
    df.write.mode("overwrite").parquet(f"{SILVER}/movies")

    print("Silver movies cleaned and overwritten.")




#  CLEAN USERS  (SLOW-CHANGING, FULL OVERWRITE)

def clean_users():
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

    df = (
        df.withColumn("ingestion_date", current_date())
          .withColumn("source_system", lit("bronze"))
    )

    df = df.coalesce(1)
    df.write.mode("overwrite").parquet(f"{SILVER}/users")

    print("Silver users cleaned and overwritten.")




#  CLEAN WATCH HISTORY  (FAST-CHANGING,  INCREMENTAL MERGE)

def clean_watch_history():
    bronze_batch_path = f"{BRONZE}/watch_history"
    bronze_stream_path = f"{BRONZE}/watch_history_stream"
    silver_path = f"{SILVER}/watch_history"

    # Expected schema (correct order)
    expected_cols = [
        "session_id",
        "user_id",
        "movie_id",
        "watch_date",
        "device_type",
        "watch_duration_minutes",
        "progress_percentage",
        "action",
        "quality",
        "location_country",
        "is_download",
        "user_rating"
    ]

    # Function to align schema for both batch + stream
    def align_schema(df):
        return (
            df
            .withColumn("watch_date", col("watch_date").cast("string"))
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

    # Load existing Silver
    try:
        silver_existing = spark.read.parquet(silver_path)
        silver_exists = True
    except:
        silver_existing = None
        silver_exists = False

    # Load Bronze batch + stream
    bronze_dfs = []

    try:
        bronze_batch = spark.read.parquet(bronze_batch_path)
        bronze_dfs.append(align_schema(bronze_batch))
    except:
        print("No Bronze batch watch_history found.")

    try:
        bronze_stream = spark.read.parquet(bronze_stream_path)
        bronze_dfs.append(align_schema(bronze_stream))
    except:
        print("No Bronze stream watch_history found.")

    if not bronze_dfs:
        print("No Bronze watch_history data found.")
        
    
    print("Loaded batch:", "OK" if 'bronze_batch' in locals() else "NO")
    print("Loaded stream:", "OK" if 'bronze_stream' in locals() else "NO")
    print("Bronze dfs count:", len(bronze_dfs))
    
    if len(bronze_dfs) > 0:
        print("Bronze total rows:", bronze_dfs[0].count())
    else:
        print('Bronze total rows: 0')

    print("Silver exists:", silver_exists)
    
    if not bronze_dfs:
        print('No Bronze watch_history data found.')
        return
        
    # Union all Bronze sources safely
    bronze_df = bronze_dfs[0]
    for extra in bronze_dfs[1:]:
        bronze_df = bronze_df.union(extra)

    # Keep only NEW rows
    if silver_exists:
        bronze_new = bronze_df.join(
            silver_existing.select("session_id"),
            on="session_id",
            how="left_anti"
        )
    else:
        bronze_new = bronze_df

    if bronze_new.count() == 0:
        print("No new watch_history rows to clean.")
        return

    # Clean new rows
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

    df = (
        df.withColumn("ingestion_date", current_date())
          .withColumn("source_system", lit("bronze"))
    )

    # Merge with existing Silver
    if silver_exists:
        df = silver_existing.union(df)

    df = df.dropDuplicates(["session_id"])
    df = df.coalesce(1)
    df.write.mode("overwrite").parquet(silver_path)

    print("Silver watch_history updated from batch + stream.")




clean_movies()
clean_users()
clean_watch_history()
