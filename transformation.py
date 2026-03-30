import logging
from pyspark.sql import SparkSession


# Logging Configuration

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/tmp/michael/logs/transformation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Spark Session (Hive Enabled but NO saveAsTable)

spark = (
    SparkSession.builder
    .appName("GoldLayer")
    .config("spark.sql.warehouse.dir", "/warehouse/tablespace/external/hive")
    .enableHiveSupport()
    .getOrCreate()
)

# Base path for all external Hive tables
BASE = "/warehouse/tablespace/external/hive/michael.db"

def overwrite_external(df, table_name):
    """Write DataFrame directly to Hive external table location."""
    path = f"{BASE}/{table_name}"
    logger.info(f"Overwriting external table path: {path}")
    df.write.mode("overwrite").parquet(path)


# Load Silver Layer

SILVER = "/tmp/michael/project/silver"

logger.info("Loading Silver tables...")
movies = spark.read.parquet(f"{SILVER}/movies")
users = spark.read.parquet(f"{SILVER}/users")
watch = spark.read.parquet(f"{SILVER}/watch_history")

movies.createOrReplaceTempView("movies")
users.createOrReplaceTempView("users")
watch.createOrReplaceTempView("watch_history")

logger.info("Running Gold layer transformations...")


# 1. User Engagement

logger.info("Processing user_engagement...")
user_engagement = spark.sql("""
    SELECT
        user_id,
        COUNT(*) AS sessions,
        SUM(watch_duration_minutes) AS total_watch_minutes,
        AVG(watch_duration_minutes) AS avg_session_minutes,
        MAX(watch_date) AS last_watch_date
    FROM watch_history
    GROUP BY user_id
""")
overwrite_external(user_engagement, "user_engagement")


# 2. Top Movies

logger.info("Processing top_movies...")
top_movies = spark.sql("""
    SELECT
        m.movie_id,
        m.title,
        SUM(w.watch_duration_minutes) AS total_watch_minutes,
        COUNT(*) AS total_views
    FROM watch_history w
    JOIN movies m ON w.movie_id = m.movie_id
    GROUP BY m.movie_id, m.title
    ORDER BY total_watch_minutes DESC
""")
overwrite_external(top_movies, "top_movies")


# 3. Genre Popularity

logger.info("Processing genre_popularity...")
genre_popularity = spark.sql("""
    SELECT
        m.genre_primary AS genre,
        COUNT(*) AS views,
        SUM(w.watch_duration_minutes) AS total_watch_minutes
    FROM watch_history w
    JOIN movies m ON w.movie_id = m.movie_id
    GROUP BY m.genre_primary
    ORDER BY total_watch_minutes DESC
""")
overwrite_external(genre_popularity, "genre_popularity")


# 4. Daily Active Users

logger.info("Processing daily_active_users...")
daily_active_users = spark.sql("""
    SELECT
        watch_date,
        COUNT(DISTINCT user_id) AS dau,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY watch_date
    ORDER BY watch_date
""")
overwrite_external(daily_active_users, "daily_active_users")


# 5. Peak Hours

logger.info("Processing peak_hours...")
peak_hours = spark.sql("""
    SELECT
        watch_date,
        COUNT(*) AS sessions,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY watch_date
    ORDER BY watch_date
""")
overwrite_external(peak_hours, "peak_hours")


# 6. Country Insights

logger.info("Processing country_insights...")
country_insights = spark.sql("""
    SELECT
        location_country AS country,
        COUNT(*) AS sessions,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY location_country
    ORDER BY total_watch_minutes DESC
""")
overwrite_external(country_insights, "country_insights")


# 7. Ratings vs Popularity

logger.info("Processing ratings_vs_popularity...")
ratings_vs_popularity = spark.sql("""
    SELECT
        m.movie_id,
        m.title,
        m.imdb_rating,
        COUNT(*) AS total_views,
        SUM(w.watch_duration_minutes) AS total_watch_minutes
    FROM movies m
    LEFT JOIN watch_history w ON m.movie_id = w.movie_id
    GROUP BY m.movie_id, m.title, m.imdb_rating
    ORDER BY total_views DESC
""")
overwrite_external(ratings_vs_popularity, "ratings_vs_popularity")


# 8. Completion Rate

logger.info("Processing completion_rate...")
completion_rate = spark.sql("""
    SELECT
        movie_id,
        AVG(progress_percentage) AS avg_completion_rate
    FROM watch_history
    GROUP BY movie_id
""")
overwrite_external(completion_rate, "completion_rate")

logger.info("=== GOLD LAYER COMPLETED SUCCESSFULLY ===")

