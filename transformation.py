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


# Spark Session (Hive Enabled)


spark = (
    SparkSession.builder
    .appName("GoldLayer")
    .config("spark.sql.warehouse.dir", "/warehouse/tablespace/external/hive")
    .enableHiveSupport()
    .getOrCreate()
)

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

logger.info("Writing user_engagement to Hive...")
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
user_engagement.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("michael.user_engagement")


# 2. Top Movies

logger.info("Writing top_movies to Hive...")
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
top_movies.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("michael.top_movies")


# 3. Genre Popularity

logger.info("Writing genre_popularity to Hive...")
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
genre_popularity.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("michael.genre_popularity")


# 4. Daily Active Users

logger.info("Writing daily_active_users to Hive...")
daily_active_users = spark.sql("""
    SELECT
        watch_date,
        COUNT(DISTINCT user_id) AS dau,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY watch_date
    ORDER BY watch_date
""")
daily_active_users.write \
    .mode("overwrite") \
    .partitionBy("watch_date") \
    .format("hive") \
    .saveAsTable("michael.daily_active_users")


# 5. Peak Hours

logger.info("Writing peak_hours to Hive...")
peak_hours = spark.sql("""
    SELECT
        watch_date,
        COUNT(*) AS sessions,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY watch_date
    ORDER BY watch_date
""")
peak_hours.write \
    .mode("overwrite") \
    .partitionBy("watch_date") \
    .format("hive") \
    .saveAsTable("michael.peak_hours")


# 6. Country Insights

logger.info("Writing country_insights to Hive...")
country_insights = spark.sql("""
    SELECT
        location_country AS country,
        COUNT(*) AS sessions,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY location_country
    ORDER BY total_watch_minutes DESC
""")
country_insights.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("michael.country_insights")


# 7. Ratings vs Popularity

logger.info("Writing ratings_vs_popularity to Hive...")
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
ratings_vs_popularity.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("michael.ratings_vs_popularity")

# 8. Completion Rate

logger.info("Writing completion_rate to Hive...")
completion_rate = spark.sql("""
    SELECT
        movie_id,
        AVG(progress_percentage) AS avg_completion_rate
    FROM watch_history
    GROUP BY movie_id
""")
completion_rate.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("michael.completion_rate")


logger.info("=== GOLD LAYER COMPLETED SUCCESSFULLY ===")

