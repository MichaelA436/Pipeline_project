from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("GoldLayer").config("spark.sql.catalogImplementation", "in-memory").getOrCreate())


# Paths
SILVER = "/tmp/michael/project/silver"

# Postgres connection
jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"
properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Load Silver
movies = spark.read.parquet(f"{SILVER}/movies")
users = spark.read.parquet(f"{SILVER}/users")
watch = spark.read.parquet(f"{SILVER}/watch_history")

movies.createOrReplaceTempView("movies")
users.createOrReplaceTempView("users")
watch.createOrReplaceTempView("watch_history")



# Insight 1. User Engagement

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

user_engagement.write.jdbc(jdbc_url, "michael.user_engagement", "overwrite", properties)



# Insight 2. Top Movies (by watch time)

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

top_movies.write.jdbc(jdbc_url, "michael.top_movies", "overwrite", properties)



# 3. Genre Popularity

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

genre_popularity.write.jdbc(jdbc_url, "michael.genre_popularity", "overwrite", properties)



# 4. Daily Active Users (DAU)

daily_active_users = spark.sql("""
    SELECT
        watch_date,
        COUNT(DISTINCT user_id) AS dau,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY watch_date
    ORDER BY watch_date
""")

daily_active_users.write.jdbc(jdbc_url, "michael.daily_active_users", "overwrite", properties)



# 5. Peak Viewing Hours

peak_hours = spark.sql("""
    SELECT
        watch_date,
        COUNT(*) AS sessions,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY watch_date
    ORDER BY watch_date
""")


peak_hours.write.jdbc(jdbc_url, "michael.peak_hours", "overwrite", properties)



# 6. COUNTRY-LEVEL VIEWING

country_insights = spark.sql("""
    SELECT
        location_country AS country,
        COUNT(*) AS sessions,
        SUM(watch_duration_minutes) AS total_watch_minutes
    FROM watch_history
    GROUP BY location_country
    ORDER BY total_watch_minutes DESC
""")

country_insights.write.jdbc(jdbc_url, "michael.country_insights", "overwrite", properties)



# 7. Ratings VS Popularity

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

ratings_vs_popularity.write.jdbc(jdbc_url, "michael.ratings_vs_popularity", "overwrite", properties)



# 8. Completion Rate

completion_rate = spark.sql("""
    SELECT
        movie_id,
        AVG(progress_percentage) AS avg_completion_rate
    FROM watch_history
    GROUP BY movie_id
""")

completion_rate.write.jdbc(jdbc_url, "michael.completion_rate", "overwrite", properties)


print("Gold layer tables successfully written to PostgreSQL.")
