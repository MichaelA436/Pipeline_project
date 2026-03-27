import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-transformation") \
        .master("local[*]") \
        .getOrCreate()

def test_user_engagement_transformation(spark):
    watch = spark.createDataFrame(
        [
            ("u1", 30, "2024-01-01"),
            ("u1", 60, "2024-01-02"),
            ("u2", 45, "2024-01-01")
        ],
        ["user_id", "watch_duration_minutes", "watch_date"]
    )

    watch.createOrReplaceTempView("watch_history")

    result = spark.sql("""
        SELECT
            user_id,
            COUNT(*) AS sessions,
            SUM(watch_duration_minutes) AS total_watch_minutes
        FROM watch_history
        GROUP BY user_id
    """)

    rows = {r["user_id"]: r["total_watch_minutes"] for r in result.collect()}

    assert rows["u1"] == 90
    assert rows["u2"] == 45
