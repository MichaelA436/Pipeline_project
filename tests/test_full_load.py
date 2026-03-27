import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-full-load") \
        .master("local[*]") \
        .getOrCreate()

def test_full_load_users_schema(spark):
    df = spark.createDataFrame(
        [
            ("u1", "John", "john@example.com"),
            ("u2", "Jane", "jane@example.com")
        ],
        ["user_id", "name", "email"]
    )

    assert "user_id" in df.columns
    assert "email" in df.columns
    assert df.count() == 2
