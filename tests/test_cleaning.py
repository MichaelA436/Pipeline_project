import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import IntegerType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-cleaning") \
        .master("local[*]") \
        .getOrCreate()

def test_clean_users_casting(spark):
    df = spark.createDataFrame(
        [
            ("u1", "John", "john@example.com", "25"),
            ("u2", "Jane", "jane@example.com", "30")
        ],
        ["user_id", "name", "email", "age"]
    )

    cleaned = df.withColumn("age", col("age").cast(IntegerType()))

    assert cleaned.schema["age"].dataType == IntegerType()

def test_clean_users_trim(spark):
    df = spark.createDataFrame(
        [("u1", " John ", " john@example.com ")],
        ["user_id", "name", "email"]
    )

    cleaned = df.withColumn("name", trim(col("name")))

    assert cleaned.collect()[0]["name"] == "John"
