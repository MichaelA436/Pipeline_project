from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FullLoad").getOrCreate()

jdbc_url = "jdbc:postgresql://13.42.152.118:5432/testdb"
properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}


def users_file():
    # DIRECT Bronze
    df = spark.read.jdbc(url=jdbc_url, table="michael.users", properties=properties)
    df.write.mode("overwrite").parquet("/tmp/michael/project/bronze/users")
    

def movies_file():
    # RAW  (Bronze)
    df = spark.read.jdbc(url=jdbc_url, table="michael.movies", properties=properties)

    # 1. Write to RAW zone (simulated HDFS)
    raw_path = "250226hdfs/michael/project/bronze/movies"
    df.write.mode("overwrite").parquet(raw_path)

    # 2. Read RAW back in
    df_raw = spark.read.parquet(raw_path)

    # 3. Write to Bronze
    df_raw.write.mode("overwrite").parquet("/tmp/michael/project/bronze/movies")

    print("Movies loaded RAW ? Bronze.")


users_file()
movies_file()
