# Example of Creating Data Frames in PySpark
# based on MongoDB Back-end Collections

from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import pyspark.sql.functions as F

load_dotenv()

# Create the SparkSession with the
# mongo-spark connector enabled on classpath:
spark = (
    SparkSession.builder.appName("Expectation-Failure-Reporting")
    .config(
        "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
    )
    .getOrCreate()
)

# Read from Mongodb db/collection into DataFrame
df = (
    spark.read.format("mongodb")
    .option("spark.mongodb.read.database", os.getenv("VERIFICATION_MONGO_DB_NAME"))
    .option("spark.mongodb.read.collection", "pipeline")
    .option(
        "spark.mongodb.read.connection.uri",
        os.getenv("VERIFICATION_MONGO_CONNECTION_STR"),
    )
    .load()
)

print("Inferred Schema:")
df.printSchema()

# Filter to just Failed records:
df = df.filter(F.col("success") == False)

# Report on Failures today:
df_today_failures = (
    df.withColumn(
        "truncated_timestamp", F.substring(F.col("meta.validation_time"), 1, 8)
    )
    .withColumn("date", F.to_date(F.col("truncated_timestamp"), "yyyyMMdd"))
    .filter(F.col("date") == F.current_date())
    .orderBy(F.col("meta.validation_time").desc())
    .select(
        [
            "pipeline_name",
            "meta.validation_time",
            F.explode("results.expectation_config.expectation_type").alias(
                "expectation_type"
            ),
        ]
    )
)

df_today_failures.show(10, False)

# Report on Column failures:
df_col_failures = (
    df.select([F.explode("results.expectation_config.kwargs.column").alias("column")])
    .groupBy(F.col("column"))
    .agg(F.count(F.lit(1)).alias("failure count"))
)

df_col_failures.show(10, False)
