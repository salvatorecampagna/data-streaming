import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

TOPIC = "com.udacity.sf.crime.calls.v1"
BOOTSTRAP_SERVERS = "localhost:9092"
RADIO_CODE_JSON_FILEPATH = "./data/radio_code.json"

call_schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", TimestampType(), True),
    StructField("call_date", TimestampType(), True),
    StructField("offense_date", TimestampType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True),
])

disposition_schema = StructType([
    StructField("disposition_code", StringType(), True),
    StructField("description", StringType(), True)
])

def run_spark_job(spark):

    spark.sparkContext.setLogLevel("INFO")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    df.printSchema()

    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col("value"), call_schema).alias("DF"))\
        .select("DF.*")

    distinct_table = service_table \
        .select(
            psf.col("call_date_time"),
            psf.col("original_crime_type_name"),
            psf.col("disposition")
        ) \
        .distinct()

    # count the number of original crime type
    agg_df = distinct_table \
        .withWatermark("call_date_time", "1 hour") \
        .groupBy(
            psf.window(psf.col("call_date_time"), "1 hour", "10 minutes"),
            psf.col("original_crime_type_name"),
            psf.col("disposition")
        ) \
        .agg({"original_crime_type_name": "count"}) \
        .orderBy("count(original_crime_type_name)", ascending=False)

    agg_query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .queryName("query 1 - crime type count over time") \
        .start()

    print(agg_query.lastProgress)
    print(agg_query.status)
    # agg_query.awaitTermination()

    radio_code_json_filepath = RADIO_CODE_JSON_FILEPATH
    radio_code_df = spark \
        .read \
        .option("multiline", "true") \
        .schema(disposition_schema) \
        .json(radio_code_json_filepath)

    # rename 'disposition_code' to 'disposition' to join data
    radio_code_df = radio_code_df \
        .withColumnRenamed("disposition_code", "disposition")

    join_df = agg_df \
        .join(
            other=radio_code_df,
            on="disposition",
            how="left"
        )

    join_query = join_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .queryName("query 2 - crime type count with disposition over time") \
        .start()

    print(join_query.lastProgress)
    print(join_query.status)

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
