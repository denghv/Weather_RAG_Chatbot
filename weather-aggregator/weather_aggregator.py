#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Weather Data Aggregator using Spark Structured Streaming

This application reads weather data from Kafka, performs windowed aggregation
to calculate daily max/min values for each weather metric, and stores results in MinIO.
Uses outputMode = "update" to continuously update daily aggregates.
"""

import os
import json
import time
import logging
from datetime import datetime
import pytz

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, max as spark_max, min as spark_min, avg, count,
    current_timestamp, date_format, when, isnan, isnull
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
    MapType, IntegerType, LongType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("WeatherAggregator")

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather-data")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "weather-aggregates")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "/tmp/checkpoints/weather-aggregator")

def define_weather_schema():
    """Define the schema for weather data from Kafka"""
    return StructType([
        StructField("location", StructType([
            StructField("name", StringType(), True),
            StructField("region", StringType(), True),
            StructField("country", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("tz_id", StringType(), True),
            StructField("localtime_epoch", LongType(), True),
            StructField("localtime", StringType(), True)
        ]), True),
        StructField("current", StructType([
            StructField("last_updated_epoch", LongType(), True),
            StructField("last_updated", StringType(), True),
            StructField("temp_c", DoubleType(), True),
            StructField("temp_f", DoubleType(), True),
            StructField("is_day", IntegerType(), True),
            StructField("condition", StructType([
                StructField("text", StringType(), True),
                StructField("icon", StringType(), True),
                StructField("code", IntegerType(), True)
            ]), True),
            StructField("wind_mph", DoubleType(), True),
            StructField("wind_kph", DoubleType(), True),
            StructField("wind_degree", IntegerType(), True),
            StructField("wind_dir", StringType(), True),
            StructField("pressure_mb", DoubleType(), True),
            StructField("pressure_in", DoubleType(), True),
            StructField("precip_mm", DoubleType(), True),
            StructField("precip_in", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("cloud", IntegerType(), True),
            StructField("feelslike_c", DoubleType(), True),
            StructField("feelslike_f", DoubleType(), True),
            StructField("vis_km", DoubleType(), True),
            StructField("vis_miles", DoubleType(), True),
            StructField("uv", DoubleType(), True),
            StructField("gust_mph", DoubleType(), True),
            StructField("gust_kph", DoubleType(), True),
            StructField("air_quality", StructType([
                StructField("co", DoubleType(), True),
                StructField("no2", DoubleType(), True),
                StructField("o3", DoubleType(), True),
                StructField("so2", DoubleType(), True),
                StructField("pm2_5", DoubleType(), True),
                StructField("pm10", DoubleType(), True),
                StructField("us-epa-index", IntegerType(), True),
                StructField("gb-defra-index", IntegerType(), True)
            ]), True)
        ]), True)
    ])

def create_spark_session():
    """Create and configure Spark session with MinIO support"""
    logger.info("Creating Spark session with Kafka and MinIO integration")

    return (SparkSession.builder
            .appName("WeatherAggregator")
            .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.streaming.stateStore.providerClass",
                   "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.jars.packages",
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "1g")
            .getOrCreate())

def process_weather_data():
    """Process weather data from Kafka and calculate daily max/min aggregations"""
    logger.info("Starting Weather Data Aggregator")

    try:
        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        # Define schema
        schema = define_weather_schema()
        logger.info("Schema defined successfully")

        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")

        # Read data from Kafka
        kafka_df = (spark
                    .readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                    .option("subscribe", KAFKA_TOPIC)
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    .load())

        logger.info("Connected to Kafka, parsing data...")

        # Parse JSON data from Kafka
        parsed_df = (kafka_df
                     .select(
                         col("key").cast("string").alias("province_key"),
                         from_json(col("value").cast("string"), schema).alias("data"),
                         col("timestamp").alias("kafka_timestamp")
                     ))

        # Extract required fields
        weather_data = (parsed_df
                        .select(
                            col("province_key"),
                            col("kafka_timestamp"),
                            col("data.location.name").alias("location_name"),
                            col("data.location.region").alias("region"),
                            col("data.location.country").alias("country"),
                            col("data.current.temp_c").alias("temp_c"),
                            col("data.current.temp_f").alias("temp_f"),
                            col("data.current.humidity").alias("humidity"),
                            col("data.current.pressure_mb").alias("pressure_mb"),
                            col("data.current.wind_kph").alias("wind_kph"),
                            col("data.current.precip_mm").alias("precip_mm"),
                            col("data.current.cloud").alias("cloud"),
                            col("data.current.feelslike_c").alias("feelslike_c"),
                            col("data.current.vis_km").alias("vis_km"),
                            col("data.current.uv").alias("uv"),
                            col("data.current.air_quality.pm2_5").alias("pm2_5"),
                            col("data.current.air_quality.pm10").alias("pm10"),
                            col("data.current.air_quality.co").alias("co"),
                            col("data.current.air_quality.no2").alias("no2"),
                            col("data.current.air_quality.o3").alias("o3"),
                            col("data.current.air_quality.so2").alias("so2")
                        ))

        logger.info("Performing daily windowed aggregation")

        # Calculate daily max/min aggregations with 1-day window
        daily_aggregates = (weather_data
                            .withWatermark("kafka_timestamp", "1 hour")
                            .groupBy(
                                window(col("kafka_timestamp"), "1 day"),
                                col("location_name"),
                                col("region"),
                                col("country")
                            )
                            .agg(
                                # Temperature metrics
                                spark_max("temp_c").alias("max_temp_c"),
                                spark_min("temp_c").alias("min_temp_c"),

                                # Humidity metrics
                                spark_max("humidity").alias("max_humidity"),
                                spark_min("humidity").alias("min_humidity"),

                                # Pressure metrics
                                spark_max("pressure_mb").alias("max_pressure_mb"),
                                spark_min("pressure_mb").alias("min_pressure_mb"),

                                # Wind metrics
                                spark_max("wind_kph").alias("max_wind_kph"),
                                spark_min("wind_kph").alias("min_wind_kph"),

                                # Precipitation metrics
                                spark_max("precip_mm").alias("max_precip_mm"),
                                spark_min("precip_mm").alias("min_precip_mm"),

                                # Air quality metrics
                                spark_max("pm2_5").alias("max_pm2_5"),
                                spark_min("pm2_5").alias("min_pm2_5"),
                                spark_max("pm10").alias("max_pm10"),
                                spark_min("pm10").alias("min_pm10"),

                                # UV index
                                spark_max("uv").alias("max_uv"),
                                spark_min("uv").alias("min_uv"),

                                # Visibility
                                spark_max("vis_km").alias("max_vis_km"),
                                spark_min("vis_km").alias("min_vis_km"),

                                # Processing metadata
                                current_timestamp().alias("last_updated")
                            ))

        # Add date string for partitioning
        output_df = (daily_aggregates
                     .select(
                         date_format(col("window.start"), "yyyy-MM-dd").alias("date"),
                         col("window.start").alias("window_start"),
                         col("window.end").alias("window_end"),
                         col("location_name"),
                         col("region"),
                         col("country"),
                         col("max_temp_c"),
                         col("min_temp_c"),
                         col("max_humidity"),
                         col("min_humidity"),
                         col("max_pressure_mb"),
                         col("min_pressure_mb"),
                         col("max_wind_kph"),
                         col("min_wind_kph"),
                         col("max_precip_mm"),
                         col("min_precip_mm"),
                         col("max_pm2_5"),
                         col("min_pm2_5"),
                         col("max_pm10"),
                         col("min_pm10"),
                         col("max_uv"),
                         col("min_uv"),
                         col("max_vis_km"),
                         col("min_vis_km"),
                         col("last_updated")
                     ))

        # Write results to MinIO
        logger.info("Setting up MinIO write stream")
        query = (output_df
                 .writeStream
                 .outputMode("update")  # Sử dụng update mode để ghi đè khi có giá trị mới
                 .foreachBatch(write_to_minio)
                 .option("checkpointLocation", CHECKPOINT_LOCATION)
                 .start())

        logger.info("Stream processing started, waiting for termination")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in process_weather_data: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

def write_to_minio(batch_df, batch_id):
    """Write batch data to MinIO with partitioning by date and location"""
    try:
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id} is empty, nothing to write")
            return

        count = batch_df.count()
        logger.info(f"Processing batch {batch_id} with {count} aggregated records")

        # Write to MinIO with partitioning by date and location
        output_path = f"s3a://{MINIO_BUCKET}/daily-aggregates"

        (batch_df
         .coalesce(1)  # Reduce number of files
         .write
         .mode("overwrite")  # Overwrite existing data for the same date/location
         .partitionBy("date", "location_name")
         .parquet(output_path))

        logger.info(f"Successfully wrote batch {batch_id} to MinIO at {output_path}")

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        raise

def main():
    """Main function to run the Weather Aggregator"""
    logger.info("Starting Weather Data Aggregator")

    # Wait for services to start
    logger.info("Waiting for Kafka and MinIO services to start...")
    time.sleep(30)

    try:
        process_weather_data()
    except Exception as e:
        logger.error(f"Error in weather aggregator: {e}")
        raise

if __name__ == "__main__":
    main()


