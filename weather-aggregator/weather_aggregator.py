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
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather-data")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "weather-aggregates")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "/tmp/checkpoints/weather-aggregator")

def define_weather_schema():
    """Define the schema for weather data from Kafka"""
    return StructType([
        StructField("province", StringType(), True),
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
    """Create and configure Spark session with MinIO support for local mode"""
    logger.info("Creating Spark session with Kafka and MinIO integration in local mode")

    return (SparkSession.builder
            .appName("WeatherAggregator")
            .master("local[*]")  # Sử dụng tất cả các core có sẵn trong local mode
            .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.sql.streaming.stateStore.providerClass",
                   "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
            .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution for streaming
            .config("spark.sql.shuffle.partitions", "2")  # Reduce shuffle partitions for small data
            .config("spark.default.parallelism", "2")  # Set default parallelism to match cores
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.driver.memory", "1g")
            .config("spark.driver.host", "0.0.0.0")  # Ensure driver binds to all interfaces
            .config("spark.network.timeout", "120s")  # Increase network timeout
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
        # Lưu ý: confluent-kafka producer gửi dữ liệu dưới dạng JSON string được mã hóa UTF-8
        parsed_df = (kafka_df
                     .select(
                         # Không cần sử dụng key vì province đã được thêm vào dữ liệu JSON
                         from_json(col("value").cast("string"), schema).alias("data"),
                         col("timestamp").alias("kafka_timestamp")
                     ))

        # Extract required fields
        weather_data = (parsed_df
                        .select(
                            col("data.province").alias("province"),
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
                                col("province"),
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
                         col("province"),
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
                 .trigger(processingTime='1 minute')  # Process data in 1-minute batches
                 .outputMode("update")  # Use update mode to overwrite when new values arrive
                 .option("checkpointLocation", CHECKPOINT_LOCATION)
                 .foreachBatch(write_to_minio)
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

        # Write to MinIO with partitioning by date, province and location
        output_path = f"s3a://{MINIO_BUCKET}/daily-aggregates"
        
        try:
            # First try to write the data
            (batch_df
             .coalesce(1)  # Reduce number of files
             .write
             .mode("overwrite")  # Overwrite existing data for the same date/province/location
             .partitionBy("date", "province", "location_name")
             .parquet(output_path))
            
            logger.info(f"Successfully wrote batch {batch_id} to MinIO at {output_path}")
        except Exception as write_error:
            # If writing fails, try to write to a simpler path without partitioning
            logger.warning(f"Error writing partitioned data: {str(write_error)}. Trying simplified approach...")
            simple_path = f"s3a://{MINIO_BUCKET}/daily-aggregates-batch-{batch_id}"
            
            (batch_df
             .coalesce(1)
             .write
             .mode("overwrite")
             .parquet(simple_path))
            
            logger.info(f"Successfully wrote batch {batch_id} to MinIO at {simple_path} using simplified approach")

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {str(e)}")
        # Log the error but don't raise to keep the stream running
        # This allows the job to continue even if one batch fails

def main():
    """Main function to run the Weather Aggregator"""
    logger.info("Starting Weather Data Aggregator")

    # Wait for services to start
    logger.info("Waiting for Kafka and MinIO services to start...")
    time.sleep(30)

    # Retry mechanism for the main process
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            process_weather_data()
            break  # If successful, exit the loop
        except Exception as e:
            retry_count += 1
            logger.error(f"Error in weather aggregator (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                logger.info(f"Retrying in 10 seconds...")
                time.sleep(10)
            else:
                logger.error("Maximum retry attempts reached. Exiting.")
                raise

if __name__ == "__main__":
    main()





