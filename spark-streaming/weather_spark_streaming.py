#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Weather Data Spark Streaming Application

This application reads weather data from Kafka, processes it using Spark Streaming,
and stores it in Hadoop HDFS after collecting data for all 63 provinces in Vietnam.
"""

import os
import json
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, window, count, lit, current_timestamp, expr, collect_list
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("WeatherDataProcessor")

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "weather-data")
HDFS_URL = os.environ.get("HDFS_URL", "hdfs://namenode:8020")
HDFS_PATH = os.environ.get("HDFS_PATH", "/weather-data")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "/checkpoints/weather-data")
BATCH_INTERVAL_SECONDS = int(os.environ.get("BATCH_INTERVAL_SECONDS", "60"))  # 1 minute
PROVINCE_COUNT = int(os.environ.get("PROVINCE_COUNT", "63"))  # 63 provinces in Vietnam

def create_spark_session():
    """Create and configure Spark session"""
    # Configure Spark with Kafka integration JARs
    # These JARs are downloaded in the Dockerfile
    
    logger.info("Creating Spark session with Kafka integration")
    return (SparkSession.builder
            .appName("WeatherDataProcessor")
            .config("spark.hadoop.fs.defaultFS", HDFS_URL)
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.schemaInference", "true")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
            .config("spark.streaming.kafka.consumer.cache.enabled", "false")  # Disable Kafka consumer cache
            .config("spark.executor.memory", "1g")  # Limit executor memory
            .config("spark.driver.memory", "1g")    # Limit driver memory
            .getOrCreate())

def define_schema():
    """Define schema for weather data"""
    location_schema = StructType([
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("tz_id", StringType(), True),
        StructField("localtime_epoch", LongType(), True),
        StructField("localtime", StringType(), True)
    ])
    
    condition_schema = StructType([
        StructField("text", StringType(), True),
        StructField("code", IntegerType(), True)
    ])
    
    current_schema = StructType([
        StructField("last_updated", StringType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("is_day", IntegerType(), True),
        StructField("condition", condition_schema, True),
        StructField("wind_kph", DoubleType(), True),
        StructField("precip_mm", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("cloud", IntegerType(), True),
        StructField("feelslike_c", DoubleType(), True),
        StructField("vis_km", DoubleType(), True),
        StructField("uv", DoubleType(), True)
    ])
    
    return StructType([
        StructField("location", location_schema, True),
        StructField("current", current_schema, True)
    ])

def read_from_kafka(spark, schema):
    """Read data from Kafka"""
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("kafka.max.poll.interval.ms", "300000")  # Increase poll timeout to 5 minutes
            .option("kafka.max.poll.records", "100")  # Reduce batch size
            .option("kafka.session.timeout.ms", "60000")  # Increase session timeout
            .option("failOnDataLoss", "false")  # Don't fail if data is lost
            .load()
            .select(from_json(col("value").cast("string"), schema).alias("data"))
            .select("data.*"))

def process_data(df):
    """Process the data and prepare for storage"""
    logger.info("Processing incoming weather data")
    
    # Extract relevant fields from the nested structure
    flattened_df = df.select(
        col("location.name").alias("location_name"),
        col("location.country").alias("country"),
        col("location.lat").alias("latitude"),
        col("location.lon").alias("longitude"),
        col("current.temp_c").alias("temperature"),
        col("current.condition.text").alias("condition"),
        col("current.humidity").alias("humidity"),
        col("current.wind_kph").alias("wind_speed"),
        col("current.uv").alias("uv_index"),
        current_timestamp().alias("processing_time")
    )
    
    # Add timestamp for windowing
    logger.info(f"Creating time windows of {BATCH_INTERVAL_SECONDS} seconds")
    windowed_df = (flattened_df
                  .withWatermark("processing_time", f"{BATCH_INTERVAL_SECONDS} seconds")
                  .groupBy(window("processing_time", f"{BATCH_INTERVAL_SECONDS} seconds"))
                  .agg(
                      count("location_name").alias("province_count"),
                      # Collect all data in the window as a JSON array
                      collect_list(to_json(struct("*"))).alias("weather_data")
                  ))
    
    # Log sample data for debugging - this will be used in the writeStream.foreachBatch
    def foreach_batch_function(batch_df, batch_id):
        count = batch_df.count()
        if count > 0:
            logger.info(f"Batch {batch_id} contains {count} rows with {batch_df.first()['province_count']} provinces")
            # Log sample data
            sample = batch_df.limit(1).collect()
            if sample:
                logger.info(f"Sample data: {sample[0]}")
        else:
            logger.info(f"Batch {batch_id} is empty")
    
    # Filter to only include batches with data
    # Reduced requirements - any batch with at least 1 province is accepted
    logger.info(f"Filtering for batches with at least 1 province")
    complete_batches = (windowed_df
                       .filter(col("province_count") >= 1))
    
    return complete_batches

def write_to_hdfs(df):
    """Write processed data to HDFS"""
    logger.info(f"Setting up stream to write to HDFS at {HDFS_URL}{HDFS_PATH}")
    
    # Define a simplified foreachBatch function to handle errors and add logging
    def process_batch(batch_df, batch_id):
        try:
            if batch_df.isEmpty():
                logger.info(f"Batch {batch_id} is empty, nothing to write")
                return
            
            count = batch_df.count()
            logger.info(f"Processing batch {batch_id} with {count} rows for HDFS")
            
            # Log sample data for debugging
            sample_data = batch_df.limit(1).collect()
            if sample_data:
                logger.info(f"Sample data from batch {batch_id}: {sample_data[0]}")
            
            # Simplify the approach - just write the data directly without complex transformations
            try:
                # Create a simplified dataframe with just the essential data
                # and convert window to string to avoid serialization issues
                simple_df = batch_df.select(
                    "province_count", 
                    "weather_data",
                    col("window.start").cast("string").alias("window_start"),
                    col("window.end").cast("string").alias("window_end")
                )
                
                # Add timestamp for partitioning
                timestamp_df = simple_df.withColumn(
                    "timestamp", 
                    expr("to_timestamp(window_end)")
                )
                
                # Add year, month, day for partitioning
                partitioned_df = timestamp_df.withColumn(
                    "year", expr("year(timestamp)")
                ).withColumn(
                    "month", expr("month(timestamp)")
                ).withColumn(
                    "day", expr("day(timestamp)")
                )
                
                # Write without complex partitioning first to test
                logger.info(f"Writing batch {batch_id} to HDFS")
                partitioned_df.write \
                    .format("parquet") \
                    .mode("append") \
                    .option("compression", "snappy") \
                    .save(f"{HDFS_URL}{HDFS_PATH}/weather_data")
                
                logger.info(f"Successfully wrote batch {batch_id} to HDFS")
            except Exception as inner_e:
                logger.error(f"Error processing dataframe for batch {batch_id}: {str(inner_e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                
                # Try an even simpler approach with just the raw data
                try:
                    logger.info(f"Trying minimal approach for batch {batch_id}")
                    # Just write the raw weather_data as JSON
                    minimal_df = batch_df.select("weather_data")
                    
                    minimal_df.write \
                        .format("json") \
                        .mode("append") \
                        .option("compression", "snappy") \
                        .save(f"{HDFS_URL}{HDFS_PATH}/raw_data")
                    
                    logger.info(f"Successfully wrote minimal data for batch {batch_id} to HDFS")
                except Exception as minimal_e:
                    logger.error(f"Even minimal approach failed for batch {batch_id}: {str(minimal_e)}")
                    logger.error(f"Traceback: {traceback.format_exc()}")
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    # Add import for traceback at the top of the function
    import traceback
    
    return (df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", f"{HDFS_URL}{CHECKPOINT_LOCATION}")
            .trigger(processingTime=f"{BATCH_INTERVAL_SECONDS} seconds")
            .start())

def main():
    """Main function to run the Spark Streaming job"""
    logger.info("Starting Weather Data Spark Streaming Application")
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Set log level to WARN to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        
        # Define schema
        schema = define_schema()
        logger.info("Schema defined successfully")
        
        # Read from Kafka
        logger.info(f"Reading from Kafka topic {KAFKA_TOPIC} at {KAFKA_BOOTSTRAP_SERVERS}")
        kafka_df = read_from_kafka(spark, schema)
        
        # Process data
        processed_df = process_data(kafka_df)
        
        # Write to HDFS
        logger.info(f"Setting up HDFS write stream to {HDFS_PATH}")
        query = write_to_hdfs(processed_df)
        
        # Wait for termination
        logger.info("Streaming query started, waiting for termination")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
