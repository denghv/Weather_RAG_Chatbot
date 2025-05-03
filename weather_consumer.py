import json
import logging
import os
import time
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import pytz
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'weather-data')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'weather-influxdb-consumer')

# InfluxDB configuration
INFLUXDB_URL = os.environ.get('INFLUXDB_URL', 'http://influxdb:8086')
INFLUXDB_TOKEN = os.environ.get('INFLUXDB_TOKEN', 'my-token')
INFLUXDB_ORG = os.environ.get('INFLUXDB_ORG', 'my-org')
INFLUXDB_BUCKET = os.environ.get('INFLUXDB_BUCKET', 'weather')

def create_kafka_consumer():
    """Create and return a Kafka consumer instance with retry logic."""
    max_retries = 10
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=False,
                max_poll_interval_ms=300000,  # 5 minutes
                session_timeout_ms=30000,
                request_timeout_ms=60000
            )
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{max_retries} failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                raise

def create_influxdb_client():
    """Create and return an InfluxDB client instance."""
    try:
        client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG
        )
        logger.info(f"Connected to InfluxDB at {INFLUXDB_URL}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to InfluxDB: {e}")
        raise

def weather_data_to_point(weather_data):
    """
    Convert weather data to InfluxDB point in line protocol format.
    Example output format: weather,location=Hanoi condition="Partly cloudy",temp_c=26.2,pm2_5=154.66,pm10=155.585,uv=4.2
    """
    try:
        # Extract location name
        location = weather_data.get('location', {}).get('name', 'Unknown')
        
        # Extract current weather data
        current = weather_data.get('current', {})
        temp_c = current.get('temp_c')
        condition_text = current.get('condition', {}).get('text', '')
        uv_index = current.get('uv')  # Extract UV index
        
        # Extract air quality data if available
        air_quality = current.get('air_quality', {})
        
        # Log air quality data for debugging
        if not air_quality or len(air_quality) == 0:
            logger.warning(f"No air quality data found for {location}")
            logger.debug(f"Current weather data: {current}")
        else:
            logger.info(f"Air quality data found for {location}: {air_quality}")
            
        pm2_5 = air_quality.get('pm2_5')
        pm10 = air_quality.get('pm10')
        
        # Create a Point with 'weather' as measurement and location as tag
        timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        current_time = datetime.now(timezone)
        point = Point("weather").tag("location", location).time(current_time)
        
        # Add fields
        if condition_text:
            point = point.field("condition", condition_text)
        if temp_c is not None:
            point = point.field("temp_c", float(temp_c))
        if uv_index is not None:
            point = point.field("uv", float(uv_index))
        if pm2_5 is not None:
            point = point.field("pm2_5", float(pm2_5))
        if pm10 is not None:
            point = point.field("pm10", float(pm10))
        
        # Log the line protocol format for debugging
        logger.debug(f"Line protocol: weather,location={location} condition=\"{condition_text}\",temp_c={temp_c},uv={uv_index or 'null'},pm2_5={pm2_5 or 'null'},pm10={pm10 or 'null'}")
        
        return point
    except Exception as e:
        logger.error(f"Error converting weather data to point: {e}")
        logger.error(f"Weather data: {weather_data}")
        return None

def process_weather_data():
    """Consume weather data from Kafka and write to InfluxDB."""
    while True:
        try:
            consumer = create_kafka_consumer()
            influxdb_client = create_influxdb_client()
            write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
            
            logger.info(f"Starting to consume messages from Kafka topic: {KAFKA_TOPIC}")
            
            for message in consumer:
                try:
                    weather_data = message.value
                    province = message.key.decode('utf-8') if message.key else 'Unknown'
                    
                    logger.info(f"Received weather data for {province}")
                    
                    # Convert to InfluxDB point
                    point = weather_data_to_point(weather_data)
                    
                    if point:
                        # Write to InfluxDB
                        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                        logger.info(f"Successfully wrote data for {province} to InfluxDB")
                    
                    # Commit offset
                    consumer.commit()
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
        except Exception as e:
            logger.error(f"Error in process_weather_data: {e}")
            logger.info("Retrying in 30 seconds...")
            time.sleep(30)  # Wait 30 seconds before retrying

if __name__ == "__main__":
    try:
        logger.info("Weather data consumer starting...")
        process_weather_data()
    except KeyboardInterrupt:
        logger.info("Weather data consumer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
