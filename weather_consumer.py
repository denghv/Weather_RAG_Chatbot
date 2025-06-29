import json
import logging
import os
import time
import socket
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
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

def check_kafka_connection():
    """Check if Kafka broker is reachable"""
    try:
        host, port_str = KAFKA_BOOTSTRAP_SERVERS.split(':')
        port = int(port_str)
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            logger.info(f"Kafka broker {KAFKA_BOOTSTRAP_SERVERS} is reachable")
            return True
        else:
            logger.error(f"Kafka broker {KAFKA_BOOTSTRAP_SERVERS} is not reachable, error code: {result}")
            return False
    except Exception as e:
        logger.error(f"Error checking Kafka connection to {KAFKA_BOOTSTRAP_SERVERS}: {e}")
        return False

def create_kafka_consumer():
    """Create and return a Kafka consumer instance with retry logic."""
    max_retries = 10
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # Cấu hình consumer sử dụng confluent-kafka
            conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': KAFKA_GROUP_ID,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
                'max.poll.interval.ms': 300000,  # 5 minutes
                'session.timeout.ms': 30000,
                'request.timeout.ms': 60000
            }
            
            consumer = Consumer(conf)
            
            # Subscribe to topic
            consumer.subscribe([KAFKA_TOPIC])
            
            # Test connection with a poll call
            consumer.poll(0)
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except KafkaException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to connect to Kafka (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                raise

def create_influxdb_client():
    """Create and return an InfluxDB client instance with retry logic."""
    max_retries = 10
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            client = InfluxDBClient(
                url=INFLUXDB_URL,
                token=INFLUXDB_TOKEN,
                org=INFLUXDB_ORG
            )
            
            # Test connection by getting health
            health = client.health()
            logger.info(f"Successfully connected to InfluxDB at {INFLUXDB_URL}")
            logger.info(f"InfluxDB status: {health.status}")
            return client
        
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to connect to InfluxDB (attempt {attempt + 1}/{max_retries}): {e}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to InfluxDB after {max_retries} attempts: {e}")
                raise


def weather_data_to_point(weather_data):
    """
    Convert weather data to InfluxDB point in line protocol format.
    Example output format: weather,location=Hanoi condition="Partly cloudy",temp_c=26.2,pm2_5=154.66,pm10=155.585,uv=4.2
    """
    try:
        # Extract location name
        location = weather_data.get('location', {}).get('name', '')
        province = weather_data.get('province', '')
        
        # Extract current weather data
        current = weather_data.get('current', {})
        temp_c = current.get('temp_c')
        condition_text = current.get('condition', {}).get('text', '')
        uv_index = current.get('uv')  # Extract UV index
        
        # Extract air quality data if available
        air_quality = current.get('air_quality', {})
        pm2_5 = air_quality.get('pm2_5')
        pm10 = air_quality.get('pm10')
        
        # Create a Point with 'weather' as measurement and location as tag
        timezone = pytz.timezone('Asia/Ho_Chi_Minh')
        current_time = datetime.now(timezone)
        point = Point("weather").tag("location", location).tag("province", province).time(current_time)
        
        # Add fields
        if condition_text:
            point = point.field("condition", condition_text)
        if temp_c is not None:
            point = point.field("temp_c", float(temp_c))
        if uv_index is not None:
            point = point.field("uv", float(uv_index))
        # Add the requested additional fields
        if 'wind_kph' in current and current['wind_kph'] is not None:
            point = point.field("wind_kph", float(current['wind_kph']))
        if 'precip_mm' in current and current['precip_mm'] is not None:
            point = point.field("precip_mm", float(current['precip_mm']))
        if 'humidity' in current and current['humidity'] is not None:
            point = point.field("humidity", int(current['humidity']))
        if 'cloud' in current and current['cloud'] is not None:
            point = point.field("cloud", int(current['cloud']))
        if pm2_5 is not None:
            point = point.field("pm2_5", float(pm2_5))
        if pm10 is not None:
            point = point.field("pm10", float(pm10))
        
        # Log the line protocol format for debugging
        logger.debug(f"Line protocol: weather,location={location},province={province} condition=\"{condition_text}\",temp_c={temp_c},uv={uv_index or 'null'},pm2_5={pm2_5 or 'null'},pm10={pm10 or 'null'}")
        
        return point
    except Exception as e:
        logger.error(f"Error converting weather data to point: {e}")
        logger.error(f"Weather data: {weather_data}")
        return None

def process_weather_data():
    """Consume weather data from Kafka and write to InfluxDB."""
    while True:
        try:
            # Tạo Kafka consumer
            consumer = create_kafka_consumer()
            logger.info(f"Successfully created Kafka consumer for topic {KAFKA_TOPIC}")
            
            # Tạo InfluxDB client và kiểm tra/khởi tạo bucket
            influxdb_client = create_influxdb_client()
            write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
            logger.info(f"Successfully created InfluxDB write API for bucket {INFLUXDB_BUCKET}")
            
            logger.info(f"Starting to consume messages from Kafka topic: {KAFKA_TOPIC}")
            
            # Process messages
            running = True
            while running:
                try:
                    # Poll for messages
                    msg = consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        # No message available, continue polling
                        continue
                        
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event - not an error
                            logger.debug(f"Reached end of partition {msg.partition()}")
                        else:
                            # Error
                            logger.error(f"Error while consuming message: {msg.error()}")
                        continue
                    
                    # Get the message value (bytes)
                    raw_bytes = msg.value()
                    province = msg.key().decode('utf-8') if msg.key() else ''
                    
                    try:
                        # Decode bytes to string and parse JSON
                        if raw_bytes is not None:
                            raw_string = raw_bytes.decode('utf-8')
                            weather_json = json.loads(raw_string)
                        else:
                            logger.error("Received null message value")
                            consumer.commit(msg)
                            continue
                        
                        # Add province to weather data for use in point creation
                        weather_json['province'] = province
                        
                        location = weather_json.get('location', {})
                        current = weather_json.get('current', {})
                        air_quality = current.get('air_quality', {})
                        
                        # Log chi tiết dữ liệu nhận được
                        location_name = location.get('name', '')
                        location_display = location_name
                        if province and location_name:
                            location_display = f"{province} - {location_name}"
                        elif province:
                            location_display = province
                        
                        logger.info(f"Received weather data for {location_display}:")
                        logger.info(f"  Time: {current.get('last_updated', '')}")
                        
                        logger.info(f"  Temperature: {current.get('temp_c')}°C")
                        logger.info(f"  Condition: {current.get('condition', {}).get('text', '')}")
                        logger.info(f"  Wind: {current.get('wind_kph')} kph, {current.get('wind_dir')}")
                        logger.info(f"  Humidity: {current.get('humidity')}%")
                        logger.info(f"  Precipitation: {current.get('precip_mm')} mm")
                        logger.info(f"  Cloud: {current.get('cloud')}%")
                        logger.info(f"  UV Index: {current.get('uv')}")
                        
                        
                        # Log thông tin chất lượng không khí
                        if air_quality and air_quality.get('pm2_5') and air_quality.get('pm10'):
                            logger.info(f"  Air Quality - PM2.5: {air_quality.get('pm2_5')}, PM10: {air_quality.get('pm10')}")
                        
                        # Convert to InfluxDB point
                        point = weather_data_to_point(weather_json)
                        
                        if point:
                            # Write to InfluxDB
                            write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                            logger.info(f"Successfully wrote data for {location_name} to InfluxDB")
                        
                        # Commit offset
                        consumer.commit(msg)
                        
                        # Flush to make sure the message is committed
                        consumer.poll(0)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON data: {e}")
                        logger.error(f"Raw string: {raw_string[:200]}..." if 'raw_string' in locals() else "Raw string not available")
                        consumer.commit(msg)  # Commit offset even for bad messages to avoid reprocessing
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    try:
                        consumer.commit(msg)
                    except Exception as commit_error:
                        logger.error(f"Error committing offset: {commit_error}")
            
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            break
            
        except Exception as e:
            logger.error(f"Error in main consumer loop: {e}")
            logger.info("Restarting consumer in 10 seconds...")
            time.sleep(10)
            
        finally:
            # Clean up resources
            try:
                if 'consumer' in locals() and consumer:
                    consumer.close()
                    logger.info("Kafka consumer closed")
                if 'influxdb_client' in locals() and influxdb_client:
                    influxdb_client.close()
                    logger.info("InfluxDB client closed")
            except Exception as e:
                logger.error(f"Error closing resources: {e}")

if __name__ == "__main__":
    try:
        logger.info("Weather data consumer starting...")
        
        # Kiểm tra kết nối Kafka trước khi bắt đầu
        max_retries = 30
        retry_delay = 5
        max_retry_delay = 30
        current_delay = retry_delay
        
        for attempt in range(max_retries):
            logger.info(f"Checking Kafka connection, attempt {attempt+1}/{max_retries}")
            if check_kafka_connection():
                logger.info("Successfully connected to Kafka broker")
                break
            else:
                logger.warning(f"Cannot connect to Kafka broker, retrying in {current_delay} seconds...")
                time.sleep(current_delay)
                # Tăng thời gian chờ giữa các lần thử (progressive backoff)
                if current_delay < max_retry_delay:
                    current_delay = min(current_delay + 5, max_retry_delay)
                if attempt == max_retries - 1:
                    logger.error("Failed to connect to Kafka broker after maximum retries")
        
        # Check InfluxDB connection
        try:
            influxdb_client = create_influxdb_client()
            logger.info("Successfully connected to InfluxDB")
            influxdb_client.close()
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            logger.warning("Will retry connection during processing")
        
        process_weather_data()
    except KeyboardInterrupt:
        logger.info("Weather data consumer stopped by user")
    except Exception as e:
        logger.error(f"Error in weather data consumer: {e}")
        raise
