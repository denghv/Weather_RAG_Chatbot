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
    max_retries = 5
    retry_delay = 5  # seconds
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Cấu hình consumer
            config = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': KAFKA_GROUP_ID,
                'auto.offset.reset': 'earliest',  # Đọc từ đầu nếu không có offset đã commit
                'enable.auto.commit': False,      # Tắt auto commit để xử lý thủ công
                'max.poll.interval.ms': 300000,   # 5 phút
                'session.timeout.ms': 60000,      # 1 phút
                'request.timeout.ms': 30000       # 30 giây
            }
            
            # Tạo consumer
            consumer = Consumer(config)
            
            # Subscribe vào topic
            consumer.subscribe([KAFKA_TOPIC])
            
            # Lấy thông tin về các partition của topic
            metadata = consumer.list_topics(KAFKA_TOPIC, timeout=10)
            if KAFKA_TOPIC in metadata.topics:
                partitions = metadata.topics[KAFKA_TOPIC].partitions
                logger.info(f"Kafka consumer subscribed to topic {KAFKA_TOPIC} with {len(partitions)} partitions")
                for partition_id in partitions:
                    logger.info(f"  - Partition {partition_id}")
            else:
                logger.warning(f"Topic {KAFKA_TOPIC} not found in metadata")
            
            return consumer
            
        except Exception as e:
            retry_count += 1
            logger.error(f"Attempt {retry_count}/{max_retries} to create Kafka consumer failed: {e}")
            
            if retry_count < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Failed to create Kafka consumer.")
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
            message_count = 0
            expected_messages = 63  # Số lượng tỉnh thành dự kiến
            
            # Thời gian bắt đầu xử lý batch
            batch_start_time = time.time()
            
            while running:
                try:
                    # Poll for messages with timeout
                    # Tăng timeout để đảm bảo có đủ thời gian đọc tất cả messages
                    msg = consumer.poll(timeout=5.0)
                    
                    # Kiểm tra nếu đã nhận đủ số lượng message dự kiến
                    if message_count >= expected_messages:
                        logger.info(f"Received all {expected_messages} expected messages. Waiting for next batch...")
                        # Đợi 10 phút trước khi bắt đầu batch mới
                        time.sleep(600)
                        message_count = 0
                        batch_start_time = time.time()
                        continue
                    
                    # Kiểm tra nếu đã quá thời gian chờ cho một batch (15 phút)
                    current_time = time.time()
                    if current_time - batch_start_time > 900:  # 15 phút = 900 giây
                        logger.warning(f"Batch timeout reached. Only received {message_count}/{expected_messages} messages. Starting new batch.")
                        message_count = 0
                        batch_start_time = current_time
                        continue
                    
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
                    
                    # Log thông tin về partition và key
                    logger.info(f"Consumer received: {province} from partition {msg.partition()}")
                    
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
                        
                        # Log thông tin về partition và key hash
                        if province:
                            # Lấy số lượng partitions từ consumer
                            metadata = consumer.list_topics(KAFKA_TOPIC, timeout=5)
                            if KAFKA_TOPIC in metadata.topics:
                                num_partitions = len(metadata.topics[KAFKA_TOPIC].partitions)
                                partition_hash = hash(province) % num_partitions
                                logger.info(f"Key: {province}, hash = {partition_hash}, actual partition = {msg.partition()}")
                                
                                # Kiểm tra xem hash có khớp với partition thực tế không
                                if partition_hash != msg.partition():
                                    logger.warning(f"Hash partition mismatch for {province}: expected {partition_hash}, got {msg.partition()}")
                        
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
                            logger.info(f"Successfully wrote data for {location_display} to InfluxDB")
                            
                            # Tăng số lượng message đã xử lý
                            message_count += 1
                            logger.info(f"Processed {message_count}/{expected_messages} messages in current batch")
                        
                        # Commit offset sau mỗi message
                        consumer.commit(msg)
                        
                        # Đảm bảo không có message nào bị bỏ sót bằng cách poll thêm
                        # nhưng không đợi (timeout=0)
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
        if not check_kafka_connection():
            logger.error("Cannot connect to Kafka. Exiting...")
            sys.exit(1)
            
        # Kiểm tra kết nối InfluxDB
        try:
            client = create_influxdb_client()
            health = client.health()
            logger.info(f"InfluxDB health check: {health.status}")
            client.close()
        except Exception as e:
            logger.error(f"InfluxDB health check failed: {e}")
            sys.exit(1)
        
        # Hiển thị thông tin về consumer group và instance ID
        logger.info(f"Consumer Group ID: {KAFKA_GROUP_ID}")
        logger.info(f"Consumer Instance: {socket.gethostname()}")
        
        # Bắt đầu xử lý dữ liệu thởi tiết
        process_weather_data()
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)