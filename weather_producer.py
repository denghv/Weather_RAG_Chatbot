import json
import time
import logging
import os
import asyncio
import aiohttp
import socket
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from provinces import VIETNAM_PROVINCES

# location name correction mapping
LOCATION_CORRECTIONS = {
    "An Long": "Long An",
    "Ap Binh Quang": "Quang Binh",
    "Ap Binh Thuan": "Binh Thuan",
    "Buon Bubo Dak Nong": "Dak Nong",
    "Dong Lam": "Lam Dong",
    "Goc Nghe": "Nghe An",
    "Hoa Khanh": "Khanh Hoa",
    "Hue": "Thua Thien Hue",
    "Kien": "Kien Giang",
    "Lak Lak": "Dak Lak",
    "Ninh Quang": "Quang Ninh",
    "Thuan Ninh": "Ninh Thuan",
    "Tien Tien": "Tien Giang",
    "Yen Hung": "Hung Yen"
}

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Weather API configuration
WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY', 'YOUR_WEATHER_API_KEY_HERE')
WEATHER_API_URL = "http://api.weatherapi.com/v1/current.json"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka1:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'weather-data')

def delivery_callback(err, msg):
    """Callback function for Kafka producer delivery reports"""
    if err:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
        try:
            # Decode và log một phần nội dung để xác nhận
            value = msg.value().decode('utf-8')
            data = json.loads(value)
            province = data.get('province', 'Unknown')
            logger.debug(f'Confirmed delivery for province: {province}')
        except Exception as e:
            logger.warning(f'Could not decode message content: {e}')

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

def create_kafka_producer():
    """Create and return a Kafka producer instance with retry logic."""
    max_retries = 10
    retry_delay = 5  # seconds
    
    # Kiểm tra kết nối trước khi tạo producer
    if not check_kafka_connection():
        logger.error("Cannot connect to Kafka broker")
        time.sleep(10)  # Đợi một chút trước khi thử lại
    
    for attempt in range(max_retries):
        try:
            #confluent-kafka
            conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'client.id': 'weather-producer',
                'acks': '1',
                'message.timeout.ms': '10000'
            }
            producer = Producer(conf)
            logger.info(f"Successfully connected to Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to connect to Kafka (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                raise

async def fetch_weather_data(session, province):
    """Fetch weather data for a specific province asynchronously."""
    try:
        params = {
            'key': WEATHER_API_KEY,
            'q': province,
            'aqi': 'yes'  # Include air quality data
        }
        
        async with session.get(WEATHER_API_URL, params=params) as response:
            if response.status == 200:
                data = await response.json()
                logger.info(f"Successfully fetched weather data for {province}")
                return province, data
            else:
                error_text = await response.text()
                logger.error(f"Failed to fetch weather data for {province}: HTTP {response.status}, {error_text}")
                return province, None
    except Exception as e:
        logger.error(f"Error fetching weather data for {province}: {e}")
        return province, None

async def fetch_all_weather_data():
    """Fetch weather data for all provinces asynchronously."""
    async with aiohttp.ClientSession() as session:
        # Create tasks for all provinces
        tasks = [fetch_weather_data(session, province) for province in VIETNAM_PROVINCES]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        valid_results = {}
        for result in results:
            if isinstance(result, tuple) and len(result) == 2 and result[1] is not None:
                province, data = result
                valid_results[province] = data
        
        logger.info(f"Successfully fetched weather data for {len(valid_results)}/{len(VIETNAM_PROVINCES)} provinces")
        return valid_results

def correct_location_name(location_name):
    """Correct location name based on predefined mapping."""
    if location_name in LOCATION_CORRECTIONS:
        corrected_name = LOCATION_CORRECTIONS[location_name]
        logger.info(f"Corrected location name from '{location_name}' to '{corrected_name}'")
        return corrected_name
    return location_name

async def produce_weather_data_async():
    """Produce weather data to Kafka topic."""
    while True:
        try:
            # Kiểm tra kết nối Kafka trước khi tạo producer
            if not check_kafka_connection():
                logger.warning("Kafka connection not available. Waiting 30 seconds before retry...")
                await asyncio.sleep(30)
                continue
            
            # Tạo Kafka producer
            producer = create_kafka_producer()
            logger.info(f"Successfully created Kafka producer")
            
            # Lấy số lượng partitions của topic
            admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
            topic_metadata = admin_client.list_topics(timeout=10)
            
            if KAFKA_TOPIC in topic_metadata.topics:
                num_partitions = len(topic_metadata.topics[KAFKA_TOPIC].partitions)
                logger.info(f"Topic {KAFKA_TOPIC} has {num_partitions} partitions")
            else:
                # Nếu topic chưa tồn tại, giả định 6 partitions
                num_partitions = 6
                logger.warning(f"Topic {KAFKA_TOPIC} not found. Assuming {num_partitions} partitions")
            
            # dữ liệu thời tiết cho tất cả các tỉnh
            weather_data_dict = await fetch_all_weather_data()
            
            # số lượng tin nhắn đã gửi thành công
            success_count = 0
            
            # phân phối partition
            partition_distribution = {i: 0 for i in range(num_partitions)}
            
            # Gửi dữ liệu thời tiết cho từng tỉnh
            for province, weather_data in weather_data_dict.items():
                try:
                    # Log chi tiết dữ liệu thời tiết
                    location = weather_data.get('location', {})
                    current = weather_data.get('current', {})
                    air_quality = current.get('air_quality', {})
                    
                    location_name = location.get('name', '')
                    logger.info(f"Sending weather data for {province} - {location_name}:")
                    logger.info(f"  Time: {current.get('last_updated', '')}")
                    
                    logger.info(f"  Temperature: {current.get('temp_c')}°C")
                    logger.info(f"  Condition: {current.get('condition', {}).get('text', '')}")
                    logger.info(f"  Wind: {current.get('wind_kph')} kph, {current.get('wind_dir')}")
                    logger.info(f"  Humidity: {current.get('humidity')}%")
                    logger.info(f"  Precipitation: {current.get('precip_mm')} mm")
                    logger.info(f"  Cloud: {current.get('cloud')}%")
                    logger.info(f"  UV Index: {current.get('uv')}")
                    
                    # Log thông tin chất lượng không khí
                    if air_quality:
                        logger.info(f"  Air Quality - PM2.5: {air_quality.get('pm2_5')}, PM10: {air_quality.get('pm10')}")
                    else:
                        logger.info(f"  No air quality data available")
                    
                    # Thêm tên tỉnh vào dữ liệu thời tiết
                    weather_data['province'] = province
                    
                    # Chuyển dữ liệu thành JSON string
                    json_data = json.dumps(weather_data)
                    
                    # Sử dụng tên tỉnh làm key để đảm bảo phân phối đều
                    key = province.encode('utf-8')
                    
                    # Tính toán partition dự kiến dựa trên key
                    partition_hash = hash(province) % num_partitions
                    partition_distribution[partition_hash] += 1
                    
                    # Log thông tin phân phối
                    logger.info(f"Sent province: {province}, key hash = {partition_hash} (partition {partition_hash}/{num_partitions})")
                    
                    # Gửi dữ liệu đến Kafka sử dụng confluent-kafka với key
                    try:
                        producer.produce(
                            topic=KAFKA_TOPIC,
                            key=key,
                            value=json_data.encode('utf-8'),
                            callback=delivery_callback
                        )
                        # Đảm bảo gửi dữ liệu ngay lập tức
                        producer.poll(0)
                        success_count += 1
                        logger.info(f"Successfully sent data for {province} to Kafka topic {KAFKA_TOPIC}")
                    except Exception as e:
                        logger.error(f"Error sending data for {province}: {e}")
                except Exception as e:
                    logger.error(f"Error processing data for {province}: {e}")
            
            # Hiển thị phân phối partition
            logger.info(f"Partition distribution: {partition_distribution}")
            
            # Đảm bảo tất cả dữ liệu được gửi đi
            if producer:
                try:
                    # Đợi cho tất cả các tin nhắn được gửi đi
                    remaining = producer.flush(timeout=10)
                    if remaining > 0:
                        logger.warning(f"Failed to flush all messages. {remaining} messages remain")
                except Exception as e:
                    logger.error(f"Error flushing producer: {e}")
                    
            logger.info(f"Completed sending weather data to Kafka: {success_count}/{len(weather_data_dict)} successful")
            
            # Wait for 10 minutes before the next batch
            await asyncio.sleep(600)  # 600 seconds = 10 minutes
            
        except Exception as e:
            logger.error(f"Error in produce_weather_data_async: {e}")
            logger.info("Retrying in 30 seconds...")
            await asyncio.sleep(30)  # Wait 30 seconds before retrying

if __name__ == "__main__":
    try:
        logger.info("Weather data producer starting...")
        
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
        
        asyncio.run(produce_weather_data_async())
    except KeyboardInterrupt:
        logger.info("Weather data producer stopped by user")
    except Exception as e:
        logger.error(f"Error in weather data producer: {e}")
    finally:
        logger.info("Weather data producer stopped")
