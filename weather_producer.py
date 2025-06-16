import json
import time
import requests
import logging
import os
import random
from kafka import KafkaProducer
from provinces import VIETNAM_PROVINCES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Weather API configuration
WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY', 'ea8aa08895454e78b10145125253003')
WEATHER_API_URL = "http://api.weatherapi.com/v1/current.json"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'weather-data')

def create_kafka_producer():
    """Create and return a Kafka producer instance with retry logic."""
    max_retries = 10
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # Cấu hình producer với partitioner ngẫu nhiên
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8'),
                # Sử dụng RoundRobinPartitioner để phân phối đều giữa các partition
                partitioner=lambda key, all_partitions, available: random.choice(all_partitions)
            )
            logger.info(f"Successfully connected to Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to connect to Kafka (attempt {attempt+1}/{max_retries}): {e}")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                raise

def fetch_weather_data(province):
    """Fetch weather data for a specific province."""
    params = {
        'key': WEATHER_API_KEY,
        'q': province,
        'aqi': 'yes'  # Request air quality data
    }
    
    try:
        response = requests.get(WEATHER_API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Log air quality data if available
        if 'current' in data and 'air_quality' in data['current']:
            logger.info(f"Received air quality data for {province}: {data['current']['air_quality']}")
        else:
            logger.warning(f"No air quality data received for {province}")
            
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather data for {province}: {e}")
        return None

def produce_weather_data():
    """Fetch weather data for all provinces and send to Kafka."""
    while True:
        try:
            producer = create_kafka_producer()
            logger.info("Starting to fetch weather data for all provinces...")
            
            for province in VIETNAM_PROVINCES:
                try:
                    weather_data = fetch_weather_data(province)
                    
                    if weather_data:
                        # Vẫn giữ province làm key nhưng partitioner sẽ phân phối ngẫu nhiên
                        future = producer.send(KAFKA_TOPIC, key=province, value=weather_data)
                        # Wait for the message to be delivered
                        record_metadata = future.get(timeout=10)
                        logger.info(f"Sent weather data for {province} to Kafka topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
                        
                        # Add a small delay between API calls to avoid rate limiting
                        time.sleep(1)
                except Exception as e:
                    logger.error(f"Error processing {province}: {e}")
            
            logger.info("Completed fetching weather data for all provinces")
            producer.flush()  # Ensure all messages are sent before closing
            
            # Wait for 10 minutes before the next batch
            time.sleep(600)  # 600 seconds = 10 minutes
            
        except Exception as e:
            logger.error(f"Error in produce_weather_data: {e}")
            logger.info("Retrying in 30 seconds...")
            time.sleep(30)  # Wait 30 seconds before retrying

if __name__ == "__main__":
    try:
        logger.info("Weather data producer starting...")
        produce_weather_data()
    except KeyboardInterrupt:
        logger.info("Weather data producer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
