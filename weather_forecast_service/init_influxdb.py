import os
import sys
import logging
from influxdb_client import InfluxDBClient

# Add the parent directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

# Import configuration
from config import INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('init_influxdb')


def init_influxdb():
    """Initialize InfluxDB by creating the required bucket."""
    try:
        # Create InfluxDB client
        client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        logger.info("Connected to InfluxDB")
        
        # Get the buckets API
        buckets_api = client.buckets_api()
        
        # Check if the bucket already exists
        bucket = buckets_api.find_bucket_by_name(INFLUXDB_BUCKET)
        
        if bucket is None:
            # Create the bucket if it doesn't exist
            logger.info(f"Creating bucket '{INFLUXDB_BUCKET}'")
            buckets_api.create_bucket(bucket_name=INFLUXDB_BUCKET, org=INFLUXDB_ORG)
            logger.info(f"Bucket '{INFLUXDB_BUCKET}' created successfully")
        else:
            logger.info(f"Bucket '{INFLUXDB_BUCKET}' already exists")
        
        # Close the client
        client.close()
        
        return True
    except Exception as e:
        logger.error(f"Error initializing InfluxDB: {e}")
        return False


if __name__ == "__main__":
    if init_influxdb():
        logger.info("InfluxDB initialization completed successfully")
    else:
        logger.error("Failed to initialize InfluxDB")
