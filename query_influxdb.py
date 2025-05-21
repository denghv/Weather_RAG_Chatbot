import sys
from influxdb_client import InfluxDBClient

# InfluxDB configuration from your config file
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "my-token"
INFLUXDB_ORG = "my-org"
INFLUXDB_BUCKET = "weather_forecast"

# Create a client
client = InfluxDBClient(
    url=INFLUXDB_URL,
    token=INFLUXDB_TOKEN,
    org=INFLUXDB_ORG
)

# Create query API
query_api = client.query_api()

# Define the Flux query
query = f'''
from(bucket: "{INFLUXDB_BUCKET}")
    |> range(start: -7d)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''

try:
    # Execute the query
    result = query_api.query_data_frame(query=query)
    
    if result is not None and not result.empty:
        # Display the results
        print(f"\nFound {len(result)} records in the weather_forecast bucket:\n")
        print(result[['_time', 'location', 'max_temp', 'min_temp', 'rainfall', 'humidity', 'cloud_cover', 'condition']])
    else:
        print("\nNo data found in the weather_forecast bucket.")
        print("Make sure the forecast service has been running and writing data to InfluxDB.")

except Exception as e:
    print(f"Error querying InfluxDB: {e}")
    
    # Check if the bucket exists
    try:
        buckets_api = client.buckets_api()
        buckets = buckets_api.find_buckets().buckets
        print("\nAvailable buckets:")
        for bucket in buckets:
            print(f"- {bucket.name}")
    except Exception as bucket_error:
        print(f"Error listing buckets: {bucket_error}")

finally:
    client.close()
