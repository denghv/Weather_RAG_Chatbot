import os
import json
from influxdb_client import InfluxDBClient
from datetime import datetime, timedelta

# Function to get InfluxDB client
def get_influxdb_client():
    return InfluxDBClient(
        url=os.environ.get("INFLUXDB_URL", "http://influxdb:8086"),
        token=os.environ.get("INFLUXDB_TOKEN", "my-token"),
        org=os.environ.get("INFLUXDB_ORG", "my-org")
    )

# Function to query forecast data
def get_forecast_data(location, days=7):
    """
    Get weather forecast data for a specific location
    
    Args:
        location (str): Location name
        days (int): Number of days to forecast (default: 7)
        
    Returns:
        list: List of forecast data points
    """
    client = get_influxdb_client()
    query_api = client.query_api()
    
    # Build the query
    query = f'''
    from(bucket: "weather_forecast")
        |> range(start: 0)
        |> filter(fn: (r) => r.location == "{location}")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"])
        |> limit(n: {days})
    '''
    
    try:
        result = query_api.query(org=os.environ.get("INFLUXDB_ORG", "my-org"), query=query)
        
        # Process results
        forecast_data = []
        for table in result:
            for record in table.records:
                forecast_data.append({
                    "date": record.get_time().strftime("%Y-%m-%d"),
                    "location": record.values.get("location"),
                    "max_temp": record.values.get("max_temp"),
                    "min_temp": record.values.get("min_temp"),
                    "rainfall": record.values.get("rainfall"),
                    "humidity": record.values.get("humidity"),
                    "cloud_cover": record.values.get("cloud_cover"),
                    "condition": record.values.get("condition"),
                    "latitude": record.values.get("latitude"),
                    "longitude": record.values.get("longitude")
                })
        
        return forecast_data
    except Exception as e:
        print(f"Error querying forecast data: {e}")
        return []
    finally:
        client.close()

# Function to format forecast data for display
def format_forecast_data(forecast_data):
    """
    Format forecast data for display
    
    Args:
        forecast_data (list): List of forecast data points
        
    Returns:
        dict: Formatted forecast data grouped by location
    """
    if not forecast_data:
        return {}
    
    # Group data by location
    grouped_data = {}
    for item in forecast_data:
        location = item.get("location", "Unknown")
        date = item.get("date")
        
        if location not in grouped_data:
            grouped_data[location] = {}
        
        grouped_data[location][date] = {
            "max_temp": item.get("max_temp"),
            "min_temp": item.get("min_temp"),
            "rainfall": item.get("rainfall"),
            "humidity": item.get("humidity"),
            "cloud_cover": item.get("cloud_cover"),
            "condition": item.get("condition")
        }
    
    return grouped_data

# Function to get forecast for a specific location
def get_location_forecast(location, days=7):
    """
    Get weather forecast for a specific location
    
    Args:
        location (str): Location name
        days (int): Number of days to forecast (default: 7)
        
    Returns:
        dict: Formatted forecast data
    """
    forecast_data = get_forecast_data(location, days)
    return format_forecast_data(forecast_data)
