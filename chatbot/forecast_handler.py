import os
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient

# Function to get InfluxDB client
def get_influxdb_client():
    return InfluxDBClient(
        url=os.environ.get("INFLUXDB_URL", "http://influxdb:8086"),
        token=os.environ.get("INFLUXDB_TOKEN", "my-token"),
        org=os.environ.get("INFLUXDB_ORG", "my-org")
    )

# Function to query forecast data from InfluxDB
def get_forecast_data(locations, days=7):
    """
    Get weather forecast data for specific locations
    
    Args:
        locations (list): List of location names
        days (int): Number of days to forecast (default: 7)
        
    Returns:
        dict: Dictionary of forecast data by location and date
    """
    client = get_influxdb_client()
    query_api = client.query_api()
    
    # Format location filter for the query
    location_filter = ""
    if locations:
        location_conditions = [f'r.location == "{loc}"' for loc in locations]
        location_filter = f"\n    |> filter(fn: (r) => {' or '.join(location_conditions)})"
    
    # Build the query
    query = f'''
    from(bucket: "weather_forecast")
        |> range(start: 0, stop: {days}d)
        |> filter(fn: (r) => r._measurement == "weather_forecast"){location_filter}
        |> pivot(rowKey:["_time", "location"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"])
    '''
    
    try:
        result = query_api.query(org=os.environ.get("INFLUXDB_ORG", "my-org"), query=query)
        
        # Process results
        forecast_data = {}
        for table in result:
            for record in table.records:
                location = record.values.get("location")
                time = record.get_time()
                
                # Format time as string (date only for forecasts)
                date_str = time.strftime("%Y-%m-%d")
                
                # Initialize nested dictionaries if needed
                if location not in forecast_data:
                    forecast_data[location] = {}
                if date_str not in forecast_data[location]:
                    forecast_data[location][date_str] = {}
                
                # Map fields from forecast_service.py to forecast_handler.py
                field_mapping = {
                    # Direct mappings
                    "max_temp": "max_temp",
                    "min_temp": "min_temp",
                    "rainfall": "rainfall",
                    "humidity": "humidity",
                    "cloud_cover": "cloud_cover",
                    "condition": "condition",
                    # Field name conversions from forecast_service.py
                    "max": "max_temp",
                    "min": "min_temp",
                    "rain": "rainfall",
                    "humidi": "humidity",
                    "cloud": "cloud_cover"
                }
                
                # Add all fields to the forecast data
                for field in record.values.keys():
                    if field not in ["_time", "_start", "_stop", "_measurement", "location"]:
                        value = record.values.get(field)
                        if value is not None:
                            # Map the field name if needed
                            mapped_field = field_mapping.get(field, field)
                            forecast_data[location][date_str][mapped_field] = value
        
        return forecast_data
    except Exception as e:
        print(f"Error querying forecast data: {e}")
        return {}
    finally:
        client.close()

# Function to format forecast data for display
def format_forecast_response(forecast_data, question):
    """
    Format forecast data into a human-readable response
    
    Args:
        forecast_data (dict): Dictionary of forecast data by location and date
        question (str): The original user question
        
    Returns:
        str: Formatted response with forecast information
    """
    if not forecast_data:
        return "Xin lỗi, tôi không tìm thấy dữ liệu dự báo thời tiết cho địa điểm và thời gian bạn yêu cầu."
    
    response = "Dự báo thời tiết:\n\n"
    
    for location, dates in forecast_data.items():
        response += f"**{location}**:\n"
        
        for date, data in dates.items():
            # Parse the date string
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            # Format the date in Vietnamese style
            vn_date = date_obj.strftime("%d/%m/%Y")
            
            response += f"- Ngày {vn_date}:\n"
            
            # Check for condition field (could be named 'condition')
            if "condition" in data:
                response += f"  • Điều kiện: {data['condition']}\n"
                
            # Check for max temperature (could be named 'max_temp' or 'max')
            if "max_temp" in data:
                response += f"  • Nhiệt độ cao nhất: {data['max_temp']:.1f}°C\n"
            elif "max" in data:
                response += f"  • Nhiệt độ cao nhất: {data['max']:.1f}°C\n"
                
            # Check for min temperature (could be named 'min_temp' or 'min')
            if "min_temp" in data:
                response += f"  • Nhiệt độ thấp nhất: {data['min_temp']:.1f}°C\n"
            elif "min" in data:
                response += f"  • Nhiệt độ thấp nhất: {data['min']:.1f}°C\n"
                
            # Check for rainfall (could be named 'rainfall' or 'rain')
            if "rainfall" in data:
                response += f"  • Lượng mưa: {data['rainfall']:.1f} mm\n"
            elif "rain" in data:
                response += f"  • Lượng mưa: {data['rain']:.1f} mm\n"
                
            # Check for humidity (could be named 'humidity' or 'humidi')
            if "humidity" in data:
                response += f"  • Độ ẩm: {data['humidity']:.1f}%\n"
            elif "humidi" in data:
                response += f"  • Độ ẩm: {data['humidi']:.1f}%\n"
                
            # Check for cloud cover (could be named 'cloud_cover' or 'cloud')
            if "cloud_cover" in data:
                response += f"  • Độ che phủ mây: {data['cloud_cover']:.1f}%\n"
            elif "cloud" in data:
                response += f"  • Độ che phủ mây: {data['cloud']:.1f}%\n"
            
            response += "\n"
    
    # Add weather advice based on conditions
    for location, dates in forecast_data.items():
        for date, data in dates.items():
            # Check for high temperature warnings
            max_temp = None
            if "max_temp" in data:
                max_temp = float(data["max_temp"])
            elif "max" in data:
                max_temp = float(data["max"])
                
            if max_temp and max_temp > 35:
                response += f"⚠️ **Cảnh báo nhiệt độ cao tại {location}**: Nhiệt độ có thể lên đến {max_temp:.1f}°C. Hãy uống nhiều nước và tránh ra ngoài vào giữa trưa.\n"
            
            # Check for heavy rainfall warnings
            rainfall = None
            if "rainfall" in data:
                rainfall = float(data["rainfall"])
            elif "rain" in data:
                rainfall = float(data["rain"])
                
            if rainfall and rainfall > 20:
                response += f"⚠️ **Cảnh báo mưa lớn tại {location}**: Dự báo lượng mưa {rainfall:.1f}mm. Chuẩn bị ô/áo mưa và thận trọng khi di chuyển.\n"
    
    return response
