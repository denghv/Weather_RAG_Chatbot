import os
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient

# Function to translate weather conditions from English to Vietnamese
def translate_weather_condition(condition):
    """
    Translate weather condition from English to Vietnamese
    
    Args:
        condition (str): Weather condition in English
        
    Returns:
        str: Weather condition in Vietnamese
    """
    # Dictionary mapping English weather conditions to Vietnamese
    condition_map = {
        # Clear conditions
        'Sunny': 'Nắng',
        'Clear': 'Quang đãng',
        'Fine': 'Đẹp trời',
        
        # Cloudy conditions
        'Partly cloudy': 'Có mây rải rác',
        'Cloudy': 'Nhiều mây',
        'Overcast': 'U ám',
        
        # Rain conditions
        'Light rain': 'Mưa nhẹ',
        'Moderate rain': 'Mưa vừa',
        'Heavy rain': 'Mưa to',
        'Rain': 'Mưa',
        'Light Rain': 'Mưa nhẹ',
        'Moderate Rain': 'Mưa vừa',
        'Heavy Rain': 'Mưa to',
        'Patchy rain possible': 'Có thể có mưa rải rác',
        'Patchy light rain': 'Mưa nhẹ rải rác',
        'Moderate rain at times': 'Mưa vừa lúc trời',
        'Heavy rain at times': 'Mưa to lúc trời',
        'Patchy light drizzle': 'Mưa phùn nhẹ rải rác',
        'Light drizzle': 'Mưa phùn nhẹ',
        'Freezing drizzle': 'Mưa phùn lạnh cóng',
        'Heavy freezing drizzle': 'Mưa phùn lạnh cóng nặng',
        
        # Thunderstorm conditions
        'Thundery outbreaks possible': 'Có thể có dông',
        'Patchy light rain with thunder': 'Mưa nhẹ rải rác kèm sấm sét',
        'Moderate or heavy rain with thunder': 'Mưa vừa đến to kèm sấm sét',
        'Thunderstorm': 'Giông bão',
        
        # Snow and ice conditions
        'Patchy snow possible': 'Có thể có tuyết rải rác',
        'Patchy sleet possible': 'Có thể có mưa tuyết rải rác',
        'Patchy freezing drizzle possible': 'Có thể có mưa phùn đóng băng rải rác',
        'Blowing snow': 'Tuyết thổi',
        'Blizzard': 'Bão tuyết',
        'Freezing fog': 'Sương mù đóng băng',
        
        # Fog and mist
        'Fog': 'Sương mù',
        'Mist': 'Sương mù nhẹ',
        
        # Other conditions
        'Haze': 'Sương mù khô',
        'Smoke': 'Khói',
        'Dust': 'Bụi',
        'Sandstorm': 'Bão cát',
        'Tornado': 'Lốc xoáy',
        'Tropical storm': 'Bão nhiệt đới',
        'Hurricane': 'Bão',
        'Cold': 'Lạnh',
        'Hot': 'Nóng',
        'Windy': 'Gió mạnh',
        'Hail': 'Mưa đá',
    }
    
    # Return the translated condition or the original if not found
    return condition_map.get(condition, condition)

# Function to check if a date is within the valid forecast range (today to 7 days ahead)
def is_date_in_valid_range(date):
    """
    Check if a date is within the valid forecast range (today to 7 days ahead)
    
    Args:
        date (datetime.date): Date to check
        
    Returns:
        bool: True if date is within valid range, False otherwise
    """
    current_date = datetime.now().date()
    max_forecast_date = current_date + timedelta(days=7)
    
    # Date must be between today and 7 days ahead (inclusive)
    return current_date <= date <= max_forecast_date

# Function to get InfluxDB client
def get_influxdb_client():
    return InfluxDBClient(
        url=os.environ.get("INFLUXDB_URL", "http://influxdb:8086"),
        token=os.environ.get("INFLUXDB_TOKEN", "my-token"),
        org=os.environ.get("INFLUXDB_ORG", "my-org")
    )

# Function to query forecast data from InfluxDB
def get_forecast_data(locations, days=7, specific_day=None):
    """
    Get weather forecast data for specific locations
    
    Args:
        locations (list): List of location names
        days (int): Number of days to forecast (default: 7)
        specific_day (str, optional): Specific day to get forecast for ('tomorrow', '2days', etc.)
        
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
    
    # Always query for 7 days of forecast data, we'll filter specific days later
    query = f'''
    from(bucket: "weather_forecast")
        |> range(start: 0, stop: 7d)
        |> filter(fn: (r) => r._measurement == "weather_forecast"){location_filter}
        |> pivot(rowKey:["_time", "location"], columnKey: ["_field"], valueColumn: "_value")
        |> sort(columns: ["_time"])
    '''
    
    try:
        result = query_api.query(org=os.environ.get("INFLUXDB_ORG", "my-org"), query=query)
        
        # Process results
        forecast_data = {}
        
        # Get current date to filter results based on specific_day parameter
        current_date = datetime.now().date()
        target_dates = []
        
        # Determine which dates to include based on specific_day parameter
        if specific_day == 'tomorrow':
            # Only include tomorrow's date
            target_dates = [(current_date + timedelta(days=1)).strftime("%Y-%m-%d")]
        elif specific_day and specific_day.endswith('days'):
            # Handle requests for next X days or specific day
            try:
                day_value = int(specific_day.replace('days', ''))
                if 0 <= day_value <= 7:  # Ensure it's within valid range
                    if day_value == 0:  # Today
                        target_dates = [current_date.strftime("%Y-%m-%d")]
                    else:  # Specific future day
                        target_date = current_date + timedelta(days=day_value)
                        target_dates = [target_date.strftime("%Y-%m-%d")]
            except ValueError:
                # If parsing fails, include all dates (default behavior)
                pass
        
        for table in result:
            for record in table.records:
                location = record.values.get("location")
                time = record.get_time()
                
                # Format time as string (date only for forecasts)
                date_str = time.strftime("%Y-%m-%d")
                
                # Skip this record if we're filtering for specific dates and this date isn't in our target list
                if target_dates and date_str not in target_dates:
                    continue
                
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
    
    # Get all dates in the forecast data to create a more specific title
    all_dates = []
    for location in forecast_data.values():
        for date_str in location.keys():
            if date_str not in all_dates:
                all_dates.append(date_str)
    
    all_dates.sort()  # Sort dates chronologically
    
    # Create a more specific title based on the date range
    if len(all_dates) == 1:
        # Single day forecast
        date_obj = datetime.strptime(all_dates[0], "%Y-%m-%d")
        vn_date = date_obj.strftime("%d/%m/%Y")
        response = f"Dự báo thời tiết ngày {vn_date}:\n\n"
    elif len(all_dates) > 1:
        # Multiple day forecast
        start_date = datetime.strptime(all_dates[0], "%Y-%m-%d")
        end_date = datetime.strptime(all_dates[-1], "%Y-%m-%d")
        start_vn = start_date.strftime("%d/%m/%Y")
        end_vn = end_date.strftime("%d/%m/%Y")
        response = f"Dự báo thời tiết từ ngày {start_vn} đến ngày {end_vn}:\n\n"
    else:
        # Fallback if no dates (shouldn't happen)
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
                # Translate the condition from English to Vietnamese
                vn_condition = translate_weather_condition(data['condition'])
                response += f"  • Điều kiện: {vn_condition}\n"
                
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
    
    # Removed warning section as requested
    
    return response
