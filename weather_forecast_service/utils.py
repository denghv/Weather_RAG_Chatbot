import sys
import os
import logging
import pandas as pd

# Add the parent directory to the path so we can import from the main project
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import the standard province names
from provinces import VIETNAM_PROVINCES

# Configure logging
logger = logging.getLogger('weather_forecast_utils')


def standardize_province_name(province_name):
    """
    Standardize province name to match the official list in provinces.py.
    
    Args:
        province_name (str): The province name to standardize
        
    Returns:
        str: The standardized province name
    """
    if not province_name:
        return None
    
    # Convert to string and clean up
    province_name = str(province_name).strip()
    
    # Direct mapping for common variations
    mapping = {
        "TP.HCM": "Ho Chi Minh City",
        "TP. HCM": "Ho Chi Minh City",
        "Tp. HCM": "Ho Chi Minh City",
        "Tp.HCM": "Ho Chi Minh City",
        "Ho Chi Minh": "Ho Chi Minh City",
        "Hồ Chí Minh": "Ho Chi Minh City",
        "Thành phố Hồ Chí Minh": "Ho Chi Minh City",
        "Hà Nội": "Hanoi",
        "Đà Nẵng": "Da Nang",
        "Huế": "Thua Thien Hue",
        "Hue": "Thua Thien Hue",
        "Nha Trang": "Khanh Hoa",
        "Cần Thơ": "Can Tho",
        "Hải Phòng": "Hai Phong"
    }
    
    # Check if we have a direct mapping
    if province_name in mapping:
        return mapping[province_name]
    
    # Check if the name is already in the standard list
    if province_name in VIETNAM_PROVINCES:
        return province_name
    
    # Try to find the closest match
    for std_province in VIETNAM_PROVINCES:
        # Simple case-insensitive comparison
        if province_name.lower() == std_province.lower():
            return std_province
    
    # If no match found, log a warning and return the original name
    logger.warning(f"Could not standardize province name: {province_name}")
    return province_name


def load_and_standardize_historical_data(data_path):
    """
    Load historical weather data and standardize province names.
    
    Args:
        data_path (str): Path to the CSV file with historical weather data
        
    Returns:
        pd.DataFrame: DataFrame with standardized province names
    """
    logger.info(f"Loading historical data from {data_path}")
    historical_data = pd.read_csv(data_path)
    
    # Convert date to datetime
    historical_data['date'] = pd.to_datetime(historical_data['date'])
    
    # Extract month, year, day and other date features
    historical_data['month'] = historical_data['date'].dt.month
    historical_data['year'] = historical_data['date'].dt.year
    historical_data['day'] = historical_data['date'].dt.day
    historical_data['dayofweek'] = historical_data['date'].dt.dayofweek
    historical_data['dayofyear'] = historical_data['date'].dt.dayofyear
    
    # Standardize province names
    logger.info("Standardizing province names")
    historical_data['province'] = historical_data['province'].apply(standardize_province_name)
    
    # Log unique provinces after standardization
    unique_provinces = historical_data['province'].unique()
    logger.info(f"Unique provinces after standardization: {len(unique_provinces)}")
    logger.debug(f"Provinces: {sorted(unique_provinces)}")
    
    return historical_data


def get_location_coordinates(province_name):
    """
    Get approximate coordinates for a province in Vietnam.
    This is a simplified version - in a real application, you'd use a more accurate geocoding service.
    
    Args:
        province_name (str): The standardized province name
        
    Returns:
        tuple: (latitude, longitude) coordinates
    """
    # Approximate coordinates for some major provinces
    coordinates = {
        "Hanoi": (21.0285, 105.8542),
        "Ho Chi Minh City": (10.8231, 106.6297),
        "Da Nang": (16.0544, 108.2022),
        "Can Tho": (10.0452, 105.7469),
        "Hai Phong": (20.8449, 106.6881),
        "Thua Thien Hue": (16.4637, 107.5909),
        "Khanh Hoa": (12.2388, 109.1967),  # Nha Trang
        "Ba Ria-Vung Tau": (10.3461, 107.0843),
        "Lam Dong": (11.9404, 108.4583),  # Da Lat
    }
    
    # Standardize the input province name
    std_province = standardize_province_name(province_name)
    
    # Return coordinates if available, otherwise return a default
    if std_province in coordinates:
        return coordinates[std_province]
    else:
        # Default to center of Vietnam if province not found
        logger.warning(f"No coordinates found for province: {std_province}")
        return (16.0, 108.0)  # Approximate center of Vietnam
