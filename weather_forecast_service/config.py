# InfluxDB configuration
INFLUXDB_URL = "http://influxdb:8086"  # Sử dụng tên service influxdb trong Docker network
INFLUXDB_TOKEN = "my-token"  # Token mặc định từ weather_consumer.py
INFLUXDB_ORG = "my-org"  # Org mặc định từ weather_consumer.py
INFLUXDB_BUCKET = "weather_forecast"  # The new bucket for forecast data

# Model and data paths
MODEL_DIR = "/app/dataset/unified_models"
HISTORICAL_DATA_PATH = "/app/dataset/weather2009-2021.csv"

# Locations to forecast (provinces in Vietnam)
LOCATIONS = [
    "Hanoi",
    "Ho Chi Minh City",
    "Hai Phong",
    "Thua Thien Hue",
    "Khanh Hoa",  # Nha Trang
    "Can Tho"
]

# Number of days to forecast
FORECAST_DAYS = 7

# Interval between forecasts (in seconds)
FORECAST_INTERVAL = 43200  # 12 hours
