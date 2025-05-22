
# InfluxDB configuration
INFLUXDB_URL = "http://influxdb:8086"  # Using influxdb service name in Docker network
INFLUXDB_TOKEN = "my-token"  # Default token from weather_consumer.py
INFLUXDB_ORG = "my-org"  # Default org from weather_consumer.py
INFLUXDB_BUCKET = "weather_forecast"  # The new bucket for forecast data

# Model and data paths
MODEL_DIR = "/app/dataset/unified_models"
HISTORICAL_DATA_PATH = "/app/dataset/weather2009-2021.csv"

# Locations to forecast (provinces in Vietnam)
LOCATIONS = ['An Giang', 'Ba Ria-Vung Tau', 'Bac Lieu', 'Ben Tre', 'Binh Dinh', 'Binh Thuan', 'Ca Mau', 'Can Tho', 'Dak Lak', 'Dong Nai', 'Gia Lai', 'Hai Duong', 'Hai Phong', 'Hanoi', 'Ho Chi Minh City', 'Hoa Binh', 'Khanh Hoa', 'Kien Giang', 'Lam Dong', 'Long An', 'Nam Dinh', 'Nghe An', 'Ninh Thuan', 'Phu Tho', 'Phu Yen', 'Quang Nam', 'Quang Ninh', 'Soc Trang', 'Thai Nguyen', 'Thanh Hoa', 'Thua Thien Hue', 'Tien Giang', 'Tra Vinh', 'Vinh Long', 'Yen Bai']

# Number of days to forecast
FORECAST_DAYS = 7

# Interval between forecasts (in seconds)
FORECAST_INTERVAL = 43200  # 12 hours
