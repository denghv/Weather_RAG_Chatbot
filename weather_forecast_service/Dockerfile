FROM python:3.10-slim

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY weather_forecast_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the provinces module and other necessary files
COPY provinces.py /app/
COPY dataset/unified_models /app/dataset/unified_models
COPY dataset/weather2009-2021.csv /app/dataset/weather2009-2021.csv

# Copy the forecast service code
COPY weather_forecast_service /app/weather_forecast_service

# Set environment variables
ENV PYTHONPATH=/app

# Run the forecast service
CMD ["python", "/app/weather_forecast_service/forecast_service.py"]
