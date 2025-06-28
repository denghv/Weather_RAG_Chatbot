FROM python:3.9-slim

WORKDIR /app

# Install netcat for health check and librdkafka dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends netcat-traditional build-essential librdkafka-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY weather_producer.py .
COPY provinces.py .

# Run the producer directly
CMD ["python", "weather_producer.py"]
