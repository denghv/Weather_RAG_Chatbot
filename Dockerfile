FROM python:3.9-slim

WORKDIR /app

# Install netcat for the health check
RUN apt-get update && \
    apt-get install -y --no-install-recommends netcat-traditional && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY weather_producer.py .
COPY provinces.py .
COPY start.sh .

RUN chmod +x /app/start.sh

CMD ["/app/start.sh"]
