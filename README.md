# Weather RAG Chatbot

A system for collecting weather data from 63 provinces in Vietnam using WeatherAPI, Kafka, and InfluxDB.

## Components

- **Zookeeper**: Coordination service for Kafka
- **Kafka**: Message broker for storing weather data
- **InfluxDB**: Time series database for storing weather data
- **Weather Producer**: Python service that fetches weather data every 10 minutes
- **Weather Consumer**: Python service that reads data from Kafka and writes to InfluxDB

## Architecture

1. Weather data is fetched from WeatherAPI.com by the producer service
2. Data is published to a Kafka topic named "weather-data"
3. Each province's data is sent as a separate message with the province name as the key
4. The consumer service reads messages from Kafka and writes them to InfluxDB
5. All timestamps are converted to Asia/Ho_Chi_Minh timezone before storage

## Setup and Running

1. Make sure you have Docker and Docker Compose installed
2. Clone this repository
3. Run the system:
   ```bash
   docker-compose up -d
   ```
4. To check the status of the services:
   ```bash
   docker-compose ps
   ```
5. To view logs from a specific service (e.g., the consumer):
   ```bash
   docker logs weather_rag_chatbot-weather-consumer-1
   ```

## Querying Data from InfluxDB

### Accessing the InfluxDB CLI

1. Connect to the InfluxDB container:
   ```bash
   docker exec -it weather_rag_chatbot-influxdb-1 bash
   ```

2. Set environment variables for convenience (to avoid specifying org and token with each query):
   ```bash
   export INFLUX_ORG=my-org
   export INFLUX_TOKEN=my-token
   ```

### Example Queries

1. Query weather data for Hanoi from the last 30 days:
   ```bash
   influx query 'from(bucket:"weather") |> range(start: -30d) |> filter(fn: (r) => r._measurement == "weather" and r.location == "Hanoi")'
   ```

2. Query temperature data for all locations from the last hour:
   ```bash
   influx query 'from(bucket:"weather") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "weather" and r._field == "temp_c")'
   ```

3. Query air quality data (PM2.5) for Ho Chi Minh City from the last day:
   ```bash
   influx query 'from(bucket:"weather") |> range(start: -1d) |> filter(fn: (r) => r._measurement == "weather" and r._field == "pm2_5" and r.location == "Ho Chi Minh City")'
   ```

## Configuration

You can modify the following environment variables in the docker-compose.yml file:

### Weather Producer
- `WEATHER_API_KEY`: Your WeatherAPI.com API key
- `KAFKA_TOPIC`: The Kafka topic to publish data to (default: weather-data)
- `TZ`: Timezone setting (default: Asia/Ho_Chi_Minh)

### Weather Consumer
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string
- `KAFKA_TOPIC`: The Kafka topic to consume from
- `INFLUXDB_URL`: InfluxDB connection URL
- `INFLUXDB_TOKEN`: Authentication token for InfluxDB
- `INFLUXDB_ORG`: Organization name in InfluxDB
- `INFLUXDB_BUCKET`: Bucket name for storing weather data
- `TZ`: Timezone setting (default: Asia/Ho_Chi_Minh)

### InfluxDB
- `DOCKER_INFLUXDB_INIT_MODE`: Initialization mode
- `DOCKER_INFLUXDB_INIT_USERNAME`: Admin username
- `DOCKER_INFLUXDB_INIT_PASSWORD`: Admin password
- `DOCKER_INFLUXDB_INIT_ORG`: Organization name
- `DOCKER_INFLUXDB_INIT_BUCKET`: Initial bucket name
- `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN`: Admin token
- `INFLUX_ORG`: Default organization for CLI commands
- `INFLUX_TOKEN`: Default token for CLI commands
- `TZ`: Timezone setting (default: Asia/Ho_Chi_Minh)
