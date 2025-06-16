# Weather RAG Chatbot

A comprehensive system for collecting, processing, and querying weather data from 63 provinces in Vietnam using WeatherAPI, Kafka, and InfluxDB. The system includes a natural language chatbot for querying weather information.

## Components

### Data Collection and Storage
- **Kafka Cluster**: 3-broker Kafka cluster using KRaft mode (no Zookeeper required) for high-availability message streaming
- **InfluxDB**: Time series database for real-time weather data queries and storage
- **MinIO**: Object storage for aggregated weather data (daily max/min values)
- **Spark Cluster**: Apache Spark for stream processing and windowed aggregations

### Data Processing
- **Weather Producer**: Python service that fetches weather data every 10 minutes
- **Weather Consumer**: Python service that reads data from Kafka cluster and writes to InfluxDB
- **Weather Aggregator**: Spark Streaming service that performs windowed aggregation (daily max/min) and stores results in MinIO

### User Interface
- **Weather RAG Chatbot**: Flask-based web application that uses OpenAI's GPT models to answer natural language questions about weather data

## Architecture

### Data Flow
1. Weather data is fetched from WeatherAPI.com by the producer service every 10 minutes
2. Data is published to a Kafka cluster (3 brokers) topic named "weather-data" with replication factor 3
3. Each province's data is sent as a separate message with the province name as the key
4. The data flows in two parallel paths:
   - **Real-time path**: The consumer service reads messages from Kafka cluster and writes them to InfluxDB for real-time queries
   - **Aggregation path**: The weather aggregator (Spark Streaming) performs windowed aggregation to calculate daily max/min values and stores results in MinIO
5. The Weather RAG Chatbot queries InfluxDB to answer user questions about current and historical weather data

### Data Processing Details
- All timestamps are converted to Asia/Ho_Chi_Minh timezone (UTC+7) before storage
- The system processes approximately 63 weather records every 10 minutes (one per province)
- Real-time data is stored in InfluxDB for immediate queries and historical analysis
- Daily aggregations (max/min values) are calculated using Spark Streaming with 1-day windows and stored in MinIO
- Windowed aggregation uses outputMode = "update" to continuously update daily max/min values as new data arrives

## Setup and Running

1. Make sure you have Docker and Docker Compose installed
2. Clone this repository
3. Run the system:
   ```bash
   # Start Kafka cluster first
   docker-compose up -d kafka1 kafka2 kafka3

   # Wait for cluster to be ready, then start all services
   docker-compose up -d
   ```
4. Access the chatbot web interface at http://localhost:5000
5. To check the status of all services:
   ```bash
   docker-compose ps
   ```
6. To view logs from a specific service (e.g., the consumer):
   ```bash
   docker logs weather_rag_chatbot-weather-consumer-1
   ```

### Accessing Component UIs
- **InfluxDB API**: http://localhost:8086 (for direct API access, no UI component)

## Using the Weather RAG Chatbot

The Weather RAG Chatbot provides a natural language interface to query weather data for all 63 provinces in Vietnam.

### Accessing the Chatbot

1. Open your web browser and navigate to http://localhost:5000
2. Type your weather-related question in Vietnamese or English
3. The chatbot will extract location and time information from your query
4. It will then fetch the relevant weather data from InfluxDB and generate a natural language response

### Example Questions

- "Thời tiết ở Hà Nội hôm nay thế nào?"
- "Nhiệt độ ở Thành phố Hồ Chí Minh hiện tại?"
- "Chỉ số chất lượng không khí ở Đà Nẵng?"
- "Dự báo UV ở Huế?"
- "So sánh nhiệt độ giữa Hà Nội và Hồ Chí Minh?"

### How It Works

1. The chatbot uses OpenAI's GPT models to understand your natural language query
2. It extracts relevant entities (location, time, weather attributes)
3. It maps Vietnamese location names to English using a predefined mapping
4. It queries InfluxDB for the requested weather data
5. It formats the data and generates a natural language response using GPT



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

### Handling Timezone in Queries

InfluxDB stores all timestamps in UTC internally. To convert timestamps to Asia/Ho_Chi_Minh timezone (UTC+7) in your queries, use the `timeShift()` function:

```bash
# Add 7 hours to convert from UTC to Asia/Ho_Chi_Minh timezone
influx query 'from(bucket:"weather") 
  |> range(start: -1h) 
  |> filter(fn: (r) => r._measurement == "weather") 
  |> timeShift(duration: 7h, columns: ["_time"])'
```

This will display timestamps in your local timezone instead of UTC. Note that the data itself is stored correctly with the proper timestamps - it's just being displayed in UTC format when you query it directly from InfluxDB without the timeShift function.

## Kafka Cluster Management

The system now uses a 3-broker Kafka cluster with KRaft mode (no Zookeeper required) for high availability and fault tolerance.

### Cluster Configuration:
- **kafka1**: Port 9092 (client), 9093 (controller)
- **kafka2**: Port 9094 (client), 9095 (controller)
- **kafka3**: Port 9096 (client), 9097 (controller)
- **Replication Factor**: 3
- **Min In-Sync Replicas**: 2

### Management Script:
Use the provided PowerShell script for easy cluster management:
```powershell
# Start Kafka cluster
.\manage-kafka-cluster.ps1 start

# Check cluster status
.\manage-kafka-cluster.ps1 status

# Create weather-data topic
.\manage-kafka-cluster.ps1 create-topic

# View all available commands
.\manage-kafka-cluster.ps1 help
```

For detailed information about the Kafka upgrade, see [KAFKA_UPGRADE_README.md](./KAFKA_UPGRADE_README.md).

## System Simplification

The system has been simplified by removing Hadoop HDFS and Apache Spark components. All data is now stored and queried directly from InfluxDB, providing:

- **Simplified Architecture**: Fewer components to manage
- **Better Performance**: Reduced resource usage and faster startup
- **Easier Maintenance**: Single source of truth for all weather data
- **Real-time Queries**: Direct access to time-series data

For details about the Hadoop removal, see [HADOOP_REMOVAL_README.md](./HADOOP_REMOVAL_README.md).

## Configuration

You can modify the following environment variables in the docker-compose.yml file:

### Weather Producer
- `WEATHER_API_KEY`: Your WeatherAPI.com API key
- `KAFKA_TOPIC`: The Kafka topic to publish data to (default: weather-data)
- `TZ`: Timezone setting (default: Asia/Ho_Chi_Minh)

### Weather Consumer
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka cluster connection string (default: kafka1:9092,kafka2:9092,kafka3:9092)
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

### Chatbot
- `OPENAI_API_KEY`: Your OpenAI API key for GPT models
- `INFLUXDB_URL`: InfluxDB connection URL
- `INFLUXDB_TOKEN`: Authentication token for InfluxDB
- `INFLUXDB_ORG`: Organization name in InfluxDB
- `INFLUXDB_BUCKET`: Bucket name for querying weather data


