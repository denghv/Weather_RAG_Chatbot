# Weather RAG Chatbot

A comprehensive system for collecting, processing, and querying weather data from 63 provinces in Vietnam using WeatherAPI, Kafka, InfluxDB, Hadoop, and Spark. The system includes a natural language chatbot for querying weather information.

## Components

### Data Collection and Storage
- **Zookeeper**: Coordination service for Kafka
- **Kafka**: Message broker for streaming weather data
- **InfluxDB**: Time series database for real-time weather data queries
- **Hadoop (HDFS)**: Distributed file system for long-term storage and batch processing

### Data Processing
- **Weather Producer**: Python service that fetches weather data every 10 minutes
- **Weather Consumer**: Python service that reads data from Kafka and writes to InfluxDB
- **Spark Streaming**: Apache Spark application that processes data from Kafka and stores it in Hadoop

### User Interface
- **Weather RAG Chatbot**: Flask-based web application that uses OpenAI's GPT models to answer natural language questions about weather data

## Architecture

### Data Flow
1. Weather data is fetched from WeatherAPI.com by the producer service every 10 minutes
2. Data is published to a Kafka topic named "weather-data"
3. Each province's data is sent as a separate message with the province name as the key
4. The data flows in two parallel paths:
   - **Real-time path**: The consumer service reads messages from Kafka and writes them to InfluxDB for real-time queries
   - **Batch processing path**: Spark Streaming reads the same data from Kafka, processes it in micro-batches, and stores it in Hadoop HDFS for long-term storage and analysis
5. The Weather RAG Chatbot queries InfluxDB to answer user questions about current and historical weather data

### Data Processing Details
- All timestamps are converted to Asia/Ho_Chi_Minh timezone (UTC+7) before storage
- Spark Streaming aggregates data for all 63 provinces in Vietnam before storing in HDFS
- Data in HDFS is stored in Parquet format with Snappy compression for efficient storage and querying

## Setup and Running

1. Make sure you have Docker and Docker Compose installed
2. Clone this repository
3. Run the core system (Kafka, InfluxDB, data collection):
   ```bash
   docker-compose up -d
   ```
4. Run the Hadoop and Spark components:
   ```bash
   .\start_hadoop_spark.bat
   ```
5. Access the chatbot web interface at http://localhost:5000
6. To check the status of all services:
   ```bash
   docker-compose ps
   ```
7. To view logs from a specific service (e.g., the consumer):
   ```bash
   docker logs weather_rag_chatbot-weather-consumer-1
   ```

### Accessing Component UIs
- **Hadoop NameNode UI**: http://localhost:9870
- **Spark Master UI**: http://localhost:8080
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

## Querying Data from Hadoop HDFS

The weather data stored in Hadoop HDFS is organized in Parquet files partitioned by time windows. This data is ideal for batch processing and historical analysis.

### Accessing HDFS Data

1. Connect to the namenode container:
   ```bash
   docker exec -it namenode bash
   ```

2. List the weather data directory:
   ```bash
   hdfs dfs -ls /weather-data
   ```

3. View a specific partition:
   ```bash
   hdfs dfs -ls /weather-data/window=2025-05-03T15:00:00.000Z
   ```

4. Copy data to local filesystem for analysis:
   ```bash
   hdfs dfs -copyToLocal /weather-data/window=2025-05-03T15:00:00.000Z /tmp/weather-data
   ```

### Using Spark for Data Analysis

You can use Spark to analyze the weather data stored in HDFS:

1. Connect to the Spark master container:
   ```bash
   docker exec -it spark-master bash
   ```

2. Start the PySpark shell:
   ```bash
   pyspark
   ```

3. Read the Parquet data:
   ```python
   df = spark.read.parquet("hdfs://namenode:8020/weather-data")
   df.printSchema()
   ```

4. Perform analysis:
   ```python
   # Show the average temperature by location
   df.select("location", "temp_c").groupBy("location").avg("temp_c").show()
   
   # Find the locations with the highest air quality (lowest PM2.5)
   df.select("location", "pm2_5").orderBy("pm2_5").show(10)
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

### Chatbot
- `OPENAI_API_KEY`: Your OpenAI API key for GPT models
- `INFLUXDB_URL`: InfluxDB connection URL
- `INFLUXDB_TOKEN`: Authentication token for InfluxDB
- `INFLUXDB_ORG`: Organization name in InfluxDB
- `INFLUXDB_BUCKET`: Bucket name for querying weather data

### Spark Streaming
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string
- `KAFKA_TOPIC`: The Kafka topic to consume from
- `HDFS_URL`: HDFS URL (default: hdfs://namenode:8020)
- `HDFS_PATH`: Path in HDFS to store data (default: /weather-data)
- `CHECKPOINT_LOCATION`: Location for Spark checkpoints
- `BATCH_INTERVAL_SECONDS`: Micro-batch interval in seconds (default: 600)
- `PROVINCE_COUNT`: Number of provinces to collect before storing (default: 63)
