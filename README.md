# Weather RAG Chatbot

A system for collecting weather data from 63 provinces in Vietnam using WeatherAPI and Kafka.

## Components

- **Kafka**: Message broker for storing weather data
- **Weather Producer**: Python service that fetches weather data every 10 minutes
- **Kafka UI**: Web interface for monitoring Kafka topics and messages

## Setup

1. Make sure you have Docker and Docker Compose installed
2. Clone this repository
3. Run `docker-compose up -d`
4. Access Kafka UI at http://localhost:8080

## Architecture

- Weather data is fetched from WeatherAPI.com
- Data is published to a Kafka topic named "weather-data"
- Each province's data is sent as a separate message with the province name as the key

## Configuration

You can modify the following environment variables in the docker-compose.yml file:
- `WEATHER_API_KEY`: Your WeatherAPI.com API key
- `KAFKA_TOPIC`: The Kafka topic to publish data to (default: weather-data)
