#!/bin/bash
set -e

# Function to check if Kafka is ready
check_kafka() {
  echo "Checking if Kafka is ready..."
  nc -z kafka 9092
  return $?
}

# Function to check if InfluxDB is ready
check_influxdb() {
  echo "Checking if InfluxDB is ready..."
  nc -z influxdb 8086
  return $?
}

# Wait for Kafka to be ready
until check_kafka; do
  echo "Kafka is not ready yet - waiting..."
  sleep 5
done

# Wait for InfluxDB to be ready
until check_influxdb; do
  echo "InfluxDB is not ready yet - waiting..."
  sleep 5
done

echo "Kafka and InfluxDB are ready! Starting weather consumer..."
exec python weather_consumer.py
