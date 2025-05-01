#!/bin/bash
set -e

# Function to check if Kafka is ready
check_kafka() {
  echo "Checking if Kafka is ready..."
  nc -z kafka 9092
  return $?
}

# Wait for Kafka to be ready
until check_kafka; do
  echo "Kafka is not ready yet - waiting..."
  sleep 5
done

echo "Kafka is ready! Starting weather producer..."
exec python weather_producer.py
