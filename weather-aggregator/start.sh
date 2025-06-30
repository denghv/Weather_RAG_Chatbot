#!/bin/bash

# Đảm bảo biến môi trường được thiết lập với giá trị mặc định
# Sử dụng local[*] cho Spark local mode thay vì cluster mode
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-"kafka:9092"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"weather-data"}
MINIO_ENDPOINT=${MINIO_ENDPOINT:-"http://minio:9000"}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-"minioadmin"}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-"minioadmin"}
MINIO_BUCKET=${MINIO_BUCKET:-"weather-aggregates"}
CHECKPOINT_LOCATION=${CHECKPOINT_LOCATION:-"/tmp/checkpoints/weather-aggregator"}

echo "Starting Spark application in local mode with the following configuration:"
echo "SPARK_MASTER: $SPARK_MASTER"
echo "KAFKA_BOOTSTRAP_SERVERS: $KAFKA_BOOTSTRAP_SERVERS"
echo "KAFKA_TOPIC: $KAFKA_TOPIC"
echo "MINIO_ENDPOINT: $MINIO_ENDPOINT"
echo "MINIO_BUCKET: $MINIO_BUCKET"
echo "CHECKPOINT_LOCATION: $CHECKPOINT_LOCATION"

# Tạo thư mục checkpoint nếu chưa tồn tại
mkdir -p $CHECKPOINT_LOCATION

# Đợi một chút để các dịch vụ khác khởi động
echo "Waiting for services to start..."
sleep 30

# Chạy spark-submit với các tham số cho local mode
spark-submit \
  --master $SPARK_MASTER \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.6.0,org.apache.kafka:kafka_2.12:3.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.lz4:lz4-java:1.8.0,org.xerial.snappy:snappy-java:1.1.10.1 \
  --conf spark.driver.host=0.0.0.0 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.network.timeout=120s \
  --conf spark.executor.heartbeatInterval=30s \
  --conf spark.driver.memory=1g \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.default.parallelism=2 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  --conf spark.sql.streaming.checkpointLocation=$CHECKPOINT_LOCATION \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
  --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.connection.timeout=300000 \
  --conf spark.hadoop.fs.s3a.attempts.maximum=20 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /app/weather_aggregator.py





