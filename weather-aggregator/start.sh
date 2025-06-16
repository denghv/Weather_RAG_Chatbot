#!/bin/bash

# Đảm bảo biến môi trường được thiết lập với giá trị mặc định
SPARK_MASTER=${SPARK_MASTER:-"spark://spark-master:7077"}
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-"kafka1:9092,kafka2:9092,kafka3:9092"}
KAFKA_TOPIC=${KAFKA_TOPIC:-"weather-data"}
MINIO_ENDPOINT=${MINIO_ENDPOINT:-"http://minio:9000"}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-"minioadmin"}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-"minioadmin"}
MINIO_BUCKET=${MINIO_BUCKET:-"weather-aggregates"}
CHECKPOINT_LOCATION=${CHECKPOINT_LOCATION:-"/tmp/checkpoints/weather-aggregator"}

echo "Starting Spark application with the following configuration:"
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

# Chạy spark-submit với các tham số cố định (không sử dụng biến môi trường trong lệnh)
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
  --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.streaming.checkpointLocation=$CHECKPOINT_LOCATION \
  /app/weather_aggregator.py
