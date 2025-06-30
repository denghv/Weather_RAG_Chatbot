FROM bitnami/spark:3.5.3

USER root

# Cài đặt các công cụ build cần thiết
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cập nhật pip và setuptools trước
RUN pip install --upgrade pip setuptools wheel

# Cài đặt các dependencies Python trực tiếp thay vì qua requirements.txt
# Sử dụng phiên bản pandas mới hơn (2.2.0) thay vì 1.5.3
RUN pip install --no-cache-dir --only-binary=:all: \
    pyspark==3.5.3 \
    confluent-kafka==2.3.0 \
    minio==7.1.15 \
    pandas==2.2.0 \
    pyarrow==14.0.1 \
    pytz==2023.3

# Tạo thư mục jars nếu chưa tồn tại
RUN mkdir -p /opt/bitnami/spark/jars

# Sao chép script Python và script khởi động
COPY weather_aggregator.py start.sh /app/
RUN chmod +x /app/start.sh

WORKDIR /app

# Chạy script khởi động
ENTRYPOINT ["/app/start.sh"]

