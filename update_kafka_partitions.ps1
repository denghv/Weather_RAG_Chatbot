# PowerShell script để cập nhật số lượng partitions cho topic weather-data
Write-Host "Updating partitions for weather-data topic..."

# Sử dụng docker exec để chạy lệnh kafka-topics trong container kafka
# Đường dẫn đầy đủ trong hình ảnh Confluent
docker exec kafka /usr/bin/kafka-topics --bootstrap-server kafka:9092 --alter --topic weather-data --partitions 6

# Kiểm tra thông tin topic sau khi cập nhật
Write-Host "Checking topic details..."
docker exec kafka /usr/bin/kafka-topics --bootstrap-server kafka:9092 --describe --topic weather-data

# Chay lenh sau de setup kafka partitions
#powershell -ExecutionPolicy Bypass -File .\update_kafka_partitions.ps1