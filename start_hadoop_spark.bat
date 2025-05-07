@echo off
REM Start Hadoop and Spark services

echo Starting Hadoop and Spark services...
docker-compose up -d namenode datanode spark-master spark-worker

echo Waiting for Hadoop services to start...
timeout /t 30

echo Starting Spark Streaming application...
docker-compose up -d spark-streaming

echo Services started! You can access:
echo - Hadoop NameNode UI: http://localhost:9870
echo - Spark Master UI: http://localhost:8080
