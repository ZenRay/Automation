#!/bin/bash
set -e

# 停止当前运行的容器
echo "Stopping existing containers..."
docker-compose down

# 清理日志（可选）
if [ "$1" == "--clean-logs" ]; then
  echo "Cleaning logs..."
  rm -rf logs
  mkdir -p logs
fi

# 清理数据库（仅在必要时使用）
if [ "$1" == "--reset-db" ]; then
  echo "Resetting database volume..."
  docker volume rm dispatcher_mysql-db-volume || true
fi

# 启动容器
echo "Starting containers..."
docker-compose up -d

# 等待服务启动
echo "Waiting for Airflow to start..."
attempt=0
max_attempts=30
until $(curl --output /dev/null --silent --head --fail http://localhost:8080/health); do
  if [ ${attempt} -eq ${max_attempts} ]; then
    echo "Airflow failed to start after ${max_attempts} attempts. Check logs for details."
    exit 1
  fi
  
  printf '.'
  attempt=$((attempt+1))
  sleep 5
done

echo
echo "Airflow is up and running!"
echo "Access the web UI at: http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
